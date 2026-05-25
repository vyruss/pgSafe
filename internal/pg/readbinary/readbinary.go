// Package readbinary streams files out of $PGDATA via PG's
// pg_read_binary_file(filename, offset, length) SQL function. Used by
// remote-parallel backup mode where pgSafe reads the cluster files via
// libpq instead of through pg_basebackup.
//
//	The backup user needs the
//
// pg_read_server_files predefined role.
package readbinary

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultChunkSize is how many bytes each pg_read_binary_file call requests.
// 64 MiB is well below PG's 1 GiB ceiling per call and gives the network
// buffer + libpq result buffer reasonable working-set sizes.
const DefaultChunkSize = 64 * 1024 * 1024

// Reader streams a single PG-side file via repeated chunked SQL calls.
// Implements io.Reader.
type Reader struct {
	pool      *pgxpool.Pool
	path      string
	totalSize int64
	chunkSize int64
	offset    int64
	buf       []byte
	bufPos    int
	done      bool
}

// NewReader returns a Reader for path (server-relative — typically a path
// under $PGDATA, e.g. "global/pg_control"). totalSize must be the file's
// current size (obtained via pg_stat_file). chunkSize<=0 uses the default.
func NewReader(pool *pgxpool.Pool, path string, totalSize, chunkSize int64) *Reader {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Reader{
		pool:      pool,
		path:      path,
		totalSize: totalSize,
		chunkSize: chunkSize,
	}
}

// Read implements io.Reader. Returns io.EOF when offset == totalSize.
func (r *Reader) Read(p []byte) (int, error) {
	if r.bufPos < len(r.buf) {
		n := copy(p, r.buf[r.bufPos:])
		r.bufPos += n
		return n, nil
	}
	if r.done {
		return 0, io.EOF
	}
	if err := r.fetchNext(context.Background()); err != nil {
		return 0, err
	}
	if len(r.buf) == 0 {
		r.done = true
		return 0, io.EOF
	}
	n := copy(p, r.buf)
	r.bufPos = n
	return n, nil
}

func (r *Reader) fetchNext(ctx context.Context) error {
	if r.offset >= r.totalSize {
		r.buf = nil
		r.bufPos = 0
		return nil
	}
	want := r.chunkSize
	if remaining := r.totalSize - r.offset; remaining < want {
		want = remaining
	}
	row := r.pool.QueryRow(ctx,
		`SELECT pg_read_binary_file($1, $2, $3, true)`,
		r.path, r.offset, want)
	var chunk []byte
	if err := row.Scan(&chunk); err != nil {
		return fmt.Errorf("readbinary: pg_read_binary_file %s @%d len=%d: %w",
			r.path, r.offset, want, err)
	}
	if len(chunk) == 0 && r.offset < r.totalSize {
		return errors.New("readbinary: unexpected empty chunk")
	}
	r.buf = chunk
	r.bufPos = 0
	r.offset += int64(len(chunk))
	return nil
}

// Close releases the Reader's working buffer (the pool is not owned).
func (r *Reader) Close() error {
	r.buf = nil
	r.done = true
	return nil
}

// FileEntry is one PG-side file the caller wants to stream. The
// remote-parallel caller builds a slice of these via List.
type FileEntry struct {
	Path string // server-relative, e.g. "global/pg_control"
	Size int64
}

// ListPGData walks the cluster's PGDATA recursively, returning every
// regular file the caller needs to back up. The exclusion list
// matches PG's basebackup excludes (pg_wal, postmaster.pid, etc.) — the
// operator's archive_command is responsible for WAL.
//
// Implementation: a recursive CTE that only descends into directories
// (filtered via pg_stat_file().isdir), with explicit excludes for
// runtime-only PG paths.
func ListPGData(ctx context.Context, pool *pgxpool.Pool) ([]FileEntry, error) {
	const sql = `
WITH RECURSIVE walked(path, depth, isdir, size) AS (
    SELECT
        ''::text,
        0,
        true,
        0::bigint
  UNION ALL
    SELECT
        full_path,
        walked.depth + 1,
        st.isdir,
        st.size
    FROM walked,
         LATERAL pg_ls_dir(
             CASE WHEN walked.path = ''
                  THEN current_setting('data_directory')
                  ELSE current_setting('data_directory') || '/' || walked.path
             END,
             true, false
         ) AS entry,
         LATERAL (SELECT
             CASE WHEN walked.path = '' THEN entry ELSE walked.path || '/' || entry END
                  AS full_path) AS p,
         LATERAL pg_stat_file(
             current_setting('data_directory') || '/' || full_path,
             true
         ) AS st
    WHERE walked.isdir
      AND walked.depth < 12
      AND full_path NOT LIKE 'pg_wal%'
      AND full_path NOT IN ('postmaster.pid', 'postmaster.opts', 'pg_internal.init')
)
SELECT path, size
FROM walked
WHERE path <> '' AND NOT isdir
`
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("readbinary: ListPGData query: %w", err)
	}
	defer rows.Close()

	var out []FileEntry
	for rows.Next() {
		var path string
		var size int64
		if err := rows.Scan(&path, &size); err != nil {
			return nil, fmt.Errorf("readbinary: scan: %w", err)
		}
		out = append(out, FileEntry{Path: path, Size: size})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("readbinary: rows.Err: %w", err)
	}
	return out, nil
}

// ListPGDataDirs walks PGDATA and returns every directory PG expects to
// exist on startup but that contains no regular files at backup time
// (pg_notify, pg_replslot, pg_serial, pg_snapshots, pg_stat_tmp, etc.).
// Restore mkdirs these so PG's startup checks pass — they're absent from
// ListPGData (which yields only files) precisely because they're empty.
func ListPGDataDirs(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	const sql = `
WITH RECURSIVE walked(path, depth, isdir) AS (
    SELECT ''::text, 0, true
  UNION ALL
    SELECT
        full_path,
        walked.depth + 1,
        st.isdir
    FROM walked,
         LATERAL pg_ls_dir(
             CASE WHEN walked.path = ''
                  THEN current_setting('data_directory')
                  ELSE current_setting('data_directory') || '/' || walked.path
             END,
             true, false
         ) AS entry,
         LATERAL (SELECT
             CASE WHEN walked.path = '' THEN entry ELSE walked.path || '/' || entry END
                  AS full_path) AS p,
         LATERAL pg_stat_file(
             current_setting('data_directory') || '/' || full_path,
             true
         ) AS st
    WHERE walked.isdir
      AND walked.depth < 12
      AND full_path NOT LIKE 'pg_wal%'
)
SELECT path
FROM walked
WHERE path <> '' AND isdir
ORDER BY path
`
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("readbinary: ListPGDataDirs query: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("readbinary: scan dir: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("readbinary: dirs rows.Err: %w", err)
	}
	return out, nil
}
