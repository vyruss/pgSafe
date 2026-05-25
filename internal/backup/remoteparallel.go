package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/filter/pagechecksum"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/pg/bracket"
	"github.com/vyruss/pgsafe/internal/pg/readbinary"
	"golang.org/x/sync/errgroup"
)

// ModeRemoteParallel is the mid-tier backup mode: N libpq workers pulling
// chunks via pg_read_binary_file from the backup host, with client-side
// page-checksum validation before bytes hit the filter chain.
const ModeRemoteParallel Mode = "remote-parallel"

// RemoteParallelOptions extends Options with the fields specific to this
// mode. Embedded into backup.Options so Run can dispatch.
type RemoteParallelOptions struct {
	// Pool is the pgxpool.Pool the workers share for SQL traffic.
	// default sizing: at least Workers + 5 connections.
	Pool *pgxpool.Pool

	// Workers is the number of parallel goroutines. <=0 means
	// runtime.NumCPU() capped at 8.
	Workers int

	// PGVersion is needed to pick the right bracket dialect (PG 13/14 vs
	// 15+). The caller queries Cluster.Identity early; this is a
	// belt-and-braces override for tests where Identity isn't reliable.
	PGVersion int

	// PageChecksumMode selects how strictly we validate PG pages.
	// Default: ModeStrict for clusters with data_checksums=on, ModeOff
	// otherwise. The caller probes the cluster to pick a safe
	// default; operators can override via config.
	PageChecksumMode pagechecksum.Mode
}

// runRemoteParallel implements ModeRemoteParallel. It mirrors runSimple's
// step ordering (Identity → bracket.Start → file streams → bracket.Stop
// → WAL-wait → manifest+sidecar → Commit), but uses bracket+readbinary
// instead of pg_basebackup.
func runRemoteParallel(ctx context.Context, opts Options, rpOpts RemoteParallelOptions) (Result, error) {
	if rpOpts.Pool == nil {
		return Result{}, errors.New("backup: remote-parallel requires Pool")
	}
	if rpOpts.PGVersion < 13 {
		return Result{}, errors.New("backup: remote-parallel requires PGVersion >= 13")
	}

	startedAt := opts.Now()
	backupID, err := ChooseBackupID(ctx, opts.Backend, opts.Type, startedAt)
	if err != nil {
		return Result{}, err
	}

	logf := func(format string, args ...any) {
		_, _ = fmt.Fprintf(stderrFor(opts), "pgsafe backup (rp): "+format+"\n", args...)
	}
	logf("starting id=%s server=%s workers=%d", backupID, opts.Server, workersDefault(rpOpts.Workers))

	// Invariant #5 — verify the operator's archive_command before
	// bothering PG with bracket.Start.
	if rpOpts.Pool != nil {
		if err := ProbeArchive(ctx, rpOpts.Pool, opts.Backend, opts.WALTimeout); err != nil {
			return Result{}, fmt.Errorf("backup: %w", err)
		}
		logf("WAL archive reachability probe: OK")
	}

	// Cluster identity (system identifier, WAL segment size).
	id, err := opts.Cluster.Identity(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("backup: identity: %w", err)
	}
	if err := verifyClusterIdentity(ctx, opts.Backend, id.SystemIdentifier); err != nil {
		return Result{}, err
	}

	// Invariant #8 — backup-from-standby coordination.
	if err := InspectStandby(ctx, rpOpts.Pool); err != nil {
		return Result{}, fmt.Errorf("backup: %w", err)
	}

	// Bracket: pg_backup_start.
	br, err := bracket.New(rpOpts.Pool, rpOpts.PGVersion)
	if err != nil {
		return Result{}, fmt.Errorf("backup: bracket: %w", err)
	}
	startInfo, err := br.Start(ctx, opts.Label, true)
	if err != nil {
		return Result{}, fmt.Errorf("backup: bracket.Start: %w", err)
	}
	logf("bracket.Start: lsn=%s timeline=%d", startInfo.LSN, startInfo.Timeline)

	// File-list discovery from the PG side.
	files, err := readbinary.ListPGData(ctx, rpOpts.Pool)
	if err != nil {
		// Best-effort cleanup: try to stop the bracket so the cluster doesn't
		// stay in a "backup in progress" state.
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: ListPGData: %w", err)
	}
	logf("discovered %d files in $PGDATA", len(files))

	// Manifest builder.
	mb := manifest.NewBuilder(manifest.BackupStartInfo{
		SystemIdentifier: id.SystemIdentifier,
		Timeline:         startInfo.Timeline,
		StartLSN:         startInfo.LSN,
		StartTime:        startedAt.UTC(),
	})

	// Parallel file-streaming through the filter chain into the storage.
	workers := workersDefault(rpOpts.Workers)
	var (
		fileCount  int64
		bytesTotal int64
		mbMu       muLockable
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, f := range files {
		f := f
		g.Go(func() error {
			n, sum, modTime, err := streamOneFile(gctx, rpOpts, f, opts, backupID)
			if err != nil {
				return fmt.Errorf("file %s: %w", f.Path, err)
			}
			mbMu.Lock()
			mb.AddFile(f.Path, n, sum, modTime)
			mbMu.Unlock()
			atomic.AddInt64(&fileCount, 1)
			atomic.AddInt64(&bytesTotal, n)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: workers: %w", err)
	}

	// Bracket.Stop: returns stop LSN + backup_label + tablespace_map.
	stopInfo, err := br.Stop(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("backup: bracket.Stop: %w", err)
	}
	logf("bracket.Stop: lsn=%s", stopInfo.LSN)

	// Write backup_label and tablespace_map into the storage. PG expects them
	// at the cluster root on restore. We write them through the same filter
	// chain so they get the same encryption/compression treatment as data
	// files; the manifest records their plaintext SHA-256.
	if err := writeBlobThroughChain(ctx, opts, backupID, "backup_label", stopInfo.LabelFile, mb, &mbMu); err != nil {
		return Result{}, err
	}
	if len(stopInfo.SpcMapFile) > 0 {
		if err := writeBlobThroughChain(ctx, opts, backupID, "tablespace_map", stopInfo.SpcMapFile, mb, &mbMu); err != nil {
			return Result{}, err
		}
	}

	// WAL-wait — same logic as simple mode, against the bracket's stop LSN.
	logf("acquiring bracket WAL via source=%q", opts.WALSource)
	segments, err := AcquireBracketWAL(ctx, AcquireOptions{
		Backend:   opts.Backend,
		Timeline:  startInfo.Timeline,
		StartLSN:  startInfo.LSN,
		StopLSN:   stopInfo.LSN,
		SegSize:   id.WALSegmentSize,
		Timeout:   opts.WALTimeout,
		WALSource: opts.WALSource,
	})
	if err != nil {
		return Result{}, fmt.Errorf("backup: %w", err)
	}
	walRecords, err := hashWALSegments(ctx, opts.Backend, startInfo.Timeline, segments)
	if err != nil {
		return Result{}, fmt.Errorf("backup: hash WAL: %w", err)
	}

	// Build + commit manifest.
	manifestBytes, err := mb.Finalize(manifest.BackupStopInfo{
		StopLSN:  stopInfo.LSN,
		StopTime: opts.Now().UTC(),
	})
	if err != nil {
		return Result{}, fmt.Errorf("backup: finalize manifest: %w", err)
	}
	manifestRel := filepath.ToSlash(filepath.Join(backupID, "backup_manifest"))
	tmpRel := manifestRel + ".tmp"
	if err := writeStorageFileAtomic(ctx, opts.Backend, tmpRel, manifestBytes); err != nil {
		return Result{}, fmt.Errorf("backup: write manifest: %w", err)
	}

	sc := manifest.Sidecar{
		Version:              manifest.SidecarVersion,
		Server:               opts.Server,
		EncryptionRecipients: opts.Recipients,
		Compression:          opts.Compression,
		StorageLayoutVersion: 1,
		WALSegments:          walRecords,
		SystemIdentifier:     id.SystemIdentifier,
	}
	scBytes, err := manifest.MarshalSidecar(sc)
	if err != nil {
		return Result{}, fmt.Errorf("backup: marshal sidecar: %w", err)
	}
	if err := writeStorageFileAtomic(ctx, opts.Backend, filepath.ToSlash(filepath.Join(backupID, "Storage-Metadata.json")), scBytes); err != nil {
		return Result{}, fmt.Errorf("backup: write sidecar: %w", err)
	}
	if err := opts.Backend.Commit(ctx, tmpRel, manifestRel); err != nil {
		return Result{}, fmt.Errorf("backup: commit manifest: %w", err)
	}

	return Result{
		BackupID: backupID,
		StartLSN: startInfo.LSN,
		StopLSN:  stopInfo.LSN,
		Timeline: startInfo.Timeline,
		Files:    int(fileCount),
		Bytes:    bytesTotal,
		Duration: time.Since(startedAt),
	}, nil
}

// streamOneFile pulls one cluster file via readbinary, runs page-checksum
// validation, and writes through the filter chain into the storage.
func streamOneFile(
	ctx context.Context,
	rpOpts RemoteParallelOptions,
	f readbinary.FileEntry,
	opts Options,
	backupID string,
) (int64, [32]byte, time.Time, error) {
	// Reader from PG.
	rb := readbinary.NewReader(rpOpts.Pool, f.Path, f.Size, 0)
	defer func() { _ = rb.Close() }()

	// Wrap with page-checksum validator. Heap files (under base/, global/)
	// have PG page format; other files (config files, etc.) don't. We
	// only validate the heap files.
	var src io.Reader = rb
	if isHeapFile(f.Path) {
		src = pagechecksum.New(rb, rpOpts.PageChecksumMode, 0, f.Path)
	}

	// Reject malicious PG-side paths. ListPGData filters internally but
	// belt-and-braces here.
	if strings.Contains(f.Path, "..") || strings.HasPrefix(f.Path, "/") {
		return 0, [32]byte{}, time.Time{}, fmt.Errorf("traversal-style path %q rejected", f.Path)
	}

	repoPath := filepath.ToSlash(filepath.Join(backupID, f.Path))
	wc, err := opts.Backend.Put(ctx, repoPath)
	if err != nil {
		return 0, [32]byte{}, time.Time{}, fmt.Errorf("storage.Put: %w", err)
	}
	chainW, res, err := opts.Filter.Wrap(wc)
	if err != nil {
		return 0, [32]byte{}, time.Time{}, fmt.Errorf("filter.Wrap: %w", err)
	}
	if _, err := io.Copy(chainW, src); err != nil {
		_ = chainW.Close()
		return 0, [32]byte{}, time.Time{}, fmt.Errorf("copy: %w", err)
	}
	if err := chainW.Close(); err != nil {
		return 0, [32]byte{}, time.Time{}, fmt.Errorf("close: %w", err)
	}
	return res.Bytes, res.SHA256, time.Now().UTC(), nil
}

// writeBlobThroughChain feeds an in-memory byte slice through the filter
// chain into the storage and records it in the manifest. Used for backup_label
// and tablespace_map which come from bracket.Stop, not from $PGDATA.
func writeBlobThroughChain(
	ctx context.Context,
	opts Options,
	backupID, name string,
	body []byte,
	mb *manifest.Builder,
	mu *muLockable,
) error {
	repoPath := filepath.ToSlash(filepath.Join(backupID, name))
	wc, err := opts.Backend.Put(ctx, repoPath)
	if err != nil {
		return fmt.Errorf("backup: storage.Put %s: %w", name, err)
	}
	chainW, res, err := opts.Filter.Wrap(wc)
	if err != nil {
		return fmt.Errorf("backup: filter.Wrap %s: %w", name, err)
	}
	if _, err := chainW.Write(body); err != nil {
		_ = chainW.Close()
		return fmt.Errorf("backup: write %s: %w", name, err)
	}
	if err := chainW.Close(); err != nil {
		return fmt.Errorf("backup: close %s: %w", name, err)
	}
	mu.Lock()
	mb.AddFile(name, res.Bytes, res.SHA256, time.Now().UTC())
	mu.Unlock()
	return nil
}

func workersDefault(n int) int {
	if n > 0 {
		return n
	}
	w := runtime.NumCPU()
	if w > 8 {
		w = 8
	}
	if w < 1 {
		w = 1
	}
	return w
}

// isHeapFile heuristically decides whether a $PGDATA-relative path holds
// PG-formatted 8 KiB pages (heap, btree, fsm, vm, etc.). Everything under
// base/ and global/ qualifies; PG_VERSION, postgresql.conf, pg_hba.conf,
// pg_ident.conf, backup_label, etc. do not.
func isHeapFile(rel string) bool {
	return strings.HasPrefix(rel, "base/") ||
		strings.HasPrefix(rel, "global/") ||
		strings.HasPrefix(rel, "pg_tblspc/")
}

// stderrFor returns the io.Writer the caller should log to.
// Returns opts.Stderr when non-nil so integration tests can capture
// the topology log and assert against it; otherwise os.Stderr so
// real cron-driven backups surface logs the operator expects.
//
// runSimple writes to os.Stderr; we mirror that. Tests can swap by passing
// opts.Logger (work).
func stderrFor(opts Options) io.Writer {
	if opts.Stderr != nil {
		return opts.Stderr
	}
	return os.Stderr
}

// muLockable wraps sync.Mutex so the caller can pass it by pointer
// without exposing the embedded type. may swap for a per-builder
// channel-based protocol if contention shows up; for the file-list
// fan-out makes ~1000 short critical sections so a mutex is fine.
type muLockable struct {
	mu sync.Mutex
}

func (m *muLockable) Lock()   { m.mu.Lock() }
func (m *muLockable) Unlock() { m.mu.Unlock() }
