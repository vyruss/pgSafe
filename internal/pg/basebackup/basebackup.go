// Package basebackup is the simple-mode BASE_BACKUP client. shells
// out to PostgreSQL's own pg_basebackup binary with --format=tar --pgdata=-
// and exposes the resulting tar stream entry-by-entry to the caller.
//
// Why shell out instead of speaking the BASE_BACKUP replication-protocol
// natively (as  implies): writing a
// correct in-process BASE_BACKUP client is a multi-week effort that buys
// nothing for milestone. pg_basebackup is the reference
// implementation, ships with PG, and gives us exactly the tar stream the
// plan calls for.
// use the replication protocol at all — they read $PGDATA via SQL functions
// or a worker on the PG host. So the protocol-level client is *only* needed
// for simple mode and may stay as a shell-out indefinitely. Documented in
// ARCHITECTURE.md.
//
// Operator prerequisite: pg_basebackup binary on the backup host's PATH.
// Skip-able in tests via PGSAFE_SKIP_BASEBACKUP=1 when not available.
package basebackup

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
)

// Options configure one BASE_BACKUP invocation.
type Options struct {
	// DSN is the libpq connection string (must authenticate as a user with
	// REPLICATION attribute).
	DSN string

	// Label is the human-readable backup label written into backup_label.
	// Defaults to "pgsafe" if empty.
	Label string

	// IncrementalManifestPath, if non-empty, makes pg_basebackup produce an
	// incremental backup against the backup_manifest at this path. Requires
	// PG 17+ on the source cluster. The caller stages
	// the parent backup's backup_manifest into a temp file and passes its
	// path here.
	IncrementalManifestPath string

	// WALMethod selects pg_basebackup's --wal-method= argument. Empty
	// or "none" produces a backup without WAL — the caller is then
	// responsible for ensuring bracket WAL is available some other way
	// (typically by polling the archive). "fetch" packs the bracket
	// WAL into the data tar's pg_wal/ directory; the result is a
	// self-contained backup that needs no external archive to
	// restore. "stream" is rejected by pg_basebackup in
	// --pgdata=-/--format=tar mode (PG would need a second stdout)
	// and so is unsupported here.
	WALMethod string
}

// Stream is one in-flight BASE_BACKUP. Iterate entries with Next() until
// io.EOF, then call Close to surface any subprocess error.
type Stream struct {
	cmd       *exec.Cmd
	stdout    io.ReadCloser
	stderrBuf *captureBuf
	tar       *tar.Reader

	closeOnce sync.Once
	closeErr  error
}

// captureBuf is a thread-safe StringBuilder for stderr capture.
type captureBuf struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (c *captureBuf) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *captureBuf) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// buildArgs assembles the pg_basebackup CLI arguments for opts. Split
// out for unit-testing — the args list is the contract between pgsafe
// and pg_basebackup, so we pin it explicitly.
//
// pg_basebackup constraint (verified against PG source: pg_basebackup.c
// near "cannot stream write-ahead logs in tar mode to stdout"): with
// --pgdata=- and --format=tar, --wal-method=stream is rejected because
// the streamed WAL would need a second stdout. Only "none" and "fetch"
// work in the pgsafe pipeline. "fetch" packs the bracket WAL into the
// data tar's pg_wal/ entries, yielding a self-contained backup that
// restores without an external archive. "none" leaves WAL out — caller
// must then ensure the bracket segments are available some other way
// (typically by polling the archive).
func buildArgs(opts Options) ([]string, error) {
	method := opts.WALMethod
	if method == "" {
		method = "none"
	}
	switch method {
	case "none", "fetch":
	case "stream":
		return nil, errors.New("basebackup: --wal-method=stream is not supported with --pgdata=-/--format=tar (pg_basebackup limitation); use \"fetch\" for inline WAL or \"none\" for archive-tied")
	default:
		return nil, fmt.Errorf("basebackup: unknown WAL method %q", method)
	}
	args := []string{
		"-d", opts.DSN,
		"--pgdata=-",
		"--format=tar",
		"--wal-method=" + method,
		"--checkpoint=fast",
		"--label=" + opts.Label,
	}
	if opts.IncrementalManifestPath != "" {
		// In incremental mode pg_basebackup's own manifest is
		// canonical for pg_combinebackup — we cannot match its
		// format from outside, so we keep it. The caller captures the
		// backup_manifest tar entry plaintext and uses it verbatim.
		args = append(args, "--incremental="+opts.IncrementalManifestPath)
	} else {
		// For full backups we still hand-roll our own manifest because
		// the filter chain has already computed plaintext SHA-256 for
		// every file.
		args = append(args, "--no-manifest")
	}
	return args, nil
}

// Start launches pg_basebackup as a subprocess and returns a tar Stream.
// The subprocess runs until either Close is called or it exits naturally.
func Start(ctx context.Context, opts Options) (*Stream, error) {
	if opts.DSN == "" {
		return nil, errors.New("basebackup: DSN is required")
	}
	if opts.Label == "" {
		opts.Label = "pgsafe"
	}
	if _, err := exec.LookPath("pg_basebackup"); err != nil {
		return nil, fmt.Errorf("basebackup: pg_basebackup not found on PATH: %w", err)
	}

	args, err := buildArgs(opts)
	if err != nil {
		return nil, err
	}
	// args is locally constructed; the only operator-supplied input is the DSN,
	// which pg_basebackup parses as a single libpq URI. No shell interpretation.
	cmd := exec.CommandContext(ctx, "pg_basebackup", args...) //nolint:gosec

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("basebackup: stdout pipe: %w", err)
	}
	stderr := &captureBuf{}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, fmt.Errorf("basebackup: start: %w", err)
	}

	return &Stream{
		cmd:       cmd,
		stdout:    stdout,
		stderrBuf: stderr,
		tar:       tar.NewReader(stdout),
	}, nil
}

// Next advances to the next file in the tar stream. The returned io.Reader
// is valid until the next call to Next() or Close(). Returns io.EOF when
// the stream is exhausted.
func (s *Stream) Next() (*tar.Header, io.Reader, error) {
	h, err := s.tar.Next()
	if err != nil {
		return nil, nil, err
	}
	return h, s.tar, nil
}

// Close drains the rest of stdout (so the subprocess doesn't deadlock on a
// full pipe), then waits for pg_basebackup to exit. Returns the exit error
// if any, with stderr appended for diagnostics.
func (s *Stream) Close() error {
	s.closeOnce.Do(func() {
		// Drain remaining stdout to avoid SIGPIPE on the subprocess.
		_, _ = io.Copy(io.Discard, s.stdout)
		_ = s.stdout.Close()
		err := s.cmd.Wait()
		if err != nil {
			s.closeErr = fmt.Errorf("basebackup: pg_basebackup exit: %w; stderr:\n%s",
				err, s.stderrBuf.String())
		}
	})
	return s.closeErr
}

// Stderr returns the captured stderr output (useful for diagnostics during
// a long-running backup).
func (s *Stream) Stderr() string {
	return s.stderrBuf.String()
}
