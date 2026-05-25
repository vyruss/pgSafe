package backup_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

// TestAcquireBracketWALDefaultIsArchive pins the default behavior:
// an empty WALSource is treated as "archive" — the existing
// archive_command + WAL-wait flow. Bumping the default in either
// direction is a wire-protocol-grade decision, so this test will
// catch a quiet refactor that changes it.
func TestAcquireBracketWALDefaultIsArchive(t *testing.T) {
	t.Parallel()
	b, stage := newWALSourcePosix(t)
	stage(1, "000000010000000000000003")
	got, err := backup.AcquireBracketWAL(context.Background(), backup.AcquireOptions{
		Backend:   b,
		Timeline:  1,
		StartLSN:  manifest.LSN(0x3000028),
		StopLSN:   manifest.LSN(0x3000100),
		SegSize:   16 * 1024 * 1024,
		Timeout:   100 * time.Millisecond,
		WALSource: "",
	})
	if err != nil {
		t.Fatalf("default WALSource: %v", err)
	}
	if len(got) != 1 || got[0] != "000000010000000000000003" {
		t.Errorf("segments = %v, want one bracket segment", got)
	}
}

func TestAcquireBracketWALArchiveExplicit(t *testing.T) {
	t.Parallel()
	b, stage := newWALSourcePosix(t)
	stage(1, "000000010000000000000003")
	_, err := backup.AcquireBracketWAL(context.Background(), backup.AcquireOptions{
		Backend:   b,
		Timeline:  1,
		StartLSN:  manifest.LSN(0x3000028),
		StopLSN:   manifest.LSN(0x3000100),
		SegSize:   16 * 1024 * 1024,
		Timeout:   100 * time.Millisecond,
		WALSource: backup.WALSourceArchive,
	})
	if err != nil {
		t.Errorf("WALSourceArchive: %v", err)
	}
}

// TestAcquireBracketWALStreamSkipsPolling pins the stream-source
// contract: WAL is already inside the backup tar via pg_basebackup
// --wal-method=fetch, so AcquireBracketWAL must NOT poll the archive.
// The Timeout is set tight (1ms) to make polling-by-mistake fail loud.
func TestAcquireBracketWALStreamSkipsPolling(t *testing.T) {
	t.Parallel()
	b, _ := newWALSourcePosix(t)
	got, err := backup.AcquireBracketWAL(context.Background(), backup.AcquireOptions{
		Backend:   b,
		Timeline:  1,
		StartLSN:  manifest.LSN(0x3000028),
		StopLSN:   manifest.LSN(0x3000100),
		SegSize:   16 * 1024 * 1024,
		Timeout:   1 * time.Millisecond,
		WALSource: backup.WALSourceStream,
	})
	if err != nil {
		t.Fatalf("WALSourceStream: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected one bracket segment; got %v", got)
	}
}

// TestAcquireBracketWALWalgrabSkipsPolling pins the walgrab contract:
// the worker will fetch bracket segments directly from $PGDATA/pg_wal
// post-stop, so AcquireBracketWAL must NOT poll the archive. Tight
// timeout (1ms) makes accidental polling fail loud.
func TestAcquireBracketWALWalgrabSkipsPolling(t *testing.T) {
	t.Parallel()
	b, _ := newWALSourcePosix(t)
	got, err := backup.AcquireBracketWAL(context.Background(), backup.AcquireOptions{
		Backend:   b,
		Timeline:  1,
		StartLSN:  manifest.LSN(0x3000028),
		StopLSN:   manifest.LSN(0x3000100),
		SegSize:   16 * 1024 * 1024,
		Timeout:   1 * time.Millisecond,
		WALSource: backup.WALSourceWalgrab,
	})
	if err != nil {
		t.Fatalf("WALSourceWalgrab: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("expected one bracket segment; got %v", got)
	}
}

func TestAcquireBracketWALUnknownSource(t *testing.T) {
	t.Parallel()
	b, _ := newWALSourcePosix(t)
	_, err := backup.AcquireBracketWAL(context.Background(), backup.AcquireOptions{
		Backend:   b,
		Timeline:  1,
		StartLSN:  manifest.LSN(0x3000028),
		StopLSN:   manifest.LSN(0x3000100),
		SegSize:   16 * 1024 * 1024,
		Timeout:   100 * time.Millisecond,
		WALSource: "bogus",
	})
	if err == nil || !strings.Contains(err.Error(), "unknown WAL source") {
		t.Errorf("bogus WALSource: want 'unknown WAL source'; got %v", err)
	}
}

// TestAcquireBracketWALArchiveTimeout asserts the archive path still
// surfaces ErrWALWait for missing segments — same behavior as the
// pre-refactor WaitForWAL callsites.
func TestAcquireBracketWALArchiveTimeout(t *testing.T) {
	t.Parallel()
	b, _ := newWALSourcePosix(t)
	_, err := backup.AcquireBracketWAL(context.Background(), backup.AcquireOptions{
		Backend:   b,
		Timeline:  1,
		StartLSN:  manifest.LSN(0x3000028),
		StopLSN:   manifest.LSN(0x3000100),
		SegSize:   16 * 1024 * 1024,
		Timeout:   30 * time.Millisecond,
		WALSource: backup.WALSourceArchive,
	})
	if !errors.Is(err, backup.ErrWALWait) {
		t.Errorf("want ErrWALWait; got %v", err)
	}
}

func newWALSourcePosix(t *testing.T) (*posix.Backend, func(timeline uint32, name string)) {
	t.Helper()
	dir := t.TempDir()
	b, err := posix.New(posix.Options{Root: dir})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	stage := func(timeline uint32, name string) {
		body := []byte("x")
		hash := sha256.Sum256(body)
		key := archive.SegmentKey(timeline, name, hash)
		full := filepath.Join(dir, key)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, body, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	return b, stage
}
