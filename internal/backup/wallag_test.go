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

const segSize16M = 16 * 1024 * 1024

func TestWALSegmentName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		tli  uint32
		lsn  manifest.LSN
		want string
	}{
		// Cases pinned against PG's pg_walfile_name(LSN) on a 16-MiB-segment
		// cluster. Each LSN is the canonical "high32 << 32 | low32"; the
		// expected name is "<TLI><high32-hex><(low32 / segSize)-hex>"
		// per PG's xlog_internal.h:XLogFileName.
		{1, manifest.LSN(0x3000028), "000000010000000000000003"}, // hi=0, lo/seg=3
		{1, manifest.LSN(0x4000000), "000000010000000000000004"}, // boundary, lo/seg=4
		{2, manifest.LSN(0x3000028), "000000020000000000000003"}, // tli=2
		// LSN B38/F70002F0 — taken from a real cross-host backup-id where
		// the old buggy splitter produced "0000000100000000000B38F7"; the
		// canonical name is "0000000100000B38000000F7".
		{1, manifest.LSN(0xB38_F70002F0), "0000000100000B38000000F7"},
		// LSN with the high-32 bits non-zero AND lo/seg != 0:
		//   high = 0x01000000, lo = 0x01000000 → lo/seg = 1.
		{1, manifest.LSN(uint64(segSize16M) * uint64(0x100000001)), "000000010100000000000001"},
	}
	for _, c := range cases {
		got := backup.WALSegmentName(c.tli, c.lsn, segSize16M)
		if got != c.want {
			t.Errorf("WALSegmentName(tli=%d, lsn=%#x) = %q, want %q",
				c.tli, uint64(c.lsn), got, c.want)
		}
	}
}

func TestWALSegmentsBetweenSameSegment(t *testing.T) {
	t.Parallel()
	got := backup.WALSegmentsBetween(1, manifest.LSN(0x3000028), manifest.LSN(0x3000100), segSize16M)
	want := []string{"000000010000000000000003"}
	if !strings.EqualFold(strings.Join(got, ","), strings.Join(want, ",")) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestWALSegmentsBetweenSpansTwo(t *testing.T) {
	t.Parallel()
	got := backup.WALSegmentsBetween(1, manifest.LSN(0x3FFFFFE), manifest.LSN(0x4000010), segSize16M)
	want := []string{
		"000000010000000000000003",
		"000000010000000000000004",
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Stop at exact segment boundary: pg_switch_wal returns the start of the new
// (empty) segment. That segment has no backup-relevant data and won't be
// archived. The last segment we should wait for is the previous one.
func TestWALSegmentsBetweenStopAtSegmentBoundary(t *testing.T) {
	t.Parallel()
	got := backup.WALSegmentsBetween(1, manifest.LSN(0x3000028), manifest.LSN(0x4000000), segSize16M)
	want := []string{"000000010000000000000003"}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("stop at exact boundary: got %v, want %v", got, want)
	}
}

// newPosixBackend opens a fresh POSIX backend rooted at a temp dir
// for the WAL-wait tests. Returns a closure that pre-populates a fake
// segment at the canonical archive.SegmentKey location (with a fake
// SHA suffix derived from the body) so the tests can stage segments
// without going through the real archive.Push code.
func newPosixBackend(t *testing.T) (*posix.Backend, func(timeline uint32, name string)) {
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
			t.Fatalf("mkdir for %s: %v", key, err)
		}
		if err := os.WriteFile(full, body, 0o600); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}
	return b, stage
}

func TestWaitForWALSucceedsWhenAllPresent(t *testing.T) {
	t.Parallel()
	b, stage := newPosixBackend(t)
	for _, name := range []string{"AAA", "BBB", "CCC"} {
		stage(1, name)
	}
	if err := backup.WaitForWAL(context.Background(), b, 1, []string{"AAA", "BBB"}, 100*time.Millisecond); err != nil {
		t.Errorf("expected nil; got %v", err)
	}
}

func TestWaitForWALTimesOut(t *testing.T) {
	t.Parallel()
	b, _ := newPosixBackend(t)
	err := backup.WaitForWAL(context.Background(), b, 1, []string{"NEVER"}, 50*time.Millisecond)
	if err == nil {
		t.Fatal("want timeout error; got nil")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("error %q should mention timeout", err)
	}
}

func TestWaitForWALEventuallyArrives(t *testing.T) {
	t.Parallel()
	b, stage := newPosixBackend(t)
	go func() {
		time.Sleep(80 * time.Millisecond)
		stage(1, "LATE")
	}()
	if err := backup.WaitForWAL(context.Background(), b, 1, []string{"LATE"}, 2*time.Second); err != nil {
		t.Errorf("expected nil after wait; got %v", err)
	}
}

func TestWaitForWALCancelled(t *testing.T) {
	t.Parallel()
	b, _ := newPosixBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := backup.WaitForWAL(ctx, b, 1, []string{"NEVER"}, 5*time.Second)
	if err == nil {
		t.Fatal("want context error; got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled; got %v", err)
	}
}

func TestWaitForWALEmptySegments(t *testing.T) {
	t.Parallel()
	b, _ := newPosixBackend(t)
	if err := backup.WaitForWAL(context.Background(), b, 1, nil, time.Millisecond); err != nil {
		t.Errorf("expected nil for no segments; got %v", err)
	}
}
