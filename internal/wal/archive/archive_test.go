package archive_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

const seg1 = "000000010000000000000005"

// fakeWAL writes a 16 KiB file resembling a WAL segment (in size only) at
// the given path. Returns the path. A real PG WAL segment is 16 MiB; the
// archive logic doesn't care about contents or size.
// fakeWAL writes a small file at the given path. Real WAL segments are
// 16 MiB, but archive Push/Get only care about path naming and bytes; tests
// stay fast by using small bodies. The `name` parameter is here to support
// tests that need multiple segments (e.g. concurrent push tests in
// test/faults pass different names).
//
//nolint:unparam // name varies in test/faults; lint sees only this package
func fakeWAL(t *testing.T, dir, name string, body []byte) string {
	t.Helper()
	full := filepath.Join(dir, name)
	if err := os.WriteFile(full, body, 0o600); err != nil {
		t.Fatalf("write %s: %v", full, err)
	}
	return full
}

func newPOSIXBackend(t *testing.T) *posix.Backend {
	t.Helper()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return b
}

func TestPushFreshSegment(t *testing.T) {
	t.Parallel()
	b := newPOSIXBackend(t)
	body := []byte("hello-wal")
	src := fakeWAL(t, t.TempDir(), seg1, body)

	if err := archive.Push(context.Background(), b, 1, src); err != nil {
		t.Fatalf("Push: %v", err)
	}

	key, found, err := archive.FindSegment(context.Background(), b, 1, seg1)
	if err != nil {
		t.Fatalf("FindSegment: %v", err)
	}
	if !found {
		t.Fatal("FindSegment after Push: not found")
	}
	fi, err := b.Stat(context.Background(), key)
	if err != nil {
		t.Fatalf("Stat after Push: %v", err)
	}
	if fi.Size != int64(len(body)) {
		t.Errorf("uploaded size = %d, want %d", fi.Size, len(body))
	}
}

func TestPushIdempotentOnByteEqual(t *testing.T) {
	t.Parallel()
	b := newPOSIXBackend(t)
	src := fakeWAL(t, t.TempDir(), seg1, []byte("idempotent"))

	if err := archive.Push(context.Background(), b, 1, src); err != nil {
		t.Fatalf("first Push: %v", err)
	}
	// Second push of the same bytes is a no-op success.
	if err := archive.Push(context.Background(), b, 1, src); err != nil {
		t.Errorf("second Push (byte-equal): want nil, got %v", err)
	}
}

func TestPushRejectsByteUnequalRetry(t *testing.T) {
	t.Parallel()
	b := newPOSIXBackend(t)
	dir := t.TempDir()
	src := fakeWAL(t, dir, seg1, []byte("first"))

	if err := archive.Push(context.Background(), b, 1, src); err != nil {
		t.Fatalf("first Push: %v", err)
	}
	// Rewrite the source with different bytes, same name.
	if err := os.WriteFile(src, []byte("second"), 0o600); err != nil {
		t.Fatalf("rewrite source: %v", err)
	}
	err := archive.Push(context.Background(), b, 1, src)
	if !errors.Is(err, archive.ErrSegmentMismatch) {
		t.Errorf("Push of different bytes: want ErrSegmentMismatch, got %v", err)
	}
}

func TestGetRoundTrip(t *testing.T) {
	t.Parallel()
	b := newPOSIXBackend(t)
	body := []byte("the quick brown fox\n")
	src := fakeWAL(t, t.TempDir(), seg1, body)
	if err := archive.Push(context.Background(), b, 1, src); err != nil {
		t.Fatalf("Push: %v", err)
	}

	dst := filepath.Join(t.TempDir(), "fetched", seg1)
	if err := archive.Get(context.Background(), b, 1, seg1, dst); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read fetched: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("round-trip body = %q, want %q", got, body)
	}
}

func TestSegmentKeyAndTimelineParse(t *testing.T) {
	t.Parallel()
	var hash [32]byte
	for i := range hash {
		hash[i] = byte(i)
	}
	got := archive.SegmentKey(1, seg1, hash)
	want := "wal/00000001/" + seg1 + "-000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	if got != want {
		t.Errorf("SegmentKey = %q, want %q", got, want)
	}
	if prefix := archive.SegmentKeyPrefix(1, seg1); prefix != "wal/00000001/"+seg1+"-" {
		t.Errorf("SegmentKeyPrefix = %q", prefix)
	}
	tli, err := archive.TimelineFromSegment(seg1)
	if err != nil {
		t.Fatalf("TimelineFromSegment: %v", err)
	}
	if tli != 1 {
		t.Errorf("timeline = %d, want 1", tli)
	}
}

func TestTimelineFromSegmentRejectsGarbage(t *testing.T) {
	t.Parallel()
	if _, err := archive.TimelineFromSegment("nope"); err == nil {
		t.Fatal("TimelineFromSegment(\"nope\"): want error")
	}
	if _, err := archive.TimelineFromSegment("ZZZZZZZZZZZZZZZZZZZZZZZZ"); err == nil {
		t.Fatal("TimelineFromSegment(non-hex): want error")
	}
}

// TestIsArchivableFile pins the four file shapes PG hands archive_command
// via %p. Earlier versions of pgsafe rejected everything but bare 24-hex
// segments — so the first time pg_backup_stop produced the .backup
// marker, archive_command kept failing and PG's archiver hung in retry.
// Test cases derived from PG documentation + an actual pg_wal/ listing
// captured during a real-cluster backup.
func TestIsArchivableFile(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		// Bare segment (24 hex) — what PG ships during normal write.
		{"000000010000000000000003", true},
		{"0000000100000B38000000F7", true},
		// .partial — end-of-recovery partial segment.
		{"0000000100000B38000000F7.partial", true},
		// .backup — pg_backup_stop label marker.
		// Layout: <24-hex-segment>.<8-hex-offset>.backup
		{"0000000100000B38000000E1.00000028.backup", true},
		// .history — timeline history file.
		{"00000001.history", true},
		{"00000003.history", true},
		// Garbage / wrong forms.
		{"", false},
		{"nope", false},
		{"00000001000000000000000", false},                      // 23 hex
		{"000000010000000000000003abc", false},                  // 27 chars
		{"000000010000000000000003.partial.bogus", false},       // wrong suffix
		{"ZZZZZZZZZZZZZZZZZZZZZZZZ", false},                     // non-hex
		{"00000001.history.bak", false},                         // wrong suffix
		{"0000000100000B38000000E1.SHRTOFFSET.backup", false},   // .backup with non-8-hex offset
		{"0000000100000B38000000E1.00000028.backup.tmp", false}, // would clash with our .tmp
	}
	for _, c := range cases {
		got := archive.IsArchivableFile(c.name)
		if got != c.want {
			t.Errorf("IsArchivableFile(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestPushAllArchiveForms drives Push for each of the four file
// shapes PG hands archive_command via %p (segment, .partial,
// .backup, .history). Earlier versions rejected everything but bare
// 24-hex segments — so the first time pg_backup_stop produced the
// .backup marker, archive_command kept failing forever. This test
// would have caught that regression at the same layer the bug
// surfaced (archive-push CLI shells out to archive.Push).
func TestPushAllArchiveForms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
	}{
		{"000000010000000000000005"},                 // bare segment
		{"000000010000000000000005.partial"},         // partial
		{"000000010000000000000005.00000028.backup"}, // backup-stop marker
		{"00000003.history"},                         // timeline history
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			b := newPOSIXBackend(t)
			body := []byte("payload-for-" + c.name)
			src := fakeWAL(t, t.TempDir(), c.name, body)

			tli, err := archive.TimelineFromSegment(c.name)
			if err != nil {
				t.Fatalf("TimelineFromSegment(%q): %v", c.name, err)
			}
			if err := archive.Push(context.Background(), b, tli, src); err != nil {
				t.Fatalf("Push(%q): %v", c.name, err)
			}
			key, found, err := archive.FindSegment(context.Background(), b, tli, c.name)
			if err != nil {
				t.Fatalf("FindSegment(%q): %v", c.name, err)
			}
			if !found {
				t.Fatalf("FindSegment(%q): not found in storage after Push", c.name)
			}
			fi, err := b.Stat(context.Background(), key)
			if err != nil {
				t.Fatalf("Stat(%q): %v", key, err)
			}
			if fi.Size != int64(len(body)) {
				t.Errorf("uploaded size = %d, want %d", fi.Size, len(body))
			}
		})
	}
}

// TestTimelineFromSegmentAcceptsAllArchiveForms verifies that the
// timeline parser doesn't reject .partial / .backup / .history files —
// the timeline ID is the leading 8 hex chars in all four forms.
func TestTimelineFromSegmentAcceptsAllArchiveForms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		wantTLI uint32
	}{
		{"000000010000000000000003", 1},
		{"0000000100000B38000000F7.partial", 1},
		{"0000000100000B38000000E1.00000028.backup", 1},
		{"00000003.history", 3},
		{"FFFFFFFF.history", 0xFFFFFFFF},
	}
	for _, c := range cases {
		got, err := archive.TimelineFromSegment(c.name)
		if err != nil {
			t.Errorf("TimelineFromSegment(%q): unexpected error %v", c.name, err)
			continue
		}
		if got != c.wantTLI {
			t.Errorf("TimelineFromSegment(%q) = %d, want %d", c.name, got, c.wantTLI)
		}
	}
}
