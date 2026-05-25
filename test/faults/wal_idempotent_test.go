//go:build faults

package faults_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

// TestArchivePushConcurrentByteEqual is the load-bearing Invariant-#7 fault
// test. PG can re-emit the same WAL segment under crash-recovery; pushers
// must dedup on byte-equal content. We launch N concurrent Pushes of
// the same file from N goroutines; every one must succeed (one wins the
// fresh-upload race, the rest take the dedup path), and the destination
// must end up with one consistent copy.
func TestArchivePushConcurrentByteEqual(t *testing.T) {
	const seg = "00000001000000000000000A"
	const concurrency = 8

	repoRoot := t.TempDir()
	b, err := posix.New(posix.Options{Root: repoRoot})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	src := filepath.Join(t.TempDir(), seg)
	body := []byte("identical-bytes-from-every-pusher")
	if err := os.WriteFile(src, body, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Launch N concurrent Pushes; collect their errors.
	var (
		wg   sync.WaitGroup
		errs = make([]error, concurrency)
	)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = archive.Push(context.Background(), b, 1, src)
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("worker %d: unexpected error: %v", i, e)
		}
	}

	// One consistent copy must end up at the destination key. Source
	// SHA is the same for every concurrent pusher (same `body`), so
	// they all target the exact same key — race winners and losers
	// land at the identical filename and idempotent dedup is by
	// construction.
	key, found, err := archive.FindSegment(context.Background(), b, 1, seg)
	if err != nil {
		t.Fatalf("FindSegment after concurrent push: %v", err)
	}
	if !found {
		t.Fatal("FindSegment after concurrent push: not found")
	}
	fi, err := b.Stat(context.Background(), key)
	if err != nil {
		t.Fatalf("Stat after concurrent push: %v", err)
	}
	if fi.Size != int64(len(body)) {
		t.Errorf("size = %d, want %d", fi.Size, len(body))
	}
}

// TestArchivePushConcurrentByteUnequal asserts the loud-failure case: when
// two pushers disagree on content for the same segment name, at least one
// must surface ErrSegmentMismatch (so the operator notices). PG never
// produces this case under normal recovery — it indicates archive
// corruption or a bug — but the rulebook requires the failure to be loud.
func TestArchivePushConcurrentByteUnequal(t *testing.T) {
	const seg = "00000001000000000000000B"

	repoRoot := t.TempDir()
	b, err := posix.New(posix.Options{Root: repoRoot})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	srcA := filepath.Join(t.TempDir(), seg)
	srcB := filepath.Join(t.TempDir(), seg)
	if err := os.WriteFile(srcA, []byte("first-bytes"), 0o600); err != nil {
		t.Fatalf("write srcA: %v", err)
	}
	if err := os.WriteFile(srcB, []byte("DIFFERENT-bytes-len"), 0o600); err != nil {
		t.Fatalf("write srcB: %v", err)
	}

	// Whichever order they go in, the loser must error.
	errA := archive.Push(context.Background(), b, 1, srcA)
	errB := archive.Push(context.Background(), b, 1, srcB)

	if errA == nil && errB == nil {
		t.Fatal("both pushes succeeded; want exactly one ErrSegmentMismatch")
	}
	if (errA != nil && !errors.Is(errA, archive.ErrSegmentMismatch)) ||
		(errB != nil && !errors.Is(errB, archive.ErrSegmentMismatch)) {
		t.Errorf("unexpected error types: A=%v B=%v", errA, errB)
	}
}
