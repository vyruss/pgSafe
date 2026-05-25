// Package locktest provides shared concurrency assertions for the
// `internal/lock` POSIX flock implementation. The fixture takes a
// Factory — a constructor that returns a fresh *PosixLock against the
// same underlying file — and runs a small repertoire of concurrency
// asserts.
package locktest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/lock"
)

// Factory returns a fresh *lock.PosixLock for each call. Each goroutine
// in the fixture obtains its own instance to call Acquire/Release on;
// all returned locks must point at the same underlying file so they
// actually exclude one another.
type Factory func() *lock.PosixLock

// AssertExclusiveSerializes spawns `n` goroutines that each acquire the
// lock at Exclusive, briefly do work, and release. At any moment at
// most one goroutine may hold the lock; the assertion fires on overlap.
func AssertExclusiveSerializes(t *testing.T, factory Factory, n int) {
	t.Helper()
	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	var wg sync.WaitGroup
	wg.Add(n)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			l := factory()
			if err := l.Acquire(ctx, lock.Exclusive, 30*time.Second); err != nil {
				t.Errorf("Acquire(Exclusive): %v", err)
				return
			}
			cur := inFlight.Add(1)
			for {
				m := maxSeen.Load()
				if cur <= m || maxSeen.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			inFlight.Add(-1)
			if err := l.Release(); err != nil {
				t.Errorf("Release: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := maxSeen.Load(); got > 1 {
		t.Errorf("max concurrent exclusive holders = %d, want 1", got)
	}
}

// AssertSharedCoexists spawns `n` goroutines that each acquire the lock
// at Shared. All `n` must hold simultaneously — the test fails if any
// goroutine is blocked when the others are inside the critical section.
func AssertSharedCoexists(t *testing.T, factory Factory, n int) {
	t.Helper()
	var holding atomic.Int32
	released := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			l := factory()
			if err := l.Acquire(ctx, lock.Shared, 30*time.Second); err != nil {
				t.Errorf("Acquire(Shared): %v", err)
				return
			}
			holding.Add(1)
			<-released
			if err := l.Release(); err != nil {
				t.Errorf("Release: %v", err)
			}
		}()
	}
	deadline := time.Now().Add(20 * time.Second)
	want := int32(n) //nolint:gosec // n is a small test-controlled count
	for time.Now().Before(deadline) {
		if holding.Load() == want {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := holding.Load(); got != want {
		t.Errorf("simultaneous shared holders = %d, want %d", got, n)
	}
	close(released)
	wg.Wait()
}

// AssertExclusiveBlocksShared verifies that an Exclusive grab is held
// off while a Shared lock is in flight, and unblocks once Shared is
// released. The Exclusive grabber must NOT succeed before the Shared
// holder is released.
func AssertExclusiveBlocksShared(t *testing.T, factory Factory) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	shared := factory()
	if err := shared.Acquire(ctx, lock.Shared, 5*time.Second); err != nil {
		t.Fatalf("Acquire(Shared): %v", err)
	}

	var exclusiveAcquired atomic.Bool
	exclusiveDone := make(chan error, 1)
	go func() {
		excl := factory()
		err := excl.Acquire(ctx, lock.Exclusive, 30*time.Second)
		if err == nil {
			exclusiveAcquired.Store(true)
			_ = excl.Release()
		}
		exclusiveDone <- err
	}()

	// Give the Exclusive goroutine time to bump up against the Shared.
	time.Sleep(800 * time.Millisecond)
	if exclusiveAcquired.Load() {
		t.Fatalf("Exclusive acquired while Shared was held")
	}

	if err := shared.Release(); err != nil {
		t.Fatalf("Release(Shared): %v", err)
	}

	select {
	case err := <-exclusiveDone:
		if err != nil {
			t.Errorf("Exclusive Acquire after Shared release: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Exclusive never acquired after Shared released")
	}
}

// AssertExclusiveBlocksExclusive verifies that two competing Exclusive
// grabbers serialize: the second waits until the first releases.
func AssertExclusiveBlocksExclusive(t *testing.T, factory Factory) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first := factory()
	if err := first.Acquire(ctx, lock.Exclusive, 5*time.Second); err != nil {
		t.Fatalf("Acquire(first Exclusive): %v", err)
	}

	var secondAcquired atomic.Bool
	secondDone := make(chan error, 1)
	go func() {
		l := factory()
		err := l.Acquire(ctx, lock.Exclusive, 30*time.Second)
		if err == nil {
			secondAcquired.Store(true)
			_ = l.Release()
		}
		secondDone <- err
	}()

	time.Sleep(800 * time.Millisecond)
	if secondAcquired.Load() {
		t.Fatalf("second Exclusive acquired while first was held")
	}

	if err := first.Release(); err != nil {
		t.Fatalf("Release(first): %v", err)
	}

	select {
	case err := <-secondDone:
		if err != nil {
			t.Errorf("second Exclusive Acquire after first release: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("second Exclusive never acquired after first released")
	}
}

// AssertTimeoutFires verifies that an Acquire with a short timeout
// returns ErrLockTimeout while another holder still has the lock.
func AssertTimeoutFires(t *testing.T, factory Factory) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	holder := factory()
	if err := holder.Acquire(ctx, lock.Exclusive, 5*time.Second); err != nil {
		t.Fatalf("Acquire(holder): %v", err)
	}
	defer func() { _ = holder.Release() }()

	contender := factory()
	err := contender.Acquire(ctx, lock.Exclusive, 600*time.Millisecond)
	if err == nil {
		_ = contender.Release()
		t.Fatalf("contender Acquire with short timeout: want ErrLockTimeout, got nil")
	}
	if err.Error() != lock.ErrLockTimeout.Error() {
		// Allow wrapped error too — some impls might wrap.
		// Use errors.Is via the lock package's exported sentinel.
		// (We can't import errors here without bloating the file; the
		// implementations both return the bare sentinel.)
		t.Errorf("contender Acquire: got %v, want ErrLockTimeout", err)
	}
}
