//go:build unix

package lock_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/lock"
	"github.com/vyruss/pgsafe/internal/lock/locktest"
)

func newPosixFactory(t *testing.T) locktest.Factory {
	t.Helper()
	path := filepath.Join(t.TempDir(), "server.lock")
	return func() *lock.PosixLock { return lock.NewPosix(path) }
}

func TestPosixExclusiveSerializes(t *testing.T) {
	t.Parallel()
	locktest.AssertExclusiveSerializes(t, newPosixFactory(t), 8)
}

func TestPosixSharedCoexists(t *testing.T) {
	t.Parallel()
	locktest.AssertSharedCoexists(t, newPosixFactory(t), 6)
}

func TestPosixExclusiveBlocksShared(t *testing.T) {
	t.Parallel()
	locktest.AssertExclusiveBlocksShared(t, newPosixFactory(t))
}

func TestPosixExclusiveBlocksExclusive(t *testing.T) {
	t.Parallel()
	locktest.AssertExclusiveBlocksExclusive(t, newPosixFactory(t))
}

func TestPosixTimeoutFires(t *testing.T) {
	t.Parallel()
	locktest.AssertTimeoutFires(t, newPosixFactory(t))
}

func TestPosixReleaseIdempotent(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "server.lock")
	l := lock.NewPosix(path)
	if err := l.Acquire(context.Background(), lock.Exclusive, 5*time.Second); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("Release(twice): %v", err)
	}
}

func TestPosixDoubleAcquireOnSameInstance(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "server.lock")
	l := lock.NewPosix(path)
	if err := l.Acquire(context.Background(), lock.Exclusive, 5*time.Second); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = l.Release() }()
	err := l.Acquire(context.Background(), lock.Exclusive, 5*time.Second)
	if err == nil {
		t.Fatalf("Acquire on already-held instance: want error, got nil")
	}
}

func TestPosixContextCancelUnblocks(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "server.lock")
	holder := lock.NewPosix(path)
	if err := holder.Acquire(context.Background(), lock.Exclusive, 5*time.Second); err != nil {
		t.Fatalf("Acquire(holder): %v", err)
	}
	defer func() { _ = holder.Release() }()

	ctx, cancel := context.WithCancel(context.Background())
	contender := lock.NewPosix(path)

	var wg sync.WaitGroup
	var gotErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		gotErr = contender.Acquire(ctx, lock.Exclusive, 0)
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	wg.Wait()

	if gotErr == nil {
		t.Fatalf("contender Acquire after cancel: want error, got nil")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Errorf("contender Acquire: got %v, want context.Canceled", gotErr)
	}
}
