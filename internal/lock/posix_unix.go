//go:build unix

package lock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// PosixLock implements Lock against a per-server file using flock(2).
// Lock contents = "<hostname>:<pid>:<mode>:<unix-ts>" for diagnostics —
// flock is the actual exclusion mechanism, the bytes are operator-visible
// info.
//
// flock(2) releases automatically when the file descriptor closes (process
// exit, OS-killed, etc.) — this is the kernel-level guarantee that beats
// PID-text lockfiles.
type PosixLock struct {
	path string

	mu       sync.Mutex
	fd       *os.File
	heldMode Mode
	released bool
}

// NewPosix returns a PosixLock bound to lockPath. The file's parent
// directory must already exist (typically the storage backend's root,
// guaranteed by `server add`).
func NewPosix(lockPath string) *PosixLock {
	return &PosixLock{path: lockPath}
}

// Acquire opens the lock file (creating it if necessary) and calls
// flock(2) at the requested mode. Blocks until the lock is available
// or timeout elapses.
func (l *PosixLock) Acquire(ctx context.Context, mode Mode, timeout time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fd != nil {
		return errors.New("lock: already held")
	}

	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil { //nolint:gosec
		return fmt.Errorf("lock: mkdir parent: %w", err)
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // operator-supplied path
	if err != nil {
		return fmt.Errorf("lock: open %q: %w", l.path, err)
	}

	op := unix.LOCK_EX
	if mode == Shared {
		op = unix.LOCK_SH
	}

	// Non-blocking flock loop with deadline. We can't use blocking flock
	// because Go can't interrupt a blocking syscall — and the operator
	// expects timeout/Ctrl-C to work.
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		err := unix.Flock(int(f.Fd()), op|unix.LOCK_NB) //nolint:gosec // fd cast is the canonical unix.Flock pattern
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			_ = f.Close()
			return fmt.Errorf("lock: flock: %w", err)
		}
		// Already held — wait and retry.
		select {
		case <-ctx.Done():
			_ = f.Close()
			return fmt.Errorf("lock: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
		if timeout > 0 && time.Now().After(deadline) {
			_ = f.Close()
			return ErrLockTimeout
		}
	}

	// Write holder identity for diagnostics.
	hostname, _ := os.Hostname()
	body := fmt.Sprintf("%s:%d:%s:%d\n", hostname, os.Getpid(), mode, time.Now().Unix())
	_ = f.Truncate(0)
	if _, err := f.WriteAt([]byte(body), 0); err != nil {
		// Diagnostic write failed — non-fatal; we still hold the lock.
		_ = err
	}

	l.fd = f
	l.heldMode = mode
	l.released = false
	return nil
}

// Release surrenders the flock and closes the fd. Idempotent.
func (l *PosixLock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fd == nil || l.released {
		return nil
	}
	// flock auto-releases on Close; explicit LOCK_UN is belt-and-braces.
	_ = unix.Flock(int(l.fd.Fd()), unix.LOCK_UN) //nolint:gosec // fd cast is the canonical unix.Flock pattern
	if err := l.fd.Close(); err != nil {
		return fmt.Errorf("lock: close: %w", err)
	}
	l.fd = nil
	l.released = true
	return nil
}
