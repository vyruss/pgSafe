package main

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/lock"
)

// serverLockPath picks the per-server lockfile path for the given storage
// config. POSIX backends host the lockfile inside the storage root so it
// travels with the storage. Cloud backends have no shared local FS to
// coordinate against, so the lockfile lands in /tmp — flock on /tmp is
// fine because the lock only needs auto-release-on-process-death (kernel
// provides it), not durability across reboots.
func serverLockPath(cfg config.StorageConfig, server string) string {
	name := ".pgsafe-server-" + server + ".lock"
	if cfg.Type == "posix" {
		return filepath.Join(cfg.Path, name)
	}
	return filepath.Join("/tmp", "pgsafe-server-"+server+".lock")
}

// acquireServerLock opens a PosixLock against serverLockPath(cfg) and
// blocks until the lock is held at the requested mode (or timeout
// elapses). Returns a release function the caller must defer; maps
// lock.ErrLockTimeout to a §3.4 exit-code-9 ExitError.
//
// Shared by every CLI subcommand that touches a server's storage: backup,
// restore, server-delete, prune (Exclusive); info, verify, check
// (Shared).
func acquireServerLock(ctx context.Context, cfg *config.Config, mode lock.Mode, timeout time.Duration) (func(), error) {
	l := lock.NewPosix(serverLockPath(cfg.PrimaryStorage(), cfg.Server))
	if err := l.Acquire(ctx, mode, timeout); err != nil {
		if errors.Is(err, lock.ErrLockTimeout) {
			return nil, &ExitError{Code: 9, Msg: err.Error()}
		}
		return nil, &ExitError{Code: 1, Msg: err.Error()}
	}
	return func() { _ = l.Release() }, nil
}
