// Package lock implements Invariant #4 — "retention-during-active-backup
// safety." `pgsafe backup` and `pgsafe prune` must not race; if `prune`
// drops a parent full while an incremental backup against it is in
// progress, the new backup ends up referencing a non-existent ancestor.
//
// Two lock modes:
//
//	Exclusive — held by mutators (backup, restore, server-delete, prune).
//	Shared    — held by read-only commands (info, verify, check). Multiple
//	            shared holders may coexist; an exclusive grab blocks until
//	            all shared holders release. Without this split, `pgsafe
//	            info` would block during a long backup, making it useless
//	            as a monitoring source.
//
// Implementation: kernel `flock(2)` on a per-server file. Cloud-only
// deployments use the same primitive against a local lock path (typically
// in the operator's filesystem; defaulting under `/tmp` is fine — the
// lock needs auto-release-on-process-death, which `/tmp` provides via
// the kernel, not durability). The cross-machine concurrent-caller
// scenario is not in scope; teams running pgSafe from multiple boxes
// against one bucket coordinate by convention, exactly as pgBackRest
// users do today.
//
// flock auto-releases when the holding process dies (clean exit, kill
// -9, OOM, segfault), so there is no heartbeat or stale-lock reclaim to
// engineer.
package lock

import (
	"errors"
)

// Mode discriminates Shared vs Exclusive lock acquisition.
type Mode int

const (
	// Shared allows multiple holders simultaneously; blocks Exclusive.
	Shared Mode = iota
	// Exclusive allows exactly one holder; blocks any other Shared/Exclusive.
	Exclusive
)

// String renders the mode for diagnostic logging.
func (m Mode) String() string {
	switch m {
	case Shared:
		return "shared"
	case Exclusive:
		return "exclusive"
	default:
		return "unknown"
	}
}

// ErrLockTimeout is returned when Acquire's timeout elapses.
var ErrLockTimeout = errors.New("lock: acquisition timed out")
