package main

import (
	"github.com/vyruss/pgsafe/internal/backup"
)

// inferBackupMode picks a backup.Mode from the operator's resolved
// config + parallelism setting, replacing the hand-picked --mode flag.
// Per ARCHITECTURE.md "Wire architecture" — Principles §3 (no --mode):
//
//	sshTarget != ""  → pgSafe (worker on PG host, SSH-spawned or same-host
//	                   subprocess depending on whether sshTarget resolves
//	                   to localhost)
//	sshTarget == ""  → PG-native libpq from caller. Single-connection
//	                   (simple) is the default; explicit --workers turns it
//	                   into a parallel libpq run.
//
// workersExplicit signals whether the operator actually passed --workers
// — defaulting to remote-parallel without an explicit choice would change
// the wire shape under unsuspecting operators (parallel libpq has more
// connections, more memory, different failure modes).
//
// The "same-host pgSafe" case is real: an operator running pgsafe ON the
// PG host with `pg.host: pgsafe@localhost` (or just `pg.host: localhost`)
// gets a worker subprocess via local.Dial — no ssh, no libpq, just
// syscalls — exactly matching the architecture's same-host worker path.
func inferBackupMode(sshTarget string, workers int, workersExplicit bool) backup.Mode {
	if sshTarget != "" {
		return backup.ModeWorker
	}
	if workersExplicit && workers > 1 {
		return backup.ModeRemoteParallel
	}
	return backup.ModeSimple
}
