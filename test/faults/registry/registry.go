// Package registry holds the named Hook constants the fault
// injection suite triggers. Each Hook names a deterministic boundary
// inside the caller (or a worker) where a fault test wants to
// drive a specific failure mode.
//
// Producer/consumer model:
//
//   - Tests call Trigger(HookFooBar) right before the action they want
//     to interrupt. In production builds Trigger is a no-op.
//   - Production code paths under -tags=faults consult the registered
//     fault by calling ShouldFail(HookFooBar). When a test has armed a
//     fault for that hook, ShouldFail returns the test-supplied error.
//
// production call sites yet. wires individual hooks into the
// caller alongside the named fault tests.
package registry

// Hook is a typed name for one fault-injection boundary. Constants live
// here, alongside the package documentation that describes when each
// boundary fires.
type Hook string

// All hook names. These map 1:1 to the named scenarios in
// .
const (
	// HookPreFsyncManifest fires immediately before the manifest's final
	// fsync, after the .tmp file has been fully written. A kill at this
	// boundary tests Invariant #6's atomic-rename ordering.
	HookPreFsyncManifest Hook = "pre-fsync-manifest"

	// HookPostFsyncManifest fires after the manifest fsync but before the
	// containing directory's fsync. A kill here tests that recovery from
	// a half-flushed parent dir doesn't surface a manifest pgSafe never
	// promised was durable (Invariant #6).
	HookPostFsyncManifest Hook = "post-fsync-manifest"

	// HookPreCommitMulti fires after every multi-storage Put has streamed
	// its body but before any backend's Commit runs. A kill here tests
	// that an interrupted multi-storage backup leaves no backend with a
	// committed manifest (Invariant #10).
	HookPreCommitMulti Hook = "pre-commit-multi"

	// HookMidBaseBackupTar fires partway through reading the tar stream
	// from pg_basebackup. A kill here tests resume semantics under
	// Invariant #3.
	HookMidBaseBackupTar Hook = "mid-base-backup-tar"

	// HookArchivePushPartial fires when a single archive-push has written
	// to some configured backends but not all. Tests Invariant #7 (no
	// partial archive segments leak past the failure boundary).
	HookArchivePushPartial Hook = "archive-push-partial"

	// HookStandbyPromoted fires when the caller's periodic
	// pg_is_in_recovery() probe observes the standby has been promoted
	// (matches pg_basebackup's documented behaviour and pgBackRest's
	// dbPing/DbMismatchError abort). Tests Invariant #8.
	HookStandbyPromoted Hook = "standby-promoted-during-backup"

	// HookWorkerRestart fires when a hybrid-parallel worker is killed
	// mid-stream and restarted. Tests Invariant #9 cross-worker key
	// consistency.
	HookWorkerRestart Hook = "worker-restart-mid-stream"

	// HookSingleStorageOutage fires when one of N>1 backends in a multi-storage
	// configuration is configured to fail mid-backup. Tests Invariant #10
	// at-least-one durability semantics.
	HookSingleStorageOutage Hook = "single-storage-outage"
)

// AllHooks lists every hook. Useful for tests that want to
// iterate every named boundary or for documentation generators.
func AllHooks() []Hook {
	return []Hook{
		HookPreFsyncManifest,
		HookPostFsyncManifest,
		HookPreCommitMulti,
		HookMidBaseBackupTar,
		HookArchivePushPartial,
		HookStandbyPromoted,
		HookWorkerRestart,
		HookSingleStorageOutage,
	}
}
