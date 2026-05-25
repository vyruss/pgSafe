# pgSafe — the operational rulebook

This document captures the correctness invariants pgSafe enforces. Each invariant corresponds to a real production failure mode in backup tooling; each is tied to a named test in the suite, and any regression on those tests is a release blocker.

The invariants apply uniformly across all three operating modes (simple, remote-parallel, same-host/SSH worker).

These are the rules that make a backup tool trustworthy — they are not implementation details and they do not change between releases. References elsewhere in the codebase (e.g. "Invariant #3" in package docs or commit messages) point here.

---

## 1. Backup-stop / WAL-wait ordering

- **Rule:** `pg_backup_stop()` is called only after every file has been uploaded *and* its storage-side persistence is durable (fsync on POSIX, multipart-complete on object stores). After `pg_backup_stop()` returns the backup-end LSN, poll the WAL archive until every WAL segment required to make the backup recoverable is durably present. Only then write the manifest.
- **Why:** out-of-order produces backups that appear consistent but cannot be restored because a required WAL segment was never archived; or that look incomplete because their stop-LSN was recorded before files finished uploading.
- **Test:** kill the backup process between every pair of these steps; verify the next attempt either resumes correctly or marks the prior attempt as invalid.
- **Implementation:** `internal/backup/{backup,wallag,walsource}`.

## 2. Manifest write atomicity

- **Rule:** the manifest is the *only* "this backup exists and is valid" pointer. Written last. POSIX: write to `manifest.tmp`, fsync, rename to `manifest`. Object stores: upload as `manifest.tmp` key, then atomic conditional-write copy to `manifest`.
- **Why:** a partially-written manifest visible to other commands corrupts retention, restore, and verification. `info` returns lies; `restore` half-succeeds; `expire` deletes the wrong thing.
- **Test:** kill the process at every fsync boundary; assert `info` either lists the prior backup or the new one, never a corrupted view.
- **Implementation:** all five storage drivers + `internal/backup`.

## 3. Checksum-based resume (not mtime-based)

- **Rule:** when resuming a partial backup, validate per-file SHA-256 against PG's *current* file content. Skip files whose checksum matches; redo files whose checksum differs.
- **Why:** mtime-based resume silently produces incoherent backups when concurrent PG activity changes a file without changing its mtime granularity.
- **Test:** kill mid-backup, modify a file at the byte level (without touching mtime), resume, verify the modified file gets re-copied.
- **Implementation:** `internal/storage/posix`, `internal/wal/archive.Push`.

## 4. Retention-during-active-backup safety

- **Rule:** `prune` (the retention command) must never delete a backup that an in-progress backup or restore depends on. Per-server lockfile *and* the new backup's manifest records its prior-backup dependency before prune is allowed to evaluate the server.
- **Why:** a race between a starting incremental and a prune that drops its parent full produces a backup that references a non-existent ancestor.
- **Test:** run `prune` concurrently with a backup that depends on the oldest full; verify the parent full survives until the new incremental's manifest is durably written.
- **Implementation:** `internal/lock` + `cmd/pgsafe/lock.go`.

## 5. WAL archive reachability check at backup start

- **Rule:** before `pg_backup_start()`, push a probe segment through the archive and verify it lands in the storage within a configured timeout. If not, fail the backup before bothering PG with the start.
- **Why:** starting a backup against a misconfigured archive produces a backup that can never become consistent because its required WAL never arrives.
- **Test:** misconfigure `archive_command`; verify backup fails at the check stage with a clear diagnostic, not at the post-stop WAL-wait stage.
- **Implementation:** `internal/backup/probe` + `pgsafe check` (gated on `WALSource=archive`).

## 6. fsync ordering on POSIX storages

- **Rule:** file → `fsync(file)` → write directory entry → `fsync(directory)`. The 7-step durability sequence (`StepWriteTemp`, `StepFsyncFile`, `StepCloseFile`, `StepOpenDir`, `StepFsyncDirPre`, `StepRename`, `StepFsyncDirPost`) runs on every `Put.Close`.
- **Why:** power-loss between `fsync(file)` and `fsync(directory)` produces "file exists but isn't visible" or vice versa.
- **Test:** under deterministic in-process fault injection at every step boundary, verify post-crash storage is always consistent — never both `tmp` and `final` present, never `final` with partial content.
- **Implementation:** `internal/storage/posix`.

## 6.5. Atomic-rename equivalent on every backend

- **Rule:** every backend's `Commit(tmp, final)` is the cloud equivalent of POSIX rename and must be **atomic** and **refuse to overwrite an existing `final`**. Each backend implements this differently:
  - **S3:** HEAD pre-check → `CopyObject(IfNoneMatch="*")` → `DeleteObject(tmp)`.
  - **Azure Blob:** `GetProperties` pre-check → `CopyFromURL(IfNoneMatch="*")` → delete tmp.
  - **GCS:** `Attrs` pre-check → `Copier.If(Conditions{DoesNotExist:true}).Run` → delete tmp.
  - **SFTP:** `Stat` pre-check → `Rename` (server-side atomicity).
- **Why:** without per-backend atomic-rename semantics, the manifest atomicity guarantee (Invariant #2) holds only on POSIX. A clobbered manifest looks valid but covers different bytes.
- **Test:** per-driver `TestXCommitAtomicRename` + `TestXCommitRefusesOverwrite`.
- **Implementation:** all five storage drivers (`internal/storage/{posix,s3,azure,gcs,sftp}`).

## 7. Async WAL push idempotency under concurrent retry

- **Rule:** pushing the same WAL segment twice must be a no-op iff the segment content is byte-identical; an attempt to push different content for an existing segment must error loudly. Use content hash, not just segment name.
- **Why:** PG can re-emit the same segment under crash-recovery; `archive-push` must handle this without corrupting the archive.
- **Test:** simulate concurrent `archive_command` invocations on the same segment; verify dedup vs. error behavior.
- **Implementation:** `internal/wal/archive.Push`.

## 8. Backup-from-standby coordination

- **Rule:** when backing up from a standby, ensure the standby is replaying WAL from the primary's archive (otherwise WAL needed by the backup may never arrive). Check timeline consistency.
- **Why:** a standby disconnected from its primary archive produces backups whose WAL chain can't be reconstructed at restore.
- **Test:** backup from a standby, fail over the primary mid-backup, verify either the backup completes correctly or fails cleanly.
- **Implementation:** `internal/backup/standby`.

## 9. Encryption key consistency across a backup

- **Rule:** a single backup uses a single set of age recipients across all files and all workers. Workers receive the recipient list from the caller at start (in the `Configure` RPC); if a worker fails and is restarted, the same recipient list is reused for that backup.
- **Why:** per-worker key derivation or per-file rotation produces backups that can't be decrypted because different files used different keys.
- **Test:** kill and restart workers mid-backup; verify the resulting backup is fully decryptable with one identity.
- **Implementation:** `Configure.AgeRecipients` in the worker RPC.

## 10. Multi-storage write atomicity

- **Rule:** when writing to multiple storage backends (the `storages:` list), each backend's commit point (manifest write) is independent. A backup is "complete" iff *at least one* backend has a durable manifest. Failed backends are flagged but do not block the others.
- **Why:** forcing all-or-nothing across backends turns an S3 outage into a missed backup window.
- **Test:** take down one storage backend mid-backup; verify the backup completes against the surviving backend and the down backend is marked for retry.
- **Implementation:** `internal/storage/multi` + `internal/backup`.
