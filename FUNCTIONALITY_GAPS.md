# Functional Gaps vs pgBackRest

Features pgSafe does not cover, split into capabilities it still needs to implement and capabilities it has deliberately chosen not to provide. The two projects make different design choices; this document exists for operators comparing them.

> Verified against the [pgBackRest source](https://github.com/pgbackrest/pgbackrest) and pgSafe HEAD.

## Features pgSafe does not yet implement

None are blockers for the common use cases; each is a real PostgreSQL feature pgSafe currently doesn't drive. Patches welcome.

### Restore: `--target-timeline`

Maps to PostgreSQL's `recovery_target_timeline` GUC. pgSafe currently follows the default (`recovery_target_timeline = 'latest'`), so a user restoring to a specific historical timeline branch — after a failover that created a new timeline — cannot pin to a pre-failover timeline.

**Plumbing needed:** add `--target-timeline` flag in [cmd/pgsafe/root.go](cmd/pgsafe/root.go)'s restore subcommand, propagate through `restore.Options.TargetTimeline` (new field), emit `recovery_target_timeline = '<value>'` in the generated `recovery.conf`/`postgresql.auto.conf` snippet.

### Restore: `--target-exclusive`

Maps to PostgreSQL's `recovery_target_inclusive = false`. Default is inclusive (recovery stops *at* the target); exclusive stops *just before* it. pgSafe currently only supports the inclusive default.

**Plumbing needed:** add `--target-exclusive` bool flag, propagate through `restore.Options.TargetExclusive`, emit `recovery_target_inclusive = false` when set.

## Deliberate omissions

Capabilities pgSafe has chosen not to provide. These reflect different design priorities; each entry documents pgSafe's rationale plus a reversal path so the call can be revisited.

### `start` / `stop` maintenance kill-switch

A built-in command to halt unattended backup activity (the pattern is to drop a sentinel file the binary checks at startup, with an optional `--force` that SIGTERMs in-flight processes).

pgSafe expects operators to halt unattended activity through the deployment substrate instead: `systemctl stop pgsafe-backup.timer`, `kubectl scale --replicas=0`, disabling the cron entry, IAM credential revocation.

**Reasons:**

1. **Tenet 3 multi-host.** pgSafe in worker mode runs on both the caller and the PG host. A stop-file in `/tmp` on one host doesn't cover the other; a multi-host stop signal has to live storage-side, which means a storage round-trip on every invocation just to check the flag.
2. **Modern substrates are stronger.** systemd timers, k8s scaling, IAM revoke, and feature flags all exist universally now; the stop-file pattern dates from a cron-era substrate without uniform job control.
3. **Correctness chore.** Race-free per-host stop-file checks plus the SIGTERM-in-flight-PIDs path add hundreds of lines and a fault-test class. Scoped out of v1.

**Reversal:** add `cmd/pgsafe/control.go` with `pgsafe stop [--force]` / `pgsafe start`. Pick storage-side (sentinel object at `<storage-root>/.pgsafe-<server>.stop`) for multi-host correctness, or per-host (`/tmp/pgsafe-<server>.stop`) for cheaper checks at the cost of caller/worker disagreement. All entry points (`backup.Run`, `restore.Run`, `archive.Push`, `archive.Get`, `prune`, `verify`, `info`, `check`) gain a startup check. `--force` reads PIDs from [internal/lock](internal/lock)'s lock-body format `<hostname>:<pid>:<mode>:<unix-ts>` to find SIGTERM targets.

### Storage-browser verbs (`repo-ls`, `repo-get`, `repo-put`, `repo-rm`)

Subcommands for inspecting and manipulating the backup storage from inside the backup tool. pgSafe has none.

Operators use cloud-native tools instead: `aws s3 ls`, `gsutil ls`, `az storage blob list`, plain `ls` for POSIX, `sftp` for SFTP.

**Reason:** these verbs made sense when cloud-native CLIs were either immature or not universally installed. In 2026 every storage backend pgSafe supports has a first-party CLI that does this better than a reimplementation could. Adding `pgsafe repo-*` would force pgSafe to maintain N storage-driver listing/streaming surfaces purely for operator convenience already covered by tools operators have.

**Reversal:** add `cmd/pgsafe/repo.go` exposing `pgsafe repo ls|get|put|rm <path>` that dispatches via the existing `storage.Backend` interface (`List`, `Get`, `Put`, `Delete` already present).

### `manifest` query verb

A subcommand to dump a backup's manifest as JSON for operator inspection. pgSafe has no built-in.

The PG-native `backup_manifest` is stored as-is at `<storage>/<backup-id>/backup_manifest`; an operator fetches it directly (`aws s3 cp s3://.../backup_manifest -`).

**Reason:** the file is already JSON and already in a documented PG-standard location. A `pgsafe manifest` wrapper would just be a `Backend.Get` + stdout-write. Operators who want pretty-printing pipe through `jq`.

**Reversal:** trivial — add `cmd/pgsafe/manifest.go` that opens `<backup-id>/backup_manifest` via the configured storage and writes to stdout.

### `server-ping`

A subcommand to ping a remote backup server instance to verify reachability. pgSafe has no equivalent because it has no long-running server daemon — `pgsafe worker --stdio` is a per-invocation SSH-spawned subprocess, and reachability is verified via the SSH session establishing successfully (plus the topology probe documented in [ARCHITECTURE.md](ARCHITECTURE.md) "Operator footgun: accidental proxying").

**Reversal:** N/A — would require first introducing a long-running daemon mode, which is a larger architectural change than a single subcommand.

## Post-v1.0 deferred work

Real future-work items pgSafe may add in a later release. Not blockers for v1.0.

- **Native libpq BASE_BACKUP client.** pgSafe currently shells out to PostgreSQL's own `pg_basebackup` binary on PATH. A native Go replication-protocol client is a v1.1+ candidate, gated on whether performance benchmarks demand it.
- **TUI / web UI.** v1.0 is CLI-only. Monitoring integration runs through `pgsafe check --json`.
- **Cross-architecture incremental restore** (big-endian ↔ little-endian). PostgreSQL's own `pg_combinebackup` is host-byte-order on both sides; pgSafe does not bridge.

## Permanent non-goals

These will not be offered. They are fundamental scope choices, not deferred features; they are not on any future roadmap.

- **Byte-format compatibility with pgBackRest backups.** pgSafe writes its own on-storage layout and manifest format. pgBackRest-format backups are restorable only by pgBackRest; pgSafe-format backups are restorable only by pgSafe. There is no migration tool, no dual-format reader, no plan to add either.
- **Multi-storage active-replication semantics.** The `storages:` list semantics — per-backend independent commit, "at least one durable manifest" success — is the contract. Richer fan-out / consensus / quorum models are not on the roadmap.
