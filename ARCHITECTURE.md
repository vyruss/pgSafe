# pgSafe — Architecture

This document records the design decisions and module layering of pgSafe. It is updated in the same commit as the structural change it describes. For the correctness rulebook (the ten invariants), see [INVARIANTS.md](INVARIANTS.md). For features pgSafe does not yet implement, see [FUNCTIONALITY_GAPS.md](FUNCTIONALITY_GAPS.md).

## Contents

- [Toolchain](#toolchain)
- [Module path and layout](#module-path-and-layout)
- [Filter chain](#filter-chain)
- [Storage abstraction](#storage-abstraction)
- [PostgreSQL integration](#postgresql-integration)
- [Backup engine](#backup-engine)
- [Restore](#restore)
- [Wire architecture](#wire-architecture)
- [Operational commands](#operational-commands)
- [Tenet-3 scoped credentials](#tenet-3-scoped-credentials)
- [Real-cloud validation](#real-cloud-validation)
- [Divergences from pgBackRest (D-entries)](#divergences-from-pgbackrest-d-entries)

---

## Toolchain

- **Go 1.25.** `go.mod` declares `go 1.25.0`; `GOTOOLCHAIN=auto` (the default) means `go build` auto-fetches the latest 1.25.x patch on developer machines and CI runners alike.
- **Tool directive for dev tools.** `go.mod` uses the `tool` directive (Go 1.24+) to pin `goimports` and `golangci-lint` as project tools, accessible via `go tool goimports` / `go tool golangci-lint`. Avoids requiring developers to install these globally and keeps versions reproducible. The indirect-dep section of `go.mod` grows as a side-effect; that's cosmetic.
- **No vendoring.** `go.mod` + `go.sum` are the source of truth; the module proxy handles downloads.

## Module path and layout

- Go module path: `github.com/vyruss/pgsafe` (lowercase per Go convention).

```
                       ┌──────────────────────────────┐
                       │  cmd/pgsafe (cobra entrypoint)│
                       └──────────────┬───────────────┘
                                      │
                  ┌───────────────────┼───────────────────┐
                  ▼                                       ▼
       ┌────────────────────┐                  ┌────────────────────┐
       │  internal/config   │                  │  internal/backup   │
       │  (YAML + validate) │                  │  (caller)          │
       └────────────────────┘                  └─────────┬──────────┘
                                                          │
              ┌───────────────────┬───────────────────────┼──────────┐
              ▼                   ▼                       ▼          ▼
     ┌──────────────┐  ┌────────────────────┐  ┌─────────────┐ ┌──────────────┐
     │ internal/pg  │  │  internal/filter   │  │ internal/   │ │ internal/    │
     │ (libpq +     │→→│  (io.Reader pipe:  │→→│ manifest    │ │ storage      │
     │  bbsink tar) │  │  hash→compress→age)│  │ (JSON)      │ │ (Backend)    │
     └──────────────┘  └────────────────────┘  └─────────────┘ └──────────────┘
```

Module-by-module surfaces (details live in each package's doc comments):

- **`internal/config`** — `Config` struct + named sub-structs, strict YAML decode (`KnownFields(true)`), single `Validate()` seam.
- **`internal/log`** — `*slog.Logger` factory locked to `json` / `text` formats.
- **`internal/storage`** — `Backend` interface (`Open` / `Put` / `Commit` / `Get` / `Stat` / `List` / `Delete`); concrete drivers `posix`, `s3`, `azure`, `gcs`, `sftp`; fan-out via `multi`.
- **`internal/filter`** — streaming hash → compress → encrypt pipeline; `compression` (`Codec` + 4 implementations); `encryption` (age); `hash` (SHA-256); `pagechecksum` (PG FNV-1a heap-page validator).
- **`internal/manifest`** — PG-native `backup_manifest` `Builder` + `Sidecar` (Storage-Metadata.json) + LSN type + INCREMENTAL.* file format.
- **`internal/pg`** — `Cluster` seam, `conn` (pgxpool wrapper), `identity` (SQL functions only, no on-disk pg_control), `basebackup` (shells out to `pg_basebackup`), `bracket` (Modern PG 15+, Legacy PG 13/14), `readbinary` (`pg_read_binary_file` chunked reads), `walsummary` (PG 17+ WAL summarizer), `pgtest` (testcontainers fixture).
- **`internal/backup`** — caller, three mode drivers (`runSimple`, `runRemoteParallel`, `runWorker`), bracket coordination, WAL-wait, resume (`backup_manifest.copy`), standby probe (Invariant #8), archive-reachability probe (Invariant #5), incremental staging.
- **`internal/restore`** — minimal restore engine + chain-combine (`pg_combinebackup`) + PITR target emission.
- **`internal/transport`** — `ssh` (subprocess wrapper around `/usr/bin/ssh`), `rpc` (`net/rpc/jsonrpc` over stdio), `creds` (Tenet-3 scoped-credential mint + open), `local` (same-host `exec.Cmd` worker), `sftptunnel` (caller-proxied SFTP via `ssh -R`).
- **`internal/worker`** — production `rpc.WorkerService` implementation (the PG-host worker process).
- **`internal/wal/archive`** — `Push` / `Get` for `pgsafe archive-push` / `archive-get`.
- **`internal/lock`** — `flock(2)`-based per-server lock with `Shared` / `Exclusive` modes (Invariant #4).
- **`internal/retention`** — pure-function `Evaluate(records, policy) → expirable + oldest-needed-LSN`.
- **`internal/check`**, **`internal/info`**, **`internal/verify`** — diagnostic and listing operations.

---

## Filter chain

`hash → compress → encrypt → sink`. The hash sees plaintext (Invariant #3 requires plaintext checksums in the manifest). Compression precedes encryption so the encrypted stream is incompressible noise — encrypt-then-compress would leak compression-ratio side-channels.

`Chain.Wrap(sink) (io.WriteCloser, *Result, error)` returns a `*Result` that is populated on a successful `Close`; `Result` carries both the plaintext SHA-256 / byte count and the on-the-wire (post-encryption) SHA-256 / byte count. Both digests come from a single pass — the plaintext side via the explicit hash stage, the on-the-wire side via a `repoSink` adapter wrapping the storage writer. The on-the-wire digest is what the resume protocol uses to verify a prior attempt's files committed durably (it does not require the encryption identity).

Close order: capture plaintext digest → `comp.Close()` → `enc.Close()` → `sink.Close()`. A failure short-circuits but still closes downstream stages; the first error wins.

The reverse path (`Unwrap`) takes ciphertext + identities (not recipients) and yields a `ReadCloser` of reconstructed plaintext. Restore-only callers don't need to fabricate recipients to use it.

---

## Storage abstraction

`storage.Backend` is the seam:

- `Open(ctx)` is idempotent and validates connectivity (HEAD-bucket / list-container / list-files / stat-root).
- `Put(ctx, relPath) → io.WriteCloser` where `Close` is the durability point. Internally, the temp scheme differs per driver (POSIX writes `<path>.pgsafe-tmp` and renames; S3 uses a `manager.Uploader` over an `io.Pipe`).
- `Commit(ctx, tmp, final)` is the atomic-rename seam used for the `manifest.tmp → manifest` boundary. Refuses to overwrite an existing `final`. POSIX: `rename(2)` + `fsync(parent dir)`. Object stores: conditional copy + delete.
- `Get` / `Stat` / `List` / `Delete` are straightforward; `List` excludes the sibling `wal/` directory from the recursive walk so callers iterate it explicitly when needed.

### POSIX driver (Invariant #6 — fsync ordering)

`Put.Close` runs a 7-step sequence: write tmp → fsync(tmp) → close(tmp) → open(parent) → fsync(parent, pre-rename) → rename(tmp → final) → fsync(parent, post-rename).

The pre-rename `fsync(parent)` is defensive. Without it, a crash between the rename and the post-rename fsync could leave a directory state where neither tmp nor final exists (the directory entry that ever recorded tmp may not have hit the platter yet). With the pre-rename fsync, the worst case is "tmp present" or "final present" — never both gone, never both present.

`Options.Fault` is an injection point invoked after each named step (`StepWriteTemp`, `StepFsyncFile`, `StepCloseFile`, `StepOpenDir`, `StepFsyncDirPre`, `StepRename`, `StepFsyncDirPost`, plus `StepCommitRename` / `StepCommitFsync` for `Commit`). Production passes `nil`; tests use it both for step-order observation and for deterministic fault injection at each boundary.

`relPath` must satisfy `filepath.IsLocal` (no absolute, no `..` traversal); this is a caller-bug guard at the seam where files actually land.

### Cloud drivers (Invariant #6.5 — atomic-rename equivalent)

Each cloud backend's `Commit(tmp, final)` is atomic and refuses to overwrite:

| Driver | Pre-check | Atomic step | Cleanup |
|---|---|---|---|
| `s3` | `HeadObject(final)` | `CopyObject(IfNoneMatch="*")` | `DeleteObject(tmp)` |
| `azure` | `GetProperties(final)` | `CopyFromURL(IfNoneMatch="*")` | delete tmp |
| `gcs` | `Attrs(final)` | `Copier.If(Conditions{DoesNotExist:true}).Run` | delete tmp |
| `sftp` | `Stat(final)` | `Rename` (server-side atomicity) | (rename is atomic) |

The HEAD pre-check is defence-in-depth; some MinIO builds quietly accept a `CopyObject` without honouring `IfNoneMatch`. The `If-None-Match: *` capability on AWS S3 went GA in August 2024 — older clients and S3-compat backends may return `NotImplemented`; `Open()` probes for it and fails fast.

### Multi-storage fan-out (Invariant #10)

`internal/storage/multi.TeeWriter` is the "tee-once" building block. The filter chain runs exactly once and the encrypted+compressed byte stream is distributed to N backends concurrently via per-backend buffered channels + goroutines. CPU cost is flat in N; only network/disk IO scales.

```
plaintext → filter.Chain → multi.TeeWriter.Write → [ch[0] → backend[0].Put, ch[1] → backend[1].Put, ...]
```

- A backend whose goroutine reports an error closes its `fail` channel; the next `Write` detects this via non-blocking select and stops sending. `Close` returns nil if ≥1 backend succeeded.
- `multiState` in `internal/backup` tracks cumulative dead-backend state across all file writes; only alive backends receive the manifest `Commit`.
- Default channel buffer depth is 256 chunks. After zstd compression the chunks reaching `TeeWriter` are typically 0.5–2 MiB, giving ~128–512 MiB of look-ahead per backend in steady state — enough to absorb seconds of speed difference between a fast local and a cloud backend before back-pressure kicks in.

**Back-pressure and the "at-least-one" guarantee.** Invariant #10 guarantees durability when at least one backend commits successfully — it is a fault-tolerance guarantee, not a performance-isolation guarantee. A slow but healthy backend eventually fills its channel and back-pressures the main writer; backup throughput is therefore bounded by the slowest *healthy* backend. A backend that actually errors out is marked dead and dropped, and the fast backends then run at full speed. Performance isolation from a permanently-slow-but-non-failing backend is a future concern.

`backup.Result.PartialStorages` records the count of backends that failed. A non-zero `PartialStorages` with a nil error means the backup is durable on the surviving backends.

YAML knob `storages:` (list, plural) is the modern form; legacy singular `storage:` is silently promoted to a one-element list for backward compatibility.

---

## PostgreSQL integration

### Identity and bracket

`pg.Cluster` is the caller seam — one method per PG operation (`Identity`, `BaseBackup`, `Close`); a hand-written mock lives in tests. The interface deliberately hides BASE_BACKUP implementation details (shell-out vs. native libpq) so callers stay decoupled.

`internal/pg/identity` reads cluster identity (system_identifier, control version, catalog version, timeline, checkpoint LSN, WAL segment size) via SQL functions only — `pg_control_system`, `pg_control_init`, `pg_control_checkpoint`, `pg_control_recovery`. Per Tenet 1, pgsafe never parses `pg_control` on disk; we ask the running cluster.

**Non-obvious PG 18 fact:** `wal_segment_size` is *not* a column of `pg_control_checkpoint()`; it's `bytes_per_wal_segment` in `pg_control_init()`. Some older docs and adjacent tooling suggest otherwise.

`internal/pg/basebackup` shells out to PG's own `pg_basebackup --pgdata=- --format=tar --wal-method=none` for simple mode. Implementing the BASE_BACKUP message protocol natively in Go would take multi-week effort and produce *exactly* the same tar stream — remote-parallel and pgSafe-mode paths read `$PGDATA` differently anyway. `--wal-method=none` is required because `--wal-method=stream` cannot be combined with `--pgdata=-`; the operator's `archive_command` (or pgsafe's own `stream`/`walgrab` source) supplies the bracket WAL.

`internal/pg/bracket` wraps PG's non-exclusive backup bracket SQL. Two implementations (`bracketModern` for PG 15+, `bracketLegacy` for PG 13/14) share a session-pinning rule: PG's `pg_backup_start`/`pg_backup_stop` (and the legacy `pg_start_backup`/`pg_stop_backup`) require the *same* backend to call both. The bracket holds a dedicated `*pgxpool.Conn` from `Start` through `Stop` and only releases it after `Stop` returns. Without that pin, a busy pool routes `Stop` to a different connection and PG errors `non-exclusive backup is not in progress`.

### pgtest fixture

`internal/pg/pgtest` spins up `postgres:<version>` for every supported PG version (13–18). Two operational quirks worth noting for anyone debugging it:

1. **WAL archive directory mode.** The container's postgres user is UID 999. The host-mounted WAL archive dir needs mode `0o777` (or POSIX ACLs) so the container user can write to it; if the host dir doesn't exist when `docker run -v` mounts it, docker auto-creates it as root with default mode, the container's `archive_command` silently fails on permission errors, and `pg_backup_stop` hangs forever waiting for the segment. Symptom: a 240s timeout with no obvious error. The fixture creates the directory and `chmod 0o777` before the container starts.
2. **`archive_command` chmod-after-cp.** The container's postgres user writes WAL into the bind-mounted host dir, but the resulting files are mode `0o600` owned by some host UID. The host-side pgsafe (different UID) then can't read them for SHA-256 hashing. The fixture's `archive_command` is `test ! -f .../%f && cp %p .../%f && chmod 0644 .../%f`.

`summarize_wal=on` is added to the container CMD only for PG 17+ (older versions error on the unknown GUC).

### Page checksums

`internal/filter/pagechecksum` validates PG 8 KiB heap pages against PG's FNV-1a-with-32-lanes algorithm. Three modes: `ModeOff` (passthrough; used when the cluster has `data_checksums=off`), `ModeStrict` (any mismatch errors `ErrChecksumMismatch`), `ModeLax` (logs and continues — for clusters mid-`initdb --data-checksums`). The validator accepts zero-checksum pages (PG semantics for "checksums disabled") and partial trailing pages (PG occasionally writes those during recovery).

Validation only runs in remote-parallel and worker modes (where pgsafe reads `$PGDATA` directly). Simple mode bytes flow through `bbsink_copytblspc` which validates on the PG side; re-validating client-side would be redundant.

### `pg_read_binary_file`

`internal/pg/readbinary` reads PG-side files via repeated `pg_read_binary_file(path, offset, length, true)` calls (default 64 MiB chunks; below PG's 1 GiB per-call ceiling). Implements `io.Reader` so the caller wires it through the same filter chain as a tar entry. `ListPGData(ctx, pool)` walks `$PGDATA` via a recursive CTE over `pg_ls_dir` + `pg_stat_file`, excluding `pg_wal*`, `postmaster.pid`, `postmaster.opts`, `pg_internal.init`. Requires `pg_read_server_files` membership.

---

## Backup engine

### Three operating modes

| Mode | File-data path | Bracket connections to PG | When to use |
|---|---|---|---|
| **simple** (PG-native) | Caller shells out to `pg_basebackup`, consuming the `bbsink_copytblspc` tar stream over a single libpq replication connection | 1 | Smallest deployment; works wherever libpq reaches PG |
| **remote-parallel** (PG-native) | N libpq workers calling `pg_read_binary_file` from the backup host, with client-side page-checksum validation | N (one per worker) plus 1–2 for the bracket | Faster than simple, doesn't need PG-host shell access |
| **worker** (pgSafe-mode) | A worker process on the PG host reads `$PGDATA` via OS syscalls and streams bytes directly to storage; same-host or SSH-spawned | 1–2 for the bracket on the caller; the worker bypasses libpq entirely for bulk data | Fastest on fast local disks; the only mode that supports Tenet-3 scoped credentials |

All three share the same filter chain, manifest format, sidecar, and rulebook. Only the file-data ingest path differs.

### Mode inference

There is no `--mode` flag. The caller inspects the YAML — `pg.host` set + reachable via ssh → worker mode; multiple workers + no `pg.host` → remote-parallel; default → simple. `--workers=N` is the only operator-facing parallelism knob.

This is a deliberate UX choice. Mode is a property of the deployment shape (where the caller, PG, and storage live relative to each other), not a per-invocation flag. Letting the operator override it is a footgun (you can ask for worker mode on a cluster you can't SSH to; you get a failure that looks like "ssh refused" rather than "your config is incoherent").

### WAL source

Every backup needs the WAL covering `[pg_backup_start LSN, pg_backup_stop LSN]` for restore to replay. pgsafe supports three sources:

| `--wal-source` | Delivery | Restore looks at |
|---|---|---|
| `archive` (default) | PG's `archive_command` (typically `pgsafe archive-push %p`) ships every segment to `<storage>/wal/<TLI>/<seg>-<sha>` before `pg_backup_stop` returns. | `<storage>/wal/<TLI>/` |
| `stream` | PG-native simple mode only. `pg_basebackup --wal-method=fetch` packs the bracket WAL into the data tar's `pg_wal/` entries — backup is self-contained. | `<backup>/pg_wal/` |
| `walgrab` | pgSafe-mode worker only. After `pg_backup_stop`, the worker reads `$PGDATA/pg_wal/<bracket-segs>` directly and ships them through the same RPC pipeline as data files. | `<backup>/pg_wal/` |

`--standalone` is shorthand: picks `stream` in PG-native simple mode and `walgrab` in pgSafe-mode, and prints a warning that PITR reach is constrained to the bracket window unless `archive_mode=on` is configured separately.

Mode/source compatibility is enforced upfront in `backup.Run`:

| Mode | `archive` | `stream` | `walgrab` |
|---|---|---|---|
| simple | ✓ | ✓ | ✗ (no worker for the disk read) |
| remote-parallel | ✓ | ✗ (one stdout, can't carry inline WAL) | ✗ |
| worker | ✓ | ✗ (worker uses syscalls, not pg_basebackup) | ✓ |

Restore is unaware of which source produced a backup. It looks for `<backup>/pg_wal/<seg>` first and falls back to `<storage>/wal/<TLI>/` only if the inline path is empty. Archive-tied and inline-WAL backups are byte-shape-identical from restore's perspective.

### Incremental backups (PG 17+)

`internal/pg/walsummary` wraps PG 17's WAL summarizer surface — `pg_get_wal_summarizer_state`, `pg_available_wal_summaries`, `pg_wal_summary_contents`. Version-gated with `ErrUnsupported` on PG <17; `pgsafe backup --type=incr` errors cleanly on older clusters.

`internal/manifest` encodes the PG 17+ INCREMENTAL.* file format: `magic(4) | num_blocks(4) | truncation_block_length(4) | block_numbers[num_blocks](4 each) | block_data[num_blocks](8192 each)`, all little-endian.

Backup flow for `--type=incr --parent=<id>`:

1. Read the parent's `backup_manifest` from storage; stage to a temp file.
2. Pass `--incremental=<staged>` to `pg_basebackup`; incremental mode keeps the canonical PG manifest (no `--no-manifest` opt-out here).
3. Tar processing intercepts the `backup_manifest` entry plaintext (bypassing the filter chain). The captured bytes are byte-for-byte what `pg_combinebackup` accepts.
4. The hand-rolled manifest finalizer is skipped — the captured PG manifest is written directly as `<backupID>/backup_manifest`.

Restore chain combine: `internal/restore/chain.go` walks `parent_backup_id` from leaf to root via sidecars; for each chain entry it decrypts every file into `<sibling-of-target>/.pgsafe-stage-XXXXXX/<id>/` and writes the captured manifest at the chain entry's root. Then it shells out: `pg_combinebackup <full> <incr1> ... --output=<target>`. The stage directory is a sibling of the target (via `os.MkdirTemp`) because `pg_combinebackup` refuses a non-empty target.

`manifest.Builder.UpdateStartLSN` reseats the WAL-Ranges Start-LSN to the canonical `backup_label`-derived value after parsing. Without it the manifest's recorded LSN is the cluster's CheckpointLSN at backup-start time, which precedes the actual `pg_backup_start` LSN and breaks `pg_combinebackup`'s parent-LSN expectation check.

**Manifest version specifics:** Manifest version is **2** (PG 18); `System-Identifier` is required at the top level. Every file declares `"Checksum-Algorithm": "SHA256"` (not PG's default `CRC32C`) so the manifest's checksums match the plaintext SHA-256 the filter chain produces (Invariant #3). The manifest is hand-emitted (no `encoding/json`) because the trailing `Manifest-Checksum` is a SHA-256 over the bytes up to itself; reformatting the body changes the bytes hashed and `pg_verifybackup` rejects.

### Backup-ID format

`<YYYYMMDD>T<HHMMSS><F|I>`, second resolution. Full backups end `F`; incrementals end `I`. Parent linkage lives in the sidecar's `ParentBackupID` field, not in the ID itself (same shape as pgBackRest's flat backup-ID).

`ChooseBackupID` queries the storage for the most recent existing label and adds 1 s if the formatted timestamp would collide. Concurrent same-host backups are prevented by `internal/lock`; this guards the cross-host case where two operators share a storage and start within the same second.

### Resume protocol

pgsafe resumes an interrupted backup by reusing the prior attempt's bytes on storage, not by re-reading the PG source. The design mirrors pgBackRest's [backupResumeFind/backupResumeClean](https://github.com/pgbackrest/pgbackrest/blob/main/src/command/backup/backup.c#L702) but stays caller-side end-to-end.

- **Checkpoint.** Every K files (default 10), pgsafe writes `<backupID>/backup_manifest.copy` — a pgsafe-native JSON shape with per-file `{path, plaintextSize, plaintextSHA, repoSize, repoSHA}`. Distinct from PG's `backup_manifest` (byte-precise, only emitted on successful finalize).
- **Discovery.** Next backup-start lists the storage prefix once. Per backup-id directory: `backup_manifest` present → completed, skip; no `.copy` → skip; `.copy` present + grace-expired → reap the directory (auto-prune); `.copy` present + fresh + gate-matches (PgsafeVersion, BackupType, ParentBackupID, Compression, EncryptionRecipients, SystemIdentifier all equal) → resume target.
- **Resume-clean.** Walks the chosen backup-id's files, sha256s each one on storage, compares to the recorded `RepoSHA256`. Mismatched files (torn mid-write, stale) are `Backend.Delete`'d so the new attempt starts from a known-clean per-file state.
- **Per-file skip.** The caller's main loop short-circuits paths that survived resume-clean.
- **Path denylist.** `backup_label`, `backup_manifest`, `tablespace_map` are never reused.

YAML knobs: `backup.resume`, `backup.resume_checkpoint_every_n_files`, `backup.resume_grace_period`. CLI override: `--no-resume`.

---

## Restore

`internal/restore` handles file copy, PITR target selection, parallel file copy, tablespace remap, signal-file generation, WAL fetch, and incremental chain combination.

PITR targets become PG GUCs in the generated `postgresql.auto.conf` snippet: `TargetTime` → `recovery_target_time`, `TargetXID` → `recovery_target_xid`, `TargetLSN` → `recovery_target_lsn`, `TargetName` → `recovery_target_name`, `TargetAction` → `recovery_target_action`. At most one target may be set (CLI enforces); `TargetAction` defaults to `pause`.

Tablespace remap (`--tablespace OID=PATH`) rewrites `tablespace_map` and the `pg_tblspc/<oid>` symlinks before the cluster boots.

Parallel restore uses `errgroup` with `SetLimit(--workers)`; default is 4.

**Two non-obvious operational details:**

- **Empty directories.** PG's tar stream includes empty directory entries (`pg_notify`, `pg_stat`, `pg_subtrans`, etc.) that the cluster needs at startup. The Files array doesn't capture them, so the caller collects directory paths and stores them in the sidecar's `Directories []string` field. Restore mkdirs each before placing files.
- **The `/bin/false` `restore_command`.** PG requires `restore_command` to be present when `recovery.signal` is set, but our restore pre-stages all needed WAL into `pg_wal/`. PG consumes those first and only falls back to `restore_command` after — at which point `/bin/false` signals end-of-archive and recovery completes cleanly.

---

## Wire architecture

How pgsafe drives backup and restore across the network.

### PG-native vs pgSafe mode

The single distinction that matters: **does pgsafe run a worker process on the PG host?**

- **PG-native** — *workerless*. pgsafe runs only on the caller and reads PG through libpq, using the replication protocol (`BASE_BACKUP` for sequential reads, `pg_read_binary_file` for parallel-by-relation). Same mechanism `pg_basebackup` uses.
- **pgSafe mode** — *workered*. pgsafe runs on the caller AND spawns a worker process on the PG host (via SSH, or as a same-host subprocess when the caller is already there). The worker reads `$PGDATA` files via OS syscalls, bypassing libpq for bulk data.

### Principles

1. **The only host pgsafe knows about as itself is the *caller* — wherever the operator typed `pgsafe`.** Everything else is "what's reachable from here, and how." There is no caller / PG-host / storage-host role inside the binary; those are deployment labels.
2. **The YAML is caller-relative by definition.** Every field describes the world from the caller's vantage point: `pg.host` = "how *I* reach pg-host"; storage credentials are "creds *I* have for the backend." Different callers in the same topology have different YAMLs.
3. **Shape is inferable, not a flag.** pgsafe inspects the YAML — what's local, what's reachable natively, what needs ssh — and picks the architecture. No `--mode` flag.

### Scenario matrix

The full deployment matrix:

| Caller is | Storage is | Caller→worker/storage | Transport | Parallelization via |
|---|---|---|---|---|
| PG host | POSIX, same host | local (no network) | OS syscalls | N goroutines on local fs |
| PG host | Cloud | HTTPS → cloud | cloud SDK | N HTTP/2 streams |
| PG host | Real SFTP server | SSH (caller↔storage) | SSH+SFTP, single encryption | SFTP request-id pipelining |
| Operator host | POSIX on PG host | SSH (caller↔worker) | OS syscalls on worker | N goroutines on worker |
| Operator host | POSIX on operator host | SSH (caller↔worker) | SSH+SFTP doubly encrypted via reverse-forward into caller's in-process `pkg/sftp.Server` | SFTP request-id pipelining |
| Operator host | Cloud, reachable from worker | SSH (caller↔worker) | cloud SDK on worker, HTTPS direct to cloud | N HTTP/2 streams |
| Operator host | Cloud, only reachable from caller | SSH (caller↔worker) + SSH `-D` SOCKS5 forward | HTTPS via cloud SDK, SOCKS-tunneled to caller, then HTTPS caller→cloud (TLS preserved e2e) | N HTTP/2 streams |
| Operator host | Real SFTP, reachable from worker | SSH (caller↔worker) | SSH+SFTP from worker to storage, single encryption | SFTP request-id pipelining |
| Operator host | Real SFTP, only reachable from caller | SSH (caller↔worker) + SSH `-R` forward | SSH+SFTP doubly encrypted via caller's TCP forward | SFTP request-id pipelining |

The unified principle for the "only reachable from caller" rows: **the caller proxies at TCP level using SSH's native forwarding** — `-R` for SFTP-over-SSH, `-D` for arbitrary HTTPS endpoints. No application-layer proxy code; the worker uses its existing storage backends unchanged.

### Operator footgun: accidental proxying

The "only reachable from caller" rows have a real cost: throughput caps at the caller's NIC, and bytes traverse SSH twice. When the worker *could* reach storage directly, picking the proxy path is strictly slower for no benefit. Operators can land in this trap by writing a YAML on a laptop that has a VPN route to storage the PG host doesn't share, or by forgetting to provision the PG host's egress.

Safeguards, in order of importance:

1. **Probe at session start.** Before any bulk transfer, the caller asks the worker to attempt a one-shot reachability check against the configured storage (SFTP `Stat`, cloud `HeadBucket`).
2. **Log the resolved topology in plain English.** Every run prints which mode is active, where the worker is (if any), and how bytes flow. Operators scanning cron logs see the chosen shape at a glance.
3. **YAML lockdown.** `pg.storage_reach: native_only` aborts the run on probe failure rather than silently fallback. `via_caller` forces proxy mode regardless.
4. **Interactive prompt is opt-in.** `pgsafe restore --confirm-proxy` prompts before falling back to caller-proxy mode at a terminal; not the default because pgsafe runs under cron/systemd.

### Parallelism default

`--workers=4`. Measured ~1.86× speedup at 4 vs 1 on a 5M-row, 721 MB demo cluster (gzip+age); gains plateau by ~8. In worker mode the worker reads `$PGDATA` via syscalls, so the bracket holds 1–2 libpq connections regardless of N. In PG-native mode each worker holds its own libpq connection.

---

## Operational commands

### Per-server lockfile (Invariant #4)

`internal/lock.PosixLock` wraps `unix.Flock(2)` with two modes:

- **Exclusive** — acquired by `backup` and `prune` before any resource opens. Ensures a concurrent prune cannot delete files a backup is still writing.
- **Shared** — acquired by `info`, `verify`, `check`. They don't block each other but do wait for any in-progress exclusive mutation.

Lock path: `<storage-root>/.pgsafe-server-<server>.lock` for POSIX backends; `/tmp/pgsafe-server-<server>.lock` for cloud-only deployments (kernel auto-releases on process death). Lock-file body is `<hostname>:<pid>:<mode>:<unix-ts>` for operator diagnostics; `flock(2)` is the actual exclusion mechanism.

The cloud-sentinel-with-heartbeat design was explicitly considered and rejected: TOCTOU on the `Commit` step, clock-skew handling, and heartbeat-failure modes are real bug classes, not theoretical ones, and the distributed-caller scenario that justifies them doesn't arise in practice (deployment convention handles it, same as pgBackRest).

### Retention and WAL pruning

`internal/retention.Evaluate` is a pure function: given `[]BackupRecord` and `Policy`, returns `(ExpirableBackupIDs, OldestNeededLSN)`. Chain semantics: a full backup and all its descendant incrementals form an atomic unit — the full survives until every incremental in its chain is also expirable.

WAL pruning uses lexicographic comparison: 24-hex-char segment names sort chronologically within a timeline. `OldestNeededLSN` is converted to the corresponding segment name at the PG default 16 MiB segment size; any segment whose name is lex-less than that cutoff is pruned.

---

## Tenet-3 scoped credentials

`internal/transport/creds` is the Tenet-3 layer: the caller (running on the backup host, where long-lived storage credentials are allowed) mints a short-lived, prefix-scoped, write-only credential and delivers it to the PG-host worker over the JSON-RPC channel. The worker uses the credential only for the duration of the backup; it never persists to disk on the PG host.

| Backend | Mechanism | Lifetime |
|---|---|---|
| S3 | `sts:AssumeRole` with inline session policy narrowed to `s3:PutObject + s3:AbortMultipartUpload` on `arn:aws:s3:::<bucket>/<prefix>/*` | 1–2 hours |
| Azure Blob | Service SAS scoped to container + prefix with `write+create+add` perms | 1–2 hours |
| GCS | `iamcredentials.GenerateAccessToken` for a target service account, scope `https://www.googleapis.com/auth/devstorage.read_write` | ~1 hour |
| SFTP | PEM key bytes shipped in-memory; password-only configs refused | process lifetime |

`OpenBackendFromCredential(ctx, c)` is the worker-side companion: takes a `Credential` and returns an opened `storage.Backend` using only the in-memory credential. The credential lives in the worker's heap until process exit; nothing persists.

`internal/worker` is the production `rpc.WorkerService` implementation. `runWorkerBackup` is the caller-side orchestration: `bracket.Start` → query PG for `data_directory` → mint scoped credentials → `ssh.Dial` → `Hello` (asserts protocol-version match) → `Configure` (ships creds + recipients + file list + `data_directory`) → parallel `StreamFile` (N goroutines over a single connection; `net/rpc` dispatches one handler goroutine per call) → `WriteBlob backup_label` + `tablespace_map` (filtered) → `bracket.Stop` → WAL-wait → `WriteBlob backup_manifest.tmp` + `Storage-Metadata.json` (plaintext) → `Commit`. **Every backend write goes through the worker** — the worker is the only process with the scoped credential.

---

## Real-cloud validation

Real-cloud round-trips (S3 STS, Azure SAS, GCS impersonation) are gated on `PGSAFE_REAL_CLOUD=1` and not part of `run-ci-local.sh`. They require per-developer cloud accounts with documented IAM/role templates. Structural verification (banned permissions absent, `spr=https` enforced on SAS, sane expiration) runs unconditionally; the round-trips themselves require real endpoints.

**Why structural rather than round-trip for Azure SAS:** real Azure is HTTPS-only; Azurite is HTTP-only. We refuse to relax `MintAzureSAS` to allow HTTP — that would let workers transmit credentials in cleartext on a compromised network path.

---

## Divergences from pgBackRest (D-entries)

pgsafe inherits its operational rulebook (the ten invariants) from pgBackRest, but takes those concepts and re-expresses them in a different language and a different UX. Each D-entry below documents one UX choice that diverges from pgBackRest's surface, with the rationale and a reversal path so the choice can be revisited.

Operators arriving from pgBackRest may want pgsafe to behave like the tool they know. The reversal sections exist so this is a documentation decision, not an engineering trap.

### D-001 — `stanza` → `server`

- **pgBackRest:** a *stanza* is a named bundle of config describing one PG cluster to back up. Term comes from poetry.
- **pgSafe:** a *server*. Matches Barman's vocabulary (~12 years of operator usage) and operators' actual mental model ("I'm backing up the production server").
- **Reason:** "stanza" is well-known to operators who've used pgBackRest, but reads as unfamiliar jargon to operators arriving from Barman, restic, k8s tooling, or new ones learning their first PG backup tool. pgsafe targets that wider audience. Since pgsafe is not byte-compatible with pgBackRest backups, inheriting the surface vocabulary doesn't buy migration.
- **Reversal:** in every file, replace `server` ↔ `stanza` and `Server` ↔ `Stanza` only in: YAML key `server:`; field `config.Config.Server`; field `manifest.Sidecar.Server` + JSON tag `"server"`; field `backup.Options.Server`; CLI flag `--server`; operator-facing log/error strings; prose in README and ARCHITECTURE; test identifiers.

### D-002 — `stanza-create` → `server add`

- **pgBackRest:** `pgbackrest stanza-create` initializes the on-disk storage for a stanza.
- **pgSafe:** `pgsafe server add` — `server` is a parent command with `list`, `upgrade`, `delete`, `check` siblings.
- **Reason:** stanza-create carries the rejected stanza vocabulary; `init` was rejected because it collides with PostgreSQL's `initdb` (operators reading "pgsafe init" would expect cluster initialization). Nesting under `server` reads idiomatically (`docker container add`, `kubectl create cluster`).
- **Reversal:** in `cmd/pgsafe/root.go`, revert `newServerCmd()` + `newServerAddCmd()` to `newStanzaCreateCmd()` with `Use: "stanza-create"`.

### D-003 — `storage` → `Backend`

- **pgBackRest:** the storage location is uniformly called the *storage* / *repo*.
- **pgSafe:** *storage backend*. The Go interface is `storage.Backend`; concrete drivers are `posix.Backend`, `s3.Backend`, etc. The YAML key is `storage:` (single) or `storages:` (list).
- **Reason:** "repo" in the programmer ear primarily means "git repo"; "storage backend" matches industry vocabulary (AWS, Velero, kubectl). `storage.Storage` would have stuttered in Go; `Backend` reads as "a storage backend" without that collision.
- **Reversal:** atomic project-wide rename via `gofmt -r 'storage.Backend -> storage.Storage'` plus parallel `gofmt -r '<pkg>.Backend -> <pkg>.Storage'` for every driver package; rename `StorageConfig` → `RepoConfig`; update YAML keys + sidecar field + JSON keys + error strings.

### D-005 — incremental backup-ID format

- **pgBackRest:** flat ID `<timestamp>F` (full), `<timestamp>D` (differential), or `<timestamp>I` (incremental). Parent linkage lives in the manifest's `backup-prior` field.
- **pgSafe:** `<UTC-timestamp>F` for full and `<UTC-timestamp>I` for incremental — same flat shape. Parent linkage lives in the sidecar's `ParentBackupID` field.
- **Reason:** keeping IDs flat allows `aws s3 ls`-style chronological browsing without sidecar reads.

### D-006 — system SSH (`os/exec /usr/bin/ssh`)

- **pgBackRest:** TLS-served custom protocol for cross-host control plane.
- **pgSafe:** `/usr/bin/ssh` subprocess via `os/exec`; operator's existing OpenSSH config governs everything (`~/.ssh/config`, `known_hosts`, agent, `ProxyJump`, `ControlMaster`).
- **Reason:** zero new code paths to audit, no TLS cert lifecycle to manage, `ssh-agent` and `ProxyJump` work out of the box.
- **Reversal:** a `golang.org/x/crypto/ssh`-backed implementation would replace `internal/transport/ssh`'s `os/exec` body; the public `Session` shape stays identical.

### D-007 — JSON-RPC over stdio

- **pgBackRest:** custom binary wire protocol.
- **pgSafe:** `net/rpc/jsonrpc` (stdlib).
- **Reason:** stdlib, plain Go interfaces as schema, human-readable for debugging via `socat -v`.
- **Reversal:** swap to gRPC; method signatures already line up (Capitalized, two-arg, second is pointer to result).

### D-008 — Tenet-3 scoped credentials

- **pgBackRest:** credentials live in the config file on every host that participates.
- **pgSafe:** the caller mints scoped, short-lived credentials per backup; they live in worker process memory and never touch the PG host's disk. PG-host compromise yields write-only, prefix-bounded, ~1-hour-expiring tokens.
- **Reversal:** replace the `Credential` payload in `Configure` with the operator's long-lived static keys read from config; same field, different content.

### D-009 — WAL archive layout

- **pgBackRest:** `archive/<stanza>/<version>-N/<segment-hi>/<segment>`.
- **pgSafe:** `<storage-root>/wal/<timeline>/<segment>`.
- **Reason:** operators tail-monitor `<storage-root>/wal/<current-timeline>/` for archive lag; the layout matches what `pg_basebackup --waldir` produces locally.
- **Reversal:** rename in `internal/wal/archive`'s `Push` / `Get`; backup-IDs and manifests don't reference WAL paths directly so the change is contained.

### D-010 — `archive_command` shell-out (not `archive_library`)

- **pgBackRest:** PG 15+ `archive_library` (a `.so` loaded into the PG backend process).
- **pgSafe:** `archive_command` shell-out only.
- **Reason:** Tenet 1 (no in-process PG code) — a `.so` running inside the PG backend can crash, leak, or block PG checkpoints. The 1–2 ms `os/exec` overhead per WAL segment is invisible against 16 MiB segment latency to any object store.
- **Reversal:** implement an `archive_library` shim that CGO-calls into Go's archive-push code; trades Tenet 1 for archive-command-spawn overhead and is rejected for v1.

### D-011 — unified retention fields

- **pgBackRest:** `--repo-retention-full`, `--repo-retention-diff`, `--repo-retention-archive`.
- **pgSafe:** `keep_fulls`, `keep_full_age`, `keep_daily/weekly/monthly` — one coherent policy.
- **Reason:** pgsafe has no "differential" axis (full + incremental chain is the only structure); the three pgBackRest levers collapse.
- **Reversal:** rename fields in `internal/retention.Policy`; evaluator logic unchanged.

### D-012 — `storages:` list

- **pgBackRest:** flat numeric suffixes (`repo1-*`, `repo2-*`).
- **pgSafe:** YAML-idiomatic list, per-storage type+config nested.
- **Reversal:** config-loader flattens list to numbered keys; internal code unchanged.

### D-013 — annotation in `Storage-Metadata.json` sidecar

- **pgBackRest:** separate per-stanza notes file.
- **pgSafe:** the per-backup sidecar carries an `Annotation` field, so the annotation travels with the backup across restore and move.
- **Reversal:** separate `Storage-Annotations.json` at server root.

### D-014 — `check --json` vs `info --json`

- **pgBackRest:** mixes monitoring + backup metadata into `info --output=json`.
- **pgSafe:** `check --json` is the operator-monitoring contract; `info --json` is backup metadata.
- **Reason:** splitting lets monitoring dashboards poll `check` without parsing backup metadata.
- **Reversal:** `--full-status` flag combining both.

### D-015 — `flock(2)` for per-server locking

- **pgBackRest:** PID-text lockfile (survives hard kill until manually cleaned).
- **pgSafe:** `flock(2)` everywhere — the kernel auto-releases on process death.
- **Reversal:** reintroduce `internal/lock/cloud.go` with heartbeat+TTL+CAS for distributed-caller scenarios.

### D-016 — Shared/Exclusive lock mode split

- **pgBackRest:** treats the lock as binary.
- **pgSafe:** `Shared` mode lets `info`/`verify`/`check` run concurrently without blocking each other, while still serializing against `Exclusive` mutators (`backup`, `prune`).
- **Reversal:** collapse modes; every command takes `Exclusive`.

### D-017 — `pgsafe prune` (not `expire`)

- **pgBackRest:** `pgbackrest expire`.
- **pgSafe:** `pgsafe prune` — the verb used by git, restic, borg, kopia.
- **Reason:** "prune" is the unambiguous action verb that operators from neighbouring tools recognise on first read.
- **Reversal:** rename cobra `Use:` field and CLI docs; `internal/retention` evaluator is name-agnostic.

### D-018 — same-host worker via `exec.Cmd`

- **pgBackRest:** inlines the local worker as goroutines in the caller process.
- **pgSafe:** same-host worker is still a separate process spawned via `exec.Cmd` (`internal/transport/local`).
- **Reason:** uniform isolation boundary regardless of locality; the worker is the worker, whether local or remote.
- **Reversal:** collapse `local` into an in-process `WorkerService` call.

### D-019 — N goroutines over ONE `net/rpc` connection

- **pgBackRest:** N worker processes over N connections.
- **pgSafe:** N goroutines over a single `net/rpc` connection. `net/rpc.Client` multiplexes parallel calls by sequence number; `net/rpc.Server` dispatches one goroutine per call. `errgroup.SetLimit(workers)` caps in-flight calls.
- **Reason:** hardened production sshd configs throttle simultaneous connections aggressively (default `MaxStartups 10:30:100`). One SSH subprocess per backup keeps pgsafe portable to those environments. `MaxSessions=1` is sufficient.
- **Reversal:** spawn N `transport.Session`s and partition the file list.

### D-020 — `WALSource` enum (archive_mode not mandatory)

- **pgBackRest:** demands `archive_mode=on` + a working `archive_command` before it will take a backup.
- **pgSafe:** when the operator has no archive plumbing — dev/test snapshots, ad-hoc remote backups, airgapped one-shot copies — `--wal-source=stream` (PG-native) or `--wal-source=walgrab` (pgSafe-mode) gets the bracket WAL inline through the same connection that took the backup, and the Invariant #5 archive-reachability probe is skipped.
- **Reversal:** re-enable the probe unconditionally and refuse `--wal-source=stream/walgrab`.

### D-020a — config-less invocation (`--no-config` parity)

- pgsafe accepts every config field as a CLI flag, so you can run a full backup without any YAML on disk. POSIX storage only in config-less mode; cloud backends carry too many auth knobs to fit cleanly on the CLI, and their auth chains are the value proposition of a YAML config.
- **Reversal:** re-require `--config`; collapse `resolveConfigFromFlags` into the YAML-only path.

### D-021 — resume of an interrupted backup

See [Resume protocol](#resume-protocol) above for the design. Three deliberate divergences from pgBackRest:

- **Filter-chain produces both digests in one pass.** `filter.Result` carries plaintext SHA256 AND on-the-wire SHA256. pgBackRest stores them as separate `pgFileChecksum` and `repoFileChecksum` fields in its manifest; pgsafe's `manifest.Builder` does the same via `AddFile` + `SetLatestRepoChecksum`.
- **No wire extension to the worker.** The caller drives the per-file `StreamFile` loop in pgSafe-mode and simply doesn't call `StreamFile` for reusable paths. pgBackRest does similar — resume info travels in the manifest, not via RPC.
- **Auto-prune folded into discovery.** Stale `.copy`-only directories (older than `backup.resume_grace_period`, default 24h) are reaped during the same walk that picks the resume target. pgBackRest has [`expire`](https://github.com/pgbackrest/pgbackrest/blob/main/src/command/expire/expire.c) for retention but doesn't fold abandoned-resume cleanup the same way.

**Reversal:** set `backup.resume: false` in defaults — discovery becomes a no-op and existing backups behave as pre-resume pgsafe.
