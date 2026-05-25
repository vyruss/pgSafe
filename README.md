# pgSafe

> [!WARNING]
> **Alpha-maturity software. Do not back up production data with it.**
>
> CLI, config, and storage format may change between commits; backups taken with one version may not restore with the next.

A modern PostgreSQL backup tool, written from scratch in idiomatic Go as a spiritual successor to pgBackRest. pgSafe shares its concepts and operational rules (a ten-invariant rulebook drawn from a decade of production-hardened lessons), and none of its code. The goal is functional parity for the common deployment patterns: full and incremental backups, point-in-time recovery, and five storage backends (POSIX, S3, Azure Blob, GCS, SFTP) across PostgreSQL 13 through 18.

**Status: v0.1.0 is in development.**

## Building

Requires [Go 1.25](https://go.dev/dl/) or newer. With `GOTOOLCHAIN=auto` (the default), `go build` downloads the right toolchain on demand from the version pinned in `go.mod`.

```sh
make build       # single linux/amd64 binary into bin/pgsafe
make release     # all four release targets into dist/
```

The release matrix is `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, statically linked under `CGO_ENABLED=0` with `-ldflags='-s -w'`.

## CLI synopsis

```
pgsafe [--config <path>] [--server <name>] [--log-level <level>] [--log-format <fmt>] <command>
```

**Top-level flags** (precedence is CLI > YAML > built-in default):

| Flag | Purpose | Default |
|---|---|---|
| `--config <path>` | YAML config file (or use config-less flags below) | — |
| `--server <name>` | overrides `server` from YAML | YAML value |
| `--log-level` | `debug`, `info`, `warn`, `error` | `info` |
| `--log-format` | `json` or `text` | `json` |

**Subcommands**:

- `pgsafe server add` — initialize a server's storage (creates directory tree and writes `Storage-Metadata.json`). Refuses to clobber an existing storage.
- `pgsafe server list` / `upgrade` / `delete` — server lifecycle.
- `pgsafe backup --type <full|incr>` — take a base backup. `--type=incr` requires `--parent <backup-id>` and PG 17+ (uses the WAL summarizer). Modes are inferred from the config; `--workers N` controls parallelism. Add `--standalone` for self-contained backups that don't require `archive_command`.
- `pgsafe restore --target <dir>` — restore a backup. Supports parallel file copy (`--workers`), tablespace remap (`--tablespace OID=PATH`, repeatable), PITR (`--target-time` / `--target-xid` / `--target-lsn` / `--target-name`, mutually exclusive, with `--target-action pause|promote|shutdown`), incremental chain combination via `pg_combinebackup`, and standby-signal generation (`--standby`).
- `pgsafe info [--json]` — list backups in the storage.
- `pgsafe verify [--backup-id <id>]` — re-hash stored files against the manifest; exit code 5 on mismatch.
- `pgsafe prune [--dry-run]` — apply retention rules and delete expirable backups + WAL.
- `pgsafe check [--json]` — operator-diagnosis battery (storage reachable, sidecars decodable, chain integrity, WAL coverage, archive_command, standby coordination).
- `pgsafe annotate <backup-id> --note "..."` — annotate a backup with operator notes carried in the sidecar.
- `pgsafe archive-push <segment-path>` — WAL push, intended for PostgreSQL's `archive_command`.
- `pgsafe archive-get <segment-name> <dest-path>` — WAL fetch, intended for `restore_command`.
- `pgsafe worker stdio` — JSON-RPC worker over stdin/stdout, used by the same-host/SSH worker mode (not invoked directly by operators).

Exit codes: `0` success, `1` generic failure, `2` usage/config error, `3` PG-side, `4` storage-side, `5` invariant violation, `8` retention violates chain integrity, `130` signal.

## Configuration schema

YAML shape (every top-level key is required unless explicitly defaulted; the `storage` block has one sub-section per supported backend, of which exactly one must be populated to match `storage.type`):

```yaml
server: demo                                   # name of this PG server (Barman-style)
pg:
  conn_string: "host=... port=... user=... dbname=..."  # libpq URI or DSN
  version: 18                                  # PG major version (13..18)
storage:
  type: posix                                  # posix | s3 | azure | gcs | sftp
  path: /var/lib/pgsafe/store                  # absolute path; required for type=posix
compression:
  codec: zstd                                  # gzip | lz4 | zstd | bzip2
  level: 3                                     # codec-specific
encryption:
  recipients:                                  # one or more age public keys
    - "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
log:
  format: json                                 # json | text (default: json)
  level: info                                  # debug|info|warn|error (default: info)
```

Unknown YAML keys are rejected (`yaml.Decoder.KnownFields(true)`); the file is parsed strictly so that typos surface at config-load time, not mid-backup.

### Storage-backend sub-configurations

Use the sub-section that matches `storage.type`:

```yaml
# S3 (or any S3-compatible store: MinIO, R2, B2)
storage:
  type: s3
  s3:
    bucket: pgsafe-prod
    region: us-east-1
    endpoint: https://s3.amazonaws.com         # optional; required for non-AWS
    use_path_style: false                      # true for MinIO
    access_key_id: AKIA...                     # empty → AWS default credential chain
    secret_access_key: "..."
    prefix: prod/                              # optional key prefix

# Azure Blob Storage
storage:
  type: azure
  azure:
    account_name: pgsafeprod
    container: backups
    account_key: "..."                         # OR sas_token: ...  OR connection_string: ...
    blob_endpoint: ""                          # optional; for Azurite/government cloud
    prefix: ""

# Google Cloud Storage
storage:
  type: gcs
  gcs:
    bucket: pgsafe-prod
    credentials_file: /etc/pgsafe/gcs.json     # empty → Application Default Credentials
    prefix: ""

# SFTP (over SSH)
storage:
  type: sftp
  sftp:
    host: backup.example.com
    port: 22
    username: pgsafe
    password: ""                               # OR private_key_file: /path/to/id_ed25519
    base_path: /srv/pgsafe
    host_key: "ssh-ed25519 AAAA..."            # required unless insecure_ignore_host_key: true
```

## Operating pgSafe

### Initialise a server

```sh
pgsafe server add --config pgsafe.yml
```

Creates `Storage-Metadata.json` in the storage root. Refuses to overwrite an existing storage — use `server upgrade` to change compression or recipients.

### Take a backup

#### Config-less invocation (no YAML required)

```sh
# Bare flags — no YAML required. Useful for ad-hoc backups,
# scripted runs, container entrypoints. Add --standalone to skip the
# archive_command requirement entirely.
pgsafe backup \
    --server demo \
    --pg-conn-string "postgres://user@host/db" \
    --pg-version 18 \
    --storage-path /var/lib/pgsafe/storage \
    --encryption-recipient age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p \
    --standalone --type full
```

POSIX storage only in config-less mode; cloud backends carry too many auth knobs to fit cleanly on the CLI and require `--config <yaml>`.

#### Config-driven (recommended for production)

```sh
# Full backup — assumes archive_mode=on + pgsafe archive-push wired in (default).
pgsafe backup --config pgsafe.yml --type full

# Self-contained backup — no archive_command / archive_mode required.
# Bracket WAL is packed inline (PG-native: --wal-method=fetch; pgSafe-mode:
# worker reads $PGDATA/pg_wal directly). PITR reach is the bracket window only.
pgsafe backup --config pgsafe.yml --type full --standalone

# Explicit WAL-source pick (advanced):
pgsafe backup --config pgsafe.yml --wal-source=archive   # default, archive-tied
pgsafe backup --config pgsafe.yml --wal-source=stream    # PG-native simple, inline WAL
pgsafe backup --config pgsafe.yml --wal-source=walgrab   # pgSafe-mode worker, inline WAL

# Incremental (PG 17+, chained to a specific parent)
pgsafe backup --config pgsafe.yml --type incr --parent 20260428T120000F
```

### List backups

```sh
pgsafe info --config pgsafe.yml           # tabular
pgsafe info --config pgsafe.yml --json    # JSON (scriptable)
```

### Verify backup integrity

Re-hashes every stored file against the manifest SHA-256 and re-verifies the manifest checksum:

```sh
pgsafe verify --config pgsafe.yml                        # all backups
pgsafe verify --config pgsafe.yml --backup-id 20260428T120000F
```

Exit code 5 if any mismatch is found.

### Prune old backups

```yaml
# Retention policy in pgsafe.yml
retention:
  keep_fulls: 4           # keep the 4 most recent full-backup chains
  keep_full_age: 30d      # OR keep everything newer than 30 days
  keep_daily: 7           # daily slot: one full per day for 7 days
  keep_weekly: 4          # weekly slot
  keep_monthly: 12        # monthly slot
```

```sh
pgsafe prune --config pgsafe.yml --dry-run   # preview
pgsafe prune --config pgsafe.yml             # execute
```

Prune also removes WAL segments older than the oldest surviving backup's start LSN. Exit code 8 if the policy would violate chain integrity.

### Operator health check

```sh
pgsafe check --config pgsafe.yml           # text
pgsafe check --config pgsafe.yml --json    # JSON for monitoring integration
```

Runs six probes in order: storage reachable, sidecars decodable, chain integrity, WAL coverage, archive_command, standby coordination. Exit code 5 on any FAIL.

### Annotate a backup

```sh
pgsafe annotate --config pgsafe.yml 20260428T120000F --note "pre-upgrade snapshot"
```

### Multi-storage configuration

Two backends, one local and one cloud offsite. The filter chain (compress + encrypt) runs **once**; the same byte stream is written to both backends in parallel (`internal/storage/multi.TeeWriter`). CPU cost is flat regardless of backend count. The backup is durable when at least one backend commits the manifest (Invariant #10).

```yaml
server: prod-db
pg:
  conn_string: "host=localhost port=5432 user=pgsafe dbname=postgres sslmode=prefer"
  version: 18

storages:
  - type: posix
    path: /var/lib/pgsafe/local
  - type: s3
    s3:
      bucket: pgsafe-prod-offsite
      region: us-east-1

compression: { codec: zstd, level: 3 }
encryption:
  recipients:
    - "age1..."
```

Single-storage operators use the legacy `storage:` form (still accepted, silently promoted to a one-element list internally):

```yaml
storage:
  type: posix
  path: /var/lib/pgsafe/local
```

### `pgsafe check` runbook

| Probe | RED means | Fix |
|---|---|---|
| `storage_reachable` | backend unreachable | Check path/bucket/credentials |
| `info_decodable` | sidecar corrupt | `pgsafe server upgrade` or restore from another storage |
| `chain_integrity` | orphaned incremental | Run `pgsafe prune` or `pgsafe annotate` to record a note |
| `wal_expected` | WAL segment gap | Check `archive_command`; run `pgsafe archive-push` manually |
| `archive_command` | probe write failed | Verify `pgsafe archive-push` invocation in `postgresql.conf` |
| `standby_coordination` | WAL receiver stalled | Check streaming replication; primary must be reachable |

### Known operational gotchas

A handful of behaviors are worth knowing about before they bite you in production.

- **S3 conditional-write availability.** S3's `If-None-Match: *` (required by Invariant #6.5 for atomic-rename on object stores) reached general availability in August 2024. Older S3-compat backends and some MinIO versions return `NotImplemented`. pgSafe probes for it at `Open()` and fails fast with an operator-actionable error if the response is wrong; bring the backend up to date or move to a different region.
- **Page-checksum mode interacts with `data_checksums`.** Remote-parallel mode validates PG heap pages against PG's FNV-1a checksum before bytes hit the filter chain. Clusters that have `data_checksums=off` (the historical PG default, before checksums were cheap) get a one-time WARN at backup start; validation is skipped (`ModeOff`) because there's nothing on the page to check. Set `pagechecksum: strict` only when you've confirmed the cluster has checksums enabled cluster-wide.
- **Worker count vs PG `max_connections`.** In remote-parallel and worker modes, each backup worker holds its own libpq connection. pgSafe queries `current_setting('max_connections')` at start and warns if `workers + 5 > max_connections`. The "+5" is headroom for the bracket session, the identity probe, and any concurrent operator queries.
- **Cloud SSE + filter-chain encryption are independent.** Bucket-level SSE (S3 SSE-KMS, Azure encrypt-at-rest, GCS CMEK) protects bytes at rest in the bucket; pgSafe's age encryption protects bytes before they leave the backup host. Double-encryption is fine — there is no security regression — but the age recipients are still the only audience that can decrypt the actual content. Losing every age private key loses every backup, regardless of bucket SSE.
- **`pg_combinebackup` stage-space at restore.** Restoring an incremental chain shells out to PostgreSQL's `pg_combinebackup`, which requires the full chain to be decrypted and staged on disk before the merge runs. Required free space ≈ sum of decompressed sizes of the full plus every incremental in the chain. The stage directory lives at a sibling of `--target`; if the target's filesystem can't hold both the stage and the final cluster, restore fails partway through.

## Contributing

Every change runs the full TDD stack (unit + integration + E2E). Before pushing, **`./run-ci-local.sh` must exit 0** — this is the canonical pre-commit gate, identical step-for-step to GitHub Actions CI.

```sh
./run-ci-local.sh
```

The script does, in order:

1. `gofmt -l` — formatting must be clean.
2. `goimports -l` (via `go tool goimports`).
3. `golangci-lint run ./...` (via `go tool golangci-lint`).
4. `go vet ./...`.
5. `go test -race -short ./...` — unit tests.
6. Build all four release targets.
7. `go test -race -tags=integration ./...` (integration tests; spin up real PG via `testcontainers-go`).
8. `go test -race -tags=faults ./test/faults/...` (fault-injection tests for the rulebook invariants).
9. `go test -race -tags=e2e ./test/e2e/...` (E2E tests).
10. Stale `ci-*.log` purge.

Set `PGSAFE_SKIP_INTEGRATION=1` or `PGSAFE_SKIP_E2E=1` to skip steps 7 / 8 locally when iterating on unit tests; CI never skips.

## Design references

- [INVARIANTS.md](INVARIANTS.md) — the operational rulebook (the ten correctness invariants every backup must hold).
- [ARCHITECTURE.md](ARCHITECTURE.md) — design decisions and module layering, updated as the codebase grows.
- [FUNCTIONALITY_GAPS.md](FUNCTIONALITY_GAPS.md) — features not yet implemented, plus deliberate design omissions and their reversal paths.

## Licence

See [LICENCE.md](LICENCE.md).
