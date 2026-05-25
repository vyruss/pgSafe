// Package backup is the simple-mode backup caller. The exported
// surface is Options + Run; the per-step machinery (WAL-wait, manifest
// builder integration, backup-label parser) lives alongside in private
// helpers so tests can drive each piece in isolation.
//
// Step ordering follows Invariant #1 (the hardened-semantics rulebook):
//
//  0. (stubbed) probe the WAL archive — wires this when archive-push
//     ships.
//  1. open the BASE_BACKUP stream (which internally calls pg_backup_start
//     server-side).
//  2. for each tar entry: filter chain → storage. Capture plaintext SHA-256.
//  3. close every storage writer (durability point on POSIX = fsync ordering).
//  4. force a WAL switch + read pg_current_wal_insert_lsn() → stop LSN.
//  5. WAL-wait: poll <storage>/wal/ for the segment(s) containing start..stop.
//  6. assemble the PG-native backup_manifest + Storage-Metadata sidecar.
//  7. atomic-rename backup_manifest.tmp → backup_manifest (Invariant #2).
//
// Cancellation: ctx propagates to the BASE_BACKUP subprocess, storage Put/Commit,
// and WAL-wait. A cancelled context leaves the storage in an Invariant-#2-valid
// state (at most a *.tmp file, never a malformed final manifest).
package backup

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/pg"
	"github.com/vyruss/pgsafe/internal/pg/basebackup"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/storage/multi"
)

// Sentinel errors classify operator-actionable backup failures. The
// CLI uses errors.Is to map these into stable exit codes — every new
// failure category should add a sentinel here rather than rely on
// substring-matching the error message (which is fragile to wording
// or localization changes).
var (
	// ErrWALWait is returned when the WAL-archive probe or the
	// post-bracket WAL-wait times out. Maps to exit code 5.
	ErrWALWait = errors.New("backup: WAL-wait")

	// ErrPGProtocol is returned for failures rooted in PG-side
	// protocol calls (pg_basebackup subprocess, pg_switch_wal,
	// pg_backup_start/stop, stop-LSN parsing). Maps to exit code 3.
	ErrPGProtocol = errors.New("backup: PG protocol")

	// ErrStorage is returned for storage-backend failures that aren't
	// transient (manifest write, atomic-rename, fsync, open). Maps to
	// exit code 4.
	ErrStorage = errors.New("backup: storage")
)

// FormatBackupID renders the canonical pgsafe backup-ID for a start
// timestamp + backup type. Format: `<YYYYMMDD>T<HHMMSS><F|I>` —
// second resolution, mirrors pgbackrest's `backupLabelFormat`
// (pgbackrest uses `-` between date and time; pgsafe uses `T` for
// ISO 8601 basic-format consistency, but the resolution is identical).
//
// Collision avoidance for "two backups starting in the same wall-
// clock second" is the caller's responsibility — see ChooseBackupID,
// which adds 1s and re-checks against the most-recent existing label,
// matching pgbackrest's algorithm in
// command/backup/backup.c:backupLabelCreate.
func FormatBackupID(startedAt time.Time, t Type) string {
	suffix := "F"
	if t == TypeIncremental {
		suffix = "I"
	}
	return startedAt.UTC().Format("20060102T150405") + suffix
}

// ChooseBackupID picks a unique backup label for `now`, querying the
// storage for the most recent existing label and adding 1s if the
// formatted timestamp would collide. Matches pgbackrest's
// backupLabelCreate (command/backup/backup.c:46): generate, compare,
// +1s on collision, error on "still <= latest" with a clock-skew
// hint.
//
// Concurrent same-host backups are prevented by internal/lock; this
// algorithm guards the cross-host case where two operators share a
// storage and start within the same second. It is NOT a substitute for
// the per-server lock — without the lock, two concurrent ChooseBackupID
// calls on different hosts could race past each other; with it, only
// the wall-clock-collision case matters.
func ChooseBackupID(ctx context.Context, b storage.Backend, t Type, now time.Time) (string, error) {
	candidate := FormatBackupID(now, t)
	latest, err := latestStorageLabel(ctx, b)
	if err != nil {
		return "", err
	}
	if latest != "" && candidate <= latest {
		candidate = FormatBackupID(now.Add(time.Second), t)
		if candidate <= latest {
			return "", fmt.Errorf("backup: new label %q is not later than latest %q (clock skew or timezone change?)",
				candidate, latest)
		}
	}
	return candidate, nil
}

// latestStorageLabel returns the lexicographically largest backup ID
// already in the storage (any type), or "" when the storage has no
// backups yet. Identifies a backup by the existence of
// "<id>/Storage-Metadata.json" — that's pgsafe's per-backup
// sidecar; whether `backup_manifest` has been atomically committed
// doesn't matter for collision purposes (an in-progress label still
// claims the timestamp slot).
func latestStorageLabel(ctx context.Context, b storage.Backend) (string, error) {
	infos, err := b.List(ctx, "")
	if err != nil {
		return "", fmt.Errorf("backup: list storage for label collision check: %w", err)
	}
	var labels []string
	for _, fi := range infos {
		if path.Base(fi.Path) != "Storage-Metadata.json" {
			continue
		}
		dir := path.Dir(fi.Path)
		// Skip the storage-root sidecar (path = "Storage-Metadata.json"
		// → dir = "."), keep only per-backup sidecars whose dir is the
		// backup ID.
		if dir == "" || dir == "." {
			continue
		}
		labels = append(labels, dir)
	}
	if len(labels) == 0 {
		return "", nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(labels)))
	return labels[0], nil
}

// Mode discriminates the backup driver. Empty defaults to ModeSimple.
type Mode string

// Backup-driver mode constants.
const (
	// ModeSimple shells out to pg_basebackup over libpq from the caller.
	ModeSimple = "simple"
)

// Type discriminates full vs. incremental backups. Empty defaults
// to TypeFull for backwards compatibility with callers.
type Type string

// Backup-type constants written into the backup-id suffix and the manifest.
const (
	// TypeFull is a self-contained physical backup (suffix "F").
	TypeFull Type = "full"
	// TypeIncremental is a backup chained against a parent's manifest (suffix "I").
	TypeIncremental Type = "incremental"
)

// StopLSNFunc returns the post-basebackup stop LSN. Production implementations
// run SELECT pg_switch_wal() against a real pool; tests inject a fake.
type StopLSNFunc func(ctx context.Context) (manifest.LSN, error)

// NewPoolStopLSNFunc adapts a pgxpool.Pool into the StopLSNFunc seam.
func NewPoolStopLSNFunc(pool *pgxpool.Pool) StopLSNFunc {
	return func(ctx context.Context) (manifest.LSN, error) {
		return switchAndCurrentLSN(ctx, pool)
	}
}

// Options configure one backup invocation.
type Options struct {
	Cluster pg.Cluster
	Backend storage.Backend
	// Backends is the multi-storage list (Invariant #10). When non-empty,
	// runSimple fans file writes to all backends via multi.TeeWriter and
	// commits the manifest to each backend independently. The backup is
	// "complete" when at least one backend commits successfully.
	// If Backends is empty, Backend is used (single-storage path, unchanged).
	Backends   []storage.Backend
	Filter     *filter.Chain
	StopLSN    StopLSNFunc
	Mode       Mode
	Server     string
	Label      string
	DSN        string        // libpq URI for pg_basebackup subprocess
	WALTimeout time.Duration // bound for the post-stop WAL-wait
	WALSource  WALSource     // archive/stream/walgrab; empty = archive (current default)

	// ResumeDisabled, when true, skips the in-progress
	// backup_manifest.copy checkpoint AND skips the resumable-
	// backup discovery step on this run. Operators force a fresh
	// backup via --no-resume; default behavior is "resume when a
	// compatible candidate exists".
	ResumeDisabled bool

	// ResumeCheckpointEveryN sets the per-file checkpoint cadence
	// for backup_manifest.copy. Lower = finer resume granularity
	// + more storage Puts; higher = coarser + fewer Puts.
	// Zero falls back to DefaultResumeCheckpointEveryN.
	ResumeCheckpointEveryN int

	// ResumeGracePeriod is the maximum age of a backup_manifest.copy
	// before findResumable treats it as abandoned and reaps the
	// entire backup-id directory at the next backup-start. Zero
	// disables auto-pruning (resume still works on stale candidates).
	// Mirrors pgbackrest's --repo-retention-archive-time semantics
	// for resumable artifacts.
	ResumeGracePeriod time.Duration

	// PgsafeVersion is the binary version recorded in the resume
	// checkpoint (and refused on cross-version resume). Wired by
	// the CLI from the cmd/pgsafe build-time version string.
	PgsafeVersion string

	Recipients  []string  // age public keys (passed through to sidecar)
	Compression string    // codec:level string for the sidecar
	ScratchDir  string    // local FS scratch for staging files; "" → os.TempDir()
	Stderr      io.Writer // caller log sink; nil → os.Stderr (tests inject a bytes.Buffer to assert)
	Now         func() time.Time

	// RemoteParallel is required when Mode == ModeRemoteParallel and
	// ignored otherwise. Holds the pgxpool.Pool the workers share,
	// the worker count, and page-checksum mode.
	RemoteParallel RemoteParallelOptions

	// Worker is required when Mode == ModeWorker and ignored otherwise.
	// Holds the SSH target, scoped-cred config, and the caller-side
	// pool used for bracket only.
	Worker WorkerOptions

	// Pool is the caller-side pgxpool.Pool used for the
	// Invariant #5 reachability probe. When non-nil, the caller
	// runs `pg_switch_wal()` and waits for the resulting segment to land
	// in WALDir before pg_backup_start. Tests with a mocked pg.Cluster
	// can leave this nil to skip the probe.
	Pool *pgxpool.Pool

	// Type selects full vs. incremental. Empty == TypeFull.
	Type Type

	// ParentBackupID identifies the parent in the incremental chain.
	// Required when Type == TypeIncremental; ignored otherwise. The
	// caller reads <parent>/backup_manifest from the storage and stages it
	// for pg_basebackup --incremental=<path>.
	ParentBackupID string
}

// Result describes a finished backup.
type Result struct {
	BackupID string
	StartLSN manifest.LSN
	StopLSN  manifest.LSN
	Timeline uint32
	Files    int
	Bytes    int64
	Duration time.Duration
	// PartialStorages is the count of backends that failed to commit the manifest
	// in a multi-storage run. Zero in single-storage mode. A non-zero value with a
	// nil error means the backup is durable on (TotalStorages - PartialStorages)
	// backends but failed on PartialStorages (Invariant #10).
	PartialStorages int
}

// Run executes one backup. The caller is responsible for
// constructing the Cluster and Storage and ensuring they're Open()-ed.
func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.Mode == "" {
		opts.Mode = ModeSimple
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.WALTimeout == 0 {
		opts.WALTimeout = 30 * time.Minute
	}
	// Normalize: if Backends is non-empty, Backend must also be set to the
	// primary (for WAL-wait, parent-manifest staging, etc.). Callers that only
	// set Backends must ensure Backends[0] is also Backend. If Backends is
	// empty we fall through to the existing single-backend path.
	if len(opts.Backends) > 0 && opts.Backend == nil {
		opts.Backend = opts.Backends[0]
	}
	if opts.Cluster == nil || opts.Backend == nil || opts.Filter == nil {
		return Result{}, errors.New("backup: Cluster, Storage, Filter required")
	}
	// WAL-source / mode compat. stream needs pg_basebackup
	// --wal-method=fetch which only the simple-mode pipeline drives;
	// walgrab needs a pgSafe-mode worker with $PGDATA access. Reject
	// upfront — a silent fall-through to "WAL not where restore
	// expects it" is the worst outcome.
	switch opts.WALSource {
	case "", WALSourceArchive:
		// Always OK.
	case WALSourceStream:
		if opts.Mode != ModeSimple {
			return Result{}, fmt.Errorf("backup: WALSource=stream requires Mode=simple (got %q)", opts.Mode)
		}
	case WALSourceWalgrab:
		if opts.Mode != ModeWorker {
			return Result{}, fmt.Errorf("backup: WALSource=walgrab requires Mode=worker (got %q)", opts.Mode)
		}
	default:
		return Result{}, fmt.Errorf("backup: unknown WALSource %q", opts.WALSource)
	}
	// Pre-flight free-space hint (POSIX-only — cloud backends have
	// effectively unbounded space). Best-effort: a low-disk warning
	// goes to stderr; we don't fail the backup here because the
	// caller doesn't know in advance how big the cluster is.
	// Real out-of-space lands as a backend Put error mid-backup.
	if pb, ok := opts.Backend.(interface{ FreeBytes() (uint64, error) }); ok {
		if avail, err := pb.FreeBytes(); err == nil && avail < 1<<30 {
			_, _ = fmt.Fprintf(stderrFor(opts),
				"pgsafe backup: WARNING: storage has only %.2f GiB free; backup may run out of space.\n",
				float64(avail)/float64(1<<30))
		}
	}
	switch opts.Mode {
	case ModeSimple:
		return runSimple(ctx, opts)
	case ModeRemoteParallel:
		return runRemoteParallel(ctx, opts, opts.RemoteParallel)
	case ModeWorker:
		return runWorkerBackup(ctx, opts, opts.Worker)
	default:
		return Result{}, fmt.Errorf("backup: mode %q not implemented", opts.Mode)
	}
}

// multiState tracks per-backend liveness across a multi-storage backup. Dead
// backends are skipped on subsequent Put calls; only alive backends receive
// the manifest Commit. This keeps CPU cost flat (filter chain runs once via
// TeeWriter) while isolating backend failures — Invariant #10.
type multiState struct {
	backends []storage.Backend
	dead     []bool
}

func newMultiState(backends []storage.Backend) *multiState {
	return &multiState{
		backends: backends,
		dead:     make([]bool, len(backends)),
	}
}

// putMulti opens Put on all alive backends and returns a TeeWriter + index
// mapping so the caller can merge Results back to the global dead list.
func (ms *multiState) putMulti(ctx context.Context, relPath string) (*multi.TeeWriter, []int, error) {
	sinks := make([]io.WriteCloser, 0, len(ms.backends))
	sinkIdx := make([]int, 0, len(ms.backends))
	for i, b := range ms.backends {
		if ms.dead[i] {
			continue
		}
		wc, err := b.Put(ctx, relPath)
		if err != nil {
			ms.dead[i] = true
			continue
		}
		sinks = append(sinks, wc)
		sinkIdx = append(sinkIdx, i)
	}
	if len(sinks) == 0 {
		return nil, nil, errors.New("backup: all backends dead")
	}
	return multi.New(sinks), sinkIdx, nil
}

// merge updates the global dead list from a finished TeeWriter's Results.
func (ms *multiState) merge(results []error, sinkIdx []int) {
	for j, err := range results {
		if err != nil {
			ms.dead[sinkIdx[j]] = true
		}
	}
}

// commitMulti calls Commit on all alive backends independently. Returns the
// count of backends that committed successfully and the last error seen.
func (ms *multiState) commitMulti(ctx context.Context, tmp, final string) (int, error) {
	succeeded := 0
	var lastErr error
	for i, b := range ms.backends {
		if ms.dead[i] {
			continue
		}
		if err := b.Commit(ctx, tmp, final); err != nil {
			ms.dead[i] = true
			lastErr = err
			continue
		}
		succeeded++
	}
	return succeeded, lastErr
}

// failedCount returns the number of backends that failed (dead=true).
func (ms *multiState) failedCount() int {
	n := 0
	for _, d := range ms.dead {
		if d {
			n++
		}
	}
	return n
}

// runSimple is the simple-mode driver — §3.2.7 in code.
func runSimple(ctx context.Context, opts Options) (Result, error) {
	startedAt := opts.Now()
	backupID, err := ChooseBackupID(ctx, opts.Backend, opts.Type, startedAt)
	if err != nil {
		return Result{}, err
	}

	logf := func(format string, args ...any) {
		_, _ = fmt.Fprintf(stderrFor(opts), "pgsafe backup: "+format+"\n", args...)
	}
	logf("starting id=%s mode=%s server=%s", backupID, opts.Mode, opts.Server)

	// Invariant #5 — verify the operator's archive_command before
	// bothering PG with pg_backup_start. Skipped for inline-WAL
	// sources (stream/walgrab) — the bracket WAL doesn't go through
	// archive_command for those, so probing the archive's
	// reachability is at best wasted I/O and at worst (when no
	// archive plumbing exists) an infinite hang.
	if opts.Pool != nil && walRecordsNeeded(opts.WALSource) {
		if err := ProbeArchive(ctx, opts.Pool, opts.Backend, opts.WALTimeout); err != nil {
			return Result{}, fmt.Errorf("backup: %w", err)
		}
		logf("WAL archive reachability probe: OK")
	}

	// Identity (system identifier, WAL segment size, timeline).
	id, err := opts.Cluster.Identity(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("backup: identity: %w", err)
	}
	if err := verifyClusterIdentity(ctx, opts.Backend, id.SystemIdentifier); err != nil {
		return Result{}, err
	}

	// Invariant #8 — refuse cleanly if the cluster is a standby that's
	// disconnected from its primary's WAL stream. Skipped when no Pool
	// is configured (test paths with mocked Cluster).
	if opts.Pool != nil {
		if err := InspectStandby(ctx, opts.Pool); err != nil {
			return Result{}, fmt.Errorf("backup: %w", err)
		}
	}

	// Resume discovery (RESUME.md Step 2): if a prior attempt left a
	// backup_manifest.copy and every gate field matches the current
	// run, we resume in place using its backup-id. Per RESUME.md
	// "same-id" semantics: the new attempt overwrites whatever the
	// prior one left (writer.Close uses POSIX rename(2) which
	// silently overwrites, so re-uploading a file is a no-op of
	// the right shape). Files already on storage with a matching
	// SHA are reusable bytes-on-disk; later steps add the actual
	// "skip the upload" optimization on top of this plumbing.
	resumedFrom, _ := tryResume(ctx, opts, id.SystemIdentifier)
	var resumePlan *reusablePlan
	if resumedFrom != nil {
		logf("resuming backup id=%s from %d-file checkpoint at %s",
			resumedFrom.BackupID, len(resumedFrom.Files), resumedFrom.CheckpointedAt.Format(time.RFC3339))
		backupID = resumedFrom.BackupID
		resumePlan = cleanResumable(ctx, opts.Backend, resumedFrom, logf)
	}

	// For incrementals we stage the parent backup_manifest into a temp file
	// so pg_basebackup can read it via --incremental=<path>. The temp file
	// is removed once pg_basebackup is done with it.
	var parentManifestPath string
	if opts.Type == TypeIncremental {
		if opts.ParentBackupID == "" {
			return Result{}, errors.New("backup: incremental requires ParentBackupID")
		}
		path, cleanup, err := stageParentManifest(ctx, opts.Backend, opts.ParentBackupID, opts.ScratchDir)
		if err != nil {
			return Result{}, fmt.Errorf("backup: stage parent manifest: %w", err)
		}
		parentManifestPath = path
		defer cleanup()
		// Backup-ID convention for incrementals: <parent>_<timestamp>I
		backupID = opts.ParentBackupID + "_" + startedAt.UTC().Format("20060102T150405") + "I"
	}

	// Stream BASE_BACKUP, filter each entry into the storage at <backup-id>/<path>.
	// For WALSource=stream the worker tells pg_basebackup --wal-method=fetch
	// so the bracket WAL arrives inline as pg_wal/<seg> tar entries (no
	// separate archive needed for THIS backup to restore).
	walMethod := ""
	if opts.WALSource == WALSourceStream {
		walMethod = "fetch"
	}
	bb, err := opts.Cluster.BaseBackup(ctx, basebackup.Options{
		DSN:                     opts.DSN,
		Label:                   opts.Label,
		IncrementalManifestPath: parentManifestPath,
		WALMethod:               walMethod,
	})
	if err != nil {
		return Result{}, fmt.Errorf("backup: open BASE_BACKUP: %w", err)
	}
	defer func() { _ = bb.Close() }()

	mb := manifest.NewBuilder(manifest.BackupStartInfo{
		SystemIdentifier: id.SystemIdentifier,
		Timeline:         id.Timeline,
		StartLSN:         id.CheckpointLSN, // overwritten below from backup_label
		StartTime:        startedAt.UTC(),
	})

	// Resume checkpointer (RESUME.md Step 1). nil when --no-resume
	// or for backends where the checkpoint write is not yet wired
	// (multi-storage uses opts.Backend = backends[0] as the
	// checkpoint sink — secondary backends won't have a .copy from
	// this run; that's an acceptable limitation).
	rc := newResumeCheckpointer(opts, opts.Backend, backupID,
		startedAt, id.SystemIdentifier, id.Timeline, id.CheckpointLSN)

	var (
		fileCount  int
		bytesTotal int64
		startLSN   = id.CheckpointLSN
		timeline   = id.Timeline
		labelSeen  bool
		dirs       []string
		pgManifest []byte // canonical pg_basebackup manifest (incremental mode only)
	)

	// Multi-storage tracking (Invariant #10). When opts.Backends has more than
	// one entry, each file is tee'd to all alive backends via multi.TeeWriter.
	// ms is nil in the single-backend path (no overhead).
	var ms *multiState
	if len(opts.Backends) > 1 {
		ms = newMultiState(opts.Backends)
	}

	// 4 MiB read buffer: large enough that each filter.Chain.Write() call
	// feeds the compressor a meaningful chunk, reducing per-call overhead and
	// giving TeeWriter goroutines fat slices to work on per wakeup.
	copyBuf := make([]byte, 4<<20)

	for {
		hdr, r, err := bb.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Result{}, fmt.Errorf("backup: tar.Next: %w", err)
		}
		if hdr.Typeflag == tar.TypeDir {
			// PG's tar contains empty directory entries (pg_notify, pg_stat,
			// pg_subtrans, etc.) that the cluster needs at startup. Track
			// their relative paths in the sidecar so restore can mkdir them.
			rel := strings.TrimRight(hdr.Name, "/")
			if rel != "" && rel != "." {
				dirs = append(dirs, rel)
			}
			continue
		}
		if hdr.Typeflag != 0 && hdr.Typeflag != '0' {
			continue // symlinks etc.
		}

		// Reject malicious tar entries that try to escape the backup directory.
		// pg_basebackup never produces these; this is belt-and-braces guarding
		// against a hypothetically compromised PG.
		if strings.Contains(hdr.Name, "..") || strings.HasPrefix(hdr.Name, "/") {
			return Result{}, fmt.Errorf("backup: refusing tar entry with traversal-style path %q", hdr.Name)
		}

		// Resume reuse-skip: if the prior attempt already wrote this
		// path's bytes durably (cleanResumable verified the on-disk
		// SHA), drain the tar entry without piping it through the
		// filter chain, and inherit the prior plaintext SHA + repo
		// digest into the new manifest. The byte-savings is
		// proportional to how much of the prior attempt completed.
		if resumePlan != nil {
			if reused, ok := resumePlan.files[hdr.Name]; ok {
				if _, err := io.Copy(io.Discard, r); err != nil {
					return Result{}, fmt.Errorf("backup: drain reused %s: %w", hdr.Name, err)
				}
				mb.AddFile(hdr.Name, reused.Size, reused.SHA256, hdr.ModTime)
				mb.SetLatestRepoChecksum(reused.RepoSize, reused.RepoSHA256)
				fileCount++
				bytesTotal += reused.Size
				rc.onAddFile(ctx, mb)
				continue
			}
		}

		// In incremental mode, pg_basebackup writes a backup_manifest tar
		// entry whose format pg_combinebackup expects byte-for-byte. We
		// can't reproduce that from outside, so we capture it plaintext
		// and skip the filter chain — saved later as <backupID>/backup_manifest.
		if hdr.Name == "backup_manifest" && opts.Type == TypeIncremental {
			body, err := io.ReadAll(r)
			if err != nil {
				return Result{}, fmt.Errorf("backup: read backup_manifest: %w", err)
			}
			pgManifest = body
			continue
		}

		// Two paths through the filter chain:
		//   - regular file → encrypted+compressed into <backup-id>/<path>
		//   - backup_label → also stashed plaintext-side so we can extract
		//     start LSN without decrypt.
		var labelBuf strings.Builder
		src := r
		if hdr.Name == "backup_label" {
			src = io.TeeReader(r, &labelBuf)
			labelSeen = true
		}

		// hdr.Name is validated above to reject traversal-style paths;
		// gosec's G305 lint can't see the upstream guard.
		repoPath := filepath.ToSlash(filepath.Join(backupID, hdr.Name)) //nolint:gosec

		// Open the sink(s): single-backend uses Backend.Put directly; multi-
		// storage opens Put on every alive backend and fans output via TeeWriter.
		var chainW io.WriteCloser
		var res *filter.Result
		if ms != nil {
			tw, sinkIdx, err := ms.putMulti(ctx, repoPath)
			if err != nil {
				return Result{}, fmt.Errorf("backup: multi Put %s: %w", repoPath, err)
			}
			chainW, res, err = opts.Filter.Wrap(tw)
			if err != nil {
				return Result{}, fmt.Errorf("backup: filter.Wrap: %w", err)
			}
			if _, err := io.CopyBuffer(chainW, src, copyBuf); err != nil {
				_ = chainW.Close()
				return Result{}, fmt.Errorf("backup: copy %s: %w", hdr.Name, err)
			}
			if err := chainW.Close(); err != nil {
				// TeeWriter.Close returns nil when ≥1 backend succeeded.
				return Result{}, fmt.Errorf("backup: close %s: %w", hdr.Name, err)
			}
			ms.merge(tw.Results(), sinkIdx)
		} else {
			wc, err := opts.Backend.Put(ctx, repoPath)
			if err != nil {
				return Result{}, fmt.Errorf("backup: storage.Put %s: %w", repoPath, err)
			}
			chainW, res, err = opts.Filter.Wrap(wc)
			if err != nil {
				return Result{}, fmt.Errorf("backup: filter.Wrap: %w", err)
			}
			if _, err := io.CopyBuffer(chainW, src, copyBuf); err != nil {
				_ = chainW.Close()
				return Result{}, fmt.Errorf("backup: copy %s: %w", hdr.Name, err)
			}
			if err := chainW.Close(); err != nil {
				return Result{}, fmt.Errorf("backup: close %s: %w", hdr.Name, err)
			}
		}

		fileCount++
		bytesTotal += res.Bytes
		// PG 17+ pg_basebackup --incremental emits per-relfork files named
		// "<dir>/INCREMENTAL.<relfilenode>[_<fork>]". Mark them in the
		// manifest with the Incremental flag (the caller can't peek
		// inside the encrypted payload to count blocks here, so we record 0
		// — pg_combinebackup re-reads block counts from the file body).
		if isIncrementalFileName(hdr.Name) {
			mb.AddIncrementalFile(hdr.Name, res.Bytes, res.SHA256, nil, hdr.ModTime)
		} else {
			mb.AddFile(hdr.Name, res.Bytes, res.SHA256, hdr.ModTime)
		}
		// Resume gate: record the on-the-wire bytes count and SHA so
		// the next attempt can verify the storage file is intact
		// without needing the encryption identity.
		mb.SetLatestRepoChecksum(res.RepoBytes, res.RepoSHA256)
		rc.onAddFile(ctx, mb)

		if hdr.Name == "backup_label" {
			s, t, err := parseBackupLabel(labelBuf.String())
			if err == nil {
				startLSN = s
				timeline = t
				// Reseat the manifest's WAL-Ranges Start-LSN / timeline to
				// the backup_label's canonical START WAL LOCATION. Without
				// this, pg_combinebackup rejects the chain because the
				// manifest's recorded LSN doesn't match what
				// pg_basebackup --incremental expects.
				mb.UpdateStartLSN(s, t)
			}
		}
	}
	if err := bb.Close(); err != nil {
		return Result{}, fmt.Errorf("backup: pg_basebackup: %w", err)
	}
	if !labelSeen {
		return Result{}, errors.New("backup: backup_label not present in tar — pg_basebackup output is malformed")
	}
	logf("pg_basebackup complete: %d files, %d bytes; startLSN=%s timeline=%d", fileCount, bytesTotal, startLSN, timeline)

	// Force a WAL switch and read the now-current insert LSN. The previous
	// segment (containing startLSN..stopLSN) is now complete and being
	// archived by archive_command.
	if opts.StopLSN == nil {
		return Result{}, errors.New("backup: StopLSN func is required")
	}
	stopLSN, err := opts.StopLSN(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("backup: stop-LSN query: %w", err)
	}
	logf("stop LSN: %s; WAL-wait for segments containing [%s, %s] in storage", stopLSN, startLSN, stopLSN)

	// WAL-wait (Invariant #1).
	logf("waiting for bracket WAL via source=%q", opts.WALSource)
	segments, err := AcquireBracketWAL(ctx, AcquireOptions{
		Backend:   opts.Backend,
		Timeline:  timeline,
		StartLSN:  startLSN,
		StopLSN:   stopLSN,
		SegSize:   id.WALSegmentSize,
		Timeout:   opts.WALTimeout,
		WALSource: opts.WALSource,
	})
	if err != nil {
		return Result{}, fmt.Errorf("backup: %w", err)
	}
	logf("WAL-wait done")

	// Hash each WAL segment for the sidecar. Inline-WAL sources
	// (stream/walgrab) leave walRecords empty — the bracket WAL lives
	// inside the backup itself at <backup-id>/pg_wal/<seg> rather than
	// in the archive prefix, so the sidecar's archive-shaped record is
	// not meaningful. Restore looks at <backup>/pg_wal/ in that case.
	var walRecords []manifest.WALSegmentRecord
	if walRecordsNeeded(opts.WALSource) {
		walRecords, err = hashWALSegments(ctx, opts.Backend, timeline, segments)
		if err != nil {
			return Result{}, fmt.Errorf("backup: hash WAL: %w", err)
		}
	}

	// Build our backup_manifest and write atomically. For incremental backups,
	// we use pg_basebackup's own canonical manifest (captured from the tar)
	// because pg_combinebackup needs PG's exact format for incremental file
	// metadata — our hand-rolled emitter can't reproduce it byte-for-byte.
	var manifestBytes []byte
	if pgManifest != nil {
		manifestBytes = pgManifest
	} else {
		manifestBytes, err = mb.Finalize(manifest.BackupStopInfo{
			StopLSN:  stopLSN,
			StopTime: opts.Now().UTC(),
		})
		if err != nil {
			return Result{}, fmt.Errorf("backup: finalize manifest: %w", err)
		}
	}
	// Write manifest.tmp and sidecar to all alive backends, then Commit.
	manifestTmpRel := filepath.ToSlash(filepath.Join(backupID, "backup_manifest.tmp"))
	sidecarRel := filepath.ToSlash(filepath.Join(backupID, "Storage-Metadata.json"))
	manifestRel := filepath.ToSlash(filepath.Join(backupID, "backup_manifest"))

	if ms != nil {
		// Multi-storage path: write to all alive backends, track per-backend errors.
		if err := writeMultiFileAtomic(ctx, ms, manifestTmpRel, manifestBytes); err != nil {
			return Result{}, fmt.Errorf("backup: write manifest (multi): %w", err)
		}
	} else {
		if err := writeStorageFileAtomic(ctx, opts.Backend, manifestTmpRel, manifestBytes); err != nil {
			return Result{}, fmt.Errorf("backup: write manifest: %w", err)
		}
	}

	// Sidecar with WAL records and the empty-dir list for restore.
	sidecarType := manifest.BackupTypeFull
	if opts.Type == TypeIncremental {
		sidecarType = manifest.BackupTypeIncremental
	}
	sc := manifest.Sidecar{
		Version:              manifest.SidecarVersion,
		Server:               opts.Server,
		EncryptionRecipients: opts.Recipients,
		Compression:          opts.Compression,
		StorageLayoutVersion: 1,
		WALSegments:          walRecords,
		Directories:          dirs,
		Type:                 sidecarType,
		ParentBackupID:       opts.ParentBackupID,
		SystemIdentifier:     id.SystemIdentifier,
	}
	scBytes, err := manifest.MarshalSidecar(sc)
	if err != nil {
		return Result{}, fmt.Errorf("backup: marshal sidecar: %w", err)
	}
	if ms != nil {
		if err := writeMultiFileAtomic(ctx, ms, sidecarRel, scBytes); err != nil {
			return Result{}, fmt.Errorf("backup: write sidecar (multi): %w", err)
		}
	} else {
		if err := writeStorageFileAtomic(ctx, opts.Backend, sidecarRel, scBytes); err != nil {
			return Result{}, fmt.Errorf("backup: write sidecar: %w", err)
		}
	}

	// Final atomic rename: backup_manifest.tmp → backup_manifest. After this,
	// the backup is "live" — info/restore see it.
	var partialStorages int
	if ms != nil {
		succeeded, commitErr := ms.commitMulti(ctx, manifestTmpRel, manifestRel)
		if succeeded == 0 {
			return Result{}, fmt.Errorf("backup: commit manifest (all backends): %w", commitErr)
		}
		partialStorages = ms.failedCount()
		logf("manifest committed: %d backends succeeded, %d failed", succeeded, partialStorages)
	} else {
		if err := opts.Backend.Commit(ctx, manifestTmpRel, manifestRel); err != nil {
			return Result{}, fmt.Errorf("backup: commit manifest: %w", err)
		}
	}

	return Result{
		BackupID:        backupID,
		StartLSN:        startLSN,
		StopLSN:         stopLSN,
		Timeline:        timeline,
		Files:           fileCount,
		Bytes:           bytesTotal,
		Duration:        time.Since(startedAt),
		PartialStorages: partialStorages,
	}, nil
}

// writeStorageFileAtomic puts data at relPath using the storage's Put/Close cycle.
// Caller wants the result visible after Close; for files that should be
// invisible until a later Commit, use Put directly with a *.tmp suffix.
func writeStorageFileAtomic(ctx context.Context, storage storage.Backend, relPath string, data []byte) error {
	wc, err := storage.Put(ctx, relPath)
	if err != nil {
		return err
	}
	if _, err := wc.Write(data); err != nil {
		_ = wc.Close()
		return err
	}
	return wc.Close()
}

// writeMultiFileAtomic writes data to all alive backends in ms via TeeWriter.
// Returns nil if at least one backend received the data successfully; updates
// ms.dead for any backends that failed.
func writeMultiFileAtomic(ctx context.Context, ms *multiState, relPath string, data []byte) error {
	tw, sinkIdx, err := ms.putMulti(ctx, relPath)
	if err != nil {
		return err
	}
	if _, err := tw.Write(data); err != nil {
		_ = tw.Close()
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	ms.merge(tw.Results(), sinkIdx)
	return nil
}
