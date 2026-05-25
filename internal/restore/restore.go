// Package restore is the minimal restore caller. Its scope
// is exactly what  names as the milestone:
// untar the basebackup into $PGDATA, copy archived WAL into pg_wal/, drop a
// recovery.signal so PG enters recovery on first start. Tablespace remap,
// PITR target selection, parallel file copy
package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/filter/compression"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/wal/archive"
	"golang.org/x/sync/errgroup"
)

// Options configure one restore.
type Options struct {
	Backend    storage.Backend
	Target     string         // $PGDATA destination
	Identities []age.Identity // age private keys for decryption

	// BackupID picks a specific backup directory inside the storage. If empty,
	// restore picks the latest one.
	BackupID string

	// Workers controls parallel file decryption + decompression. <= 0 means
	// runtime.NumCPU() capped at 8.
	Workers int

	// TablespaceRemap maps tablespace OID → host filesystem path. When set,
	// restore writes a new tablespace_map at the target so PG opens the
	// non-default tablespaces at the operator's chosen locations.
	TablespaceRemap map[uint64]string

	// StandbyMode, if true, writes standby.signal in addition to (instead
	// of) recovery.signal — the cluster will start as a standby.
	StandbyMode bool

	// RestoreCommand overrides the default `cp` from the storage's wal/ dir.
	// Empty means use the default ("/bin/false" for end-of-archive sentinel
	// when the storage's wal/ has been pre-staged into pg_wal/).
	RestoreCommand string

	// PITR target — at most one may be set (CLI enforces the exclusive-or).
	TargetTime   *time.Time
	TargetXID    *uint64
	TargetLSN    *manifest.LSN
	TargetName   string
	TargetAction string // "pause" | "promote" | "shutdown"; default "pause"

	// Worker, when non-nil, dispatches the restore through a PG-host
	// worker over SSH instead of running it locally on the caller.
	// Backend points at the caller-local source storage (the caller
	// exposes it via SFTP-tunnel for the worker), Target is the
	// worker-local $PGDATA path, Identities ship via Configure.
	Worker *WorkerOptions
}

// Result reports what restore did.
type Result struct {
	BackupID string
	Files    int
	WAL      int
	Bytes    int64
}

// Run executes a restore.
func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.Target == "" {
		return Result{}, errors.New("restore: Target is required")
	}
	if !filepath.IsAbs(opts.Target) {
		return Result{}, fmt.Errorf("restore: Target %q must be absolute", opts.Target)
	}
	if len(opts.Identities) == 0 {
		return Result{}, errors.New("restore: Identities required")
	}
	// Worker path doesn't need a caller-side Backend (the worker
	// constructs its own from the SFTP-tunnel cred). Dispatch before
	// the Backend nil-check.
	if opts.Worker != nil {
		return runWorkerRestore(ctx, opts)
	}
	if opts.Backend == nil {
		return Result{}, errors.New("restore: Storage is required")
	}
	if err := validatePITRExclusive(opts); err != nil {
		return Result{}, err
	}

	backupID := opts.BackupID
	if backupID == "" {
		var err error
		backupID, err = findLatestBackup(ctx, opts.Backend)
		if err != nil {
			return Result{}, err
		}
	}

	if err := os.MkdirAll(opts.Target, 0o700); err != nil {
		return Result{}, fmt.Errorf("restore: mkdir target: %w", err)
	}

	// Read sidecar to learn the codec used at backup time.
	sidecarRC, err := opts.Backend.Get(ctx, path.Join(backupID, "Storage-Metadata.json"))
	if err != nil {
		return Result{}, fmt.Errorf("restore: read sidecar: %w", err)
	}
	sidecarBytes, err := io.ReadAll(sidecarRC)
	_ = sidecarRC.Close()
	if err != nil {
		return Result{}, fmt.Errorf("restore: read sidecar bytes: %w", err)
	}
	sc, err := manifest.UnmarshalSidecar(sidecarBytes)
	if err != nil {
		return Result{}, fmt.Errorf("restore: parse sidecar: %w", err)
	}
	codecName := strings.SplitN(sc.Compression, ":", 2)[0]
	codec, err := compression.Get(codecName)
	if err != nil {
		return Result{}, fmt.Errorf("restore: codec from sidecar: %w", err)
	}

	// Incremental restore: walk the chain from leaf to root, decrypt each
	// backup into a per-id stage subdirectory, then shell out to
	// pg_combinebackup to merge them into the target.
	if sc.Type == manifest.BackupTypeIncremental {
		fileCount, bytesTotal, err := runCombineChain(ctx, opts, codec, backupID)
		if err != nil {
			return Result{}, err
		}
		// After combine, fall through to WAL+signal+PITR setup, but skip the
		// per-file decrypt loop and set up the manifest path directly.
		walCount, err := finishRestore(ctx, opts, backupID)
		if err != nil {
			return Result{}, err
		}
		return Result{
			BackupID: backupID,
			Files:    fileCount,
			WAL:      walCount,
			Bytes:    bytesTotal,
		}, nil
	}

	// Read manifest to get the file list.
	manifestRC, err := opts.Backend.Get(ctx, path.Join(backupID, "backup_manifest"))
	if err != nil {
		return Result{}, fmt.Errorf("restore: read manifest: %w", err)
	}
	manifestBytes, err := io.ReadAll(manifestRC)
	_ = manifestRC.Close()
	if err != nil {
		return Result{}, fmt.Errorf("restore: read manifest bytes: %w", err)
	}
	files, err := parseManifestFiles(manifestBytes)
	if err != nil {
		return Result{}, fmt.Errorf("restore: parse manifest: %w", err)
	}

	// Re-create empty directories captured by the sidecar BEFORE restoring
	// files into them, so subdirectories that exist purely for their
	// presence (pg_notify, pg_stat_tmp, etc.) are in place when PG starts.
	for _, dir := range sc.Directories {
		dirPath := filepath.Join(opts.Target, filepath.FromSlash(dir))
		if err := os.MkdirAll(dirPath, 0o700); err != nil {
			return Result{}, fmt.Errorf("restore: mkdir %s: %w", dir, err)
		}
	}

	// Restore each file: decrypt + decompress → target.
	// Parallelism: opts.Workers (or NumCPU capped at 8 by default).
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers > 8 {
			workers = 8
		}
	}
	var (
		fileCount  int64
		bytesTotal int64
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, f := range files {
		f := f
		g.Go(func() error {
			n, err := restoreOneFile(gctx, opts.Backend, codecName, opts.Identities, backupID, f.Path, opts.Target)
			if err != nil {
				return fmt.Errorf("restore: file %s: %w", f.Path, err)
			}
			atomic.AddInt64(&fileCount, 1)
			atomic.AddInt64(&bytesTotal, n)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return Result{}, err
	}

	walCount, err := finishRestore(ctx, opts, backupID)
	if err != nil {
		return Result{}, err
	}
	return Result{
		BackupID: backupID,
		Files:    int(fileCount),
		WAL:      walCount,
		Bytes:    bytesTotal,
	}, nil
}

// finishRestore performs the WAL copy + signal/PITR/tablespace setup that's
// common to both full and incremental restores. Call it after the data files
// are in place at opts.Target.
func finishRestore(ctx context.Context, opts Options, backupID string) (int, error) {
	// Copy the manifest itself into the target so pg_verifybackup (and other
	// downstream consumers) see it at the conventional location. The manifest
	// was stored plaintext in the storage, so it's a straight copy — no filter.
	// For incremental restores, pg_combinebackup wrote a fresh
	// backup_manifest already; skip in that case.
	manifestTarget := filepath.Join(opts.Target, "backup_manifest")
	if _, err := os.Stat(manifestTarget); errors.Is(err, os.ErrNotExist) {
		if err := copyStorageFile(ctx, opts.Backend, path.Join(backupID, "backup_manifest"), manifestTarget); err != nil {
			return 0, fmt.Errorf("restore: copy backup_manifest: %w", err)
		}
	}

	// Copy WAL segments from <storage>/wal/<TLI>/<seg>-<sha> into
	// <target>/pg_wal/<seg>. PG recovery starts from backup_label and
	// replays from pg_wal; PG only knows the bare 24-hex segment name,
	// not pgsafe's source-SHA suffix.
	walTarget := filepath.Join(opts.Target, "pg_wal")
	if err := os.MkdirAll(walTarget, 0o700); err != nil {
		return 0, fmt.Errorf("restore: mkdir pg_wal: %w", err)
	}
	walEntries, err := opts.Backend.List(ctx, "wal")
	if err != nil {
		return 0, fmt.Errorf("restore: list wal: %w", err)
	}
	walCount := 0
	for _, w := range walEntries {
		base := path.Base(w.Path)
		if base == w.Path {
			continue
		}
		// Strip the "-<sha256-hex>" suffix added by archive.Push so PG
		// sees a vanilla 24-char WAL filename. Files without the
		// suffix (legacy tmp/.partial/etc) are skipped.
		segName, ok := archive.SegmentNameFromBasename(base)
		if !ok {
			continue
		}
		if err := copyStorageFile(ctx, opts.Backend, w.Path, filepath.Join(walTarget, segName)); err != nil {
			return 0, fmt.Errorf("restore: copy wal %s: %w", w.Path, err)
		}
		walCount++
	}

	// Tablespace remap (after files, before signal files).
	if err := applyTablespaceRemap(opts.Target, opts.TablespaceRemap); err != nil {
		return 0, fmt.Errorf("restore: tablespace remap: %w", err)
	}

	// recovery.signal (or standby.signal).
	sigName := "recovery.signal"
	if opts.StandbyMode {
		sigName = "standby.signal"
	}
	sigPath := filepath.Join(opts.Target, sigName)
	if err := os.WriteFile(sigPath, nil, 0o600); err != nil {
		return 0, fmt.Errorf("restore: %s: %w", sigName, err)
	}

	// postgresql.auto.conf appendix.
	autoConfPath := filepath.Join(opts.Target, "postgresql.auto.conf")
	autoConf, err := os.ReadFile(autoConfPath) //nolint:gosec
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("restore: read postgresql.auto.conf: %w", err)
	}
	restoreCmd := opts.RestoreCommand
	if restoreCmd == "" {
		restoreCmd = "/bin/false"
	}
	if !strings.Contains(string(autoConf), "restore_command") {
		autoConf = append(autoConf,
			[]byte(fmt.Sprintf("\nrestore_command = '%s'\n", restoreCmd))...)
	}
	if pitr := pitrLines(opts); pitr != nil {
		autoConf = append(autoConf, pitr...)
	}
	if err := os.WriteFile(autoConfPath, autoConf, 0o600); err != nil { //nolint:gosec
		return 0, fmt.Errorf("restore: write postgresql.auto.conf: %w", err)
	}

	return walCount, nil
}

func restoreOneFile(
	ctx context.Context,
	storage storage.Backend,
	codecName string,
	identities []age.Identity,
	backupID, relPath, target string,
) (int64, error) {
	src, err := storage.Get(ctx, path.Join(backupID, relPath))
	if err != nil {
		return 0, err
	}
	defer func() { _ = src.Close() }()

	rd, _, err := filter.Unwrap(codecName, src, identities)
	if err != nil {
		return 0, fmt.Errorf("unwrap: %w", err)
	}
	defer func() { _ = rd.Close() }()

	dstPath := filepath.Join(target, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o700); err != nil {
		return 0, fmt.Errorf("mkdir: %w", err)
	}
	dst, err := os.Create(dstPath) //nolint:gosec // path validated upstream
	if err != nil {
		return 0, fmt.Errorf("create: %w", err)
	}
	n, err := io.Copy(dst, rd)
	cerr := dst.Close()
	if err != nil {
		return 0, fmt.Errorf("copy: %w", err)
	}
	if cerr != nil {
		return 0, fmt.Errorf("close: %w", cerr)
	}
	return n, nil
}

func copyStorageFile(ctx context.Context, storage storage.Backend, repoPath, dstPath string) error {
	src, err := storage.Get(ctx, repoPath)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.Create(dstPath) //nolint:gosec // dst is constructed from operator-supplied target
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}

// findLatestBackup picks the most recent backup directory (one ending in "F").
// multi-backup support replaces this with a manifest-info-driven pick.
func findLatestBackup(ctx context.Context, storage storage.Backend) (string, error) {
	all, err := storage.List(ctx, "")
	if err != nil {
		return "", fmt.Errorf("findLatestBackup: list: %w", err)
	}
	var ids []string
	seen := map[string]bool{}
	for _, fi := range all {
		// Files inside <backup-id>/... — the first segment is the candidate.
		parts := strings.SplitN(fi.Path, "/", 2)
		if len(parts) < 2 {
			continue
		}
		first := parts[0]
		// Backup ID conventions: full = "<ts>F", incremental = "<parent>_<ts>I".
		if !strings.HasSuffix(first, "F") && !strings.HasSuffix(first, "I") {
			continue
		}
		if !seen[first] {
			seen[first] = true
			ids = append(ids, first)
		}
	}
	if len(ids) == 0 {
		return "", errors.New("findLatestBackup: no backup directories found")
	}
	sort.Strings(ids)
	return ids[len(ids)-1], nil
}

// parseManifestFiles extracts the Files array from a backup_manifest JSON.
// We use encoding/json here (decoding only); the byte-precise hand-roll on
// the WRITE side doesn't apply to read.
func parseManifestFiles(data []byte) ([]ManifestFile, error) {
	type wrapper struct {
		Files []ManifestFile `json:"Files"`
	}
	var w wrapper
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&w); err != nil {
		return nil, err
	}
	return w.Files, nil
}

// ManifestFile is a thin echo of one PG backup_manifest Files entry. Only
// the restore-relevant fields are kept. JSON tags match PG's
// PascalCase keys so we can decode straight from the manifest.
type ManifestFile struct {
	Path     string `json:"Path"`
	Size     int64  `json:"Size"`
	Checksum string `json:"Checksum"`
}

// validatePITRExclusive enforces that at most one of the four PITR target
// options is set. The CLI also enforces this at the cobra layer; this is
// the defense-in-depth check at the API boundary.
func validatePITRExclusive(opts Options) error {
	count := 0
	if opts.TargetTime != nil {
		count++
	}
	if opts.TargetXID != nil {
		count++
	}
	if opts.TargetLSN != nil {
		count++
	}
	if opts.TargetName != "" {
		count++
	}
	if count > 1 {
		return errors.New("restore: at most one of --target-time / --target-xid / --target-lsn / --target-name may be set")
	}
	if opts.TargetAction != "" &&
		opts.TargetAction != "pause" &&
		opts.TargetAction != "promote" &&
		opts.TargetAction != "shutdown" {
		return fmt.Errorf("restore: --target-action %q must be one of pause|promote|shutdown",
			opts.TargetAction)
	}
	return nil
}

// pitrLines returns the recovery_target_* GUC lines (with trailing
// newlines) for the given PITR options, or nil if no target is set.
// recovery_target_action defaults to "pause" when any target is set; this
// keeps the restored cluster in a recoverable state for operator
// inspection before promotion.
func pitrLines(opts Options) []byte {
	var b strings.Builder
	switch {
	case opts.TargetTime != nil:
		fmt.Fprintf(&b, "recovery_target_time = '%s'\n",
			opts.TargetTime.UTC().Format(time.RFC3339))
	case opts.TargetXID != nil:
		fmt.Fprintf(&b, "recovery_target_xid = '%d'\n", *opts.TargetXID)
	case opts.TargetLSN != nil:
		fmt.Fprintf(&b, "recovery_target_lsn = '%s'\n", *opts.TargetLSN)
	case opts.TargetName != "":
		fmt.Fprintf(&b, "recovery_target_name = '%s'\n", opts.TargetName)
	default:
		return nil
	}
	action := opts.TargetAction
	if action == "" {
		action = "pause"
	}
	fmt.Fprintf(&b, "recovery_target_action = '%s'\n", action)
	return []byte(b.String())
}

// applyTablespaceRemap writes a new tablespace_map at the target reflecting
// opts.TablespaceRemap, and recreates symlinks under pg_tblspc/ pointing at
// the operator's chosen paths. Called after files are restored.
func applyTablespaceRemap(target string, remap map[uint64]string) error {
	if len(remap) == 0 {
		return nil
	}
	// Read the original tablespace_map (one "<oid> <path>" line per
	// non-default tablespace) and produce a new one with remapped paths.
	mapPath := filepath.Join(target, "tablespace_map")
	original, err := os.ReadFile(mapPath) //nolint:gosec
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Backup had no non-default tablespaces; nothing to remap.
			return nil
		}
		return fmt.Errorf("read tablespace_map: %w", err)
	}

	var out strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(string(original)), "\n") {
		if line == "" {
			continue
		}
		var oid uint64
		var oldPath string
		if _, err := fmt.Sscanf(line, "%d %s", &oid, &oldPath); err != nil {
			return fmt.Errorf("parse tablespace_map line %q: %w", line, err)
		}
		newPath, ok := remap[oid]
		if !ok {
			// Operator didn't supply a remap for this tablespace; keep original.
			fmt.Fprintf(&out, "%d %s\n", oid, oldPath)
			continue
		}
		fmt.Fprintf(&out, "%d %s\n", oid, newPath)

		// Recreate the pg_tblspc symlink to point at the new path.
		linkPath := filepath.Join(target, "pg_tblspc", fmt.Sprintf("%d", oid))
		_ = os.Remove(linkPath)
		if err := os.Symlink(newPath, linkPath); err != nil {
			return fmt.Errorf("symlink %s → %s: %w", linkPath, newPath, err)
		}
		// Ensure the operator-chosen path exists.
		if err := os.MkdirAll(newPath, 0o700); err != nil {
			return fmt.Errorf("mkdir tablespace target %s: %w", newPath, err)
		}
	}

	if err := os.WriteFile(mapPath, []byte(out.String()), 0o600); err != nil {
		return fmt.Errorf("write tablespace_map: %w", err)
	}
	return nil
}

// LoadIdentityFile reads an age identity file (one X25519 key per line in
// "AGE-SECRET-KEY-..." form). Operators pass --identity-file to restore.
func LoadIdentityFile(path string) ([]age.Identity, error) {
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied path by design
	if err != nil {
		return nil, fmt.Errorf("identity-file: %w", err)
	}
	ids, err := age.ParseIdentities(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("identity-file: parse: %w", err)
	}
	if len(ids) == 0 {
		return nil, errors.New("identity-file: no identities found")
	}
	return ids, nil
}
