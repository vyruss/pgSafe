package restore

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/vyruss/pgsafe/internal/filter/compression"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
	"golang.org/x/sync/errgroup"
)

// runCombineChain materializes the entire incremental chain rooted at the
// backup whose sidecar Type is "incremental". The leaf's sidecar is read
// from the storage to find the parent; we walk parent links until we hit a
// "full" sidecar. Each backup in the chain is decrypted into a per-id
// stage subdirectory, then `pg_combinebackup full incr1 ... -o <target>`
// merges them into opts.Target.
//
//   - §3.2.7. gate.
func runCombineChain(ctx context.Context, opts Options, codec compression.Codec, leafID string) (int, int64, error) {
	if _, err := exec.LookPath("pg_combinebackup"); err != nil {
		return 0, 0, fmt.Errorf("restore: pg_combinebackup not on PATH: %w", err)
	}

	chain, err := buildChain(ctx, opts.Backend, leafID)
	if err != nil {
		return 0, 0, fmt.Errorf("restore: build chain: %w", err)
	}
	if len(chain) < 2 {
		return 0, 0, fmt.Errorf("restore: chain has %d entries; expected at least full + 1 incremental", len(chain))
	}

	// Stage directory is a sibling of opts.Target so pg_combinebackup sees
	// opts.Target as empty. Same filesystem == cheap rename within stage,
	// but since we materialize then exec pg_combinebackup which reads, that
	// doesn't matter here.
	stageRoot, err := os.MkdirTemp(filepath.Dir(opts.Target), ".pgsafe-stage-")
	if err != nil {
		return 0, 0, fmt.Errorf("restore: mkdir stage: %w", err)
	}
	// Set PGSAFE_DEBUG_STAGE=<dir> to copy staged backups out before cleanup.
	defer func() {
		if dbgDir := os.Getenv("PGSAFE_DEBUG_STAGE"); dbgDir != "" {
			_ = exec.Command("cp", "-a", stageRoot, dbgDir).Run() //nolint:gosec // debug only
		}
		_ = os.RemoveAll(stageRoot)
	}()

	// Stage each chain backup (full → ... → leaf) into stageRoot/<id>/.
	var (
		fileCount  int64
		bytesTotal int64
	)
	for _, id := range chain {
		dst := filepath.Join(stageRoot, id)
		if err := os.MkdirAll(dst, 0o700); err != nil {
			return 0, 0, fmt.Errorf("restore: mkdir stage/%s: %w", id, err)
		}
		fc, bc, err := stageBackupContents(ctx, opts, codec, id, dst)
		if err != nil {
			return 0, 0, fmt.Errorf("restore: stage %s: %w", id, err)
		}
		fileCount += int64(fc)
		bytesTotal += bc
	}

	// pg_combinebackup full incr1 incr2 ... -o <target>
	args := make([]string, 0, len(chain)+2)
	for _, id := range chain {
		args = append(args, filepath.Join(stageRoot, id))
	}
	args = append(args, "--output", opts.Target)
	cmd := exec.CommandContext(ctx, "pg_combinebackup", args...) //nolint:gosec // args derived from validated stage paths
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return 0, 0, fmt.Errorf("restore: pg_combinebackup: %w", err)
	}

	return int(fileCount), bytesTotal, nil
}

// buildChain walks parent_backup_id pointers from leafID back to the chain's
// root (a backup whose sidecar Type == "full"). Returns the chain ordered
// root-to-leaf, suitable for direct argv to pg_combinebackup.
func buildChain(ctx context.Context, storage storage.Backend, leafID string) ([]string, error) {
	const maxDepth = 64
	var chain []string
	id := leafID
	for i := 0; i < maxDepth; i++ {
		sc, err := readSidecar(ctx, storage, id)
		if err != nil {
			return nil, fmt.Errorf("read sidecar for %s: %w", id, err)
		}
		chain = append([]string{id}, chain...) // prepend
		if sc.Type == manifest.BackupTypeFull || sc.Type == "" {
			return chain, nil
		}
		if sc.Type != manifest.BackupTypeIncremental {
			return nil, fmt.Errorf("backup %s: unrecognized sidecar type %q", id, sc.Type)
		}
		if sc.ParentBackupID == "" {
			return nil, fmt.Errorf("incremental backup %s has no parent_backup_id", id)
		}
		id = sc.ParentBackupID
	}
	return nil, fmt.Errorf("backup chain depth exceeded %d (cycle?)", maxDepth)
}

func readSidecar(ctx context.Context, storage storage.Backend, backupID string) (manifest.Sidecar, error) {
	rc, err := storage.Get(ctx, path.Join(backupID, "Storage-Metadata.json"))
	if err != nil {
		return manifest.Sidecar{}, err
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return manifest.Sidecar{}, err
	}
	return manifest.UnmarshalSidecar(body)
}

// stageBackupContents decrypts every file in backupID into dst, recreating
// the empty directories from the sidecar. Mirrors the per-file restore loop
// but writes into an arbitrary destination instead of the operator-supplied
// target. Used to materialize one chain entry for pg_combinebackup.
func stageBackupContents(
	ctx context.Context,
	opts Options,
	codec compression.Codec,
	backupID, dst string,
) (int, int64, error) {
	// Sidecar — for the directory list.
	sc, err := readSidecar(ctx, opts.Backend, backupID)
	if err != nil {
		return 0, 0, err
	}
	for _, dir := range sc.Directories {
		dirPath := filepath.Join(dst, filepath.FromSlash(dir))
		if err := os.MkdirAll(dirPath, 0o700); err != nil {
			return 0, 0, fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}

	// Manifest — for the file list. pg_combinebackup needs each chain
	// backup's backup_manifest at its root, so we copy it through.
	manifestRC, err := opts.Backend.Get(ctx, path.Join(backupID, "backup_manifest"))
	if err != nil {
		return 0, 0, fmt.Errorf("read manifest: %w", err)
	}
	manifestBytes, err := io.ReadAll(manifestRC)
	_ = manifestRC.Close()
	if err != nil {
		return 0, 0, fmt.Errorf("read manifest bytes: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dst, "backup_manifest"), manifestBytes, 0o600); err != nil {
		return 0, 0, fmt.Errorf("write manifest: %w", err)
	}
	files, err := parseManifestFiles(manifestBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("parse manifest: %w", err)
	}

	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers > 8 {
			workers = 8
		}
	}
	var fileCount, bytesTotal int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, f := range files {
		f := f
		g.Go(func() error {
			n, err := restoreOneFile(gctx, opts.Backend, codec.Name(), opts.Identities, backupID, f.Path, dst)
			if err != nil {
				return fmt.Errorf("file %s: %w", f.Path, err)
			}
			atomic.AddInt64(&fileCount, 1)
			atomic.AddInt64(&bytesTotal, n)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, 0, err
	}

	return int(fileCount), bytesTotal, nil
}
