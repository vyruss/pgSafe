package main

import (
	"context"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/lock"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// newAnnotateCmd builds the `pgsafe annotate` cobra command. Reads
// the existing Storage-Metadata.json sidecar for the named backup,
// sets Annotation, writes back atomically via the storage backend's
// Put + Commit pair.
func newAnnotateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "annotate BACKUP_ID",
		Short: "Attach a free-form note to a backup's sidecar",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			note, _ := cmd.Flags().GetString("note")
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")
			return runAnnotate(cmd.Context(), cfg, cmd.OutOrStdout(), args[0], note, lockTimeout)
		},
	}
	cmd.Flags().String("note", "", "annotation text (empty clears the existing note)")
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	return cmd
}

// runAnnotate is a mutator (rewrites the sidecar), so it acquires
// the Exclusive per-server lock. Atomic replacement uses the
// backend's Put + Commit pair: write to a `.tmp` key, Delete the
// existing final, then Commit. Same Invariant #6 / #6.5 sequence as
// every other manifest write — it must not corrupt the sidecar
// mid-write.
func runAnnotate(ctx context.Context, cfg *config.Config, out io.Writer, backupID, note string, lockTimeout time.Duration) error {
	releaseLock, err := acquireServerLock(ctx, cfg, lock.Exclusive, lockTimeout)
	if err != nil {
		return err
	}
	defer releaseLock()

	backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	defer cleanup()

	key := path.Join(backupID, "Storage-Metadata.json")
	sc, err := readSidecar(ctx, backend, key)
	if err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("annotate: read sidecar: %v", err)}
	}
	sc.Annotation = note
	scBytes, err := manifest.MarshalSidecar(sc)
	if err != nil {
		return &ExitError{Code: 1, Msg: fmt.Sprintf("annotate: marshal: %v", err)}
	}
	if err := atomicReplaceSidecar(ctx, backend, key, scBytes); err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("annotate: replace sidecar: %v", err)}
	}
	if _, err := fmt.Fprintf(out, "annotate %s: note=%q\n", backupID, note); err != nil {
		return err
	}
	return nil
}

// readSidecar fetches and parses the per-backup sidecar. Local to
// the annotate command (info has its own copy at the data-plane
// level; both are tiny enough that DRY-ing them isn't worth coupling
// the CLI to internal/info's private helpers).
func readSidecar(ctx context.Context, b storage.Backend, key string) (manifest.Sidecar, error) {
	rc, err := b.Get(ctx, key)
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

// atomicReplaceSidecar overwrites the sidecar at `key` via Put +
// Delete + Commit. The order is: Put .tmp, Delete final, Commit
// .tmp → final. POSIX `Commit` is rename-based and refuses to
// overwrite, so the Delete-then-Commit dance is required to
// re-materialise the final under the same name without ever leaving
// the storage in an inconsistent state — at every intermediate
// point either the old or the new sidecar is fully present.
func atomicReplaceSidecar(ctx context.Context, b storage.Backend, key string, body []byte) error {
	tmp := key + ".tmp"
	wc, err := b.Put(ctx, tmp)
	if err != nil {
		return fmt.Errorf("put %s: %w", tmp, err)
	}
	if _, err := wc.Write(body); err != nil {
		_ = wc.Close()
		return fmt.Errorf("write: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	// Delete the existing final so Commit's "refuses to overwrite"
	// guard doesn't reject the rename. Tolerate ErrNotExist for the
	// rare case where the sidecar was just cleared.
	if err := b.Delete(ctx, key); err != nil {
		// We can't import os here without growing the diff; the
		// per-backend wrapped errors carry the path so operators
		// see what failed.
		return fmt.Errorf("delete %s: %w", key, err)
	}
	if err := b.Commit(ctx, tmp, key); err != nil {
		return fmt.Errorf("commit %s → %s: %w", tmp, key, err)
	}
	return nil
}
