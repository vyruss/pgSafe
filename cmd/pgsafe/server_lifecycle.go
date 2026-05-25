package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/lock"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// newServerListCmd builds `pgsafe server list`. Enumerates distinct
// servers found in the storage backend (from each backup's per-backup
// sidecar plus the per-server root sidecar created by `server add`).
func newServerListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List servers present in the storage backend",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			asJSON, _ := cmd.Flags().GetBool("json")
			return runServerList(cmd.Context(), cfg, cmd.OutOrStdout(), asJSON)
		},
	}
	cmd.Flags().Bool("json", false, "emit JSON instead of the default summary")
	return cmd
}

// newServerUpgradeCmd builds `pgsafe server upgrade`. Rewrites the
// per-server root sidecar with the current config's compression and
// encryption recipients. Server name is immutable.
func newServerUpgradeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Rewrite the per-server sidecar with current config (compression, recipients)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")
			return runServerUpgrade(cmd.Context(), cfg, cmd.OutOrStdout(), lockTimeout)
		},
	}
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	return cmd
}

// newServerDeleteCmd builds `pgsafe server delete`. Removes every
// backup belonging to the configured server, then deletes the
// per-server root sidecar. Requires --force when any backups exist.
func newServerDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Remove every backup for the configured server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			force, _ := cmd.Flags().GetBool("force")
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")
			return runServerDelete(cmd.Context(), cfg, cmd.OutOrStdout(), force, lockTimeout)
		},
	}
	cmd.Flags().Bool("force", false, "proceed even when backups exist (mandatory for non-empty servers)")
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	return cmd
}

// runServerList scans the backend for distinct server names. Counts
// backups per server so operators see at-a-glance whether a server is
// active or stale.
func runServerList(ctx context.Context, cfg *config.Config, out io.Writer, asJSON bool) error {
	backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	defer cleanup()

	records, _, err := info.List(ctx, backend)
	if err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("server list: %v", err)}
	}
	counts := map[string]int{}
	for _, r := range records {
		if r.Server != "" {
			counts[r.Server]++
		}
	}
	// Also pick up servers with a root sidecar but no backups yet.
	if rootSC, err := readRootSidecar(ctx, backend); err == nil && rootSC.Server != "" {
		if _, ok := counts[rootSC.Server]; !ok {
			counts[rootSC.Server] = 0
		}
	}

	type entry struct {
		Server  string `json:"server"`
		Backups int    `json:"backups"`
	}
	out2 := make([]entry, 0, len(counts))
	for s, n := range counts {
		out2 = append(out2, entry{Server: s, Backups: n})
	}
	sort.Slice(out2, func(i, j int) bool { return out2[i].Server < out2[j].Server })

	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(out2)
	}
	if len(out2) == 0 {
		_, err := fmt.Fprintln(out, "(no servers)")
		return err
	}
	for _, e := range out2 {
		if _, err := fmt.Fprintf(out, "%s\t%d backup(s)\n", e.Server, e.Backups); err != nil {
			return err
		}
	}
	return nil
}

// runServerUpgrade reads the root sidecar, updates compression +
// recipients from the config, and atomically replaces it. Refuses to
// change Server (immutable identity).
func runServerUpgrade(ctx context.Context, cfg *config.Config, out io.Writer, lockTimeout time.Duration) error {
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

	sc, err := readRootSidecar(ctx, backend)
	if err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("server upgrade: read sidecar: %v", err)}
	}
	if sc.Server != cfg.Server {
		return errCfg("server upgrade: configured server %q does not match storage's %q (server name is immutable)", cfg.Server, sc.Server)
	}
	sc.EncryptionRecipients = cfg.Encryption.Recipients
	sc.Compression = fmt.Sprintf("%s:%d", cfg.Compression.Codec, cfg.Compression.Level)
	scBytes, err := manifest.MarshalSidecar(sc)
	if err != nil {
		return &ExitError{Code: 1, Msg: err.Error()}
	}
	if err := atomicReplaceSidecar(ctx, backend, sidecarFilename, scBytes); err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("server upgrade: %v", err)}
	}
	if _, err := fmt.Fprintf(out, "server %q upgraded: codec=%s recipients=%d\n", cfg.Server, sc.Compression, len(sc.EncryptionRecipients)); err != nil {
		return err
	}
	return nil
}

// runServerDelete walks every backup belonging to cfg.Server and
// removes it file-by-file, then deletes the root sidecar last so a
// crash mid-delete leaves the server's identity intact (next
// invocation can resume).
func runServerDelete(ctx context.Context, cfg *config.Config, out io.Writer, force bool, lockTimeout time.Duration) error {
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

	records, _, err := info.List(ctx, backend)
	if err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("server delete: %v", err)}
	}
	var ours []string
	for _, r := range records {
		if r.Server == cfg.Server {
			ours = append(ours, r.BackupID)
		}
	}
	if len(ours) > 0 && !force {
		return errCfg("server delete: %d backup(s) exist for %q; pass --force to proceed", len(ours), cfg.Server)
	}

	deleted := 0
	for _, id := range ours {
		files, err := backend.List(ctx, id)
		if err != nil {
			return &ExitError{Code: 4, Msg: fmt.Sprintf("server delete: list %s: %v", id, err)}
		}
		for _, fi := range files {
			if err := backend.Delete(ctx, fi.Path); err != nil {
				return &ExitError{Code: 4, Msg: fmt.Sprintf("server delete: delete %s: %v", fi.Path, err)}
			}
			deleted++
		}
	}

	// Root sidecar last — its absence is the durable "server is gone"
	// signal.
	if err := backend.Delete(ctx, sidecarFilename); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("server delete: root sidecar: %v", err)}
	}

	if _, err := fmt.Fprintf(out, "server %q deleted: %d backup(s), %d file(s)\n", cfg.Server, len(ours), deleted); err != nil {
		return err
	}
	return nil
}

// readRootSidecar reads the per-server Storage-Metadata.json at
// the backend root.
func readRootSidecar(ctx context.Context, b storage.Backend) (manifest.Sidecar, error) {
	rc, err := b.Get(ctx, sidecarFilename)
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
