package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/lock"
)

// newInfoCmd builds the `pgsafe info` cobra command. Tabular by
// default; `--json` switches to a stable schema for monitoring.
// `--server NAME` filters multi-server storages to one entry.
func newInfoCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info",
		Short: "List backups in the storage backend",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			asJSON, _ := cmd.Flags().GetBool("json")
			serverFilter, _ := cmd.Flags().GetString("server-filter")
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")
			return runInfo(cmd.Context(), cfg, cmd.OutOrStdout(), asJSON, serverFilter, lockTimeout)
		},
	}
	cmd.Flags().Bool("json", false, "emit JSON instead of the default table")
	cmd.Flags().String("server-filter", "", "show only entries whose Server matches NAME")
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	return cmd
}

// runInfo opens the storage backend, acquires the per-server Shared
// lock (so it doesn't block during a backup; doesn't get blocked by
// other readers), calls info.List, applies the optional server-filter,
// and prints either the table or the JSON form.
func runInfo(ctx context.Context, cfg *config.Config, out io.Writer, asJSON bool, serverFilter string, lockTimeout time.Duration) error {
	releaseLock, err := acquireServerLock(ctx, cfg, lock.Shared, lockTimeout)
	if err != nil {
		return err
	}
	defer releaseLock()

	backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	defer cleanup()

	records, warnings, err := info.List(ctx, backend)
	if err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("info: %v", err)}
	}
	if serverFilter != "" {
		filtered := records[:0]
		for _, r := range records {
			if r.Server == serverFilter {
				filtered = append(filtered, r)
			}
		}
		records = filtered
	}
	if asJSON {
		return info.FormatJSON(out, records, warnings)
	}
	return info.FormatTable(out, records, warnings)
}
