package main

// `pgsafe archive-push %p` and `pgsafe archive-get %f %p` are the operator's
// glue between PG's archive_command / restore_command and pgSafe's storage
// backend. Both shell-out forms are simple wrappers around
// internal/wal/archive.

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

func newArchivePushCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "archive-push <segment-path>",
		Short: "Push one WAL segment into the configured storage backend (called by PG archive_command)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
			if err != nil {
				return &ExitError{Code: 4, Msg: err.Error()}
			}
			defer cleanup()

			segPath := args[0]
			segName := filepath.Base(segPath)
			tli, err := archive.TimelineFromSegment(segName)
			if err != nil {
				return errCfg("archive-push: %v", err)
			}

			if err := archive.Push(ctx, backend, tli, segPath); err != nil {
				return &ExitError{Code: 4, Msg: err.Error()}
			}
			return nil
		},
	}
}

func newArchiveGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "archive-get <segment-name> <dest-path>",
		Short: "Fetch one WAL segment from the configured storage backend (called by PG restore_command)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
			if err != nil {
				return &ExitError{Code: 4, Msg: err.Error()}
			}
			defer cleanup()

			segName := args[0]
			destPath := args[1]
			tli, err := archive.TimelineFromSegment(segName)
			if err != nil {
				return errCfg("archive-get: %v", err)
			}
			if err := archive.Get(ctx, backend, tli, segName, destPath); err != nil {
				// PG treats a non-zero exit as "WAL not yet available;
				// retry later." We exit 1 (rather than 4) because the
				// expected case is "the next segment hasn't been
				// archived yet" — not an operator-actionable error.
				return fmt.Errorf("archive-get: %w", err)
			}
			return nil
		},
	}
}
