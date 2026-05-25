package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/lock"
	"github.com/vyruss/pgsafe/internal/retention"
)

// newPruneCmd builds the `pgsafe prune` cobra command. Retention is
// configured via flags in v1 (--keep-fulls, --keep-age, --keep-daily,
// --keep-weekly, --keep-monthly). The YAML `retention:` block is a
// follow-up cycle; the CLI here accepts every flag the YAML schema
// will eventually mirror.
func newPruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Apply retention rules and delete expirable backups + WAL",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			policy := retention.Policy{}
			policy.KeepFulls, _ = cmd.Flags().GetInt("keep-fulls")
			policy.KeepFullAge, _ = cmd.Flags().GetDuration("keep-age")
			policy.KeepDaily, _ = cmd.Flags().GetInt("keep-daily")
			policy.KeepWeekly, _ = cmd.Flags().GetInt("keep-weekly")
			policy.KeepMonthly, _ = cmd.Flags().GetInt("keep-monthly")
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			asJSON, _ := cmd.Flags().GetBool("json")
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")
			return runPrune(cmd.Context(), cfg, cmd.OutOrStdout(), policy, dryRun, asJSON, lockTimeout)
		},
	}
	cmd.Flags().Int("keep-fulls", 0, "keep the N most recent chains (0 = rule disabled)")
	cmd.Flags().Duration("keep-age", 0, "keep chains whose full is younger than this duration")
	cmd.Flags().Int("keep-daily", 0, "keep one chain per day for the past N days")
	cmd.Flags().Int("keep-weekly", 0, "keep one chain per week for the past N weeks")
	cmd.Flags().Int("keep-monthly", 0, "keep one chain per month for the past N months")
	cmd.Flags().Bool("dry-run", false, "show what would be deleted without doing it")
	cmd.Flags().Bool("json", false, "emit JSON instead of the default summary")
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	return cmd
}

// runPrune drives `internal/retention` from the CLI. Per §3.4 exit-code
// mapping: lock timeout → 9 (via acquireServerLock); empty policy → 8
// (retention safety; refuse to act on the obvious foot-gun); storage-side
// errors → 4.
func runPrune(ctx context.Context, cfg *config.Config, out io.Writer, policy retention.Policy, dryRun, asJSON bool, lockTimeout time.Duration) error {
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
		return &ExitError{Code: 4, Msg: fmt.Sprintf("prune: %v", err)}
	}
	plan, err := retention.Evaluate(records, policy)
	if err != nil {
		if errors.Is(err, retention.ErrEmptyPolicy) {
			return &ExitError{Code: 8, Msg: err.Error()}
		}
		return &ExitError{Code: 1, Msg: err.Error()}
	}

	res, err := retention.Prune(ctx, backend, plan, dryRun)
	if err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("prune: %v", err)}
	}

	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(struct {
			DryRun bool                  `json:"dry_run"`
			Plan   retention.Plan        `json:"plan"`
			Result retention.PruneResult `json:"result"`
		}{DryRun: dryRun, Plan: plan, Result: res})
	}
	prefix := ""
	if dryRun {
		prefix = "(dry-run) "
	}
	if _, err := fmt.Fprintf(out, "%sprune: deleted %d backup(s), %d file(s), %d WAL segment(s)\n",
		prefix, len(res.BackupsDeleted), res.FilesDeleted, len(res.WALSegmentsDeleted)); err != nil {
		return err
	}
	for _, id := range res.BackupsDeleted {
		if _, err := fmt.Fprintf(out, "  - %s\n", id); err != nil {
			return err
		}
	}
	return nil
}
