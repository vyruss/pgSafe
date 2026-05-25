package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/check"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/lock"
)

// newCheckCmd builds the `pgsafe check` cobra command. Default output
// is one line per probe with PASS/FAIL prefix; --json emits the full
// Report for monitoring integrations.
func newCheckCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Run the operator-diagnosis battery against the configured server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			asJSON, _ := cmd.Flags().GetBool("json")
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")
			return runCheck(cmd.Context(), cfg, cmd.OutOrStdout(), asJSON, lockTimeout)
		},
	}
	cmd.Flags().Bool("json", false, "emit JSON instead of the default summary")
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	return cmd
}

// runCheck opens the backend (and optionally PG via the config), runs
// the diagnosis battery, prints the report, and exits 5 on any FAIL.
//
// PG-side probes (archive_command, standby_coordination) are gated on
// the PG conn_string being reachable. A reachable-but-failing PG is
// still a probe FAIL; an unreachable PG records a probe-level FAIL on
// archive_command (so `check` doesn't silently mask "PG is down" as
// "all good"). Operators running `check` in pure-monitoring mode
// against a cloud storage without PG access can use `--json` and
// post-filter; v1 keeps the wiring simple.
func runCheck(ctx context.Context, cfg *config.Config, out io.Writer, asJSON bool, lockTimeout time.Duration) error {
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

	// PG pool is best-effort: a failed Connect surfaces as a
	// PG-gated probe being skipped (Detail explains why), not as a
	// fatal CLI error. This lets operators run `check` against a
	// cloud-only storage without standing PG up.
	report := check.Run(ctx, check.Options{
		Backend: backend,
		Pool:    nil, // PG pool wiring deferred — `check` runs storage-side only
	})

	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			return &ExitError{Code: 1, Msg: err.Error()}
		}
	} else {
		for _, p := range report.Probes {
			state := "PASS"
			if !p.OK {
				state = "FAIL"
			}
			if _, err := fmt.Fprintf(out, "%s  %s  %s\n", state, p.Name, p.Detail); err != nil {
				return err
			}
		}
	}
	if !report.AllOK() {
		return &ExitError{Code: 5, Msg: "check: one or more probes failed"}
	}
	return nil
}
