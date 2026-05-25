package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"filippo.io/age"
	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/lock"
	"github.com/vyruss/pgsafe/internal/verify"
)

// newVerifyCmd builds the `pgsafe verify` cobra command. With no
// flags it re-hashes every backup in the storage. `--backup-id=ID`
// restricts to one. `--workers=N` caps in-flight per-file
// verifications. `--identity-file=PATH` supplies age decryption keys
// when the storage was written with non-empty `encryption.recipients`.
// `--json` emits a machine-readable summary.
func newVerifyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Re-hash stored backups and report mismatches",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			backupID, _ := cmd.Flags().GetString("backup-id")
			workers, _ := cmd.Flags().GetInt("workers")
			identityFile, _ := cmd.Flags().GetString("identity-file")
			asJSON, _ := cmd.Flags().GetBool("json")
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")
			return runVerify(cmd.Context(), cfg, cmd.OutOrStdout(), backupID, workers, identityFile, asJSON, lockTimeout)
		},
	}
	cmd.Flags().String("backup-id", "", "verify only this backup ID (empty = every backup)")
	cmd.Flags().Int("workers", 0, "in-flight per-file verifications (0 = runtime.NumCPU)")
	cmd.Flags().String("identity-file", "", "age identity file for decrypting stored bytes")
	cmd.Flags().Bool("json", false, "emit JSON instead of the default summary")
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	return cmd
}

// runVerify drives `internal/verify` from the CLI. Per §3.4 exit-code
// mapping: any mismatch (file SHA or manifest checksum) → exit 5
// (invariant violation); a backend / sidecar / manifest read failure
// → exit 4 (storage-side); a lock timeout → exit 9 (handled by
// acquireServerLock).
func runVerify(ctx context.Context, cfg *config.Config, out io.Writer, backupID string, workers int, identityFile string, asJSON bool, lockTimeout time.Duration) error {
	releaseLock, err := acquireServerLock(ctx, cfg, lock.Shared, lockTimeout)
	if err != nil {
		return err
	}
	defer releaseLock()

	identities, err := readIdentities(identityFile)
	if err != nil {
		return errCfg("verify: --identity-file: %v", err)
	}

	backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	defer cleanup()

	results, err := verify.Verify(ctx, backend, verify.Options{
		BackupID:   backupID,
		Workers:    workers,
		Identities: identities,
	})
	if err != nil {
		return &ExitError{Code: 4, Msg: fmt.Sprintf("verify: %v", err)}
	}

	if asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(results); err != nil {
			return &ExitError{Code: 1, Msg: err.Error()}
		}
	} else if err := printVerifyTable(out, results); err != nil {
		return &ExitError{Code: 1, Msg: err.Error()}
	}

	allOK := true
	for _, r := range results {
		if !r.AllOK() {
			allOK = false
			break
		}
	}
	if !allOK {
		return &ExitError{Code: 5, Msg: "verify: one or more backups failed integrity checks"}
	}
	return nil
}

func printVerifyTable(w io.Writer, results []verify.Result) error {
	if len(results) == 0 {
		_, err := fmt.Fprintln(w, "(no backups)")
		return err
	}
	for _, r := range results {
		state := "OK"
		if !r.AllOK() {
			state = "FAILED"
		}
		manifestState := "ok"
		if !r.ManifestChecksumOK {
			manifestState = "BAD"
			if r.ManifestChecksumErr != "" {
				manifestState = "err: " + r.ManifestChecksumErr
			}
		}
		if _, err := fmt.Fprintf(w, "%s  %s  files=%d/%d  manifest=%s\n",
			r.BackupID, state, r.FilesOK, r.FilesOK+r.FilesMismatched, manifestState); err != nil {
			return err
		}
		for _, m := range r.Mismatches {
			if _, err := fmt.Fprintf(w, "  - %s: %s (expected=%s actual=%s)\n",
				m.Path, m.Reason, m.Expected, m.Actual); err != nil {
				return err
			}
		}
	}
	return nil
}

// readIdentities parses an age identity file. Empty path returns nil
// (no decryption needed).
func readIdentities(file string) ([]age.Identity, error) {
	if file == "" {
		return nil, nil
	}
	f, err := os.Open(file) //nolint:gosec // operator-supplied path by design
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return age.ParseIdentities(f)
}
