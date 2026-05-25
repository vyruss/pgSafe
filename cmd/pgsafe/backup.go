package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"filippo.io/age"
	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/backup"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/lock"
	"github.com/vyruss/pgsafe/internal/pg"
	"github.com/vyruss/pgsafe/internal/pg/conn"
)

func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Take a base backup of a server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			backupType, _ := cmd.Flags().GetString("type")
			parent, _ := cmd.Flags().GetString("parent")
			lockTimeout, _ := cmd.Flags().GetDuration("lock-timeout")

			// CLI > YAML > unset for the SSH-reach knobs.
			sshTarget, sshExtraArgsRaw := resolveSSHReach(cmd, cfg)
			remoteCommandRaw, _ := cmd.Flags().GetString("remote-command")
			sshExtraArgs, err := shellSplit(sshExtraArgsRaw)
			if err != nil {
				return errCfg("backup: --ssh-extra-args: %v", err)
			}
			remoteCommand, err := shellSplit(remoteCommandRaw)
			if err != nil {
				return errCfg("backup: --remote-command: %v", err)
			}
			workers, _ := cmd.Flags().GetInt("workers")
			workersExplicit := cmd.Flags().Changed("workers")
			confirmProxy, _ := cmd.Flags().GetBool("confirm-proxy")

			// Mode is inferred from caller's resolved config: pg.host
			// set → pgSafe (worker on PG host); pg.host unset →
			// PG-native libpq (simple by default, parallel when the
			// operator explicitly requested --workers). The flags below
			// are pgSafe-only; reject when there's no worker to receive
			// them.
			mode := inferBackupMode(sshTarget, workers, workersExplicit)
			if mode != backup.ModeWorker {
				if cmd.Flags().Changed("remote-command") {
					return errCfg("backup: --remote-command requires pg.host (pgSafe mode); set pg.host in the YAML or pass --ssh-target")
				}
				if cmd.Flags().Changed("direct-write") {
					return errCfg("backup: --direct-write requires pg.host (pgSafe mode); set pg.host in the YAML or pass --ssh-target")
				}
				if cmd.Flags().Changed("confirm-proxy") {
					return errCfg("backup: --confirm-proxy requires pg.host (pgSafe mode); set pg.host in the YAML or pass --ssh-target")
				}
			}
			// --direct-write controls whether the worker uploads
			// encrypted bytes directly to storage (default, true), or
			// streams them back to the caller over RPC for the
			// caller to write (false). Default is true for every
			// storage type: best throughput, simplest topology. Set
			// false when the PG host can't reach storage (corp
			// firewall blocks outbound to S3, etc.) but the
			// caller can — bytes then flow
			// worker → caller → storage at the cost of one extra
			// RPC hop. For storage.type=posix with pg.host set, the
			// dispatch ignores this flag and engages the SFTP-tunnel
			// transform automatically (the worker has no other way to
			// reach caller's local POSIX).
			workerWritesDirectly, _ := cmd.Flags().GetBool("direct-write")

			// --wal-source picks where the bracket WAL comes from.
			// --standalone is shorthand for "the inline-WAL source
			// available in this mode" — stream for PG-native simple,
			// walgrab for pgSafe-mode worker — plus a loud warning
			// at backup-start.
			walSource, _ := cmd.Flags().GetString("wal-source")
			standalone, _ := cmd.Flags().GetBool("standalone")
			noResume, _ := cmd.Flags().GetBool("no-resume")
			if standalone {
				inferred := "stream"
				if mode == backup.ModeWorker {
					inferred = "walgrab"
				}
				if walSource != "" && walSource != inferred {
					return errCfg("backup: --standalone implies --wal-source=%s in mode=%q; got --wal-source=%q",
						inferred, mode, walSource)
				}
				walSource = inferred
			}

			switch backupType {
			case "full":
				if parent != "" {
					return errCfg("backup: --parent only valid with --type=incr")
				}
				return runBackup(cmd.Context(), cfg, cmd.OutOrStdout(), backup.TypeFull, "", lockTimeout, mode, sshTarget, sshExtraArgs, remoteCommand, workers, workerWritesDirectly, confirmProxy, backup.WALSource(walSource), standalone, noResume)
			case "incr":
				if parent == "" {
					return errCfg("backup: --type=incr requires --parent=<backup-id>")
				}
				return runBackup(cmd.Context(), cfg, cmd.OutOrStdout(), backup.TypeIncremental, parent, lockTimeout, mode, sshTarget, sshExtraArgs, remoteCommand, workers, workerWritesDirectly, confirmProxy, backup.WALSource(walSource), standalone, noResume)
			default:
				return errCfg("backup: --type=%q must be full or incr", backupType)
			}
		},
	}
	cmd.Flags().String("type", "full", "backup type (full|incr)")
	cmd.Flags().String("parent", "", "parent backup ID for --type=incr (PG 17+)")
	cmd.Flags().Duration("lock-timeout", 0, "max wait for the per-server lock (0 = wait forever)")
	cmd.Flags().String("ssh-target", "", "SSH target for the PG host (user@host[:port]); shadows pg.host in YAML. Empty (and pg.host empty) = PG-native (libpq) mode.")
	cmd.Flags().String("ssh-extra-args", "", `extra args for /usr/bin/ssh, shell-quoted (e.g. --ssh-extra-args="-p 2222 -i ~/.ssh/id_ed25519")`)
	cmd.Flags().String("remote-command", "", `argv for the worker on PG host, shell-quoted. Default: pgsafe worker stdio. (e.g. --remote-command="sudo -u postgres -E /usr/local/bin/pgsafe worker stdio")`)
	cmd.Flags().Int("workers", 4, "parallelism. PG-native: workers=1 → single libpq connection (simple), workers>1 → parallel libpq. pgSafe: parallel StreamFile RPCs over the worker session.")
	cmd.Flags().Bool("direct-write", true, "in pgSafe mode, the worker uploads encrypted bytes directly to storage (default). Set --direct-write=false when the PG host can't reach storage (e.g. corp firewall blocks outbound to S3) — encrypted bytes then stream back to the caller via RPC and the caller writes. Ignored for POSIX storage on the caller's host (an SFTP-tunnel always engages there).")
	cmd.Flags().Bool("confirm-proxy", false, "for storage_reach=auto: prompt before falling back to caller-proxy when the worker can't reach storage directly (interactive only; EOF on stdin reads as 'no')")
	cmd.Flags().String("wal-source", "", "where the bracket WAL comes from: archive (default — operator runs archive_command into <storage>/wal/), stream (pg_basebackup --wal-method=fetch packs WAL into the backup; PG-native simple mode only — no PITR beyond the bracket unless an archive exists separately), walgrab (pgSafe-mode worker reads $PGDATA/pg_wal directly after pg_backup_stop). Empty = archive.")
	cmd.Flags().Bool("standalone", false, "shorthand for --wal-source=stream (simple mode only) — produces a self-contained backup that restores without an external WAL archive. PITR is limited to the bracket window unless archive_mode=on is configured separately. Operator says 'I know'.")
	cmd.Flags().Bool("no-resume", false, "force a fresh backup even when a compatible backup_manifest.copy exists in the storage. Mirrors pgbackrest's --no-resume. Default behavior: resume in place when a compatible checkpoint is present.")
	return cmd
}

// runBackup wires up the caller from a resolved config. Errors map to
// §3.4 exit codes: PG-side → 3, storage-side → 4, invariant violation → 5,
// lock-acquisition timeout → 9.
func runBackup(ctx context.Context, cfg *config.Config, out io.Writer, btype backup.Type, parent string, lockTimeout time.Duration, mode backup.Mode, sshTarget string, sshExtraArgs, remoteCommand []string, workers int, workerWritesDirectly, confirmProxy bool, walSource backup.WALSource, standalone, noResume bool) error {
	// Per-server Exclusive lock (Invariant #4). Acquired before any
	// resource opens so a contended run fails fast without bothering PG
	// or the storage backend. Released last via defer.
	releaseLock, err := acquireServerLock(ctx, cfg, lock.Exclusive, lockTimeout)
	if err != nil {
		return err
	}
	defer releaseLock()

	pool, err := conn.Connect(ctx, cfg.PG.ConnString)
	if err != nil {
		return &ExitError{Code: 3, Msg: err.Error()}
	}
	defer pool.Close()

	cluster, err := pg.Open(ctx, cfg.PG.ConnString)
	if err != nil {
		return &ExitError{Code: 3, Msg: err.Error()}
	}
	defer cluster.Close()

	backends, cleanup, err := openBackends(ctx, cfg.Storages)
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	defer cleanup()

	recipients, err := parseAgeRecipients(cfg.Encryption.Recipients)
	if err != nil {
		return errCfg("%v", err)
	}
	chain, err := filter.NewChain(filter.Options{
		Codec:      cfg.Compression.Codec,
		Level:      cfg.Compression.Level,
		Recipients: recipients,
	})
	if err != nil {
		return errCfg("%v", err)
	}

	opts := backup.Options{
		Cluster:        cluster,
		Backend:        backends[0],
		Backends:       backends,
		Filter:         chain,
		Pool:           pool, // Invariant #5 reachability probe
		StopLSN:        backup.NewPoolStopLSNFunc(pool),
		Mode:           mode,
		Server:         cfg.Server,
		Label:          "pgsafe-" + cfg.Server,
		DSN:            cfg.PG.ConnString,
		WALTimeout:     cfg.Backup.ArchiveTimeout,
		WALSource:      walSource,
		ScratchDir:     cfg.Backup.ScratchDir,
		Recipients:     cfg.Encryption.Recipients,
		Compression:    fmt.Sprintf("%s:%d", cfg.Compression.Codec, cfg.Compression.Level),
		Type:           btype,
		ParentBackupID: parent,
		PgsafeVersion:  version,
		// Resume defaults: YAML can disable via backup.resume=false;
		// CLI --no-resume overrides regardless. Either path
		// produces the same opts.ResumeDisabled=true behavior in
		// runSimple's resume-discovery step.
		ResumeDisabled:         noResume || (cfg.Backup.Resume != nil && !*cfg.Backup.Resume),
		ResumeCheckpointEveryN: cfg.Backup.ResumeCheckpointEveryN,
		ResumeGracePeriod:      cfg.Backup.ResumeGracePeriod,
	}
	if standalone {
		_, _ = fmt.Fprintf(out, "pgsafe backup: WARNING: --standalone — bracket WAL packed inline; PITR limited to backup window unless archive_mode=on is configured separately.\n")
	}

	switch mode {
	case backup.ModeSimple:
		// existing simple-mode path; nothing extra needed.
	case backup.ModeRemoteParallel:
		opts.RemoteParallel = backup.RemoteParallelOptions{
			Pool:      pool,
			Workers:   workers,
			PGVersion: cfg.PG.Version,
		}
	case backup.ModeWorker:
		opts.Worker = backup.WorkerOptions{
			Pool:                 pool,
			PGVersion:            cfg.PG.Version,
			SSHTarget:            sshTarget, // empty → same-host local subprocess
			SSHExtraArgs:         sshExtraArgs,
			RemoteCommand:        remoteCommand, // nil → library default
			Storage:              cfg.PrimaryStorage(),
			Workers:              workers,
			WorkerWritesDirectly: workerWritesDirectly,
			StorageReach:         cfg.PG.StorageReach,
			ConfirmProxy:         confirmProxy,
		}
	}

	res, err := backup.Run(ctx, opts)
	if err != nil {
		// Sentinel-driven exit-code mapping. Each backup-package
		// failure category wraps a stable error variable
		// (backup.ErrWALWait, backup.ErrPGProtocol, backup.ErrStorage,
		// ErrArchiveUnreachable); we test via errors.Is rather than
		// substring matching. New failure categories add sentinels in
		// the relevant package; legalising the sentinel here is a
		// one-line change.
		switch {
		case backup.IsArchiveUnreachable(err) || errors.Is(err, backup.ErrWALWait):
			return &ExitError{Code: 5, Msg: err.Error()}
		case errors.Is(err, backup.ErrPGProtocol):
			return &ExitError{Code: 3, Msg: err.Error()}
		case errors.Is(err, backup.ErrStorage):
			return &ExitError{Code: 4, Msg: err.Error()}
		default:
			return &ExitError{Code: 1, Msg: err.Error()}
		}
	}

	if _, err := fmt.Fprintf(out, "backup %s complete: %d files, %d bytes, start=%s stop=%s, duration=%s\n",
		res.BackupID, res.Files, res.Bytes, res.StartLSN, res.StopLSN, res.Duration); err != nil {
		return &ExitError{Code: 1, Msg: err.Error()}
	}
	// Multi-storage partial-failure surface: backup.Run returns nil error
	// when AT LEAST ONE backend committed (Invariant #10). The
	// caller counts backends that didn't make it into res.PartialStorages.
	// Surface that on stderr so a cron-driven backup running into a
	// silently-degraded multi-storage setup is not invisible.
	if res.PartialStorages > 0 {
		_, _ = fmt.Fprintf(os.Stderr,
			"pgsafe backup: WARNING: %d backend(s) failed to commit; backup is durable on the rest. "+
				"Investigate before relying on the failed storages for restore.\n",
			res.PartialStorages)
	}
	return nil
}

// parseAgeRecipients parses a slice of age public-key strings into age.Recipient
// values suitable for filter.NewChain. Returns the first parse error encountered.
func parseAgeRecipients(raw []string) ([]age.Recipient, error) {
	out := make([]age.Recipient, 0, len(raw))
	for _, s := range raw {
		r, err := age.ParseX25519Recipient(s)
		if err != nil {
			return nil, fmt.Errorf("parse age recipient %q: %w", s, err)
		}
		out = append(out, r)
	}
	return out, nil
}
