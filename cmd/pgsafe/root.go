package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/restore"
	"github.com/vyruss/pgsafe/internal/storage"
)

// sidecarFilename is the locked on-disk name of pgSafe's Storage-Metadata
// JSON sidecar ().
const sidecarFilename = "Storage-Metadata.json"

// version is set at build time via -ldflags '-X main.version=...'. The Cycle-0
// default stays as a development sentinel until the first tagged release.
var version = "v0.0.0-dev" //nolint:gochecknoglobals

// ExitError is the typed error a subcommand returns when it wants to dictate
// the process exit code, per
type ExitError struct {
	Code int
	Msg  string
}

func (e *ExitError) Error() string { return e.Msg }

func errCfg(format string, a ...any) error {
	return &ExitError{Code: 2, Msg: fmt.Sprintf(format, a...)}
}

// newRootCmd builds a fresh root command tree. Tests call this and drive it
// via SetArgs / SetOut / SetErr so they share no global state with main().
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "pgsafe",
		Short:         "PostgreSQL backup tool",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.PersistentFlags().String("config", "", "path to YAML config; omit to build one from --pg-* / --storage-* / --encryption-* flags (config-less mode, mirrors pgbackrest's --no-config)")
	root.PersistentFlags().String("server", "", "server name; overrides config when --config is set, required when --config is empty")
	root.PersistentFlags().String("log-level", "", "log level (debug|info|warn|error); overrides config when --config is set")
	root.PersistentFlags().String("log-format", "", "log format (json|text); overrides config when --config is set")
	// Config-less flags (used only when --config is empty). Each maps
	// directly to the corresponding YAML field — same precedence as
	// --config + flag overlays, just inverted: when --config is set the
	// YAML is the base and these are ignored; when empty they ARE the
	// config. Mirrors pgbackrest's CLI-only invocation pattern.
	root.PersistentFlags().String("pg-conn-string", "", "libpq URI for the PG cluster (config-less mode)")
	root.PersistentFlags().Int("pg-version", 0, "PG major version, 13..18 (config-less mode); leave 0 to auto-detect from server_version")
	root.PersistentFlags().String("pg-host", "", "SSH target user@host[:port] when caller != pghost (config-less mode); empty = PG-native libpq")
	root.PersistentFlags().String("storage-type", "", "storage backend type: posix|s3|azure|gcs|sftp (config-less mode); only posix is supported in config-less mode for now")
	root.PersistentFlags().String("storage-path", "", "storage root directory for storage-type=posix (config-less mode)")
	root.PersistentFlags().StringSlice("encryption-recipient", nil, "age public key (repeatable; config-less mode)")
	root.PersistentFlags().String("compression-codec", "zstd", "compression codec: zstd|gzip|none (config-less mode)")
	root.PersistentFlags().Int("compression-level", 3, "compression level (config-less mode)")

	root.AddCommand(
		newServerCmd(),
		newBackupCmd(),
		newRestoreCmd(),
		newInfoCmd(),
		newVerifyCmd(),
		newPruneCmd(),
		newCheckCmd(),
		newAnnotateCmd(),
		newWorkerCmd(),
		newArchivePushCmd(),
		newArchiveGetCmd(),
	)
	return root
}

// resolveConfig produces a validated Config from EITHER a YAML file
// (--config=<path> + flag overlays) OR a flag-only invocation (--config
// empty, build everything from --pg-* / --storage-* / --encryption-*).
// The flag-only path mirrors pgbackrest's --no-config + bare flags
// pattern; useful for ad-hoc backups, scripted runs, container
// entrypoints where a config file is awkward.
func resolveConfig(cmd *cobra.Command) (*config.Config, error) {
	configPath, _ := cmd.Flags().GetString("config")
	if configPath == "" {
		return resolveConfigFromFlags(cmd)
	}
	f, err := os.Open(configPath) //nolint:gosec // operator-supplied path by design
	if err != nil {
		return nil, errCfg("--config: %v", err)
	}
	defer func() { _ = f.Close() }()

	cfg, err := config.Load(f)
	if err != nil {
		return nil, errCfg("%v", err)
	}

	if v, _ := cmd.Flags().GetString("server"); v != "" {
		cfg.Server = v
	}
	if v, _ := cmd.Flags().GetString("log-level"); v != "" {
		cfg.Log.Level = v
	}
	if v, _ := cmd.Flags().GetString("log-format"); v != "" {
		cfg.Log.Format = v
	}

	if err := cfg.Validate(); err != nil {
		return nil, errCfg("%v", err)
	}
	return cfg, nil
}

// resolveConfigFromFlags builds a Config purely from CLI flags. Required
// fields surface as a single aggregated error so the operator gets the
// full punch list at once rather than one-flag-at-a-time. POSIX is the
// only storage type supported in config-less mode for now — cloud
// backends carry too many auth knobs to fit cleanly on the CLI; their
// auth chains are exactly what makes a YAML config valuable.
func resolveConfigFromFlags(cmd *cobra.Command) (*config.Config, error) {
	server, _ := cmd.Flags().GetString("server")
	pgConn, _ := cmd.Flags().GetString("pg-conn-string")
	pgVersion, _ := cmd.Flags().GetInt("pg-version")
	pgHost, _ := cmd.Flags().GetString("pg-host")
	storageType, _ := cmd.Flags().GetString("storage-type")
	storagePath, _ := cmd.Flags().GetString("storage-path")
	recipients, _ := cmd.Flags().GetStringSlice("encryption-recipient")
	compCodec, _ := cmd.Flags().GetString("compression-codec")
	compLevel, _ := cmd.Flags().GetInt("compression-level")
	logLevel, _ := cmd.Flags().GetString("log-level")
	logFormat, _ := cmd.Flags().GetString("log-format")

	var missing []string
	if server == "" {
		missing = append(missing, "--server")
	}
	if pgConn == "" {
		missing = append(missing, "--pg-conn-string")
	}
	if storageType == "" {
		storageType = "posix" // sensible default — most config-less use cases
	}
	if storageType != "posix" {
		return nil, errCfg("--storage-type=%q: only posix is supported in config-less mode (cloud backends require --config)", storageType)
	}
	if storagePath == "" {
		missing = append(missing, "--storage-path")
	}
	if len(recipients) == 0 {
		missing = append(missing, "--encryption-recipient")
	}
	if len(missing) > 0 {
		return nil, errCfg("config-less mode requires: %s (or pass --config=<yaml-path>)", strings.Join(missing, ", "))
	}

	cfg := &config.Config{
		Server: server,
		PG: config.PGConfig{
			ConnString: pgConn,
			Version:    pgVersion,
			Host:       pgHost,
		},
		Storages: []config.StorageConfig{
			{Type: storageType, Path: storagePath},
		},
		Compression: config.CompressionConfig{Codec: compCodec, Level: compLevel},
		Encryption:  config.EncryptionConfig{Recipients: recipients},
		Log:         config.LogConfig{Level: logLevel, Format: logFormat},
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, errCfg("%v", err)
	}
	return cfg, nil
}

// newServerCmd is the parent of all per-server lifecycle subcommands.
// shipped `server add`; adds `server list`,
// `server upgrade`, `server delete`.
func newServerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Manage backed-up servers",
	}
	cmd.AddCommand(
		newServerAddCmd(),
		newServerListCmd(),
		newServerUpgradeCmd(),
		newServerDeleteCmd(),
	)
	return cmd
}

func newServerAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add",
		Short: "Initialize the storage for a server",
		Long:  "Create the on-disk storage directory for a server, including the Storage-Metadata.json sidecar. Refuses to overwrite an existing storage.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			return runServerAdd(cmd.Context(), cfg, cmd.OutOrStdout())
		},
	}
}

// runServerAdd is the verb for `pgsafe server add`. Refuses to clobber an
// existing sidecar; a `server upgrade` command is the path for
// changing one in-place.
func runServerAdd(ctx context.Context, cfg *config.Config, out io.Writer) error {
	backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	defer cleanup()
	if _, err := backend.Stat(ctx, sidecarFilename); err == nil {
		return errCfg("server add: %s/%s already exists; refusing to overwrite", storageLocation(cfg.PrimaryStorage()), sidecarFilename)
	} else if !errors.Is(err, os.ErrNotExist) {
		return &ExitError{Code: 4, Msg: err.Error()}
	}

	sc := manifest.Sidecar{
		Version:              manifest.SidecarVersion,
		Server:               cfg.Server,
		EncryptionRecipients: cfg.Encryption.Recipients,
		Compression:          fmt.Sprintf("%s:%d", cfg.Compression.Codec, cfg.Compression.Level),
		StorageLayoutVersion: 1,
	}
	data, err := manifest.MarshalSidecar(sc)
	if err != nil {
		return &ExitError{Code: 1, Msg: err.Error()}
	}

	wc, err := backend.Put(ctx, sidecarFilename)
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	if _, err := wc.Write(data); err != nil {
		_ = wc.Close()
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	if err := wc.Close(); err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}

	if _, err := fmt.Fprintf(out, "server %q initialized at %s\n", cfg.Server, storageLocation(cfg.PrimaryStorage())); err != nil {
		return &ExitError{Code: 1, Msg: err.Error()}
	}
	return nil
}

// storageLocation returns a human-readable description of where the backend
// lives, for status/error messages. POSIX gets the path; cloud backends get
// a type-prefixed identifier.
func storageLocation(c config.StorageConfig) string {
	switch c.Type {
	case "posix":
		return c.Path
	case "s3":
		return fmt.Sprintf("s3://%s", c.S3.Bucket)
	case "azure":
		return fmt.Sprintf("azure://%s/%s", c.Azure.AccountName, c.Azure.Container)
	case "gcs":
		return fmt.Sprintf("gs://%s", c.GCS.Bucket)
	case "sftp":
		return fmt.Sprintf("sftp://%s@%s%s", c.SFTP.Username, c.SFTP.Host, c.SFTP.BasePath)
	default:
		return c.Type
	}
}

func newRestoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a backup into a target directory",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := resolveConfig(cmd)
			if err != nil {
				return err
			}
			target, _ := cmd.Flags().GetString("target")
			if target == "" {
				return errCfg("restore: --target is required")
			}
			identityFile, _ := cmd.Flags().GetString("identity-file")
			if identityFile == "" {
				return errCfg("restore: --identity-file is required")
			}
			return runRestore(cmd.Context(), cmd, cfg, target, identityFile, cmd.OutOrStdout())
		},
	}
	cmd.Flags().String("target", "", "directory to restore into (required, absolute path)")
	cmd.Flags().String("identity-file", "", "path to age identity (private-key) file (required)")
	cmd.Flags().String("backup-id", "", "specific backup ID to restore (default: latest)")
	cmd.Flags().Int("workers", 4, "parallel restore workers; --workers=1 for single-connection behaviour")
	cmd.Flags().StringArray("tablespace", nil, "remap a tablespace as OID=PATH (repeatable)")
	cmd.Flags().Bool("standby", false, "produce a standby (writes standby.signal instead of recovery.signal)")
	cmd.Flags().String("restore-command", "", "override PG restore_command (default: /bin/false sentinel)")

	// PITR target group — at most one may be set; CLI enforces it.
	cmd.Flags().String("target-time", "", "PITR target: ISO 8601 timestamp")
	cmd.Flags().String("target-xid", "", "PITR target: transaction ID")
	cmd.Flags().String("target-lsn", "", "PITR target: LSN in hi/lo hex form")
	cmd.Flags().String("target-name", "", "PITR target: named restore point")
	cmd.Flags().String("target-action", "", "after PITR target: pause | promote | shutdown (default pause)")
	return cmd
}

func runRestore(ctx context.Context, cmd *cobra.Command, cfg *config.Config, target, identityFile string, out io.Writer) error {
	backend, cleanup, err := openBackend(ctx, cfg.PrimaryStorage())
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}
	defer cleanup()

	identities, err := restore.LoadIdentityFile(identityFile)
	if err != nil {
		return errCfg("%v", err)
	}

	opts, err := buildRestoreOptions(cmd, backend, target, identities)
	if err != nil {
		return errCfg("%v", err)
	}

	res, err := restore.Run(ctx, opts)
	if err != nil {
		return &ExitError{Code: 4, Msg: err.Error()}
	}

	if _, err := fmt.Fprintf(out, "restore %s: %d files, %d WAL segments, %d bytes\n",
		res.BackupID, res.Files, res.WAL, res.Bytes); err != nil {
		return &ExitError{Code: 1, Msg: err.Error()}
	}
	return nil
}

// buildRestoreOptions collects the restore flags from cobra into
// a restore.Options. Returns a "usage error" suitable for exit code 2 when
// flags are mutually-exclusive-violating or malformed.
func buildRestoreOptions(cmd *cobra.Command, backend storage.Backend, target string, identities []age.Identity) (restore.Options, error) {
	opts := restore.Options{
		Backend:    backend,
		Target:     target,
		Identities: identities,
	}
	if id, _ := cmd.Flags().GetString("backup-id"); id != "" {
		opts.BackupID = id
	}
	if w, _ := cmd.Flags().GetInt("workers"); w > 0 {
		opts.Workers = w
	}
	if cmd.Flags().Changed("standby") {
		s, _ := cmd.Flags().GetBool("standby")
		opts.StandbyMode = s
	}
	if rc, _ := cmd.Flags().GetString("restore-command"); rc != "" {
		opts.RestoreCommand = rc
	}

	tablespaces, _ := cmd.Flags().GetStringArray("tablespace")
	if len(tablespaces) > 0 {
		opts.TablespaceRemap = make(map[uint64]string, len(tablespaces))
		for _, kv := range tablespaces {
			eq := strings.Index(kv, "=")
			if eq <= 0 {
				return restore.Options{}, fmt.Errorf("--tablespace %q must be OID=PATH", kv)
			}
			var oid uint64
			if _, err := fmt.Sscanf(kv[:eq], "%d", &oid); err != nil {
				return restore.Options{}, fmt.Errorf("--tablespace %q: parse OID: %w", kv, err)
			}
			opts.TablespaceRemap[oid] = kv[eq+1:]
		}
	}

	// PITR group: enforce mutual exclusion and parse the chosen one.
	pitrSet := 0
	if v, _ := cmd.Flags().GetString("target-time"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return restore.Options{}, fmt.Errorf("--target-time %q: %w", v, err)
		}
		opts.TargetTime = &t
		pitrSet++
	}
	if v, _ := cmd.Flags().GetString("target-xid"); v != "" {
		var xid uint64
		if _, err := fmt.Sscanf(v, "%d", &xid); err != nil {
			return restore.Options{}, fmt.Errorf("--target-xid %q: parse: %w", v, err)
		}
		opts.TargetXID = &xid
		pitrSet++
	}
	if v, _ := cmd.Flags().GetString("target-lsn"); v != "" {
		lsn, err := manifest.ParseLSN(v)
		if err != nil {
			return restore.Options{}, fmt.Errorf("--target-lsn %q: %w", v, err)
		}
		opts.TargetLSN = &lsn
		pitrSet++
	}
	if v, _ := cmd.Flags().GetString("target-name"); v != "" {
		opts.TargetName = v
		pitrSet++
	}
	if pitrSet > 1 {
		return restore.Options{}, errors.New("at most one of --target-time / --target-xid / --target-lsn / --target-name may be set")
	}
	if v, _ := cmd.Flags().GetString("target-action"); v != "" {
		opts.TargetAction = v
	}

	return opts, nil
}

// errExit unwraps to the int exit code for an error returned from cobra.
// It returns 2 for any unrecognized error (cobra flag/parse errors).
func errExit(err error) int {
	if err == nil {
		return 0
	}
	var ee *ExitError
	if errors.As(err, &ee) {
		return ee.Code
	}
	return 2
}
