// Package config parses, validates, and defaults the pgSafe YAML configuration.
//
// The full schema is documented  The
// public surface is exactly Config, Load, and Validate per that section.
package config

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"filippo.io/age"
	"gopkg.in/yaml.v3"
)

// Config is the in-memory shape of a pgSafe per-server configuration. Field names
// are exposed as-is to callers (CLI, caller); YAML keys are lowercase
// snake_case via the struct tags.
//
// Use `storages:` (a list) for all storage backends. Multiple backends enable
// the multi-storage invariant (#10): backup writes to all; restore reads from
// the first reachable.
type Config struct {
	Server      string            `yaml:"server"`
	PG          PGConfig          `yaml:"pg"`
	Storages    []StorageConfig   `yaml:"storages,omitempty"`
	Compression CompressionConfig `yaml:"compression"`
	Encryption  EncryptionConfig  `yaml:"encryption"`
	Log         LogConfig         `yaml:"log"`
	Backup      BackupConfig      `yaml:"backup,omitempty"`
}

// BackupConfig collects backup-caller-side knobs that aren't
// per-backend. Will grow as more caller knobs surface
// (parallelism caps, retry policy, resume).
type BackupConfig struct {
	// ArchiveTimeout bounds the caller's WAL-archive polling. It
	// covers two operations that share the same nature ("wait for a WAL
	// segment shipped by archive_command to land in the configured
	// storage"):
	//   1. The pre-`pg_backup_start` reachability probe.
	//   2. The post-`pg_backup_stop` WAL-wait that's the durability
	//      hand-off (Invariant #1).
	// Default 30 minutes — generous enough for cloud-archive paths
	// where transient backpressure can stall a single segment for
	// minutes. Operators with fast local archive can shorten it; ops
	// shops with very high WAL volume can extend it. Unbounded waits
	// are deliberately not allowed; a stuck archive_command shouldn't
	// hang a cron job forever. Mirrors pgbackrest's --archive-timeout.
	ArchiveTimeout time.Duration `yaml:"archive_timeout,omitempty"`

	// ScratchDir is the local filesystem directory pgsafe uses for
	// transient staging files (e.g. the parent manifest staged during
	// an incremental backup so pg_basebackup --incremental can mmap
	// it). Defaults to os.TempDir(). Containerised callers with
	// small /tmp should override it to a path with enough free space
	// for the largest cluster's pg_basebackup-format manifest.
	ScratchDir string `yaml:"scratch_dir,omitempty"`

	// Resume controls whether an interrupted prior backup is
	// continued in place rather than starting fresh. Default true.
	// CLI --no-resume forces a fresh backup even when a compatible
	// candidate exists. ARCHITECTURE.md D-021 describes the gate.
	Resume *bool `yaml:"resume,omitempty"`

	// ResumeCheckpointEveryN sets the per-file checkpoint cadence
	// for backup_manifest.copy. Lower = finer resume granularity at
	// the cost of more storage Puts; higher = coarser + cheaper.
	// Zero / unset → DefaultResumeCheckpointEveryN (10).
	ResumeCheckpointEveryN int `yaml:"resume_checkpoint_every_n_files,omitempty"`

	// ResumeGracePeriod is the maximum age of a .copy-only backup
	// directory that pgsafe prune will reclaim. Defaults to 24h.
	// Set to 0 to disable the prune sweep (operator manages cleanup).
	ResumeGracePeriod time.Duration `yaml:"resume_grace_period,omitempty"`
}

// PrimaryStorage returns the first storage backend config. It is the
// "source of truth" for single-storage commands (info, verify, prune,
// check, annotate, server lifecycle). Multi-storage backup uses the full
// Storages slice. Always valid after Load (Validate enforces len≥1).
func (c *Config) PrimaryStorage() StorageConfig {
	return c.Storages[0]
}

// PGConfig is the YAML `pg:` block — how the caller reaches the
// PostgreSQL cluster (libpq DSN + optional SSH details for pgSafe-mode).
type PGConfig struct {
	ConnString string `yaml:"conn_string"`
	Version    int    `yaml:"version"`

	// Host is the SSH target ("user@host[:port]") used to reach the
	// pg-host from the caller. Empty when the caller is on the pg-host
	// itself (PGDATA local) — the caller-relative way to declare
	// "PGDATA is remote-via-ssh." CLI --ssh-target overrides this.
	Host string `yaml:"host"`

	// SSHExtraArgs is a shell-quoted string of extra arguments for
	// /usr/bin/ssh (e.g. "-p 2222 -i ~/.ssh/id_ed25519"). CLI
	// --ssh-extra-args overrides this.
	SSHExtraArgs string `yaml:"ssh_extra_args"`

	// StorageReach controls how pgsafe decides whether the worker
	// can reach storage natively or whether the caller must proxy:
	//   ""           same as "auto"
	//   "auto"       probe at session start; fall back to caller-proxy
	//                if the worker can't reach storage. (default)
	//   "native_only" require direct worker→storage reach; abort if
	//                the probe fails. For ops shops that prefer
	//                hard failure over silent slow-path fallback.
	//   "via_caller" skip the probe; force caller-proxy mode.
	// See ARCHITECTURE.md "Operator footgun: accidental
	// proxying" for the rationale.
	StorageReach string `yaml:"storage_reach,omitempty"`
}

// StorageConfig discriminates by Type and inlines the per-backend sub-config.
// Exactly one of {Path, S3, Azure, GCS, SFTP} must be populated, matching Type.
type StorageConfig struct {
	Type string `yaml:"type"`

	// Path is the on-disk root for type=posix.
	Path string `yaml:"path"`

	// Cloud-backend sub-configs. Tagged ",omitempty" so a YAML file with
	// only the active backend's section round-trips cleanly.
	S3    *S3Config    `yaml:"s3,omitempty"`
	Azure *AzureConfig `yaml:"azure,omitempty"`
	GCS   *GCSConfig   `yaml:"gcs,omitempty"`
	SFTP  *SFTPConfig  `yaml:"sftp,omitempty"`
}

// S3Config covers the operator-supplied side of an S3 (or S3-compatible)
// backend. Credentials follow the standard AWS chain when AccessKey/SecretKey
// are empty (env vars, ~/.aws/credentials, IAM role).
type S3Config struct {
	Bucket          string `yaml:"bucket"`
	Region          string `yaml:"region"`
	Endpoint        string `yaml:"endpoint,omitempty"`       // for S3-compatible (MinIO, etc.)
	UsePathStyle    bool   `yaml:"use_path_style,omitempty"` // mandatory for MinIO
	AccessKeyID     string `yaml:"access_key_id,omitempty"`  // empty → AWS default chain
	SecretAccessKey string `yaml:"secret_access_key,omitempty"`
	Prefix          string `yaml:"prefix,omitempty"`
}

// AzureConfig covers Azure Blob Storage. Either AccountKey or SASToken must
// be set; ConnectionString overrides both. BlobEndpoint is for Azurite or
// other test emulators.
type AzureConfig struct {
	AccountName      string `yaml:"account_name"`
	Container        string `yaml:"container"`
	AccountKey       string `yaml:"account_key,omitempty"`
	SASToken         string `yaml:"sas_token,omitempty"`
	ConnectionString string `yaml:"connection_string,omitempty"`
	BlobEndpoint     string `yaml:"blob_endpoint,omitempty"`
	Prefix           string `yaml:"prefix,omitempty"`
}

// GCSConfig covers Google Cloud Storage. Empty CredentialsFile uses
// Application Default Credentials.
type GCSConfig struct {
	Bucket          string `yaml:"bucket"`
	CredentialsFile string `yaml:"credentials_file,omitempty"`
	Endpoint        string `yaml:"endpoint,omitempty"` // for fake-gcs-server
	Prefix          string `yaml:"prefix,omitempty"`
}

// SFTPConfig covers SFTP. Either Password or PrivateKeyFile must be set.
type SFTPConfig struct {
	Host                  string `yaml:"host"`
	Port                  int    `yaml:"port,omitempty"` // default 22
	Username              string `yaml:"username"`
	Password              string `yaml:"password,omitempty"`
	PrivateKeyFile        string `yaml:"private_key_file,omitempty"`
	BasePath              string `yaml:"base_path"`
	HostKey               string `yaml:"host_key,omitempty"` // for known_hosts pinning
	InsecureIgnoreHostKey bool   `yaml:"insecure_ignore_host_key,omitempty"`
}

// CompressionConfig is the YAML `compression:` block. Codec must be
// one of the values in validCodecs; Level is codec-specific (0 picks
// the codec default).
type CompressionConfig struct {
	Codec string `yaml:"codec"`
	Level int    `yaml:"level"`
}

// EncryptionConfig is the YAML `encryption:` block. Each recipient is
// an age public key (`age1...`); at least one is required.
type EncryptionConfig struct {
	Recipients []string `yaml:"recipients"`
}

// LogConfig is the YAML `log:` block. Format is `json` or `text`;
// Level is `debug`, `info`, `warn`, or `error`.
type LogConfig struct {
	Format string `yaml:"format"`
	Level  string `yaml:"level"`
}

// validCodecs, validStorageTypes, validPGVersions, and validLog{Formats,Levels}
// are the locked surfaces. Any value outside these sets is a Validate
// error; widening one is a deliberate design change with its own RFC.
var (
	validCodecs       = []string{"gzip", "lz4", "zstd", "bzip2"}
	validStorageTypes = []string{"posix", "s3", "azure", "gcs", "sftp"}
	validPGVersions   = []int{13, 14, 15, 16, 17, 18}
	validLogFormats   = []string{"json", "text"}
	validLogLevels    = []string{"debug", "info", "warn", "error"}
)

// validateBackend enforces that the right sub-config is populated for the
// declared Type and that its required fields are non-empty. Cross-checks
// (e.g. "exactly one of password/key") happen here too.
func (r *StorageConfig) validateBackend() error {
	// Reject sub-configs that don't match Type — guards against the operator
	// leaving stale credentials in YAML when switching backends.
	mismatched := []string{}
	if r.Type != "posix" && r.Path != "" {
		mismatched = append(mismatched, "path")
	}
	if r.Type != "s3" && r.S3 != nil {
		mismatched = append(mismatched, "s3")
	}
	if r.Type != "azure" && r.Azure != nil {
		mismatched = append(mismatched, "azure")
	}
	if r.Type != "gcs" && r.GCS != nil {
		mismatched = append(mismatched, "gcs")
	}
	if r.Type != "sftp" && r.SFTP != nil {
		mismatched = append(mismatched, "sftp")
	}
	if len(mismatched) > 0 {
		return fmt.Errorf("config: storage.type = %q but unrelated section(s) present: %s",
			r.Type, strings.Join(mismatched, ", "))
	}

	switch r.Type {
	case "posix":
		if r.Path == "" {
			return errors.New("config: storage.path must be non-empty for type=posix")
		}
		if !filepath.IsAbs(r.Path) {
			return fmt.Errorf("config: storage.path = %q must be an absolute path", r.Path)
		}
	case "s3":
		if r.S3 == nil {
			return errors.New("config: storage.s3 section required for type=s3")
		}
		if r.S3.Bucket == "" {
			return errors.New("config: storage.s3.bucket must be non-empty")
		}
		if r.S3.Region == "" {
			return errors.New("config: storage.s3.region must be non-empty")
		}
	case "azure":
		if r.Azure == nil {
			return errors.New("config: storage.azure section required for type=azure")
		}
		if r.Azure.AccountName == "" && r.Azure.ConnectionString == "" {
			return errors.New("config: storage.azure.account_name or connection_string required")
		}
		if r.Azure.Container == "" {
			return errors.New("config: storage.azure.container must be non-empty")
		}
		if r.Azure.AccountKey == "" && r.Azure.SASToken == "" && r.Azure.ConnectionString == "" {
			return errors.New("config: storage.azure requires one of account_key, sas_token, or connection_string")
		}
	case "gcs":
		if r.GCS == nil {
			return errors.New("config: storage.gcs section required for type=gcs")
		}
		if r.GCS.Bucket == "" {
			return errors.New("config: storage.gcs.bucket must be non-empty")
		}
	case "sftp":
		if r.SFTP == nil {
			return errors.New("config: storage.sftp section required for type=sftp")
		}
		if r.SFTP.Host == "" {
			return errors.New("config: storage.sftp.host must be non-empty")
		}
		if r.SFTP.Username == "" {
			return errors.New("config: storage.sftp.username must be non-empty")
		}
		if r.SFTP.BasePath == "" {
			return errors.New("config: storage.sftp.base_path must be non-empty")
		}
		if r.SFTP.Password == "" && r.SFTP.PrivateKeyFile == "" {
			return errors.New("config: storage.sftp requires one of password or private_key_file")
		}
		if r.SFTP.HostKey == "" && !r.SFTP.InsecureIgnoreHostKey {
			return errors.New("config: storage.sftp requires host_key (recommended) or insecure_ignore_host_key=true")
		}
	}
	return nil
}

// Load reads YAML from r and returns the parsed Config. Unknown fields are
// rejected loudly (yaml.Decoder.KnownFields(true)) so a typo in a key never
// silently degrades to a default.
func Load(r io.Reader) (*Config, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	c.applyDefaults()
	return &c, nil
}

// ApplyDefaults populates defaultable fields in-place. Load calls this
// implicitly; callers that build a Config from scratch (CLI-only /
// config-less mode) need the same defaults applied before Validate.
func (c *Config) ApplyDefaults() { c.applyDefaults() }

func (c *Config) applyDefaults() {
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Backup.ArchiveTimeout == 0 {
		c.Backup.ArchiveTimeout = 30 * time.Minute
	}
	if c.Backup.Resume == nil {
		t := true
		c.Backup.Resume = &t
	}
	if c.Backup.ResumeGracePeriod == 0 {
		c.Backup.ResumeGracePeriod = 24 * time.Hour
	}
}

// Validate enforces every constraint on a parsed Config. The first
// failing field is returned as a descriptive error; callers should treat any
// validation error as a config-level (exit code 2) failure.
func (c *Config) Validate() error {
	if c.Server == "" {
		return errors.New("config: server must be non-empty")
	}
	if c.PG.ConnString == "" {
		return errors.New("config: pg.conn_string must be non-empty")
	}
	if !slices.Contains(validPGVersions, c.PG.Version) {
		return fmt.Errorf("config: pg.version = %d; valid: %v", c.PG.Version, validPGVersions)
	}
	switch c.PG.StorageReach {
	case "", "auto", "native_only", "via_caller":
		// ok
	default:
		return fmt.Errorf("config: pg.storage_reach = %q; valid: auto, native_only, via_caller",
			c.PG.StorageReach)
	}
	if len(c.Storages) == 0 {
		return errors.New("config: at least one storage must be configured (use storage: or storages:)")
	}
	for i, s := range c.Storages {
		if !slices.Contains(validStorageTypes, s.Type) {
			return fmt.Errorf("config: storages[%d].type = %q; valid: %s",
				i, s.Type, strings.Join(validStorageTypes, ", "))
		}
		if err := s.validateBackend(); err != nil {
			if len(c.Storages) == 1 {
				return err // single backend: omit index for cleaner messages
			}
			return fmt.Errorf("config: storages[%d]: %w", i, err)
		}
	}
	if !slices.Contains(validCodecs, c.Compression.Codec) {
		return fmt.Errorf("config: compression.codec = %q; valid: %s",
			c.Compression.Codec, strings.Join(validCodecs, ", "))
	}
	if c.Compression.Level < 0 {
		return fmt.Errorf("config: compression.level = %d must be non-negative", c.Compression.Level)
	}
	if len(c.Encryption.Recipients) == 0 {
		return errors.New("config: encryption.recipients must declare at least one age public key")
	}
	for i, r := range c.Encryption.Recipients {
		if _, err := age.ParseX25519Recipient(r); err != nil {
			return fmt.Errorf("config: encryption.recipients[%d] = %q: %w", i, r, err)
		}
	}
	if !slices.Contains(validLogFormats, c.Log.Format) {
		return fmt.Errorf("config: log.format = %q; valid: %s",
			c.Log.Format, strings.Join(validLogFormats, ", "))
	}
	if !slices.Contains(validLogLevels, c.Log.Level) {
		return fmt.Errorf("config: log.level = %q; valid: %s",
			c.Log.Level, strings.Join(validLogLevels, ", "))
	}
	return nil
}
