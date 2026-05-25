// Endpoint tests for the pgsafe CLI surface, per
//
// Every subcommand has a test that exercises:
//   - the happy "stub returns the documented not-implemented error" path,
//   - the exit code mapping defined in §3.4,
//   - the §3.2.8 flag→YAML precedence rule.
package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/lock"
)

const validYAML = `
server: yamlserver
pg:
  conn_string: "host=localhost port=5432 user=pgsafe dbname=postgres"
  version: 18
storages:
  - type: posix
    path: /var/lib/pgsafe/storage
compression:
  codec: zstd
  level: 3
encryption:
  recipients:
    - "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"
log:
  format: json
  level: info
`

// replaceServerAddRunE finds the `server add` subcommand in root and swaps
// its RunE so the precedence tests can capture the resolved config without
// actually executing the verb.
func replaceServerAddRunE(root *cobra.Command, runE func(cmd *cobra.Command, _ []string) error) {
	for _, top := range root.Commands() {
		if top.Use != "server" {
			continue
		}
		for _, sub := range top.Commands() {
			if sub.Use == "add" {
				sub.RunE = runE
				return
			}
		}
	}
}

// runRoot drives a fresh root command tree with the given args and returns
// the resulting exit code together with whatever was written to stdout/stderr.
func runRoot(t *testing.T, args ...string) (rc int, stdout, stderr string) {
	t.Helper()
	root := newRootCmd()
	var sout, serr bytes.Buffer
	root.SetOut(&sout)
	root.SetErr(&serr)
	root.SetArgs(args)
	err := root.Execute()
	return errExit(err), sout.String(), serr.String()
}

// writeFixtureConfig writes validYAML to a tempfile and returns its path.
func writeFixtureConfig(t *testing.T) string {
	t.Helper()
	return writeFixtureConfigWithStorage(t, "/var/lib/pgsafe/storage")
}

// writeFixtureConfigWithStorage writes validYAML with the given storage.path to a
// tempfile and returns the config path. Use this in tests that actually do
// storage I/O so each test gets its own scratch directory.
func writeFixtureConfigWithStorage(t *testing.T, repoPath string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := strings.Replace(validYAML, "/var/lib/pgsafe/storage", repoPath, 1)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write fixture config: %v", err)
	}
	return path
}

func TestRootHelpExitsZero(t *testing.T) {
	t.Parallel()
	rc, stdout, _ := runRoot(t, "--help")
	if rc != 0 {
		t.Errorf("--help exit code = %d, want 0", rc)
	}
	if !strings.Contains(stdout, "PostgreSQL backup tool") {
		t.Errorf("--help stdout missing description; got %q", stdout)
	}
}

func TestRootVersionExitsZero(t *testing.T) {
	t.Parallel()
	rc, stdout, _ := runRoot(t, "--version")
	if rc != 0 {
		t.Errorf("--version exit code = %d, want 0", rc)
	}
	if !strings.Contains(stdout, "v") {
		t.Errorf("--version stdout missing 'v'; got %q", stdout)
	}
}

func TestRootUnknownFlagExits2(t *testing.T) {
	t.Parallel()
	rc, _, _ := runRoot(t, "--no-such-flag")
	if rc != 2 {
		t.Errorf("--no-such-flag exit code = %d, want 2", rc)
	}
}

func TestServerAddRequiresConfig(t *testing.T) {
	t.Parallel()
	rc, _, stderr := runRoot(t, "server", "add")
	if rc != 2 {
		t.Errorf("missing --config exit code = %d, want 2", rc)
	}
	if !strings.Contains(stderr, "--config") {
		t.Errorf("stderr should name --config; got %q", stderr)
	}
}

func TestServerAddHappyPath(t *testing.T) {
	t.Parallel()
	repoDir := filepath.Join(t.TempDir(), "storage")
	cfg := writeFixtureConfigWithStorage(t, repoDir)

	rc, stdout, stderr := runRoot(t, "server", "add", "--config", cfg)
	if rc != 0 {
		t.Fatalf("exit = %d, want 0; stderr=%q", rc, stderr)
	}
	if !strings.Contains(stdout, "yamlserver") {
		t.Errorf("stdout missing server name; got %q", stdout)
	}

	// Sidecar file lands at the locked path.
	sidecar := filepath.Join(repoDir, "Storage-Metadata.json")
	data, err := os.ReadFile(sidecar) //nolint:gosec
	if err != nil {
		t.Fatalf("sidecar missing: %v", err)
	}
	if !strings.Contains(string(data), `"server": "yamlserver"`) {
		t.Errorf("sidecar content unexpected: %s", data)
	}

	// wal/ subdirectory created.
	if _, err := os.Stat(filepath.Join(repoDir, "wal")); err != nil {
		t.Errorf("wal/ missing: %v", err)
	}
}

func TestServerAddRefusesOverwrite(t *testing.T) {
	t.Parallel()
	repoDir := filepath.Join(t.TempDir(), "storage")
	cfg := writeFixtureConfigWithStorage(t, repoDir)

	if rc, _, stderr := runRoot(t, "server", "add", "--config", cfg); rc != 0 {
		t.Fatalf("first init rc = %d; stderr=%q", rc, stderr)
	}
	rc, _, stderr := runRoot(t, "server", "add", "--config", cfg)
	if rc != 2 {
		t.Errorf("second init rc = %d, want 2; stderr=%q", rc, stderr)
	}
	if !strings.Contains(stderr, "already exists") {
		t.Errorf("stderr should explain refusal; got %q", stderr)
	}
}

// TestBackupConnectFailureExitCode3 covers the §3.4 mapping: PG-side errors
// (here: bad credentials) get exit code 3.
//
// The fixture YAML's conn_string targets localhost:5432 with a non-existent
// user, so without a PG server the connection refusal also produces exit
// code 3 (or 2 depending on whether a server is listening).
func TestBackupConnectFailureExitCode3(t *testing.T) {
	t.Parallel()
	// Use a writable storage path so the per-server lock acquisition (which
	// runs before PG connect) succeeds and the test exercises its
	// intended failure mode — PG connect — rather than mkdir-failed.
	cfg := writeFixtureConfigWithStorage(t, t.TempDir())
	rc, _, stderr := runRoot(t, "backup", "--config", cfg)
	if rc != 3 {
		t.Errorf("exit code = %d, want 3 (PG-side failure); stderr=%q", rc, stderr)
	}
}

func TestBackupIncrRequiresParent(t *testing.T) {
	t.Parallel()
	cfg := writeFixtureConfig(t)
	rc, _, stderr := runRoot(t, "backup", "--type", "incr", "--config", cfg)
	if rc != 2 {
		t.Errorf("exit code = %d, want 2 (config error); stderr=%q", rc, stderr)
	}
	if !strings.Contains(stderr, "--parent") {
		t.Errorf("--type=incr stderr should mention --parent; got %q", stderr)
	}
}

// TestBackupLockTimeoutExitCode9 proves the per-server lock (Invariant
// #4) blocks a concurrent backup and the timeout maps to exit 9. We
// hold the lock from the test goroutine via the same flock primitive
// the CLI uses, then run `pgsafe backup --lock-timeout=200ms` and
// expect ErrLockTimeout to surface as exit 9.
func TestBackupLockTimeoutExitCode9(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	cfgPath := writeFixtureConfigWithStorage(t, storage)

	// Hold the per-server lock from the test goroutine. The fixture's
	// server name is `yamlserver` (see validYAML) and storage type is
	// posix, so the lock path follows serverLockPath()'s posix branch.
	holder := lock.NewPosix(serverLockPath(config.StorageConfig{Type: "posix", Path: storage}, "yamlserver"))
	if err := holder.Acquire(context.Background(), lock.Exclusive, 5*time.Second); err != nil {
		t.Fatalf("test holder Acquire: %v", err)
	}
	defer func() { _ = holder.Release() }()

	rc, _, stderr := runRoot(t, "backup", "--config", cfgPath, "--lock-timeout", "200ms")
	if rc != 9 {
		t.Errorf("exit code = %d, want 9 (lock-acquisition timeout); stderr=%q", rc, stderr)
	}
}

func TestBackupParentRejectedOnFull(t *testing.T) {
	t.Parallel()
	cfg := writeFixtureConfig(t)
	rc, _, stderr := runRoot(t, "backup", "--parent", "abc", "--config", cfg)
	if rc != 2 {
		t.Errorf("exit code = %d, want 2 (config error); stderr=%q", rc, stderr)
	}
	if !strings.Contains(stderr, "--parent only valid") {
		t.Errorf("stderr should explain --parent restriction; got %q", stderr)
	}
}

func TestBackupBadType(t *testing.T) {
	t.Parallel()
	cfg := writeFixtureConfig(t)
	rc, _, stderr := runRoot(t, "backup", "--type", "diff", "--config", cfg)
	if rc != 2 {
		t.Errorf("--type=diff exit code = %d, want 2", rc)
	}
	if !strings.Contains(stderr, "full or incr") {
		t.Errorf("stderr should explain valid types; got %q", stderr)
	}
}

func TestRestoreRequiresTarget(t *testing.T) {
	t.Parallel()
	cfg := writeFixtureConfig(t)
	rc, _, stderr := runRoot(t, "restore", "--config", cfg)
	if rc != 2 {
		t.Errorf("missing --target exit code = %d, want 2", rc)
	}
	if !strings.Contains(stderr, "--target") {
		t.Errorf("stderr should name --target; got %q", stderr)
	}
}

// TestRestoreRequiresIdentityFile asserts §3.2.8: restore needs an age
// identity to decrypt; missing --identity-file is a usage error (exit 2).
func TestRestoreRequiresIdentityFile(t *testing.T) {
	t.Parallel()
	cfg := writeFixtureConfig(t)
	rc, _, stderr := runRoot(t, "restore", "--config", cfg, "--target", "/tmp/nope")
	if rc != 2 {
		t.Errorf("exit code = %d, want 2 (usage error)", rc)
	}
	if !strings.Contains(stderr, "--identity-file") {
		t.Errorf("stderr should name --identity-file; got %q", stderr)
	}
}

func TestConfigInvalidYAMLExits2(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("server: [unclosed"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	rc, _, _ := runRoot(t, "server", "add", "--config", path)
	if rc != 2 {
		t.Errorf("malformed YAML exit code = %d, want 2", rc)
	}
}

func TestConfigMissingFileExits2(t *testing.T) {
	t.Parallel()
	rc, _, _ := runRoot(t, "server", "add", "--config", "/nonexistent.yaml")
	if rc != 2 {
		t.Errorf("missing config exit code = %d, want 2", rc)
	}
}

// TestResolveConfigCLIOverride asserts the §3.2.8 precedence rule directly
// against the resolveConfig helper. CLI flags beat YAML; YAML beats default.
func TestResolveConfigCLIOverride(t *testing.T) {
	t.Parallel()
	cfgPath := writeFixtureConfig(t)

	root := newRootCmd()
	root.SetArgs([]string{
		"server", "add",
		"--config", cfgPath,
		"--server", "cliserver",
		"--log-level", "warn",
		"--log-format", "text",
	})
	// We don't actually want `server add` to run; replace its RunE with
	// one that captures the resolved config and returns nil.
	var captured struct {
		Server, LogLevel, LogFormat string
	}
	replaceServerAddRunE(root, func(cmd *cobra.Command, _ []string) error {
		resolved, err := resolveConfig(cmd)
		if err != nil {
			return err
		}
		captured.Server = resolved.Server
		captured.LogLevel = resolved.Log.Level
		captured.LogFormat = resolved.Log.Format
		return nil
	})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if captured.Server != "cliserver" {
		t.Errorf("CLI server not applied: got %q", captured.Server)
	}
	if captured.LogLevel != "warn" {
		t.Errorf("CLI log-level not applied: got %q", captured.LogLevel)
	}
	if captured.LogFormat != "text" {
		t.Errorf("CLI log-format not applied: got %q", captured.LogFormat)
	}
}

// TestResolveConfigYAMLDefault asserts that without CLI overrides, YAML
// values flow through unchanged.
func TestResolveConfigYAMLDefault(t *testing.T) {
	t.Parallel()
	cfgPath := writeFixtureConfig(t)

	root := newRootCmd()
	root.SetArgs([]string{"server", "add", "--config", cfgPath})

	var capturedServer, capturedLevel string
	replaceServerAddRunE(root, func(cmd *cobra.Command, _ []string) error {
		resolved, err := resolveConfig(cmd)
		if err != nil {
			return err
		}
		capturedServer = resolved.Server
		capturedLevel = resolved.Log.Level
		return nil
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if capturedServer != "yamlserver" {
		t.Errorf("YAML server not observed: got %q", capturedServer)
	}
	if capturedLevel != "info" {
		t.Errorf("YAML log-level not observed: got %q", capturedLevel)
	}
}
