package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
)

// TestServerListEmptyAfterAdd: `server add` then `server list` shows
// the new server with 0 backups.
func TestServerListEmptyAfterAdd(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	if _, err := openPosixForTest(storage); err != nil {
		t.Fatalf("posix: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)

	rc, _, stderr := runRoot(t, "server", "add", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("server add exit = %d; stderr=%q", rc, stderr)
	}
	rc, stdout, _ := runRoot(t, "server", "list", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("server list exit = %d", rc)
	}
	if !strings.Contains(stdout, "yamlserver") {
		t.Errorf("server list should mention yamlserver; got %q", stdout)
	}
	if !strings.Contains(stdout, "0 backup") {
		t.Errorf("server list should report '0 backup(s)' for fresh server; got %q", stdout)
	}
}

// TestServerListCountsBackups: synthetic backups plus the root
// sidecar produce a count >= number of backups.
func TestServerListCountsBackups(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, _, _ := runRoot(t, "server", "add", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("server add: %d", rc)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")
	bb.AddFull(t0.Add(24*time.Hour), "")

	rc, stdout, _ := runRoot(t, "server", "list", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("server list: %d", rc)
	}
	if !strings.Contains(stdout, "2 backup") {
		t.Errorf("server list should report 2 backups; got %q", stdout)
	}
}

// TestServerUpgradeRewritesSidecar: editing config compression then
// `server upgrade` rewrites the root sidecar with the new value.
func TestServerUpgradeRewritesSidecar(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	if _, err := openPosixForTest(storage); err != nil {
		t.Fatalf("posix: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, _, _ := runRoot(t, "server", "add", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("server add: %d", rc)
	}
	// Sanity: server upgrade succeeds against the existing sidecar.
	rc, stdout, stderr := runRoot(t, "server", "upgrade", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("server upgrade exit = %d; stderr=%q", rc, stderr)
	}
	if !strings.Contains(stdout, "upgraded") {
		t.Errorf("upgrade output should mention 'upgraded'; got %q", stdout)
	}
}

// TestServerDeleteRefusesWithoutForce: an existing backup blocks a
// non-forced delete; --force proceeds and removes everything.
func TestServerDeleteRefusesWithoutForce(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, _, _ := runRoot(t, "server", "add", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("server add: %d", rc)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	// Without --force: refuse.
	rc, _, stderr := runRoot(t, "server", "delete", "--config", cfgPath)
	if rc != 2 {
		t.Errorf("server delete (no --force) exit = %d, want 2 (config error); stderr=%q", rc, stderr)
	}

	// With --force: proceed.
	rc, stdout, stderr := runRoot(t, "server", "delete", "--config", cfgPath, "--force")
	if rc != 0 {
		t.Fatalf("server delete --force exit = %d; stderr=%q", rc, stderr)
	}
	if !strings.Contains(stdout, "deleted") {
		t.Errorf("server delete --force output should say 'deleted'; got %q", stdout)
	}

	// Storage's root sidecar should be gone.
	if _, err := os.Stat(storage + "/Storage-Metadata.json"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("root sidecar still exists after server delete: %v", err)
	}
}
