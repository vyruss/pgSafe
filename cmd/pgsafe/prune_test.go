package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/info"
)

// TestPruneDryRunDoesNotDeleteAnything: --dry-run reports the plan
// but every backup remains on disk afterwards.
func TestPruneDryRunDoesNotDeleteAnything(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")
	bb.AddFull(t0.Add(24*time.Hour), "")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "prune", "--config", cfgPath, "--keep-fulls", "1", "--dry-run")
	if rc != 0 {
		t.Fatalf("prune --dry-run exit = %d; stderr=%q stdout=%q", rc, stderr, stdout)
	}
	if !strings.Contains(stdout, "dry-run") {
		t.Errorf("--dry-run output should include 'dry-run' marker; got %q", stdout)
	}
	recs, _, _ := info.List(context.Background(), b)
	if len(recs) != 2 {
		t.Errorf("dry-run should preserve all backups; got %d records", len(recs))
	}
}

// TestPruneActuallyDeletes: without --dry-run, expirable backups go.
func TestPruneActuallyDeletes(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")
	bb.AddFull(t0.Add(24*time.Hour), "")
	bb.AddFull(t0.Add(48*time.Hour), "")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "prune", "--config", cfgPath, "--keep-fulls", "1")
	if rc != 0 {
		t.Fatalf("prune exit = %d; stderr=%q stdout=%q", rc, stderr, stdout)
	}
	recs, _, _ := info.List(context.Background(), b)
	if len(recs) != 1 {
		t.Errorf("after prune: expected 1 surviving backup; got %d (%v)", len(recs), recs)
	}
}

// TestPruneEmptyPolicyExitCode8: invoking prune with no --keep-* flags
// returns exit 8 (retention safety) without touching the storage.
func TestPruneEmptyPolicyExitCode8(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, _, stderr := runRoot(t, "prune", "--config", cfgPath)
	if rc != 8 {
		t.Errorf("empty-policy prune exit = %d, want 8; stderr=%q", rc, stderr)
	}
	// Storage intact.
	if _, err := os.Stat(storage); err != nil {
		t.Errorf("storage should still exist; got %v", errors.Unwrap(err))
	}
}

// TestPruneJSONOutput: --json produces a parseable summary.
func TestPruneJSONOutput(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")
	bb.AddFull(t0.Add(24*time.Hour), "")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "prune", "--config", cfgPath, "--keep-fulls", "1", "--json", "--dry-run")
	if rc != 0 {
		t.Fatalf("prune --json exit = %d; stderr=%q", rc, stderr)
	}
	for _, want := range []string{`"dry_run"`, `"plan"`, `"result"`, `"backups_deleted"`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("prune --json missing %q; got %q", want, stdout)
		}
	}
}
