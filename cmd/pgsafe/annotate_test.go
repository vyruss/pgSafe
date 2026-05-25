package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
)

// TestAnnotateRoundTrip: annotate sets the note, info shows it.
func TestAnnotateRoundTrip(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	id := bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, _, stderr := runRoot(t, "annotate", id, "--config", cfgPath, "--note", "RC1 baseline")
	if rc != 0 {
		t.Fatalf("annotate exit = %d; stderr=%q", rc, stderr)
	}
	rc, stdout, _ := runRoot(t, "info", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("info exit = %d", rc)
	}
	if !strings.Contains(stdout, "RC1 baseline") {
		t.Errorf("info output missing annotation; got %q", stdout)
	}
}

// TestAnnotateClearsNote: re-annotating with --note "" clears the
// existing annotation.
func TestAnnotateClearsNote(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	id := bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "initial")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, _, _ := runRoot(t, "annotate", id, "--config", cfgPath, "--note", "")
	if rc != 0 {
		t.Fatalf("annotate (clear) exit = %d", rc)
	}
	rc, stdout, _ := runRoot(t, "info", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("info exit = %d", rc)
	}
	if strings.Contains(stdout, "initial") {
		t.Errorf("info output should NOT contain previous annotation 'initial' after clear; got %q", stdout)
	}
}

// TestAnnotateMissingBackupExitCode4: pointing at an unknown backup ID
// returns a storage-side error (exit 4).
func TestAnnotateMissingBackupExitCode4(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	if _, err := openPosixForTest(storage); err != nil {
		t.Fatalf("posix: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)

	rc, _, stderr := runRoot(t, "annotate", "20260101T000000F", "--config", cfgPath, "--note", "x")
	if rc != 4 {
		t.Errorf("annotate missing-backup exit = %d, want 4; stderr=%q", rc, stderr)
	}
}
