package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

// TestInfoEmptyStorage: `pgsafe info --config X` against a freshly-opened
// storage prints "no backups" and exits 0.
func TestInfoEmptyStorage(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	if _, err := openPosixForTest(storage); err != nil {
		t.Fatalf("posix open: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)

	rc, stdout, stderr := runRoot(t, "info", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("info exit code = %d, want 0; stderr=%q", rc, stderr)
	}
	if !strings.Contains(stdout, "no backups") {
		t.Errorf("info stdout = %q, want contains 'no backups'", stdout)
	}
}

// TestInfoTableShowsChain: a synthetic full+incr+full chain renders
// with header columns, server name, and annotations visible.
func TestInfoTableShowsChain(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix open: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	full := bb.AddFull(t0, "")
	bb.AddIncremental(full, t0.Add(2*time.Hour), "RC1")
	bb.AddFull(t0.Add(24*time.Hour), "second-day")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "info", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("info exit code = %d; stderr=%q", rc, stderr)
	}
	for _, want := range []string{"ID", "TYPE", "SERVER", "yamlserver", "RC1", "second-day", "incremental"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("info stdout missing %q; got:\n%s", want, stdout)
		}
	}
}

// TestInfoJSONStrictDecode: the --json output round-trips strict JSON
// decoding with DisallowUnknownFields, so the schema is monitoring-
// stable.
func TestInfoJSONStrictDecode(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix open: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "rc1")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "info", "--config", cfgPath, "--json")
	if rc != 0 {
		t.Fatalf("info --json exit code = %d; stderr=%q", rc, stderr)
	}
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	var got struct {
		Backups  []map[string]any `json:"backups"`
		Warnings []map[string]any `json:"warnings"`
	}
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("strict json.Decode: %v\noutput: %s", err, stdout)
	}
	if len(got.Backups) != 1 {
		t.Fatalf("Backups length = %d, want 1", len(got.Backups))
	}
	if got.Backups[0]["server"] != "yamlserver" {
		t.Errorf("server = %v, want yamlserver", got.Backups[0]["server"])
	}
	if got.Backups[0]["annotation"] != "rc1" {
		t.Errorf("annotation = %v, want rc1", got.Backups[0]["annotation"])
	}
}

// TestInfoServerFilter: --server-filter NAME drops entries whose Server
// does not match.
func TestInfoServerFilter(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix open: %v", err)
	}
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb1 := backuptest.New(context.Background(), b, "yamlserver")
	bb1.AddFull(t0, "alpha")
	bb2 := backuptest.New(context.Background(), b, "other")
	bb2.AddFull(t0.Add(time.Hour), "bravo")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "info", "--config", cfgPath, "--server-filter", "yamlserver")
	if rc != 0 {
		t.Fatalf("info exit = %d; stderr=%q", rc, stderr)
	}
	if !strings.Contains(stdout, "alpha") {
		t.Errorf("filtered output should contain 'alpha'; got:\n%s", stdout)
	}
	if strings.Contains(stdout, "bravo") {
		t.Errorf("filtered output should NOT contain 'bravo' (other-server entry); got:\n%s", stdout)
	}
}

func openPosixForTest(root string) (*posix.Backend, error) {
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		return nil, err
	}
	if err := b.Open(context.Background()); err != nil {
		return nil, err
	}
	return b, nil
}
