package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
)

// TestCheckEmptyStorageExitsZero: `pgsafe check` against an empty storage
// passes every applicable probe and exits 0.
func TestCheckEmptyStorageExitsZero(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	if _, err := openPosixForTest(storage); err != nil {
		t.Fatalf("posix: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)

	rc, stdout, stderr := runRoot(t, "check", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("check empty-storage exit = %d; stderr=%q stdout=%q", rc, stderr, stdout)
	}
	if !strings.Contains(stdout, "PASS") {
		t.Errorf("check stdout should report PASS lines; got %q", stdout)
	}
}

// TestCheckOrphanIncrementalExitCode5: an orphaned incremental
// fails the chain_integrity probe and the CLI exits 5.
func TestCheckOrphanIncrementalExitCode5(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	bb.AddOrphanedIncremental(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "20990101T000000F")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "check", "--config", cfgPath)
	if rc != 5 {
		t.Fatalf("check orphan-incremental exit = %d, want 5; stderr=%q stdout=%q", rc, stderr, stdout)
	}
	if !strings.Contains(stdout, "FAIL  chain_integrity") {
		t.Errorf("check should report chain_integrity FAIL; got %q", stdout)
	}
}

// TestCheckJSON: --json output decodes into the documented Report
// shape with the probes in stable order.
func TestCheckJSON(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	if _, err := openPosixForTest(storage); err != nil {
		t.Fatalf("posix: %v", err)
	}
	cfgPath := writeFixtureConfigWithStorage(t, storage)

	rc, stdout, _ := runRoot(t, "check", "--config", cfgPath, "--json")
	if rc != 0 {
		t.Fatalf("check --json exit = %d; stdout=%q", rc, stdout)
	}
	var got struct {
		Probes []struct {
			Name   string `json:"name"`
			OK     bool   `json:"ok"`
			Detail string `json:"detail"`
		} `json:"probes"`
	}
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("strict json.Decode: %v\noutput: %s", err, stdout)
	}
	if len(got.Probes) < 4 {
		t.Errorf("expected at least 4 probes; got %d", len(got.Probes))
	}
}
