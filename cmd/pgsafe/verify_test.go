package main

import (
	"context"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

// TestVerifyHappyPathExitsZero: untouched synthetic backup verifies
// cleanly and exits 0 with an "OK" line per backup.
func TestVerifyHappyPathExitsZero(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix open: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "verify", "--config", cfgPath)
	if rc != 0 {
		t.Fatalf("verify exit = %d, want 0; stderr=%q stdout=%q", rc, stderr, stdout)
	}
	if !strings.Contains(stdout, "OK") {
		t.Errorf("verify stdout should report OK; got %q", stdout)
	}
}

// TestVerifyDetectsCorruptionExitCode5: a corrupted file makes
// verify exit 5 and surface the path in the error report.
func TestVerifyDetectsCorruptionExitCode5(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix open: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	id := bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	// Corrupt one byte of PG_VERSION post-backup.
	corruptStored(t, b, path.Join(id, "PG_VERSION"))

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, stderr := runRoot(t, "verify", "--config", cfgPath)
	if rc != 5 {
		t.Fatalf("verify exit on corruption = %d, want 5; stderr=%q stdout=%q", rc, stderr, stdout)
	}
	if !strings.Contains(stdout, "PG_VERSION") {
		t.Errorf("verify output should mention corrupted file path; got %q", stdout)
	}
	if !strings.Contains(stdout, "FAILED") {
		t.Errorf("verify output should mention FAILED; got %q", stdout)
	}
}

// TestVerifyJSONStrictDecode: --json output round-trips strict JSON
// decoding (each element is a verify.Result with the expected fields).
func TestVerifyJSONStrictDecode(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	b, err := openPosixForTest(storage)
	if err != nil {
		t.Fatalf("posix open: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "yamlserver")
	bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	cfgPath := writeFixtureConfigWithStorage(t, storage)
	rc, stdout, _ := runRoot(t, "verify", "--config", cfgPath, "--json")
	if rc != 0 {
		t.Fatalf("verify --json exit = %d, want 0; stdout=%q", rc, stdout)
	}
	if !strings.Contains(stdout, `"backup_id"`) || !strings.Contains(stdout, `"files_ok"`) || !strings.Contains(stdout, `"manifest_checksum_ok"`) {
		t.Errorf("verify --json missing expected keys; got %q", stdout)
	}
}

// corruptStored flips one byte in the middle of the file at relPath
// to simulate post-backup bit-rot.
func corruptStored(t *testing.T, b *posix.Backend, relPath string) {
	t.Helper()
	ctx := context.Background()
	rc, err := b.Get(ctx, relPath)
	if err != nil {
		t.Fatalf("Get(%s): %v", relPath, err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	body[len(body)/2] ^= 0xff
	if err := b.Delete(ctx, relPath); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	wc, err := b.Put(ctx, relPath)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := wc.Write(body); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
