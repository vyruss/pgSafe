package verify_test

import (
	"context"
	"io"
	"path"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/verify"
)

func newStorage(t *testing.T) *posix.Backend {
	t.Helper()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return b
}

// TestVerifyAllOKOnUntouchedStorage: a freshly-built synthetic backup
// passes verification — every file SHA matches the manifest, the
// manifest checksum verifies.
func TestVerifyAllOKOnUntouchedStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newStorage(t)
	bb := backuptest.New(ctx, b, "demo")
	id := bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	results, err := verify.Verify(ctx, b, verify.Options{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	r := results[0]
	if r.BackupID != id {
		t.Errorf("BackupID = %q, want %q", r.BackupID, id)
	}
	if !r.AllOK() {
		t.Errorf("expected AllOK; got %+v", r)
	}
	if r.FilesOK == 0 {
		t.Errorf("FilesOK = 0, want > 0")
	}
}

// TestVerifyDetectsCorruptFile: corrupting one byte of a stored file
// surfaces as a Mismatch with the path and the recomputed SHA.
func TestVerifyDetectsCorruptFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newStorage(t)
	bb := backuptest.New(ctx, b, "demo")
	id := bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	corrupt(t, b, path.Join(id, "PG_VERSION"))

	results, err := verify.Verify(ctx, b, verify.Options{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d", len(results))
	}
	r := results[0]
	if r.AllOK() {
		t.Fatalf("AllOK after corruption: want false, got true (result %+v)", r)
	}
	if r.FilesMismatched != 1 {
		t.Errorf("FilesMismatched = %d, want 1", r.FilesMismatched)
	}
	if len(r.Mismatches) != 1 {
		t.Fatalf("Mismatches len = %d, want 1", len(r.Mismatches))
	}
	m := r.Mismatches[0]
	if m.Path != "PG_VERSION" {
		t.Errorf("Mismatch.Path = %q, want PG_VERSION", m.Path)
	}
	if m.Expected == "" || m.Actual == "" {
		t.Errorf("Mismatch must carry both Expected and Actual hashes; got %+v", m)
	}
	if m.Expected == m.Actual {
		t.Errorf("Expected and Actual SHAs match — corruption not detected")
	}
}

// TestVerifySingleBackupID: opts.BackupID restricts verification to
// just that one backup, even when the storage holds others.
func TestVerifySingleBackupID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newStorage(t)
	bb := backuptest.New(ctx, b, "demo")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	pick := bb.AddFull(t0, "alpha")
	bb.AddFull(t0.Add(24*time.Hour), "bravo")

	results, err := verify.Verify(ctx, b, verify.Options{BackupID: pick})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if results[0].BackupID != pick {
		t.Errorf("BackupID = %q, want %q", results[0].BackupID, pick)
	}
}

// TestVerifyManifestChecksum: corrupting the manifest itself surfaces
// as ManifestChecksumOK=false (independent of file SHAs).
func TestVerifyManifestChecksum(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newStorage(t)
	bb := backuptest.New(ctx, b, "demo")
	id := bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	corrupt(t, b, path.Join(id, "backup_manifest"))

	results, err := verify.Verify(ctx, b, verify.Options{})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d", len(results))
	}
	r := results[0]
	if r.ManifestChecksumOK {
		t.Errorf("ManifestChecksumOK = true after manifest corruption; want false")
	}
}

// corrupt flips one byte in the middle of the file at relPath. Used to
// simulate bit-rot post-backup.
func corrupt(t *testing.T, b *posix.Backend, relPath string) {
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
	if len(body) == 0 {
		t.Fatalf("file %s is empty; cannot corrupt", relPath)
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
