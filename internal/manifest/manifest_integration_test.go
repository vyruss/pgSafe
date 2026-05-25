//go:build integration

package manifest_test

import (
	"crypto/sha256"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
)

// TestManifestPassesPgVerifybackup is the load-bearing Cycle-4 gate: the
// manifest produced by our Builder must survive PG's own pg_verifybackup
// when pointed at a matching on-disk fixture.
//
// We don't have pg_verifybackup on the host, so the test execs it inside
// the postgres:18 docker image. Skipped if docker isn't available.
func TestManifestPassesPgVerifybackup(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH; pg_verifybackup integration test skipped")
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil { //nolint:gosec // explicit test-fixture perm
		t.Fatalf("chmod tempdir: %v", err)
	}

	// Two fixture files, deliberately different sizes and contents.
	fixtures := map[string][]byte{
		"PG_VERSION": []byte("18\n"),
		"data.bin":   []byte("the quick brown fox jumps over the lazy dog\n"),
	}
	type rec struct {
		size    int64
		sum     [32]byte
		modTime time.Time
	}
	recs := make(map[string]rec)
	for name, content := range fixtures {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, content, 0o644); err != nil { //nolint:gosec
			t.Fatalf("write %s: %v", name, err)
		}
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		recs[name] = rec{
			size:    int64(len(content)),
			sum:     sha256.Sum256(content),
			modTime: fi.ModTime(),
		}
	}

	// Build the manifest. Real System-Identifier and LSN values don't matter
	// to pg_verifybackup as long as WAL-Ranges is structurally valid AND we
	// pass --no-parse-wal so it doesn't try to follow them.
	b := manifest.NewBuilder(manifest.BackupStartInfo{
		SystemIdentifier: 7633557436145790995,
		Timeline:         1,
		StartLSN:         manifest.LSN(0x2000028),
		StartTime:        time.Now().UTC(),
	})
	for name, r := range recs {
		b.AddFile(name, r.size, r.sum, r.modTime)
	}
	out, err := b.Finalize(manifest.BackupStopInfo{
		StopLSN:  manifest.LSN(0x2000120),
		StopTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "backup_manifest"), out, 0o644); err != nil { //nolint:gosec
		t.Fatalf("write manifest: %v", err)
	}

	// Run pg_verifybackup inside the postgres:18 image against the fixture.
	cmd := exec.Command("docker", "run", "--rm",
		"--user", "0:0",
		"-v", dir+":/backup:ro",
		"postgres:18",
		"pg_verifybackup", "--no-parse-wal", "/backup",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("pg_verifybackup rejected our manifest: %v\n--- output ---\n%s\n--- manifest ---\n%s",
			err, output, out)
	}
	t.Logf("pg_verifybackup OK: %s", output)
}
