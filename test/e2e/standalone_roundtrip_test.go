//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/jackc/pgx/v5"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestE2EStandaloneBackupRestore is the gate for the user's
// "just take a backup from some location" use case. It exercises the
// --standalone path end-to-end:
//
//   - PG 18 container is brought up with the default pgtest fixture
//     (archive_mode=on, but THIS TEST DOES NOT WIRE THE ARCHIVE TO
//     PGSAFE'S STORAGE — there's deliberately no symlink between
//     pg.WALArchive and <storage>/wal). The container's archive_command
//     ships WAL into pg.WALArchive but pgsafe never looks there.
//   - `pgsafe backup --standalone` runs without any archive
//     reachability dance. pg_basebackup --wal-method=fetch packs the
//     bracket WAL into the data tar's pg_wal/ entries, the filter chain
//     writes them to <storage>/<backup-id>/pg_wal/<seg>.
//   - `pgsafe restore` extracts the backup as usual; pg_wal/* files
//     come out of the data-file restore loop, no archive lookup needed.
//   - The restored cluster boots, recovers from backup_label using the
//     inline WAL, becomes consistent, and serves the test data.
//
// If this test breaks, "self-contained backup" is broken — the most
// important user-facing simplification of the WALSource design.
func TestE2EStandaloneBackupRestore(t *testing.T) {
	pg := pgtest.StartPG18(t)
	bin := pgsafeBinary(t)
	ctx := context.Background()

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("GenerateX25519Identity: %v", err)
	}
	cfgDir := t.TempDir()
	storageDir := filepath.Join(cfgDir, "storage")
	if err := os.MkdirAll(storageDir, 0o755); err != nil { //nolint:gosec
		t.Fatalf("mkdir storage: %v", err)
	}

	// NB: no symlink between pg.WALArchive and storageDir/wal — the
	// whole point of standalone is that no archive plumbing is needed.

	configPath := filepath.Join(cfgDir, "pgsafe.yaml")
	configBody := fmt.Sprintf(e2eConfigTemplate, pg.SuperDSN, storageDir, id.Recipient().String())
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	identityPath := filepath.Join(cfgDir, "identity.txt")
	if err := os.WriteFile(identityPath, []byte(id.String()+"\n"), 0o600); err != nil {
		t.Fatalf("write identity: %v", err)
	}

	runPgsafe(t, bin, "server", "add", "--config", configPath)

	conn, err := pgx.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		`CREATE TABLE pgsafe_standalone (id int PRIMARY KEY, payload text)`,
		`INSERT INTO pgsafe_standalone SELECT i, 'row-'||i FROM generate_series(1, 50) AS i`,
		`CHECKPOINT`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	stdout, _ := runPgsafe(t, bin, "backup", "--config", configPath, "--type", "full", "--standalone")
	if !strings.Contains(stdout, "complete") {
		t.Errorf("backup stdout = %q; want \"complete\"", stdout)
	}
	// The standalone warning should be loud — operators must know that
	// PITR is constrained to the bracket window unless an archive is
	// running separately. If this regresses, operators can land in the
	// "I thought my backups were PITR-capable" trap.
	if !strings.Contains(stdout, "--standalone") || !strings.Contains(stdout, "PITR") {
		t.Errorf("standalone warning missing from stdout:\n%s", stdout)
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	runPgsafe(t, bin, "restore",
		"--config", configPath,
		"--target", restoreDir,
		"--identity-file", identityPath,
	)

	// The bracket WAL must be in <restoreDir>/pg_wal/* — that's where
	// pg_basebackup --wal-method=fetch placed it and where the filter
	// chain wrote it. If this directory is empty, recovery will fail
	// and the test below would diagnose it later, but checking here
	// gives a sharper error.
	walEntries, err := os.ReadDir(filepath.Join(restoreDir, "pg_wal"))
	if err != nil {
		t.Fatalf("readdir pg_wal: %v", err)
	}
	walCount := 0
	for _, e := range walEntries {
		if !e.IsDir() {
			walCount++
		}
	}
	if walCount == 0 {
		t.Fatal("standalone restore: pg_wal/ has no segments — inline WAL was lost between backup and restore")
	}
	t.Logf("standalone restore has %d files in pg_wal/", walCount)

	// Boot it; the restored cluster recovers from backup_label using
	// the inline WAL and exposes the test data.
	if err := bootStandaloneAndQuery(t, restoreDir); err != nil {
		t.Fatalf("boot+query restored cluster: %v", err)
	}
}

// bootStandaloneAndQuery boots the restored cluster in a postgres:18
// container, waits for it to become ready, and asserts the seed data is
// intact. Distinct from bootRestoredAndAmcheck: amcheck doesn't tell us
// whether the WAL replay actually saw the bracket data — we have to
// SELECT to be sure.
func bootStandaloneAndQuery(t *testing.T, restoreDir string) error {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH")
		return nil
	}
	containerName := fmt.Sprintf("pgsafe-standalone-%d-%d", os.Getpid(), time.Now().UnixNano())
	defer func() {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	}()
	uid := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	run := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--user", uid,
		"-e", "PGDATA=/data",
		"-v", restoreDir+":/data:rw",
		"postgres:18",
		"postgres", "-D", "/data",
	)
	if out, err := run.CombinedOutput(); err != nil {
		return fmt.Errorf("docker run: %w\n%s", err, out)
	}
	for i := 0; i < 30; i++ {
		probe := exec.Command("docker", "exec", containerName,
			"psql", "-U", "postgres", "-d", "postgres", "-tA", "-c", "SELECT 1")
		if out, err := probe.CombinedOutput(); err == nil && strings.Contains(string(out), "1") {
			break
		}
		if i == 29 {
			logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
			return fmt.Errorf("PG not ready in 30s\n--- logs ---\n%s", logs)
		}
		time.Sleep(1 * time.Second)
	}
	out, err := exec.Command("docker", "exec", containerName,
		"psql", "-U", "postgres", "-d", "postgres", "-tA", "-c",
		"SELECT count(*) FROM pgsafe_standalone").CombinedOutput()
	if err != nil {
		return fmt.Errorf("query: %w\n%s", err, out)
	}
	got := strings.TrimSpace(string(out))
	if got != "50" {
		return fmt.Errorf("row count = %q, want 50", got)
	}
	t.Logf("standalone-restore data intact: 50 rows recovered")
	return nil
}
