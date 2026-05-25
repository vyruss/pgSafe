//go:build e2e

// Package e2e_test is the milestone harness: backup → restore →
// validation. Tests under this build tag spin up real PG 18 containers and
// exercise the whole stack end-to-end via the pgsafe CLI binary.
package e2e_test

// Shared helpers (pgsafeBinary, runPgsafe, testLogWriter, runPgVerifybackup)
// live in helpers_test.go so the hybrid-parallel demo (e2e_hybrid build tag)
// can reuse them.

import (
	"context"
	"fmt"
	"io"
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

const e2eConfigTemplate = `
server: e2e
pg:
  conn_string: "%s"
  version: 18
storages:
  - type: posix
    path: "%s"
compression:
  codec: zstd
  level: 3
encryption:
  recipients:
    - "%s"
log:
  format: json
  level: info
`

func TestE2EHappyPath_BackupRestoreVerify(t *testing.T) {
	pg := pgtest.StartPG18(t)
	bin := pgsafeBinary(t)
	ctx := context.Background()

	// age key pair: recipient (public) goes in config; identity (private)
	// goes in a separate file passed to restore.
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("GenerateX25519Identity: %v", err)
	}
	cfgDir := t.TempDir()
	repoDir := filepath.Join(cfgDir, "storage")
	if err := os.MkdirAll(repoDir, 0o755); err != nil { //nolint:gosec
		t.Fatalf("mkdir storage: %v", err)
	}
	// Match WAL archive perms — the container's postgres user (UID 999)
	// writes here via archive_command.
	if err := os.Chmod(repoDir, 0o777); err != nil { //nolint:gosec
		t.Fatalf("chmod storage: %v", err)
	}
	// Re-point the cluster's archive_command at the storage's wal/ directory so
	// the WAL-wait step finds segments where it expects them.
	walDir := filepath.Join(repoDir, "wal")
	if err := os.MkdirAll(walDir, 0o777); err != nil { //nolint:gosec
		t.Fatalf("mkdir wal: %v", err)
	}
	if err := os.Chmod(walDir, 0o777); err != nil { //nolint:gosec
		t.Fatalf("chmod wal: %v", err)
	}

	// Make pg.WALArchive the SAME directory the cluster archive_command
	// writes to — by symlinking. The pgtest fixture's archive_command points
	// at /var/lib/pgsafe-wal inside the container, which is bind-mounted
	// from pg.WALArchive on the host. We need WAL to also be visible at
	// repoDir/wal so pgsafe's WAL-wait sees it.
	//
	// expedient: mirror the contents post-archive by copy. A real
	// operator would set archive_command directly to write into <storage>/wal.
	if err := os.RemoveAll(walDir); err != nil {
		t.Fatalf("rm wal: %v", err)
	}
	if err := os.Symlink(pg.WALArchive, walDir); err != nil {
		t.Fatalf("symlink wal -> %s: %v", pg.WALArchive, err)
	}

	configPath := filepath.Join(cfgDir, "pgsafe.yaml")
	configBody := fmt.Sprintf(e2eConfigTemplate, pg.DSN, repoDir, id.Recipient().String())
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	identityPath := filepath.Join(cfgDir, "identity.txt")
	if err := os.WriteFile(identityPath, []byte(id.String()+"\n"), 0o600); err != nil {
		t.Fatalf("write identity: %v", err)
	}

	// Step 1: initialize the server's storage
	runPgsafe(t, bin, "server", "add", "--config", configPath)

	// Step 2: populate dataset.
	conn, err := pgx.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		`CREATE TABLE pgsafe_e2e (id int PRIMARY KEY, payload text)`,
		`INSERT INTO pgsafe_e2e SELECT i, repeat('x', 32) FROM generate_series(1, 100) AS i`,
		`CHECKPOINT`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	// Step 3: backup. With superuser DSN — the test's pgsafe user has
	// REPLICATION but for SQL the test simplifies by running as superuser.
	cfg2 := strings.Replace(configBody, pg.DSN, pg.SuperDSN, 1)
	configPath2 := filepath.Join(cfgDir, "pgsafe-super.yaml")
	if err := os.WriteFile(configPath2, []byte(cfg2), 0o600); err != nil {
		t.Fatalf("write super config: %v", err)
	}
	stdout, _ := runPgsafe(t, bin, "backup", "--config", configPath2, "--type", "full")
	if !strings.Contains(stdout, "backup ") || !strings.Contains(stdout, "complete") {
		t.Errorf("backup stdout = %q; want \"backup ... complete\"", stdout)
	}

	// Step 4: restore.
	restoreDir := filepath.Join(t.TempDir(), "restored")
	runPgsafe(t, bin, "restore",
		"--config", configPath2,
		"--target", restoreDir,
		"--identity-file", identityPath,
	)

	// Step 5: validate via pg_verifybackup against the restored target.
	if err := runPgVerifybackup(t, restoreDir); err != nil {
		t.Fatalf("pg_verifybackup rejected restore: %v", err)
	}

	// Sanity: a couple of expected files should be present, and PG_VERSION
	// should round-trip exactly to "18".
	verBytes, err := os.ReadFile(filepath.Join(restoreDir, "PG_VERSION")) //nolint:gosec
	if err != nil {
		t.Fatalf("read PG_VERSION: %v", err)
	}
	if got := strings.TrimSpace(string(verBytes)); got != "18" {
		t.Errorf("PG_VERSION = %q, want 18", got)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "global", "pg_control")); err != nil {
		t.Errorf("global/pg_control missing in restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "recovery.signal")); err != nil {
		t.Errorf("recovery.signal not generated: %v", err)
	}

	// Step 6: boot the restored dir as a fresh PG cluster and run pg_amcheck.
	// This is the §1.4 DoD #6 verification — the restore is genuinely
	// recoverable, not just "manifest-valid".
	if err := bootRestoredAndAmcheck(t, restoreDir); err != nil {
		t.Fatalf("amcheck against restored cluster: %v", err)
	}
}

// bootRestoredAndAmcheck boots a postgres:18 container against restoreDir
// and runs pg_amcheck. Domain post-conditions (row counts, schema checks)
// belong in the caller — this helper only asserts that the cluster boots
// to consistency and pg_amcheck reports clean.
func bootRestoredAndAmcheck(t *testing.T, restoreDir string) error {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH")
		return nil
	}

	containerName := fmt.Sprintf("pgsafe-amcheck-%d-%d", os.Getpid(), time.Now().UnixNano())
	defer func() {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	}()

	// Container runs as the host UID so bind-mounted file ownership lines
	// up. recovery.signal triggers recovery; archived WAL in restoreDir/pg_wal
	// is replayed. No --rm, so docker logs is available if startup fails.
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
		return fmt.Errorf("docker run failed: %w\n%s", err, out)
	}

	for i := 0; i < 30; i++ {
		probe := exec.Command("docker", "exec", containerName,
			"psql", "-U", "postgres", "-d", "postgres", "-tA", "-c", "SELECT 1")
		if out, err := probe.CombinedOutput(); err == nil && strings.Contains(string(out), "1") {
			break
		}
		if i == 29 {
			logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
			inspect, _ := exec.Command("docker", "inspect", "--format",
				"{{.State.Status}} {{.State.ExitCode}} {{.State.Error}}", containerName).CombinedOutput()
			return fmt.Errorf("PG did not become ready in 30s\n--- inspect ---\n%s\n--- logs ---\n%s",
				inspect, logs)
		}
		time.Sleep(1 * time.Second)
	}

	createExt := exec.Command("docker", "exec", containerName,
		"psql", "-U", "postgres", "-d", "postgres",
		"-c", "CREATE EXTENSION IF NOT EXISTS amcheck")
	if out, err := createExt.CombinedOutput(); err != nil {
		return fmt.Errorf("install amcheck: %w\n%s", err, out)
	}
	out, err := exec.Command("docker", "exec", containerName,
		"pg_amcheck", "-U", "postgres", "-d", "postgres").CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_amcheck failed: %w\n%s", err, out)
	}
	t.Logf("pg_amcheck OK; output: %s", out)
	return nil
}

// quietBuf is a discard io.Writer; declared once to satisfy linters where
// we don't actually want subprocess output.
type quietBuf struct{}

func (quietBuf) Write(p []byte) (int, error) { return len(p), nil }

var _ io.Writer = quietBuf{}
