//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/jackc/pgx/v5"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestE2ECrashAndRecover is the §1.4 DoD #7 verification:
//  1. start a backup, kill pgsafe mid-stream
//  2. assert post-crash storage state is Invariant-#2-valid (no malformed
//     backup_manifest at the final name; at most a *.tmp remnant)
//  3. start a second backup against the same storage; it must succeed
//  4. restore from the second backup, boot, pg_amcheck clean
//
// The kill is via SIGKILL — pgsafe currently has no graceful shutdown path
// (§3.3 wires signal.NotifyContext into main). SIGKILL is also the
// stronger test: it simulates an OOM or a pulled power cord, the cases
// Invariant #2 was designed to survive.
func TestE2ECrashAndRecover(t *testing.T) {
	pg := pgtest.StartPG18(t)
	bin := pgsafeBinary(t)
	ctx := context.Background()

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("GenerateX25519Identity: %v", err)
	}
	cfgDir := t.TempDir()
	repoDir := filepath.Join(cfgDir, "storage")
	if err := os.MkdirAll(repoDir, 0o777); err != nil { //nolint:gosec
		t.Fatalf("mkdir storage: %v", err)
	}
	if err := os.Chmod(repoDir, 0o777); err != nil { //nolint:gosec
		t.Fatalf("chmod storage: %v", err)
	}
	walDir := filepath.Join(repoDir, "wal")
	if err := os.Symlink(pg.WALArchive, walDir); err != nil {
		t.Fatalf("symlink wal: %v", err)
	}

	configPath := filepath.Join(cfgDir, "pgsafe.yaml")
	configBody := fmt.Sprintf(e2eConfigTemplate, pg.SuperDSN, repoDir, id.Recipient().String())
	if err := os.WriteFile(configPath, []byte(configBody), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	identityPath := filepath.Join(cfgDir, "identity.txt")
	if err := os.WriteFile(identityPath, []byte(id.String()+"\n"), 0o600); err != nil {
		t.Fatalf("write identity: %v", err)
	}

	runPgsafe(t, bin, "server", "add", "--config", configPath)

	// Populate a small dataset.
	conn, err := pgx.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	for _, sql := range []string{
		`CREATE TABLE pgsafe_e2e (id int PRIMARY KEY, payload text)`,
		`INSERT INTO pgsafe_e2e SELECT i, repeat('x', 32) FROM generate_series(1, 100) AS i`,
		`CHECKPOINT`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL: %v", err)
		}
	}
	_ = conn.Close(ctx)

	// Step 1: start a backup that will be killed mid-stream.
	cmd := exec.Command(bin, "backup", "--config", configPath, "--type", "full")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start pgsafe: %v", err)
	}

	// Wait long enough for pg_basebackup to be in flight (typical run is
	// ~2s; 300ms gets us mid-stream while files are landing in the storage).
	time.Sleep(300 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("send SIGKILL: %v", err)
	}
	_ = cmd.Wait()
	t.Logf("first backup killed; stderr (truncated):\n%s", truncate(stderr.String(), 400))

	// Step 2: assert no final backup_manifest exists in any backup directory.
	// The Invariant-#2 valid post-crash states are: no backup-id dir at all,
	// or a backup-id dir with at most a backup_manifest.tmp.
	entries, err := os.ReadDir(repoDir)
	if err != nil {
		t.Fatalf("readdir storage: %v", err)
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasSuffix(e.Name(), "F") {
			continue
		}
		bdir := filepath.Join(repoDir, e.Name())
		if _, err := os.Stat(filepath.Join(bdir, "backup_manifest")); err == nil {
			t.Errorf("Invariant #2 violated: %s/backup_manifest exists despite mid-stream kill", e.Name())
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("Invariant #2 stat err: %v", err)
		}
		// A backup_manifest.tmp is allowed (the unprivileged-write tmp); also
		// allowed: nothing at all (kill before manifest write started).
	}

	// Step 3: a fresh backup must complete cleanly despite remnants.
	stdout, _ := runPgsafe(t, bin, "backup", "--config", configPath, "--type", "full")
	if !strings.Contains(stdout, "complete") {
		t.Fatalf("second backup output unexpected: %q", stdout)
	}

	// Step 4: restore from the latest backup and boot.
	restoreDir := filepath.Join(t.TempDir(), "restored")
	runPgsafe(t, bin, "restore",
		"--config", configPath,
		"--target", restoreDir,
		"--identity-file", identityPath,
	)
	if err := runPgVerifybackup(t, restoreDir); err != nil {
		t.Fatalf("pg_verifybackup after recovery: %v", err)
	}
	if err := bootRestoredAndAmcheck(t, restoreDir); err != nil {
		t.Fatalf("amcheck after crash-recover: %v", err)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
