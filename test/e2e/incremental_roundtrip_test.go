//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"filippo.io/age"
	"github.com/jackc/pgx/v5"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestE2EIncrementalChainRoundtrip is the Cycle-6 gate: full → incr
// → restore via pg_combinebackup → boot → pg_amcheck clean. Runs only
// against PG 17+ (when WAL summarizer + pg_basebackup --incremental exist).
func TestE2EIncrementalChainRoundtrip(t *testing.T) {
	if _, err := exec.LookPath("pg_combinebackup"); err != nil {
		t.Skip("pg_combinebackup not on PATH; skipping incremental E2E")
	}
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

	// Initial dataset.
	conn, err := pgx.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		`CREATE TABLE pgsafe_incr (id int PRIMARY KEY, payload text)`,
		`INSERT INTO pgsafe_incr SELECT i, repeat('x', 32) FROM generate_series(1, 100) AS i`,
		`CHECKPOINT`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	// 1. Full backup.
	runPgsafe(t, bin, "backup", "--config", configPath, "--type", "full")

	fullID, err := latestBackupID(repoDir)
	if err != nil {
		t.Fatalf("latestBackupID(full): %v", err)
	}
	t.Logf("full backup id = %s", fullID)

	// 2. More data + force WAL turnover so the summarizer has work.
	for _, sql := range []string{
		`INSERT INTO pgsafe_incr SELECT i, repeat('y', 32) FROM generate_series(101, 200) AS i`,
		`CHECKPOINT`,
		`SELECT pg_switch_wal()`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	// 3. Incremental backup against the full's parent ID.
	runPgsafe(t, bin, "backup", "--config", configPath,
		"--type", "incr", "--parent", fullID)

	incrID, err := latestBackupID(repoDir)
	if err != nil {
		t.Fatalf("latestBackupID(incr): %v", err)
	}
	if incrID == fullID {
		t.Fatalf("incremental backup id = full id = %s; expected new id", fullID)
	}
	if !strings.Contains(incrID, fullID) {
		t.Errorf("incremental id %q should contain parent %q", incrID, fullID)
	}
	t.Logf("incremental backup id = %s", incrID)

	// 4. Restore the incremental — should detect chain + run pg_combinebackup.
	restoreDir := filepath.Join(t.TempDir(), "restored-incr")
	runPgsafe(t, bin, "restore",
		"--config", configPath,
		"--target", restoreDir,
		"--identity-file", identityPath,
		"--backup-id", incrID,
	)

	// 5. Sanity checks on restored tree.
	if _, err := os.Stat(filepath.Join(restoreDir, "PG_VERSION")); err != nil {
		t.Errorf("PG_VERSION missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "global", "pg_control")); err != nil {
		t.Errorf("global/pg_control missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "recovery.signal")); err != nil {
		t.Errorf("recovery.signal missing: %v", err)
	}

	// 6. Boot the restored cluster + amcheck.
	if err := bootRestoredAndAmcheck(t, restoreDir); err != nil {
		t.Fatalf("amcheck against combined cluster: %v", err)
	}
}

// latestBackupID picks the lexicographically-largest top-level entry in
// repoDir that ends in "F" or "I" — both full and incremental. Used by E2E
// tests after a backup completes.
func latestBackupID(repoDir string) (string, error) {
	entries, err := os.ReadDir(repoDir)
	if err != nil {
		return "", err
	}
	var ids []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name == "wal" {
			continue
		}
		if strings.HasSuffix(name, "F") || strings.HasSuffix(name, "I") {
			ids = append(ids, name)
		}
	}
	if len(ids) == 0 {
		return "", fmt.Errorf("no backup directories found in %s", repoDir)
	}
	sort.Strings(ids)
	return ids[len(ids)-1], nil
}
