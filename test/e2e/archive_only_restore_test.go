//go:build e2e

package e2e_test

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

// TestE2EArchiveOnlyRestore is the load-bearing Cycle-2 gate per
// DoD #13a. Proves the WAL archive — not the
// in-tree pg_wal directory — is the durable WAL store: we run a full backup,
// restore it, then **delete every file under <restored>/pg_wal/** before
// booting PG with `restore_command = pgsafe archive-get %f %p`. The cluster
// must reach consistency by pulling segments from the archive.
//
// If this test passes against an empty pg_wal, archive-get is doing real
// work; if it passes only because pg_wal already had what it needed, it
// would not be a real archive-get test.
func TestE2EArchiveOnlyRestore(t *testing.T) {
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

	// archive-only mode: route the cluster's archive_command at the storage's
	// wal/ subdirectory in the archive layout (wal/<TLI>/<seg>) by bind-link.
	// The backup caller's WAL-wait still polls <storage>/wal/
	// (timeline-flat layout); this test's restore boot uses the archive
	// layout for archive-get, which is wal/<TLI>/<seg> per
	// internal/wal/archive.SegmentKey. To satisfy both, we let the
	// caller continue using the legacy flat layout for the backup
	// itself, then for the restore boot we point archive-get at the same
	// directory tree.
	walDir := filepath.Join(repoDir, "wal")
	if err := os.RemoveAll(walDir); err != nil {
		t.Fatalf("rm wal: %v", err)
	}
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

	// Populate dataset and force a few WAL switches so multiple segments
	// land in the archive.
	conn, err := pgx.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		`CREATE TABLE pgsafe_archive (id int PRIMARY KEY, payload text)`,
		`INSERT INTO pgsafe_archive SELECT i, repeat('x', 64) FROM generate_series(1, 200) AS i`,
		`CHECKPOINT`,
		`SELECT pg_switch_wal()`,
		`INSERT INTO pgsafe_archive SELECT i, repeat('y', 64) FROM generate_series(201, 400) AS i`,
		`CHECKPOINT`,
		`SELECT pg_switch_wal()`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	// Backup.
	runPgsafe(t, bin, "backup", "--config", configPath, "--type", "full")

	// Break the symlink and replace walDir with a real directory containing
	// the archive layout (wal/<TLI>/<seg>). The restored container's
	// bind-mount sees real files this way; symlinks pointing at host paths
	// outside the bind don't resolve inside the container.
	if err := os.Remove(walDir); err != nil {
		t.Fatalf("remove symlink: %v", err)
	}
	tli := uint32(1)
	tliDir := filepath.Join(walDir, fmt.Sprintf("%08X", tli))
	if err := os.MkdirAll(tliDir, 0o777); err != nil { //nolint:gosec
		t.Fatalf("mkdir tli: %v", err)
	}
	// pgtest's archive_command writes WALArchive/<TLI>/<seg>-<sha256-hex>
	// matching archive.SegmentKey. Files have the form
	// "<24-hex-segname>-<64-hex-sha>" — 89 chars total. Read from there.
	srcTLI := filepath.Join(pg.WALArchive, fmt.Sprintf("%08X", tli))
	srcEntries, err := os.ReadDir(srcTLI)
	if err != nil {
		t.Fatalf("readdir %s: %v", srcTLI, err)
	}
	var copied int
	for _, e := range srcEntries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		// "<24>-<64>" = 89 chars and contains a '-' at index 24.
		if len(name) != 89 || name[24] != '-' {
			continue
		}
		from := filepath.Join(srcTLI, name)
		to := filepath.Join(tliDir, name)
		if err := copyFile(from, to); err != nil {
			t.Fatalf("copy %s -> %s: %v", from, to, err)
		}
		copied++
	}
	if copied < 2 {
		t.Fatalf("only %d WAL segments copied; need at least 2 (one for the second insert batch)", copied)
	}
	t.Logf("staged %d WAL segments in %s", copied, tliDir)

	// Restore (still uses the wal/ flat layout for its own copy step;
	// downstream we'll override pg_wal/ before boot).
	restoreDir := filepath.Join(t.TempDir(), "restored")
	runPgsafe(t, bin, "restore",
		"--config", configPath,
		"--target", restoreDir,
		"--identity-file", identityPath,
		"--restore-command", "/usr/bin/false", // initial value; we rewrite postgresql.auto.conf next
	)

	// THE LOAD-BEARING STEP: empty pg_wal so PG MUST use restore_command.
	pgWal := filepath.Join(restoreDir, "pg_wal")
	if entries, err := os.ReadDir(pgWal); err == nil {
		for _, e := range entries {
			full := filepath.Join(pgWal, e.Name())
			if e.IsDir() {
				_ = os.RemoveAll(full)
			} else {
				_ = os.Remove(full)
			}
		}
	}

	// Rewrite postgresql.auto.conf so restore_command points at the host's
	// pgsafe binary, with the config file accessible inside the container.
	autoConf := filepath.Join(restoreDir, "postgresql.auto.conf")
	body := []byte("restore_command = '/pgsafe archive-get \"%f\" \"%p\" --config /pgsafe-config.yaml'\n")
	if err := os.WriteFile(autoConf, body, 0o600); err != nil {
		t.Fatalf("write postgresql.auto.conf: %v", err)
	}

	// Boot the restored cluster with the binary + config bind-mounted in.
	if err := bootArchiveOnlyAndAmcheck(t, restoreDir, bin, configPath); err != nil {
		t.Fatalf("archive-only restore failed: %v", err)
	}
}

// bootArchiveOnlyAndAmcheck mirrors bootRestoredAndAmcheck but additionally
// bind-mounts the host's freshly-built pgsafe binary into the container so
// PG's restore_command can shell out to it for archive-get.
func bootArchiveOnlyAndAmcheck(t *testing.T, restoreDir, hostBinary, hostConfig string) error {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH")
		return nil
	}

	// Cross-compile a Linux/amd64 pgsafe so the binary runs inside the
	// postgres:18 container regardless of the host OS.
	binDir := t.TempDir()
	linuxBin := filepath.Join(binDir, "pgsafe")
	build := exec.Command("go", "build", "-o", linuxBin, "../../cmd/pgsafe")
	hostEnv := os.Environ()
	cleanEnv := make([]string, 0, len(hostEnv))
	for _, e := range hostEnv {
		if len(e) > 5 && (e[:5] == "GOOS=" || e[:7] == "GOARCH=") {
			continue
		}
		cleanEnv = append(cleanEnv, e)
	}
	build.Env = append([]string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"}, cleanEnv...)
	if out, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("build linux pgsafe: %w\n%s", err, out)
	}
	_ = hostBinary // host binary is for restore; the in-container one is the cross-compile above

	// The container's restore_command will read --config /pgsafe-config.yaml.
	// Rewrite the config so /storage (the bind mount inside the container) is
	// the storage path, not the test's host path.
	cfg, err := os.ReadFile(hostConfig) //nolint:gosec
	if err != nil {
		return fmt.Errorf("read host config: %w", err)
	}
	innerConfig := strings.Replace(string(cfg),
		filepath.Dir(filepath.Dir(restoreDir))+"/", "",
		-1)
	// Robust: replace any "path: <abs-host-repoDir>" with "path: /storage".
	innerConfig = rewriteStoragePath(innerConfig, "/storage")
	innerCfgPath := filepath.Join(binDir, "pgsafe-config.yaml")
	if err := os.WriteFile(innerCfgPath, []byte(innerConfig), 0o644); err != nil { //nolint:gosec
		return fmt.Errorf("write inner config: %w", err)
	}

	hostStorage := repoFromConfig(string(cfg))
	if hostStorage == "" {
		return fmt.Errorf("could not extract storage.path from config")
	}

	containerName := fmt.Sprintf("pgsafe-archive-%d-%d", os.Getpid(), time.Now().UnixNano())
	defer func() {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	}()

	uid := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	run := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--user", uid,
		"-e", "PGDATA=/data",
		"-v", restoreDir+":/data:rw",
		"-v", hostStorage+":/storage:ro",
		"-v", linuxBin+":/pgsafe:ro",
		"-v", innerCfgPath+":/pgsafe-config.yaml:ro",
		"postgres:18",
		"postgres", "-D", "/data",
	)
	if out, err := run.CombinedOutput(); err != nil {
		return fmt.Errorf("docker run failed: %w\n%s", err, out)
	}

	// Wait for PG to accept connections — recovery needs to fetch WAL via
	// archive-get, so this can take longer than the standard 30s; use 60s.
	for i := 0; i < 60; i++ {
		probe := exec.Command("docker", "exec", containerName,
			"psql", "-U", "postgres", "-d", "postgres", "-tA", "-c", "SELECT 1")
		if out, err := probe.CombinedOutput(); err == nil && strings.Contains(string(out), "1") {
			break
		}
		if i == 59 {
			logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
			return fmt.Errorf("PG did not become ready in 60s\n--- logs ---\n%s", logs)
		}
		time.Sleep(1 * time.Second)
	}

	// Confirm both halves of the dataset (the second half required WAL replay
	// from the archive — without it, count would be 200, not 400).
	out, err := exec.Command("docker", "exec", containerName,
		"psql", "-U", "postgres", "-d", "postgres", "-tA",
		"-c", "SELECT count(*) FROM pgsafe_archive").CombinedOutput()
	if err != nil {
		return fmt.Errorf("count: %w\n%s", err, out)
	}
	if got := strings.TrimSpace(string(out)); got != "400" {
		return fmt.Errorf("row count = %q, want 400 (the second 200 rows landed only in archived WAL)", got)
	}

	createExt := exec.Command("docker", "exec", containerName,
		"psql", "-U", "postgres", "-d", "postgres",
		"-c", "CREATE EXTENSION IF NOT EXISTS amcheck")
	if out, err := createExt.CombinedOutput(); err != nil {
		return fmt.Errorf("install amcheck: %w\n%s", err, out)
	}
	amcheck := exec.Command("docker", "exec", containerName,
		"pg_amcheck", "-U", "postgres", "-d", "postgres")
	out, err = amcheck.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_amcheck: %w\n%s", err, out)
	}
	t.Logf("archive-only restore amcheck OK: %s", out)
	return nil
}

// copyFile is a 64KiB-buffer fs copy. Used to materialize WAL segments
// from the original PG container's bind-mount into the restore-side
// archive layout.
func copyFile(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // test fixture path
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// rewriteStoragePath replaces the storage.path: line in YAML body with newPath.
func rewriteStoragePath(body, newPath string) string {
	lines := strings.Split(body, "\n")
	for i, l := range lines {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "path:") {
			indent := l[:len(l)-len(strings.TrimLeft(l, " \t"))]
			lines[i] = indent + "path: " + newPath
		}
	}
	return strings.Join(lines, "\n")
}

// repoFromConfig extracts the storage.path: value from the YAML body and
// strips surrounding quotes (the e2e fixture template wraps the path in
// double-quotes for clarity).
func repoFromConfig(body string) string {
	for _, l := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "path:") {
			val := strings.TrimSpace(strings.TrimPrefix(trimmed, "path:"))
			val = strings.Trim(val, `"'`)
			return val
		}
	}
	return ""
}
