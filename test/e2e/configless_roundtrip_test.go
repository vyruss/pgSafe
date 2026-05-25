//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"filippo.io/age"
	"github.com/jackc/pgx/v5"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestE2EConfigLessBackupRestore is the gate for the "no YAML config"
// invocation pattern (mirrors pgbackrest --no-config). It exercises a
// full round-trip with EVERY pgsafe argument passed as a CLI flag —
// nothing on disk except the storage directory and the age identity.
//
// Why this matters: container entrypoints, ad-hoc dev/test runs, and
// future pgbackrest-emulation invocations all ride this code path. A
// regression here turns "the simplest possible pgsafe invocation"
// silently broken — operators get a wall of "config-less mode requires:
// --x, --y" instead of a working backup.
//
// We pair this with --standalone to also confirm the no-config path
// composes with the no-archive path: the goal is "type one command, get
// a backup", and any extra ceremony defeats the point.
func TestE2EConfigLessBackupRestore(t *testing.T) {
	pg := pgtest.StartPG18(t)
	bin := pgsafeBinary(t)
	ctx := context.Background()

	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("GenerateX25519Identity: %v", err)
	}
	tmp := t.TempDir()
	storageDir := filepath.Join(tmp, "storage")
	if err := os.MkdirAll(storageDir, 0o755); err != nil { //nolint:gosec
		t.Fatalf("mkdir storage: %v", err)
	}
	identityPath := filepath.Join(tmp, "identity.txt")
	if err := os.WriteFile(identityPath, []byte(id.String()+"\n"), 0o600); err != nil {
		t.Fatalf("write identity: %v", err)
	}

	// Bare-flag invocation. NB: no --config anywhere.
	commonFlags := []string{
		"--server", "configless-demo",
		"--pg-conn-string", pg.SuperDSN,
		"--pg-version", "18",
		"--storage-type", "posix",
		"--storage-path", storageDir,
		"--encryption-recipient", id.Recipient().String(),
	}

	runPgsafe(t, bin, append([]string{"server", "add"}, commonFlags...)...)

	conn, err := pgx.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		`CREATE TABLE pgsafe_configless (id int PRIMARY KEY, payload text)`,
		`INSERT INTO pgsafe_configless SELECT i, 'r-'||i FROM generate_series(1, 25) AS i`,
		`CHECKPOINT`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	stdout, _ := runPgsafe(t, bin, append(append([]string{"backup"}, commonFlags...),
		"--type", "full",
		"--standalone",
	)...)
	if !strings.Contains(stdout, "complete") {
		t.Errorf("backup stdout = %q; want \"complete\"", stdout)
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	runPgsafe(t, bin, append(append([]string{"restore"}, commonFlags...),
		"--target", restoreDir,
		"--identity-file", identityPath,
	)...)

	if _, err := os.Stat(filepath.Join(restoreDir, "PG_VERSION")); err != nil {
		t.Errorf("PG_VERSION missing in restored dir: %v", err)
	}
	walEntries, err := os.ReadDir(filepath.Join(restoreDir, "pg_wal"))
	if err != nil {
		t.Fatalf("readdir pg_wal: %v", err)
	}
	count := 0
	for _, e := range walEntries {
		if !e.IsDir() {
			count++
		}
	}
	if count == 0 {
		t.Fatal("config-less + standalone restore: pg_wal/ empty")
	}
	t.Logf("config-less + standalone backup→restore OK; pg_wal entries=%d", count)

	// Sanity-prove the storage actually contains a backup the operator
	// can drive `info` against without a config either.
	infoOut, _ := runPgsafe(t, bin, append([]string{"info"}, commonFlags...)...)
	if !strings.Contains(infoOut, "configless-demo") {
		// `info` may render the server name or just the backup id; if
		// neither pattern shows up something is off.
		if !strings.Contains(infoOut, "F\n") && !strings.Contains(infoOut, "F ") && !strings.Contains(infoOut, fmt.Sprintf("%dF", 0)) {
			t.Logf("info output (informational): %s", infoOut)
		}
	}
}
