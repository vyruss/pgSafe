//go:build integration

package pgtest_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestPgVerifybackupRoundtrip is the Cycle-2 anchor: a real pg_basebackup
// inside the PG container produces a valid backup_manifest, and pg_verifybackup
// invoked through PG.PgVerifybackup reports it clean. Pins the wrapper's
// happy path before plumbs it into the actual matrix cells.
func TestPgVerifybackupRoundtrip(t *testing.T) {
	pg := pgtest.StartPG(t, 18)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Carve out a directory inside the container for pg_basebackup output.
	// Using /tmp avoids the postgres user's $HOME and any volume-mount
	// owner-mismatch surprises.
	const backupDir = "/tmp/pgsafe-verify-backup"
	code, out, err := pg.Exec(ctx, "rm", "-rf", backupDir)
	if err != nil {
		t.Fatalf("Exec rm: %v", err)
	}
	if code != 0 {
		t.Fatalf("rm -rf %s: exit %d\n%s", backupDir, code, out)
	}

	// pg_basebackup runs as the postgres image's "postgres" superuser; both
	// it and pg_verifybackup ship in the postgres:N image.
	code, out, err = pg.Exec(ctx,
		"pg_basebackup",
		"-h", "127.0.0.1",
		"-U", "postgres",
		"-D", backupDir,
		"--no-password",
		"--checkpoint=fast",
	)
	if err != nil {
		t.Fatalf("pg_basebackup exec: %v", err)
	}
	if code != 0 {
		t.Fatalf("pg_basebackup exit %d:\n%s", code, out)
	}

	if err := pg.PgVerifybackup(ctx, backupDir); err != nil {
		t.Fatalf("PgVerifybackup: %v", err)
	}
}

// TestPgVerifybackupDetectsCorruption pins the negative path: if a backup
// file is mutated post-backup, pg_verifybackup must report it. Without this
// guard, "PgVerifybackup returns nil" wouldn't actually prove anything —
// the wrapper might be silently masking a verifier failure.
func TestPgVerifybackupDetectsCorruption(t *testing.T) {
	pg := pgtest.StartPG(t, 18)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const backupDir = "/tmp/pgsafe-verify-corrupt"
	if code, out, err := pg.Exec(ctx, "rm", "-rf", backupDir); err != nil || code != 0 {
		t.Fatalf("rm -rf: code=%d err=%v out=%s", code, err, out)
	}
	if code, out, err := pg.Exec(ctx,
		"pg_basebackup",
		"-h", "127.0.0.1",
		"-U", "postgres",
		"-D", backupDir,
		"--no-password",
		"--checkpoint=fast",
	); err != nil || code != 0 {
		t.Fatalf("pg_basebackup: code=%d err=%v out=%s", code, err, out)
	}

	// Mutate one byte of PG_VERSION (small file, present in every cluster).
	// pg_verifybackup re-hashes every file in the manifest and must catch this.
	if code, out, err := pg.Exec(ctx, "sh", "-c", "echo 'X' >> "+backupDir+"/PG_VERSION"); err != nil || code != 0 {
		t.Fatalf("corrupt step: code=%d err=%v out=%s", code, err, out)
	}

	err := pg.PgVerifybackup(ctx, backupDir)
	if err == nil {
		t.Fatal("PgVerifybackup returned nil after corruption — verifier was silently passing")
	}
	if !strings.Contains(err.Error(), "PG_VERSION") {
		t.Errorf("expected error to mention the corrupted file, got: %v", err)
	}
}
