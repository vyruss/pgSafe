// Package pgtest provides a test-only helper that spins up a PostgreSQL
// container (via testcontainers-go) configured with the prerequisites locked
// : wal_level=replica,
// archive_mode=on, a backup user with REPLICATION + pg_read_server_files,
// and an archive_command pointing at a host-mounted directory. summarize_wal
// is enabled on PG 17+ (where it exists) for incremental-backup testing.
//
// Tests under //go:build integration call StartPG(t, version) — or the
// compatibility shim StartPG18(t) — and receive a DSN plus the
// host path of the WAL archive directory.
package pgtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SupportedVersions lists every PG major-version that pgSafe targets. The
// E2E matrix iterates over this slice; CI gates fail if any cell breaks.
var SupportedVersions = []int{13, 14, 15, 16, 17, 18}

// PG is a running PostgreSQL test instance with the cluster prerequisites
// applied for whichever version was requested.
type PG struct {
	// Version is the PG major version (13..18).
	Version int

	// DSN authenticates as the pgsafe backup user (REPLICATION attribute).
	DSN string

	// SuperDSN authenticates as the test superuser; for setup queries only.
	SuperDSN string

	// WALArchive is the host path the cluster's archive_command writes to.
	WALArchive string

	// container is the underlying testcontainers handle, exposed via Exec.
	// Tests use this to run PG-version-matched binaries (pg_basebackup,
	// pg_verifybackup) inside the same image that produced the data — avoids
	// requiring N PG-client installs on the test host.
	container testcontainers.Container
}

// StartPG launches a fresh postgres:<version> container with all
// pgSafe-required prerequisites applied. The container terminates when the
// test ends (t.Cleanup). version must be one of SupportedVersions.
func StartPG(t *testing.T, version int) *PG {
	t.Helper()
	if !isSupportedVersion(version) {
		t.Fatalf("StartPG: unsupported version %d (want one of %v)", version, SupportedVersions)
	}
	pg, c, err := createContainer(context.Background(), version, t.TempDir())
	if err != nil {
		t.Fatalf("StartPG (PG %d): %v", version, err)
	}
	t.Cleanup(func() {
		_ = c.Terminate(context.Background())
	})
	return pg
}

// createContainer is the shared container-bring-up logic used by both
// StartPG (unpooled, lifetime tied to t.Cleanup) and StartPGPooled (in
// pool.go, lifetime tied to the package-level pool). Returns the *PG plus
// the container handle so the caller can manage its lifetime.
//
// hostDir is the temp directory the host-side WAL-archive bind-mount and
// HBA-init script live in; callers pass t.TempDir() (unpooled) or a
// package-managed directory (pooled).
func createContainer(ctx context.Context, version int, hostDir string) (*PG, *postgres.PostgresContainer, error) {
	walArchive := filepath.Join(hostDir, "wal-archive")
	if err := os.MkdirAll(walArchive, 0o777); err != nil { //nolint:gosec // container postgres user writes here
		return nil, nil, fmt.Errorf("mkdir wal-archive: %w", err)
	}
	if err := os.Chmod(walArchive, 0o777); err != nil { //nolint:gosec
		return nil, nil, fmt.Errorf("chmod wal-archive: %w", err)
	}

	// archive_command writes into /var/lib/pgsafe-wal/<TLI>/<seg>-<sha256-hex>
	// matching pgsafe's archive.SegmentKey layout (`wal/<TLI>/<seg>-<sha>`,
	// the same suffixed-name pattern pgbackrest uses). The bind-mount
	// on the host puts WALArchive at the same place the storage
	// backend's `wal/` prefix points to.
	archiveCmd := "f=%f; tli=$(echo $f | cut -c1-8); sha=$(sha256sum %p | cut -d' ' -f1); mkdir -p /var/lib/pgsafe-wal/$tli && chmod 0777 /var/lib/pgsafe-wal/$tli && test ! -f /var/lib/pgsafe-wal/$tli/$f-$sha && cp %p /var/lib/pgsafe-wal/$tli/$f-$sha && chmod 0666 /var/lib/pgsafe-wal/$tli/$f-$sha"

	hbaScript := filepath.Join(hostDir, "00-pgsafe-replication-hba.sh")
	hbaScriptContent := "#!/bin/sh\nset -e\n" +
		"echo 'host replication all all trust' >> \"$PGDATA/pg_hba.conf\"\n" +
		"echo 'host all all all trust' >> \"$PGDATA/pg_hba.conf\"\n"
	if err := os.WriteFile(hbaScript, []byte(hbaScriptContent), 0o755); err != nil { //nolint:gosec
		return nil, nil, fmt.Errorf("write hba script: %w", err)
	}

	// summarize_wal is a PG 17+ GUC; setting it on older versions errors with
	// "unrecognized configuration parameter".
	pgArgs := []string{
		"postgres",
		"-c", "wal_level=replica",
		"-c", "archive_mode=on",
		"-c", "archive_command=" + archiveCmd,
	}
	if version >= 17 {
		pgArgs = append(pgArgs, "-c", "summarize_wal=on")
	}

	image := "postgres:" + strconv.Itoa(version)
	c, err := postgres.Run(ctx, image,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_INITDB_ARGS": "--encoding=UTF8 --locale=C",
		}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds,
				walArchive+":/var/lib/pgsafe-wal:rw",
				hbaScript+":/docker-entrypoint-initdb.d/00-pgsafe-replication-hba.sh:ro",
			)
		}),
		testcontainers.WithCmd(pgArgs...),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres.Run: %w", err)
	}

	superDSN, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, nil, fmt.Errorf("ConnectionString: %w", err)
	}

	// Create the backup user. pg_read_server_files is a predefined role in PG 11+.
	conn, err := pgx.Connect(ctx, superDSN)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, nil, fmt.Errorf("connect super: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	for _, s := range []string{
		`CREATE USER pgsafe WITH PASSWORD 'pgsafe' REPLICATION LOGIN`,
		`GRANT pg_read_server_files TO pgsafe`,
	} {
		if _, err := conn.Exec(ctx, s); err != nil &&
			!strings.Contains(err.Error(), "already exists") {
			_ = c.Terminate(ctx)
			return nil, nil, fmt.Errorf("setup: %s: %w", s, err)
		}
	}

	backupDSN := strings.Replace(superDSN, "postgres:postgres@", "pgsafe:pgsafe@", 1)

	return &PG{
		Version:    version,
		DSN:        backupDSN,
		SuperDSN:   superDSN,
		WALArchive: walArchive,
		container:  c,
	}, c, nil
}

// StartPG18 is a compatibility shim. Identical to StartPG(t, 18).
// Existing tests that expect specifically PG 18 keep using this name; the
// matrix uses StartPG(t, version).
func StartPG18(t *testing.T) *PG {
	t.Helper()
	return StartPG(t, 18)
}

// MustExec runs a single SQL statement against the superuser connection,
// failing the test on error. Convenience for setup-time DDL.
func (p *PG) MustExec(t *testing.T, sql string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, p.SuperDSN)
	if err != nil {
		t.Fatalf("MustExec connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("MustExec %q: %v", trim(sql), err)
	}
}

func isSupportedVersion(v int) bool {
	for _, s := range SupportedVersions {
		if v == s {
			return true
		}
	}
	return false
}

func trim(s string) string {
	if len(s) <= 80 {
		return s
	}
	return fmt.Sprintf("%s... (%d chars)", s[:80], len(s))
}
