package pgtest

// Pool of long-lived per-PG-version containers used by the matrix test
// (test/matrix/matrix_test.go). One container per PG major version is
// brought up lazily on the first StartPGPooled call; every subsequent
// call for the same version returns a *PG that points at a freshly
// created database in that container, so concurrent tests don't collide
// on schema state.
//
// Lifetime: the pool's containers are torn down by CleanupPool, which
// the matrix test's TestMain calls before exit. StartPGPooled does NOT
// register a t.Cleanup — that would tear the container down at the end
// of the first test that used it, defeating the whole point.
//
// Concurrency: StartPGPooled is safe to call from many goroutines (the
// matrix test fans out via t.Parallel at the outer pg=N level). One
// pooledContainer per version, sync.Once-gated; database creation goes
// through a dedicated mutex per container so we don't open N concurrent
// CREATE DATABASE connections to the same cluster.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// pooledContainer holds one shared container per PG version.
type pooledContainer struct {
	once    sync.Once
	pg      *PG // base *PG for the shared container; DSN points at "postgres" db
	c       *postgres.PostgresContainer
	hostDir string
	err     error

	// dbMu serialises CREATE DATABASE calls to this container so we don't
	// hammer the cluster with N parallel auth+DDL handshakes when many
	// goroutines call StartPGPooled at once.
	dbMu sync.Mutex
}

var (
	poolMu  sync.Mutex
	pool    = map[int]*pooledContainer{}
	dbCount atomic.Int64

	// poolHostDirRoot anchors the per-pooled-container temp directories.
	// Set on first use; cleaned by CleanupPool.
	poolHostDirRoot     string
	poolHostDirRootOnce sync.Once
	poolHostDirRootErr  error
)

// StartPGPooled returns a *PG backed by a long-lived shared container per
// PG version. Each call gets a freshly created database in that container,
// so concurrent matrix-test cells don't collide. The shared container is
// torn down by CleanupPool (called from TestMain).
//
// Use for the matrix test only. Tests that need crash-safety or
// fault-injection isolation should use StartPG (unpooled, fresh container
// per test).
func StartPGPooled(t *testing.T, version int) *PG {
	t.Helper()
	if !isSupportedVersion(version) {
		t.Fatalf("StartPGPooled: unsupported version %d (want one of %v)", version, SupportedVersions)
	}

	pc := getPooledContainer(version)
	pc.once.Do(func() {
		hostDir, err := poolHostDirFor(version)
		if err != nil {
			pc.err = fmt.Errorf("host dir: %w", err)
			return
		}
		pc.hostDir = hostDir
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		pc.pg, pc.c, pc.err = createContainer(ctx, version, hostDir)
	})
	if pc.err != nil {
		t.Fatalf("StartPGPooled(%d): %v", version, pc.err)
	}

	return cloneWithFreshDB(t, pc)
}

// CleanupPool terminates every pooled container and removes their host
// temp directories. Idempotent. The matrix test's TestMain must call this
// before exit; otherwise containers leak past the test process.
func CleanupPool() {
	poolMu.Lock()
	versions := make([]*pooledContainer, 0, len(pool))
	for _, pc := range pool {
		versions = append(versions, pc)
	}
	pool = map[int]*pooledContainer{}
	poolMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, pc := range versions {
		if pc.c != nil {
			_ = pc.c.Terminate(ctx)
		}
	}
}

func getPooledContainer(version int) *pooledContainer {
	poolMu.Lock()
	defer poolMu.Unlock()
	pc, ok := pool[version]
	if !ok {
		pc = &pooledContainer{}
		pool[version] = pc
	}
	return pc
}

// cloneWithFreshDB creates a unique database in the pooled container and
// returns a *PG whose DSN points at that database. The base superDSN
// (pointing at the bootstrap "postgres" database) is preserved on
// SuperDSN so callers can still issue cluster-level admin queries.
func cloneWithFreshDB(t *testing.T, pc *pooledContainer) *PG {
	t.Helper()
	dbName := fmt.Sprintf("matrix_v%d_%d", pc.pg.Version, dbCount.Add(1))

	pc.dbMu.Lock()
	defer pc.dbMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, pc.pg.SuperDSN)
	if err != nil {
		t.Fatalf("StartPGPooled: connect super: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("StartPGPooled: CREATE DATABASE %s: %v", dbName, err)
	}

	cp := *pc.pg
	cp.DSN = swapDatabase(pc.pg.DSN, dbName)
	cp.SuperDSN = swapDatabase(pc.pg.SuperDSN, dbName)
	return &cp
}

// swapDatabase rewrites the database segment of a DSN URL: the trailing
// "/<dbname>" path component before any "?query" is replaced with /<new>.
// pgtest builds DSNs in URL form (e.g. postgres://user:pw@host:port/postgres?sslmode=disable),
// so this is structural rather than a regex hunt.
func swapDatabase(dsn, newDB string) string {
	q := ""
	base := dsn
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		base = dsn[:i]
		q = dsn[i:]
	}
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[:i+1] + newDB
	}
	return base + q
}

func poolHostDirFor(version int) (string, error) {
	poolHostDirRootOnce.Do(func() {
		// One root, used for the lifetime of the test process.
		// We create per-version subdirs under it.
		root, err := os.MkdirTemp("", "pgsafe-matrix-pool-")
		if err != nil {
			poolHostDirRootErr = err
			return
		}
		poolHostDirRoot = root
	})
	if poolHostDirRootErr != nil {
		return "", poolHostDirRootErr
	}
	dir := fmt.Sprintf("%s/v%d", poolHostDirRoot, version)
	return dir, nil
}
