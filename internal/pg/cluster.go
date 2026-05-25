// Package pg defines the Cluster interface — the seam between the caller
// (internal/backup) and PostgreSQL. Production composes
// internal/pg/conn + identity + basebackup; tests use hand-written
// mocks. The interface intentionally hides BASE_BACKUP implementation
// details (shell-out vs. native libpq replication client) so the
// caller stays decoupled.
package pg

import (
	"archive/tar"
	"context"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/pg/basebackup"
	"github.com/vyruss/pgsafe/internal/pg/conn"
	"github.com/vyruss/pgsafe/internal/pg/identity"
)

// BaseBackupStream is the iterator the caller consumes. The concrete
// production type is *basebackup.Stream; tests use a hand-written struct that
// implements these two methods.
type BaseBackupStream interface {
	Next() (*tar.Header, io.Reader, error)
	Close() error
}

// Cluster is the abstract handle on a PG instance.
type Cluster interface {
	// Identity returns the cluster's stable identifiers (system identifier,
	// timeline, control version).
	Identity(ctx context.Context) (identity.Identity, error)

	// BaseBackup launches a BASE_BACKUP. The returned stream yields the tar
	// stream entry-by-entry; the caller is responsible for closing it.
	BaseBackup(ctx context.Context, opts basebackup.Options) (BaseBackupStream, error)

	// Close releases any underlying resources (pgxpool, etc.).
	Close()
}

// RealCluster is the production implementation. It owns a pgxpool.Pool for
// SQL queries; BaseBackup delegates to internal/pg/basebackup which spawns
// pg_basebackup as a subprocess.
type RealCluster struct {
	pool *pgxpool.Pool
}

// Open establishes the connection pool. Subsequent Identity/BaseBackup calls
// reuse it.
func Open(ctx context.Context, dsn string) (*RealCluster, error) {
	pool, err := conn.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &RealCluster{pool: pool}, nil
}

// Identity calls into internal/pg/identity using the existing pool.
func (c *RealCluster) Identity(ctx context.Context) (identity.Identity, error) {
	return identity.Read(ctx, c.pool)
}

// BaseBackup spawns pg_basebackup with the configured DSN.
func (c *RealCluster) BaseBackup(ctx context.Context, opts basebackup.Options) (BaseBackupStream, error) {
	return basebackup.Start(ctx, opts)
}

// Close drains the pool. Safe to call multiple times.
func (c *RealCluster) Close() {
	if c.pool != nil {
		c.pool.Close()
		c.pool = nil
	}
}
