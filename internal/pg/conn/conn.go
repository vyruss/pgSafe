// Package conn is the pgx/v5 connection-pool seam for pgSafe. Subcommands
// that need plain SQL (identity, future bracket, info, verify) acquire a
// *pgxpool.Pool from here; replication-protocol operations (base backup) go
// through internal/pg/basebackup, which doesn't share this layer.
//
// sslmode=prefer is the default (matches libpq).
package conn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultConnectTimeout is how long Connect blocks before giving up.
// CLI flag surface (§3.2.8) doesn't expose this; future cycles can promote
// it if operator demand surfaces.
const DefaultConnectTimeout = 10 * time.Second

// Connect parses dsn, defaults sslmode=prefer if absent, and opens a pool.
// The returned pool is alive until the caller calls Close().
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("conn: parse %q: %w", dsn, err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if cfg.ConnConfig.TLSConfig == nil && !strings.Contains(dsn, "sslmode=") {
		// pgx defaults sslmode based on TLS availability; force "prefer" per
		// project convention when the operator hasn't opted in or out.
		cfg.ConnConfig.RuntimeParams["sslmode"] = "prefer"
	}
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = DefaultConnectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("conn: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("conn: ping: %w", err)
	}
	return pool, nil
}
