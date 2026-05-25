// Package identity reads the cluster identity fields from a running PG via
// SQL functions. pgSafe never parses pg_control on disk; we ask the running
// cluster instead.
//
// : pg_control_system, pg_control_init,
// pg_control_checkpoint, pg_control_recovery.
package identity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/manifest"
)

// Identity is the snapshot of cluster identity used by every backup. The
// SystemIdentifier and Timeline are written to the backup_manifest; the rest
// support cross-version sanity checks
type Identity struct {
	SystemIdentifier uint64
	PGControlVersion int32
	CatalogVersion   int32
	Timeline         uint32
	CheckpointLSN    manifest.LSN
	WALSegmentSize   int64
	IsInRecovery     bool
}

// Read returns the cluster identity. Each piece comes from a separate SQL
// function so different PG versions can vary one without breaking others.
func Read(ctx context.Context, pool *pgxpool.Pool) (Identity, error) {
	var id Identity

	// pg_control_system: system_identifier, pg_control_version, catalog_version_no
	row := pool.QueryRow(ctx, `
		SELECT system_identifier, pg_control_version, catalog_version_no
		FROM pg_control_system()
	`)
	if err := row.Scan(&id.SystemIdentifier, &id.PGControlVersion, &id.CatalogVersion); err != nil {
		return Identity{}, fmt.Errorf("identity: pg_control_system: %w", err)
	}

	// pg_control_checkpoint: timeline_id, checkpoint_lsn (LSN type returned as text)
	var checkpointLSNText string
	row = pool.QueryRow(ctx, `
		SELECT timeline_id, checkpoint_lsn::text
		FROM pg_control_checkpoint()
	`)
	if err := row.Scan(&id.Timeline, &checkpointLSNText); err != nil {
		return Identity{}, fmt.Errorf("identity: pg_control_checkpoint: %w", err)
	}
	parsed, err := manifest.ParseLSN(checkpointLSNText)
	if err != nil {
		return Identity{}, fmt.Errorf("identity: parse checkpoint_lsn %q: %w", checkpointLSNText, err)
	}
	id.CheckpointLSN = parsed

	// pg_control_init: bytes_per_wal_segment (the WAL segment size)
	row = pool.QueryRow(ctx, `SELECT bytes_per_wal_segment FROM pg_control_init()`)
	if err := row.Scan(&id.WALSegmentSize); err != nil {
		return Identity{}, fmt.Errorf("identity: pg_control_init: %w", err)
	}

	// pg_is_in_recovery: are we on a standby?
	row = pool.QueryRow(ctx, `SELECT pg_is_in_recovery()`)
	if err := row.Scan(&id.IsInRecovery); err != nil {
		return Identity{}, fmt.Errorf("identity: pg_is_in_recovery: %w", err)
	}

	return id, nil
}
