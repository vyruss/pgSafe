// Package bracket wraps PG's `pg_backup_start` / `pg_backup_stop`
// (PG 15+) and the legacy `pg_start_backup` / `pg_stop_backup` (PG 13/14)
// non-exclusive functions. Used by the remote-parallel backup mode where
// pgSafe does the bracketing itself instead of delegating to pg_basebackup.
package bracket

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/manifest"
)

// Bracket is the abstract handle on a non-replication-protocol backup
// bracket. Production builds bracket.New(pool, version); tests use a
// hand-written mock satisfying this interface.
type Bracket interface {
	// Start begins the backup. label is human-readable; fast=true forces
	// an immediate checkpoint, fast=false waits for a normal checkpoint
	// (slower but lower load on the cluster).
	Start(ctx context.Context, label string, fast bool) (StartInfo, error)

	// Stop ends the backup. Returns the stop LSN and the contents of the
	// backup_label and tablespace_map files (which the caller must
	// write into the storage's backup directory at the cluster root).
	Stop(ctx context.Context) (StopInfo, error)
}

// StartInfo carries pg_backup_start() output.
type StartInfo struct {
	LSN      manifest.LSN
	Timeline uint32
}

// StopInfo carries pg_backup_stop() output. PG 15+ returns label_file +
// spcmap_file as part of the stop call; PG 13/14 returns them via
// pg_stop_backup output too. Both encode tablespace location info if any.
type StopInfo struct {
	LSN         manifest.LSN
	LabelFile   []byte
	SpcMapFile  []byte
	WALSegments []string // segment names covering [start, stop] on this timeline
}

// New returns a Bracket implementation for the given PG major version.
// pgVersion must be >=13 and <=18 (the supported range).
func New(pool *pgxpool.Pool, pgVersion int) (Bracket, error) {
	if pool == nil {
		return nil, errors.New("bracket: pool is required")
	}
	switch {
	case pgVersion >= 15:
		return &bracketModern{pool: pool}, nil
	case pgVersion >= 13:
		return &bracketLegacy{pool: pool}, nil
	default:
		return nil, fmt.Errorf("bracket: PG %d not supported", pgVersion)
	}
}
