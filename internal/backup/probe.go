package backup

// Invariant #5 (WAL archive reachability) lives in ProbeArchive +
// ErrArchiveUnreachable below.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

// ErrArchiveUnreachable is returned when the sentinel doesn't appear within the
// configured timeout. Operator-actionable: their archive_command is broken.
var ErrArchiveUnreachable = errors.New("probe: WAL archive unreachable — archive_command failed or timed out")

// ProbeArchive verifies the operator's archive_command is shipping
// WAL into the configured storage. Steps:
//
//  1. SELECT pg_create_restore_point — emits a cheap WAL record so the
//     subsequent pg_switch_wal isn't a no-op.
//  2. SELECT pg_walfile_name(pg_switch_wal()) — forces PG to roll the
//     current segment and returns its filename. The sealed segment is
//     now flowing through the operator's archive_command.
//  3. Poll backend.Stat(archive.SegmentKey(tli, segName)) until it
//     appears or the timeout elapses.
//
// The forced WAL switch is a real WAL operation, not a synthetic
// sentinel — PG always rolls when pg_switch_wal() is called. The next
// backup's pg_backup_start() picks up from the new segment, so the
// probe doesn't inflate WAL volume by more than one switch per backup.
func ProbeArchive(ctx context.Context, pool *pgxpool.Pool, b storage.Backend, timeout time.Duration) error {
	if b == nil {
		return fmt.Errorf("probe: backend is required")
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	// Force WAL activity so the subsequent pg_switch_wal isn't a no-op.
	// PG's pg_switch_wal is documented to do nothing if no records have
	// been written since the last switch; in that case archive_command is
	// not invoked and the probe spins forever. pg_create_restore_point
	// emits one WAL record cheaply.
	probeName := fmt.Sprintf("pgsafe-probe-%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx,
		`SELECT pg_create_restore_point($1)`, probeName); err != nil {
		return fmt.Errorf("probe: pg_create_restore_point: %w", err)
	}

	// Force a WAL switch and capture the now-sealed segment's filename.
	// pg_switch_wal returns the LSN of the *next* (empty) segment, which
	// won't archive until it itself fills/switches. The segment we expect
	// to see in the archive is the one containing the LSN BEFORE the
	// switch — that's the sealed segment archive_command is shipping.
	var segName string
	if err := pool.QueryRow(ctx, `
		WITH pre AS (SELECT pg_current_wal_lsn() AS lsn),
		     post AS (SELECT pg_switch_wal())
		SELECT pg_walfile_name(pre.lsn) FROM pre, post
	`).Scan(&segName); err != nil {
		return fmt.Errorf("probe: pg_switch_wal: %w", err)
	}

	tli, err := archive.TimelineFromSegment(segName)
	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}

	// Poll the backend for ANY <seg>-<sha> match. The SHA suffix is
	// what archive-push wrote; we don't know it here, so FindSegment
	// lists wal/<TLI>/ and picks the entry whose prefix matches.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, found, findErr := archive.FindSegment(ctx, b, tli, segName)
		if findErr != nil {
			return fmt.Errorf("probe: %w", findErr)
		}
		if found {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("probe: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("%w: %s not seen in storage after %s", ErrArchiveUnreachable, segName, timeout)
}

// IsArchiveUnreachable reports whether err originated in ProbeArchive.
// The CLI uses this to map probe failures to the same exit code as
// post-bracket WAL-wait timeouts.
func IsArchiveUnreachable(err error) bool { return errors.Is(err, ErrArchiveUnreachable) }
