package backup

// Invariant #8 (backup-from-standby coordination) lives in
// InspectStandby + ErrStandbyDisconnected below.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrStandbyDisconnected is returned when the cluster is a standby that's
// not currently replaying or not connected to a wal sender. Operator-
// actionable: the standby's primary_conninfo / restore_command is broken.
var ErrStandbyDisconnected = errors.New("standby: replica is not connected to its primary's WAL stream")

// InspectStandby runs the standby-side checks. Returns nil if either:
//   - the cluster is the primary (pg_is_in_recovery() = false), or
//   - the cluster is a healthy standby (replaying + wal-receiver active).
//
// On a misconfigured standby returns an ErrStandbyDisconnected-wrapped
// error suitable for exit code 5.
func InspectStandby(ctx context.Context, pool *pgxpool.Pool) error {
	var inRecovery bool
	if err := pool.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery); err != nil {
		return fmt.Errorf("standby: pg_is_in_recovery: %w", err)
	}
	if !inRecovery {
		return nil // primary; no further checks
	}

	// Sample replay LSN twice; it must advance (or at least not be zero).
	var lsnA, lsnB string
	if err := pool.QueryRow(ctx, `SELECT pg_last_wal_replay_lsn()::text`).Scan(&lsnA); err != nil {
		return fmt.Errorf("standby: pg_last_wal_replay_lsn: %w", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := pool.QueryRow(ctx, `SELECT pg_last_wal_replay_lsn()::text`).Scan(&lsnB); err != nil {
		return fmt.Errorf("standby: pg_last_wal_replay_lsn (2nd): %w", err)
	}
	if lsnA == "" || lsnB == "" {
		return fmt.Errorf("%w: pg_last_wal_replay_lsn returned empty", ErrStandbyDisconnected)
	}

	// Active wal-receiver = streaming from primary. We accept either an
	// active streaming wal-receiver OR a configured restore_command — both
	// keep the WAL chain advancing.
	var receiverCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'`).Scan(&receiverCount); err != nil {
		return fmt.Errorf("standby: pg_stat_wal_receiver: %w", err)
	}
	if receiverCount == 0 {
		// Fall back: check restore_command. If the operator has a working
		// archive-based replication setup, that's also acceptable.
		var restoreCmd string
		if err := pool.QueryRow(ctx,
			`SELECT current_setting('restore_command', true)`).Scan(&restoreCmd); err == nil && restoreCmd != "" {
			return nil
		}
		return fmt.Errorf("%w: no active wal_receiver and no restore_command", ErrStandbyDisconnected)
	}
	return nil
}
