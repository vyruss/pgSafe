package backup

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

// parseBackupLabel extracts the START WAL LOCATION and START TIMELINE fields
// from a PG-emitted backup_label. The format is documented at
// https://www.postgresql.org/docs/18/continuous-archiving.html (paragraph
// "The contents of the backup_label file"); we parse only the fields the
// caller actually needs.
func parseBackupLabel(text string) (manifest.LSN, uint32, error) {
	mLSN := regexp.MustCompile(`(?m)^START WAL LOCATION:\s+([0-9A-Fa-f]+/[0-9A-Fa-f]+)`)
	mTLI := regexp.MustCompile(`(?m)^START TIMELINE:\s+(\d+)`)

	lsnMatch := mLSN.FindStringSubmatch(text)
	if lsnMatch == nil {
		return 0, 0, errors.New("parseBackupLabel: missing START WAL LOCATION")
	}
	tliMatch := mTLI.FindStringSubmatch(text)
	if tliMatch == nil {
		return 0, 0, errors.New("parseBackupLabel: missing START TIMELINE")
	}

	lsn, err := manifest.ParseLSN(lsnMatch[1])
	if err != nil {
		return 0, 0, fmt.Errorf("parseBackupLabel: %w", err)
	}
	tli, err := strconv.ParseUint(tliMatch[1], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("parseBackupLabel: timeline: %w", err)
	}
	return lsn, uint32(tli), nil //nolint:gosec // strconv guarantees uint32 fit
}

// switchAndCurrentLSN forces a WAL switch on a primary, then reads the
// current insert LSN. The switched-out segment is now complete and is being
// archived by archive_command — that's the segment WaitForWAL is waiting for.
//
// On a standby SELECT pg_switch_wal() errors; only backs up from a
// primary so that's not a concern (hardening for standby).
func switchAndCurrentLSN(ctx context.Context, pool *pgxpool.Pool) (manifest.LSN, error) {
	if pool == nil {
		return 0, errors.New("switchAndCurrentLSN: pool is required")
	}
	var switchLSN string
	if err := pool.QueryRow(ctx, `SELECT pg_switch_wal()::text`).Scan(&switchLSN); err != nil {
		return 0, fmt.Errorf("pg_switch_wal: %w", err)
	}
	// pg_switch_wal returns the LSN of the segment switch boundary. The next
	// reads of pg_current_wal_insert_lsn give the post-switch position; we
	// use the switch LSN itself as our stop LSN since that's where the
	// segment we care about ends.
	lsn, err := manifest.ParseLSN(switchLSN)
	if err != nil {
		return 0, fmt.Errorf("parse switch LSN %q: %w", switchLSN, err)
	}
	return lsn, nil
}

// hashWALSegments computes SHA-256 of each named segment as stored in
// the configured storage backend. Used to populate the
// Storage-Metadata.json sidecar. Backend-agnostic: the caller
// streams via b.Get so any driver works (POSIX, S3, Azure, GCS, SFTP).
//
// Each segment is stored at "wal/<TLI>/<seg>-<srcSha>"; the suffix is
// the source SHA which we don't know here, so FindSegment is what
// resolves the actual key.
func hashWALSegments(ctx context.Context, b storage.Backend, timeline uint32, names []string) ([]manifest.WALSegmentRecord, error) {
	out := make([]manifest.WALSegmentRecord, 0, len(names))
	for _, n := range names {
		key, found, err := archive.FindSegment(ctx, b, timeline, n)
		if err != nil {
			return nil, fmt.Errorf("hashWALSegments: %w", err)
		}
		if !found {
			return nil, fmt.Errorf("hashWALSegments: %s not found in storage", n)
		}
		rc, err := b.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("hashWALSegments: backend.Get %s: %w", key, err)
		}
		h := sha256.New()
		size, copyErr := io.Copy(h, rc)
		_ = rc.Close()
		if copyErr != nil {
			return nil, fmt.Errorf("hashWALSegments: read %s: %w", key, copyErr)
		}
		var sum [32]byte
		copy(sum[:], h.Sum(nil))
		out = append(out, manifest.WALSegmentRecord{
			Name:   n,
			Size:   size,
			SHA256: sum,
		})
	}
	return out, nil
}
