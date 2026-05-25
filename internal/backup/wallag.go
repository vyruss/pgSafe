package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

// segmentsPerXLogID is PG's XLogSegmentsPerXLogId macro: how many
// segments fit in one 4 GiB "major" XLog ID. For a 16 MiB segment
// (the default), this is 0x100. Pre-PG-11 the segment size was a
// compile-time constant; it is now operator-controlled at initdb time
// and reported by pg_control.
func segmentsPerXLogID(segmentBytes int64) uint64 {
	return uint64(0x100000000) / uint64(segmentBytes) //nolint:gosec // segmentBytes is PG's xlog_seg_size, always a positive power of two between 1 MiB and 1 GiB.
}

// WALSegmentName formats the canonical 24-hex-char WAL filename for
// the segment containing lsn on the given timeline. Output matches
// PG's `pg_walfile_name(lsn)` AND pgbackrest's walSegmentNext exactly,
// derived from PG's xlog_internal.h:XLogFileName macro:
//
//	segNo  = lsn / segmentBytes
//	major  = segNo / XLogSegmentsPerXLogId   (high 32 bits of LSN, i.e. the
//	                                          part before "/" in PG's text form)
//	minor  = segNo % XLogSegmentsPerXLogId   (segment index within the major)
//	name   = sprintf("%08X%08X%08X", tli, major, minor)
//
// Mismatching this is a foot-gun: the caller polls the backend
// for a name PG never produced, WAL-wait spins to timeout, backup
// fails.
func WALSegmentName(timeline uint32, lsn manifest.LSN, segmentBytes int64) string {
	if segmentBytes <= 0 {
		return ""
	}
	segsPerID := segmentsPerXLogID(segmentBytes)
	segNo := uint64(lsn) / uint64(segmentBytes)
	major := uint32(segNo / segsPerID) //nolint:gosec
	minor := uint32(segNo % segsPerID) //nolint:gosec
	return fmt.Sprintf("%08X%08X%08X", timeline, major, minor)
}

// WALSegmentsBetween returns every segment file required to make a backup
// recoverable from start through stop on the same timeline. If start and
// stop fall within the same segment, the slice has one entry.
//
// stop is treated as the position where the *next* data would be written
// (i.e. exclusive). pg_switch_wal returns this kind of LSN: the START of
// the new (empty) segment. So a stop at exact segment boundary means the
// data ends at the previous segment — that's the last we wait for.
func WALSegmentsBetween(timeline uint32, start, stop manifest.LSN, segmentBytes int64) []string {
	if segmentBytes <= 0 || stop < start {
		return nil
	}
	segsPerID := segmentsPerXLogID(segmentBytes)
	first := uint64(start) / uint64(segmentBytes)
	last := uint64(stop) / uint64(segmentBytes)
	if uint64(stop)%uint64(segmentBytes) == 0 && stop > start && last > 0 {
		// stop falls exactly on a segment boundary — that segment is empty
		// and won't be archived. The last segment with backup-relevant data
		// is the previous one.
		last--
	}
	out := make([]string, 0, last-first+1)
	for n := first; n <= last; n++ {
		major := uint32(n / segsPerID) //nolint:gosec
		minor := uint32(n % segsPerID) //nolint:gosec
		out = append(out, fmt.Sprintf("%08X%08X%08X", timeline, major, minor))
	}
	return out
}

// WaitForWAL polls the configured storage backend until every name in
// segments has appeared at its archive.SegmentKey(timeline, name)
// location, with exponential-backoff retries inside the per-call
// timeout. Returns nil on success, an error wrapping ctx on
// cancellation, a wrapped backend error on a non-ErrNotExist failure
// (auth, permission, configuration), or a timeout message otherwise.
//
// Implements the Invariant #1 polling step of
//
//	The same code path works for
//
// any backend (POSIX, S3, Azure, GCS, SFTP) — the backend's Stat
// implementation is the only thing that varies. Mirrors pgbackrest's
// walSegmentFindOne pattern (storageList over the storage, regardless of
// driver).
//
// Transient retry policy: this loop polls for segment ARRIVAL only.
// Transient transport errors (5xx, network blips) belong inside the
// SDK / backend driver — both AWS, Azure, GCS clients have their own
// jittered exponential backoff and we should not double-retry on top.
// Anything that bubbles up here as a non-ErrNotExist error is
// operator-actionable (auth expired, permission denied, bucket
// missing) and we surface it immediately rather than burning the
// timeout window on a guaranteed-to-fail poll.
func WaitForWAL(ctx context.Context, b storage.Backend, timeline uint32, segments []string, timeout time.Duration) error {
	if len(segments) == 0 {
		return nil
	}
	if b == nil {
		return errors.New("WAL-wait: backend is required")
	}
	deadline := time.Now().Add(timeout)
	delay := 50 * time.Millisecond
	for {
		missing, err := missingSegments(ctx, b, timeline, segments)
		if err != nil {
			return fmt.Errorf("WAL-wait: %w", err)
		}
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: timeout after %s; missing %v", ErrWALWait, timeout, missing)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("WAL-wait: %w", ctx.Err())
		case <-time.After(delay):
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

// missingSegments returns the subset of segments not yet visible in
// the backend. The storage stores each segment as `<seg>-<srcSha>`, so a
// match is "any wal/<TLI>/<seg>-* entry exists." A non-ErrNotExist
// backend error short-circuits with a wrapped error — we trust the
// SDK to have already done its retries.
func missingSegments(ctx context.Context, b storage.Backend, timeline uint32, segments []string) ([]string, error) {
	var out []string
	for _, name := range segments {
		_, found, err := archive.FindSegment(ctx, b, timeline, name)
		if err != nil {
			return nil, err
		}
		if !found {
			out = append(out, name)
		}
	}
	return out, nil
}
