package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// WALSource picks how the bracket-spanning WAL — every record between
// pg_backup_start and pg_backup_stop — gets attached to a backup so
// restore can replay it. PG's physical-backup contract requires this
// WAL to exist somewhere; pgsafe supports three delivery shapes:
//
//   - WALSourceArchive: the WAL flows through PG's archive_command
//     (typically `pgsafe archive-push %p`) into <storage>/wal/<TLI>/
//     before pg_backup_stop returns. Same model as pgbackrest's
//     archive-tied default. Required when the operator wants PITR
//     beyond the bracket window — the archive keeps growing past
//     the backup, extending the recoverable range.
//
//   - WALSourceStream: pg_basebackup grabs the bracket WAL inline
//     during backup via a second replication connection (PG-native
//     --wal-method=stream/fetch). The WAL ends up in the backup tar
//     under pg_wal/. Self-contained backup; no external archive
//     required for THIS backup to restore. Operators can still run
//     archive_mode=on independently for PITR.
//
//   - WALSourceWalgrab: the pgSafe-mode worker has direct read access
//     to $PGDATA/pg_wal — it ships the bracket segments straight
//     from disk after pg_backup_stop, no second replication
//     connection needed. Only available when caller != pghost and a
//     worker process is in the loop.
//
// Restore doesn't need to know which source produced a backup — it
// looks for pg_wal/<bracket-segs> inside the backup first and falls
// back to <storage>/wal/<TLI>/ if absent. The source choice is purely
// a delivery question on the backup side.
type WALSource string

const (
	// WALSourceArchive polls <storage>/wal/<TLI>/ until bracket
	// segments arrive. Default — pre-WALSource code paths get this.
	WALSourceArchive WALSource = "archive"

	// WALSourceStream tells pg_basebackup to pack bracket WAL into
	// the backup tar via --wal-method=stream/fetch.
	WALSourceStream WALSource = "stream"

	// WALSourceWalgrab has the pgSafe-mode worker read
	// $PGDATA/pg_wal directly after pg_backup_stop. Caller != pghost only.
	WALSourceWalgrab WALSource = "walgrab"
)

// AcquireOptions parameterises AcquireBracketWAL. Bundling the inputs
// in a struct keeps the three call sites symmetrical and lets us add
// fields (e.g. a worker handle for walgrab) without touching their
// shape every time.
type AcquireOptions struct {
	Backend   storage.Backend
	Timeline  uint32
	StartLSN  manifest.LSN
	StopLSN   manifest.LSN
	SegSize   int64
	Timeout   time.Duration
	WALSource WALSource
}

// walRecordsNeeded reports whether the WAL-record sidecar field
// should be populated for a backup taken with this source.
//
//   - archive (or empty): yes — bracket segments live in <storage>/wal/<TLI>/,
//     hashWALSegments fetches them and records hashes into the sidecar.
//   - stream / walgrab: no — bracket WAL is inside the backup at
//     <backup-id>/pg_wal/<seg>, the manifest already records every file
//     in the backup, and a separate "WAL-shipped-to-archive" view
//     would just be misleading.
func walRecordsNeeded(s WALSource) bool {
	return s == "" || s == WALSourceArchive
}

// AcquireBracketWAL ensures the WAL covering [StartLSN, StopLSN] is
// available for restore, dispatching on WALSource. Returns the
// segment names so the caller can hash them for the manifest.
func AcquireBracketWAL(ctx context.Context, opts AcquireOptions) ([]string, error) {
	source := opts.WALSource
	if source == "" {
		source = WALSourceArchive
	}
	segments := WALSegmentsBetween(opts.Timeline, opts.StartLSN, opts.StopLSN, opts.SegSize)
	switch source {
	case WALSourceArchive:
		if err := WaitForWAL(ctx, opts.Backend, opts.Timeline, segments, opts.Timeout); err != nil {
			return nil, err
		}
		return segments, nil
	case WALSourceStream:
		// pg_basebackup --wal-method=fetch already packed the bracket
		// WAL into the data tar's pg_wal/ entries — they were written
		// to <backup-id>/pg_wal/<seg> by the streaming filter chain.
		// Nothing to wait for; nothing to fetch from the archive.
		return segments, nil
	case WALSourceWalgrab:
		// The pgSafe-mode worker reads $PGDATA/pg_wal/<seg> directly
		// after pg_backup_stop and routes the bytes through the same
		// StreamFile/StreamChunk pipeline as data files. The fetch
		// itself happens in pgsafe_worker.go (this dispatch only
		// computes the segment list); like stream, no archive poll.
		return segments, nil
	default:
		return nil, fmt.Errorf("unknown WAL source %q", source)
	}
}
