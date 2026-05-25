package retention

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// DefaultWALSegmentSize is the byte size used to translate
// OldestNeededLSN → cutoff segment name. PG's universal default is 16
// MiB; clusters using non-default segment sizes are extraordinarily
// rare. Configurable knob deferred until a real customer hits it.
const DefaultWALSegmentSize int64 = 16 * 1024 * 1024

// PruneResult summarises one prune execution.
type PruneResult struct {
	BackupsDeleted     []string `json:"backups_deleted"`
	FilesDeleted       int      `json:"files_deleted"`
	WALSegmentsDeleted []string `json:"wal_segments_deleted,omitempty"`
}

// Prune executes a Plan against the storage backend. Each expirable
// backup directory is enumerated and deleted file-by-file (the only
// mutating operation `prune` performs); afterwards WAL segments older
// than Plan.OldestNeededLSN are pruned. Errors deleting individual
// files are wrapped with the file path so operators can see which
// objects survived.
//
// dryRun=true returns a fully-populated PruneResult describing what
// WOULD be deleted, without touching the backend.
func Prune(ctx context.Context, b storage.Backend, plan Plan, dryRun bool) (PruneResult, error) {
	res := PruneResult{}
	for _, id := range plan.ExpirableBackupIDs {
		files, err := b.List(ctx, id)
		if err != nil {
			return res, fmt.Errorf("prune: list %s: %w", id, err)
		}
		for _, fi := range files {
			if !dryRun {
				if err := b.Delete(ctx, fi.Path); err != nil {
					return res, fmt.Errorf("prune: delete %s: %w", fi.Path, err)
				}
			}
			res.FilesDeleted++
		}
		res.BackupsDeleted = append(res.BackupsDeleted, id)
	}
	walDeleted, err := pruneWAL(ctx, b, plan.OldestNeededLSN, dryRun)
	if err != nil {
		return res, err
	}
	res.WALSegmentsDeleted = walDeleted
	return res, nil
}

// pruneWAL enumerates wal/<TLI>/<segment> entries and deletes any
// whose segment name lex-precedes the cutoff for that timeline.
// Lex-compare on 24-hex segment names equals chronological order
// within a timeline (PG's name format guarantees it). Across
// timelines the comparison is per-timeline-bounded; older timelines'
// segments stay until any kept backup that needs them is itself
// expired.
//
// oldest=0 means "no kept backups" — refuse to prune WAL in that
// case (the operator would lose every restore point).
func pruneWAL(ctx context.Context, b storage.Backend, oldest manifest.LSN, dryRun bool) ([]string, error) {
	if uint64(oldest) == 0 {
		return nil, nil
	}
	walEntries, err := b.List(ctx, "wal")
	if err != nil {
		return nil, fmt.Errorf("prune: list wal: %w", err)
	}
	type tliCutoff struct{ name string }
	cutoffs := map[uint32]tliCutoff{}
	var deleted []string
	for _, fi := range walEntries {
		base := path.Base(fi.Path)
		if len(base) != 24 {
			continue
		}
		// Parse timeline from the path's parent directory if present
		// (`wal/<TLI>/<seg>`); otherwise the segment's first 8 hex
		// chars are the timeline.
		tli, segSegOK := parseTimelineFromPath(fi.Path)
		if !segSegOK {
			continue
		}
		cut, ok := cutoffs[tli]
		if !ok {
			cutSeg := walSegmentNameForTimeline(tli, oldest)
			cut = tliCutoff{name: cutSeg}
			cutoffs[tli] = cut
		}
		if base >= cut.name {
			continue
		}
		if !dryRun {
			if err := b.Delete(ctx, fi.Path); err != nil {
				return deleted, fmt.Errorf("prune: delete wal %s: %w", fi.Path, err)
			}
		}
		deleted = append(deleted, fi.Path)
	}
	return deleted, nil
}

// parseTimelineFromPath returns the timeline embedded in the path
// `wal/<TLI>/<seg>` (preferred, since the storage layout uses it) or
// in the segment basename's first 8 hex characters (fallback). The
// boolean is false for paths that don't look like WAL segments at all.
func parseTimelineFromPath(p string) (uint32, bool) {
	parts := strings.Split(p, "/")
	if len(parts) >= 3 && parts[0] == "wal" {
		var tli uint32
		if _, err := fmt.Sscanf(parts[len(parts)-2], "%X", &tli); err == nil {
			return tli, true
		}
	}
	base := path.Base(p)
	if len(base) != 24 {
		return 0, false
	}
	var tli uint32
	if _, err := fmt.Sscanf(base[:8], "%X", &tli); err != nil {
		return 0, false
	}
	return tli, true
}

// walSegmentNameForTimeline computes the cutoff segment name for a
// given timeline + LSN at the default segment size. Inlined rather
// than depending on `internal/backup` to keep the dependency graph
// flat (retention is the leaf evaluator).
func walSegmentNameForTimeline(tli uint32, lsn manifest.LSN) string {
	segNo := uint64(lsn) / uint64(DefaultWALSegmentSize)
	hi := uint32(segNo >> 32)        //nolint:gosec // segment number split by definition
	lo := uint32(segNo & 0xFFFFFFFF) //nolint:gosec
	return fmt.Sprintf("%08X%08X%08X", tli, hi, lo)
}
