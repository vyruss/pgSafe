// Package archive is pgSafe's native asynchronous WAL archive. PG's
// archive_command shells out to `pgsafe archive-push %p`; PG's
// restore_command shells out to `pgsafe archive-get %f %p`. The exact
// CLI shape lives in cmd/pgsafe/archive.go; this package owns the
// storage-side Push/Get/Probe primitives and the Invariant #7
// idempotency story.
//
// Storage layout for archived WAL:
//
//	<storage-root>/wal/<timeline>/<segment>-<sha256-hex>
//
// The SHA-256 suffix encodes the source-side hash of the raw PG WAL
// bytes (as PG hands them to archive_command via %p). Mirrors
// pgbackrest's filename-suffix idempotency: identical source bytes
// always produce the same suffixed filename regardless of what
// transformation the storage backend (or any future filter chain
// applied to WAL) does to the bytes-on-disk. Two same-named segments
// arriving with different source bytes show up as two separate keys
// (collision detection is loud), and an idempotent retry of the same
// bytes is a perfect filename hit.
//
// Invariant #7 — "async WAL push idempotency under concurrent retry":
//
//	Push(seg) lists wal/<tli>/<seg>-* via Backend.List.
//	  - No match → write to <seg>-<srcHash>. Done.
//	  - Exact-suffix match (existing == srcHash) → idempotent no-op.
//	  - Match with a different suffix → ErrSegmentMismatch. Operator
//	    must investigate (real archive corruption: same segment name,
//	    different source bytes — split brain or PG bug).
//	  - Multiple matches → ErrSegmentMismatch (would have meant the
//	    last write was non-deterministic). Loud-fail.
//
// This pattern survives changes to compression / encryption / storage
// driver because the SHA is computed BEFORE any transformation. A
// pgsafe upgrade that flips the WAL filter chain doesn't break
// archive idempotency; the suffix stays stable.
package archive

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/vyruss/pgsafe/internal/storage"
)

// ErrSegmentMismatch is returned by Push when the destination already holds
// a segment with the requested name but different bytes. This is a
// loud-failure case — the operator must investigate; it usually means
// archive corruption (someone replaced a segment) or a bug in pgSafe.
var ErrSegmentMismatch = errors.New("wal/archive: existing segment differs from source")

// SegmentDir returns the storage-relative directory that holds all
// suffix-encoded variants of a given segment: "wal/<TLI>/". List this
// to find the actual stored filename, which is one of
// "<seg>-<sha256-hex>" entries.
func SegmentDir(timeline uint32) string {
	return fmt.Sprintf("wal/%08X", timeline)
}

// SegmentKey returns the canonical storage-relative key for one WAL
// segment given its source-side SHA-256 hash. The SHA-suffix encodes
// the raw PG WAL bytes (post-`%p`, pre any pgsafe transformation), so
// idempotent re-pushes of the same source land at the same key
// regardless of how the bytes-on-storage are stored.
func SegmentKey(timeline uint32, segmentName string, srcHash [32]byte) string {
	return fmt.Sprintf("%s/%s-%s", SegmentDir(timeline), segmentName, hex.EncodeToString(srcHash[:]))
}

// SegmentKeyPrefix returns the unsuffixed prefix of a segment's storage
// key: "wal/<TLI>/<seg>-". Used by the WAL probe + WAL-wait to find a
// segment by name without knowing its source-SHA suffix.
func SegmentKeyPrefix(timeline uint32, segmentName string) string {
	return fmt.Sprintf("%s/%s-", SegmentDir(timeline), segmentName)
}

// SegmentNameFromBasename strips the "-<sha256-hex>" suffix from a
// storage-stored WAL basename like "<seg>-<sha>" and returns the bare
// PG-recognised segment name. Returns ok=false for any name that
// doesn't fit the expected layout (24-char hex segment + '-' + 64-char
// hex SHA).
func SegmentNameFromBasename(base string) (string, bool) {
	if len(base) != 24+1+64 || base[24] != '-' {
		return "", false
	}
	return base[:24], true
}

// FindSegment returns the full storage key for the named segment by
// listing wal/<TLI>/ and matching the prefix. The boolean is false
// (with no error) when no segment with that name is in the storage
// yet. Multiple matches are reported as ErrSegmentMismatch — that
// would mean the same source segment landed at two different SHA
// suffixes, which can only be archive corruption or a pgsafe bug.
func FindSegment(ctx context.Context, b storage.Backend, timeline uint32, segmentName string) (string, bool, error) {
	prefix := SegmentKeyPrefix(timeline, segmentName)
	infos, err := b.List(ctx, SegmentDir(timeline))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("wal/archive: list %s: %w", SegmentDir(timeline), err)
	}
	var matches []string
	for _, fi := range infos {
		if strings.HasPrefix(fi.Path, prefix) && !strings.HasSuffix(fi.Path, ".tmp") {
			// Exclude in-flight `.<rand>.tmp` keys from the match set
			// so a concurrent push doesn't masquerade as a finished
			// segment.
			matches = append(matches, fi.Path)
		}
	}
	switch len(matches) {
	case 0:
		return "", false, nil
	case 1:
		return matches[0], true, nil
	default:
		return "", false, fmt.Errorf("%w: %d copies of %s in storage: %v",
			ErrSegmentMismatch, len(matches), segmentName, matches)
	}
}

// Push uploads one WAL segment file at sourcePath into b. timeline is
// the PG timeline ID the segment belongs to (decoded from the
// filename's first 8 hex chars by the caller). Returns nil on success
// (including the idempotent no-op case where an identical segment
// already exists at the same source-SHA suffix).
//
// The destination key is "wal/<TLI>/<seg>-<srcSha>" where srcSha is
// the SHA-256 of the raw bytes PG handed us via %p — the same scheme
// pgbackrest uses (with SHA-1 instead). Same source bytes always
// land at the same key; an idempotent retry sees the existing key and
// skips the upload. A re-archive of the same segment name with
// different bytes (split-brain primary, archive corruption) lands at
// a DIFFERENT key — both copies are visible and FindSegment surfaces
// it as ErrSegmentMismatch on the next read.
func Push(ctx context.Context, b storage.Backend, timeline uint32, sourcePath string) error {
	segName := filepath.Base(sourcePath)

	// Hash the source first — the source SHA is the suffix of the
	// destination key.
	srcHash, err := hashFile(sourcePath)
	if err != nil {
		return fmt.Errorf("wal/archive: hash source %s: %w", sourcePath, err)
	}
	key := SegmentKey(timeline, segName, srcHash)

	// Idempotent fast-path: same source SHA already in the storage.
	if _, statErr := b.Stat(ctx, key); statErr == nil {
		return nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("wal/archive: stat %s: %w", key, statErr)
	}

	// Concurrent-push race window: another worker may have committed
	// THE SAME source bytes (same SHA) between our Stat above and now.
	// FindSegment surfaces that as a hit on our exact `key`; treat
	// that as idempotent. A hit on a DIFFERENT suffix means the
	// segment name was already archived with different source bytes
	// — real archive corruption (split-brain primary, etc.); surface
	// loudly.
	if other, found, err := FindSegment(ctx, b, timeline, segName); err != nil {
		return err
	} else if found {
		if other == key {
			return nil
		}
		return fmt.Errorf("%w: %s already in storage at %s with different source SHA",
			ErrSegmentMismatch, segName, other)
	}

	// Fresh upload. Per-call random tmp suffix so concurrent pushers
	// can't collide on the same `.tmp` key — each one's Put lands in
	// its own staging file and they race fairly for the Commit. With
	// the SHA-suffix scheme any race winner is by definition byte-for-
	// byte identical to the loser's source, so a Commit collision is
	// also a clean idempotent success.
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return fmt.Errorf("wal/archive: rand: %w", err)
	}
	tmpKey := fmt.Sprintf("%s.%s.tmp", key, hex.EncodeToString(rnd[:]))
	if err := streamFileTo(ctx, b, sourcePath, tmpKey); err != nil {
		// Best-effort: drop any partial blob the failed Put may have
		// finalized on the backend (some cloud SDKs commit on Close
		// even from a mid-stream error path). No-op for POSIX.
		_ = b.Delete(ctx, tmpKey)
		return err
	}
	commitErr := b.Commit(ctx, tmpKey, key)
	if commitErr == nil {
		return nil
	}
	// Concurrent pusher already promoted an identical-SHA tmp to the
	// final key. The bytes ARE the source bytes (same SHA), so this is
	// idempotent success. Clean up our now-orphan tmp.
	if errors.Is(commitErr, os.ErrExist) {
		_ = b.Delete(ctx, tmpKey)
		return nil
	}
	// Non-os.ErrExist Commit failure: also try to delete our tmp so we
	// don't leave a one-shot orphan behind on a cloud backend.
	if delErr := b.Delete(ctx, tmpKey); delErr != nil && !errors.Is(delErr, os.ErrNotExist) {
		_ = delErr
	}
	return fmt.Errorf("wal/archive: Commit %s → %s: %w", tmpKey, key, commitErr)
}

// streamFileTo Put-then-Close streams sourcePath into key.
func streamFileTo(ctx context.Context, b storage.Backend, sourcePath, key string) error {
	src, err := os.Open(sourcePath) //nolint:gosec // operator-supplied path by design (PG passes %p)
	if err != nil {
		return fmt.Errorf("wal/archive: open %s: %w", sourcePath, err)
	}
	defer func() { _ = src.Close() }()
	wc, err := b.Put(ctx, key)
	if err != nil {
		return fmt.Errorf("wal/archive: Put %s: %w", key, err)
	}
	if _, err := io.Copy(wc, src); err != nil {
		_ = wc.Close()
		return fmt.Errorf("wal/archive: copy %s → %s: %w", sourcePath, key, err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("wal/archive: close %s: %w", key, err)
	}
	return nil
}

// hashFile returns the SHA-256 of the file at path. Streaming, no full
// file read into memory.
func hashFile(path string) ([32]byte, error) {
	var sum [32]byte
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return sum, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return sum, err
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// Get downloads one WAL segment from b into destPath. Used by PG's
// restore_command via `pgsafe archive-get %f %p`. Stream-copies; we
// don't fsync because PG fsyncs after replay.
//
// The storage stores segments at "wal/<TLI>/<seg>-<sha>"; we don't know
// the SHA suffix at restore time, so FindSegment lists the directory
// and picks the matching prefix.
func Get(ctx context.Context, b storage.Backend, timeline uint32, segmentName, destPath string) error {
	key, found, err := FindSegment(ctx, b, timeline, segmentName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("wal/archive: Get %s/%s: %w", SegmentDir(timeline), segmentName, os.ErrNotExist)
	}
	rc, err := b.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("wal/archive: Get %s: %w", key, err)
	}
	defer func() { _ = rc.Close() }()

	if err := os.MkdirAll(filepath.Dir(destPath), 0o700); err != nil {
		return fmt.Errorf("wal/archive: mkdir for %s: %w", destPath, err)
	}
	dst, err := os.Create(destPath) //nolint:gosec // dest is operator-supplied (PG passes %p)
	if err != nil {
		return fmt.Errorf("wal/archive: create %s: %w", destPath, err)
	}
	if _, err := io.Copy(dst, rc); err != nil {
		_ = dst.Close()
		return fmt.Errorf("wal/archive: copy %s → %s: %w", key, destPath, err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("wal/archive: close %s: %w", destPath, err)
	}
	return nil
}

// IsSegmentName reports whether s looks like a PG WAL segment file name
// (24 hex chars, no extension). Strictly the bare-segment form; for the
// permissive "any file PG ships via archive_command" check use
// IsArchivableFile.
func IsSegmentName(s string) bool {
	s = path.Base(s)
	if len(s) != 24 {
		return false
	}
	return isHex(s)
}

// IsArchivableFile reports whether name is one of the four file forms
// PG hands to archive_command via %p:
//
//   - "<24-hex>"                           — bare WAL segment
//   - "<24-hex>.<8-hex>.backup"            — pg_backup_stop label marker
//   - "<24-hex>.partial"                   — end-of-recovery partial segment
//   - "<8-hex>.history"                    — timeline history file
//
// archive-push must accept all four; rejecting any non-segment form is
// a foot-gun (PG retries the same archive_command forever and blocks
// the archiver). pgbackrest accepts the same set
// (src/info/archive.c:archiveValidateExtension).
func IsArchivableFile(name string) bool {
	name = path.Base(name)
	if IsSegmentName(name) {
		return true
	}
	switch {
	case len(name) == 24+1+8+len(".backup") && name[24] == '.' && name[33] == '.' &&
		strings.HasSuffix(name, ".backup") &&
		isHex(name[:24]) && isHex(name[25:33]):
		return true
	case len(name) == 24+len(".partial") && strings.HasSuffix(name, ".partial") &&
		isHex(name[:24]):
		return true
	case len(name) == 8+len(".history") && strings.HasSuffix(name, ".history") &&
		isHex(name[:8]):
		return true
	}
	return false
}

func isHex(s string) bool {
	for _, c := range s {
		if !strings.ContainsRune("0123456789ABCDEFabcdef", c) {
			return false
		}
	}
	return true
}

// TimelineFromSegment extracts the timeline ID from any archivable
// file's leading hex prefix. Bare segments, .backup markers, and
// .partial files share the 8-hex-char timeline at offset 0; .history
// files have an 8-hex-char timeline of their own at offset 0.
func TimelineFromSegment(seg string) (uint32, error) {
	seg = path.Base(seg)
	if !IsArchivableFile(seg) {
		return 0, fmt.Errorf("wal/archive: %q is not a PG archive file", seg)
	}
	var tli uint32
	if _, err := fmt.Sscanf(seg[:8], "%08X", &tli); err != nil {
		return 0, fmt.Errorf("wal/archive: parse timeline from %q: %w", seg, err)
	}
	return tli, nil
}
