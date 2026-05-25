// Package info builds operator-friendly views of a pgSafe storage backend.
// It walks the backend, decodes every backup's Storage-Metadata.json
// sidecar, and returns a slice of BackupRecord ordered chronologically.
// building block reused by `pgsafe info`, `pgsafe verify`,
// `pgsafe prune`, and `pgsafe check`.
package info

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// BackupRecord is one entry in the listing. Fields combine the sidecar
// (server-side metadata pgSafe writes) and the manifest header (PG-native
// structural facts the caller captured).
type BackupRecord struct {
	BackupID       string    `json:"backup_id"`
	Type           string    `json:"type"` // "full" | "incremental"
	ParentBackupID string    `json:"parent_backup_id,omitempty"`
	Server         string    `json:"server"`
	Compression    string    `json:"compression,omitempty"`
	Recipients     []string  `json:"encryption_recipients,omitempty"`
	Annotation     string    `json:"annotation,omitempty"`
	StartLSN       string    `json:"start_lsn,omitempty"`
	StopLSN        string    `json:"stop_lsn,omitempty"`
	Timeline       uint32    `json:"timeline,omitempty"`
	StartTime      time.Time `json:"start_time,omitempty"`
	StopTime       time.Time `json:"stop_time,omitempty"`
	Files          int       `json:"files,omitempty"`
	Bytes          int64     `json:"bytes,omitempty"`
}

// Age returns the duration since this backup completed. Zero StopTime
// (e.g. a backup whose manifest didn't yet record it) returns 0.
func (r BackupRecord) Age() time.Duration {
	if r.StopTime.IsZero() {
		return 0
	}
	return time.Since(r.StopTime)
}

// List walks the backend and returns the chronologically-ordered list of
// BackupRecord. Errors decoding any single sidecar are reported as
// per-record warnings (see Warnings) — a corrupt sidecar shouldn't make
// `info` fail to display the rest of the storage.
func List(ctx context.Context, b storage.Backend) ([]BackupRecord, []Warning, error) {
	all, err := b.List(ctx, "")
	if err != nil {
		return nil, nil, fmt.Errorf("info: List: %w", err)
	}

	// Find candidate backup directories — top-level entries whose ID
	// suffix matches the caller's "F" / "I" convention.
	dirs := map[string]struct{}{}
	for _, fi := range all {
		parts := strings.SplitN(fi.Path, "/", 2)
		if len(parts) < 2 {
			continue
		}
		first := parts[0]
		if !strings.HasSuffix(first, "F") && !strings.HasSuffix(first, "I") {
			continue
		}
		dirs[first] = struct{}{}
	}

	var (
		out      []BackupRecord
		warnings []Warning
	)
	ids := make([]string, 0, len(dirs))
	for id := range dirs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		rec, err := loadOne(ctx, b, id)
		if err != nil {
			warnings = append(warnings, Warning{BackupID: id, Err: err})
			continue
		}
		out = append(out, rec)
	}
	return out, warnings, nil
}

// Warning records a decoding error for one backup directory. `info` shows
// these as red entries; `prune` skips them (refuses to delete what it
// can't decode).
type Warning struct {
	BackupID string
	Err      error
}

func (w Warning) String() string {
	return fmt.Sprintf("warning: backup %s: %v", w.BackupID, w.Err)
}

// loadOne reads a single backup's sidecar (and as much of the manifest as
// `internal/restore` already knows how to parse) into a BackupRecord.
func loadOne(ctx context.Context, b storage.Backend, backupID string) (BackupRecord, error) {
	rec := BackupRecord{BackupID: backupID}

	// Type derives from the suffix.
	switch {
	case strings.HasSuffix(backupID, "I"):
		rec.Type = manifest.BackupTypeIncremental
		// "<parent>_<ts>I" — parent is the part before the underscore.
		if i := strings.LastIndex(backupID[:len(backupID)-1], "_"); i > 0 {
			rec.ParentBackupID = backupID[:i]
		}
	case strings.HasSuffix(backupID, "F"):
		rec.Type = manifest.BackupTypeFull
	}

	// Sidecar (server, compression, recipients, annotation).
	sc, err := readSidecar(ctx, b, backupID)
	if err != nil {
		return rec, fmt.Errorf("read sidecar: %w", err)
	}
	rec.Server = sc.Server
	rec.Compression = sc.Compression
	rec.Recipients = sc.EncryptionRecipients
	rec.Annotation = sc.Annotation
	if sc.Type != "" {
		// Sidecar wins over the suffix-derived guess (e.g. for unusual
		// backup-ID schemes a future cycle might introduce).
		rec.Type = sc.Type
	}
	if sc.ParentBackupID != "" {
		rec.ParentBackupID = sc.ParentBackupID
	}

	// Manifest (timeline, LSNs, file count, total bytes, times).
	if hdr, err := readManifestHeader(ctx, b, backupID); err == nil {
		rec.Timeline = hdr.Timeline
		rec.StartLSN = hdr.StartLSN
		rec.StopLSN = hdr.StopLSN
		rec.StartTime = hdr.StartTime
		rec.StopTime = hdr.StopTime
		rec.Files = hdr.FileCount
		rec.Bytes = hdr.Bytes
	}
	return rec, nil
}

// readSidecar reads + decodes the Storage-Metadata.json sidecar for one
// backup.
func readSidecar(ctx context.Context, b storage.Backend, backupID string) (manifest.Sidecar, error) {
	rc, err := b.Get(ctx, path.Join(backupID, "Storage-Metadata.json"))
	if err != nil {
		return manifest.Sidecar{}, err
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return manifest.Sidecar{}, err
	}
	return manifest.UnmarshalSidecar(body)
}

// manifestHeader is the subset of backup_manifest fields `info` cares
// about. Extracted by a forgiving JSON decoder (we don't fail if an
// expected field is missing — the manifest is PG-native, not pgSafe-owned).
type manifestHeader struct {
	Timeline  uint32
	StartLSN  string
	StopLSN   string
	StartTime time.Time
	StopTime  time.Time
	FileCount int
	Bytes     int64
}

func readManifestHeader(ctx context.Context, b storage.Backend, backupID string) (manifestHeader, error) {
	rc, err := b.Get(ctx, path.Join(backupID, "backup_manifest"))
	if err != nil {
		return manifestHeader{}, err
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return manifestHeader{}, err
	}

	var raw struct {
		Files     []rawFile     `json:"Files"`
		WALRanges []rawWALRange `json:"WAL-Ranges"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return manifestHeader{}, err
	}

	hdr := manifestHeader{
		FileCount: len(raw.Files),
	}
	for _, f := range raw.Files {
		hdr.Bytes += f.Size
		if t, err := time.Parse("2006-01-02 15:04:05 GMT", f.LastModified); err == nil {
			if hdr.StartTime.IsZero() || t.Before(hdr.StartTime) {
				hdr.StartTime = t
			}
			if t.After(hdr.StopTime) {
				hdr.StopTime = t
			}
		}
	}
	if len(raw.WALRanges) > 0 {
		hdr.Timeline = raw.WALRanges[0].Timeline
		hdr.StartLSN = raw.WALRanges[0].StartLSN
		hdr.StopLSN = raw.WALRanges[0].EndLSN
	}
	return hdr, nil
}

type rawFile struct {
	Path         string `json:"Path"`
	Size         int64  `json:"Size"`
	LastModified string `json:"Last-Modified"`
}

type rawWALRange struct {
	Timeline uint32 `json:"Timeline"`
	StartLSN string `json:"Start-LSN"`
	EndLSN   string `json:"End-LSN"`
}
