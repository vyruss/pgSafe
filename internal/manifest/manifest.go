// PostgreSQL backup_manifest JSON producer.
//
// PG's manifest format (PostgreSQL-Backup-Manifest-Version: 2 in PG 18) is
// not arbitrary JSON: the trailing "Manifest-Checksum" field is a SHA-256 of
// the manifest body, computed at the byte boundary where that field starts.
// Re-formatting the body changes the checksum, so this generator emits the
// exact whitespace pattern PG uses and matches PG's checksum scheme.
//
//  (Builder), §3.2.4 (SHA-256 plaintext
// hashes from the filter chain plug straight into AddFile).

package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ManifestVersion is the PostgreSQL-Backup-Manifest-Version this builder
// emits. PG 18 uses version 2.
const ManifestVersion = 2

// LSN is a PostgreSQL Log Sequence Number — a 64-bit value displayed as
// "high32/low32" hex (e.g. "0/12345678"). The zero value is the canonical
// "before any WAL" sentinel.
type LSN uint64

// String formats the LSN in PG's canonical "hi/lo" hex form (uppercase, no
// leading zeros).
func (l LSN) String() string {
	v := uint64(l)
	hi := uint32(v >> 32)        //nolint:gosec // PG LSN: high 32 bits by definition
	lo := uint32(v & 0xFFFFFFFF) //nolint:gosec // PG LSN: low 32 bits by definition
	return fmt.Sprintf("%X/%X", hi, lo)
}

// ParseLSN parses PG's canonical "hi/lo" hex form. Hex digits are case-
// insensitive; leading zeros are tolerated.
func ParseLSN(s string) (LSN, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, fmt.Errorf("manifest: invalid LSN %q", s)
	}
	if strings.Contains(parts[1], "/") {
		return 0, fmt.Errorf("manifest: invalid LSN %q", s)
	}
	var hi, lo uint64
	if _, err := fmt.Sscanf(parts[0], "%x", &hi); err != nil {
		return 0, fmt.Errorf("manifest: invalid LSN %q: %w", s, err)
	}
	if _, err := fmt.Sscanf(parts[1], "%x", &lo); err != nil {
		return 0, fmt.Errorf("manifest: invalid LSN %q: %w", s, err)
	}
	if hi > 0xFFFFFFFF || lo > 0xFFFFFFFF {
		return 0, fmt.Errorf("manifest: LSN component out of range in %q", s)
	}
	return LSN(hi<<32 | lo), nil
}

// BackupStartInfo carries the start-of-backup state captured from PG via
// pg_backup_start() (wires the actual call) plus cluster identity.
type BackupStartInfo struct {
	SystemIdentifier uint64
	Timeline         uint32
	StartLSN         LSN
	StartTime        time.Time
}

// BackupStopInfo carries pg_backup_stop() output. only needs the
// stop LSN (for the WAL-Ranges entry) and the stop time.
type BackupStopInfo struct {
	StopLSN  LSN
	StopTime time.Time
}

// Builder accumulates per-file state and the WAL range, then produces the
// full manifest on Finalize. Builders are not safe for concurrent use; one
// builder per backup, one goroutine.
type Builder struct {
	start BackupStartInfo
	files []fileRec
}

type fileRec struct {
	path        string
	size        int64
	sha256      [32]byte
	modTime     time.Time
	incremental bool
	blockCount  uint32 // populated only when incremental=true

	// Repo-side accounting (for resume): on-storage byte count and
	// SHA-256 of the encrypted+compressed bytes that landed at
	// <backupID>/<path>. Zero values mean "no repo info captured" —
	// the file won't be reusable on the next attempt's resume gate.
	// Populated via SetLatestRepoChecksum after AddFile from the
	// caller's filter.Result (the chain produces both plaintext and
	// repo digests in one pass).
	repoSize   int64
	repoSHA256 [32]byte
}

// NewBuilder returns a Builder seeded with the start info.
func NewBuilder(start BackupStartInfo) *Builder {
	return &Builder{start: start}
}

// UpdateStartLSN replaces the Start-LSN+timeline initially seeded into the
// Builder. Used by the caller after parsing backup_label, whose
// "START WAL LOCATION" is the canonical start of the backup. The seeded
// CheckpointLSN/timeline at NewBuilder time may pre-date pg_backup_start;
// pg_combinebackup rejects the chain when the manifest's WAL-Ranges
// Start-LSN doesn't match what pg_basebackup --incremental recorded.
func (b *Builder) UpdateStartLSN(lsn LSN, timeline uint32) {
	b.start.StartLSN = lsn
	b.start.Timeline = timeline
}

// AddFile records one file. SHA-256 is the digest of the plaintext (per the
// filter chain ).
func (b *Builder) AddFile(path string, size int64, sha256Sum [32]byte, modTime time.Time) {
	b.files = append(b.files, fileRec{
		path:    path,
		size:    size,
		sha256:  sha256Sum,
		modTime: modTime.UTC(),
	})
}

// SetLatestRepoChecksum populates the repo-side digest for the
// most-recently-added file. Two-step setter (rather than extending
// AddFile's signature) keeps the manifest API stable for callers that
// don't have repo info (e.g. inline blobs, WAL segment hashing) while
// letting the resume gate read repo digests from the same Builder.
//
// No-op if no file has been added yet — callers that always pair
// AddFile + SetLatestRepoChecksum are immune to this; ones that don't
// pair them safely fall through to "no repo info captured".
func (b *Builder) SetLatestRepoChecksum(repoSize int64, repoSHA [32]byte) {
	if len(b.files) == 0 {
		return
	}
	b.files[len(b.files)-1].repoSize = repoSize
	b.files[len(b.files)-1].repoSHA256 = repoSHA
}

// Finalize produces the manifest bytes. The closing Manifest-Checksum is a
// SHA-256 of every byte that comes before it.
func (b *Builder) Finalize(stop BackupStopInfo) ([]byte, error) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "{ \"PostgreSQL-Backup-Manifest-Version\": %d,\n", ManifestVersion)
	fmt.Fprintf(&buf, "\"System-Identifier\": %d,\n", b.start.SystemIdentifier)

	buf.WriteString("\"Files\": [")
	for i, f := range b.files {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('\n')
		fmt.Fprintf(&buf, `{ "Path": %s, "Size": %d, "Last-Modified": "%s", "Checksum-Algorithm": "SHA256", "Checksum": "%s"`,
			jsonString(f.path),
			f.size,
			f.modTime.UTC().Format("2006-01-02 15:04:05 GMT"),
			hex.EncodeToString(f.sha256[:]),
		)
		if f.incremental {
			buf.WriteString(`, "Incremental": true`)
		}
		buf.WriteString(" }")
	}
	if len(b.files) > 0 {
		buf.WriteByte('\n')
	}
	buf.WriteString("],\n")

	buf.WriteString("\"WAL-Ranges\": [")
	buf.WriteString("\n")
	fmt.Fprintf(&buf, `{ "Timeline": %d, "Start-LSN": "%s", "End-LSN": "%s" }`,
		b.start.Timeline, b.start.StartLSN, stop.StopLSN)
	buf.WriteString("\n],\n")

	// Compute SHA-256 of everything written so far; that's the Manifest-Checksum.
	sum := sha256.Sum256(buf.Bytes())
	fmt.Fprintf(&buf, "\"Manifest-Checksum\": \"%s\"}\n", hex.EncodeToString(sum[:]))

	return buf.Bytes(), nil
}

// jsonString returns a JSON-quoted string literal for s. We hand-roll instead
// of pulling in encoding/json because the surrounding manifest format is
// hand-formatted to match PG's whitespace exactly — encoding/json would not
// give us byte-precise output.
func jsonString(s string) string {
	var buf strings.Builder
	buf.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&buf, `\u%04x`, r)
			} else {
				buf.WriteRune(r)
			}
		}
	}
	buf.WriteByte('"')
	return buf.String()
}
