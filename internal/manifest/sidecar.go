// Package manifest produces and consumes the PG-native backup_manifest JSON
// the pgSafe Storage-Metadata.json sidecar
// (this file).
package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// SidecarVersion is the schema version of Storage-Metadata.json. Bump
// only with a deliberate compat plan; readers reject unknown fields, so a
// bump is a wire-protocol break.
//
// Schema history:
//   - v1: WALSegments + Directories + EncryptionRecipients +
//     Compression.
//   - v2: adds Type and ParentBackupID for incremental
//     chain reconstruction.
//   - v3: adds Annotation (operator-set free-form note)
//     and LockHeld (true for sidecars written by an in-progress backup,
//     used by Invariant-#4 crash diagnosis to spot killed-mid-backup state).
//     v2 readers see new fields silently because both are `omitempty`.
const SidecarVersion = 3

// BackupTypeFull marks a self-contained backup (the chain root).
const BackupTypeFull = "full"

// BackupTypeIncremental marks a backup that depends on its ParentBackupID.
const BackupTypeIncremental = "incremental"

// Sidecar is the pgSafe-private companion to PG's backup_manifest JSON. It
// carries information PG's spec doesn't (encryption recipients, codec, our
// observed WAL segment hashes) and is read by every command that needs to
// know how the backup was produced.
type Sidecar struct {
	Version              int                `json:"version"`
	Server               string             `json:"server"`
	EncryptionRecipients []string           `json:"encryption_recipients"`
	Compression          string             `json:"compression"`
	StorageLayoutVersion int                `json:"storage_layout_version"`
	WALSegments          []WALSegmentRecord `json:"wal_segments"`

	// SystemIdentifier is PG's pg_control system_identifier — a
	// 64-bit cluster-unique value created by initdb. Pgsafe records
	// it on every backup so subsequent backups can detect that the
	// operator is pointing the same storage+server name at a different
	// cluster (cluster restored from another backup, initdb'd a new
	// cluster, accidentally pointed config at staging vs prod, etc.).
	// Empty in pre-#10 sidecars; readers treat empty as "trust the
	// caller" for backwards compatibility with existing storages.
	SystemIdentifier uint64 `json:"system_identifier,omitempty"`

	// Directories lists relative paths of empty directories captured by the
	// backup. PG's tar stream includes empty dir entries (pg_notify,
	// pg_stat, etc.) but the manifest's Files array only covers regular
	// files; restore must recreate these by walking this list.
	Directories []string `json:"directories"`

	// Type is BackupTypeFull or BackupTypeIncremental. Empty in v1 sidecars;
	// readers default empty to BackupTypeFull for backwards compatibility
	// with backups.
	Type string `json:"type,omitempty"`

	// ParentBackupID is the BackupID of the immediate parent in the
	// incremental chain. Empty for full backups; required for incremental
	// backups.
	ParentBackupID string `json:"parent_backup_id,omitempty"`

	// Annotation is an operator-supplied free-form note attached via
	// `pgsafe annotate`. Persists in the sidecar so it travels with the
	// backup. v2 sidecars decode with empty annotation.
	Annotation string `json:"annotation,omitempty"`

	// LockHeld is true for sidecars written by an in-progress backup; the
	// final atomic-rename of the manifest flips it false. A sidecar found
	// with LockHeld=true and no committed manifest is a killed-mid-backup
	// state Invariant-#4 crash diagnosis can spot. v2 sidecars default to
	// false on read.
	LockHeld bool `json:"lock_held,omitempty"`
}

// WALSegmentRecord captures the shape of one WAL segment observed by the
// WAL-wait phase of a backup (Invariant #1). Stored in the sidecar; not
// part of PG's backup_manifest spec.
type WALSegmentRecord struct {
	Name   string   `json:"name"`
	Size   int64    `json:"size"`
	SHA256 [32]byte `json:"sha256"`
}

// MarshalSidecar serializes s as indented JSON. The indentation is for human
// readability; round-trip semantics are unchanged.
func MarshalSidecar(s Sidecar) ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}

// UnmarshalSidecar parses data into a Sidecar, rejecting unknown fields so
// schema bumps are caught loudly.
func UnmarshalSidecar(data []byte) (Sidecar, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var s Sidecar
	if err := dec.Decode(&s); err != nil {
		return Sidecar{}, fmt.Errorf("manifest: decode sidecar: %w", err)
	}
	return s, nil
}
