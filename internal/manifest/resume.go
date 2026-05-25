package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// ResumeCheckpoint is the live, mid-backup checkpoint pgsafe writes
// to <backupID>/backup_manifest.copy after every K files. It is
// pgsafe-internal (NOT PG's backup_manifest format) and exists solely
// to support resume of an interrupted backup: the next invocation
// reads it back, validates compatibility, and reuses files whose
// SHA-256 matches.
//
// Mirrors pgbackrest's backup.manifest.copy in role; we don't share
// its on-disk shape — pgbackrest uses a custom INI-ish file, pgsafe
// uses pretty-printed JSON for ergonomic ad-hoc inspection.
//
// The validation envelope (everything except Files) is the resume
// gate per RESUME.md: any mismatched field invalidates the checkpoint
// and the next backup starts fresh.
type ResumeCheckpoint struct {
	// Schema version of this checkpoint. Bump when the resume format
	// changes in a breaking way; older checkpoints will fail
	// version-match and be ignored (fresh backup, no error).
	Version int `json:"version"`

	// PgsafeVersion is the binary version that wrote the checkpoint.
	// pgsafe refuses to resume across binary versions — implementers
	// stay free to change file formats / filter chains across
	// releases without invariant-breaking fallout.
	PgsafeVersion string `json:"pgsafe_version"`

	// BackupID is the directory the resumable files live under.
	// Embedded for forensics; the discovery loop already knows the
	// ID from the directory listing, but having it inside the
	// checkpoint defends against operator drag-and-drop mistakes.
	BackupID string `json:"backup_id"`

	// BackupType discriminates full vs. incremental. Full-resume only
	// resumes a full; incr-resume only resumes an incr against the
	// same parent (see ParentBackupID).
	BackupType string `json:"backup_type"`

	// ParentBackupID is the parent in an incremental chain. Empty
	// for full backups. Mismatch invalidates resume.
	ParentBackupID string `json:"parent_backup_id,omitempty"`

	// Compression and EncryptionRecipients lock the filter-chain
	// shape: same plaintext + same chain produces identical bytes
	// on disk, which is what makes the SHA-based reuse gate sound.
	Compression          string   `json:"compression"`
	EncryptionRecipients []string `json:"encryption_recipients"`

	// SystemIdentifier guards against operator pointing config at a
	// different cluster — a SystemIdentifier mismatch always
	// invalidates resume regardless of every other field.
	SystemIdentifier uint64 `json:"system_identifier"`

	// Timeline + StartLSN + StartTime — informational; not part of
	// the resume gate (the file SHAs are what gate reuse) but useful
	// in forensics and operator-facing diagnostics.
	Timeline  uint32    `json:"timeline"`
	StartLSN  LSN       `json:"start_lsn"`
	StartTime time.Time `json:"start_time"`

	// CheckpointedAt is the wall-clock time of the most recent
	// checkpoint Put. Used by `prune` to decide whether a
	// `.copy`-only backup directory is "stale" (older than the
	// resume_grace_period) and can be reaped.
	CheckpointedAt time.Time `json:"checkpointed_at"`

	// Files is the per-file index of what's already on disk under
	// <backupID>/<path>, with the plaintext SHA-256 used to gate
	// reuse. Order is the order files were appended via AddFile —
	// stable but not load-bearing for resume.
	Files []ResumeFileEntry `json:"files"`
}

// ResumeFileEntry is one file's reuse gate. Path matches the manifest
// path (relative to the backup directory). Size and SHA256 are the
// PLAINTEXT-side digest (what restore will see after decrypt +
// decompress); RepoSize and RepoSHA256 are the on-storage digest
// (what a worker would compute by sha256-ing the file at
// <backupID>/<path> directly). The repo digest is the resume gate —
// it lets the worker verify the prior attempt's bytes-on-disk are
// intact without needing the encryption identity. Mirrors
// pgbackrest's repoFileChecksum field.
type ResumeFileEntry struct {
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	SHA256     [32]byte  `json:"sha256"`
	ModTime    time.Time `json:"mod_time"`
	RepoSize   int64     `json:"repo_size,omitempty"`
	RepoSHA256 [32]byte  `json:"repo_sha256,omitempty"`
}

// ResumeCheckpointVersion is the on-disk schema version. Increment
// when adding fields that older readers must reject; merely
// appending optional fields does not require a bump.
const ResumeCheckpointVersion = 1

// MarshalResumeCheckpoint serializes c as indented JSON. Indentation
// is for human readability; round-trip semantics are unchanged.
func MarshalResumeCheckpoint(c ResumeCheckpoint) ([]byte, error) {
	if c.Version == 0 {
		c.Version = ResumeCheckpointVersion
	}
	return json.MarshalIndent(c, "", "  ")
}

// UnmarshalResumeCheckpoint parses data into a ResumeCheckpoint,
// rejecting unknown fields so a schema bump in a newer pgsafe binary
// is caught loudly by an older reader (resume refused, fresh backup
// starts — same defensive posture as the sidecar reader).
func UnmarshalResumeCheckpoint(data []byte) (ResumeCheckpoint, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c ResumeCheckpoint
	if err := dec.Decode(&c); err != nil {
		return ResumeCheckpoint{}, fmt.Errorf("manifest: decode resume checkpoint: %w", err)
	}
	if c.Version != ResumeCheckpointVersion {
		return ResumeCheckpoint{}, fmt.Errorf("manifest: resume checkpoint version %d, want %d", c.Version, ResumeCheckpointVersion)
	}
	return c, nil
}

// AccumulatedFiles returns the per-file records the Builder has
// accumulated so far. The returned slice is a defensive copy — the
// caller is free to retain or modify it without affecting the
// builder. Used by the resume-checkpoint writer to snapshot the
// files-so-far without coupling the checkpoint code to the Builder's
// internal field layout.
func (b *Builder) AccumulatedFiles() []ResumeFileEntry {
	out := make([]ResumeFileEntry, len(b.files))
	for i, f := range b.files {
		out[i] = ResumeFileEntry{
			Path:       f.path,
			Size:       f.size,
			SHA256:     f.sha256,
			ModTime:    f.modTime,
			RepoSize:   f.repoSize,
			RepoSHA256: f.repoSHA256,
		}
	}
	return out
}
