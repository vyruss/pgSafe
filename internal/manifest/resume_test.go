package manifest_test

import (
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
)

// TestResumeCheckpointRoundTrip pins the marshal/unmarshal contract
// for the .copy file. If this regresses, resume turns into a silent
// no-op — every backup runs as a fresh backup because the discovery
// step can't decode older checkpoints. Catches schema drift loudly.
func TestResumeCheckpointRoundTrip(t *testing.T) {
	t.Parallel()
	want := manifest.ResumeCheckpoint{
		Version:       manifest.ResumeCheckpointVersion,
		PgsafeVersion: "v0.5.0",
		BackupID:      "20260430T120000F",
		BackupType:    "full",
		Compression:   "zstd:3",
		EncryptionRecipients: []string{
			"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p",
		},
		SystemIdentifier: 6789012345,
		Timeline:         1,
		StartLSN:         manifest.LSN(0x3000028),
		StartTime:        time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
		CheckpointedAt:   time.Date(2026, 4, 30, 12, 0, 5, 0, time.UTC),
		Files: []manifest.ResumeFileEntry{
			{Path: "PG_VERSION", Size: 3, SHA256: [32]byte{1, 2, 3}, ModTime: time.Date(2026, 4, 30, 12, 0, 1, 0, time.UTC)},
			{Path: "global/pg_control", Size: 8192, SHA256: [32]byte{4, 5, 6}, ModTime: time.Date(2026, 4, 30, 12, 0, 2, 0, time.UTC)},
		},
	}

	bytes, err := manifest.MarshalResumeCheckpoint(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := manifest.UnmarshalResumeCheckpoint(bytes)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.BackupID != want.BackupID || got.BackupType != want.BackupType {
		t.Errorf("BackupID/Type: got %+v, want %+v", got, want)
	}
	if got.SystemIdentifier != want.SystemIdentifier {
		t.Errorf("SystemIdentifier: got %d, want %d", got.SystemIdentifier, want.SystemIdentifier)
	}
	if len(got.Files) != len(want.Files) {
		t.Fatalf("Files: len got %d, want %d", len(got.Files), len(want.Files))
	}
	for i := range got.Files {
		if got.Files[i] != want.Files[i] {
			t.Errorf("Files[%d]: got %+v, want %+v", i, got.Files[i], want.Files[i])
		}
	}
}

// TestResumeCheckpointRejectsUnknownFields pins the defensive posture:
// a future pgsafe binary that adds fields will produce checkpoints an
// older binary can't decode → older binary starts a fresh backup
// (loud, correct) rather than silently acting on a partial parse
// (quiet, dangerous). Mirrors the sidecar reader's stance.
func TestResumeCheckpointRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	body := []byte(`{
        "version": 1,
        "pgsafe_version": "v0.5.0",
        "backup_id": "X",
        "backup_type": "full",
        "compression": "zstd:3",
        "encryption_recipients": [],
        "system_identifier": 1,
        "timeline": 1,
        "start_lsn": 0,
        "start_time": "2026-04-30T12:00:00Z",
        "checkpointed_at": "2026-04-30T12:00:00Z",
        "files": [],
        "fictional_field_from_the_future": 42
    }`)
	_, err := manifest.UnmarshalResumeCheckpoint(body)
	if err == nil {
		t.Fatal("expected error decoding unknown field")
	}
	if !strings.Contains(err.Error(), "fictional_field_from_the_future") {
		t.Errorf("error %q should name the unknown field", err)
	}
}

// TestResumeCheckpointRejectsVersionMismatch — version-numbered
// schema lets a future schema bump be caught at the version check
// rather than as a hard-to-debug field-level decode failure.
func TestResumeCheckpointRejectsVersionMismatch(t *testing.T) {
	t.Parallel()
	body := []byte(`{
        "version": 99,
        "pgsafe_version": "v999.0.0",
        "backup_id": "X",
        "backup_type": "full",
        "compression": "zstd:3",
        "encryption_recipients": [],
        "system_identifier": 1,
        "timeline": 1,
        "start_lsn": 0,
        "start_time": "2026-04-30T12:00:00Z",
        "checkpointed_at": "2026-04-30T12:00:00Z",
        "files": []
    }`)
	_, err := manifest.UnmarshalResumeCheckpoint(body)
	if err == nil {
		t.Fatal("expected version-mismatch error")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("error %q should mention version", err)
	}
}

// TestAccumulatedFilesIsDefensiveCopy: the checkpoint writer expects
// to be able to snapshot mb's accumulated state without worrying that
// concurrent AddFile calls between snapshot and serialize will splice
// in surprise entries. AccumulatedFiles MUST return a copy, not a
// shared slice — guarded here.
func TestAccumulatedFilesIsDefensiveCopy(t *testing.T) {
	t.Parallel()
	mb := manifest.NewBuilder(manifest.BackupStartInfo{
		SystemIdentifier: 1, Timeline: 1, StartLSN: 0,
	})
	mb.AddFile("a", 1, [32]byte{1}, time.Now())
	snap := mb.AccumulatedFiles()
	mb.AddFile("b", 2, [32]byte{2}, time.Now())
	if len(snap) != 1 {
		t.Errorf("snap mutated by post-snapshot AddFile: len=%d, want 1", len(snap))
	}
}
