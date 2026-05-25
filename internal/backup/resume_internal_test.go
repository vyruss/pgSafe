package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

// TestResumeCheckpointerWritesAtCadence pins the load-bearing
// behavior of Step 1: every Nth call to onAddFile produces a fresh
// backup_manifest.copy on the storage, and the file decodes to a
// well-formed ResumeCheckpoint with the expected file count.
//
// If this regresses, resume's silent-no-op failure mode bites: the
// next backup's discovery step finds no .copy and starts fresh, but
// the operator believes resume is on. Tested directly against the
// real posix backend (not a mock) so atomic-rename + the
// Delete-prior pattern are exercised end-to-end.
func TestResumeCheckpointerWritesAtCadence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b, err := posix.New(posix.Options{Root: dir})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	mb := manifest.NewBuilder(manifest.BackupStartInfo{
		SystemIdentifier: 1234,
		Timeline:         1,
		StartLSN:         manifest.LSN(0x3000028),
		StartTime:        time.Now().UTC(),
	})
	rc := newResumeCheckpointer(
		Options{
			Type:                   TypeFull,
			Compression:            "zstd:3",
			Recipients:             []string{"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"},
			ResumeCheckpointEveryN: 3,
			PgsafeVersion:          "v0.0.0-test",
		},
		b, "20260430T120000F",
		time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
		1234, 1, manifest.LSN(0x3000028),
	)
	if rc == nil {
		t.Fatal("checkpointer constructor returned nil with Resume enabled")
	}

	for i := 0; i < 7; i++ {
		mb.AddFile("f"+string(rune('a'+i)), int64(i+1), [32]byte{byte(i)}, time.Now())
		rc.onAddFile(context.Background(), mb)
	}

	cpPath := filepath.Join(dir, "20260430T120000F", "backup_manifest.copy")
	rcb, err := os.ReadFile(cpPath) //nolint:gosec // path under t.TempDir()
	if err != nil {
		t.Fatalf("read .copy: %v", err)
	}
	cp, err := manifest.UnmarshalResumeCheckpoint(rcb)
	if err != nil {
		t.Fatalf("decode .copy: %v", err)
	}
	// 7 AddFiles with cadence 3 → flushes at 3, 6 → most recent
	// flush captured 6 files. Asserts onAddFile actually triggers
	// the flush at the right cadence (NOT every-AddFile, NOT
	// once-at-end).
	if len(cp.Files) != 6 {
		t.Errorf("Files in latest checkpoint = %d, want 6", len(cp.Files))
	}
	if cp.BackupType != string(TypeFull) {
		t.Errorf("BackupType = %q, want %q", cp.BackupType, TypeFull)
	}
	if cp.SystemIdentifier != 1234 {
		t.Errorf("SystemIdentifier = %d, want 1234", cp.SystemIdentifier)
	}
}

// TestResumeCheckpointerDisabledWhenOptOut: ResumeDisabled=true MUST
// produce a nil checkpointer (zero overhead, no .copy on disk).
// onAddFile on nil is a documented no-op.
func TestResumeCheckpointerDisabledWhenOptOut(t *testing.T) {
	t.Parallel()
	rc := newResumeCheckpointer(
		Options{ResumeDisabled: true},
		nil, "id", time.Now(), 0, 0, 0,
	)
	if rc != nil {
		t.Fatal("ResumeDisabled=true should yield nil checkpointer")
	}
	rc.onAddFile(context.Background(), nil) // must not panic
}

// TestResumeCheckpointerOverwrites: subsequent flushes overwrite the
// previous .copy via Delete-then-Commit. If we forget the Delete,
// the second flush hits posix.Backend.Commit's "refuses to
// overwrite" guard and the checkpointer self-disables — silently
// breaking resume. Pinned here.
func TestResumeCheckpointerOverwrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b, err := posix.New(posix.Options{Root: dir})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	mb := manifest.NewBuilder(manifest.BackupStartInfo{SystemIdentifier: 1, Timeline: 1})
	rc := newResumeCheckpointer(
		Options{Type: TypeFull, ResumeCheckpointEveryN: 1},
		b, "X", time.Now(), 1, 1, 0,
	)
	for i := 0; i < 3; i++ {
		mb.AddFile("f", 1, [32]byte{byte(i)}, time.Now())
		rc.onAddFile(context.Background(), mb)
	}
	if rc.disabled {
		t.Fatal("checkpointer self-disabled — second flush probably hit overwrite-refuses error")
	}
}

// stageFakeBackup creates a fake backup directory at <root>/<id> with
// the given files. If withFinal=true, also writes backup_manifest;
// if cp != nil, writes backup_manifest.copy from it. Used by the
// findResumable tests to lay out repo states without running a real
// backup.
func stageFakeBackup(t *testing.T, root, id string, withFinal bool, cp *manifest.ResumeCheckpoint) {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if withFinal {
		if err := os.WriteFile(filepath.Join(dir, "backup_manifest"), []byte("{}"), 0o600); err != nil {
			t.Fatalf("write backup_manifest: %v", err)
		}
	}
	if cp != nil {
		body, err := manifest.MarshalResumeCheckpoint(*cp)
		if err != nil {
			t.Fatalf("marshal checkpoint: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "backup_manifest.copy"), body, 0o600); err != nil {
			t.Fatalf("write checkpoint: %v", err)
		}
	}
}

func openPosixForResumeTest(t *testing.T) (string, *posix.Backend) {
	t.Helper()
	dir := t.TempDir()
	b, err := posix.New(posix.Options{Root: dir})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return dir, b
}

func mkCheckpoint() manifest.ResumeCheckpoint {
	return manifest.ResumeCheckpoint{
		Version:              manifest.ResumeCheckpointVersion,
		PgsafeVersion:        "v0.5.0",
		BackupID:             "20260430T120000F",
		BackupType:           "full",
		Compression:          "zstd:3",
		EncryptionRecipients: []string{"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"},
		SystemIdentifier:     1234,
		Timeline:             1,
		StartLSN:             manifest.LSN(0x3000028),
		StartTime:            time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
		CheckpointedAt:       time.Date(2026, 4, 30, 12, 0, 5, 0, time.UTC),
	}
}

func mkGate(cp manifest.ResumeCheckpoint) resumeGate {
	return resumeGate{
		PgsafeVersion:        cp.PgsafeVersion,
		BackupType:           cp.BackupType,
		ParentBackupID:       cp.ParentBackupID,
		Compression:          cp.Compression,
		EncryptionRecipients: cp.EncryptionRecipients,
		SystemIdentifier:     cp.SystemIdentifier,
	}
}

// TestFindResumableEmptyStorage: no backups → nothing to resume,
// nothing to error about. The "fresh storage" path must be silent.
func TestFindResumableEmptyStorage(t *testing.T) {
	t.Parallel()
	_, b := openPosixForResumeTest(t)
	cp, found, err := findResumable(context.Background(), b, resumeGate{}, 0, time.Time{}, nil)
	if err != nil || found || cp != nil {
		t.Fatalf("empty storage: got (%v, %v, %v); want (nil, false, nil)", cp, found, err)
	}
}

// TestFindResumableSkipsCompleted: a backup with backup_manifest is
// completed → not resumable. Mirrors pgbackrest's "no .copy without a
// .manifest counterpart" rule.
func TestFindResumableSkipsCompleted(t *testing.T) {
	t.Parallel()
	dir, b := openPosixForResumeTest(t)
	cp := mkCheckpoint()
	stageFakeBackup(t, dir, "20260430T120000F", true, &cp) // both .manifest and .copy
	got, found, err := findResumable(context.Background(), b, mkGate(cp), 0, time.Time{}, nil)
	if err != nil {
		t.Fatalf("findResumable: %v", err)
	}
	if found {
		t.Errorf("completed backup must not be resumable; got %+v", got)
	}
}

// TestFindResumableHappyPath: a single .copy-only backup matching the
// gate is the resume candidate.
func TestFindResumableHappyPath(t *testing.T) {
	t.Parallel()
	dir, b := openPosixForResumeTest(t)
	cp := mkCheckpoint()
	stageFakeBackup(t, dir, cp.BackupID, false, &cp)
	got, found, err := findResumable(context.Background(), b, mkGate(cp), 0, time.Time{}, nil)
	if err != nil || !found {
		t.Fatalf("findResumable: (%v, %v, %v); want a match", got, found, err)
	}
	if got.BackupID != cp.BackupID {
		t.Errorf("BackupID = %q; want %q", got.BackupID, cp.BackupID)
	}
}

// TestFindResumableValidationGate: each gate field must independently
// invalidate a candidate when the new run differs. Catches a typo in
// the validation logic that would silently widen the gate.
func TestFindResumableValidationGate(t *testing.T) {
	t.Parallel()
	cp := mkCheckpoint()
	cases := []struct {
		name   string
		mutate func(g *resumeGate)
	}{
		{"PgsafeVersion", func(g *resumeGate) { g.PgsafeVersion = "v999.9.9" }},
		{"BackupType", func(g *resumeGate) { g.BackupType = "incremental" }},
		{"Compression", func(g *resumeGate) { g.Compression = "gzip:6" }},
		{"SystemIdentifier", func(g *resumeGate) { g.SystemIdentifier = 999 }},
		{"EncryptionRecipients", func(g *resumeGate) { g.EncryptionRecipients = []string{"age1somethingelse"} }},
		{"ParentBackupID", func(g *resumeGate) { g.ParentBackupID = "not-empty" }},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			dir, b := openPosixForResumeTest(t)
			stageFakeBackup(t, dir, cp.BackupID, false, &cp)
			gate := mkGate(cp)
			c.mutate(&gate)
			_, found, err := findResumable(context.Background(), b, gate, 0, time.Time{}, nil)
			if err != nil {
				t.Fatalf("findResumable: %v", err)
			}
			if found {
				t.Errorf("mismatched %s should NOT match", c.name)
			}
		})
	}
}

// TestFindResumablePicksMostRecent: when multiple candidates are
// resumable, the latest backup-id wins. Discovery order must be
// lexicographic descending.
func TestFindResumablePicksMostRecent(t *testing.T) {
	t.Parallel()
	dir, b := openPosixForResumeTest(t)
	older := mkCheckpoint()
	older.BackupID = "20260101T000000F"
	newer := mkCheckpoint()
	newer.BackupID = "20260430T120000F"
	stageFakeBackup(t, dir, older.BackupID, false, &older)
	stageFakeBackup(t, dir, newer.BackupID, false, &newer)
	got, found, err := findResumable(context.Background(), b, mkGate(newer), 0, time.Time{}, nil)
	if err != nil || !found {
		t.Fatalf("findResumable: (%v, %v, %v)", got, found, err)
	}
	if got.BackupID != newer.BackupID {
		t.Errorf("BackupID = %q; want %q (most recent)", got.BackupID, newer.BackupID)
	}
}

// TestFindResumableSkipsCorruptCheckpoint: a corrupt .copy isn't
// fatal — discovery just moves on. The previous attempt's bytes
// might be torn (writer killed mid-write); resume must tolerate it.
func TestFindResumableSkipsCorruptCheckpoint(t *testing.T) {
	t.Parallel()
	dir, b := openPosixForResumeTest(t)
	if err := os.MkdirAll(filepath.Join(dir, "20260430T120000F"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "20260430T120000F", "backup_manifest.copy"),
		[]byte("not json"), 0o600); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	_, found, err := findResumable(context.Background(), b, resumeGate{PgsafeVersion: "v0.5.0"}, 0, time.Time{}, nil)
	if err != nil {
		t.Fatalf("findResumable: %v (corrupt .copy must be skipped, not error)", err)
	}
	if found {
		t.Errorf("corrupt .copy should not match")
	}
}

// TestFindResumableReapsAbandonedCandidates pins the auto-prune-on-
// resume contract: a .copy older than the grace period is reaped
// (the entire backup-id directory deleted) at backup-start, not
// silently kept. Avoids the need for a separate `pgsafe prune
// --resume-orphans` subcommand — the discovery walk already has
// everything it needs.
func TestFindResumableReapsAbandonedCandidates(t *testing.T) {
	t.Parallel()
	dir, b := openPosixForResumeTest(t)

	// Stage a candidate whose CheckpointedAt is 48 hours old.
	old := mkCheckpoint()
	old.BackupID = "20260101T000000F"
	old.CheckpointedAt = time.Now().Add(-48 * time.Hour)
	stageFakeBackup(t, dir, old.BackupID, false, &old)

	// Also stage some leaf files inside it so we can verify the
	// reap deletes everything under the backup-id, not just the
	// .copy file.
	if err := os.WriteFile(filepath.Join(dir, old.BackupID, "PG_VERSION"),
		[]byte("18\n"), 0o600); err != nil {
		t.Fatalf("stage PG_VERSION: %v", err)
	}

	now := time.Now()
	_, found, err := findResumable(context.Background(), b,
		mkGate(old), 24*time.Hour, now, nil)
	if err != nil {
		t.Fatalf("findResumable: %v", err)
	}
	if found {
		t.Errorf("a 48h-old .copy should NOT be returned as resumable when grace=24h")
	}
	// Reap deletes every file under the prefix; the directory itself
	// may remain empty (Backend has no RemoveDir primitive). What
	// matters is that the .copy and leaf files are gone — the next
	// findResumable won't see it as a candidate.
	for _, leaf := range []string{"backup_manifest.copy", "PG_VERSION"} {
		p := filepath.Join(dir, old.BackupID, leaf)
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("reap: %s should be deleted; got stat err=%v", leaf, err)
		}
	}
}

// TestFindResumableKeepsRecentCandidates: a .copy within the grace
// period must NOT be reaped — it's the resume target.
func TestFindResumableKeepsRecentCandidates(t *testing.T) {
	t.Parallel()
	dir, b := openPosixForResumeTest(t)
	cp := mkCheckpoint()
	cp.CheckpointedAt = time.Now().Add(-1 * time.Hour)
	stageFakeBackup(t, dir, cp.BackupID, false, &cp)

	got, found, err := findResumable(context.Background(), b,
		mkGate(cp), 24*time.Hour, time.Now(), nil)
	if err != nil || !found {
		t.Fatalf("recent .copy should match; got (%v, %v, %v)", got, found, err)
	}
	if _, err := os.Stat(filepath.Join(dir, cp.BackupID)); err != nil {
		t.Errorf("recent backup-id dir should still exist: %v", err)
	}
}

// TestFindResumableGraceZeroDisablesReaping: grace=0 disables
// auto-pruning entirely. Stale .copys stay on disk; operator opt-out
// via backup.resume_grace_period: 0 in YAML.
func TestFindResumableGraceZeroDisablesReaping(t *testing.T) {
	t.Parallel()
	dir, b := openPosixForResumeTest(t)
	cp := mkCheckpoint()
	cp.CheckpointedAt = time.Now().Add(-30 * 24 * time.Hour)
	stageFakeBackup(t, dir, cp.BackupID, false, &cp)

	_, _, err := findResumable(context.Background(), b,
		mkGate(cp), 0, time.Now(), nil)
	if err != nil {
		t.Fatalf("findResumable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, cp.BackupID)); err != nil {
		t.Errorf("grace=0: dir should NOT have been reaped; got %v", err)
	}
}

// TestLooksLikeBackupID pins the backup-id shape filter that keeps
// findResumable from trying to decode wal/, locks, and the
// server-root sidecar. False positives waste a Stat per directory;
// false negatives drop legitimate resume candidates.
func TestLooksLikeBackupID(t *testing.T) {
	t.Parallel()
	cases := []struct {
		s    string
		want bool
	}{
		{"20260430T120000F", true},
		{"20260430T120000I", true},
		{"20260101T000000F_20260102T030405I", true},
		{"wal", false},
		{".pgsafe-server-demo.lock", false},
		{"Storage-Metadata.json", false},
		{"20260430T12000F", false},  // 5-digit time
		{"20260430X120000F", false}, // wrong separator
		{"20260430T120000Z", false}, // wrong type suffix
		{"", false},
	}
	for _, c := range cases {
		got := looksLikeBackupID(c.s)
		if got != c.want {
			t.Errorf("looksLikeBackupID(%q) = %v; want %v", c.s, got, c.want)
		}
	}
}
