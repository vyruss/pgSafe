package backup_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/backup"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/pg"
	"github.com/vyruss/pgsafe/internal/pg/basebackup"
	"github.com/vyruss/pgsafe/internal/pg/identity"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

// callRecorder records the order of mock-method invocations so the
// caller-order test can assert Invariant #1 step ordering.
type callRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *callRecorder) record(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, s)
}

func (r *callRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// fakeStream serves a fixed list of in-memory files as if they came from
// pg_basebackup's tar stream.
type fakeStream struct {
	rec      *callRecorder
	headers  []*tar.Header
	bodies   [][]byte
	idx      int
	closed   bool
	closeErr error
}

func (s *fakeStream) Next() (*tar.Header, io.Reader, error) {
	if s.idx >= len(s.headers) {
		return nil, nil, io.EOF
	}
	h := s.headers[s.idx]
	body := s.bodies[s.idx]
	s.idx++
	s.rec.record("Stream.Next:" + h.Name)
	return h, bytes.NewReader(body), nil
}

func (s *fakeStream) Close() error {
	s.closed = true
	s.rec.record("Stream.Close")
	return s.closeErr
}

// fakeCluster implements pg.Cluster against in-memory state.
type fakeCluster struct {
	rec    *callRecorder
	id     identity.Identity
	stream *fakeStream
}

func (c *fakeCluster) Identity(_ context.Context) (identity.Identity, error) {
	c.rec.record("Cluster.Identity")
	return c.id, nil
}

func (c *fakeCluster) BaseBackup(_ context.Context, _ basebackup.Options) (pg.BaseBackupStream, error) {
	c.rec.record("Cluster.BaseBackup")
	return c.stream, nil
}

func (c *fakeCluster) Close() { c.rec.record("Cluster.Close") }

// fixtureLabel builds a valid backup_label content for the given LSN.
// Timeline is hardcoded to 1 in the format string; we ignore the parameter
// (left for callers that need to vary it once we have a multi-timeline test).
//
//nolint:unparam // current callers all pass the same startLSN; signature anticipates per-test variants
func fixtureLabel(lsn manifest.LSN, _ uint32) string {
	return "START WAL LOCATION: " + lsn.String() +
		" (file 000000010000000000000003)\n" +
		"BACKUP METHOD: streamed\nBACKUP FROM: primary\nLABEL: pgsafe\n" +
		"START TIMELINE: 1\n"
}

// TestCallerStrictStepOrdering is the load-bearing Invariant #1 test
// per
//
// Mocks for Cluster + tar Stream + StopLSNFunc record their invocations into
// a shared call recorder. The test asserts the resulting sequence matches
// the documented step order: identity → BaseBackup → file streams →
// Stream.Close → StopLSN → (no further) → manifest written → Commit.
func TestCallerStrictStepOrdering(t *testing.T) {
	t.Parallel()

	rec := &callRecorder{}

	const segSize = 16 * 1024 * 1024
	startLSN := manifest.LSN(0x3000028)
	stopLSN := manifest.LSN(0x3000200)
	timeline := uint32(1)

	tarFiles := []*tar.Header{
		{Name: "backup_label", Size: 0, Typeflag: tar.TypeReg, ModTime: time.Now()},
		{Name: "PG_VERSION", Size: 3, Typeflag: tar.TypeReg, ModTime: time.Now()},
		{Name: "global/pg_control", Size: 8, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
	tarBodies := [][]byte{
		[]byte(fixtureLabel(startLSN, timeline)),
		[]byte("18\n"),
		[]byte("controlb"),
	}
	tarFiles[0].Size = int64(len(tarBodies[0]))

	stream := &fakeStream{rec: rec, headers: tarFiles, bodies: tarBodies}
	cluster := &fakeCluster{
		rec: rec,
		id: identity.Identity{
			SystemIdentifier: 12345,
			Timeline:         timeline,
			CheckpointLSN:    startLSN,
			WALSegmentSize:   segSize,
		},
		stream: stream,
	}

	// POSIX storage backed by a tempdir.
	root := filepath.Join(t.TempDir(), "storage")
	storage, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := storage.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Pre-populate the segment that WAL-wait expects in the storage at
	// archive.SegmentKey(timeline, name, srcSha) so the test doesn't
	// actually wait. Backend-relative path: wal/<TLI>/<seg>-<srcSha>.
	expectedSeg := backup.WALSegmentName(timeline, stopLSN, segSize)
	walBody := []byte("walbytes")
	walHash := sha256.Sum256(walBody)
	walPath := filepath.Join(root, archive.SegmentKey(timeline, expectedSeg, walHash))
	if err := os.MkdirAll(filepath.Dir(walPath), 0o750); err != nil {
		t.Fatalf("seed WAL mkdir: %v", err)
	}
	if err := os.WriteFile(walPath, walBody, 0o600); err != nil {
		t.Fatalf("seed WAL: %v", err)
	}

	// Filter chain.
	id, _ := age.GenerateX25519Identity()
	chain, err := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})
	if err != nil {
		t.Fatalf("filter.NewChain: %v", err)
	}

	res, err := backup.Run(context.Background(), backup.Options{
		Cluster: cluster,
		Backend: storage,
		Filter:  chain,
		Mode:    backup.ModeSimple,
		Server:  "demo",
		Label:   "pgsafe-test",
		StopLSN: func(_ context.Context) (manifest.LSN, error) {
			rec.record("StopLSN")
			return stopLSN, nil
		},
		Now: func() time.Time {
			return time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Files != 3 {
		t.Errorf("Files = %d, want 3", res.Files)
	}
	if res.StartLSN != startLSN {
		t.Errorf("StartLSN = %s, want %s", res.StartLSN, startLSN)
	}
	if res.StopLSN != stopLSN {
		t.Errorf("StopLSN = %s, want %s", res.StopLSN, stopLSN)
	}

	// Step-order assertions: the per-Invariant-#1 sequence.
	got := rec.snapshot()

	// 1. Identity is the very first PG-side call.
	mustBeFirst(t, got, "Cluster.Identity")

	// 2. BaseBackup happens after Identity, before any Stream.Next.
	mustBeBefore(t, got, "Cluster.BaseBackup", "Stream.Next:backup_label")

	// 3. Stream.Close happens after the last Stream.Next.
	lastNext := lastIndexPrefix(got, "Stream.Next:")
	closeIdx := slices.Index(got, "Stream.Close")
	if closeIdx < lastNext {
		t.Errorf("Stream.Close (%d) appeared before last Stream.Next (%d)", closeIdx, lastNext)
	}

	// 4. StopLSN is called AFTER Stream.Close (i.e., after every file is durable).
	mustBeBefore(t, got, "Stream.Close", "StopLSN")
	mustBeBefore(t, got, "StopLSN", "" /* sentinel: no more PG calls */)

	// Final storage state: backup_manifest exists at <root>/<backup-id>/backup_manifest.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var backupDir string
	for _, e := range entries {
		if e.IsDir() && strings.HasSuffix(e.Name(), "F") && e.Name() != "wal" {
			backupDir = e.Name()
		}
	}
	if backupDir == "" {
		t.Fatalf("no backup directory created in %s", root)
	}
	if _, err := os.Stat(filepath.Join(root, backupDir, "backup_manifest")); err != nil {
		t.Errorf("backup_manifest missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, backupDir, "backup_manifest.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("backup_manifest.tmp leaked after Commit; should be renamed")
	}
	if _, err := os.Stat(filepath.Join(root, backupDir, "Storage-Metadata.json")); err != nil {
		t.Errorf("Storage-Metadata.json missing: %v", err)
	}
}

// TestMultiStorageBackup runs a simple-mode backup against two POSIX backends
// and asserts that both receive the manifest (Invariant #10: durable on all
// alive backends). Result.PartialStorages == 0 means both succeeded.
func TestMultiStorageBackup(t *testing.T) {
	t.Parallel()

	const segSize = 16 * 1024 * 1024
	startLSN := manifest.LSN(0x3000028)
	stopLSN := manifest.LSN(0x3000200)
	timeline := uint32(1)

	tarFiles := []*tar.Header{
		{Name: "backup_label", Typeflag: tar.TypeReg, ModTime: time.Now()},
		{Name: "PG_VERSION", Size: 3, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
	label := fixtureLabel(startLSN, timeline)
	tarFiles[0].Size = int64(len(label))
	tarBodies := [][]byte{[]byte(label), []byte("18\n")}
	stream := &fakeStream{rec: &callRecorder{}, headers: tarFiles, bodies: tarBodies}
	cluster := &fakeCluster{
		rec: &callRecorder{},
		id: identity.Identity{
			SystemIdentifier: 99999,
			Timeline:         timeline,
			CheckpointLSN:    startLSN,
			WALSegmentSize:   segSize,
		},
		stream: stream,
	}

	// Two independent POSIX storages in separate tmpdirs.
	repo0Root := filepath.Join(t.TempDir(), "repo0")
	repo1Root := filepath.Join(t.TempDir(), "repo1")
	openStorage := func(root string) *posix.Backend {
		b, err := posix.New(posix.Options{Root: root})
		if err != nil {
			t.Fatalf("posix.New(%s): %v", root, err)
		}
		if err := b.Open(context.Background()); err != nil {
			t.Fatalf("Open(%s): %v", root, err)
		}
		// Seed WAL segment at archive.SegmentKey location so WAL-wait
		// + hashWALSegments find it.
		seg := backup.WALSegmentName(timeline, stopLSN, segSize)
		body := []byte("wal")
		walHash := sha256.Sum256(body)
		walPath := filepath.Join(root, archive.SegmentKey(timeline, seg, walHash))
		if err := os.MkdirAll(filepath.Dir(walPath), 0o750); err != nil {
			t.Fatalf("seed WAL mkdir: %v", err)
		}
		if err := os.WriteFile(walPath, body, 0o600); err != nil {
			t.Fatalf("seed WAL: %v", err)
		}
		return b
	}
	b0 := openStorage(repo0Root)
	b1 := openStorage(repo1Root)

	id, _ := age.GenerateX25519Identity()
	chain, err := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})
	if err != nil {
		t.Fatalf("filter.NewChain: %v", err)
	}

	res, err := backup.Run(context.Background(), backup.Options{
		Cluster:  cluster,
		Backend:  b0,
		Backends: []storage.Backend{b0, b1},
		Filter:   chain,
		Mode:     backup.ModeSimple,
		Server:   "multi-test",
		Label:    "pgsafe-multi",
		StopLSN:  func(_ context.Context) (manifest.LSN, error) { return stopLSN, nil },
		Now:      func() time.Time { return time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("Run (multi): %v", err)
	}
	if res.PartialStorages != 0 {
		t.Errorf("PartialStorages = %d, want 0 (both backends should commit)", res.PartialStorages)
	}

	// Both storages must have a final backup_manifest (not just .tmp).
	for _, root := range []string{repo0Root, repo1Root} {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", root, err)
		}
		var found bool
		for _, e := range entries {
			if e.IsDir() && strings.HasSuffix(e.Name(), "F") {
				mf := filepath.Join(root, e.Name(), "backup_manifest")
				if _, err := os.Stat(mf); err != nil {
					t.Errorf("backup_manifest missing in %s: %v", root, err)
				}
				found = true
			}
		}
		if !found {
			t.Errorf("no backup dir found in %s", root)
		}
	}
}

func mustBeFirst(t *testing.T, calls []string, want string) {
	t.Helper()
	for _, c := range calls {
		if strings.HasPrefix(c, "Stream.") || c == "StopLSN" {
			break
		}
		if c == want {
			return
		}
	}
	t.Errorf("expected %q to be the first non-stream call; got %v", want, calls)
}

func mustBeBefore(t *testing.T, calls []string, before, after string) {
	t.Helper()
	bIdx := slices.Index(calls, before)
	if bIdx < 0 {
		t.Errorf("call %q not present in trace %v", before, calls)
		return
	}
	if after == "" {
		return // sentinel: no further check
	}
	aIdx := slices.Index(calls, after)
	if aIdx < 0 {
		t.Errorf("call %q not present in trace %v", after, calls)
		return
	}
	if bIdx >= aIdx {
		t.Errorf("expected %q before %q; got at indices %d and %d in %v", before, after, bIdx, aIdx, calls)
	}
}

// TestRunResumeSkipsValidatedReuploads — RESUME.md Step 4 contract:
// when the resumable manifest's RepoSHA matches the on-storage file,
// the new attempt MUST NOT overwrite it. Verified by pre-staging a
// sentinel byte in the file's storage location and a matching .copy,
// then asserting the sentinel survives the backup run. If reuse
// regresses to "always re-upload," the sentinel is overwritten by
// the filter chain output and the test catches it.
func TestRunResumeSkipsValidatedReuploads(t *testing.T) {
	t.Parallel()

	const segSize = 16 * 1024 * 1024
	const prestagedID = "20260101T000000F"
	startLSN := manifest.LSN(0x3000028)
	stopLSN := manifest.LSN(0x3000200)
	timeline := uint32(1)
	const sysID = 12345

	tarFiles := []*tar.Header{
		{Name: "backup_label", Size: 0, Typeflag: tar.TypeReg, ModTime: time.Now()},
		{Name: "PG_VERSION", Size: 3, Typeflag: tar.TypeReg, ModTime: time.Now()},
		{Name: "global/pg_control", Size: 8, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
	tarBodies := [][]byte{
		[]byte(fixtureLabel(startLSN, timeline)),
		[]byte("18\n"),
		[]byte("controlb"),
	}
	tarFiles[0].Size = int64(len(tarBodies[0]))

	rec := &callRecorder{}
	stream := &fakeStream{rec: rec, headers: tarFiles, bodies: tarBodies}
	cluster := &fakeCluster{
		rec: rec,
		id: identity.Identity{
			SystemIdentifier: sysID,
			Timeline:         timeline,
			CheckpointLSN:    startLSN,
			WALSegmentSize:   segSize,
		},
		stream: stream,
	}

	root := filepath.Join(t.TempDir(), "storage")
	st, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := st.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Pre-stage the prior attempt's PG_VERSION as a sentinel byte
	// stream + the matching .copy entry. backup_label is denylisted
	// from reuse so we don't pre-stage it; pg_control is left to
	// the standard path (no reuse data → re-upload).
	prestagedDir := filepath.Join(root, prestagedID)
	if err := os.MkdirAll(prestagedDir, 0o750); err != nil {
		t.Fatalf("mkdir prestaged: %v", err)
	}
	sentinel := []byte("RESUME_SENTINEL_PG_VERSION")
	sentinelSHA := sha256.Sum256(sentinel)
	if err := os.WriteFile(filepath.Join(prestagedDir, "PG_VERSION"), sentinel, 0o600); err != nil {
		t.Fatalf("stage sentinel PG_VERSION: %v", err)
	}

	cp := manifest.ResumeCheckpoint{
		Version:              manifest.ResumeCheckpointVersion,
		PgsafeVersion:        "v0.0.0-test",
		BackupID:             prestagedID,
		BackupType:           string(backup.TypeFull),
		Compression:          "gzip:0",
		EncryptionRecipients: []string{},
		SystemIdentifier:     sysID,
		Timeline:             timeline,
		StartLSN:             startLSN,
		StartTime:            time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		CheckpointedAt:       time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC),
		Files: []manifest.ResumeFileEntry{
			{
				Path:       "PG_VERSION",
				Size:       int64(len("18\n")),
				SHA256:     sha256.Sum256([]byte("18\n")),
				ModTime:    time.Now().UTC(),
				RepoSize:   int64(len(sentinel)),
				RepoSHA256: sentinelSHA,
			},
		},
	}
	cpBody, err := manifest.MarshalResumeCheckpoint(cp)
	if err != nil {
		t.Fatalf("marshal cp: %v", err)
	}
	if err := os.WriteFile(filepath.Join(prestagedDir, "backup_manifest.copy"), cpBody, 0o600); err != nil {
		t.Fatalf("write .copy: %v", err)
	}
	expectedSeg := backup.WALSegmentName(timeline, stopLSN, segSize)
	walBody := []byte("walbytes")
	walHash := sha256.Sum256(walBody)
	walPath := filepath.Join(root, archive.SegmentKey(timeline, expectedSeg, walHash))
	if err := os.MkdirAll(filepath.Dir(walPath), 0o750); err != nil {
		t.Fatalf("seed WAL mkdir: %v", err)
	}
	if err := os.WriteFile(walPath, walBody, 0o600); err != nil {
		t.Fatalf("seed WAL: %v", err)
	}

	id2, _ := age.GenerateX25519Identity()
	chain, err := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id2.Recipient()},
	})
	if err != nil {
		t.Fatalf("filter.NewChain: %v", err)
	}

	res, err := backup.Run(context.Background(), backup.Options{
		Cluster:       cluster,
		Backend:       st,
		Filter:        chain,
		Mode:          backup.ModeSimple,
		Server:        "demo",
		Label:         "pgsafe-resume-skip-test",
		Compression:   "gzip:0",
		PgsafeVersion: "v0.0.0-test",
		StopLSN: func(_ context.Context) (manifest.LSN, error) {
			return stopLSN, nil
		},
		Now: func() time.Time {
			return time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.BackupID != prestagedID {
		t.Errorf("BackupID = %q; want resume-in-place %q", res.BackupID, prestagedID)
	}
	// The crux: PG_VERSION on storage must STILL be the sentinel,
	// proving the new attempt didn't overwrite it. If reuse-skip
	// regresses, PG_VERSION will be the filter-chain ciphertext of
	// "18\n" — different bytes, sentinel lost.
	got, err := os.ReadFile(filepath.Join(prestagedDir, "PG_VERSION"))
	if err != nil {
		t.Fatalf("read post-backup PG_VERSION: %v", err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Errorf("PG_VERSION was overwritten — reuse-skip didn't fire.\ngot bytes (sha256=%x)\nwant sentinel (sha256=%x)",
			sha256.Sum256(got), sentinelSHA)
	}
}

// TestRunResumesBackupID — RESUME.md Step 3 contract: when a
// compatible backup_manifest.copy already exists in the storage,
// backup.Run resumes in place using the same backup-id rather than
// minting a fresh one. Mirrors pgbackrest's "same-label resume"
// behavior; without this, the .copy would be orphaned (next run
// would call ChooseBackupID and start a new id, leaving the prior
// .copy on disk forever).
func TestRunResumesBackupID(t *testing.T) {
	t.Parallel()

	const segSize = 16 * 1024 * 1024
	const prestagedID = "20260101T000000F"
	startLSN := manifest.LSN(0x3000028)
	stopLSN := manifest.LSN(0x3000200)
	timeline := uint32(1)
	const sysID = 12345

	tarFiles := []*tar.Header{
		{Name: "backup_label", Size: 0, Typeflag: tar.TypeReg, ModTime: time.Now()},
		{Name: "PG_VERSION", Size: 3, Typeflag: tar.TypeReg, ModTime: time.Now()},
		{Name: "global/pg_control", Size: 8, Typeflag: tar.TypeReg, ModTime: time.Now()},
	}
	tarBodies := [][]byte{
		[]byte(fixtureLabel(startLSN, timeline)),
		[]byte("18\n"),
		[]byte("controlb"),
	}
	tarFiles[0].Size = int64(len(tarBodies[0]))

	rec := &callRecorder{}
	stream := &fakeStream{rec: rec, headers: tarFiles, bodies: tarBodies}
	cluster := &fakeCluster{
		rec: rec,
		id: identity.Identity{
			SystemIdentifier: sysID,
			Timeline:         timeline,
			CheckpointLSN:    startLSN,
			WALSegmentSize:   segSize,
		},
		stream: stream,
	}

	root := filepath.Join(t.TempDir(), "storage")
	st, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := st.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Pre-stage the .copy + bracket-spanning WAL segment so the run
	// has something to resume into AND can complete (WAL-wait must
	// see the segment).
	cp := manifest.ResumeCheckpoint{
		Version:              manifest.ResumeCheckpointVersion,
		PgsafeVersion:        "v0.0.0-test",
		BackupID:             prestagedID,
		BackupType:           string(backup.TypeFull),
		Compression:          "gzip:0",
		EncryptionRecipients: []string{},
		// SystemIdentifier matches the cluster identity below.
		SystemIdentifier: sysID,
		Timeline:         timeline,
		StartLSN:         startLSN,
		StartTime:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		CheckpointedAt:   time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC),
	}
	cpBody, err := manifest.MarshalResumeCheckpoint(cp)
	if err != nil {
		t.Fatalf("marshal cp: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, prestagedID), 0o750); err != nil {
		t.Fatalf("mkdir prestaged dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, prestagedID, "backup_manifest.copy"), cpBody, 0o600); err != nil {
		t.Fatalf("stage .copy: %v", err)
	}
	expectedSeg := backup.WALSegmentName(timeline, stopLSN, segSize)
	walBody := []byte("walbytes")
	walHash := sha256.Sum256(walBody)
	walPath := filepath.Join(root, archive.SegmentKey(timeline, expectedSeg, walHash))
	if err := os.MkdirAll(filepath.Dir(walPath), 0o750); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	if err := os.WriteFile(walPath, walBody, 0o600); err != nil {
		t.Fatalf("seed WAL: %v", err)
	}

	id2, _ := age.GenerateX25519Identity()
	chain, err := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id2.Recipient()},
	})
	if err != nil {
		t.Fatalf("filter.NewChain: %v", err)
	}

	res, err := backup.Run(context.Background(), backup.Options{
		Cluster:       cluster,
		Backend:       st,
		Filter:        chain,
		Mode:          backup.ModeSimple,
		Server:        "demo",
		Label:         "pgsafe-resume-test",
		Compression:   "gzip:0",
		PgsafeVersion: "v0.0.0-test",
		StopLSN: func(_ context.Context) (manifest.LSN, error) {
			return stopLSN, nil
		},
		Now: func() time.Time {
			return time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.BackupID != prestagedID {
		t.Errorf("BackupID = %q; want resume-in-place %q", res.BackupID, prestagedID)
	}
}

func lastIndexPrefix(calls []string, prefix string) int {
	last := -1
	for i, c := range calls {
		if strings.HasPrefix(c, prefix) {
			last = i
		}
	}
	return last
}
