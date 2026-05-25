package backup

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// resumeCheckpointFilename is the storage-relative basename of the
// in-progress manifest. Sits next to the final backup_manifest;
// presence-of-copy + absence-of-final is the marker discovery uses
// to identify a resumable backup. Mirrors pgbackrest's
// backup.manifest.copy filename pattern (different shape inside).
const resumeCheckpointFilename = "backup_manifest.copy"

// DefaultResumeCheckpointEveryN is the default checkpoint cadence: a
// backup writes backup_manifest.copy after every N files added. Lower
// gives tighter resume granularity at the cost of more storage Puts;
// higher amortises Put cost. K=10 is the RESUME.md plan's default.
const DefaultResumeCheckpointEveryN = 10

// resumeCheckpointer batches per-file events into periodic
// backup_manifest.copy writes. Construction is cheap; the per-file
// hot path is one integer mod-and-compare. When a flush fails the
// checkpointer self-disables — a flaky storage shouldn't fail an
// otherwise-successful backup, and the next pgsafe invocation
// against this backup-id will simply not see a resumable
// checkpoint and start fresh.
type resumeCheckpointer struct {
	backend  storage.Backend
	backupID string
	stderr   io.Writer
	every    int
	now      func() time.Time

	staticMeta manifest.ResumeCheckpoint // header fields that don't change across checkpoints

	counter  int
	disabled bool
}

// newResumeCheckpointer wires a checkpointer to backend + opts. The
// static envelope (BackupID, BackupType, ParentBackupID, Compression,
// Recipients, SystemIdentifier, Timeline, StartLSN, StartTime) is
// captured once; subsequent flushes only refresh CheckpointedAt and
// Files. Returns nil when resume is disabled (operator opt-out, or
// every <= 0).
func newResumeCheckpointer(opts Options, backend storage.Backend, backupID string, startedAt time.Time, sysID uint64, timeline uint32, startLSN manifest.LSN) *resumeCheckpointer {
	every := opts.ResumeCheckpointEveryN
	if every == 0 {
		every = DefaultResumeCheckpointEveryN
	}
	if opts.ResumeDisabled || every <= 0 || backend == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &resumeCheckpointer{
		backend:  backend,
		backupID: backupID,
		stderr:   stderrFor(opts),
		every:    every,
		now:      now,
		staticMeta: manifest.ResumeCheckpoint{
			Version:              manifest.ResumeCheckpointVersion,
			PgsafeVersion:        opts.PgsafeVersion,
			BackupID:             backupID,
			BackupType:           string(opts.Type),
			ParentBackupID:       opts.ParentBackupID,
			Compression:          opts.Compression,
			EncryptionRecipients: opts.Recipients,
			SystemIdentifier:     sysID,
			Timeline:             timeline,
			StartLSN:             startLSN,
			StartTime:            startedAt.UTC(),
		},
	}
}

// onAddFile is the per-AddFile hook. Increments the counter and,
// every `every`-th call, writes a fresh checkpoint. Errors are
// logged + the checkpointer self-disables; the backup itself does
// not fail.
func (rc *resumeCheckpointer) onAddFile(ctx context.Context, mb *manifest.Builder) {
	if rc == nil || rc.disabled {
		return
	}
	rc.counter++
	if rc.counter%rc.every != 0 {
		return
	}
	if err := rc.flush(ctx, mb); err != nil {
		_, _ = fmt.Fprintf(rc.stderr,
			"pgsafe backup: WARNING: resume checkpoint write failed: %v; resume disabled for this backup.\n", err)
		rc.disabled = true
	}
}

func (rc *resumeCheckpointer) flush(ctx context.Context, mb *manifest.Builder) error {
	cp := rc.staticMeta
	cp.CheckpointedAt = rc.now().UTC()
	cp.Files = mb.AccumulatedFiles()
	body, err := manifest.MarshalResumeCheckpoint(cp)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	final := path.Join(rc.backupID, resumeCheckpointFilename)
	tmp := final + ".staging"
	// Backend.Commit refuses to overwrite an existing final, so the
	// previous checkpoint must be removed before we rename the new
	// one in. Delete is idempotent for missing files (errors that
	// aren't NotExist surface as a Put/Commit failure later).
	if err := rc.backend.Delete(ctx, final); err != nil && !isNotExistErr(err) {
		return fmt.Errorf("backend.Delete prior checkpoint: %w", err)
	}
	wc, err := rc.backend.Put(ctx, tmp)
	if err != nil {
		return fmt.Errorf("backend.Put: %w", err)
	}
	if _, err := wc.Write(body); err != nil {
		_ = wc.Close()
		return fmt.Errorf("backend.Write: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("backend.Close: %w", err)
	}
	if err := rc.backend.Commit(ctx, tmp, final); err != nil {
		return fmt.Errorf("backend.Commit: %w", err)
	}
	return nil
}

func isNotExistErr(err error) bool {
	return err != nil && errors.Is(err, os.ErrNotExist)
}

// resumeGate is the minimum set of fields a candidate
// backup_manifest.copy must agree with the in-flight run on, before
// pgsafe will reuse any files from it. Any mismatch invalidates the
// candidate and discovery moves on to the next one (or returns
// "no resumable", which means start fresh).
type resumeGate struct {
	PgsafeVersion        string
	BackupType           string
	ParentBackupID       string
	Compression          string
	EncryptionRecipients []string
	SystemIdentifier     uint64
}

// findResumable scans backend for a single resumable backup directory
// matching gate, opportunistically reaping abandoned candidates as it
// goes. A directory is resumable when it has a backup_manifest.copy
// AND no backup_manifest (the latter would mean the backup completed
// durably and there's nothing to resume). Candidates are inspected in
// lexicographic-descending ID order so the most-recent abandoned
// attempt wins.
//
// Auto-prune behavior: when grace > 0, any candidate whose .copy is
// older than `now - grace` is treated as abandoned and the entire
// backup-id directory is removed. This collapses what would otherwise
// be a separate `pgsafe prune --resume-orphans` sweep into a free
// side-effect of the discovery walk — we hold the per-server lock
// while scanning, and the storage list we already need to do covers
// the same prefix.
//
// Read-only on the happy path: zero risk to existing backups even on
// discovery errors. Returns (nil, false, nil) when no resumable
// candidate is found — the caller starts a fresh backup. A non-nil
// error means a transient backend failure during enumeration.
func findResumable(ctx context.Context, b storage.Backend, gate resumeGate, grace time.Duration, now time.Time, log func(string, ...any)) (*manifest.ResumeCheckpoint, bool, error) {
	if b == nil {
		return nil, false, nil
	}
	if log == nil {
		log = func(string, ...any) {}
	}
	infos, err := b.List(ctx, "")
	if err != nil {
		return nil, false, fmt.Errorf("findResumable: list: %w", err)
	}
	// Group every existing path under its top-level backup-ID
	// directory so we can ask three cheap questions per candidate
	// without re-listing: does it have backup_manifest? does it
	// have backup_manifest.copy? what's the .copy body?
	ids := candidateBackupIDs(infos)
	for _, id := range ids {
		// Skip completed: backup_manifest exists.
		if _, err := b.Stat(ctx, path.Join(id, "backup_manifest")); err == nil {
			continue
		} else if !isNotExistErr(err) {
			return nil, false, fmt.Errorf("findResumable: stat %s/backup_manifest: %w", id, err)
		}
		// Need a backup_manifest.copy.
		copyRel := path.Join(id, resumeCheckpointFilename)
		if _, err := b.Stat(ctx, copyRel); err != nil {
			if isNotExistErr(err) {
				continue
			}
			return nil, false, fmt.Errorf("findResumable: stat %s: %w", copyRel, err)
		}
		body, err := readWholeFile(ctx, b, copyRel)
		if err != nil {
			return nil, false, fmt.Errorf("findResumable: read %s: %w", copyRel, err)
		}
		cp, err := manifest.UnmarshalResumeCheckpoint(body)
		if err != nil {
			// Corrupt / wrong-version checkpoint isn't fatal — just
			// not resumable. Garbage-collect it (best-effort) so it
			// doesn't accumulate and clutter discovery on the next
			// run. Skip on delete failure; not fatal.
			log("resume: corrupt .copy at %s; reaping (decode: %v)", id, err)
			reapAbandoned(ctx, b, id, infos, log)
			continue
		}
		// Age-out: a .copy older than the grace period means the
		// prior attempt was abandoned long enough ago that the
		// operator's intent is "fresh backup, not a stale resume."
		// Reap and skip.
		if grace > 0 && !cp.CheckpointedAt.IsZero() && now.Sub(cp.CheckpointedAt) > grace {
			log("resume: %s exceeds grace period (%s old, grace=%s); reaping",
				id, now.Sub(cp.CheckpointedAt).Round(time.Second), grace)
			reapAbandoned(ctx, b, id, infos, log)
			continue
		}
		if !checkpointMatches(cp, gate) {
			// Compatible-but-mismatched: leave alone (operator may be
			// running parallel attempts under different keys/versions
			// and the grace-period reaper will catch them eventually).
			continue
		}
		// First match wins.
		return &cp, true, nil
	}
	return nil, false, nil
}

// reapAbandoned deletes every storage object under <id>/. infos is
// the pre-fetched List output so we don't re-list the storage just to
// enumerate per-id files. Best-effort: a per-file Delete error does
// NOT abort the reap; we log and continue. The whole point is to
// keep abandoned candidates from accumulating, and a partial reap
// leaves things no worse than before.
func reapAbandoned(ctx context.Context, b storage.Backend, id string, infos []storage.FileInfo, log func(string, ...any)) {
	prefix := id + "/"
	deleted := 0
	for _, fi := range infos {
		if !strings.HasPrefix(fi.Path, prefix) {
			continue
		}
		if err := b.Delete(ctx, fi.Path); err != nil && !isNotExistErr(err) {
			log("resume: reap %s: %v", fi.Path, err)
			continue
		}
		deleted++
	}
	log("resume: reaped %d objects under %s/", deleted, id)
}

// candidateBackupIDs returns the set of top-level directory names in
// infos, sorted lexicographic-descending. The pgsafe backup-ID
// format ("<YYYYMMDD>T<HHMMSS><F|I>", optionally suffixed with
// "_<ts>I" for incrementals) sorts chronologically by string compare,
// so descending order = most-recent first — the right hint for
// "the abandoned backup is probably the one I just killed."
func candidateBackupIDs(infos []storage.FileInfo) []string {
	seen := make(map[string]struct{}, len(infos))
	for _, fi := range infos {
		// First path component.
		i := strings.IndexByte(fi.Path, '/')
		if i <= 0 {
			continue
		}
		id := fi.Path[:i]
		// Skip the wal/ archive prefix and any other
		// non-backup-id top-level directory.
		if !looksLikeBackupID(id) {
			continue
		}
		seen[id] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out
}

// looksLikeBackupID is a cheap shape-check: a backup-id starts with
// 8 digits + 'T' + 6 digits + 'F' or 'I' (full or incremental). The
// full regex is more involved (incrementals can have a parent
// suffix); the shape check just needs to exclude wal/ etc.
func looksLikeBackupID(s string) bool {
	if len(s) < 16 || s[8] != 'T' {
		return false
	}
	for i := 0; i < 8; i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	for i := 9; i < 15; i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	suffix := s[15]
	return suffix == 'F' || suffix == 'I'
}

// checkpointMatches is the resume-gate comparison. Returns false on
// any mismatch — the caller skips this candidate and moves on.
func checkpointMatches(cp manifest.ResumeCheckpoint, gate resumeGate) bool {
	if cp.PgsafeVersion != gate.PgsafeVersion {
		return false
	}
	if cp.BackupType != gate.BackupType {
		return false
	}
	if cp.ParentBackupID != gate.ParentBackupID {
		return false
	}
	if cp.Compression != gate.Compression {
		return false
	}
	if cp.SystemIdentifier != gate.SystemIdentifier {
		return false
	}
	if !stringSliceEqual(cp.EncryptionRecipients, gate.EncryptionRecipients) {
		return false
	}
	return true
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// tryResume runs findResumable against opts.Backend with a gate
// derived from the in-flight run's parameters. Returns the
// matching checkpoint or nil. Best-effort: a backend hiccup during
// discovery does NOT fail the backup — the operator's logs see
// the error and the backup runs as fresh. Pinned by tests in
// resume_internal_test.go (TestFindResumable*).
func tryResume(ctx context.Context, opts Options, sysID uint64) (*manifest.ResumeCheckpoint, error) {
	if opts.ResumeDisabled {
		return nil, nil
	}
	bt := opts.Type
	if bt == "" {
		bt = TypeFull
	}
	gate := resumeGate{
		PgsafeVersion:        opts.PgsafeVersion,
		BackupType:           string(bt),
		ParentBackupID:       opts.ParentBackupID,
		Compression:          opts.Compression,
		EncryptionRecipients: opts.Recipients,
		SystemIdentifier:     sysID,
	}
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	stderr := stderrFor(opts)
	logf := func(format string, args ...any) {
		_, _ = fmt.Fprintf(stderr, "pgsafe backup: "+format+"\n", args...)
	}
	cp, found, err := findResumable(ctx, opts.Backend, gate, opts.ResumeGracePeriod, now(), logf)
	if err != nil {
		_, _ = fmt.Fprintf(stderr,
			"pgsafe backup: WARNING: resume discovery failed: %v; starting fresh.\n", err)
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return cp, nil
}

// reusablePlan is what cleanResumable hands back: a path → entry map
// of files that have been verified intact on storage from a prior
// attempt. The new attempt's per-file loop checks this map before
// uploading; matched paths skip the upload entirely.
type reusablePlan struct {
	files map[string]manifest.ResumeFileEntry
}

// reusableSkipPaths lists paths that pgsafe never reuses, even when
// the resumable manifest has them and the on-storage bytes verify.
// Two reasons to deny-list a path:
//
//   - Per-attempt content (backup_label, backup_manifest): every new
//     pg_backup_start synthesizes a fresh backup_label whose START
//     LSN belongs to THIS attempt, not the prior one. Reusing the
//     prior label would make restore replay the wrong WAL bracket.
//   - Caller-side parsing dependency: backup_label is also parsed
//     in-memory to extract start LSN; reuse skips that parse.
//
// global/pg_control is intentionally NOT on this list — pg_control
// does change after each checkpoint, but the SHA-mismatch case
// surfaces as repo-side hash mismatch in cleanResumable and the
// stale file is deleted there. If pg_control is byte-identical
// between attempts (no checkpoint happened in between), reuse is
// safe and saves a tiny upload.
func reusableSkipPath(p string) bool {
	switch p {
	case "backup_label", "backup_manifest", "tablespace_map":
		return true
	}
	return false
}

// cleanResumable walks the resumable backup's storage files,
// re-hashes each one, and returns a plan of validated reuses.
// Mirrors pgbackrest's backupResumeClean: stale files (size mismatch,
// hash mismatch, missing) are deleted from storage so the new
// attempt starts from a known-clean per-file state. Reuse-eligible
// files stay in place; their entries are returned for the per-file
// loop to skip-and-record.
//
// Best-effort: a hash failure on one file doesn't fail the backup —
// that file is just not reusable. Returns the plan even on partial
// errors so the caller can proceed with whatever it could verify.
func cleanResumable(ctx context.Context, b storage.Backend, cp *manifest.ResumeCheckpoint, log func(string, ...any)) *reusablePlan {
	plan := &reusablePlan{files: map[string]manifest.ResumeFileEntry{}}
	if b == nil || cp == nil {
		return plan
	}
	verified, deleted := 0, 0
	for _, f := range cp.Files {
		if reusableSkipPath(f.Path) {
			continue
		}
		// No repo digest recorded → nothing to verify against; skip.
		// The new attempt will re-upload (and the prior bytes get
		// silently overwritten by the writer.Close rename).
		empty := [32]byte{}
		if f.RepoSize == 0 || f.RepoSHA256 == empty {
			continue
		}
		rel := path.Join(cp.BackupID, f.Path)
		ok, hashErr := storageRepoSHAMatches(ctx, b, rel, f.RepoSize, f.RepoSHA256)
		if hashErr != nil {
			log("resume clean: hash %s: %v (treating as not reusable)", rel, hashErr)
			continue
		}
		if !ok {
			// Stale or torn from a prior partial attempt — delete
			// so the new run's writer.Close doesn't have to silently
			// overwrite something half-written.
			if delErr := b.Delete(ctx, rel); delErr != nil && !isNotExistErr(delErr) {
				log("resume clean: delete %s: %v", rel, delErr)
			}
			deleted++
			continue
		}
		plan.files[f.Path] = f
		verified++
	}
	log("resume clean: %d reusable, %d stale-deleted (of %d candidates)", verified, deleted, len(cp.Files))
	return plan
}

// storageRepoSHAMatches Stats then hashes a storage file, comparing
// its on-disk SHA-256 to expSHA. Returns (false, nil) for missing
// files or size mismatch; (true, nil) only when both size AND SHA
// match. Errors short-circuit (transport / permission / corruption).
func storageRepoSHAMatches(ctx context.Context, b storage.Backend, rel string, expSize int64, expSHA [32]byte) (bool, error) {
	fi, err := b.Stat(ctx, rel)
	if err != nil {
		if isNotExistErr(err) {
			return false, nil
		}
		return false, err
	}
	if fi.Size != expSize {
		return false, nil
	}
	rc, err := b.Get(ctx, rel)
	if err != nil {
		return false, err
	}
	defer func() { _ = rc.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return false, err
	}
	var got [32]byte
	copy(got[:], h.Sum(nil))
	return got == expSHA, nil
}

// readWholeFile streams b.Get into memory. backup_manifest.copy is
// pgsafe's own JSON; bounded by the total file count of the backup,
// in practice <1 MiB even for large clusters.
func readWholeFile(ctx context.Context, b storage.Backend, rel string) ([]byte, error) {
	rc, err := b.Get(ctx, rel)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}
