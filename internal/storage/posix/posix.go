// Package posix is the POSIX filesystem implementation of storage.Backend.
//
// The Put and Commit operations enforce the durability sequence in
//
//	 (Invariant #6, fsync ordering):
//
//		Put closes the writer with: fsync(file) → fsync(parent dir) →
//		rename(temp → final) → fsync(parent dir).
//		Commit does: rename(tmp → final) → fsync(parent dir), refusing to overwrite.
//
// Tests inject fault points via Options.Fault to validate every kill boundary
// per the §5 traceability table.
package posix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/vyruss/pgsafe/internal/storage"
)

// Step names mark each fault-injection boundary inside the durability
// sequence. Tests use these to assert post-crash storage state at each step.
const (
	StepWriteTemp    = "write_temp"
	StepFsyncFile    = "fsync_file"
	StepCloseFile    = "close_file"
	StepOpenDir      = "open_dir"
	StepFsyncDirPre  = "fsync_dir_pre"
	StepRename       = "rename"
	StepFsyncDirPost = "fsync_dir_post"
	StepCommitRename = "commit_rename"
	StepCommitFsync  = "commit_fsync_dir"
)

// FaultFn is invoked after each named step in Put.Close and Commit. If it
// returns a non-nil error, the operation aborts and that error is surfaced
// to the caller. Production callers leave Options.Fault nil.
type FaultFn func(step string) error

// Options configures a POSIX storage.
type Options struct {
	Root  string
	Fault FaultFn
}

// Backend is the POSIX driver. Construct via New, then call Open before use.
type Backend struct {
	root  string
	fault FaultFn
}

// New validates options and returns a Backend. It does not touch the filesystem.
func New(opts Options) (*Backend, error) {
	if opts.Root == "" {
		return nil, errors.New("posix: Root is required")
	}
	if !filepath.IsAbs(opts.Root) {
		return nil, fmt.Errorf("posix: Root %q must be an absolute path", opts.Root)
	}
	return &Backend{root: filepath.Clean(opts.Root), fault: opts.Fault}, nil
}

// Open creates the root and the wal/ sibling directory if missing. Idempotent.
func (r *Backend) Open(_ context.Context) error {
	for _, dir := range []string{r.root, filepath.Join(r.root, "wal")} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("posix: mkdir %q: %w", dir, err)
		}
	}
	return nil
}

// FreeBytes returns the user-available free space (bytes) on the
// filesystem hosting the storage root. Used by the backup caller
// for a pre-flight free-space check; cloud backends return 0 (their
// free space is effectively unbounded). Returns an error if statfs
// itself fails.
func (r *Backend) FreeBytes() (uint64, error) {
	return statfsAvailBytes(r.root)
}

func (r *Backend) abs(rel string) (string, error) {
	if !filepath.IsLocal(rel) {
		return "", fmt.Errorf("posix: relPath %q must be a relative, non-traversing path", rel)
	}
	return filepath.Join(r.root, rel), nil
}

func (r *Backend) hook(step string) error {
	if r.fault == nil {
		return nil
	}
	return r.fault(step)
}

// Put opens a writer that streams content into a temp file. Calling Close on
// the writer triggers the durability sequence and atomically renames the
// temp into place at relPath.
func (r *Backend) Put(_ context.Context, relPath string) (io.WriteCloser, error) {
	abs, err := r.abs(relPath)
	if err != nil {
		return nil, err
	}
	parent := filepath.Dir(abs)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return nil, fmt.Errorf("posix: mkdir %q: %w", parent, err)
	}
	tmp := abs + ".pgsafe-tmp"
	// abs is r.root + a filepath.IsLocal(rel) path; tmp is our own suffix.
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec // path is validated upstream by abs()
	if err != nil {
		return nil, fmt.Errorf("posix: create %q: %w", tmp, err)
	}
	if err := r.hook(StepWriteTemp); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &writer{storage: r, file: f, tmpAbs: tmp, finalAbs: abs}, nil
}

type writer struct {
	storage  *Backend
	file     *os.File
	tmpAbs   string
	finalAbs string
	closed   bool
}

func (w *writer) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

// Close runs steps 2-7 of the §3.2.3 durability sequence.
func (w *writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return fmt.Errorf("posix: fsync(file): %w", err)
	}
	if err := w.storage.hook(StepFsyncFile); err != nil {
		_ = w.file.Close()
		return err
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("posix: close(file): %w", err)
	}
	if err := w.storage.hook(StepCloseFile); err != nil {
		return err
	}

	dir, err := os.Open(filepath.Dir(w.finalAbs))
	if err != nil {
		return fmt.Errorf("posix: open(dir): %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := w.storage.hook(StepOpenDir); err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("posix: fsync(dir) pre: %w", err)
	}
	if err := w.storage.hook(StepFsyncDirPre); err != nil {
		return err
	}
	if err := os.Rename(w.tmpAbs, w.finalAbs); err != nil {
		return fmt.Errorf("posix: rename: %w", err)
	}
	if err := w.storage.hook(StepRename); err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("posix: fsync(dir) post: %w", err)
	}
	return w.storage.hook(StepFsyncDirPost)
}

// Commit atomic-renames tmp → final and fsyncs the parent directory.
// Refuses to overwrite an existing final via a stat pre-check, with the
// caveat that under concurrent committers racing to the same `final`
// the pre-check has a TOCTOU window (POSIX `rename(2)` silently
// overwrites). Real callers don't hit that window: WAL archive-push
// uses unique segment names per call, manifest commit is gated by
// `internal/lock` flock, and prune deletes finals before re-committing.
// Cloud backends provide stricter atomic-refuse-overwrite via
// conditional-put headers; do not rely on POSIX matching that.
func (r *Backend) Commit(_ context.Context, tmp, final string) error {
	tmpAbs, err := r.abs(tmp)
	if err != nil {
		return err
	}
	finalAbs, err := r.abs(final)
	if err != nil {
		return err
	}
	if _, err := os.Stat(finalAbs); err == nil {
		return fmt.Errorf("posix: Commit refuses to overwrite existing %q: %w",
			final, os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("posix: stat final: %w", err)
	}
	if err := os.Rename(tmpAbs, finalAbs); err != nil {
		return fmt.Errorf("posix: Commit rename: %w", err)
	}
	if err := r.hook(StepCommitRename); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(finalAbs))
	if err != nil {
		return fmt.Errorf("posix: Commit open(dir): %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("posix: Commit fsync(dir): %w", err)
	}
	return r.hook(StepCommitFsync)
}

// Get streams a file out for restore.
func (r *Backend) Get(_ context.Context, relPath string) (io.ReadCloser, error) {
	abs, err := r.abs(relPath)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abs) //nolint:gosec // operator-supplied storage by design
	if err != nil {
		return nil, fmt.Errorf("posix: open %q: %w", relPath, err)
	}
	return f, nil
}

// Stat reports existence and size for relPath.
func (r *Backend) Stat(_ context.Context, relPath string) (storage.FileInfo, error) {
	abs, err := r.abs(relPath)
	if err != nil {
		return storage.FileInfo{}, err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return storage.FileInfo{}, fmt.Errorf("posix: stat %q: %w", relPath, err)
	}
	return storage.FileInfo{
		Path:    relPath,
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
	}, nil
}

// List enumerates every file under prefix recursively. Skips temp files and
// the wal/ subdir (callers iterate that explicitly when needed).
func (r *Backend) List(_ context.Context, prefix string) ([]storage.FileInfo, error) {
	var rootAbs string
	if prefix == "" {
		rootAbs = r.root
	} else {
		var err error
		rootAbs, err = r.abs(prefix)
		if err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(rootAbs); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	// filepath.WalkDir uses Lstat at the root, so a symlinked directory is
	// treated as a single non-dir entry. Resolve the walk root once so
	// callers can safely point List at a symlink (the WAL-archive bind-
	// mount in tests does exactly this). Returned FileInfo.Path stays
	// relative to the *requested* layout, not the resolved one.
	walkRoot := rootAbs
	if resolved, err := filepath.EvalSymlinks(rootAbs); err == nil {
		walkRoot = resolved
	}
	var out []storage.FileInfo
	err := filepath.WalkDir(walkRoot, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			// A file vanished between readdir and lstat — typical of
			// concurrent writers committing tmp files out of the
			// directory. Skip rather than aborting the whole walk.
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.Contains(d.Name(), ".pgsafe-tmp") {
			return nil
		}
		// Rel-within-walk; reattach the prefix so callers see the original
		// (pre-symlink) layout.
		relInWalk, relErr := filepath.Rel(walkRoot, p)
		if relErr != nil {
			return relErr
		}
		var rel string
		if prefix == "" {
			rel = filepath.ToSlash(relInWalk)
		} else {
			rel = filepath.ToSlash(filepath.Join(prefix, relInWalk))
		}
		fi, statErr := d.Info()
		if statErr != nil {
			return statErr
		}
		// Skip wal/ entries when walking from the storage root — callers list
		// the wal/ directory explicitly via List(ctx, "wal") when needed.
		if prefix == "" {
			if strings.HasPrefix(rel, "wal/") || rel == "wal" {
				return nil
			}
		}
		out = append(out, storage.FileInfo{
			Path:    rel,
			Size:    fi.Size(),
			ModTime: fi.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes a single file at relPath. Returns an os.ErrNotExist-wrapped
// error when the file is absent so callers can errors.Is-detect that case
// portably across backends. Fsyncs the parent directory so the deletion is
// durable (Invariant #6 ordering: rename + fsync(parent) is what makes
// changes survive a crash; the same applies to unlinks).
func (r *Backend) Delete(_ context.Context, relPath string) error {
	abs, err := r.abs(relPath)
	if err != nil {
		return err
	}
	if err := os.Remove(abs); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("posix: Delete %q: %w", relPath, os.ErrNotExist)
		}
		return fmt.Errorf("posix: Delete %q: %w", relPath, err)
	}
	dir, err := os.Open(filepath.Dir(abs))
	if err != nil {
		// The unlink already succeeded; failing to open the parent for
		// fsync is unusual but not fatal — log and return success.
		return nil
	}
	defer func() { _ = dir.Close() }()
	_ = dir.Sync()
	return nil
}
