// Package sftp implements storage.Backend against a remote POSIX
// filesystem reached via SFTP (over SSH). Operators wire *sftp.Client
// themselves; the driver owns path-level operations only.
//
//	atomicity story for SFTP:
//	 - Put → opens a *.pgsafe-tmp file, streams content, fsyncs (if the
//	   server supports the `fsync@openssh.com` extension), closes, and
//	   renames to the final path. The rename is atomic on the remote
//	   filesystem (POSIX rename(2) over SFTP) when source and destination
//	   are on the same filesystem.
//	 - Commit → Rename(tmp, final). Refuses overwrite via Stat pre-check.
//
// Errors that mean "not found" wrap os.ErrNotExist; "exists" wraps
// os.ErrExist.
package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/sftp"
	pgsafestorage "github.com/vyruss/pgsafe/internal/storage"
)

// Options configure a Backend.
type Options struct {
	// Client is an *sftp.Client wired by the operator (or test setup) over
	// an existing *ssh.Client connection.
	Client *sftp.Client

	// BasePath is the absolute server-side path under which the storage lives.
	// All relPaths are interpreted relative to BasePath.
	BasePath string
}

// Backend implements storage.Backend against SFTP.
type Backend struct {
	client *sftp.Client
	base   string
}

// New validates options. Does not touch the remote filesystem.
func New(opts Options) (*Backend, error) {
	if opts.Client == nil {
		return nil, errors.New("sftp: Client is required")
	}
	if opts.BasePath == "" {
		return nil, errors.New("sftp: BasePath is required")
	}
	return &Backend{
		client: opts.Client,
		base:   strings.TrimSuffix(opts.BasePath, "/"),
	}, nil
}

// Open verifies the base path exists and is a directory. Idempotent. The
// pkg/sftp client is synchronous and offers no context-aware variants, so
// ctx is currently unused; we keep the parameter to match storage.Backend.
func (r *Backend) Open(_ context.Context) error {
	fi, err := r.client.Stat(r.base)
	if err != nil {
		// Try to create it; some SFTP servers chroot to the parent and
		// require us to mkdir -p the BasePath before first use.
		if errors.Is(err, os.ErrNotExist) {
			if err := r.client.MkdirAll(r.base); err != nil {
				return fmt.Errorf("sftp: MkdirAll %q: %w", r.base, err)
			}
			return nil
		}
		return fmt.Errorf("sftp: Stat base %q: %w", r.base, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("sftp: BasePath %q is not a directory", r.base)
	}
	return nil
}

func (r *Backend) abs(relPath string) string {
	return path.Join(r.base, relPath)
}

// Put returns a streaming WriteCloser. Writes go to a *.pgsafe-tmp suffix;
// Close fsyncs (best-effort), closes, and atomically renames to relPath.
func (r *Backend) Put(_ context.Context, relPath string) (io.WriteCloser, error) {
	if relPath == "" {
		return nil, errors.New("sftp: relPath is required")
	}
	full := r.abs(relPath)

	// Ensure parent dir exists.
	if err := r.client.MkdirAll(path.Dir(full)); err != nil {
		return nil, fmt.Errorf("sftp: MkdirAll %q: %w", path.Dir(full), err)
	}

	tmp := full + ".pgsafe-tmp"
	f, err := r.client.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("sftp: Create %q: %w", tmp, err)
	}
	return &writeCloser{
		client:    r.client,
		f:         f,
		tmpPath:   tmp,
		finalPath: full,
	}, nil
}

type writeCloser struct {
	client    *sftp.Client
	f         *sftp.File
	tmpPath   string
	finalPath string
	closed    bool
}

func (w *writeCloser) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// Close fsyncs (best-effort), closes the tmp file, and renames it to final.
// On rename failure (e.g., because final exists), the tmp is left in place;
// the caller can retry or clean up.
func (w *writeCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Best-effort fsync via the openssh-server extension. Old servers
	// don't support it; we ignore that error.
	_ = w.f.Sync()
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("sftp: close tmp: %w", err)
	}
	// Posix-rename: most SFTP servers support it. The plain Rename in
	// pkg/sftp uses SSH_FXP_RENAME which is non-overwriting on most
	// servers; that's actually what we want for Put (we don't overwrite
	// because we just created the tmp). PosixRename is the overwriting
	// variant — we use plain Rename intentionally.
	if err := w.client.Rename(w.tmpPath, w.finalPath); err != nil {
		// Remove the orphan tmp before returning.
		_ = w.client.Remove(w.tmpPath)
		return fmt.Errorf("sftp: rename tmp→final: %w", err)
	}
	return nil
}

// Commit renames tmp → final, refusing to overwrite an existing final.
func (r *Backend) Commit(_ context.Context, tmp, final string) error {
	tmpAbs := r.abs(tmp)
	finalAbs := r.abs(final)

	// Pre-check: Stat the final; refuse if it exists.
	if _, err := r.client.Stat(finalAbs); err == nil {
		return fmt.Errorf("sftp: Commit refuses to overwrite existing %q: %w",
			final, os.ErrExist)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("sftp: Stat(final): %w", err)
	}

	// Plain Rename is non-overwriting on POSIX servers — closes the race
	// after the Stat pre-check.
	if err := r.client.Rename(tmpAbs, finalAbs); err != nil {
		return fmt.Errorf("sftp: Commit rename: %w", err)
	}
	return nil
}

// Get streams a file out for restore.
func (r *Backend) Get(_ context.Context, relPath string) (io.ReadCloser, error) {
	f, err := r.client.Open(r.abs(relPath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("sftp: Get %q: %w", relPath, os.ErrNotExist)
		}
		return nil, fmt.Errorf("sftp: Open %q: %w", relPath, err)
	}
	return f, nil
}

// Stat reports size and modification time.
func (r *Backend) Stat(_ context.Context, relPath string) (pgsafestorage.FileInfo, error) {
	fi, err := r.client.Stat(r.abs(relPath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return pgsafestorage.FileInfo{}, fmt.Errorf("sftp: Stat %q: %w", relPath, os.ErrNotExist)
		}
		return pgsafestorage.FileInfo{}, fmt.Errorf("sftp: Stat %q: %w", relPath, err)
	}
	return pgsafestorage.FileInfo{
		Path:    relPath,
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
	}, nil
}

// List enumerates files under prefix recursively. SFTP doesn't have
// native prefix-listing; we walk via Walk (which the pkg/sftp library
// implements as a recursive ReadDir).
func (r *Backend) List(_ context.Context, prefix string) ([]pgsafestorage.FileInfo, error) {
	rootAbs := r.base
	if prefix != "" {
		rootAbs = r.abs(prefix)
	}
	if _, err := r.client.Stat(rootAbs); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var out []pgsafestorage.FileInfo
	walker := r.client.Walk(rootAbs)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			return nil, fmt.Errorf("sftp: walk: %w", err)
		}
		fi := walker.Stat()
		if fi.IsDir() {
			continue
		}
		// Skip our own .pgsafe-tmp orphans.
		if strings.Contains(fi.Name(), ".pgsafe-tmp") {
			continue
		}
		rel, err := relativeTo(r.base, walker.Path())
		if err != nil {
			return nil, err
		}
		out = append(out, pgsafestorage.FileInfo{
			Path:    rel,
			Size:    fi.Size(),
			ModTime: fi.ModTime(),
		})
	}
	return out, nil
}

// Delete removes a single file via SFTP Remove. Surfaces os.ErrNotExist
// for absent files so callers can errors.Is-detect portably.
func (r *Backend) Delete(_ context.Context, relPath string) error {
	abs := r.abs(relPath)
	if err := r.client.Remove(abs); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("sftp: Delete %q: %w", relPath, os.ErrNotExist)
		}
		return fmt.Errorf("sftp: Delete %q: %w", relPath, err)
	}
	return nil
}

func relativeTo(base, p string) (string, error) {
	if !strings.HasPrefix(p, base+"/") {
		if p == base {
			return "", nil
		}
		return "", fmt.Errorf("sftp: path %q is outside base %q", p, base)
	}
	return strings.TrimPrefix(p, base+"/"), nil
}

// Stub to keep `time` referenced if a future helper uses it; harmless.
var _ = time.Now
