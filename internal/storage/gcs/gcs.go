// Package gcs implements storage.Backend against Google Cloud Storage.
// Operators wire *storage.Client themselves; the driver owns bucket-level
// operations only.
//
//	atomicity story for GCS:
//	 - Put → uploads via Object.NewWriter; Close finalizes the object
//	   atomically. New object is only visible once Close returns.
//	 - Commit → Object.CopierFrom(tmp).Run() with the destination's
//	   `If-Generation-Match: 0` precondition (only succeeds if the
//	   destination doesn't exist). Then Object(tmp).Delete().
//
// Errors that mean "not found" are wrapped with os.ErrNotExist; "exists"
// errors are wrapped with os.ErrExist.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	pgsafestorage "github.com/vyruss/pgsafe/internal/storage"
	"google.golang.org/api/iterator"
)

// Options configure a Backend.
type Options struct {
	Client *storage.Client
	Bucket string
	Prefix string // optional object-name prefix
}

// Backend implements storage.Backend against GCS.
type Backend struct {
	client *storage.Client
	bucket string
	prefix string
}

// New validates options. Does not touch GCS.
func New(opts Options) (*Backend, error) {
	if opts.Client == nil {
		return nil, errors.New("gcs: Client is required")
	}
	if opts.Bucket == "" {
		return nil, errors.New("gcs: Bucket is required")
	}
	return &Backend{
		client: opts.Client,
		bucket: opts.Bucket,
		prefix: strings.TrimSuffix(opts.Prefix, "/"),
	}, nil
}

// Open verifies bucket reachability via Bucket.Attrs. Idempotent.
func (r *Backend) Open(ctx context.Context) error {
	if _, err := r.client.Bucket(r.bucket).Attrs(ctx); err != nil {
		return fmt.Errorf("gcs: Bucket(%q).Attrs: %w", r.bucket, err)
	}
	return nil
}

func (r *Backend) objectName(relPath string) string {
	if r.prefix == "" {
		return relPath
	}
	return r.prefix + "/" + relPath
}

func (r *Backend) obj(relPath string) *storage.ObjectHandle {
	return r.client.Bucket(r.bucket).Object(r.objectName(relPath))
}

// Put returns a streaming WriteCloser that uploads to relPath. The Writer's
// ChunkSize is 0 (single-shot upload), which avoids the resumable-upload
// fragility against fake-gcs-server emulators in CI.
func (r *Backend) Put(ctx context.Context, relPath string) (io.WriteCloser, error) {
	if relPath == "" {
		return nil, errors.New("gcs: relPath is required")
	}
	w := r.obj(relPath).NewWriter(ctx)
	w.ChunkSize = 0
	return w, nil
}

// Commit copies tmp → final with a generation-zero precondition (refuses
// to overwrite an existing final), then deletes tmp.
func (r *Backend) Commit(ctx context.Context, tmp, final string) error {
	tmpObj := r.obj(tmp)
	finalObj := r.obj(final)

	// Step 1: existence pre-check. Even though If-Generation-Match: 0 on
	// Copier.Run is reliable on real GCS, the pre-check gives a friendly
	// error on emulators that may not honor it.
	if _, err := finalObj.Attrs(ctx); err == nil {
		return fmt.Errorf("gcs: Commit refuses to overwrite existing %q: %w",
			final, os.ErrExist)
	} else if !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("gcs: Attrs(final): %w", err)
	}

	// Step 2: server-side copy with If-Generation-Match: 0 (i.e., "only
	// create if not exists"). Closes the race window left by the pre-check.
	copier := finalObj.If(storage.Conditions{DoesNotExist: true}).
		CopierFrom(tmpObj)
	if _, err := copier.Run(ctx); err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("gcs: Commit refuses to overwrite existing %q: %w",
				final, os.ErrExist)
		}
		return fmt.Errorf("gcs: Copier.Run: %w", err)
	}

	if err := tmpObj.Delete(ctx); err != nil {
		return fmt.Errorf("gcs: Delete(tmp): %w", err)
	}
	return nil
}

// Get streams an object out for restore.
func (r *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, error) {
	rc, err := r.obj(relPath).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, fmt.Errorf("gcs: Get %q: %w", relPath, os.ErrNotExist)
		}
		return nil, fmt.Errorf("gcs: NewReader %q: %w", relPath, err)
	}
	return rc, nil
}

// Stat reports size and modification time.
func (r *Backend) Stat(ctx context.Context, relPath string) (pgsafestorage.FileInfo, error) {
	attrs, err := r.obj(relPath).Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return pgsafestorage.FileInfo{}, fmt.Errorf("gcs: Stat %q: %w", relPath, os.ErrNotExist)
		}
		return pgsafestorage.FileInfo{}, fmt.Errorf("gcs: Attrs %q: %w", relPath, err)
	}
	return pgsafestorage.FileInfo{
		Path:    relPath,
		Size:    attrs.Size,
		ModTime: attrs.Updated,
	}, nil
}

// List enumerates objects whose names start with prefix.
func (r *Backend) List(ctx context.Context, prefix string) ([]pgsafestorage.FileInfo, error) {
	listPrefix := r.objectName(prefix)
	if listPrefix != "" && !strings.HasSuffix(listPrefix, "/") && prefix != "" {
		listPrefix += "/"
	}

	it := r.client.Bucket(r.bucket).Objects(ctx, &storage.Query{Prefix: listPrefix})
	var out []pgsafestorage.FileInfo
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs: Objects iterator: %w", err)
		}
		out = append(out, pgsafestorage.FileInfo{
			Path:    stripPrefix(r.prefix, attrs.Name),
			Size:    attrs.Size,
			ModTime: attrs.Updated,
		})
	}
	return out, nil
}

// Delete removes a single object. The GCS SDK returns
// storage.ErrObjectNotExist for absent objects; we wrap into os.ErrNotExist
// so callers can errors.Is-detect portably.
func (r *Backend) Delete(ctx context.Context, relPath string) error {
	if err := r.obj(relPath).Delete(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("gcs: Delete %q: %w", relPath, os.ErrNotExist)
		}
		return fmt.Errorf("gcs: Delete %q: %w", relPath, err)
	}
	return nil
}

func stripPrefix(repoPrefix, name string) string {
	if repoPrefix == "" {
		return name
	}
	return strings.TrimPrefix(name, repoPrefix+"/")
}

// isPreconditionFailed returns true when the GCS SDK's
// If-Generation-Match: 0 precondition refused because the destination
// exists.
func isPreconditionFailed(err error) bool {
	// The SDK wraps HTTP 412 as a googleapi.Error with Code=412.
	// We match by string to avoid a heavyweight dep on googleapi here;
	// the storage SDK's error message format is stable.
	msg := err.Error()
	return strings.Contains(msg, "Precondition Failed") ||
		strings.Contains(msg, "conditionNotMet") ||
		strings.Contains(msg, "412")
}
