// Package storage defines the Backend interface that every backup storage
// driver implements. ships a single driver (POSIX); adds S3,
// Azure, GCS, SFTP behind the same interface.
//
// The interface is locked per  and is
// a contract — it must still hold when cloud drivers land. Any
// driver-specific knobs go on the driver's constructor (e.g. posix.Options),
// never on the interface.
package storage

import (
	"context"
	"io"
	"time"
)

// FileInfo describes one file in a storage. Returned by Stat and List.
type FileInfo struct {
	Path    string
	Size    int64
	ModTime time.Time
}

// Backend is the abstract storage seam. Implementations carry their own
// configuration; callers only see this interface.
type Backend interface {
	// Open creates or opens the storage. Idempotent.
	Open(ctx context.Context) error

	// Put streams a file into the storage at relPath. Closing the returned
	// writer is the durability point: on POSIX this is fsync(file)+fsync(dir);
	// on object stores it will be CompleteMultipartUpload.
	Put(ctx context.Context, relPath string) (io.WriteCloser, error)

	// Commit atomic-renames a tmp file (the one Put just produced) to the
	// final name. POSIX: rename(2) + fsync(dir). Object stores: conditional
	// copy + delete. Refuses to overwrite an existing final file.
	Commit(ctx context.Context, tmp, final string) error

	// Get streams a file out of the storage for restore.
	Get(ctx context.Context, relPath string) (io.ReadCloser, error)

	// Stat reports existence and size.
	Stat(ctx context.Context, relPath string) (FileInfo, error)

	// List enumerates files under prefix. Recursive.
	List(ctx context.Context, prefix string) ([]FileInfo, error)

	// Delete removes a single object at relPath. Returns os.ErrNotExist
	// (wrapped) when the object is absent. added this for `pgsafe
	// prune`; it is the only mutating operation introduces beyond
	// the existing Put/Commit pair.
	Delete(ctx context.Context, relPath string) error
}
