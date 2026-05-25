// Package azure implements storage.Backend against Azure Blob Storage.
// Operators wire *azblob.Client (Account name + key, or DefaultAzureCredential)
// themselves; the driver owns container-level operations only.
//
//	atomicity story for Azure:
//	 - Put → uploads via UploadStream (block-blob, multipart-equivalent).
//	   Atomic from a reader's perspective: the new blob is only visible
//	   once UploadStream commits the block list.
//	 - Commit → CopyBlob(tmp → final) with an If-None-Match: * conditional;
//	   refuses to overwrite an existing final. Then DeleteBlob(tmp).
//
// Errors that mean "not found" are wrapped with os.ErrNotExist; "exists"
// errors are wrapped with os.ErrExist. Callers using errors.Is get
// portable behavior across all backends.
package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/vyruss/pgsafe/internal/storage"
)

// Options configure a Backend.
type Options struct {
	// ContainerClient is the SDK client scoped to a container, already
	// wired with credentials. Test code uses
	// cloudtest.NewAzureContainerClient(t, ep); production code builds it
	// from azblob.ServiceClient.NewContainerClient.
	ContainerClient *container.Client

	// Prefix is an optional blob-name prefix within the container.
	Prefix string
}

// Backend implements storage.Backend against Azure Blob.
type Backend struct {
	cc     *container.Client
	prefix string
}

// New validates options and returns a Backend. Does not touch the container.
func New(opts Options) (*Backend, error) {
	if opts.ContainerClient == nil {
		return nil, errors.New("azure: ContainerClient is required")
	}
	return &Backend{
		cc:     opts.ContainerClient,
		prefix: strings.TrimSuffix(opts.Prefix, "/"),
	}, nil
}

// Open verifies container reachability via GetProperties. Idempotent.
func (r *Backend) Open(ctx context.Context) error {
	if _, err := r.cc.GetProperties(ctx, nil); err != nil {
		return fmt.Errorf("azure: GetProperties: %w", err)
	}
	return nil
}

func (r *Backend) blobName(relPath string) string {
	if r.prefix == "" {
		return relPath
	}
	return r.prefix + "/" + relPath
}

// Put returns a streaming WriteCloser that uploads via UploadStream
// (blockblob), which handles chunking transparently. Closing the writer
// is the durability point.
func (r *Backend) Put(ctx context.Context, relPath string) (io.WriteCloser, error) {
	if relPath == "" {
		return nil, errors.New("azure: relPath is required")
	}
	bbc := r.cc.NewBlockBlobClient(r.blobName(relPath))

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, err := bbc.UploadStream(ctx, pr, &blockblob.UploadStreamOptions{})
		errCh <- err
		_ = pr.CloseWithError(err)
	}()
	return &writeCloser{pw: pw, errCh: errCh}, nil
}

type writeCloser struct {
	pw     *io.PipeWriter
	errCh  <-chan error
	closed bool
}

func (w *writeCloser) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *writeCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.pw.Close(); err != nil {
		return fmt.Errorf("azure: close pipe: %w", err)
	}
	if err := <-w.errCh; err != nil {
		return fmt.Errorf("azure: upload: %w", err)
	}
	return nil
}

// Commit copies tmp → final (refusing to overwrite an existing final via
// HEAD pre-check + If-None-Match: *), then deletes tmp.
func (r *Backend) Commit(ctx context.Context, tmp, final string) error {
	tmpName := r.blobName(tmp)
	finalName := r.blobName(final)

	finalClient := r.cc.NewBlobClient(finalName)
	tmpClient := r.cc.NewBlobClient(tmpName)

	// Step 1: existence pre-check.
	if _, err := finalClient.GetProperties(ctx, nil); err == nil {
		return fmt.Errorf("azure: Commit refuses to overwrite existing %q: %w",
			final, os.ErrExist)
	} else if !isNotFound(err) {
		return fmt.Errorf("azure: GetProperties(final): %w", err)
	}

	// Step 2: server-side copy from tmp to final, refusing destination overwrite.
	tmpURL := tmpClient.URL()
	if _, err := finalClient.CopyFromURL(ctx, tmpURL, &blob.CopyFromURLOptions{
		BlobAccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: to.Ptr(azcore.ETag("*")),
			},
		},
	}); err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("azure: Commit refuses to overwrite existing %q: %w",
				final, os.ErrExist)
		}
		return fmt.Errorf("azure: CopyFromURL: %w", err)
	}

	if _, err := tmpClient.Delete(ctx, nil); err != nil {
		return fmt.Errorf("azure: Delete(tmp): %w", err)
	}
	return nil
}

// Get streams a blob out for restore.
func (r *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, error) {
	bc := r.cc.NewBlobClient(r.blobName(relPath))
	resp, err := bc.DownloadStream(ctx, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("azure: Get %q: %w", relPath, os.ErrNotExist)
		}
		return nil, fmt.Errorf("azure: DownloadStream %q: %w", relPath, err)
	}
	return resp.Body, nil
}

// Stat reports size and modification time.
func (r *Backend) Stat(ctx context.Context, relPath string) (storage.FileInfo, error) {
	bc := r.cc.NewBlobClient(r.blobName(relPath))
	resp, err := bc.GetProperties(ctx, nil)
	if err != nil {
		if isNotFound(err) {
			return storage.FileInfo{}, fmt.Errorf("azure: Stat %q: %w", relPath, os.ErrNotExist)
		}
		return storage.FileInfo{}, fmt.Errorf("azure: GetProperties %q: %w", relPath, err)
	}
	fi := storage.FileInfo{Path: relPath}
	if resp.ContentLength != nil {
		fi.Size = *resp.ContentLength
	}
	if resp.LastModified != nil {
		fi.ModTime = *resp.LastModified
	}
	return fi, nil
}

// List enumerates blobs whose names start with prefix.
func (r *Backend) List(ctx context.Context, prefix string) ([]storage.FileInfo, error) {
	listPrefix := r.blobName(prefix)
	if listPrefix != "" && !strings.HasSuffix(listPrefix, "/") && prefix != "" {
		listPrefix += "/"
	}

	pager := r.cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &listPrefix,
	})
	var out []storage.FileInfo
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: ListBlobsFlat: %w", err)
		}
		for _, b := range page.Segment.BlobItems {
			fi := storage.FileInfo{Path: stripPrefix(r.prefix, *b.Name)}
			if b.Properties != nil {
				if b.Properties.ContentLength != nil {
					fi.Size = *b.Properties.ContentLength
				}
				if b.Properties.LastModified != nil {
					fi.ModTime = *b.Properties.LastModified
				}
			}
			out = append(out, fi)
		}
	}
	return out, nil
}

// Delete removes a single blob. Surfaces os.ErrNotExist for absent blobs
// so callers can errors.Is-detect that case portably.
func (r *Backend) Delete(ctx context.Context, relPath string) error {
	bc := r.cc.NewBlobClient(r.blobName(relPath))
	if _, err := bc.Delete(ctx, nil); err != nil {
		if isNotFound(err) {
			return fmt.Errorf("azure: Delete %q: %w", relPath, os.ErrNotExist)
		}
		return fmt.Errorf("azure: Delete %q: %w", relPath, err)
	}
	return nil
}

func stripPrefix(repoPrefix, name string) string {
	if repoPrefix == "" {
		return name
	}
	return strings.TrimPrefix(name, repoPrefix+"/")
}

// isNotFound returns true for Azure SDK errors that mean "the blob/container
// doesn't exist".
func isNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) {
		return false
	}
	return respErr.ErrorCode == "BlobNotFound" || respErr.ErrorCode == "ContainerNotFound"
}

// isPreconditionFailed returns true when an If-None-Match conditional write
// refused because the destination exists.
func isPreconditionFailed(err error) bool {
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) {
		return false
	}
	return respErr.StatusCode == 412 || respErr.ErrorCode == "ConditionNotMet"
}
