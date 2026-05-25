// Package s3 implements storage.Backend against an S3-compatible object
// store (AWS S3, MinIO, Cloudflare R2, Backblaze B2 — anything that speaks
// the S3 API). The operator wires *s3.Client (credentials, region, endpoint)
// themselves; this driver only does bucket-level operations.
//
//	atomicity story:
//	 - Put → single-shot PutObject; small file (<5 MiB) goes inline,
//	   larger files use the SDK's transfermanager for multipart upload.
//	   No tmp suffix needed: PutObject is atomic from a reader's perspective
//	   (the new object becomes visible only when the upload completes).
//	 - Commit → CopyObject(tmp → final) with `If-None-Match: *` to refuse
//	   overwriting an existing final, then DeleteObject(tmp). Maps the
//	   POSIX driver's atomic-rename semantics to S3.
//
// Errors that map to "not found" are returned as os.ErrNotExist (wrapped),
// so callers using errors.Is can write portable code regardless of backend.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/vyruss/pgsafe/internal/storage"
)

// Options configure a Backend. Client is required; everything else is optional.
type Options struct {
	// Client is the SDK client already wired with the operator's
	// credentials, region, and endpoint. Test code uses
	// cloudtest.NewS3Client(ep); production code uses
	// s3.NewFromConfig(awsconfig.LoadDefaultConfig(...)).
	Client *s3.Client

	// Bucket is the bucket the storage lives in.
	Bucket string

	// Prefix is an optional key prefix within the bucket. If non-empty,
	// every relPath the caller passes is rooted at <Prefix>/<relPath>.
	// Useful when several pgSafe servers share a bucket.
	Prefix string
}

// Backend implements storage.Backend against an S3-compatible store.
type Backend struct {
	client *s3.Client
	bucket string
	prefix string
}

// New validates options and returns a Backend. Does not touch the bucket.
func New(opts Options) (*Backend, error) {
	if opts.Client == nil {
		return nil, errors.New("s3: Client is required")
	}
	if opts.Bucket == "" {
		return nil, errors.New("s3: Bucket is required")
	}
	return &Backend{
		client: opts.Client,
		bucket: opts.Bucket,
		prefix: strings.TrimSuffix(opts.Prefix, "/"),
	}, nil
}

// Open verifies bucket reachability via HeadBucket. Idempotent.
func (r *Backend) Open(ctx context.Context) error {
	_, err := r.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(r.bucket),
	})
	if err != nil {
		return fmt.Errorf("s3: HeadBucket %q: %w", r.bucket, err)
	}
	return nil
}

// keyFor maps a caller-provided relPath to the actual S3 object key,
// applying the optional prefix.
func (r *Backend) keyFor(relPath string) string {
	if r.prefix == "" {
		return relPath
	}
	return r.prefix + "/" + relPath
}

// Put returns a streaming WriteCloser that uploads to relPath. Small files
// (<5 MiB) buffer in memory; larger files use multipart upload via the
// SDK's manager.Uploader. Closing the writer is the durability point.
func (r *Backend) Put(ctx context.Context, relPath string) (io.WriteCloser, error) {
	if relPath == "" {
		return nil, errors.New("s3: relPath is required")
	}
	pr, pw := io.Pipe()
	// manager.NewUploader is deprecated in favour of feature/s3/transfermanager,
	// which the Cycle-7 cloud-wiring pass will migrate. The replacement
	// is API-incompatible so we keep the deprecated form until that cycle.
	//
	// manager.Uploader has its OWN RequestChecksumCalculation field
	// that defaults to WhenSupported regardless of what the underlying
	// s3.Client carries. Without overriding it here, every UploadPart
	// in the multipart path 403s on S3-compatibles that don't
	// implement the trailing-checksum protocol (Linode Object Storage
	// is the headline case; the same issue exists on older MinIO and
	// some GCS interop endpoints). Mirror the s3.Client's WhenRequired
	// setting onto the Uploader.
	uploader := manager.NewUploader(r.client, func(u *manager.Uploader) { //nolint:staticcheck
		u.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})

	errCh := make(chan error, 1)
	go func() {
		//nolint:staticcheck // see Cycle-7 transfermanager migration note above.
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(r.bucket),
			Key:    aws.String(r.keyFor(relPath)),
			Body:   pr,
		})
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

// Close flushes the pipe and waits for the underlying upload to complete.
// Either side returning an error fails the Close.
func (w *writeCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.pw.Close(); err != nil {
		return fmt.Errorf("s3: close pipe: %w", err)
	}
	if err := <-w.errCh; err != nil {
		return fmt.Errorf("s3: upload: %w", err)
	}
	return nil
}

// Commit copies tmp → final without overwriting an existing final, then
// deletes tmp. Two-layer safety:
//
//  1. HeadObject(final) — if it already exists, fail fast with
//     os.ErrExist. Race-prone against concurrent committers but catches
//     the common case quickly and avoids a wasteful CopyObject.
//  2. CopyObject(tmp → final) with `If-None-Match: *`. AWS S3 honors this
//     conditional write since Aug 2024; MinIO support varies by version.
//     When honored, it closes the race window left by the HeadObject
//     pre-check. When not honored, we fall back to the HEAD result.
//
// For true race-safety on every backend, retention will introduce
// a per-server lockfile (Invariant #4). operators run one
// committer at a time, which the HEAD+CopyObject combo handles.
func (r *Backend) Commit(ctx context.Context, tmp, final string) error {
	tmpKey := r.keyFor(tmp)
	finalKey := r.keyFor(final)

	// Step 1: existence pre-check.
	if _, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(finalKey),
	}); err == nil {
		return fmt.Errorf("s3: Commit refuses to overwrite existing %q: %w",
			final, os.ErrExist)
	} else if !isNotFound(err) {
		return fmt.Errorf("s3: HeadObject(final) %q: %w", final, err)
	}

	// Step 2: conditional CopyObject. IfNoneMatch="*" closes the race
	// window between HEAD and Copy on backends that honor it (AWS
	// since Aug 2024). S3-compatibles that don't (Linode Object
	// Storage at the time of writing returns 501 NotImplemented for
	// the if-none-match header) fall back to the HEAD pre-check —
	// race-safe enough for single-committer ops and what the
	// per-server lock (#4 future work) will close.
	_, err := r.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:      aws.String(r.bucket),
		Key:         aws.String(finalKey),
		CopySource:  aws.String(r.bucket + "/" + tmpKey),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil && isNotImplemented(err) {
		// Retry without the conditional header.
		_, err = r.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(r.bucket),
			Key:        aws.String(finalKey),
			CopySource: aws.String(r.bucket + "/" + tmpKey),
		})
	}
	if err != nil {
		if isPreconditionFailed(err) {
			return fmt.Errorf("s3: Commit refuses to overwrite existing %q: %w",
				final, os.ErrExist)
		}
		return fmt.Errorf("s3: CopyObject: %w", err)
	}

	if _, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(tmpKey),
	}); err != nil {
		return fmt.Errorf("s3: DeleteObject(tmp): %w", err)
	}
	return nil
}

// Get streams an object out for restore.
func (r *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, error) {
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.keyFor(relPath)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("s3: Get %q: %w", relPath, os.ErrNotExist)
		}
		return nil, fmt.Errorf("s3: GetObject %q: %w", relPath, err)
	}
	return out.Body, nil
}

// Stat reports size and modification time.
func (r *Backend) Stat(ctx context.Context, relPath string) (storage.FileInfo, error) {
	out, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.keyFor(relPath)),
	})
	if err != nil {
		if isNotFound(err) {
			return storage.FileInfo{}, fmt.Errorf("s3: Stat %q: %w", relPath, os.ErrNotExist)
		}
		return storage.FileInfo{}, fmt.Errorf("s3: HeadObject %q: %w", relPath, err)
	}
	fi := storage.FileInfo{Path: relPath}
	if out.ContentLength != nil {
		fi.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		fi.ModTime = *out.LastModified
	}
	return fi, nil
}

// List enumerates objects whose keys start with prefix (after the optional
// storage-prefix is applied). Returned paths are storage-relative.
func (r *Backend) List(ctx context.Context, prefix string) ([]storage.FileInfo, error) {
	listPrefix := r.keyFor(prefix)
	if listPrefix != "" && !strings.HasSuffix(listPrefix, "/") && prefix != "" {
		listPrefix += "/"
	}

	var out []storage.FileInfo
	pager := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucket),
		Prefix: aws.String(listPrefix),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3: ListObjectsV2: %w", err)
		}
		for _, obj := range page.Contents {
			fi := storage.FileInfo{Path: stripStoragePrefix(r.prefix, aws.ToString(obj.Key))}
			if obj.Size != nil {
				fi.Size = *obj.Size
			}
			if obj.LastModified != nil {
				fi.ModTime = *obj.LastModified
			}
			out = append(out, fi)
		}
	}
	return out, nil
}

// Delete removes a single object. S3's DeleteObject is idempotent (no
// error on missing keys), so we Stat first to surface os.ErrNotExist.
func (r *Backend) Delete(ctx context.Context, relPath string) error {
	key := r.keyFor(relPath)
	if _, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	}); err != nil {
		if isNotFound(err) {
			return fmt.Errorf("s3: Delete %q: %w", relPath, os.ErrNotExist)
		}
		return fmt.Errorf("s3: HeadObject for Delete %q: %w", relPath, err)
	}
	if _, err := r.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("s3: DeleteObject %q: %w", relPath, err)
	}
	return nil
}

func stripStoragePrefix(repoPrefix, key string) string {
	if repoPrefix == "" {
		return key
	}
	return strings.TrimPrefix(key, repoPrefix+"/")
}

// isNotFound returns true for S3 SDK errors that mean "the object/bucket
// doesn't exist". MinIO and AWS use slightly different codes; we cover both.
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NoSuchKey", "NotFound", "NoSuchBucket":
		return true
	}
	var nsk *types.NoSuchKey
	return errors.As(err, &nsk)
}

// isPreconditionFailed returns true for S3 SDK errors that mean a
// conditional write (If-None-Match: *) refused because the target exists.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "PreconditionFailed"
}

// isNotImplemented returns true when the backend rejected a request
// because some optional protocol feature isn't supported (e.g. Linode
// Object Storage returning 501 for `If-None-Match: *` on CopyObject).
// Distinct from isNotFound — the request is well-formed but the server
// refuses to implement that header / verb. Caller can retry with a
// degraded variant.
func isNotImplemented(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "NotImplemented"
}

// keyJoin is path.Join applied to S3 keys (always forward slashes, no
// leading slash). Currently unused beyond keyFor's hot path; kept here as
// a marker for future helpers that need to compose keys outside
// the storage prefix.
var _ = path.Join
