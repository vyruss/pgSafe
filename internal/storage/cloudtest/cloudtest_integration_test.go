//go:build integration_cloud

package cloudtest_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"path"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vyruss/pgsafe/internal/storage/cloudtest"
)

// Each test below is the Cycle-0 smoke: spin the emulator, do one
// round-trip via the production SDK, assert bytes match. The point is to
// prove the fixture works; the real driver TDD lands in later cycles.

func TestS3SmokeRoundTrip(t *testing.T) {
	t.Parallel()
	ep := cloudtest.StartS3(t)
	client := cloudtest.NewS3Client(ep)
	ctx := context.Background()

	payload := []byte("hello s3\n")
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(ep.Bucket),
		Key:    aws.String("hello.txt"),
		Body:   bytes.NewReader(payload),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ep.Bucket),
		Key:    aws.String("hello.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = out.Body.Close() }()
	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("S3 round-trip mismatch: got %q, want %q", got, payload)
	}
}

func TestAzuriteSmokeRoundTrip(t *testing.T) {
	t.Parallel()
	ep := cloudtest.StartAzurite(t)
	containerClient := cloudtest.NewAzureContainerClient(t, ep)
	ctx := context.Background()

	payload := []byte("hello azure\n")
	blobClient := containerClient.NewBlockBlobClient("hello.txt")
	if _, err := blobClient.UploadStream(ctx, bytes.NewReader(payload), nil); err != nil {
		t.Fatalf("UploadStream: %v", err)
	}

	resp, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{})
	if err != nil {
		t.Fatalf("DownloadStream: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("Azure round-trip mismatch: got %q, want %q", got, payload)
	}
}

// TestGCSSmokeRoundTrip exercises the fake-gcs-server emulator via direct
// HTTP calls (the GCS REST/JSON API), not the Go cloud.google.com/go/storage
// SDK. Reason: the SDK's resumable-upload protocol interacts badly with
// fake-gcs-server's URL rewriting, requires STORAGE_EMULATOR_HOST to be set
// process-wide (which conflicts with t.Parallel and cross-test isolation),
// and the workarounds are brittle.
// these issues with proper setup; just needs to confirm the
// emulator is reachable.
func TestGCSSmokeRoundTrip(t *testing.T) {
	t.Parallel()
	ep := cloudtest.StartGCS(t)

	payload := []byte("hello gcs\n")

	// Single-shot upload via the JSON-API media endpoint.
	uploadURL := ep.URL + "/upload/storage/v1/b/" + ep.Bucket +
		"/o?uploadType=media&name=hello.txt"
	resp, err := http.Post(uploadURL, "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("upload POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("upload: HTTP %d", resp.StatusCode)
	}

	// Download via the JSON-API media endpoint.
	downloadURL := ep.URL + "/download/storage/v1/b/" + ep.Bucket +
		"/o/hello.txt?alt=media"
	dresp, err := http.Get(downloadURL)
	if err != nil {
		t.Fatalf("download GET: %v", err)
	}
	defer func() { _ = dresp.Body.Close() }()
	got, err := io.ReadAll(dresp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("GCS round-trip mismatch: got %q, want %q", got, payload)
	}
}

func TestSFTPSmokeRoundTrip(t *testing.T) {
	t.Parallel()
	ep := cloudtest.StartSFTP(t)
	client := cloudtest.NewSFTPClient(t, ep)

	payload := []byte("hello sftp\n")
	remote := path.Join(ep.BasePath, "hello.txt")

	wf, err := client.Create(remote)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := wf.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rf, err := client.Open(remote)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = rf.Close() }()
	got, err := io.ReadAll(rf)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("SFTP round-trip mismatch: got %q, want %q", got, payload)
	}
}
