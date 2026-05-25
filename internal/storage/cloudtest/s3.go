package cloudtest

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

// S3Endpoint carries the connection details for the spun-up MinIO container.
// MinIO speaks the S3 protocol, so the production S3 driver (which uses
// aws-sdk-go-v2/service/s3) talks to this without changes — the operator
// only sees a different endpoint URL.
type S3Endpoint struct {
	URL       string // http://host:port (no path)
	AccessKey string
	SecretKey string
	Region    string // arbitrary; MinIO accepts anything but the SDK requires a value
	Bucket    string // pre-created
}

// StartS3 launches a MinIO container, creates a fresh bucket, and returns
// the endpoint. The container terminates when the test ends.
func StartS3(t *testing.T) S3Endpoint {
	t.Helper()
	ctx := context.Background()

	c, err := minio.Run(ctx, "minio/minio:RELEASE.2025-04-22T22-12-26Z")
	if err != nil {
		t.Fatalf("minio.Run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	connStr, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("minio ConnectionString: %v", err)
	}
	// Container's ConnectionString returns "host:port" — promote to URL.
	endpointURL := connStr
	if !strings.HasPrefix(endpointURL, "http") {
		endpointURL = "http://" + endpointURL
	}
	if _, err := url.Parse(endpointURL); err != nil {
		t.Fatalf("parse endpoint %q: %v", endpointURL, err)
	}

	ep := S3Endpoint{
		URL:       endpointURL,
		AccessKey: c.Username,
		SecretKey: c.Password,
		Region:    "us-east-1",
		Bucket:    "pgsafe-test",
	}

	// Pre-create the bucket so tests don't have to.
	client := NewS3Client(ep)
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(ep.Bucket),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	return ep
}

// NewS3Client returns a path-style S3 client wired to the given emulator
// endpoint. Path-style is required for MinIO (no virtual-host DNS).
func NewS3Client(ep S3Endpoint) *s3.Client {
	return s3.NewFromConfig(aws.Config{
		Region: ep.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			ep.AccessKey, ep.SecretKey, "",
		),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ep.URL)
		o.UsePathStyle = true
	})
}
