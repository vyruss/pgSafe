package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/vyruss/pgsafe/internal/config"
)

// TestNewS3ClientCompatDefaults pins the two checksum knobs that make
// pgsafe's S3 backend work against non-AWS S3-compatibles. Without
// these the SDK adds x-amz-sdk-checksum-algorithm + trailing CRC32 on
// every PutObject/UploadPart and asks for response checksums on every
// GET — which Linode Object Storage / older MinIO / some GCS interop
// endpoints reject with 403 AccessDenied or 501 NotImplemented.
//
// Pgsafe is built for portable backups across operator-chosen object
// stores; the AWS-only protocol extensions are off-by-default. This
// test will fail loudly if either default is dropped or flipped.
func TestNewS3ClientCompatDefaults(t *testing.T) {
	t.Parallel()
	c := &config.S3Config{
		Bucket:          "pgsafe-test",
		Region:          "us-east-1",
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	cli, err := newS3Client(context.Background(), c)
	if err != nil {
		t.Fatalf("newS3Client: %v", err)
	}
	opts := cli.Options()
	if opts.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired {
		t.Errorf("RequestChecksumCalculation = %v, want WhenRequired (%v)",
			opts.RequestChecksumCalculation, aws.RequestChecksumCalculationWhenRequired)
	}
	if opts.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenRequired {
		t.Errorf("ResponseChecksumValidation = %v, want WhenRequired (%v)",
			opts.ResponseChecksumValidation, aws.ResponseChecksumValidationWhenRequired)
	}
}

// TestNewS3ClientPropagatesEndpointAndPathStyle verifies the
// S3-compatible knobs (custom endpoint host, path-style addressing)
// reach the underlying client. Linode/MinIO/etc. need both.
func TestNewS3ClientPropagatesEndpointAndPathStyle(t *testing.T) {
	t.Parallel()
	c := &config.S3Config{
		Bucket:          "pgsafe-test",
		Region:          "gb-lon-1",
		Endpoint:        "https://gb-lon-1.linodeobjects.com",
		UsePathStyle:    true,
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	}
	cli, err := newS3Client(context.Background(), c)
	if err != nil {
		t.Fatalf("newS3Client: %v", err)
	}
	opts := cli.Options()
	if opts.BaseEndpoint == nil || *opts.BaseEndpoint != c.Endpoint {
		t.Errorf("BaseEndpoint = %v, want %q", opts.BaseEndpoint, c.Endpoint)
	}
	if !opts.UsePathStyle {
		t.Errorf("UsePathStyle = false, want true")
	}
}
