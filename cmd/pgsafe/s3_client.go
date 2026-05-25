package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vyruss/pgsafe/internal/config"
)

// newS3Client builds an aws-sdk-go-v2 S3 client from pgsafe's
// config.S3Config. Extracted from openS3 so its compatibility-critical
// option choices can be unit-tested independently of credential
// loading and backend wiring.
//
// Compatibility-critical defaults: aws-sdk-go-v2 ≥ v1.31 defaults BOTH
// checksum knobs to "WhenSupported", which makes every
// PutObject/UploadPart ship an x-amz-sdk-checksum-algorithm header
// (and a trailing CRC32 chunk) and every GetObject/HEAD request ask
// the server for response body checksums to validate. AWS S3 itself
// honors all of this, but every S3-compatible we have access to
// (Linode Object Storage, older MinIO, some GCS interop endpoints)
// returns 403 AccessDenied or 501 NotImplemented for the
// trailing-checksum SigV4 + response-checksum negotiation flows.
// "WhenRequired" on both reverts to fixed-payload SigV4 with no extra
// checksum headers — works everywhere AWS S3 works AND on every
// S3-compatible we have access to. Pgsafe is built for portable
// backups across operator-chosen object stores; we err on the side
// of compatibility over AWS-specific protocol features.
//
// Confirmed by Linode Object Storage's own documentation, which
// recommends `request_checksum_calculation=when_required` for SDK
// versions released after Jan 15 2025.
func newS3Client(ctx context.Context, c *config.S3Config) (*awss3.Client, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(c.Region),
	}
	if c.AccessKeyID != "" && c.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("storage s3: aws config: %w", err)
	}
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = &c.Endpoint
		}
		if c.UsePathStyle {
			o.UsePathStyle = true
		}
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}), nil
}
