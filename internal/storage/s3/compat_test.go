package s3_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vyruss/pgsafe/internal/storage/s3"
)

// TestS3PutOmitsAWSChecksumExtensions sends a Put through the real
// SDK against an httptest server and asserts the request does NOT
// carry the AWS-only checksum negotiation headers that some
// S3-compatibles reject (Linode Object Storage, older MinIO,
// some-GCS-interop). Catches an SDK upgrade that flips the default
// of either RequestChecksumCalculation or ResponseChecksumValidation
// past the unit-test pin in cmd/pgsafe/s3_client_test.go.
//
// What we look for on the wire:
//
//   - x-amz-sdk-checksum-algorithm header (request)
//   - aws-chunked Content-Encoding (request, indicates trailing CRC32)
//   - x-amz-trailer header (request)
//   - x-amz-checksum-mode header (request, asks server for response checksum)
func TestS3PutOmitsAWSChecksumExtensions(t *testing.T) {
	t.Parallel()
	stub := newS3CaptureStub(t)
	defer stub.Close()

	client := newCompatTestClient(t, stub.URL)
	b, err := s3.New(s3.Options{
		Client: client,
		Bucket: "pgsafe-compat",
		Prefix: "",
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	w, err := b.Put(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := w.Write([]byte("world")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = w.Close()

	bad := []string{
		"X-Amz-Sdk-Checksum-Algorithm",
		"X-Amz-Trailer",
		"X-Amz-Checksum-Mode",
	}
	for _, req := range stub.requests() {
		for _, h := range bad {
			if v := req.Header.Get(h); v != "" {
				t.Errorf("request %s %s carried %s=%q (AWS-only extension; rejected by some S3-compatibles)",
					req.Method, req.URL.Path, h, v)
			}
		}
		// aws-chunked indicates trailing-CRC32 framing — incompatible
		// with WhenRequired semantics.
		if ce := req.Header.Get("Content-Encoding"); strings.Contains(ce, "aws-chunked") {
			t.Errorf("request %s %s used aws-chunked encoding (trailing checksum framing)",
				req.Method, req.URL.Path)
		}
	}
}

// s3CaptureStub is an httptest.Server that records every incoming
// request (headers + body) and replies with a benign 200 so the SDK
// doesn't retry/error in ways that hide the original request shape.
type s3CaptureStub struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []*http.Request
}

func newS3CaptureStub(t *testing.T) *s3CaptureStub {
	t.Helper()
	s := &s3CaptureStub{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		// Re-attach the body in case anything inspects it after we record.
		r.Body = io.NopCloser(bytes.NewReader(body))
		s.mu.Lock()
		s.reqs = append(s.reqs, r)
		s.mu.Unlock()
		// Reply that the bucket exists for HeadBucket (Open's probe)
		// and that PutObject/HeadObject succeed.
		w.WriteHeader(http.StatusOK)
	}))
	return s
}

func (s *s3CaptureStub) requests() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*http.Request, len(s.reqs))
	copy(out, s.reqs)
	return out
}

// newCompatTestClient builds an S3 client wired at the stub server,
// applying the SAME compat-default knobs the production
// newS3Client helper does. Mirrors cmd/pgsafe/s3_client.go's option
// choices; if the production helper changes its defaults this test
// must update too (and that's the point — drift surfaces as test
// failures, not silent regression in production).
func newCompatTestClient(t *testing.T, endpoint string) *awss3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKIAEXAMPLE", "secretEXAMPLE", "")),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}
