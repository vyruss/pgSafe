//go:build integration_cloud

package cloudtest_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	gcsstorage "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	pkgsftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"google.golang.org/api/option"

	"github.com/vyruss/pgsafe/internal/storage/azure"
	"github.com/vyruss/pgsafe/internal/storage/gcs"
	"github.com/vyruss/pgsafe/internal/storage/s3"
	pgsafesftp "github.com/vyruss/pgsafe/internal/storage/sftp"
)

// Release-engineering smoke tests against real (non-emulator) backends.
// Each test is gated on PGSAFE_REAL_CLOUD=1
// and the per-backend env vars below; absent any of them, the test
// skips. Designed to run pre-release on a machine with credentials
// out-of-band, NOT in regular CI (the integration_cloud build tag is
// already in run-ci-local.sh, but the env gates make these no-ops
// without creds).
//
// What each test asserts:
//
//   - Put + Commit + Get round-trips a small payload
//   - The pgsafe Backend constructed via the production helpers
//     (newS3Client / newAzureContainerClient / etc., mirrored here
//     for test isolation) reaches the real provider correctly with
//     the conservative compat-defaults set
//
// What these tests do NOT assert (deliberately): they don't pin
// specific request shapes — that's Phase 2's job (compat_test.go in
// each per-driver package). Phase 3 is "does it actually work
// end-to-end against the operator's real account."
//
// Required env per provider (skipped otherwise):
//
//	S3:    PGSAFE_REAL_S3_BUCKET, PGSAFE_REAL_S3_REGION
//	       (creds via standard AWS chain — env, ~/.aws/credentials, IAM)
//	       Optional: PGSAFE_REAL_S3_ENDPOINT (for non-AWS S3-compatibles
//	       like Linode), PGSAFE_REAL_S3_PATH_STYLE=1
//	Azure: PGSAFE_REAL_AZURE_ACCOUNT, PGSAFE_REAL_AZURE_CONTAINER,
//	       PGSAFE_REAL_AZURE_KEY (account key)
//	GCS:   PGSAFE_REAL_GCS_BUCKET
//	       (creds via Application Default Credentials)
//	SFTP:  PGSAFE_REAL_SFTP_HOST, PGSAFE_REAL_SFTP_USER,
//	       PGSAFE_REAL_SFTP_KEY (path to private key),
//	       PGSAFE_REAL_SFTP_HOSTKEY (authorized_keys-format host key),
//	       PGSAFE_REAL_SFTP_BASEPATH

func skipUnlessRealCloud(t *testing.T) {
	t.Helper()
	if os.Getenv("PGSAFE_REAL_CLOUD") != "1" {
		t.Skip("set PGSAFE_REAL_CLOUD=1 + per-provider env vars to run")
	}
}

// TestRealS3RoundTrip exercises the S3 backend against a real AWS-
// compatible endpoint. Pinned compat-defaults from
// cmd/pgsafe/s3_client.go are mirrored here.
func TestRealS3RoundTrip(t *testing.T) {
	skipUnlessRealCloud(t)
	bucket := os.Getenv("PGSAFE_REAL_S3_BUCKET")
	region := os.Getenv("PGSAFE_REAL_S3_REGION")
	if bucket == "" || region == "" {
		t.Skip("PGSAFE_REAL_S3_BUCKET + PGSAFE_REAL_S3_REGION required")
	}

	ctx := context.Background()
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if k, s := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"); k != "" && s != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(k, s, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	endpoint := os.Getenv("PGSAFE_REAL_S3_ENDPOINT")
	pathStyle := os.Getenv("PGSAFE_REAL_S3_PATH_STYLE") == "1"
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}
		if pathStyle {
			o.UsePathStyle = true
		}
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	b, err := s3.New(s3.Options{
		Client: client,
		Bucket: bucket,
		Prefix: realCloudPrefix(t, "s3"),
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	roundTrip(t, b, "real-s3")
}

// TestRealAzureRoundTrip exercises the Azure backend against a real
// storage account. Account key auth (User Delegation SAS lands later).
func TestRealAzureRoundTrip(t *testing.T) {
	skipUnlessRealCloud(t)
	account := os.Getenv("PGSAFE_REAL_AZURE_ACCOUNT")
	containerName := os.Getenv("PGSAFE_REAL_AZURE_CONTAINER")
	key := os.Getenv("PGSAFE_REAL_AZURE_KEY")
	if account == "" || containerName == "" || key == "" {
		t.Skip("PGSAFE_REAL_AZURE_{ACCOUNT,CONTAINER,KEY} required")
	}

	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		t.Fatalf("SharedKeyCredential: %v", err)
	}
	url := "https://" + account + ".blob.core.windows.net/"
	svc, err := azblob.NewClientWithSharedKeyCredential(url, cred,
		&azblob.ClientOptions{})
	if err != nil {
		t.Fatalf("NewClientWithSharedKeyCredential: %v", err)
	}
	cc := svc.ServiceClient().NewContainerClient(containerName)
	b, err := azure.New(azure.Options{
		ContainerClient: cc,
		Prefix:          realCloudPrefix(t, "azure"),
	})
	if err != nil {
		t.Fatalf("azure.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	roundTrip(t, b, "real-azure")
	_ = azcore.ETag("") // pin azcore import
}

// TestRealGCSRoundTrip exercises the GCS backend against a real
// bucket. Credentials come from Application Default Credentials.
func TestRealGCSRoundTrip(t *testing.T) {
	skipUnlessRealCloud(t)
	bucket := os.Getenv("PGSAFE_REAL_GCS_BUCKET")
	if bucket == "" {
		t.Skip("PGSAFE_REAL_GCS_BUCKET required")
	}

	ctx := context.Background()
	clientOpts := []option.ClientOption{}
	if cred := os.Getenv("PGSAFE_REAL_GCS_CREDENTIALS_FILE"); cred != "" {
		clientOpts = append(clientOpts, option.WithCredentialsFile(cred)) //nolint:staticcheck
	}
	client, err := gcsstorage.NewClient(ctx, clientOpts...)
	if err != nil {
		t.Fatalf("gcs NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()
	b, err := gcs.New(gcs.Options{
		Client: client,
		Bucket: bucket,
		Prefix: realCloudPrefix(t, "gcs"),
	})
	if err != nil {
		t.Fatalf("gcs.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	roundTrip(t, b, "real-gcs")
}

// TestRealSFTPRoundTrip exercises the SFTP backend against a real
// third-party SFTP server (e.g. NAS vendor, OpenSSH).
func TestRealSFTPRoundTrip(t *testing.T) {
	skipUnlessRealCloud(t)
	host := os.Getenv("PGSAFE_REAL_SFTP_HOST")
	user := os.Getenv("PGSAFE_REAL_SFTP_USER")
	keyFile := os.Getenv("PGSAFE_REAL_SFTP_KEY")
	hostKey := os.Getenv("PGSAFE_REAL_SFTP_HOSTKEY")
	basePath := os.Getenv("PGSAFE_REAL_SFTP_BASEPATH")
	if host == "" || user == "" || keyFile == "" || hostKey == "" || basePath == "" {
		t.Skip("PGSAFE_REAL_SFTP_{HOST,USER,KEY,HOSTKEY,BASEPATH} required")
	}

	keyBytes, err := os.ReadFile(keyFile) //nolint:gosec
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(hostKey))
	if err != nil {
		t.Fatalf("parse host key: %v", err)
	}

	cfg := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.FixedHostKey(pub),
		Timeout:         15 * time.Second,
	}
	addr := host
	if !strings.Contains(addr, ":") {
		addr += ":22"
	}
	sshConn, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatalf("ssh.Dial: %v", err)
	}
	defer func() { _ = sshConn.Close() }()
	cli, err := pkgsftp.NewClient(sshConn)
	if err != nil {
		t.Fatalf("sftp.NewClient: %v", err)
	}
	defer func() { _ = cli.Close() }()

	ctx := context.Background()
	b, err := pgsafesftp.New(pgsafesftp.Options{
		Client:   cli,
		BasePath: basePath + "/" + realCloudPrefix(t, "sftp"),
	})
	if err != nil {
		t.Fatalf("pgsafesftp.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	roundTrip(t, b, "real-sftp")
}

// roundTrip is the shared Put/Commit/Get assertion shape every
// real-cloud test runs. Generates a small random payload so cache
// hits don't mask reachability bugs; cleans up the objects on exit.
func roundTrip(t *testing.T, b interface {
	Put(ctx context.Context, relPath string) (io.WriteCloser, error)
	Commit(ctx context.Context, tmp, final string) error
	Get(ctx context.Context, relPath string) (io.ReadCloser, error)
	Delete(ctx context.Context, relPath string) error
}, label string) {
	t.Helper()
	ctx := context.Background()
	tmpName := label + ".tmp"
	finalName := label + ".final"
	t.Cleanup(func() {
		_ = b.Delete(ctx, tmpName)
		_ = b.Delete(ctx, finalName)
	})

	payload := make([]byte, 4096)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	w, err := b.Put(ctx, tmpName)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Commit(ctx, tmpName, finalName); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	rc, err := b.Get(ctx, finalName)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("%s round-trip body mismatch (%d/%d bytes)", label, len(got), len(payload))
	}
}

// realCloudPrefix returns a per-test-run prefix so concurrent runs
// (different developers, parallel pre-release matrices) don't collide
// on object names. Test cleanup removes both the tmp and final
// objects but the prefix prevents cross-pollution mid-run.
func realCloudPrefix(t *testing.T, backend string) string {
	t.Helper()
	host, _ := os.Hostname()
	return "pgsafe-cloudsafety/" + backend + "/" + sanitize(host) + "/" + time.Now().UTC().Format("20060102T150405")
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "host"
	}
	return string(out)
}
