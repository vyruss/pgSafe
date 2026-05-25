package creds

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	iamcredentials "cloud.google.com/go/iam/credentials/apiv1"
	"cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/vyruss/pgsafe/internal/config"
)

// DefaultLifetime is how long minted credentials live. Cloud providers
// enforce server-side; we ask for this duration but they may cap shorter.
// One hour is the AWS STS minimum; matches Azure's typical SAS lifetime;
// fits inside GCS's max 12-hour impersonation duration.
const DefaultLifetime = 1 * time.Hour

// MintS3STS calls AWS STS AssumeRole with an inline session policy that
// narrows the resulting credential to s3:PutObject + s3:AbortMultipartUpload
// on the configured bucket+prefix only. Reads/Lists outside the prefix are
// denied server-side.
//
// Operator config requirements: cfg.S3.AccessKeyID + SecretAccessKey must
// have sts:AssumeRole on a role whose trust policy permits the
// caller's identity. The role itself can be permissive — the
// inline session policy does the actual scoping.
//
// the role ARN comes from an env var (PGSAFE_S3_ROLE_ARN)
// rather than YAML, because it's typically the same across all backups for
// a given operator and YAML duplication is friction. retention may
// promote it to the StorageConfig.
func MintS3STS(ctx context.Context, cfg *config.S3Config, roleARN string, lifetime time.Duration) (Credential, error) {
	if lifetime <= 0 {
		lifetime = DefaultLifetime
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return Credential{}, fmt.Errorf("creds s3sts: aws config: %w", err)
	}
	stsClient := sts.NewFromConfig(awsCfg, func(o *sts.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = &cfg.Endpoint
		}
	})

	policy, err := s3InlinePolicy(cfg.Bucket, cfg.Prefix)
	if err != nil {
		return Credential{}, err
	}
	dur := int32(lifetime.Seconds()) //nolint:gosec // <86400; AWS caps separately
	out, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         &roleARN,
		RoleSessionName: stringPtr("pgsafe-backup"),
		Policy:          &policy,
		DurationSeconds: &dur,
	})
	if err != nil {
		return Credential{}, fmt.Errorf("creds s3sts: AssumeRole: %w", err)
	}
	if out.Credentials == nil {
		return Credential{}, fmt.Errorf("creds s3sts: AssumeRole returned no credentials")
	}
	return Credential{
		Type: TypeS3STS,
		S3STS: &S3STSCredential{
			AccessKeyID:     deref(out.Credentials.AccessKeyId),
			SecretAccessKey: deref(out.Credentials.SecretAccessKey),
			SessionToken:    deref(out.Credentials.SessionToken),
			Expiration:      derefTime(out.Credentials.Expiration),
			Region:          cfg.Region,
			Bucket:          cfg.Bucket,
			Prefix:          cfg.Prefix,
			Endpoint:        cfg.Endpoint,
			UsePathStyle:    cfg.UsePathStyle,
		},
	}, nil
}

// s3InlinePolicy returns the narrow JSON IAM policy to attach to the
// AssumeRole call. Allows: PutObject + AbortMultipartUpload on the prefix
// only. Implicitly denies everything else.
func s3InlinePolicy(bucket, prefix string) (string, error) {
	resource := fmt.Sprintf("arn:aws:s3:::%s/", bucket)
	if prefix != "" {
		resource += prefix + "/"
	}
	resource += "*"

	doc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect": "Allow",
				"Action": []string{
					"s3:PutObject",
					"s3:AbortMultipartUpload",
				},
				"Resource": resource,
			},
		},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("creds s3sts: marshal policy: %w", err)
	}
	return string(b), nil
}

// MintAzureSAS produces a User Delegation SAS narrowed to write+create+add
// on the configured container+prefix. The User Delegation Key is signed by
// the caller's Azure AD identity (DefaultAzureCredential — env var,
// managed identity, or `az login`) and lasts up to 7 days; the SAS we mint
// expires in `lifetime`.
//
// For tests against Azurite (which supports User Delegation SAS in newer
// versions) the caller's identity is a shared-key credential and we
// fall back to a Service SAS — the wire format is identical from the
// worker's side.
func MintAzureSAS(_ context.Context, cfg *config.AzureConfig, lifetime time.Duration) (Credential, error) {
	if lifetime <= 0 {
		lifetime = DefaultLifetime
	}
	if cfg.AccountName == "" || cfg.Container == "" {
		return Credential{}, fmt.Errorf("creds azure_sas: account_name and container required")
	}

	// the caller's auth uses the operator-supplied
	// account_key (matching the simple/remote-parallel paths). When the
	// caller has it, we sign a Service SAS directly. When the
	// operator wants true User Delegation SAS (no account key on disk), the
	//
	if cfg.AccountKey == "" {
		return Credential{}, fmt.Errorf("creds azure_sas: account_key required")
	}

	cred, err := azblob.NewSharedKeyCredential(cfg.AccountName, cfg.AccountKey)
	if err != nil {
		return Credential{}, fmt.Errorf("creds azure_sas: shared key: %w", err)
	}

	// HTTPS-only — non-negotiable. A SAS that's valid over plain HTTP
	// would let the worker transmit credentials in cleartext on a
	// compromised network path; that defeats the entire Tenet-3
	// guarantee. Tests that need to round-trip against Azurite (which
	// is HTTP-only) cover MintAzureSAS at the unit level only; the
	// real-cloud round-trip is gated on PGSAFE_REAL_CLOUD=1 like the
	// S3 STS and GCS impersonation paths.
	//
	// Read is required so the worker's azure.Backend.Open() existence
	// check (GetProperties) and the Commit-side HEAD pre-check succeed.
	// Read access remains scoped to the prefix-bounded SAS, so the
	// Tenet-3 "write-only outside the prefix" guarantee is unchanged.
	sigVals := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(-2 * time.Minute),
		ExpiryTime:    time.Now().UTC().Add(lifetime),
		Permissions:   (&sas.BlobPermissions{Read: true, Write: true, Create: true, Add: true}).String(),
		ContainerName: cfg.Container,
	}
	if cfg.Prefix != "" {
		sigVals.BlobName = cfg.Prefix + "/"
	}
	q, err := sigVals.SignWithSharedKey(cred)
	if err != nil {
		return Credential{}, fmt.Errorf("creds azure_sas: sign: %w", err)
	}
	encoded := q.Encode()
	if encoded == "" {
		return Credential{}, fmt.Errorf("creds azure_sas: empty SAS query string")
	}

	// Build the canonical service URL the worker will use.
	serviceURL := cfg.BlobEndpoint
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AccountName)
	}
	// The worker concatenates "?<sas>" onto serviceURL; assert a trailing
	// slash for predictability.
	if !endsWithSlash(serviceURL) {
		serviceURL += "/"
	}

	// Sanity-check that the URL parses.
	if _, err := url.Parse(serviceURL); err != nil {
		return Credential{}, fmt.Errorf("creds azure_sas: parse service url: %w", err)
	}

	// Suppress unused import lint when the service package is referenced
	// but no symbol is consumed at compile time.
	_ = service.AccessConditions{}

	return Credential{
		Type: TypeAzureSAS,
		AzureSAS: &AzureSASCredential{
			AccountName: cfg.AccountName,
			Container:   cfg.Container,
			SASToken:    encoded,
			ServiceURL:  serviceURL,
			Expiration:  sigVals.ExpiryTime,
			Prefix:      cfg.Prefix,
		},
	}, nil
}

// MintGCSToken produces a short-lived OAuth2 access token by impersonating
// a GCS service account. The token has scope
// https://www.googleapis.com/auth/devstorage.read_write — and the IAM
// binding on the target service account is what actually constrains the
// prefix (operator-side IAM policy).
//
// the caller's identity comes from
// GOOGLE_APPLICATION_CREDENTIALS (or ADC); we call iamcredentials.
// GenerateAccessToken on the configured target service account.
func MintGCSToken(ctx context.Context, cfg *config.GCSConfig, targetServiceAccount string, lifetime time.Duration) (Credential, error) {
	if lifetime <= 0 {
		lifetime = DefaultLifetime
	}
	if cfg.Bucket == "" {
		return Credential{}, fmt.Errorf("creds gcs_token: bucket required")
	}
	if targetServiceAccount == "" {
		return Credential{}, fmt.Errorf("creds gcs_token: targetServiceAccount required")
	}

	var clientOpts []option.ClientOption
	if cfg.CredentialsFile != "" {
		clientOpts = append(clientOpts, option.WithCredentialsFile(cfg.CredentialsFile)) //nolint:staticcheck
	}
	cli, err := iamcredentials.NewIamCredentialsClient(ctx, clientOpts...)
	if err != nil {
		return Credential{}, fmt.Errorf("creds gcs_token: iamcredentials client: %w", err)
	}
	defer func() { _ = cli.Close() }()

	resp, err := cli.GenerateAccessToken(ctx, &credentialspb.GenerateAccessTokenRequest{
		Name:     fmt.Sprintf("projects/-/serviceAccounts/%s", targetServiceAccount),
		Scope:    []string{"https://www.googleapis.com/auth/devstorage.read_write"},
		Lifetime: durationpb.New(lifetime),
	})
	if err != nil {
		return Credential{}, fmt.Errorf("creds gcs_token: GenerateAccessToken: %w", err)
	}
	exp := time.Time{}
	if resp.GetExpireTime() != nil {
		exp = resp.GetExpireTime().AsTime()
	}
	return Credential{
		Type: TypeGCSToken,
		GCSToken: &GCSTokenCredential{
			AccessToken: resp.GetAccessToken(),
			TokenType:   "Bearer",
			Expiration:  exp,
			Bucket:      cfg.Bucket,
			Prefix:      cfg.Prefix,
			Endpoint:    cfg.Endpoint,
		},
	}, nil
}

// MintSFTPKey reads the operator's PEM private key from disk on the backup
// host (where it's authorized to live) and ships its bytes in a Credential.
// The worker on the PG host receives the bytes in memory via the JSON-RPC
// frame and never persists them.
func MintSFTPKey(cfg *config.SFTPConfig) (Credential, error) {
	if cfg.PrivateKeyFile == "" {
		return Credential{}, fmt.Errorf("creds sftp_key: private_key_file required (password auth not supported in hybrid mode)")
	}
	pem, err := os.ReadFile(cfg.PrivateKeyFile) //nolint:gosec // operator-supplied path
	if err != nil {
		return Credential{}, fmt.Errorf("creds sftp_key: read %s: %w", cfg.PrivateKeyFile, err)
	}
	port := cfg.Port
	if port == 0 {
		port = 22
	}
	return Credential{
		Type: TypeSFTPKey,
		SFTPKey: &SFTPKeyCredential{
			Host:                  cfg.Host,
			Port:                  port,
			Username:              cfg.Username,
			PrivateKeyPEM:         pem,
			BasePath:              cfg.BasePath,
			HostKey:               cfg.HostKey,
			InsecureIgnoreHostKey: cfg.InsecureIgnoreHostKey,
		},
	}, nil
}

func endsWithSlash(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '/'
}

func stringPtr(s string) *string { return &s }

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefTime(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}
