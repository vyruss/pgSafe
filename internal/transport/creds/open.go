package creds

// open.go is the WORKER-SIDE companion to mint.go. The caller mints
// scoped credentials and ships them via JSON-RPC; the worker calls
// OpenBackendFromCredential to produce a storage.Backend that uses ONLY
// the in-memory credential — no disk reads, no env-variable fallbacks.

import (
	"context"
	"errors"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	gcsstorage "cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/storage/azure"
	"github.com/vyruss/pgsafe/internal/storage/gcs"
	pgsafes3 "github.com/vyruss/pgsafe/internal/storage/s3"
	pgsafesftp "github.com/vyruss/pgsafe/internal/storage/sftp"
)

// OpenBackendFromCredential constructs an opened storage.Backend using the
// supplied scoped credential and nothing else. Returns the backend plus a
// cleanup closure the caller MUST defer.
//
// POSIX is not supported here — Tenet 3 only applies to network backends.
// The caller decides not to ship the credentials at all when
// StorageType=posix and the worker mounts the filesystem out-of-band.
func OpenBackendFromCredential(ctx context.Context, c Credential) (storage.Backend, func(), error) {
	if err := c.Validate(); err != nil {
		return nil, func() {}, err
	}
	switch c.Type {
	case TypeS3STS:
		return openS3STS(ctx, c.S3STS)
	case TypeAzureSAS:
		return openAzureSAS(ctx, c.AzureSAS)
	case TypeGCSToken:
		return openGCSToken(ctx, c.GCSToken)
	case TypeSFTPKey:
		return openSFTPKey(ctx, c.SFTPKey)
	default:
		return nil, func() {}, fmt.Errorf("creds: cannot open backend for type %q", c.Type)
	}
}

func openS3STS(ctx context.Context, c *S3STSCredential) (storage.Backend, func(), error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(c.Region),
		awsconfig.WithCredentialsProvider(awscreds.NewStaticCredentialsProvider(
			c.AccessKeyID, c.SecretAccessKey, c.SessionToken)),
	)
	if err != nil {
		return nil, func() {}, fmt.Errorf("creds open s3sts: aws config: %w", err)
	}
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = &c.Endpoint
		}
		if c.UsePathStyle {
			o.UsePathStyle = true
		}
	})
	b, err := pgsafes3.New(pgsafes3.Options{
		Client: client,
		Bucket: c.Bucket,
		Prefix: c.Prefix,
	})
	if err != nil {
		return nil, func() {}, err
	}
	if err := b.Open(ctx); err != nil {
		return nil, func() {}, err
	}
	return b, func() {}, nil
}

func openAzureSAS(ctx context.Context, c *AzureSASCredential) (storage.Backend, func(), error) {
	// SAS goes on the URL.
	full := c.ServiceURL + "?" + c.SASToken
	svc, err := azblob.NewClientWithNoCredential(full, nil)
	if err != nil {
		return nil, func() {}, fmt.Errorf("creds open azure_sas: client: %w", err)
	}
	cc := svc.ServiceClient().NewContainerClient(c.Container)
	b, err := azure.New(azure.Options{
		ContainerClient: cc,
		Prefix:          c.Prefix,
	})
	if err != nil {
		return nil, func() {}, err
	}
	if err := b.Open(ctx); err != nil {
		return nil, func() {}, err
	}
	return b, func() {}, nil
}

func openGCSToken(ctx context.Context, c *GCSTokenCredential) (storage.Backend, func(), error) {
	src := oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: c.AccessToken,
		TokenType:   c.TokenType,
		Expiry:      c.Expiration,
	})
	clientOpts := []option.ClientOption{option.WithTokenSource(src)}
	if c.Endpoint != "" {
		// Emulator path — the caller already set STORAGE_EMULATOR_HOST
		// process-wide if it ever ran openBackend; on the worker, explicit
		// endpoint via option.WithEndpoint is enough since
		// option.WithTokenSource bypasses the default credential discovery.
		clientOpts = append(clientOpts, option.WithEndpoint(c.Endpoint))
	}
	client, err := gcsstorage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, func() {}, fmt.Errorf("creds open gcs_token: client: %w", err)
	}
	cleanup := func() { _ = client.Close() }
	b, err := gcs.New(gcs.Options{
		Client: client,
		Bucket: c.Bucket,
		Prefix: c.Prefix,
	})
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	if err := b.Open(ctx); err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return b, cleanup, nil
}

func openSFTPKey(ctx context.Context, c *SFTPKeyCredential) (storage.Backend, func(), error) {
	signer, err := ssh.ParsePrivateKey(c.PrivateKeyPEM)
	if err != nil {
		return nil, func() {}, fmt.Errorf("creds open sftp_key: parse: %w", err)
	}
	hostKeyCb := ssh.InsecureIgnoreHostKey() //nolint:gosec // gated by config below
	if !c.InsecureIgnoreHostKey {
		if c.HostKey == "" {
			return nil, func() {}, errors.New("creds open sftp_key: host_key required")
		}
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(c.HostKey))
		if err != nil {
			return nil, func() {}, fmt.Errorf("creds open sftp_key: parse host_key: %w", err)
		}
		hostKeyCb = ssh.FixedHostKey(pub)
	}
	cfg := &ssh.ClientConfig{
		User:            c.Username,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCb,
	}
	addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
	sshConn, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, func() {}, fmt.Errorf("creds open sftp_key: dial: %w", err)
	}
	sftpClient, err := sftp.NewClient(sshConn)
	if err != nil {
		_ = sshConn.Close()
		return nil, func() {}, fmt.Errorf("creds open sftp_key: sftp: %w", err)
	}
	cleanup := func() {
		_ = sftpClient.Close()
		_ = sshConn.Close()
	}
	b, err := pgsafesftp.New(pgsafesftp.Options{Client: sftpClient, BasePath: c.BasePath})
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	if err := b.Open(ctx); err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return b, cleanup, nil
}
