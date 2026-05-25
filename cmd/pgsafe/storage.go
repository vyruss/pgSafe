package main

// openBackend dispatches the cluster's storage configuration to the right
// driver under internal/storage/{posix,s3,azure,gcs,sftp}, builds its SDK
// client from operator-supplied credentials, and returns the opened
// storage.Backend plus a cleanup the caller MUST defer.
//
// Single CLI binary today; if archive-push daemon
// retention worker) ends up needing the same dispatch, promote this back
// to a shared internal/storage package.

import (
	"context"
	"fmt"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/storage/azure"
	"github.com/vyruss/pgsafe/internal/storage/gcs"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/storage/s3"
	pgsafesftp "github.com/vyruss/pgsafe/internal/storage/sftp"
)

func openBackend(ctx context.Context, cfg config.StorageConfig) (storage.Backend, func(), error) {
	switch cfg.Type {
	case "posix":
		return openPosix(ctx, cfg)
	case "s3":
		return openS3(ctx, cfg)
	case "azure":
		return openAzure(ctx, cfg)
	case "gcs":
		return openGCS(ctx, cfg)
	case "sftp":
		return openSFTP(ctx, cfg)
	default:
		return nil, func() {}, fmt.Errorf("storage: unknown type %q", cfg.Type)
	}
}

// openBackends opens all storages in cfg.Storages and returns the ordered
// slice of backends plus a single combined cleanup. Used by the backup command
// to populate backup.Options.Backends for multi-storage fan-out (Invariant #10).
// All backends must open successfully; a failure on any one returns an error
// and cleans up the backends opened so far.
func openBackends(ctx context.Context, cfgs []config.StorageConfig) ([]storage.Backend, func(), error) {
	backends := make([]storage.Backend, 0, len(cfgs))
	cleanups := make([]func(), 0, len(cfgs))
	combined := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	for _, cfg := range cfgs {
		b, cleanup, err := openBackend(ctx, cfg)
		if err != nil {
			combined()
			return nil, func() {}, fmt.Errorf("storage: open %q backend: %w", cfg.Type, err)
		}
		backends = append(backends, b)
		cleanups = append(cleanups, cleanup)
	}
	return backends, combined, nil
}

func openPosix(ctx context.Context, cfg config.StorageConfig) (storage.Backend, func(), error) {
	b, err := posix.New(posix.Options{Root: cfg.Path})
	if err != nil {
		return nil, func() {}, err
	}
	if err := b.Open(ctx); err != nil {
		return nil, func() {}, err
	}
	return b, func() {}, nil
}

func openS3(ctx context.Context, cfg config.StorageConfig) (storage.Backend, func(), error) {
	c := cfg.S3
	client, err := newS3Client(ctx, c)
	if err != nil {
		return nil, func() {}, err
	}
	b, err := s3.New(s3.Options{
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

func openAzure(ctx context.Context, cfg config.StorageConfig) (storage.Backend, func(), error) {
	c := cfg.Azure
	cc, err := newAzureContainerClient(c)
	if err != nil {
		return nil, func() {}, fmt.Errorf("storage azure: %w", err)
	}
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

func openGCS(ctx context.Context, cfg config.StorageConfig) (storage.Backend, func(), error) {
	c := cfg.GCS
	client, _, cleanup, err := newGCSClient(ctx, c)
	if err != nil {
		return nil, func() {}, err
	}
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

func openSFTP(ctx context.Context, cfg config.StorageConfig) (storage.Backend, func(), error) {
	c := cfg.SFTP
	port := c.Port
	if port == 0 {
		port = 22
	}
	sshCfg, err := newSSHClientConfig(c)
	if err != nil {
		return nil, func() {}, err
	}
	addr := fmt.Sprintf("%s:%d", c.Host, port)
	sshConn, err := ssh.Dial("tcp", addr, sshCfg)
	if err != nil {
		return nil, func() {}, fmt.Errorf("storage sftp: dial: %w", err)
	}
	sftpClient, err := sftp.NewClient(sshConn)
	if err != nil {
		_ = sshConn.Close()
		return nil, func() {}, fmt.Errorf("storage sftp: sftp.NewClient: %w", err)
	}
	cleanup := func() {
		_ = sftpClient.Close()
		_ = sshConn.Close()
	}
	b, err := pgsafesftp.New(pgsafesftp.Options{
		Client:   sftpClient,
		BasePath: c.BasePath,
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

// stripScheme strips http(s):// from s, returning host:port. Used by openGCS
// to set STORAGE_EMULATOR_HOST. Hand-rolled to avoid pulling in net/url for
// just this one call site.
func stripScheme(s string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(s) > len(p) && s[:len(p)] == p {
			return s[len(p):]
		}
	}
	return s
}
