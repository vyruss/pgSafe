package main

import (
	"context"
	"fmt"
	"os"

	gcsstorage "cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/vyruss/pgsafe/internal/config"
)

// newGCSClient builds the GCS storage.Client pgsafe writes to, plus a
// cleanup. Extracted from openGCS so its compat-critical option choices
// can be unit-tested independently of credential loading and backend
// wiring. Returns the option list separately so tests can assert which
// flags pgsafe selected for the client.
//
// Compat-critical defaults:
//
//   - WithCredentialsFile is set ONLY when the operator gave one.
//     No silent fallback to ADC (Application Default Credentials)
//     unless they're already in the environment — that's google-cloud-go's
//     own default, not pgsafe's; we just don't add anything to override.
//   - WithoutAuthentication is set ONLY when an operator-supplied
//     Endpoint is present (emulator mode, e.g. fake-gcs-server). On
//     real GCS this would silently switch to anonymous-write, which
//     fails on any private bucket. The Endpoint check is the gate.
//   - STORAGE_EMULATOR_HOST env var: required for emulator mode
//     because some SDK code paths route through it directly,
//     bypassing option.WithEndpoint.
//   - No KMSKeyName, no per-chunk SendCRC32C/SendMD5: those are
//     per-Writer options in internal/storage/gcs and are deliberately
//     NOT set there. Per-write ChunkSize=0 (single-shot) is the
//     conservative default that works against fake-gcs-server.
func newGCSClient(ctx context.Context, c *config.GCSConfig) (*gcsstorage.Client, []option.ClientOption, func(), error) {
	clientOpts := newGCSClientOptions(c)
	client, err := gcsstorage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("storage gcs: client: %w", err)
	}
	cleanup := func() { _ = client.Close() }
	return client, clientOpts, cleanup, nil
}

// newGCSClientOptions returns just the SDK option list pgsafe will
// pass to gcsstorage.NewClient. Split out from newGCSClient so the
// option-shape decisions (which is what the compat-defaults audit
// cares about) can be unit-tested without the SDK trying to open
// credential files or dial endpoints.
//
// Side effect: emulator mode (Endpoint set) writes
// STORAGE_EMULATOR_HOST. The SDK consults this env var from internal
// code paths option.WithEndpoint can't reach, so the env mutation is
// load-bearing for emulator routing.
func newGCSClientOptions(c *config.GCSConfig) []option.ClientOption {
	clientOpts := []option.ClientOption{}
	if c.CredentialsFile != "" {
		clientOpts = append(clientOpts, option.WithCredentialsFile(c.CredentialsFile)) //nolint:staticcheck
	}
	if c.Endpoint != "" {
		_ = os.Setenv("STORAGE_EMULATOR_HOST", stripScheme(c.Endpoint))
		clientOpts = append(clientOpts, option.WithoutAuthentication())
	}
	return clientOpts
}
