package main

import (
	"os"
	"testing"

	"github.com/vyruss/pgsafe/internal/config"
)

// TestNewGCSClientOptionsNoAuthOnlyForEmulator pins the gate that
// keeps pgsafe from silently switching to anonymous-write against
// real GCS. option.WithoutAuthentication is appended ONLY when the
// operator supplied an Endpoint (emulator mode); production configs
// leave it off and rely on the SDK's default credential chain. A
// regression here means pgsafe might happily write to a public
// bucket without auth — fail loud instead of soft.
func TestNewGCSClientOptionsNoAuthOnlyForEmulator(t *testing.T) {
	t.Parallel()
	emulator := &config.GCSConfig{
		Bucket:   "pgsafe-test",
		Endpoint: "http://127.0.0.1:4443",
	}
	if got := len(newGCSClientOptions(emulator)); got != 1 {
		t.Errorf("emulator opts len = %d; want 1 (WithoutAuthentication)", got)
	}

	prod := &config.GCSConfig{Bucket: "pgsafe-prod"}
	if got := len(newGCSClientOptions(prod)); got != 0 {
		t.Errorf("prod opts len = %d; want 0 (no WithoutAuthentication, no WithCredentialsFile)", got)
	}
}

// TestNewGCSClientOptionsSetsEmulatorEnv pins the
// STORAGE_EMULATOR_HOST env var, which some SDK code paths consult
// directly. Without it, calls can leak through to www.googleapis.com
// even with option.WithEndpoint in place — surfacing as confusing
// auth/404 errors instead of "the emulator config didn't take."
func TestNewGCSClientOptionsSetsEmulatorEnv(t *testing.T) {
	_ = os.Unsetenv("STORAGE_EMULATOR_HOST")
	c := &config.GCSConfig{
		Bucket:   "pgsafe-test",
		Endpoint: "http://127.0.0.1:4443",
	}
	_ = newGCSClientOptions(c)
	if got := os.Getenv("STORAGE_EMULATOR_HOST"); got != "127.0.0.1:4443" {
		t.Errorf("STORAGE_EMULATOR_HOST = %q; want %q (scheme stripped)", got, "127.0.0.1:4443")
	}
	_ = os.Unsetenv("STORAGE_EMULATOR_HOST")
}

// TestNewGCSClientOptionsCredentialsFileFlag pins that
// operator-supplied CredentialsFile becomes a single client option,
// with no other pgsafe-injected behavior changes. Regression:
// silently dropping the credentials file would route auth through
// ADC (which usually fails with a non-obvious error), or silently
// adding extra options would shadow operator intent.
func TestNewGCSClientOptionsCredentialsFileFlag(t *testing.T) {
	t.Parallel()
	c := &config.GCSConfig{
		Bucket:          "pgsafe-prod",
		CredentialsFile: "/etc/pgsafe/gcs.json",
	}
	if got := len(newGCSClientOptions(c)); got != 1 {
		t.Errorf("opts len = %d; want 1 (just WithCredentialsFile)", got)
	}
}
