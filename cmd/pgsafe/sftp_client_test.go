package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/vyruss/pgsafe/internal/config"
)

// TestNewSSHClientConfigRefusesNoHostKey pins the
// strict-host-key-by-default policy. Without an explicit
// insecure_ignore_host_key, an SFTP config that forgets host_key
// must refuse to construct — never silently accept a TOFU connection
// that would expose backups to MITM. If this regresses, an operator
// who copies a partial YAML loses the host-key check silently.
func TestNewSSHClientConfigRefusesNoHostKey(t *testing.T) {
	t.Parallel()
	c := &config.SFTPConfig{
		Host:           "backups.example.com",
		Username:       "pgsafe",
		PrivateKeyFile: stagePrivateKey(t),
		// HostKey deliberately empty.
	}
	_, err := newSSHClientConfig(c)
	if err == nil {
		t.Fatal("missing host_key with insecure flag unset: want error, got nil")
	}
	if !strings.Contains(err.Error(), "host_key") {
		t.Errorf("error %q should mention host_key", err)
	}
}

// TestNewSSHClientConfigInsecureExplicit pins that the insecure
// fallback works only when explicitly opted in. Pgsafe never falls
// back to InsecureIgnoreHostKey on its own initiative.
func TestNewSSHClientConfigInsecureExplicit(t *testing.T) {
	t.Parallel()
	c := &config.SFTPConfig{
		Host:                  "backups.example.com",
		Username:              "pgsafe",
		PrivateKeyFile:        stagePrivateKey(t),
		InsecureIgnoreHostKey: true,
	}
	cfg, err := newSSHClientConfig(c)
	if err != nil {
		t.Fatalf("newSSHClientConfig: %v", err)
	}
	if cfg.HostKeyCallback == nil {
		t.Fatal("HostKeyCallback unset")
	}
	// We can't compare callbacks for equality, but we CAN check that
	// the callback accepts an arbitrary key — which is the defining
	// behavior of InsecureIgnoreHostKey.
	if err := cfg.HostKeyCallback("any:22", nil, nil); err != nil {
		t.Errorf("InsecureIgnoreHostKey should accept any key; got %v", err)
	}
}

// TestNewSSHClientConfigRefusesNoAuth pins the no-auth-method failure
// mode. SSH library would otherwise silently dial with `none` auth,
// which most servers reject AND surfaces as a confusing error far from
// config-load. Fail at config-load instead.
func TestNewSSHClientConfigRefusesNoAuth(t *testing.T) {
	t.Parallel()
	c := &config.SFTPConfig{
		Host:                  "backups.example.com",
		Username:              "pgsafe",
		HostKey:               testHostKey(),
		InsecureIgnoreHostKey: false,
		// No password, no PrivateKeyFile.
	}
	_, err := newSSHClientConfig(c)
	if err == nil {
		t.Fatal("no auth method: want error, got nil")
	}
	if !strings.Contains(err.Error(), "auth") {
		t.Errorf("error %q should mention missing auth", err)
	}
}

// TestNewSSHClientConfigParsesHostKey verifies that an
// authorized_keys-format host_key reaches the FixedHostKey callback.
// Catches a typo where the parsed key isn't actually attached to the
// callback (which would silently fall back to Insecure semantics).
func TestNewSSHClientConfigParsesHostKey(t *testing.T) {
	t.Parallel()
	c := &config.SFTPConfig{
		Host:           "backups.example.com",
		Username:       "pgsafe",
		PrivateKeyFile: stagePrivateKey(t),
		HostKey:        testHostKey(),
	}
	cfg, err := newSSHClientConfig(c)
	if err != nil {
		t.Fatalf("newSSHClientConfig: %v", err)
	}
	if cfg.HostKeyCallback == nil {
		t.Fatal("HostKeyCallback unset")
	}
	// Invoke the callback with a DIFFERENT key than the configured
	// one. FixedHostKey must reject (mismatch); InsecureIgnoreHostKey
	// would accept. This is the load-bearing assertion that the
	// strict path actually attaches the operator's key.
	otherPub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(otherHostKey()))
	if err != nil {
		t.Fatalf("parse comparison key: %v", err)
	}
	if err := cfg.HostKeyCallback("any:22", nil, otherPub); err == nil {
		t.Error("FixedHostKey accepted a different host key — InsecureIgnoreHostKey may have leaked through")
	}
}

// stagePrivateKey writes a freshly-generated ed25519 PEM into a
// tempfile and returns its path. Used by tests that need a parseable
// private_key_file without committing one to the repo.
func stagePrivateKey(t *testing.T) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	pemBytes, err := ssh.MarshalPrivateKey(priv, "pgsafe-test")
	if err != nil {
		t.Fatalf("ssh.MarshalPrivateKey: %v", err)
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "id_ed25519")
	if err := os.WriteFile(p, pem.EncodeToMemory(pemBytes), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return p
}

// testHostKey returns the authorized_keys form of a fresh ed25519
// public key. Each call yields a different key — call it twice when a
// test needs a "configured key vs. a different key" mismatch.
func testHostKey() string { return freshHostKey() }

// otherHostKey is a second freshly-generated host key, different from
// testHostKey()'s by construction (rand bytes don't repeat).
func otherHostKey() string { return freshHostKey() }

func freshHostKey() string {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		panic(err)
	}
	return string(ssh.MarshalAuthorizedKey(sshPub))
}
