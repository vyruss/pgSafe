package creds_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/transport/creds"
	"golang.org/x/crypto/ssh"
)

// TestMarshalRoundTrip exercises every variant of the discriminated union
// Credential type. Each marshal+unmarshal pair must preserve identity.
func TestMarshalRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []creds.Credential{
		{Type: creds.TypeNone},
		{
			Type: creds.TypeS3STS,
			S3STS: &creds.S3STSCredential{
				AccessKeyID:     "AKIA-test",
				SecretAccessKey: "secret",
				SessionToken:    "session",
				Expiration:      time.Now().UTC().Truncate(time.Second).Add(time.Hour),
				Region:          "us-east-1",
				Bucket:          "demo",
				Prefix:          "p",
			},
		},
		{
			Type: creds.TypeAzureSAS,
			AzureSAS: &creds.AzureSASCredential{
				AccountName: "demo",
				Container:   "backups",
				SASToken:    "sv=2023&se=...",
				ServiceURL:  "https://demo.blob.core.windows.net/",
				Expiration:  time.Now().UTC().Truncate(time.Second).Add(time.Hour),
			},
		},
		{
			Type: creds.TypeGCSToken,
			GCSToken: &creds.GCSTokenCredential{
				AccessToken: "ya29.a0",
				TokenType:   "Bearer",
				Expiration:  time.Now().UTC().Truncate(time.Second).Add(time.Hour),
				Bucket:      "demo",
			},
		},
		{
			Type: creds.TypeSFTPKey,
			SFTPKey: &creds.SFTPKeyCredential{
				Host:                  "backup.example.com",
				Port:                  22,
				Username:              "pgsafe",
				PrivateKeyPEM:         []byte("-----BEGIN OPENSSH PRIVATE KEY-----\n..."),
				BasePath:              "/srv/pgsafe",
				InsecureIgnoreHostKey: true,
			},
		},
	}
	for _, want := range cases {
		want := want
		t.Run(string(want.Type), func(t *testing.T) {
			t.Parallel()
			b, err := want.Marshal()
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			got, err := creds.Unmarshal(b)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if got.Type != want.Type {
				t.Errorf("Type = %q, want %q", got.Type, want.Type)
			}
			if !sameCred(got, want) {
				t.Errorf("payload mismatch:\nwant: %+v\ngot:  %+v", want, got)
			}
		})
	}
}

func sameCred(a, b creds.Credential) bool {
	switch a.Type {
	case creds.TypeNone:
		return b.Type == creds.TypeNone
	case creds.TypeS3STS:
		return a.S3STS != nil && b.S3STS != nil &&
			a.S3STS.AccessKeyID == b.S3STS.AccessKeyID &&
			a.S3STS.Bucket == b.S3STS.Bucket
	case creds.TypeAzureSAS:
		return a.AzureSAS != nil && b.AzureSAS != nil &&
			a.AzureSAS.SASToken == b.AzureSAS.SASToken
	case creds.TypeGCSToken:
		return a.GCSToken != nil && b.GCSToken != nil &&
			a.GCSToken.AccessToken == b.GCSToken.AccessToken
	case creds.TypeSFTPKey:
		return a.SFTPKey != nil && b.SFTPKey != nil &&
			a.SFTPKey.Username == b.SFTPKey.Username &&
			string(a.SFTPKey.PrivateKeyPEM) == string(b.SFTPKey.PrivateKeyPEM)
	}
	return false
}

// TestValidateRejectsMismatchedVariant confirms a type tag without its
// matching payload errors. Guards against forged frames where a malicious
// peer sets Type=s3sts but provides azure_sas bytes.
func TestValidateRejectsMismatchedVariant(t *testing.T) {
	t.Parallel()
	c := creds.Credential{Type: creds.TypeS3STS} // no S3STS payload
	if err := c.Validate(); err == nil {
		t.Fatal("Validate: want error for missing payload")
	}
	c2 := creds.Credential{Type: "bogus"}
	if err := c2.Validate(); err == nil {
		t.Fatal("Validate: want error for unknown type")
	}
}

// TestMintSFTPKeyReadsKeyFile exercises the trivial case: read PEM bytes
// from disk on the backup host, ship them in a Credential, parse on the
// other side. No emulator needed; this is the load-bearing behavior for
// the SFTP path.
func TestMintSFTPKeyReadsKeyFile(t *testing.T) {
	t.Parallel()

	// Generate an ed25519 key, write it as OpenSSH PEM to a tempfile.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	_ = pub
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("MarshalPrivateKey: %v", err)
	}
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "id_ed25519")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	cred, err := creds.MintSFTPKey(&config.SFTPConfig{
		Host:                  "sftp.example.com",
		Port:                  2222,
		Username:              "pgsafe",
		PrivateKeyFile:        keyPath,
		BasePath:              "/srv/pgsafe",
		InsecureIgnoreHostKey: true,
	})
	if err != nil {
		t.Fatalf("MintSFTPKey: %v", err)
	}
	if cred.Type != creds.TypeSFTPKey {
		t.Errorf("Type = %q, want %q", cred.Type, creds.TypeSFTPKey)
	}
	if cred.SFTPKey.Username != "pgsafe" {
		t.Errorf("Username = %q", cred.SFTPKey.Username)
	}
	if !strings.Contains(string(cred.SFTPKey.PrivateKeyPEM), "PRIVATE KEY") {
		t.Errorf("PrivateKeyPEM does not look like a PEM block:\n%s", cred.SFTPKey.PrivateKeyPEM)
	}
	// Verify we can re-parse it (worker side).
	if _, err := ssh.ParsePrivateKey(cred.SFTPKey.PrivateKeyPEM); err != nil {
		t.Errorf("worker-side ParsePrivateKey: %v", err)
	}
}

// TestMintSFTPKeyRejectsPasswordOnly confirms v1 refuses password
// auth: Tenet 3 mandates keys (not transmittable secrets we can't
// scope/expire).
func TestMintSFTPKeyRejectsPasswordOnly(t *testing.T) {
	t.Parallel()
	_, err := creds.MintSFTPKey(&config.SFTPConfig{
		Host:     "sftp.example.com",
		Username: "pgsafe",
		Password: "hunter2",
		BasePath: "/srv/pgsafe",
	})
	if err == nil {
		t.Fatal("MintSFTPKey: want error for password-only config, got nil")
	}
}

// TestMintAzureSASRequiresAccountKey is a sanity test for the v1
// surface
func TestMintAzureSASRequiresAccountKey(t *testing.T) {
	t.Parallel()
	_, err := creds.MintAzureSAS(t.Context(), &config.AzureConfig{
		AccountName: "demo",
		Container:   "backups",
	}, time.Hour)
	if err == nil {
		t.Fatal("MintAzureSAS: want error when AccountKey is empty")
	}
}

// _ catches unused-import warnings in case errors becomes the only
// errors.Is target removed.
var _ = errors.Is
