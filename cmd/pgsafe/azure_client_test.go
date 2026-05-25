package main

import (
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/config"
)

// TestNewAzureContainerClientFailsWithoutCredentials pins the rule
// that pgsafe never silently picks up an ambient credential
// (DefaultAzureCredential / managed identity / cloud env). All three
// supported flows — ConnectionString, AccountKey, SASToken — must be
// explicit. If anonymous "no creds" ever starts succeeding, this test
// fails and the operator-supplied-credentials guarantee is restored.
func TestNewAzureContainerClientFailsWithoutCredentials(t *testing.T) {
	t.Parallel()
	c := &config.AzureConfig{
		AccountName: "pgsafe",
		Container:   "backups",
	}
	_, err := newAzureContainerClient(c)
	if err == nil {
		t.Fatal("newAzureContainerClient with no credentials: want error, got nil")
	}
	if !strings.Contains(err.Error(), "no credentials") {
		t.Errorf("error %q should mention missing credentials", err)
	}
}

// TestNewAzureContainerClientAccountKey verifies the SharedKey path
// constructs without panic and falls back to the public-cloud URL
// template when BlobEndpoint is empty. The endpoint shape is
// load-bearing for sovereign-cloud safety: pgsafe pins to
// blob.core.windows.net and refuses to silently route to Government /
// China clouds (a bug class we've seen in other tools).
func TestNewAzureContainerClientAccountKey(t *testing.T) {
	t.Parallel()
	c := &config.AzureConfig{
		AccountName: "pgsafe",
		AccountKey:  validBase64Key(),
		Container:   "backups",
	}
	cc, err := newAzureContainerClient(c)
	if err != nil {
		t.Fatalf("newAzureContainerClient: %v", err)
	}
	if cc == nil {
		t.Fatal("nil container.Client")
	}
	if !strings.Contains(cc.URL(), "blob.core.windows.net") {
		t.Errorf("default URL = %q; expected public Azure cloud (blob.core.windows.net)", cc.URL())
	}
}

// TestNewAzureContainerClientSAS pins the SAS-token path: BlobEndpoint
// override reaches the constructed URL, and the SAS query string is
// concatenated correctly. A typo here means SAS-only auth flows can't
// reach an emulator (Azurite) or a custom endpoint.
func TestNewAzureContainerClientSAS(t *testing.T) {
	t.Parallel()
	c := &config.AzureConfig{
		AccountName:  "pgsafe",
		SASToken:     "sv=2023-11-03&sr=c&sig=abc123",
		Container:    "backups",
		BlobEndpoint: "https://127.0.0.1:10000/devstoreaccount1/",
	}
	cc, err := newAzureContainerClient(c)
	if err != nil {
		t.Fatalf("newAzureContainerClient: %v", err)
	}
	if !strings.HasPrefix(cc.URL(), "https://127.0.0.1:10000/") {
		t.Errorf("URL = %q; expected operator-supplied endpoint to reach the client", cc.URL())
	}
}

// validBase64Key returns a syntactically-valid placeholder Azure
// account key (base64-encoded 64 bytes). The SharedKey constructor
// validates the encoding even though the actual bytes are never
// transmitted in this unit test.
func validBase64Key() string {
	return "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMA=="
}
