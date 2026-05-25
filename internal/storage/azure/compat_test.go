package azure_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/vyruss/pgsafe/internal/storage/azure"
)

// TestAzurePutOmitsCustomEncryptionAndTier sends a Put through the
// real Azure SDK against an httptest server and asserts the request
// does NOT carry the AWS-only-style protocol extensions that Azure
// also has (customer-provided encryption keys, per-blob access tier
// override, server-side encryption scope). pgsafe defaults to
// account-level encryption + account-default tier; if the SDK ever
// flips a default to send these per-request, this test catches it.
//
// Bad headers we look for:
//
//   - x-ms-encryption-key              — customer-provided key
//   - x-ms-encryption-key-sha256       — customer-provided key digest
//   - x-ms-encryption-algorithm        — custom encryption negotiation
//   - x-ms-encryption-scope            — encryption scope override
//   - x-ms-access-tier                 — Hot/Cool/Archive override
//   - x-ms-immutability-policy-mode    — WORM lock override
func TestAzurePutOmitsCustomEncryptionAndTier(t *testing.T) {
	t.Parallel()
	stub := newAzureCaptureStub(t)
	defer stub.Close()

	cc := newAzureCompatTestClient(t, stub.URL)
	b, err := azure.New(azure.Options{ContainerClient: cc})
	if err != nil {
		t.Fatalf("azure.New: %v", err)
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
		"X-Ms-Encryption-Key",
		"X-Ms-Encryption-Key-Sha256",
		"X-Ms-Encryption-Algorithm",
		"X-Ms-Encryption-Scope",
		"X-Ms-Access-Tier",
		"X-Ms-Immutability-Policy-Mode",
	}
	for _, req := range stub.requests() {
		for _, h := range bad {
			if v := req.Header.Get(h); v != "" {
				t.Errorf("request %s %s carried %s=%q (custom-encryption / tier-override; pgsafe relies on account defaults)",
					req.Method, req.URL.Path, h, v)
			}
		}
	}
}

type azureCaptureStub struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []*http.Request
}

func newAzureCaptureStub(t *testing.T) *azureCaptureStub {
	t.Helper()
	s := &azureCaptureStub{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))
		s.mu.Lock()
		s.reqs = append(s.reqs, r)
		s.mu.Unlock()
		// PutBlock / PutBlockList: 201 Created.
		// PutBlob (single-shot): 201 Created.
		// HeadBlob / GetBlob / GetProperties: 404 to short-circuit
		// existence checks but allow the upload chain to proceed.
		switch r.Method {
		case http.MethodPut:
			w.Header().Set("ETag", `"e0"`)
			w.WriteHeader(http.StatusCreated)
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	return s
}

func (s *azureCaptureStub) requests() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*http.Request, len(s.reqs))
	copy(out, s.reqs)
	return out
}

// newAzureCompatTestClient builds a container.Client wired at the
// stub. Uses NoCredential auth (the SAS-token path with an
// inline-stub URL) so requests are signed-via-URL rather than
// requiring real key material.
func newAzureCompatTestClient(t *testing.T, endpoint string) *container.Client {
	t.Helper()
	// Trim any trailing slash so the SAS suffix concatenation matches
	// the production code shape.
	endpoint = strings.TrimRight(endpoint, "/")
	full := endpoint + "/?fakesas=1"
	svc, err := azblob.NewClientWithNoCredential(full, &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{},
	})
	if err != nil {
		t.Fatalf("NewClientWithNoCredential: %v", err)
	}
	return svc.ServiceClient().NewContainerClient("pgsafe-compat")
}
