package gcs_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	gcsstorage "cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/vyruss/pgsafe/internal/storage/gcs"
)

// TestGCSPutOmitsCustomEncryptionAndCMEK sends a Put through the
// real GCS SDK against an httptest server (with STORAGE_EMULATOR_HOST
// pointing at it) and asserts the request does NOT carry the
// AWS-only-style protocol extensions GCS supports (customer-supplied
// encryption keys, KMS keys via CMEK, per-chunk hash assertions).
// pgsafe relies on bucket-default encryption + leaves CMEK to the
// operator's bucket policy; if the SDK ever flips a default to
// send these per-request, this test catches it.
//
// Bad headers we look for:
//
//   - x-goog-encryption-key                  — customer-supplied key
//   - x-goog-encryption-key-sha256           — customer-supplied key digest
//   - x-goog-encryption-algorithm            — non-default cipher negotiation
//   - x-goog-encryption-kms-key-name         — CMEK override
//   - x-goog-hash                            — per-chunk MD5/CRC32C
//   - x-goog-content-length-range            — bounded-write override
func TestGCSPutOmitsCustomEncryptionAndCMEK(t *testing.T) {
	stub := newGCSCaptureStub(t)
	defer stub.Close()

	// STORAGE_EMULATOR_HOST is the load-bearing signal — the SDK
	// routes ALL requests through this host when set, with no auth.
	host := stub.Listener.Addr().String()
	t.Setenv("STORAGE_EMULATOR_HOST", host)

	client, err := gcsstorage.NewClient(context.Background(),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	b, err := gcs.New(gcs.Options{
		Client: client,
		Bucket: "pgsafe-compat",
	})
	if err != nil {
		t.Fatalf("gcs.New: %v", err)
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
		"X-Goog-Encryption-Key",
		"X-Goog-Encryption-Key-Sha256",
		"X-Goog-Encryption-Algorithm",
		"X-Goog-Encryption-Kms-Key-Name",
		"X-Goog-Hash",
		"X-Goog-Content-Length-Range",
	}
	for _, req := range stub.requests() {
		for _, h := range bad {
			if v := req.Header.Get(h); v != "" {
				t.Errorf("request %s %s carried %s=%q (custom-encryption / CMEK / per-chunk hash; pgsafe relies on bucket defaults)",
					req.Method, req.URL.Path, h, v)
			}
		}
	}
}

type gcsCaptureStub struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []*http.Request
}

func newGCSCaptureStub(t *testing.T) *gcsCaptureStub {
	t.Helper()
	s := &gcsCaptureStub{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))
		s.mu.Lock()
		s.reqs = append(s.reqs, r)
		s.mu.Unlock()
		// GCS REST: POST /upload/storage/v1/b/<bucket>/o for resumable
		// or single-shot uploads. Reply 200 with a minimal valid JSON
		// envelope so the SDK considers the upload successful.
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"kind":"storage#object","bucket":"pgsafe-compat","name":"%s"}`, r.URL.Path)
	}))
	return s
}

func (s *gcsCaptureStub) requests() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*http.Request, len(s.reqs))
	copy(out, s.reqs)
	return out
}

// Ensure os.Setenv-with-restore semantics — we don't want
// STORAGE_EMULATOR_HOST leaking into other tests.
func init() {
	_ = os.Unsetenv("STORAGE_EMULATOR_HOST")
}
