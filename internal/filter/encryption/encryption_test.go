package encryption_test

import (
	"bytes"
	"io"
	"testing"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/filter/encryption"
)

func mustIdentity(t *testing.T) *age.X25519Identity {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("GenerateX25519Identity: %v", err)
	}
	return id
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)

	var sink bytes.Buffer
	wc, err := encryption.NewWriter(&sink, []age.Recipient{id.Recipient()})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	payload := []byte("the quick brown fox\n")
	if _, err := wc.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := encryption.NewReader(&sink, []age.Identity{id})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Errorf("round-trip mismatch: got %q, want %q", out, payload)
	}
}

func TestMultipleRecipients(t *testing.T) {
	t.Parallel()
	id1 := mustIdentity(t)
	id2 := mustIdentity(t)

	var sink bytes.Buffer
	wc, err := encryption.NewWriter(&sink, []age.Recipient{id1.Recipient(), id2.Recipient()})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := wc.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Either identity decrypts.
	for i, id := range []*age.X25519Identity{id1, id2} {
		t.Run([]string{"id1", "id2"}[i], func(t *testing.T) {
			cipher := bytes.NewReader(sink.Bytes())
			r, err := encryption.NewReader(cipher, []age.Identity{id})
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			out, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if string(out) != "payload" {
				t.Errorf("decrypt result %q, want %q", out, "payload")
			}
		})
	}
}

func TestNoRecipients(t *testing.T) {
	t.Parallel()
	_, err := encryption.NewWriter(&bytes.Buffer{}, nil)
	if err == nil {
		t.Fatal("NewWriter with no recipients: want error")
	}
}

func TestWrongIdentity(t *testing.T) {
	t.Parallel()
	id1 := mustIdentity(t)
	wrong := mustIdentity(t)

	var sink bytes.Buffer
	wc, _ := encryption.NewWriter(&sink, []age.Recipient{id1.Recipient()})
	_, _ = wc.Write([]byte("secret"))
	_ = wc.Close()

	_, err := encryption.NewReader(&sink, []age.Identity{wrong})
	if err == nil {
		t.Fatal("NewReader with wrong identity: want error")
	}
}

func TestEmptyPayload(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)

	var sink bytes.Buffer
	wc, _ := encryption.NewWriter(&sink, []age.Recipient{id.Recipient()})
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := encryption.NewReader(&sink, []age.Identity{id})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("empty payload round-trip: got %q, want empty", out)
	}
}
