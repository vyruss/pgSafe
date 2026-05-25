package filter_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/filter/compression"
	"github.com/vyruss/pgsafe/internal/filter/encryption"
)

// nopCloser wraps a bytes.Buffer as an io.WriteCloser whose Close is a no-op.
// The chain calls Close on the sink at the end of its sequence, so we need a
// WriteCloser even for in-memory tests.
type nopCloser struct{ *bytes.Buffer }

func (nopCloser) Close() error { return nil }

func mustIdentity(t *testing.T) *age.X25519Identity {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("GenerateX25519Identity: %v", err)
	}
	return id
}

// roundTrip reverses the chain's filters and returns the recovered plaintext.
func roundTrip(t *testing.T, ct []byte, codec string, id age.Identity) []byte {
	t.Helper()
	dec, err := encryption.NewReader(bytes.NewReader(ct), []age.Identity{id})
	if err != nil {
		t.Fatalf("encryption.NewReader: %v", err)
	}
	c, err := compression.Get(codec)
	if err != nil {
		t.Fatalf("compression.Get: %v", err)
	}
	rd, err := c.NewReader(dec)
	if err != nil {
		t.Fatalf("compression.NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()
	out, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return out
}

func TestChainRoundTripPerCodec(t *testing.T) {
	for _, codec := range []string{"gzip", "lz4", "zstd", "bzip2"} {
		t.Run(codec, func(t *testing.T) {
			t.Parallel()
			id := mustIdentity(t)

			ch, err := filter.NewChain(filter.Options{
				Codec:      codec,
				Recipients: []age.Recipient{id.Recipient()},
			})
			if err != nil {
				t.Fatalf("NewChain: %v", err)
			}

			payload := make([]byte, 64<<10) // 64 KiB
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand: %v", err)
			}

			var sink bytes.Buffer
			wc, res, err := ch.Wrap(nopCloser{&sink})
			if err != nil {
				t.Fatalf("Wrap: %v", err)
			}
			if _, err := wc.Write(payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := wc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Plaintext SHA-256 captured.
			want := sha256.Sum256(payload)
			if res.SHA256 != want {
				t.Errorf("res.SHA256 = %x, want %x", res.SHA256, want)
			}
			// Plaintext byte count.
			if res.Bytes != int64(len(payload)) {
				t.Errorf("res.Bytes = %d, want %d", res.Bytes, len(payload))
			}

			// Round-trip the cipher stream back to plaintext.
			recovered := roundTrip(t, sink.Bytes(), codec, id)
			if !bytes.Equal(recovered, payload) {
				t.Errorf("round-trip plaintext mismatch (len recovered=%d, want=%d)", len(recovered), len(payload))
			}
		})
	}
}

func TestChainEmptyPayload(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)
	ch, _ := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})

	var sink bytes.Buffer
	wc, res, err := ch.Wrap(nopCloser{&sink})
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if res.Bytes != 0 {
		t.Errorf("Bytes for empty input = %d, want 0", res.Bytes)
	}
	want := sha256.Sum256(nil)
	if res.SHA256 != want {
		t.Errorf("SHA256 for empty input = %x, want %x", res.SHA256, want)
	}
	recovered := roundTrip(t, sink.Bytes(), "gzip", id)
	if len(recovered) != 0 {
		t.Errorf("empty round-trip produced %d bytes; want 0", len(recovered))
	}
}

func TestChainCloseTwiceIsNoop(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)
	ch, _ := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})
	wc, _, _ := ch.Wrap(nopCloser{&bytes.Buffer{}})
	if err := wc.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Errorf("second Close should be no-op, got: %v", err)
	}
}

func TestChainWriteAfterCloseErrors(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)
	ch, _ := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})
	wc, _, _ := ch.Wrap(nopCloser{&bytes.Buffer{}})
	_ = wc.Close()
	if _, err := wc.Write([]byte("late")); err == nil {
		t.Fatal("Write after Close: want error")
	}
}

func TestChainRejectsUnknownCodec(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)
	_, err := filter.NewChain(filter.Options{
		Codec:      "brotli",
		Recipients: []age.Recipient{id.Recipient()},
	})
	if err == nil {
		t.Fatal("NewChain unknown codec: want error")
	}
}

func TestChainRejectsNoRecipients(t *testing.T) {
	t.Parallel()
	_, err := filter.NewChain(filter.Options{Codec: "gzip"})
	if err == nil {
		t.Fatal("NewChain no recipients: want error")
	}
}

// TestChainUnwrapRoundTripPerCodec exercises the reverse direction —
// Unwrap should reconstruct plaintext from a Chain-produced ciphertext
// for every codec, populating its Result with the plaintext byte count
// and SHA-256.
func TestChainUnwrapRoundTripPerCodec(t *testing.T) {
	for _, codec := range []string{"gzip", "lz4", "zstd", "bzip2"} {
		t.Run(codec, func(t *testing.T) {
			t.Parallel()
			id := mustIdentity(t)

			ch, err := filter.NewChain(filter.Options{
				Codec:      codec,
				Recipients: []age.Recipient{id.Recipient()},
			})
			if err != nil {
				t.Fatalf("NewChain: %v", err)
			}

			payload := make([]byte, 64<<10)
			if _, err := rand.Read(payload); err != nil {
				t.Fatalf("rand: %v", err)
			}

			var ct bytes.Buffer
			wc, _, err := ch.Wrap(nopCloser{&ct})
			if err != nil {
				t.Fatalf("Wrap: %v", err)
			}
			if _, err := wc.Write(payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := wc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			rd, res, err := filter.Unwrap(codec, &ct, []age.Identity{id})
			if err != nil {
				t.Fatalf("Unwrap: %v", err)
			}
			defer func() { _ = rd.Close() }()

			recovered, err := io.ReadAll(rd)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if !bytes.Equal(recovered, payload) {
				t.Errorf("plaintext mismatch (recovered=%d, want=%d)", len(recovered), len(payload))
			}
			want := sha256.Sum256(payload)
			if res.SHA256 != want {
				t.Errorf("res.SHA256 = %x, want %x", res.SHA256, want)
			}
			if res.Bytes != int64(len(payload)) {
				t.Errorf("res.Bytes = %d, want %d", res.Bytes, len(payload))
			}
		})
	}
}

// TestChainUnwrapWrongIdentityErrors confirms an identity that wasn't
// in the original recipient list fails decryption rather than yielding
// silent garbage.
func TestChainUnwrapWrongIdentityErrors(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)
	other := mustIdentity(t)

	ch, _ := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})

	var ct bytes.Buffer
	wc, _, _ := ch.Wrap(nopCloser{&ct})
	_, _ = wc.Write([]byte("payload"))
	_ = wc.Close()

	if _, _, err := filter.Unwrap("gzip", &ct, []age.Identity{other}); err == nil {
		t.Fatal("Unwrap with wrong identity: expected error, got nil")
	}
}

// TestChainUnwrapRejectsNoIdentities — restore needs at least one age
// identity, same shape as Wrap requires at least one recipient.
func TestChainUnwrapRejectsNoIdentities(t *testing.T) {
	t.Parallel()
	if _, _, err := filter.Unwrap("gzip", bytes.NewReader(nil), nil); err == nil {
		t.Fatal("Unwrap with no identities: expected error, got nil")
	}
}

// TestChainResultRepoSHA pins the Step-4 contract: Result.RepoSHA256
// matches the on-the-wire bytes' SHA-256 (i.e. what hash a restore-time
// reader would see if it just sha256'd the storage file). This is the
// gate the resume reuse-check uses (mirrors pgbackrest's repoFileChecksum
// pattern). If the chain regresses to only computing plaintext SHA, the
// resume worker can't tell a torn-mid-write file from a clean one.
func TestChainResultRepoSHA(t *testing.T) {
	t.Parallel()
	id := mustIdentity(t)
	ch, err := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})
	if err != nil {
		t.Fatalf("NewChain: %v", err)
	}

	// Random-ish payload so compression doesn't squash to a constant.
	payload := make([]byte, 64*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	var sink bytes.Buffer
	wc, res, err := ch.Wrap(nopCloser{&sink})
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if _, err := wc.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Plaintext-side checks (existing contract — defensive).
	plainSum := sha256.Sum256(payload)
	if res.SHA256 != plainSum {
		t.Errorf("res.SHA256 plaintext mismatch")
	}
	if res.Bytes != int64(len(payload)) {
		t.Errorf("res.Bytes = %d; want %d", res.Bytes, len(payload))
	}

	// Repo-side checks (new contract).
	repoSum := sha256.Sum256(sink.Bytes())
	if res.RepoSHA256 != repoSum {
		t.Errorf("res.RepoSHA256 = %x; want %x (sha256 of on-wire bytes)",
			res.RepoSHA256, repoSum)
	}
	if res.RepoBytes != int64(sink.Len()) {
		t.Errorf("res.RepoBytes = %d; want %d (on-wire byte count)",
			res.RepoBytes, sink.Len())
	}
	if res.RepoBytes == res.Bytes {
		// gzip + age MUST add framing/auth-tag overhead — if the on-wire
		// count equals the plaintext count something is bypassing the
		// chain. Loud-fail.
		t.Error("RepoBytes == Bytes: chain bypass suspected")
	}
}
