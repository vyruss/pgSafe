package compression_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/vyruss/pgsafe/internal/filter/compression"
)

// allCodecs returns the names of every codec. Adding a codec here is
// the only place; everything else parametrizes over this slice.
var allCodecs = []string{"gzip", "lz4", "zstd", "bzip2"}

func TestGetUnknownCodec(t *testing.T) {
	t.Parallel()
	if _, err := compression.Get("brotli"); err == nil {
		t.Fatal("Get(brotli): want error")
	}
}

func TestRoundTripPerCodec(t *testing.T) {
	for _, name := range allCodecs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, err := compression.Get(name)
			if err != nil {
				t.Fatalf("Get(%s): %v", name, err)
			}
			if c.Name() != name {
				t.Errorf("Name() = %q, want %q", c.Name(), name)
			}

			payloads := map[string][]byte{
				"empty":  nil,
				"small":  []byte("the quick brown fox jumps over the lazy dog\n"),
				"random": randomBytes(t, 256<<10), // 256 KiB
			}
			for label, in := range payloads {
				t.Run(label, func(t *testing.T) {
					var compressed bytes.Buffer
					wc, err := c.NewWriter(&compressed, 0)
					if err != nil {
						t.Fatalf("NewWriter: %v", err)
					}
					if len(in) > 0 {
						if _, err := wc.Write(in); err != nil {
							t.Fatalf("Write: %v", err)
						}
					}
					if err := wc.Close(); err != nil {
						t.Fatalf("Close: %v", err)
					}

					rc, err := c.NewReader(&compressed)
					if err != nil {
						t.Fatalf("NewReader: %v", err)
					}
					out, err := io.ReadAll(rc)
					if err != nil {
						t.Fatalf("ReadAll: %v", err)
					}
					if err := rc.Close(); err != nil {
						t.Fatalf("Reader Close: %v", err)
					}
					if !bytes.Equal(in, out) {
						t.Errorf("%s/%s: round-trip mismatch (len in=%d, out=%d)",
							name, label, len(in), len(out))
					}
				})
			}
		})
	}
}

func TestLevelOverride(t *testing.T) {
	for _, name := range allCodecs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, _ := compression.Get(name)
			// Just exercise a non-zero level path; correctness comes from round-trip.
			var buf bytes.Buffer
			wc, err := c.NewWriter(&buf, 1)
			if err != nil {
				t.Fatalf("NewWriter level=1: %v", err)
			}
			_, _ = wc.Write([]byte("hello"))
			if err := wc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}
