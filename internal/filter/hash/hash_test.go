package hash_test

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/vyruss/pgsafe/internal/filter/hash"
)

func TestWriterTeesAndHashes(t *testing.T) {
	t.Parallel()

	var sink bytes.Buffer
	w := hash.NewWriter(&sink)

	payload := []byte("the quick brown fox\n")
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if !bytes.Equal(sink.Bytes(), payload) {
		t.Errorf("sink got %q, want %q", sink.Bytes(), payload)
	}
	if got, want := w.Sum(), sha256.Sum256(payload); got != want {
		t.Errorf("Sum() = %x, want %x", got, want)
	}
	if got := w.Bytes(); got != int64(len(payload)) {
		t.Errorf("Bytes() = %d, want %d", got, len(payload))
	}
}

func TestWriterHashesAcrossChunks(t *testing.T) {
	t.Parallel()

	var sink bytes.Buffer
	w := hash.NewWriter(&sink)
	chunks := [][]byte{[]byte("hello "), []byte("world\n")}
	for _, c := range chunks {
		if _, err := w.Write(c); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	want := sha256.Sum256(append(chunks[0], chunks[1]...))
	if got := w.Sum(); got != want {
		t.Errorf("Sum mismatch across chunks: got %x, want %x", got, want)
	}
}

func TestWriterEmptyInput(t *testing.T) {
	t.Parallel()

	w := hash.NewWriter(&bytes.Buffer{})
	want := sha256.Sum256(nil)
	if got := w.Sum(); got != want {
		t.Errorf("empty Sum = %x, want %x", got, want)
	}
	if got := w.Bytes(); got != 0 {
		t.Errorf("empty Bytes = %d, want 0", got)
	}
}
