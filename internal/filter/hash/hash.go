// Package hash provides a streaming SHA-256 writer that tees bytes to a sink
// while incrementally computing the digest. Used in the filter chain at the
// plaintext stage so the manifest's checksums match what lives in $PGDATA.
//
// : locked filter-chain order is
// hash → compress → encrypt → sink.
package hash

import (
	"crypto/sha256"
	"hash"
	"io"
)

// Writer tees Write calls to the underlying sink and into a SHA-256 hasher.
// Sum and Bytes may be called at any time; on a successful Close(*) they
// reflect the full input. (*) Writer is not itself an io.Closer — Close
// belongs to the chain caller.
type Writer struct {
	sink  io.Writer
	h     hash.Hash
	bytes int64
}

// NewWriter wraps sink. Subsequent Write calls go to both sink and the hasher.
func NewWriter(sink io.Writer) *Writer {
	return &Writer{sink: sink, h: sha256.New()}
}

// Write writes p to sink and updates the digest. The byte count is exactly
// what sink reports.
func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.sink.Write(p)
	if n > 0 {
		_, _ = w.h.Write(p[:n])
		w.bytes += int64(n)
	}
	return n, err
}

// Sum returns the SHA-256 of all bytes successfully written so far.
func (w *Writer) Sum() [32]byte {
	var out [32]byte
	copy(out[:], w.h.Sum(nil))
	return out
}

// Bytes returns the number of bytes successfully written so far.
func (w *Writer) Bytes() int64 { return w.bytes }
