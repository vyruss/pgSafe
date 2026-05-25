// Package filter composes the streaming hash → compress → encrypt pipeline
// that every backup file flows through on its way into the storage,
// and the inverse decrypt → decompress → hash pipeline used at restore time.
//
// The forward chain order is locked: the hash captures plaintext content
// (so the manifest's checksums match what lives in $PGDATA, satisfying
// Invariant #3), then compression, then age encryption, then the storage
// writer. The constructor returns *Result so callers can read the digest
// after Close.
package filter

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/filter/compression"
	"github.com/vyruss/pgsafe/internal/filter/encryption"
	pgsafehash "github.com/vyruss/pgsafe/internal/filter/hash"
)

// Result carries the post-Close summary of a chain Wrap.
//
// Bytes/SHA256 cover the PLAINTEXT side: the bytes that came from
// $PGDATA before any compression or encryption. These are what PG's
// backup_manifest format records and what pg_verifybackup verifies
// against.
//
// RepoBytes/RepoSHA256 cover the REPO side: the bytes that landed on
// storage after the full chain (post-compression + post-encryption).
// Used by resume to verify a prior partial backup's files actually
// committed durably — the worker re-hashes the on-storage bytes and
// compares to the recorded RepoSHA256, mirroring pgbackrest's
// repoFileChecksum gate. Unaffected by the encryption identity:
// hashes the literal on-disk bytes.
type Result struct {
	Bytes      int64
	SHA256     [32]byte
	RepoBytes  int64
	RepoSHA256 [32]byte
}

// Options configures a Chain. All fields are required; the chain has no
// optional stages.
type Options struct {
	Codec      string          // gzip | lz4 | zstd | bzip2
	Level      int             // 0 = codec default
	Recipients []age.Recipient // age public keys; non-empty
}

// Chain is the immutable configuration. One chain can produce many writers
// (one per file) via Wrap.
type Chain struct {
	codec      compression.Codec
	level      int
	recipients []age.Recipient
}

// NewChain validates options and returns a Chain ready to Wrap sinks.
func NewChain(opts Options) (*Chain, error) {
	if len(opts.Recipients) == 0 {
		return nil, errors.New("filter: at least one recipient required")
	}
	codec, err := compression.Get(opts.Codec)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	return &Chain{
		codec:      codec,
		level:      opts.Level,
		recipients: opts.Recipients,
	}, nil
}

// Wrap returns a WriteCloser that pipes user-written plaintext through the
// hash → compress → encrypt → sink chain. The returned *Result is populated
// on a successful Close; reading it before Close yields a partial state.
//
// Closing the writer also closes sink — the chain owns the durability point.
func (c *Chain) Wrap(sink io.WriteCloser) (io.WriteCloser, *Result, error) {
	// repoSink hashes + counts bytes on the way to the actual sink so
	// Result.RepoSHA256 / RepoBytes reflect what's on storage. Resume
	// uses these to verify a prior attempt's files committed durably
	// without needing the operator's encryption identity (the hash
	// is over the on-disk ciphertext, not the plaintext).
	rs := &repoSink{sink: sink, h: sha256.New()}
	encWriter, err := encryption.NewWriter(rs, c.recipients)
	if err != nil {
		return nil, nil, err
	}
	compWriter, err := c.codec.NewWriter(encWriter, c.level)
	if err != nil {
		_ = encWriter.Close()
		return nil, nil, err
	}
	hashWriter := pgsafehash.NewWriter(compWriter)
	res := &Result{}
	return &chainWriter{
		sink:   sink,
		repo:   rs,
		enc:    encWriter,
		comp:   compWriter,
		hashed: hashWriter,
		res:    res,
	}, res, nil
}

// repoSink wraps the storage-side WriteCloser with SHA-256 + byte
// counting so the chain can populate Result.RepoSHA256 / RepoBytes.
// Unlike pgsafehash.Writer (plaintext side), this one is itself a
// WriteCloser — the encryption stage closes it during chain teardown.
type repoSink struct {
	sink  io.WriteCloser
	h     hash.Hash
	bytes int64
}

func (r *repoSink) Write(p []byte) (int, error) {
	n, err := r.sink.Write(p)
	if n > 0 {
		_, _ = r.h.Write(p[:n])
		r.bytes += int64(n)
	}
	return n, err
}

func (r *repoSink) Close() error { return r.sink.Close() }

func (r *repoSink) Sum() [32]byte {
	var out [32]byte
	copy(out[:], r.h.Sum(nil))
	return out
}

func (r *repoSink) Bytes() int64 { return r.bytes }

// Unwrap is the inverse of Wrap: it consumes a ciphertext byte stream
// and exposes a reader of the reconstructed plaintext, hashing as it
// flows. Identities are the age private keys for decryption (at least
// one required); codecName names the compression codec used by the
// forward chain (matches the YAML/sidecar field). Recipients aren't
// needed on the reverse path — restore-only callers don't have to
// fabricate them to call this.
//
// The returned *Result is populated as bytes are read; it stabilises
// after the consumer reads io.EOF.
//
// Closing the returned ReadCloser tears down the decompress + decrypt
// stages but does NOT close `src` — the caller owns the underlying
// source.
func Unwrap(codecName string, src io.Reader, identities []age.Identity) (io.ReadCloser, *Result, error) {
	if len(identities) == 0 {
		return nil, nil, errors.New("filter: at least one identity required")
	}
	codec, err := compression.Get(codecName)
	if err != nil {
		return nil, nil, fmt.Errorf("filter: %w", err)
	}
	dec, err := encryption.NewReader(src, identities)
	if err != nil {
		return nil, nil, fmt.Errorf("filter: decrypt: %w", err)
	}
	zr, err := codec.NewReader(dec)
	if err != nil {
		return nil, nil, fmt.Errorf("filter: decompress: %w", err)
	}
	res := &Result{}
	return &chainReader{
		comp: zr,
		hash: sha256.New(),
		res:  res,
	}, res, nil
}

type chainReader struct {
	comp   io.ReadCloser
	hash   hash.Hash
	res    *Result
	closed bool
}

func (r *chainReader) Read(p []byte) (int, error) {
	n, err := r.comp.Read(p)
	if n > 0 {
		_, _ = r.hash.Write(p[:n])
		r.res.Bytes += int64(n)
	}
	if err == io.EOF {
		copy(r.res.SHA256[:], r.hash.Sum(nil))
	}
	return n, err
}

func (r *chainReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.comp.Close()
}

type chainWriter struct {
	sink   io.WriteCloser
	repo   *repoSink
	enc    io.WriteCloser
	comp   io.WriteCloser
	hashed *pgsafehash.Writer
	res    *Result
	closed bool
}

func (w *chainWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("filter: write after close")
	}
	return w.hashed.Write(p)
}

// Close flushes the chain in order: capture digest → close compression →
// close encryption → close sink. A failure at any step short-circuits the
// rest; subsequent layers may leak resources but the public contract is
// "Close fails loudly".
func (w *chainWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	w.res.SHA256 = w.hashed.Sum()
	w.res.Bytes = w.hashed.Bytes()

	if err := w.comp.Close(); err != nil {
		_ = w.enc.Close()
		_ = w.sink.Close()
		return fmt.Errorf("filter: compression close: %w", err)
	}
	if err := w.enc.Close(); err != nil {
		_ = w.sink.Close()
		return fmt.Errorf("filter: encryption close: %w", err)
	}
	// Encryption flush is done — every on-the-wire byte has gone
	// through repoSink. Capture the repo-side digest BEFORE closing
	// the actual sink so the operator-visible Result reflects the
	// final committed state.
	w.res.RepoSHA256 = w.repo.Sum()
	w.res.RepoBytes = w.repo.Bytes()
	if err := w.sink.Close(); err != nil {
		return fmt.Errorf("filter: sink close: %w", err)
	}
	return nil
}
