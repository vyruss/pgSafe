// Package compression provides the Codec interface used by the filter chain
// and the four codecs registered against it (gzip, lz4, zstd, bzip2).
//
//	locks the codec set; widening it is a
//
// deliberate design change. Codec resolution is a static lookup keyed by name.
package compression

import (
	"fmt"
	"io"
)

// Codec is one compression algorithm. Implementations are expected to be
// stateless; the writer and reader returned carry the per-stream state.
type Codec interface {
	// Name is the YAML config string ("gzip", "lz4", "zstd", "bzip2").
	Name() string

	// NewWriter returns a streaming compressor that writes compressed bytes
	// to sink. Closing the returned WriteCloser flushes any internal buffers
	// and finalizes the compressed stream — but does not close sink.
	// level is the codec-specific level; pass 0 for the codec's default.
	NewWriter(sink io.Writer, level int) (io.WriteCloser, error)

	// NewReader returns a streaming decompressor reading from src. Caller
	// closes the reader when done.
	NewReader(src io.Reader) (io.ReadCloser, error)
}

// Get returns the codec registered under name. Unknown names error rather
// than fall back to a default — config has already validated the name set.
func Get(name string) (Codec, error) {
	switch name {
	case "gzip":
		return gzipCodec{}, nil
	case "lz4":
		return lz4Codec{}, nil
	case "zstd":
		return zstdCodec{}, nil
	case "bzip2":
		return bzip2Codec{}, nil
	default:
		return nil, fmt.Errorf("compression: unknown codec %q", name)
	}
}
