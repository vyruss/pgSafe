package compression

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

type zstdCodec struct{}

func (zstdCodec) Name() string { return "zstd" }

func (zstdCodec) NewWriter(sink io.Writer, level int) (io.WriteCloser, error) {
	opts := []zstd.EOption{}
	if level > 0 {
		opts = append(opts, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	}
	enc, err := zstd.NewWriter(sink, opts...)
	if err != nil {
		return nil, fmt.Errorf("compression/zstd: NewWriter: %w", err)
	}
	return enc, nil
}

func (zstdCodec) NewReader(src io.Reader) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("compression/zstd: NewReader: %w", err)
	}
	return zstdReadCloser{dec}, nil
}

// zstdReadCloser adapts zstd.Decoder.Close (which returns nothing) to
// io.ReadCloser semantics by wrapping it.
type zstdReadCloser struct{ *zstd.Decoder }

func (z zstdReadCloser) Close() error { z.Decoder.Close(); return nil }
