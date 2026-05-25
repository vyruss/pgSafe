package compression

import (
	"compress/gzip"
	"fmt"
	"io"
)

type gzipCodec struct{}

func (gzipCodec) Name() string { return "gzip" }

func (gzipCodec) NewWriter(sink io.Writer, level int) (io.WriteCloser, error) {
	if level == 0 {
		level = gzip.DefaultCompression
	}
	w, err := gzip.NewWriterLevel(sink, level)
	if err != nil {
		return nil, fmt.Errorf("compression/gzip: NewWriterLevel: %w", err)
	}
	return w, nil
}

func (gzipCodec) NewReader(src io.Reader) (io.ReadCloser, error) {
	r, err := gzip.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("compression/gzip: NewReader: %w", err)
	}
	return r, nil
}
