package compression

import (
	"fmt"
	"io"

	"github.com/dsnet/compress/bzip2"
)

type bzip2Codec struct{}

func (bzip2Codec) Name() string { return "bzip2" }

func (bzip2Codec) NewWriter(sink io.Writer, level int) (io.WriteCloser, error) {
	cfg := &bzip2.WriterConfig{}
	if level > 0 {
		cfg.Level = level
	}
	w, err := bzip2.NewWriter(sink, cfg)
	if err != nil {
		return nil, fmt.Errorf("compression/bzip2: NewWriter: %w", err)
	}
	return w, nil
}

func (bzip2Codec) NewReader(src io.Reader) (io.ReadCloser, error) {
	r, err := bzip2.NewReader(src, nil)
	if err != nil {
		return nil, fmt.Errorf("compression/bzip2: NewReader: %w", err)
	}
	return r, nil
}
