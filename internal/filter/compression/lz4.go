package compression

import (
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

type lz4Codec struct{}

func (lz4Codec) Name() string { return "lz4" }

// lz4Levels maps the YAML config integer (1-9) to the lz4 enum, whose
// underlying values are not consecutive (1 << (8+n)) and so can't be cast
// directly.
var lz4Levels = [10]lz4.CompressionLevel{
	0: lz4.Fast,
	1: lz4.Level1,
	2: lz4.Level2,
	3: lz4.Level3,
	4: lz4.Level4,
	5: lz4.Level5,
	6: lz4.Level6,
	7: lz4.Level7,
	8: lz4.Level8,
	9: lz4.Level9,
}

func (lz4Codec) NewWriter(sink io.Writer, level int) (io.WriteCloser, error) {
	w := lz4.NewWriter(sink)
	if level > 0 {
		if level < 1 || level > 9 {
			return nil, fmt.Errorf("compression/lz4: level %d out of range (1-9)", level)
		}
		if err := w.Apply(lz4.CompressionLevelOption(lz4Levels[level])); err != nil {
			return nil, fmt.Errorf("compression/lz4: set level: %w", err)
		}
	}
	return w, nil
}

func (lz4Codec) NewReader(src io.Reader) (io.ReadCloser, error) {
	return lz4ReadCloser{lz4.NewReader(src)}, nil
}

// lz4ReadCloser adds Close() to lz4.Reader (which doesn't have one).
type lz4ReadCloser struct{ *lz4.Reader }

func (l lz4ReadCloser) Close() error { return nil }
