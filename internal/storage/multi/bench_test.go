package multi_test

import (
	"io"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/multi"
)

// discardSink is an io.WriteCloser that throws away its input. Isolates
// TeeWriter overhead (allocation + channel ops + goroutine handoff) from
// backend speed for the fan-out benchmark.
type discardSink struct{}

func (discardSink) Write(p []byte) (int, error) { return len(p), nil }
func (discardSink) Close() error                { return nil }

// BenchmarkTeeWriterFanOut measures throughput vs. backend count for the
// channel-based fan-out. With ReportAllocs we also surface the per-Write
// allocation cost — relevant for deciding whether buffer pooling is worth
// the complexity.
//
// Run with: go test -bench=BenchmarkTeeWriterFanOut -benchmem ./internal/storage/multi/
func BenchmarkTeeWriterFanOut(b *testing.B) {
	const chunkSize = 4 << 20 // 4 MiB — matches backup.go's io.CopyBuffer
	payload := make([]byte, chunkSize)

	for _, n := range []int{1, 2, 4, 8} {
		b.Run(name(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(chunkSize))

			sinks := make([]io.WriteCloser, n)
			for i := range sinks {
				sinks[i] = discardSink{}
			}
			tw := multi.New(sinks)

			b.ResetTimer()
			for range b.N {
				if _, err := tw.Write(payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := tw.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func name(n int) string {
	switch n {
	case 1:
		return "1backend"
	case 2:
		return "2backends"
	case 4:
		return "4backends"
	case 8:
		return "8backends"
	}
	return "n"
}
