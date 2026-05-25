package multi_test

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/storage/multi"
)

// bufSink is a simple in-memory io.WriteCloser used by tests.
type bufSink struct {
	buf    bytes.Buffer
	closed bool
	failOn int // if > 0, fail on the n-th Write call
	calls  int
}

func (b *bufSink) Write(p []byte) (int, error) {
	b.calls++
	if b.failOn > 0 && b.calls >= b.failOn {
		return 0, errors.New("bufSink: injected failure")
	}
	return b.buf.Write(p)
}

func (b *bufSink) Close() error {
	b.closed = true
	return nil
}

func TestTeeWriterSingleSink(t *testing.T) {
	t.Parallel()
	s := &bufSink{}
	tw := multi.New([]io.WriteCloser{s})

	payload := []byte("hello tee")
	if _, err := tw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := s.buf.Bytes(); !bytes.Equal(got, payload) {
		t.Errorf("sink received %q, want %q", got, payload)
	}
	if !s.closed {
		t.Error("sink was not closed")
	}
	if got := tw.Results(); len(got) != 1 || got[0] != nil {
		t.Errorf("Results = %v, want [nil]", got)
	}
}

func TestTeeWriterTwoSinks(t *testing.T) {
	t.Parallel()
	s0, s1 := &bufSink{}, &bufSink{}
	tw := multi.New([]io.WriteCloser{s0, s1})

	payload := []byte("multi-sink payload")
	if _, err := tw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, s := range []*bufSink{s0, s1} {
		if got := s.buf.Bytes(); !bytes.Equal(got, payload) {
			t.Errorf("sink[%d] received %q, want %q", i, got, payload)
		}
	}
	if live := tw.LiveIndices(); len(live) != 2 {
		t.Errorf("LiveIndices = %v, want [0 1]", live)
	}
}

// TestTeeWriterOneSinkFails verifies that a single failing sink doesn't block
// the surviving sink: the surviving sink receives all bytes, Close returns nil,
// and Results correctly records the per-sink outcomes.
func TestTeeWriterOneSinkFails(t *testing.T) {
	t.Parallel()
	good := &bufSink{}
	bad := &bufSink{failOn: 1} // fails on first Write
	tw := multi.New([]io.WriteCloser{good, bad})

	payload := []byte("resilient payload")
	// Write may succeed (returning len(p)) as long as the good sink is alive.
	if _, err := tw.Write(payload); err != nil {
		t.Fatalf("Write: unexpected error when one sink is still live: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: unexpected error when one sink succeeded: %v", err)
	}

	if got := good.buf.Bytes(); !bytes.Equal(got, payload) {
		t.Errorf("good sink received %q, want %q", got, payload)
	}
	results := tw.Results()
	if results[0] != nil {
		t.Errorf("results[0] (good) = %v, want nil", results[0])
	}
	if results[1] == nil {
		t.Errorf("results[1] (bad) = nil, want error")
	}
	if live := tw.LiveIndices(); len(live) != 1 || live[0] != 0 {
		t.Errorf("LiveIndices = %v, want [0]", live)
	}
}

// TestTeeWriterAllSinksFail confirms Close returns error when every sink has
// failed. With the channel-based implementation goroutine failure is detected
// asynchronously, so we only assert on Close() — not on the intermediate
// Write() calls — to avoid scheduling-dependent behaviour.
func TestTeeWriterAllSinksFail(t *testing.T) {
	t.Parallel()
	s0 := &bufSink{failOn: 1}
	s1 := &bufSink{failOn: 1}
	tw := multi.New([]io.WriteCloser{s0, s1})

	// Both sinks will fail on their first Write. Goroutines close their fail
	// channels asynchronously; Write() may or may not observe the failure
	// before Close() is called.
	_, _ = tw.Write([]byte("trigger failure"))

	if closeErr := tw.Close(); closeErr == nil {
		t.Fatal("Close with all failed sinks: expected error, got nil")
	}
	if live := tw.LiveIndices(); len(live) != 0 {
		t.Errorf("LiveIndices = %v, want []", live)
	}
}

// TestTeeWriterNilSink verifies that a nil entry in the sinks slice is treated
// as a pre-failed backend.
func TestTeeWriterNilSink(t *testing.T) {
	t.Parallel()
	good := &bufSink{}
	tw := multi.New([]io.WriteCloser{good, nil})

	payload := []byte("nil-sink safe")
	if _, err := tw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := good.buf.Bytes(); !bytes.Equal(got, payload) {
		t.Errorf("good sink received %q, want %q", got, payload)
	}
	results := tw.Results()
	if results[0] != nil {
		t.Errorf("results[0] (good) = %v, want nil", results[0])
	}
	if results[1] == nil {
		t.Errorf("results[1] (nil sink) = nil, want error")
	}
}

// TestTeeWriterLargePayload exercises the goroutine draining path with enough
// data to fill pipe buffers and force multiple read/write cycles.
func TestTeeWriterLargePayload(t *testing.T) {
	t.Parallel()
	s0, s1 := &bufSink{}, &bufSink{}
	tw := multi.New([]io.WriteCloser{s0, s1})

	large := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
	if _, err := tw.Write(large); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, s := range []*bufSink{s0, s1} {
		if got := s.buf.Len(); got != len(large) {
			t.Errorf("sink[%d] received %d bytes, want %d", i, got, len(large))
		}
	}
}

// slowSink simulates a backend with a fixed per-Write latency. Used by the
// parallel-drain test to prove backends actually run concurrently.
type slowSink struct {
	mu     sync.Mutex
	bytes  int
	closed bool
	delay  time.Duration
}

func (s *slowSink) Write(p []byte) (int, error) {
	time.Sleep(s.delay)
	s.mu.Lock()
	s.bytes += len(p)
	s.mu.Unlock()
	return len(p), nil
}

func (s *slowSink) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// TestTeeWriterParallelDrains is the contract test for the channel-based
// rewrite: two sinks with identical per-Write latency must drain
// concurrently, so total elapsed time ≈ max(latency) rather than sum.
//
// We send 8 chunks at 50 ms latency each. Sequential drain would take
// 8×50×2 = 800 ms; parallel drain takes 8×50 = 400 ms. We assert <600 ms,
// which gives generous margin for scheduler jitter while still catching
// any regression to serialised behaviour.
func TestTeeWriterParallelDrains(t *testing.T) {
	t.Parallel()
	const (
		chunks  = 8
		delay   = 50 * time.Millisecond
		serial  = chunks * delay * 2 // 800 ms — what we'd see if serialised
		regress = 600 * time.Millisecond
	)
	s0 := &slowSink{delay: delay}
	s1 := &slowSink{delay: delay}
	// Buffer depth ≥ chunks so Write() never blocks on back-pressure;
	// this isolates the test from buffer-fill effects and measures pure
	// goroutine concurrency.
	tw := multi.NewWithDepth([]io.WriteCloser{s0, s1}, chunks+1)

	payload := []byte("x")
	start := time.Now()
	for range chunks {
		if _, err := tw.Write(payload); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed >= regress {
		t.Errorf("elapsed = %v, want < %v (sinks appear to be running serially; serial would be ~%v)", elapsed, regress, serial)
	}
	if s0.bytes != chunks || s1.bytes != chunks {
		t.Errorf("sink byte counts = %d/%d, want %d/%d", s0.bytes, s1.bytes, chunks, chunks)
	}
}
