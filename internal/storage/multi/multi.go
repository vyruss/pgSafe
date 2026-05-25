// Package multi provides TeeWriter, which fans out one write stream to N
// io.WriteClosers concurrently. Each sink drains via its own goroutine fed
// by a buffered channel, so the main writer is decoupled from slow peers:
// writes block only when a backend's channel is full (back-pressure), not on
// every sink.Write() call. The filter chain (encrypt + compress) therefore
// runs once and the same byte stream reaches all storage backends in parallel,
// keeping CPU cost flat regardless of backend count (Invariant #10).
package multi

import (
	"errors"
	"io"
	"sync"
)

// DefaultBufDepth is the number of Write() payloads each backend channel
// buffers before the main goroutine blocks on that backend. The backup path
// feeds the filter chain with 4 MiB chunks (io.CopyBuffer); after zstd
// compression the chunks reaching TeeWriter are typically 0.5–2 MiB, so
// 256 slots gives ~128–512 MiB of look-ahead per backend in steady state —
// enough to absorb seconds of speed difference between a fast local backend
// and a cloud backend before back-pressure kicks in.
const DefaultBufDepth = 256

type worker struct {
	// ch is the send-side reference; Write() nils it when failure is detected
	// so dead backends are skipped without closing the channel prematurely.
	ch   chan []byte
	fail chan struct{} // closed by the goroutine on the first sink write error
}

// TeeWriter fans out one byte stream to N io.WriteClosers concurrently.
// A failing sink's goroutine is detected on the next Write() call and
// bypassed so surviving sinks continue unaffected.
// Close() closes all backend channels, waits for all goroutines to finish,
// and returns nil if at least one sink committed successfully.
type TeeWriter struct {
	workers  []worker
	allChans []chan []byte // original channel refs used by Close() to signal EOF
	errs     []error
	wg       sync.WaitGroup
}

// New starts one draining goroutine per sink using DefaultBufDepth.
// Nil entries in sinks are treated as pre-failed backends.
func New(sinks []io.WriteCloser) *TeeWriter {
	return NewWithDepth(sinks, DefaultBufDepth)
}

// NewWithDepth is like New but lets the caller tune the per-backend channel
// buffer depth. Larger values allow the main writer to get further ahead of
// slow backends before blocking.
func NewWithDepth(sinks []io.WriteCloser, depth int) *TeeWriter {
	t := &TeeWriter{
		workers:  make([]worker, len(sinks)),
		allChans: make([]chan []byte, len(sinks)),
		errs:     make([]error, len(sinks)),
	}
	for i, sink := range sinks {
		if sink == nil {
			t.errs[i] = errors.New("multi: nil sink")
			continue
		}
		ch := make(chan []byte, depth)
		fail := make(chan struct{})
		t.workers[i] = worker{ch: ch, fail: fail}
		t.allChans[i] = ch
		t.wg.Add(1)
		i, sink, ch, fail := i, sink, ch, fail
		go func() {
			defer t.wg.Done()
			var failed bool
			for chunk := range ch {
				if failed {
					continue // drain silently after the first write error
				}
				if _, err := sink.Write(chunk); err != nil {
					t.errs[i] = err
					close(fail)
					failed = true
				}
			}
			if failed {
				_ = sink.Close()
			} else if err := sink.Close(); err != nil {
				t.errs[i] = err
			}
		}()
	}
	return t
}

// Write copies p and fans the copy to all live backend channels.
// Sending to a full channel blocks (back-pressure from that backend) until
// the backend drains it or is detected as failed. Returns (len(p), nil) as
// long as at least one sink is still live; returns an error only when every
// sink has been detected as failed.
func (t *TeeWriter) Write(p []byte) (int, error) {
	// Copy p: the caller may reuse its buffer as soon as Write returns.
	chunk := make([]byte, len(p))
	copy(chunk, p)

	live := 0
	for i := range t.workers {
		w := &t.workers[i]
		if w.ch == nil {
			continue // pre-failed (nil sink) or already detected as dead
		}
		// Best-effort fast check: has this goroutine already reported failure?
		select {
		case <-w.fail:
			w.ch = nil
			continue
		default:
		}
		// Enqueue the chunk; block if the channel buffer is full.
		// If the goroutine fails while we're blocked, the fail case fires.
		select {
		case w.ch <- chunk:
			live++
		case <-w.fail:
			w.ch = nil
		}
	}
	if live == 0 {
		return 0, errors.New("multi: all backends failed")
	}
	return len(p), nil
}

// Close signals EOF to all backend channels and waits for all goroutines to
// finish draining. Returns nil if at least one backend committed without error.
func (t *TeeWriter) Close() error {
	for _, ch := range t.allChans {
		if ch != nil {
			close(ch)
		}
	}
	t.wg.Wait()
	for _, e := range t.errs {
		if e == nil {
			return nil
		}
	}
	return errors.New("multi: all backends failed")
}

// Results returns per-sink outcomes. Valid only after Close returns.
// A nil error means the sink received all bytes and closed cleanly.
func (t *TeeWriter) Results() []error { return t.errs }

// LiveIndices returns the indices of sinks that closed without error.
// Valid only after Close.
func (t *TeeWriter) LiveIndices() []int {
	var out []int
	for i, e := range t.errs {
		if e == nil {
			out = append(out, i)
		}
	}
	return out
}
