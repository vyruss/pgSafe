package rpc_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/transport/rpc"
	"github.com/vyruss/pgsafe/internal/transport/rpcmock"
	"golang.org/x/sync/errgroup"
)

// TestHelloRoundTrip exercises the full caller → worker → caller
// path through an in-process rpcmock pair. Cycle-1 stub-only: the worker is
// the Hello-only implementation so Configure/StreamFile/Done aren't tested
// here.
func TestHelloRoundTrip(t *testing.T) {
	t.Parallel()
	pair := rpcmock.NewPair()
	defer func() { _ = pair.Close() }()

	go func() {
		_ = rpc.Serve(pair.RemoteConn(),
			rpc.NewHelloOnlyWorker("v0.3.0-test", "/var/lib/postgresql/data"))
	}()

	client := rpc.NewClient(pair.LocalConn())
	defer func() { _ = client.Close() }()

	resp, err := client.Hello(rpc.HelloRequest{
		CallerVersion:   "v0.3.0-test",
		ProtocolVersion: rpc.Version,
	})
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if resp.WorkerVersion != "v0.3.0-test" {
		t.Errorf("WorkerVersion = %q, want %q", resp.WorkerVersion, "v0.3.0-test")
	}
	if resp.ProtocolVersion != rpc.Version {
		t.Errorf("ProtocolVersion = %q, want %q", resp.ProtocolVersion, rpc.Version)
	}
	if resp.PGDataPath != "/var/lib/postgresql/data" {
		t.Errorf("PGDataPath = %q", resp.PGDataPath)
	}
	if resp.NumCPU < 1 {
		t.Errorf("NumCPU = %d, want >= 1", resp.NumCPU)
	}
}

// TestConfigureNotImplemented confirms the Cycle-1 stub responds with a
// "not implemented" error rather than silently lying. will replace
// the stub with a real implementation; this test will be flipped to a
// happy-path assertion at that point.
func TestConfigureNotImplemented(t *testing.T) {
	t.Parallel()
	pair := rpcmock.NewPair()
	defer func() { _ = pair.Close() }()

	go func() {
		_ = rpc.Serve(pair.RemoteConn(),
			rpc.NewHelloOnlyWorker("v0.3.0-test", "/data"))
	}()

	client := rpc.NewClient(pair.LocalConn())
	defer func() { _ = client.Close() }()

	_, err := client.Configure(rpc.ConfigureRequest{BackupID: "ignored"})
	if err == nil {
		t.Fatal("Configure: want error from Hello-only stub, got nil")
	}
}

// TestProtocolMismatch — the worker reports a different protocol version;
// the client surfaces ErrProtocolMismatch. Simulated by a hand-rolled
// WorkerService that returns the wrong version string.
func TestProtocolMismatch(t *testing.T) {
	t.Parallel()
	pair := rpcmock.NewPair()
	defer func() { _ = pair.Close() }()

	go func() {
		_ = rpc.Serve(pair.RemoteConn(), &skewedWorker{})
	}()

	client := rpc.NewClient(pair.LocalConn())
	defer func() { _ = client.Close() }()

	_, err := client.Hello(rpc.HelloRequest{
		CallerVersion:   "v0.3.0-test",
		ProtocolVersion: rpc.Version,
	})
	if !errors.Is(err, rpc.ErrProtocolMismatch) {
		t.Errorf("Hello with skewed worker: want ErrProtocolMismatch, got %v", err)
	}
}

// TestStreamFileParallelOverSingleConnection asserts that net/rpc over a
// single JSON-RPC connection actually multiplexes concurrent calls — not
// just at the wire level (where the codec serializes writes), but at
// the dispatch level (the server runs handlers in their own goroutines).
//
// This is the load-bearing assertion behind5's "real
// parallelism" claim: the caller's errgroup of N goroutines all
// calling client.StreamFile concurrently MUST result in overlapping
// executions on the worker side. If net/rpc serialized handlers, the
// caller's parallelism would be a lie regardless of how many
// goroutines it spawned.
//
// The fixture's StreamFile handler records arrival, sleeps 80ms, and
// records departure. With 4 concurrent calls and a 4-cap limit, we
// expect peak concurrency = 4 (all calls in-flight at once). Anything
// less than 2 indicates net/rpc isn't actually parallelizing.
func TestStreamFileParallelOverSingleConnection(t *testing.T) {
	t.Parallel()
	pair := rpcmock.NewPair()
	defer func() { _ = pair.Close() }()

	w := &timingWorker{handlerSleep: 80 * time.Millisecond}
	go func() { _ = rpc.Serve(pair.RemoteConn(), w) }()

	client := rpc.NewClient(pair.LocalConn())
	defer func() { _ = client.Close() }()

	const concurrency = 4
	g := new(errgroup.Group)
	g.SetLimit(concurrency)
	for i := 0; i < concurrency; i++ {
		i := i
		g.Go(func() error {
			_, err := client.StreamFile(rpc.StreamFileRequest{
				Path: "fake/" + string(rune('a'+i)),
			})
			return err
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("parallel StreamFile: %v", err)
	}
	if peak := w.peakConcurrency.Load(); peak < 2 {
		t.Errorf("peak concurrent StreamFile handlers = %d, want >= 2 (parallelism is fake)", peak)
	}
}

// timingWorker records concurrent-handler counts so the parallelism
// test can assert that net/rpc actually overlaps handler executions.
type timingWorker struct {
	handlerSleep    time.Duration
	mu              sync.Mutex
	inFlight        atomic.Int32
	peakConcurrency atomic.Int32
}

func (w *timingWorker) Hello(_ *rpc.HelloRequest, resp *rpc.HelloResponse) error {
	resp.WorkerVersion = "v0-parallel-test"
	resp.ProtocolVersion = rpc.Version
	resp.OS = "testos"
	resp.NumCPU = 4
	return nil
}
func (w *timingWorker) Configure(_ *rpc.ConfigureRequest, _ *rpc.ConfigureResponse) error {
	return nil
}
func (w *timingWorker) StreamFile(req *rpc.StreamFileRequest, resp *rpc.StreamFileResponse) error {
	cur := w.inFlight.Add(1)
	for {
		p := w.peakConcurrency.Load()
		if cur <= p || w.peakConcurrency.CompareAndSwap(p, cur) {
			break
		}
	}
	defer w.inFlight.Add(-1)

	time.Sleep(w.handlerSleep)
	w.mu.Lock()
	resp.Path = req.Path
	resp.Bytes = 1
	resp.ModTime = time.Now()
	w.mu.Unlock()
	return nil
}
func (w *timingWorker) StreamChunk(_ *rpc.StreamChunkRequest, _ *rpc.StreamChunkResponse) error {
	return nil
}
func (w *timingWorker) WriteBlob(_ *rpc.WriteBlobRequest, _ *rpc.WriteBlobResponse) error {
	return nil
}
func (w *timingWorker) Commit(_ *rpc.CommitRequest, _ *rpc.CommitResponse) error { return nil }
func (w *timingWorker) Done(_ *rpc.DoneRequest, _ *rpc.DoneResponse) error       { return nil }
func (w *timingWorker) Restore(_ *rpc.RestoreRequest, _ *rpc.RestoreResponse) error {
	return nil
}
func (w *timingWorker) ProbeStorage(_ *rpc.ProbeStorageRequest, _ *rpc.ProbeStorageResponse) error {
	return nil
}

type skewedWorker struct{}

func (*skewedWorker) Hello(_ *rpc.HelloRequest, resp *rpc.HelloResponse) error {
	resp.WorkerVersion = "v0.99.0-skew"
	resp.ProtocolVersion = "phase99-vBOGUS"
	resp.OS = "skewedos"
	resp.NumCPU = 1
	return nil
}
func (*skewedWorker) Configure(_ *rpc.ConfigureRequest, _ *rpc.ConfigureResponse) error {
	return errors.New("not used")
}
func (*skewedWorker) StreamFile(_ *rpc.StreamFileRequest, _ *rpc.StreamFileResponse) error {
	return errors.New("not used")
}
func (*skewedWorker) StreamChunk(_ *rpc.StreamChunkRequest, _ *rpc.StreamChunkResponse) error {
	return errors.New("not used")
}
func (*skewedWorker) WriteBlob(_ *rpc.WriteBlobRequest, _ *rpc.WriteBlobResponse) error {
	return errors.New("not used")
}
func (*skewedWorker) Commit(_ *rpc.CommitRequest, _ *rpc.CommitResponse) error {
	return errors.New("not used")
}
func (*skewedWorker) Done(_ *rpc.DoneRequest, _ *rpc.DoneResponse) error {
	return errors.New("not used")
}
func (*skewedWorker) Restore(_ *rpc.RestoreRequest, _ *rpc.RestoreResponse) error {
	return errors.New("not used")
}
func (*skewedWorker) ProbeStorage(_ *rpc.ProbeStorageRequest, _ *rpc.ProbeStorageResponse) error {
	return errors.New("not used")
}
