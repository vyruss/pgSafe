// Package rpcmock provides in-process stdin/stdout pipe pairs for unit
// testing the caller/worker JSON-RPC transport without an actual SSH
// subprocess. Lives under internal/transport/ so it can sit beside the
// production ssh + rpc packages and use their unexported test seams when
// needed.
//
// Usage in a test:
//
//	pair := rpcmock.NewPair()
//	defer pair.Close()
//	go worker.Serve(pair.RemoteIn, pair.RemoteOut, impl)
//	client := jsonrpc.NewClient(pair.LocalConn())
//	err := client.Call("WorkerService.Hello", req, &resp)
//
// The pair exactly mirrors a *ssh.Session's stdio pair: LocalConn writes to
// RemoteIn and reads from RemoteOut. Closing the pair propagates EOF to the
// remote side, so the worker's serve loop exits.
package rpcmock

import (
	"io"
)

// Pair holds the two ends of an in-process stdio bridge.
type Pair struct {
	// LocalIn is what the caller writes to (its "stdin" toward the worker).
	// The worker reads its stdin from RemoteIn (a different *io.PipeReader,
	// connected to LocalIn via io.Pipe).
	LocalIn  *io.PipeWriter
	RemoteIn *io.PipeReader

	// LocalOut is what the caller reads from (the worker's stdout).
	// The worker writes its stdout to RemoteOut.
	LocalOut  *io.PipeReader
	RemoteOut *io.PipeWriter
}

// NewPair returns a fresh Pair. Each Pair owns four io.Pipe endpoints; close
// the Pair to release them.
func NewPair() *Pair {
	rIn, lIn := io.Pipe()
	lOut, rOut := io.Pipe()
	return &Pair{
		LocalIn:   lIn,
		RemoteIn:  rIn,
		LocalOut:  lOut,
		RemoteOut: rOut,
	}
}

// LocalConn returns an io.ReadWriteCloser the caller can hand to
// jsonrpc.NewClient. Reads from RemoteOut (worker → caller); writes to
// LocalIn (caller → worker).
func (p *Pair) LocalConn() io.ReadWriteCloser {
	return &rwCloser{r: p.LocalOut, w: p.LocalIn, close: p.Close}
}

// RemoteConn returns an io.ReadWriteCloser the worker can hand to
// jsonrpc.ServeConn. Reads from RemoteIn (caller → worker); writes to
// RemoteOut (worker → caller).
func (p *Pair) RemoteConn() io.ReadWriteCloser {
	return &rwCloser{r: p.RemoteIn, w: p.RemoteOut, close: p.Close}
}

// Close tears down all four pipe endpoints; serve loops on both sides see EOF.
func (p *Pair) Close() error {
	_ = p.LocalIn.Close()
	_ = p.RemoteIn.Close()
	_ = p.LocalOut.Close()
	_ = p.RemoteOut.Close()
	return nil
}

type rwCloser struct {
	r     io.Reader
	w     io.Writer
	close func() error
}

func (rw *rwCloser) Read(p []byte) (int, error)  { return rw.r.Read(p) }
func (rw *rwCloser) Write(p []byte) (int, error) { return rw.w.Write(p) }
func (rw *rwCloser) Close() error                { return rw.close() }
