package rpc

import (
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"runtime"
)

// WorkerService is the interface every PG-host worker implementation must
// satisfy. ships a Hello-only stub; lands the real
// Configure/StreamFile/Done bodies.
type WorkerService interface {
	Hello(req *HelloRequest, resp *HelloResponse) error
	Configure(req *ConfigureRequest, resp *ConfigureResponse) error
	StreamFile(req *StreamFileRequest, resp *StreamFileResponse) error
	StreamChunk(req *StreamChunkRequest, resp *StreamChunkResponse) error
	WriteBlob(req *WriteBlobRequest, resp *WriteBlobResponse) error
	Commit(req *CommitRequest, resp *CommitResponse) error
	Done(req *DoneRequest, resp *DoneResponse) error
	Restore(req *RestoreRequest, resp *RestoreResponse) error
	ProbeStorage(req *ProbeStorageRequest, resp *ProbeStorageResponse) error
}

// helloOnlyWorker is the Cycle-1 stub: Hello returns the worker's identity;
// every other method returns "not implemented" so the caller surfaces
// a clear error if it tries to use them too early. replaces this
// with a real implementation under cmd/pgsafe.
type helloOnlyWorker struct {
	WorkerVersion string
	PGDataPath    string
}

func (w *helloOnlyWorker) Hello(_ *HelloRequest, resp *HelloResponse) error {
	resp.WorkerVersion = w.WorkerVersion
	resp.ProtocolVersion = Version
	resp.OS = runtime.GOOS
	resp.NumCPU = runtime.NumCPU()
	resp.PGDataPath = w.PGDataPath
	return nil
}

// errNotImplemented is the sentinel the Hello-only stub returns from any
// non-Hello method. Real workers won't return this.
var errNotImplemented = errors.New("worker rpc: method not implemented in Hello-only build")

func (*helloOnlyWorker) Configure(_ *ConfigureRequest, _ *ConfigureResponse) error {
	return errNotImplemented
}
func (*helloOnlyWorker) StreamFile(_ *StreamFileRequest, _ *StreamFileResponse) error {
	return errNotImplemented
}
func (*helloOnlyWorker) StreamChunk(_ *StreamChunkRequest, _ *StreamChunkResponse) error {
	return errNotImplemented
}
func (*helloOnlyWorker) WriteBlob(_ *WriteBlobRequest, _ *WriteBlobResponse) error {
	return errNotImplemented
}
func (*helloOnlyWorker) Restore(_ *RestoreRequest, _ *RestoreResponse) error {
	return errNotImplemented
}
func (*helloOnlyWorker) ProbeStorage(_ *ProbeStorageRequest, _ *ProbeStorageResponse) error {
	return errNotImplemented
}
func (*helloOnlyWorker) Commit(_ *CommitRequest, _ *CommitResponse) error {
	return errNotImplemented
}
func (*helloOnlyWorker) Done(_ *DoneRequest, _ *DoneResponse) error {
	return errNotImplemented
}

// NewHelloOnlyWorker returns the Cycle-1 stub WorkerService. Used by
// `pgsafe worker --stdio` until ships a real worker.
func NewHelloOnlyWorker(workerVersion, pgDataPath string) WorkerService {
	return &helloOnlyWorker{WorkerVersion: workerVersion, PGDataPath: pgDataPath}
}

// Serve registers the supplied WorkerService with a fresh net/rpc server,
// then runs a single-connection JSON-RPC serve loop over the supplied
// io.ReadWriteCloser. Returns when the connection EOFs (caller side
// closed) or the codec errors.
//
// `pgsafe worker --stdio` calls this with os.Stdin + os.Stdout wrapped as
// an io.ReadWriteCloser. Tests use rpcmock.Pair.RemoteConn().
func Serve(conn io.ReadWriteCloser, impl WorkerService) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("WorkerService", impl); err != nil {
		return fmt.Errorf("rpc: register: %w", err)
	}
	srv.ServeCodec(NewDualServerCodec(conn))
	// ServeCodec returns when the client closes the conn (normal exit) or
	// on a codec error. Either way, control returns here and the worker
	// process exits.
	return nil
}
