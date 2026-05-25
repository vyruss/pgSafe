package rpc

import (
	"errors"
	"fmt"
	"io"
	"net/rpc"
)

// gobMethods are the WorkerService methods whose request/response carry
// binary payloads — they ride the gob discriminator on the dual codec to
// avoid JSON+base64 inflation. Keep this list narrow: only methods that
// genuinely move bulk bytes belong here.
var gobMethods = []string{
	"WorkerService.StreamChunk", // worker → caller: encrypted file bytes
}

// Client is the caller-side handle. Wraps a *rpc.Client to give us
// strongly-typed methods rather than untyped Call("WorkerService.Hello", …)
// at every site.
type Client struct {
	rc     *rpc.Client
	closer io.Closer
}

// NewClient constructs a Client from a duplex io.ReadWriteCloser (typically
// the caller-side end of an SSH session's stdio pair).
func NewClient(conn io.ReadWriteCloser) *Client {
	return &Client{
		rc:     rpc.NewClientWithCodec(NewDualClientCodec(conn, gobMethods)),
		closer: conn,
	}
}

// Close shuts down the underlying rpc.Client and the connection. Idempotent.
func (c *Client) Close() error {
	if c.rc != nil {
		_ = c.rc.Close()
	}
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// Hello calls WorkerService.Hello, asserts the protocol-version match, and
// returns the worker's identity record. A version mismatch surfaces as a
// distinguishable error so the caller can fail fast with a clear
// operator-facing message.
func (c *Client) Hello(req HelloRequest) (HelloResponse, error) {
	var resp HelloResponse
	if err := c.rc.Call("WorkerService.Hello", &req, &resp); err != nil {
		return HelloResponse{}, fmt.Errorf("rpc Hello: %w", err)
	}
	if resp.ProtocolVersion != Version {
		return resp, fmt.Errorf("%w: caller=%q worker=%q",
			ErrProtocolMismatch, Version, resp.ProtocolVersion)
	}
	return resp, nil
}

// Configure calls WorkerService.Configure and returns the worker's response.
// Surfaces a non-empty Error field as a Go error.
func (c *Client) Configure(req ConfigureRequest) (ConfigureResponse, error) {
	var resp ConfigureResponse
	if err := c.rc.Call("WorkerService.Configure", &req, &resp); err != nil {
		return ConfigureResponse{}, fmt.Errorf("rpc Configure: %w", err)
	}
	if resp.Error != "" {
		return resp, fmt.Errorf("rpc Configure: worker error: %s", resp.Error)
	}
	return resp, nil
}

// StreamFile calls WorkerService.StreamFile.
func (c *Client) StreamFile(req StreamFileRequest) (StreamFileResponse, error) {
	var resp StreamFileResponse
	if err := c.rc.Call("WorkerService.StreamFile", &req, &resp); err != nil {
		return StreamFileResponse{}, fmt.Errorf("rpc StreamFile %s: %w", req.Path, err)
	}
	return resp, nil
}

// StreamChunk calls WorkerService.StreamChunk — used in caller-storage
// hybrid mode where the worker has no backend and just returns the encrypted
// bytes for the caller to write to its own local backend. Goes over
// the gob discriminator on the dual codec (see gobMethods above) to avoid
// JSON+base64 inflation on the bulk payload.
func (c *Client) StreamChunk(req StreamChunkRequest) (StreamChunkResponse, error) {
	var resp StreamChunkResponse
	if err := c.rc.Call("WorkerService.StreamChunk", &req, &resp); err != nil {
		return StreamChunkResponse{}, fmt.Errorf("rpc StreamChunk %s: %w", req.Path, err)
	}
	return resp, nil
}

// WriteBlob calls WorkerService.WriteBlob — used by hybrid mode for files
// that come from in-memory bytes (backup_label, tablespace_map, manifest,
// sidecar) rather than from $PGDATA.
func (c *Client) WriteBlob(req WriteBlobRequest) (WriteBlobResponse, error) {
	var resp WriteBlobResponse
	if err := c.rc.Call("WorkerService.WriteBlob", &req, &resp); err != nil {
		return WriteBlobResponse{}, fmt.Errorf("rpc WriteBlob %s: %w", req.RepoPath, err)
	}
	return resp, nil
}

// Commit calls WorkerService.Commit.
func (c *Client) Commit(tmp, final string) error {
	var resp CommitResponse
	req := CommitRequest{Tmp: tmp, Final: final}
	if err := c.rc.Call("WorkerService.Commit", &req, &resp); err != nil {
		return fmt.Errorf("rpc Commit %s→%s: %w", tmp, final, err)
	}
	return nil
}

// Done calls WorkerService.Done.
func (c *Client) Done() (DoneResponse, error) {
	var resp DoneResponse
	req := DoneRequest{}
	if err := c.rc.Call("WorkerService.Done", &req, &resp); err != nil {
		return DoneResponse{}, fmt.Errorf("rpc Done: %w", err)
	}
	return resp, nil
}

// ProbeStorage calls WorkerService.ProbeStorage. The caller
// uses this at session start to verify the worker can reach storage
// directly, surfacing the result in the topology log so operators
// notice if pgsafe falls back to caller-proxy.
func (c *Client) ProbeStorage(req ProbeStorageRequest) (ProbeStorageResponse, error) {
	var resp ProbeStorageResponse
	if err := c.rc.Call("WorkerService.ProbeStorage", &req, &resp); err != nil {
		return ProbeStorageResponse{}, fmt.Errorf("rpc ProbeStorage: %w", err)
	}
	return resp, nil
}

// Restore calls WorkerService.Restore. The worker reconstructs its
// source backend from req.Credentials, parses age identities from PEM
// bytes, and runs the existing restore.Run pipeline against
// req.TargetPath. resp.Error is non-empty iff something failed
// worker-side (the RPC pattern: errors travel in the response).
func (c *Client) Restore(req RestoreRequest) (RestoreResponse, error) {
	var resp RestoreResponse
	if err := c.rc.Call("WorkerService.Restore", &req, &resp); err != nil {
		return RestoreResponse{}, fmt.Errorf("rpc Restore: %w", err)
	}
	return resp, nil
}

// ErrProtocolMismatch is returned by Hello when the worker speaks a
// different wire-protocol version. Operators must deploy matching pgsafe
// binaries on the caller and worker hosts.
var ErrProtocolMismatch = errors.New("rpc: protocol version mismatch")
