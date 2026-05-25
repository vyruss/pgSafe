package rpc_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"sync"
	"testing"

	pgsaferpc "github.com/vyruss/pgsafe/internal/transport/rpc"
)

// duplexPipe wires two io.Pipe pairs into a full-duplex
// io.ReadWriteCloser pair: one side's reads are the other's writes and
// vice versa. Used to drive a server codec and a client codec end-to-end
// without spawning a real subprocess.
type duplexPipe struct {
	r io.ReadCloser
	w io.WriteCloser
}

func (d *duplexPipe) Read(p []byte) (int, error)  { return d.r.Read(p) }
func (d *duplexPipe) Write(p []byte) (int, error) { return d.w.Write(p) }
func (d *duplexPipe) Close() error {
	_ = d.r.Close()
	return d.w.Close()
}

func newPipePair() (a, b io.ReadWriteCloser) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	return &duplexPipe{r: r1, w: w2}, &duplexPipe{r: r2, w: w1}
}

// echoSvc is a minimal RPC service used to round-trip both codecs.
// JSONEcho receives a struct and echoes it back; BulkEcho receives a
// []byte and returns its length plus the same bytes — exercises the
// "binary payload" path that motivates the gob codec.
type echoSvc struct{}

type StringEcho struct{ S string }

func (echoSvc) JSONEcho(req StringEcho, resp *StringEcho) error {
	resp.S = "echo:" + req.S
	return nil
}

type BulkRequest struct {
	Tag  string
	Data []byte
}

type BulkResponse struct {
	Tag    string
	Length int
	Echo   []byte
}

func (echoSvc) BulkEcho(req BulkRequest, resp *BulkResponse) error {
	resp.Tag = "ack:" + req.Tag
	resp.Length = len(req.Data)
	resp.Echo = append([]byte(nil), req.Data...)
	return nil
}

// FailEcho always returns an error — used to verify the error-path frames.
func (echoSvc) FailEcho(req StringEcho, _ *StringEcho) error {
	return errors.New("intentional failure: " + req.S)
}

// startServer spawns a goroutine that runs a fresh rpc.Server with the
// dual-codec on conn. Returns when the codec EOFs.
func startServer(t *testing.T, conn io.ReadWriteCloser) *sync.WaitGroup {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("Echo", echoSvc{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ServeCodec(pgsaferpc.NewDualServerCodec(conn))
	}()
	return &wg
}

func TestDualCodec_JSONOnly(t *testing.T) {
	srv, cli := newPipePair()
	wg := startServer(t, srv)
	defer wg.Wait()

	c := rpc.NewClientWithCodec(pgsaferpc.NewDualClientCodec(cli, nil))
	defer func() { _ = c.Close() }()

	var resp StringEcho
	if err := c.Call("Echo.JSONEcho", StringEcho{S: "hello"}, &resp); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.S != "echo:hello" {
		t.Errorf("got %q, want %q", resp.S, "echo:hello")
	}
}

func TestDualCodec_GobOnly(t *testing.T) {
	srv, cli := newPipePair()
	wg := startServer(t, srv)
	defer wg.Wait()

	c := rpc.NewClientWithCodec(pgsaferpc.NewDualClientCodec(cli, []string{"Echo.BulkEcho"}))
	defer func() { _ = c.Close() }()

	payload := bytes.Repeat([]byte("xyz"), 100_000) // 300 KB binary
	var resp BulkResponse
	if err := c.Call("Echo.BulkEcho", BulkRequest{Tag: "big", Data: payload}, &resp); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if resp.Tag != "ack:big" {
		t.Errorf("Tag = %q, want %q", resp.Tag, "ack:big")
	}
	if resp.Length != len(payload) {
		t.Errorf("Length = %d, want %d", resp.Length, len(payload))
	}
	if !bytes.Equal(resp.Echo, payload) {
		t.Errorf("Echo bytes don't round-trip (len got=%d want=%d)", len(resp.Echo), len(payload))
	}
}

// TestDualCodec_Mixed verifies a sequence of mixed-codec calls over the
// same client/server connection. JSON and gob requests must not poison
// each other's response frames.
func TestDualCodec_Mixed(t *testing.T) {
	srv, cli := newPipePair()
	wg := startServer(t, srv)
	defer wg.Wait()

	c := rpc.NewClientWithCodec(pgsaferpc.NewDualClientCodec(cli, []string{"Echo.BulkEcho"}))
	defer func() { _ = c.Close() }()

	for i := range 8 {
		// JSON method.
		var jr StringEcho
		if err := c.Call("Echo.JSONEcho", StringEcho{S: fmt.Sprintf("n=%d", i)}, &jr); err != nil {
			t.Fatalf("JSONEcho %d: %v", i, err)
		}
		want := fmt.Sprintf("echo:n=%d", i)
		if jr.S != want {
			t.Errorf("JSONEcho %d got %q, want %q", i, jr.S, want)
		}

		// Gob method, payload of varying size.
		payload := bytes.Repeat([]byte{byte(i)}, 1024*(i+1))
		var br BulkResponse
		if err := c.Call("Echo.BulkEcho", BulkRequest{Tag: "i", Data: payload}, &br); err != nil {
			t.Fatalf("BulkEcho %d: %v", i, err)
		}
		if br.Length != len(payload) || !bytes.Equal(br.Echo, payload) {
			t.Errorf("BulkEcho %d round-trip mismatch", i)
		}
	}
}

// TestDualCodec_ErrorPath confirms a server-side handler error surfaces
// as resp.Error (via the frame's errMsg field) without corrupting
// subsequent requests on the same connection.
func TestDualCodec_ErrorPath(t *testing.T) {
	srv, cli := newPipePair()
	wg := startServer(t, srv)
	defer wg.Wait()

	c := rpc.NewClientWithCodec(pgsaferpc.NewDualClientCodec(cli, nil))
	defer func() { _ = c.Close() }()

	var resp StringEcho
	err := c.Call("Echo.FailEcho", StringEcho{S: "boom"}, &resp)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got, want := err.Error(), "intentional failure: boom"; got != want {
		t.Errorf("err = %q, want %q", got, want)
	}

	// Connection must remain usable after an error frame.
	var ok StringEcho
	if err := c.Call("Echo.JSONEcho", StringEcho{S: "still alive"}, &ok); err != nil {
		t.Fatalf("post-error Call: %v", err)
	}
	if ok.S != "echo:still alive" {
		t.Errorf("post-error response = %q", ok.S)
	}
}
