package main_test

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/transport/local"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
)

// TestLocalTransportEndToEnd proves the same-host hybrid-parallel
// transport: build the pgsafe binary, spawn it as `pgsafe worker
// stdio` via local.Dial (no SSH), wire its stdio into a rpc.Client,
// run a Hello roundtrip. If this test passes, an operator running
// `pgsafe backup --mode=hybrid-parallel` on the PG host (with no
// --ssh-target) reaches the worker without /usr/bin/ssh involvement.
//
// This is the v1 hard-requirement gate from CLAUDE / project memory:
// same-host hybrid-parallel must not require SSH.
func TestLocalTransportEndToEnd(t *testing.T) {
	t.Parallel()

	// Build pgsafe for the host's GOOS/GOARCH. Same-host = same OS, so
	// no cross-compile.
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "pgsafe")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build pgsafe: %v\n%s", err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sess, err := local.Dial(ctx, local.Options{Command: []string{binPath, "worker", "stdio"}})
	if err != nil {
		t.Fatalf("local.Dial: %v", err)
	}
	defer func() { _ = sess.Close() }()

	// Drain stderr so the worker doesn't block; surface anything it
	// said to the test log.
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := sess.StderrReader().Read(buf)
			if n > 0 {
				t.Logf("worker stderr: %s", buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()

	conn := &rwCloser{r: sess.StdoutReader(), w: sess.StdinWriter(), closer: sess}
	cli := rpc.NewClient(conn)
	defer func() { _ = cli.Close() }()

	resp, err := cli.Hello(rpc.HelloRequest{
		CallerVersion:   "v0-local-transport-test",
		ProtocolVersion: rpc.Version,
	})
	if err != nil {
		t.Fatalf("Hello over local transport: %v", err)
	}
	if resp.ProtocolVersion != rpc.Version {
		t.Errorf("ProtocolVersion = %q, want %q", resp.ProtocolVersion, rpc.Version)
	}
	if resp.NumCPU < 1 {
		t.Errorf("NumCPU = %d, want >= 1", resp.NumCPU)
	}
}

// rwCloser pairs a Reader and Writer into one ReadWriteCloser; the
// closer is the underlying session, which kills the worker subprocess
// on Close.
type rwCloser struct {
	r      io.Reader
	w      io.Writer
	closer io.Closer
}

func (c *rwCloser) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *rwCloser) Write(p []byte) (int, error) { return c.w.Write(p) }
func (c *rwCloser) Close() error                { return c.closer.Close() }
