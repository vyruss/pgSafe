// Package local implements transport.Session by spawning a worker
// subprocess directly via `exec.Cmd` — the same-host counterpart to
// `internal/transport/ssh`. Used when `--ssh-target` is empty: the
// caller and the worker run on the same machine, the
// JSON-RPC channel is a pair of stdio pipes between them, and the
// SSH-over-loopback overhead (extra fork, TCP, crypto) is avoided.
//
// The lifecycle and stdio shape are identical to `transport/ssh.Session`
// so callers above the transport seam (rpc.NewClient, the
// hybrid-parallel caller) work against either implementation
// without any same-host special-case.
package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"
)

// cleanExitGrace is how long Close gives the worker to exit on its own
// after we signal EOF on stdin, before SIGKILL'ing. Many workers
// (rpc.Serve, /bin/cat) exit cleanly on stdin EOF and SIGKILL'ing them
// during that exit races with their natural completion, occasionally
// overwriting a clean exit status with "signal: killed". 2 seconds is
// long enough for any well-behaved worker on any runner; pathological
// workers still get killed.
const cleanExitGrace = 2 * time.Second

// Options describes one local-worker subprocess invocation.
type Options struct {
	// Command is the argv to exec. Production callers pass the
	// caller's own binary path plus {"worker", "stdio"}; tests
	// inject a freshly-built binary path.
	Command []string
}

// Session is one in-flight local-worker subprocess. Construct via Dial;
// tear down via Close. Implements transport.Session.
type Session struct {
	Stdin  io.WriteCloser
	Stdout io.Reader
	Stderr io.Reader

	wait func() error
	kill func()

	closeOnce sync.Once
	waitErr   error
}

// Errors returned by Wait. Same shape as `transport/ssh` so callers can
// distinguish a worker-side non-zero exit from a transport failure.
var (
	ErrTransport  = errors.New("local: transport error")
	ErrWorkerExit = errors.New("local: worker exited non-zero")
)

// Dial spawns the worker subprocess via `exec.CommandContext` with the
// configured argv. The returned Session is alive until Close is called
// or the worker exits. The caller MUST eventually call Close to release
// the subprocess.
func Dial(ctx context.Context, opts Options) (*Session, error) {
	if len(opts.Command) == 0 {
		return nil, fmt.Errorf("%w: Command is required", ErrTransport)
	}

	// opts.Command[0] is operator-supplied via --pgsafe-binary or the
	// caller's own argv[0]; not a shell pipeline.
	cmd := exec.CommandContext(ctx, opts.Command[0], opts.Command[1:]...) //nolint:gosec
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, errors.Join(ErrTransport, fmt.Errorf("stdin pipe: %w", err))
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, errors.Join(ErrTransport, fmt.Errorf("stdout pipe: %w", err))
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, errors.Join(ErrTransport, fmt.Errorf("stderr pipe: %w", err))
	}

	if err := cmd.Start(); err != nil {
		return nil, errors.Join(ErrTransport, fmt.Errorf("start: %w", err))
	}

	return &Session{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		wait: func() error {
			err := cmd.Wait()
			if err == nil {
				return nil
			}
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				return errors.Join(ErrWorkerExit, err)
			}
			return errors.Join(ErrTransport, err)
		},
		kill: func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		},
	}, nil
}

// StdinWriter returns the worker's stdin. Implements transport.Session.
func (s *Session) StdinWriter() io.WriteCloser { return s.Stdin }

// StdoutReader returns the worker's stdout. Implements transport.Session.
func (s *Session) StdoutReader() io.Reader { return s.Stdout }

// StderrReader returns the worker's stderr. Implements transport.Session.
func (s *Session) StderrReader() io.Reader { return s.Stderr }

// Close stops the worker subprocess. The first call signals EOF on
// stdin and gives the worker cleanExitGrace to exit on its own; if it
// does not, the process is SIGKILL'd. Subsequent calls are no-ops.
// Idempotent.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		_ = s.Stdin.Close()
		done := make(chan error, 1)
		go func() { done <- s.wait() }()
		select {
		case s.waitErr = <-done:
		case <-time.After(cleanExitGrace):
			s.kill()
			s.waitErr = <-done
		}
	})
	return s.waitErr
}

// Wait blocks until the worker subprocess exits and returns its status.
// Returns nil for clean exit; ErrWorkerExit-wrapped for non-zero;
// ErrTransport-wrapped for transport failures. Idempotent.
func (s *Session) Wait() error {
	s.closeOnce.Do(func() {
		s.waitErr = s.wait()
	})
	return s.waitErr
}
