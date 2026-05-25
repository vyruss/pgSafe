// Package ssh wraps /usr/bin/ssh as a subprocess and exposes its stdio as a
// Session that the JSON-RPC layer (internal/transport/rpc) reads/writes
// against. We deliberately delegate every SSH concern (key resolution,
// host-key pinning, ProxyJump, ControlMaster, agent forwarding) to the
// operator's system OpenSSH client. We add no flags beyond what the caller
// specifies in Options.ExtraArgs.
//
// Lifecycle: Dial spawns ssh; Close kills it (which propagates EOF to the
// remote command's stdin); Wait returns when the subprocess exits.
package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"
)

// cleanExitGrace is how long Close gives the remote command to exit on
// its own after we signal EOF on stdin, before SIGKILL'ing the ssh
// subprocess. Well-behaved workers (rpc.Serve, pgsafe worker stdio)
// exit on stdin EOF; SIGKILL'ing during that exit races with the
// natural completion and occasionally overwrites a clean exit status
// with "signal: killed". 2 seconds is long enough for well-behaved
// workers on any runner; pathological cases still get killed.
const cleanExitGrace = 2 * time.Second

// Options describes one ssh invocation.
type Options struct {
	// Target is what /usr/bin/ssh receives as the destination — typically
	// "user@host". The fixture's Topology.SSHTarget() returns the right
	// value for tests.
	Target string

	// ExtraArgs are passed to /usr/bin/ssh BEFORE the target. Use this for
	// non-default ports, IdentityFile pinning, UserKnownHostsFile pinning,
	// ConnectTimeout, ProxyJump, etc.
	ExtraArgs []string

	// Command is the remote command argv. The caller passes
	// {"pgsafe", "worker", "--stdio"} for hybrid-parallel mode. /usr/bin/ssh
	// concatenates this on its own argv so it lands as a single shell
	// command on the remote side; no special quoting needed for our argv
	// (no spaces or shell metacharacters in the production case).
	Command []string

	// dialFunc is a test-only seam: when set, Dial returns a *Session whose
	// stdio is whatever this function provides instead of /usr/bin/ssh's.
	// Production callers leave this nil.
	dialFunc func() (stdin io.WriteCloser, stdout io.Reader, stderr io.Reader, wait func() error, kill func(), err error)
}

// Session is one in-flight ssh subprocess. Construct via Dial; tear down via
// Close. Reads from Stdout get the remote command's stdout; writes to Stdin
// reach the remote command's stdin.
//
// Implements `internal/transport.Session` so the JSON-RPC layer can pick a
// cross-host (this) or same-host (`internal/transport/local`) worker
// transport interchangeably.
type Session struct {
	Stdin  io.WriteCloser
	Stdout io.Reader
	Stderr io.Reader

	wait func() error
	kill func()

	closeOnce sync.Once
	waitErr   error
}

// StdinWriter returns the worker's stdin. Implements transport.Session.
func (s *Session) StdinWriter() io.WriteCloser { return s.Stdin }

// StdoutReader returns the worker's stdout. Implements transport.Session.
func (s *Session) StdoutReader() io.Reader { return s.Stdout }

// StderrReader returns the worker's stderr. Implements transport.Session.
func (s *Session) StderrReader() io.Reader { return s.Stderr }

// Errors returned by Wait. Wrapping lets callers distinguish a remote-command
// non-zero exit from a transport-layer dial / signal issue.
var (
	ErrSSHTransport = errors.New("ssh: transport error")
	ErrRemoteExit   = errors.New("ssh: remote command exited non-zero")
)

// Dial spawns /usr/bin/ssh with the configured options. The returned Session
// is alive until Close is called or the remote process exits. The caller
// MUST eventually call Close (typically as a defer) to release the
// subprocess.
func Dial(ctx context.Context, opts Options) (*Session, error) {
	if opts.Target == "" {
		return nil, fmt.Errorf("%w: Target is required", ErrSSHTransport)
	}
	if len(opts.Command) == 0 {
		return nil, fmt.Errorf("%w: Command is required", ErrSSHTransport)
	}

	if opts.dialFunc != nil {
		stdin, stdout, stderr, wait, kill, err := opts.dialFunc()
		if err != nil {
			return nil, err
		}
		return &Session{Stdin: stdin, Stdout: stdout, Stderr: stderr, wait: wait, kill: kill}, nil
	}

	args := append([]string{}, opts.ExtraArgs...)
	args = append(args, opts.Target)
	args = append(args, opts.Command...)

	// /usr/bin/ssh is operator-supplied; we don't shell out to a shell here,
	// so there's no command-injection vector even if Target contained spaces.
	// gosec's G204 can't see that distinction.
	cmd := exec.CommandContext(ctx, "ssh", args...) //nolint:gosec
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, errors.Join(ErrSSHTransport, fmt.Errorf("stdin pipe: %w", err))
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, errors.Join(ErrSSHTransport, fmt.Errorf("stdout pipe: %w", err))
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, errors.Join(ErrSSHTransport, fmt.Errorf("stderr pipe: %w", err))
	}

	if err := cmd.Start(); err != nil {
		return nil, errors.Join(ErrSSHTransport, fmt.Errorf("start: %w", err))
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
				return errors.Join(ErrRemoteExit, err)
			}
			return errors.Join(ErrSSHTransport, err)
		},
		kill: func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		},
	}, nil
}

// Close stops the SSH subprocess. The first call signals EOF on stdin
// and gives the remote command cleanExitGrace to exit on its own; if it
// does not, the ssh subprocess is SIGKILL'd. All subsequent calls are
// no-ops.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		_ = s.Stdin.Close() // signal EOF to remote command
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

// Wait blocks until the remote command exits and returns its status. Returns
// nil for clean exit; an ErrRemoteExit-wrapped error for non-zero exit; an
// ErrSSHTransport-wrapped error for transport failures. Idempotent (returns
// the cached status on repeat calls).
func (s *Session) Wait() error {
	s.closeOnce.Do(func() {
		s.waitErr = s.wait()
	})
	return s.waitErr
}
