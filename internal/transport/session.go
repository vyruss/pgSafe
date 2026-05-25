// Package transport defines the cross-host stdio session abstraction
// shared by `internal/transport/ssh` (cross-host hybrid-parallel) and
// `internal/transport/local` (same-host hybrid-parallel). The
// caller picks one based on whether `--ssh-target` is set; the
// JSON-RPC client/server layer above it doesn't care which.
package transport

import "io"

// Session is one in-flight worker subprocess. The caller reads
// the worker's stdout, writes to its stdin, and drains stderr (for
// operator-visible logging). Close kills the subprocess; Wait blocks
// until it exits and returns the exit status.
type Session interface {
	// StdinWriter is the worker's stdin. The caller writes
	// JSON-RPC requests here.
	StdinWriter() io.WriteCloser

	// StdoutReader is the worker's stdout. The caller reads
	// JSON-RPC responses here.
	StdoutReader() io.Reader

	// StderrReader is the worker's stderr. Drained in a background
	// goroutine so the worker doesn't block on a full pipe and so
	// operator-visible diagnostics surface.
	StderrReader() io.Reader

	// Wait blocks until the worker subprocess exits and returns its
	// status. Idempotent.
	Wait() error

	// Close stops the worker subprocess. The first call closes stdin
	// (signalling EOF to the worker), kills the subprocess, and
	// returns Wait()'s status. Subsequent calls are no-ops.
	Close() error
}
