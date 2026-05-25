package local_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/transport"
	"github.com/vyruss/pgsafe/internal/transport/local"
)

// TestSessionImplementsTransport asserts local.Session satisfies the
// transport.Session interface at compile time.
func TestSessionImplementsTransport(t *testing.T) {
	t.Parallel()
	var _ transport.Session = (*local.Session)(nil)
}

// TestDialEchoes spawns /bin/cat as a worker stand-in, sends bytes to
// its stdin, and reads them back from stdout. Smoke-tests the
// stdin/stdout/stderr plumbing and lifecycle: Dial -> use -> Close.
func TestDialEchoes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sess, err := local.Dial(ctx, local.Options{Command: []string{"/bin/cat"}})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = sess.Close() }()

	want := []byte("hello pgsafe\n")
	if _, err := sess.StdinWriter().Write(want); err != nil {
		t.Fatalf("stdin Write: %v", err)
	}
	if err := sess.StdinWriter().Close(); err != nil {
		t.Fatalf("stdin Close: %v", err)
	}

	got, err := io.ReadAll(sess.StdoutReader())
	if err != nil {
		t.Fatalf("stdout ReadAll: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("echo: got %q, want %q", got, want)
	}

	if err := sess.Close(); err != nil {
		t.Errorf("Close after clean exit: %v", err)
	}
}

// TestDialNonexistentCommand surfaces ErrTransport when the binary path
// does not exist.
func TestDialNonexistentCommand(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sess, err := local.Dial(ctx, local.Options{Command: []string{"/no/such/binary"}})
	if err == nil {
		_ = sess.Close()
		t.Fatal("Dial nonexistent command: want error, got nil")
	}
	if !errors.Is(err, local.ErrTransport) {
		t.Errorf("err = %v, want wraps ErrTransport", err)
	}
}

// TestDialEmptyCommand rejects an empty argv before touching the OS.
func TestDialEmptyCommand(t *testing.T) {
	t.Parallel()
	_, err := local.Dial(context.Background(), local.Options{})
	if err == nil {
		t.Fatal("Dial empty Command: want error, got nil")
	}
	if !errors.Is(err, local.ErrTransport) {
		t.Errorf("err = %v, want wraps ErrTransport", err)
	}
}
