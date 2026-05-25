package ssh_test

import (
	"testing"

	"github.com/vyruss/pgsafe/internal/transport"
	"github.com/vyruss/pgsafe/internal/transport/ssh"
)

// TestSessionImplementsTransport asserts ssh.Session satisfies the
// transport.Session interface at compile time. added local
// and ssh as interchangeable transports; if either drifts from the
// shared shape this asserts on the cheap side of the test pyramid.
func TestSessionImplementsTransport(t *testing.T) {
	t.Parallel()
	var _ transport.Session = (*ssh.Session)(nil)
}
