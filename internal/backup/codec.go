package backup

import (
	"fmt"
	"strings"

	"github.com/vyruss/pgsafe/internal/transport"
)

// codecFromString parses the codec name from a "<codec>:<level>" string
// constructed by the caller from cfg.Compression.{Codec,Level}.
func codecFromString(s string) string {
	if i := strings.Index(s, ":"); i >= 0 {
		return s[:i]
	}
	return s
}

// levelFromString parses the level integer from a "<codec>:<level>" string
// constructed by the caller from cfg.Compression.{Codec,Level}.
func levelFromString(s string) int {
	if i := strings.Index(s, ":"); i >= 0 && i+1 < len(s) {
		var n int
		_, _ = fmt.Sscanf(s[i+1:], "%d", &n)
		return n
	}
	return 0
}

// sessionConn adapts a transport.Session into io.ReadWriteCloser for
// jsonrpc. The same adapter handles SSH (cross-host) and local
// (same-host) sessions; rpc.NewClient sees an io.ReadWriteCloser
// either way.
type sessionConn struct {
	sess transport.Session
}

func (c *sessionConn) Read(p []byte) (int, error)  { return c.sess.StdoutReader().Read(p) }
func (c *sessionConn) Write(p []byte) (int, error) { return c.sess.StdinWriter().Write(p) }
func (c *sessionConn) Close() error                { return c.sess.Close() }
