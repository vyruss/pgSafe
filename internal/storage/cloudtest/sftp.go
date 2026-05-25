package cloudtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/ssh"
)

// SFTPEndpoint carries the connection details for the spun-up atmoz/sftp
// container. The production SFTP driver authenticates
// via SSH; tests use password auth for simplicity since pgSafe's filter
// chain encrypts payloads at rest before they hit any network anyway.
type SFTPEndpoint struct {
	Host     string
	Port     int
	Username string
	Password string
	BasePath string // server-side path the user has write access to
}

// StartSFTP launches an atmoz/sftp container with a known user/password and
// returns the endpoint. The container terminates when the test ends.
func StartSFTP(t *testing.T) SFTPEndpoint {
	t.Helper()
	ctx := context.Background()

	const sshPort = "22/tcp"
	const user, password = "pgsafe", "pgsafe"

	req := testcontainers.ContainerRequest{
		Image:        "atmoz/sftp:latest",
		ExposedPorts: []string{sshPort},
		Cmd:          []string{user + ":" + password + ":1001:1001:upload"},
		WaitingFor:   wait.ForListeningPort(sshPort).WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("atmoz/sftp start: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("Host: %v", err)
	}
	port, err := c.MappedPort(ctx, sshPort)
	if err != nil {
		t.Fatalf("MappedPort: %v", err)
	}

	return SFTPEndpoint{
		Host:     host,
		Port:     int(port.Num()),
		Username: user,
		Password: password,
		BasePath: "/upload",
	}
}

// NewSFTPClient dials the emulator and returns an *sftp.Client tied to a
// per-test SSH connection. Both connection and client are cleaned up on test
// end.
func NewSFTPClient(t *testing.T, ep SFTPEndpoint) *sftp.Client {
	t.Helper()
	cfg := &ssh.ClientConfig{
		User:            ep.Username,
		Auth:            []ssh.AuthMethod{ssh.Password(ep.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // test-only emulator
		Timeout:         10 * time.Second,
	}
	// atmoz/sftp's sshd occasionally accepts the TCP connection a few hundred
	// milliseconds before the SSH banner is ready, so the first Dial can race
	// the handshake and surface as `connection reset by peer`. Brief retry
	// with backoff is enough — emulator startup never takes more than ~2s.
	addr := fmt.Sprintf("%s:%d", ep.Host, ep.Port)
	var (
		conn *ssh.Client
		err  error
	)
	for i := 0; i < 6; i++ {
		conn, err = ssh.Dial("tcp", addr, cfg)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i+1) * 250 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("ssh.Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client, err := sftp.NewClient(conn)
	if err != nil {
		t.Fatalf("sftp.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}
