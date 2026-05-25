package sftptunnel_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/pkg/sftp"
	"github.com/vyruss/pgsafe/internal/transport/sftptunnel"
	"golang.org/x/crypto/ssh"
)

// dialAsClient opens an *sftp.Client against a running EphemeralServer
// using the server's exposed private key for auth. Test helper.
func dialAsClient(t *testing.T, srv *sftptunnel.EphemeralServer) (*sftp.Client, func()) {
	t.Helper()
	signer, err := ssh.ParsePrivateKey(srv.ClientPrivateKeyPEM())
	if err != nil {
		t.Fatalf("ssh.ParsePrivateKey: %v", err)
	}
	cfg := &ssh.ClientConfig{
		User:            "pgsafe",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.LocalPort()))
	sshConn, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatalf("ssh.Dial: %v", err)
	}
	cli, err := sftp.NewClient(sshConn)
	if err != nil {
		_ = sshConn.Close()
		t.Fatalf("sftp.NewClient: %v", err)
	}
	cleanup := func() {
		_ = cli.Close()
		_ = sshConn.Close()
	}
	return cli, cleanup
}

func TestEphemeralServerRoundTrip(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx := context.Background()

	srv, err := sftptunnel.StartEphemeralServer(ctx, root)
	if err != nil {
		t.Fatalf("StartEphemeralServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	if srv.LocalPort() == 0 {
		t.Errorf("LocalPort = 0, want a kernel-picked port")
	}
	if len(srv.ClientPrivateKeyPEM()) == 0 {
		t.Errorf("ClientPrivateKeyPEM is empty")
	}

	cli, cleanup := dialAsClient(t, srv)
	defer cleanup()

	// Write a file. Worker side uses absolute paths under the storage
	// root (pgsafe's SFTP backend constructs path.Join(BasePath, name)).
	target := filepath.Join(root, "hello.txt")
	want := []byte("pgsafe-tunnel-roundtrip")
	f, err := cli.Create(target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify on disk under the served root.
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("disk content = %q, want %q", got, want)
	}

	// Read it back through SFTP.
	rf, err := cli.Open(target)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = rf.Close() }()
	buf := make([]byte, len(want)+8)
	n, _ := rf.Read(buf) // io.EOF is fine; pkg/sftp may return it with the read.
	if string(buf[:n]) != string(want) {
		t.Errorf("read content = %q, want %q", buf[:n], want)
	}
}

func TestEphemeralServerWrongKeyDenied(t *testing.T) {
	t.Parallel()
	srv, err := sftptunnel.StartEphemeralServer(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("StartEphemeralServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	// Generate a fresh keypair the server has never seen.
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	otherSigner, err := ssh.NewSignerFromKey(otherPriv)
	if err != nil {
		t.Fatalf("NewSignerFromKey: %v", err)
	}
	cfg := &ssh.ClientConfig{
		User:            "pgsafe",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(otherSigner)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.LocalPort()))
	if _, err := ssh.Dial("tcp", addr, cfg); err == nil {
		t.Fatal("ssh.Dial with wrong key: expected error, got nil")
	}
}

func TestEphemeralServerCloseStopsListener(t *testing.T) {
	t.Parallel()
	srv, err := sftptunnel.StartEphemeralServer(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("StartEphemeralServer: %v", err)
	}
	port := srv.LocalPort()
	signer, err := ssh.ParsePrivateKey(srv.ClientPrivateKeyPEM())
	if err != nil {
		t.Fatalf("ParsePrivateKey: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Subsequent dial should fail because the listener is gone.
	cfg := &ssh.ClientConfig{
		User:            "pgsafe",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	if _, err := ssh.Dial("tcp", addr, cfg); err == nil {
		t.Errorf("ssh.Dial after Close: expected error, got nil")
	}
}

func TestEphemeralServerCloseIdempotent(t *testing.T) {
	t.Parallel()
	srv, err := sftptunnel.StartEphemeralServer(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("StartEphemeralServer: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Errorf("second Close: %v (want nil)", err)
	}
}

func TestReverseForwardArgsArgvConstruction(t *testing.T) {
	t.Parallel()
	args := sftptunnel.ReverseForwardArgs(33333, 54321)
	want := []string{"-o", "ExitOnForwardFailure=yes", "-R", "33333:127.0.0.1:54321"}
	if len(args) != len(want) {
		t.Fatalf("argv = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("argv[%d] = %q, want %q", i, args[i], want[i])
		}
	}
}

func TestPickRemotePortInDynamicRange(t *testing.T) {
	t.Parallel()
	for i := 0; i < 100; i++ {
		p, err := sftptunnel.PickRemotePort()
		if err != nil {
			t.Fatalf("PickRemotePort: %v", err)
		}
		if p < 49152 || p > 65535 {
			t.Errorf("port %d out of dynamic range [49152, 65535]", p)
		}
	}
}
