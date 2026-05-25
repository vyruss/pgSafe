package sftp_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	pkgsftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	pgsafesftp "github.com/vyruss/pgsafe/internal/storage/sftp"
	"github.com/vyruss/pgsafe/internal/transport/sftptunnel"
)

// TestSFTPRoundTripAgainstVanillaV3Server stands up the in-process
// SSH+SFTP server pgsafe already uses for the SFTP-tunnel transport
// (sftptunnel.EphemeralServer) and runs the production SFTP backend
// through a Put/Commit/Get round trip against it. The server uses
// pkg/sftp's default options — SFTP protocol v3, no extensions, no
// posix-rename — so any pgsafe regression that depends on a v4+
// extension fails here at the protocol layer rather than only on
// older real-world servers.
//
// Compat-critical guarantees pinned:
//
//   - Backend.Put + Close work over plain SFTP write.
//   - Backend.Commit (rename tmp → final) works WITHOUT
//     posix-rename@openssh.com.
//   - Backend.Get streams back what Put wrote.
//
// A regression where pgsafe starts using PosixRename or relying on
// extension.Statvfs would surface here as a server-side
// "unsupported operation" error.
func TestSFTPRoundTripAgainstVanillaV3Server(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	srv, err := sftptunnel.StartEphemeralServer(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("StartEphemeralServer: %v", err)
	}
	defer func() { _ = srv.Close() }()

	signer, err := ssh.ParsePrivateKey(srv.ClientPrivateKeyPEM())
	if err != nil {
		t.Fatalf("parse server-issued client key: %v", err)
	}
	cfg := &ssh.ClientConfig{
		User:            "pgsafe",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // localhost stub
	}
	sshConn, err := ssh.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", srv.LocalPort()), cfg)
	if err != nil {
		t.Fatalf("ssh.Dial: %v", err)
	}
	defer func() { _ = sshConn.Close() }()

	client, err := pkgsftp.NewClient(sshConn)
	if err != nil {
		t.Fatalf("sftp.NewClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	b, err := pgsafesftp.New(pgsafesftp.Options{Client: client, BasePath: "/"})
	if err != nil {
		t.Fatalf("pgsafesftp.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}

	w, err := b.Put(ctx, "tmp.dat")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	want := []byte("hello-sftp-compat")
	if _, err := w.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := b.Commit(ctx, "tmp.dat", "final.dat"); err != nil {
		t.Fatalf("Commit (vanilla v3 server, no posix-rename): %v", err)
	}

	rc, err := b.Get(ctx, "final.dat")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round-trip mismatch:\n got %q\nwant %q", got, want)
	}
}
