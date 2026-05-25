//go:build integration_hybrid

package ssh_test

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/vyruss/pgsafe/internal/transport/rpc"
	pgsafessh "github.com/vyruss/pgsafe/internal/transport/ssh"
	"github.com/vyruss/pgsafe/internal/transport/sshtest"
)

// TestDialAndExecRemoteCommand spawns /usr/bin/ssh against the sshtest
// fixture, runs `echo hello`, asserts the output and a clean exit. Used as
// the load-bearing smoke test that the production transport package can
// drive an out-of-process SSH subprocess against the per-test container.
func TestDialAndExecRemoteCommand(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)

	sess, err := pgsafessh.Dial(context.Background(), pgsafessh.Options{
		Target:    topo.SSHTarget(),
		ExtraArgs: topo.SSHExtraArgs(),
		Command:   []string{"echo", "hello-from-remote"},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = sess.Close() }()

	out := make([]byte, 256)
	n, _ := sess.Stdout.Read(out)
	if got := string(out[:n]); got == "" || got[:len("hello-from-remote")] != "hello-from-remote" {
		t.Errorf("remote stdout = %q, want \"hello-from-remote\\n\"", got)
	}

	if err := sess.Wait(); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

// TestRemoteWorkerHello — the load-bearing Cycle-1 gate. We:
//
//  1. Build the pgsafe binary on the host.
//  2. Copy it into the sshtest container at a known path.
//  3. SSH in and exec `pgsafe worker --stdio`.
//  4. Send a Hello over the SSH stdio pair via internal/transport/rpc.
//  5. Assert the worker reports the matching protocol version.
//  6. Close the session — the worker exits cleanly within the SSH Wait.
func TestRemoteWorkerHello(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	if _, err := exec.LookPath("scp"); err != nil {
		t.Skip("scp not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)

	// Build a Linux/amd64 binary so it runs inside the postgres:18 (debian)
	// container regardless of the host OS.
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "pgsafe")
	build := exec.Command("go", "build", "-o", binPath, "../../../cmd/pgsafe")
	build.Env = append([]string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"}, environForBuild()...)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build pgsafe binary: %v\n%s", err, out)
	}

	// Copy to /home/pgsafe/pgsafe inside the container.
	if err := scpUpload(topo, binPath, "/home/pgsafe/pgsafe"); err != nil {
		t.Fatalf("scp upload: %v", err)
	}
	if err := chmodRemote(topo, "/home/pgsafe/pgsafe", "0755"); err != nil {
		t.Fatalf("chmod remote: %v", err)
	}

	// Dial + speak RPC.
	sess, err := pgsafessh.Dial(context.Background(), pgsafessh.Options{
		Target:    topo.SSHTarget(),
		ExtraArgs: topo.SSHExtraArgs(),
		Command:   []string{"/home/pgsafe/pgsafe", "worker", "stdio"},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = sess.Close() }()

	helloOK(t, sess)
}

// helloOK runs one Hello RPC against the session and asserts the response.
// Factored out so tests can reuse the shape.
func helloOK(t *testing.T, sess *pgsafessh.Session) {
	t.Helper()

	conn := &sessionConn{sess: sess}
	c := rpc.NewClient(conn)
	defer func() { _ = c.Close() }()

	resp, err := c.Hello(rpc.HelloRequest{
		CallerVersion:   "v0.0.0-test",
		ProtocolVersion: rpc.Version,
	})
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if resp.ProtocolVersion != rpc.Version {
		t.Errorf("worker ProtocolVersion = %q, want %q", resp.ProtocolVersion, rpc.Version)
	}
	if resp.OS == "" {
		t.Errorf("worker OS empty in %+v", resp)
	}
}

// sessionConn adapts an *ssh.Session into io.ReadWriteCloser for jsonrpc.
type sessionConn struct {
	sess *pgsafessh.Session
}

func (c *sessionConn) Read(p []byte) (int, error)  { return c.sess.Stdout.Read(p) }
func (c *sessionConn) Write(p []byte) (int, error) { return c.sess.Stdin.Write(p) }
func (c *sessionConn) Close() error                { return c.sess.Close() }

// scpUpload copies localPath onto the sshtest container at remotePath using
// the per-test SSH key + known_hosts.
func scpUpload(topo *sshtest.Topology, localPath, remotePath string) error {
	args := []string{
		"-P", topoPort(topo),
		"-i", topo.SSHKeyPath,
		"-o", "UserKnownHostsFile=" + topo.SSHKnownHosts,
		"-o", "StrictHostKeyChecking=yes",
		"-o", "BatchMode=yes",
		localPath,
		topo.SSHTarget() + ":" + remotePath,
	}
	cmd := exec.Command("scp", args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

// chmodRemote runs `chmod <mode> <remotePath>` over ssh.
func chmodRemote(topo *sshtest.Topology, remotePath, mode string) error {
	args := append([]string{}, topo.SSHExtraArgs()...)
	args = append(args, topo.SSHTarget(), "chmod", mode, remotePath)
	return exec.Command("ssh", args...).Run()
}

// topoPort returns the SSH port as a decimal string for scp's -P flag.
func topoPort(topo *sshtest.Topology) string {
	args := topo.SSHExtraArgs()
	for i, a := range args {
		if a == "-p" && i+1 < len(args) {
			return args[i+1]
		}
	}
	return "22"
}

// environForBuild returns the host environment minus any GOOS/GOARCH so the
// build subcommand picks up our explicit cross-build env.
func environForBuild() []string {
	out := make([]string, 0, len(os.Environ()))
	for _, e := range os.Environ() {
		if len(e) > 5 && (e[:5] == "GOOS=" || e[:7] == "GOARCH=") {
			continue
		}
		out = append(out, e)
	}
	return out
}
