//go:build faults && integration_hybrid

// The cred-disk fault test is the load-bearing Tenet-3 verification. It
// asserts pgSafe's claim that scoped storage credentials shipped to the
// PG-host worker NEVER land on disk on the PG host — the credentials live
// only in the worker process's heap and are GC'd at process exit.
//
// Test design:
//
//  1. Spin up the sshtest container.
//  2. Build a Credential payload whose secret-bearing fields contain a
//     known, exhaustively-greppable sentinel string.
//  3. SSH in, exec `pgsafe worker stdio`, send a Configure with the
//     sentinel-bearing credential.
//  4. Tear down the SSH session — worker exits; its heap is freed.
//  5. SSH in again and grep every writable filesystem path on the PG
//     host for the sentinel. ANY hit is a Tenet-3 violation.
//
// We use the SFTP credential variant: the secret-bearing field
// (PrivateKeyPEM) is opaque-to-the-test data the worker parses but
// doesn't need to actually use. We point at a non-routable RFC-5737 host
// so the connection attempt fails fast — but the cred bytes still pass
// through every in-memory code path Configure exercises (Unmarshal,
// OpenBackendFromCredential → ssh.ParsePrivateKey).
package faults_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/transport/creds"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
	pgsafessh "github.com/vyruss/pgsafe/internal/transport/ssh"
	"github.com/vyruss/pgsafe/internal/transport/sshtest"
	"golang.org/x/crypto/ssh"
)

func TestCredentialNeverWrittenToDisk(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	if _, err := exec.LookPath("scp"); err != nil {
		t.Skip("scp not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)

	// Build + ship the worker.
	binDir := t.TempDir()
	binPath := binDir + "/pgsafe"
	build := exec.Command("go", "build", "-o", binPath, "../../cmd/pgsafe")
	cleanEnv := make([]string, 0, len(os.Environ()))
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "GOOS=") || strings.HasPrefix(e, "GOARCH=") {
			continue
		}
		cleanEnv = append(cleanEnv, e)
	}
	build.Env = append([]string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"}, cleanEnv...)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build pgsafe: %v\n%s", err, out)
	}

	const remoteBin = "/tmp/pgsafe"
	if err := scpUpload(topo, binPath, remoteBin); err != nil {
		t.Fatalf("scp upload: %v", err)
	}
	if err := chmodRemote(topo, remoteBin, "0755"); err != nil {
		t.Fatalf("chmod remote: %v", err)
	}

	// Sentinel-bearing Credential. We use the SFTP variant pointed at
	// 127.0.0.1:1 (loopback, no listener) so the worker's TCP dial fails
	// IMMEDIATELY with connection-refused (no SYN/ACK timeout to wait
	// through). The PEM bytes carry the sentinel verbatim and pass through
	// every in-memory code path Configure exercises.
	sentinel := "PGSAFE-TENET3-SENTINEL-" + randHex(t)
	credPEM := buildSentinelKeyPEM(t, sentinel)
	cred := creds.Credential{
		Type: creds.TypeSFTPKey,
		SFTPKey: &creds.SFTPKeyCredential{
			Host:                  "127.0.0.1",
			Port:                  1,
			Username:              "irrelevant",
			PrivateKeyPEM:         credPEM,
			BasePath:              "/srv/never-touched",
			InsecureIgnoreHostKey: true,
		},
	}
	credBytes, err := cred.Marshal()
	if err != nil {
		t.Fatalf("marshal cred: %v", err)
	}

	// Run a Configure round-trip against the worker.
	sess, err := pgsafessh.Dial(context.Background(), pgsafessh.Options{
		Target:    topo.SSHTarget(),
		ExtraArgs: topo.SSHExtraArgs(),
		Command:   []string{"sudo", "-u", "postgres", "-E", remoteBin, "worker", "stdio"},
	})
	if err != nil {
		t.Fatalf("ssh.Dial: %v", err)
	}
	conn := &sessionConn{sess: sess}
	cli := rpc.NewClient(conn)
	if _, err := cli.Hello(rpc.HelloRequest{
		CallerVersion:   "v0.0.0-faults",
		ProtocolVersion: rpc.Version,
	}); err != nil {
		_ = cli.Close()
		t.Fatalf("Hello: %v", err)
	}
	cfgResp, cfgErr := cli.Configure(rpc.ConfigureRequest{
		BackupID:         "tenet3-test",
		StorageType:      "sftp",
		Credentials:      credBytes,
		AgeRecipients:    []string{},
		CompressionCodec: "gzip",
	})
	// Configure error is expected (unreachable host); we just need the
	// cred bytes to have flowed through the worker's parser. The worker
	// returns its error inside ConfigureResponse.Error rather than as a
	// Go error; either way, the cred bytes passed through.
	t.Logf("Configure (expected error): err=%v Error=%q", cfgErr, cfgResp.Error)
	_ = cli.Close()
	_ = sess.Wait()

	// THE LOAD-BEARING ASSERTION.
	hits, err := grepRemote(topo, sentinel)
	if err != nil {
		t.Fatalf("remote grep: %v", err)
	}
	if hits != "" {
		t.Errorf("Tenet-3 violation: sentinel %q persisted to disk on PG host:\n%s",
			sentinel, hits)
	}
}

// scpUpload copies localPath to remotePath on the topology's container.
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
	out, err := exec.Command("scp", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("scp: %w\n%s", err, out)
	}
	return nil
}

func chmodRemote(topo *sshtest.Topology, remotePath, mode string) error {
	args := append([]string{}, topo.SSHExtraArgs()...)
	args = append(args, topo.SSHTarget(), "chmod", mode, remotePath)
	return exec.Command("ssh", args...).Run()
}

func topoPort(topo *sshtest.Topology) string {
	for i, a := range topo.SSHExtraArgs() {
		if a == "-p" && i+1 < len(topo.SSHExtraArgs()) {
			return topo.SSHExtraArgs()[i+1]
		}
	}
	return strconv.Itoa(topo.SSHPort)
}

// grepRemote runs `grep -rl <sentinel>` against the writable filesystem
// paths on the PG host and returns the matching file paths (one per line)
// or "" if nothing matched. grep exit-1 (no match) is success here; any
// other non-zero exit is a real error.
func grepRemote(topo *sshtest.Topology, sentinel string) (string, error) {
	scanRoots := []string{
		"/tmp", "/var/tmp", "/home", "/run", "/root",
		"/var/lib/postgresql", "/var/lib/pgsafe-store", "/var/lib/pgsafe-wal",
	}
	args := append([]string{}, topo.SSHExtraArgs()...)
	args = append(args, topo.SSHTarget(),
		"sudo", "grep", "-rlI", "--binary-files=without-match", sentinel)
	args = append(args, scanRoots...)
	out, err := exec.Command("ssh", args...).CombinedOutput()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && ee.ExitCode() == 1 {
			return "", nil // no matches
		}
		return "", fmt.Errorf("grep: %w\n%s", err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

// sessionConn adapts an *ssh.Session into io.ReadWriteCloser for jsonrpc.
type sessionConn struct {
	sess *pgsafessh.Session
}

func (c *sessionConn) Read(p []byte) (int, error)  { return c.sess.Stdout.Read(p) }
func (c *sessionConn) Write(p []byte) (int, error) { return c.sess.Stdin.Write(p) }
func (c *sessionConn) Close() error                { return c.sess.Close() }

// buildSentinelKeyPEM returns a PEM-encoded ed25519 private key whose
// preceding comment line contains the sentinel verbatim. ssh.ParsePrivateKey
// accepts files with leading comment lines.
func buildSentinelKeyPEM(t *testing.T, sentinel string) []byte {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	block, err := ssh.MarshalPrivateKey(priv, sentinel)
	if err != nil {
		t.Fatalf("MarshalPrivateKey: %v", err)
	}
	return append([]byte("# "+sentinel+"\n"), pem.EncodeToMemory(block)...)
}

func randHex(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return fmt.Sprintf("%x", b)
}
