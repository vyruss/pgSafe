// Package sftptunnel provides the caller-side machinery for the hybrid
// shapes' bulk-byte plane: a one-shot ephemeral SSH+SFTP server bound
// to caller's loopback, plus the ssh argv that lets a worker reach it
// through an SSH reverse port-forward.
//
// The worker dials the caller's loopback (via the worker's own
// loopback, courtesy of `ssh -R 0:127.0.0.1:M`) using pgsafe's
// existing SFTP storage backend with `host: localhost, port: <N>`,
// authenticating with a one-shot password generated per session. No
// persistent listener anywhere; lifetime ends with EphemeralServer.Close.
package sftptunnel

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// EphemeralServer wraps an SSH+SFTP server bound to caller's loopback,
// authenticated by a one-shot password. Lifetime ends with Close.
type EphemeralServer struct {
	listener      net.Listener
	root          string // POSIX root the SFTP subsystem serves
	clientPrivPEM []byte // pkg/ssh-format private key the worker uses to authenticate

	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
}

// StartEphemeralServer starts an SSH+SFTP server on 127.0.0.1:0 backed
// by `root` on the local filesystem. Returns when the listener is
// accepting connections; serve loop runs in the background.
//
// Authentication: an ephemeral ed25519 client keypair is generated;
// the public half is locked into the server's auth callback, the
// private half (PEM) is exposed via ClientPrivateKeyPEM for the
// caller to ship to the worker via the existing creds.SFTPKey path.
// Tenet 3: both halves are in-memory only and die with Close.
func StartEphemeralServer(ctx context.Context, root string) (*EphemeralServer, error) {
	clientPub, clientPrivPEM, err := newEd25519Keypair()
	if err != nil {
		return nil, fmt.Errorf("sftptunnel: client keypair: %w", err)
	}
	hostSigner, err := newHostKey()
	if err != nil {
		return nil, fmt.Errorf("sftptunnel: host key: %w", err)
	}

	authorizedKey := clientPub.Marshal()
	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if subtleByteEq(key.Marshal(), authorizedKey) {
				return &ssh.Permissions{}, nil
			}
			return nil, errors.New("sftptunnel: unauthorized public key")
		},
	}
	cfg.AddHostKey(hostSigner)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("sftptunnel: listen: %w", err)
	}

	srv := &EphemeralServer{
		listener:      ln,
		root:          root,
		clientPrivPEM: clientPrivPEM,
	}
	srv.wg.Add(1)
	go srv.serve(ctx, cfg)
	return srv, nil
}

// LocalPort returns the kernel-picked TCP port the server is bound to
// on caller's loopback.
func (s *EphemeralServer) LocalPort() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

// ClientPrivateKeyPEM returns the PEM-encoded private key the worker
// must present to authenticate. Ship via the RPC Configure
// (in-memory ephemeral).
func (s *EphemeralServer) ClientPrivateKeyPEM() []byte { return s.clientPrivPEM }

// Close terminates the listener and waits for the serve loop to exit.
// Idempotent — subsequent calls return nil.
func (s *EphemeralServer) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.listener.Close()
		s.wg.Wait()
	})
	return s.closeErr
}

// serve accepts connections until the listener is closed. Each connection
// gets its own goroutine that completes the SSH handshake, finds the
// requested SFTP subsystem, and runs pkg/sftp.Server against it.
func (s *EphemeralServer) serve(ctx context.Context, cfg *ssh.ServerConfig) {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Listener closed or fatal — exit the serve loop.
			return
		}
		s.wg.Add(1)
		go s.handleConn(ctx, conn, cfg)
	}
}

func (s *EphemeralServer) handleConn(_ context.Context, conn net.Conn, cfg *ssh.ServerConfig) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()

	_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		// Auth failure or protocol error — nothing more to do.
		return
	}
	go ssh.DiscardRequests(reqs)

	for newChan := range chans {
		if newChan.ChannelType() != "session" {
			_ = newChan.Reject(ssh.UnknownChannelType, "only sessions accepted")
			continue
		}
		ch, chReqs, err := newChan.Accept()
		if err != nil {
			continue
		}
		s.wg.Add(1)
		go s.handleSession(ch, chReqs)
	}
}

func (s *EphemeralServer) handleSession(ch ssh.Channel, reqs <-chan *ssh.Request) {
	defer s.wg.Done()
	defer func() { _ = ch.Close() }()

	for req := range reqs {
		switch req.Type {
		case "subsystem":
			// SSH "subsystem" request payload is a length-prefixed string.
			// pkg/sftp's expected subsystem name is "sftp".
			if len(req.Payload) < 4 {
				_ = req.Reply(false, nil)
				return
			}
			nameLen := int(req.Payload[0])<<24 | int(req.Payload[1])<<16 | int(req.Payload[2])<<8 | int(req.Payload[3])
			if nameLen+4 > len(req.Payload) {
				_ = req.Reply(false, nil)
				return
			}
			if string(req.Payload[4:4+nameLen]) != "sftp" {
				_ = req.Reply(false, nil)
				return
			}
			_ = req.Reply(true, nil)
			s.runSFTP(ch)
			return
		default:
			if req.WantReply {
				_ = req.Reply(false, nil)
			}
		}
	}
}

// runSFTP serves SFTP on a single SSH session channel. Server is
// rooted at s.root via the WithServerWorkingDirectory option.
func (s *EphemeralServer) runSFTP(rwc io.ReadWriteCloser) {
	server, err := sftp.NewServer(rwc, sftp.WithServerWorkingDirectory(s.root))
	if err != nil {
		return
	}
	_ = server.Serve()
	_ = server.Close()
}

// ReverseForwardArgs returns the ssh argv to add for a reverse port
// forward from worker:remotePort back to caller:127.0.0.1:callerPort.
// `ExitOnForwardFailure=yes` makes ssh fail loudly if the remote bind
// can't be acquired (rather than silently leaving the worker without
// a tunnel) — caller can react by retrying with a different
// remotePort.
func ReverseForwardArgs(remotePort, callerPort int) []string {
	return []string{
		"-o", "ExitOnForwardFailure=yes",
		"-R", fmt.Sprintf("%d:127.0.0.1:%d", remotePort, callerPort),
	}
}

// PickRemotePort returns a random TCP port number in the dynamic range
// (49152-65535) suitable for the worker-side end of a reverse forward.
// We pick instead of letting the kernel choose so we don't need to
// parse ssh stderr to learn the chosen port.
func PickRemotePort() (int, error) {
	const lo, hi = 49152, 65535
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("sftptunnel: rand for remote port: %w", err)
	}
	r := int(b[0])<<8 | int(b[1])
	return lo + r%(hi-lo+1), nil
}

// newHostKey generates an ed25519 host key for the ephemeral SSH server.
// One per session — the worker disables host-key checking when dialing
// loopback (the tunnel itself is the trust boundary).
func newHostKey() (ssh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return ssh.NewSignerFromKey(priv)
}

// newEd25519Keypair generates an ed25519 keypair for ephemeral
// client-auth on the EphemeralServer. Returns the ssh.PublicKey for
// the server's authorized-key check, and the OpenSSH PEM encoding of
// the private key for shipping to the worker via creds.SFTPKey.
func newEd25519Keypair() (ssh.PublicKey, []byte, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return nil, nil, err
	}
	pemBlock, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, nil, err
	}
	return sshPub, pem.EncodeToMemory(pemBlock), nil
}

// subtleByteEq is a constant-time byte equality check.
func subtleByteEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}
