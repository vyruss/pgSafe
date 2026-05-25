// Hybrid-parallel restore: caller hosts the source storage (via an
// in-process SFTP server tunnelled over SSH back through the worker's
// session); worker runs the existing restore.Run pipeline against
// its local $PGDATA. Bytes flow caller's POSIX storage → caller's SFTP
// server → SSH reverse-forward → worker's SFTP client → worker's
// reverse filter chain (decrypt → decompress) → local PGDATA write.
//
// Symmetric to internal/backup/hybridparallel.go's caller-side I/O
// path, run in reverse. Same sftptunnel mechanism, opposite filter
// direction.

package restore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/transport"
	"github.com/vyruss/pgsafe/internal/transport/creds"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
	"github.com/vyruss/pgsafe/internal/transport/sftptunnel"
	pgsafessh "github.com/vyruss/pgsafe/internal/transport/ssh"
)

// WorkerOptions extends Options for pgSafe-mode restore (worker on PG
// host). Embedded into restore.Options; non-nil presence triggers the
// worker dispatch path.
type WorkerOptions struct {
	// SSHTarget is the worker-host SSH target ("user@host"). Required.
	SSHTarget string

	// SSHExtraArgs are passed to /usr/bin/ssh in addition to the
	// reverse-forward setup added by sftptunnel.
	SSHExtraArgs []string

	// RemoteCommand is the argv the worker runs. Empty defaults to
	// {"pgsafe", "worker", "stdio"}.
	RemoteCommand []string

	// CallerStorage is the caller-local storage backend config to
	// expose via the SFTP tunnel. Only POSIX is supported in this
	// commit; SFTP/cloud variants land in a follow-up.
	CallerStorage config.StorageConfig
}

// runWorkerRestore is the caller-side body for pgSafe-mode
// restore. Mirrors backup.runWorkerBackup's caller-side I/O variant.
func runWorkerRestore(ctx context.Context, opts Options) (Result, error) {
	w := opts.Worker
	if w == nil {
		return Result{}, errors.New("restore: Worker options required")
	}
	if w.SSHTarget == "" {
		return Result{}, errors.New("restore: Worker.SSHTarget required")
	}
	if w.CallerStorage.Type != "posix" {
		return Result{}, fmt.Errorf("restore: pgsafe-worker only supports posix caller storage (got %q)", w.CallerStorage.Type)
	}

	sftpd, err := sftptunnel.StartEphemeralServer(ctx, w.CallerStorage.Path)
	if err != nil {
		return Result{}, fmt.Errorf("restore: sftptunnel start: %w", err)
	}
	defer func() { _ = sftpd.Close() }()

	remotePort, err := sftptunnel.PickRemotePort()
	if err != nil {
		return Result{}, fmt.Errorf("restore: pick remote port: %w", err)
	}
	fwdArgs := sftptunnel.ReverseForwardArgs(remotePort, sftpd.LocalPort())
	sshArgs := append(append([]string{}, fwdArgs...), w.SSHExtraArgs...)

	cmd := w.RemoteCommand
	if len(cmd) == 0 {
		cmd = []string{"pgsafe", "worker", "stdio"}
	}
	sess, err := pgsafessh.Dial(ctx, pgsafessh.Options{
		Target:    w.SSHTarget,
		ExtraArgs: sshArgs,
		Command:   cmd,
	})
	if err != nil {
		return Result{}, fmt.Errorf("restore: ssh.Dial: %w", err)
	}
	defer func() { _ = sess.Close() }()

	go drainStderr(sess.StderrReader())

	cli := rpc.NewClient(&hpRestoreSessionConn{sess: sess})
	defer func() { _ = cli.Close() }()

	if _, err := cli.Hello(rpc.HelloRequest{
		CallerVersion:   "pgsafe-restore",
		ProtocolVersion: rpc.Version,
	}); err != nil {
		return Result{}, fmt.Errorf("restore: rpc.Hello: %w", err)
	}

	credBytes, err := (creds.Credential{
		Type: creds.TypeSFTPKey,
		SFTPKey: &creds.SFTPKeyCredential{
			Host:                  "127.0.0.1",
			Port:                  remotePort,
			Username:              "pgsafe",
			PrivateKeyPEM:         sftpd.ClientPrivateKeyPEM(),
			BasePath:              w.CallerStorage.Path,
			InsecureIgnoreHostKey: true,
		},
	}).Marshal()
	if err != nil {
		return Result{}, fmt.Errorf("restore: marshal sftp tunnel creds: %w", err)
	}

	idBlobs := make([][]byte, 0, len(opts.Identities))
	for _, id := range opts.Identities {
		s, ok := id.(*age.X25519Identity)
		if !ok {
			return Result{}, fmt.Errorf("restore: unsupported identity type %T", id)
		}
		idBlobs = append(idBlobs, []byte(s.String()))
	}

	resp, err := cli.Restore(rpc.RestoreRequest{
		BackupID:       opts.BackupID,
		StorageType:    "sftp",
		Credentials:    credBytes,
		AgeIdentities:  idBlobs,
		TargetPath:     opts.Target,
		Workers:        opts.Workers,
		StandbyMode:    opts.StandbyMode,
		RestoreCommand: opts.RestoreCommand,
	})
	if err != nil {
		return Result{}, fmt.Errorf("restore: rpc.Restore: %w", err)
	}
	if resp.Error != "" {
		return Result{}, fmt.Errorf("restore: worker: %s", resp.Error)
	}
	return Result{
		BackupID: resp.BackupID,
		Files:    resp.Files,
		WAL:      resp.WAL,
		Bytes:    resp.Bytes,
	}, nil
}

func drainStderr(r io.Reader) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			_, _ = os.Stderr.Write(buf[:n])
		}
		if err != nil {
			return
		}
	}
}

// hpRestoreSessionConn adapts a transport.Session to io.ReadWriteCloser
// for net/rpc. Same shape as backup/hybridparallel.go's sessionConn.
type hpRestoreSessionConn struct {
	sess transport.Session
}

func (c *hpRestoreSessionConn) Read(p []byte) (int, error)  { return c.sess.StdoutReader().Read(p) }
func (c *hpRestoreSessionConn) Write(p []byte) (int, error) { return c.sess.StdinWriter().Write(p) }
func (c *hpRestoreSessionConn) Close() error                { return c.sess.Close() }
