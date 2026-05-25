package main

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/crypto/ssh"

	"github.com/vyruss/pgsafe/internal/config"
)

// newSSHClientConfig builds the *ssh.ClientConfig pgsafe uses to dial
// SFTP servers. Extracted from openSFTP so the auth-method selection
// and host-key callback choice can be unit-tested independently of
// network I/O.
//
// Compat-critical defaults:
//
//   - Host-key verification is STRICT by default. The operator must
//     supply a known host_key (parsed as authorized_keys-format) or
//     explicitly set insecure_ignore_host_key=true. There is no
//     "ssh -o StrictHostKeyChecking=ask" middle ground; pgsafe
//     refuses to dial without operator-supplied host-key data.
//   - Auth methods are appended in operator-listed order. Password
//     auth is permitted (some embedded NAS targets only do password)
//     but the operator must explicitly set it; we don't fall through
//     to keyboard-interactive or agent-forwarded credentials.
//   - SFTP v3 is the negotiated protocol (pkg/sftp default). pgsafe
//     does not depend on v4+ extensions like posix-rename — Backend
//     code uses os.Rename via SFTP's standard Rename, accepting the
//     small race window on servers without atomic-rename guarantees.
func newSSHClientConfig(c *config.SFTPConfig) (*ssh.ClientConfig, error) {
	auths := []ssh.AuthMethod{}
	if c.Password != "" {
		auths = append(auths, ssh.Password(c.Password))
	}
	if c.PrivateKeyFile != "" {
		key, err := os.ReadFile(c.PrivateKeyFile) //nolint:gosec // operator-supplied path by design
		if err != nil {
			return nil, fmt.Errorf("storage sftp: read key: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("storage sftp: parse key: %w", err)
		}
		auths = append(auths, ssh.PublicKeys(signer))
	}
	if len(auths) == 0 {
		return nil, errors.New("storage sftp: no auth method (set password or private_key_file)")
	}

	hostKeyCb := ssh.InsecureIgnoreHostKey() //nolint:gosec // gated by config below
	if !c.InsecureIgnoreHostKey {
		if c.HostKey == "" {
			return nil, errors.New("storage sftp: host_key required (or set insecure_ignore_host_key)")
		}
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(c.HostKey))
		if err != nil {
			return nil, fmt.Errorf("storage sftp: parse host_key: %w", err)
		}
		hostKeyCb = ssh.FixedHostKey(pub)
	}

	return &ssh.ClientConfig{
		User:            c.Username,
		Auth:            auths,
		HostKeyCallback: hostKeyCb,
	}, nil
}
