// Package sshtest spins up a postgres:18 container with sshd enabled
// alongside, used by hybrid-parallel integration tests. The container
// runs PG normally (the standard pgtest prerequisites: wal_level=replica,
// archive_mode=on, etc.) AND an openssh-server bound to the test-host's
// loopback. Tests dial the SSH side via /usr/bin/ssh, drop a key in,
// exec a remote `pgsafe worker --stdio`, and speak JSON-RPC over the
// resulting stdio pair.
//
// pgtest fixture is the reference for the PG-side prerequisites;
// this package extends it with the SSH layer. We do NOT hide the cluster's
// archive_command setup or hba.conf bits — those still apply, since the
// hybrid-parallel caller still talks to PG via libpq from the test
// process for bracket.Start/Stop.
package sshtest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/ssh"
)

// Topology is a running PG-host container with sshd enabled, plus the
// connection metadata a test needs to: (a) reach PG via libpq, (b) ssh in
// as the worker user.
type Topology struct {
	// PG-side (matches pgtest.PG fields by convention).
	Version    int
	SuperDSN   string // postgres superuser
	DSN        string // pgsafe replication user
	WALArchive string // host path the cluster's archive_command writes to

	// SSH-side.
	SSHHost       string // 127.0.0.1
	SSHPort       int    // host-side mapped port
	SSHUser       string // "pgsafe"
	SSHKeyPath    string // private key on host filesystem (for /usr/bin/ssh -i)
	SSHKnownHosts string // pre-populated known_hosts file path

	// Hybrid-parallel POSIX shared backend: a single directory bind-mounted
	// at the same path inside the container as on the host. The caller
	// (host) opens its POSIX backend at HostStoragePath; the worker
	// (container) opens its POSIX backend at ContainerStoragePath. Both
	// resolve to the same on-disk files. Empty when the test didn't
	// request a shared backend.
	HostStoragePath      string
	ContainerStoragePath string
}

// SSHTarget returns the user@host:port string in the form /usr/bin/ssh
// accepts after applying the known_hosts file. Tests pass this to
// transport/ssh.Dial as Options.Target.
func (t *Topology) SSHTarget() string {
	return fmt.Sprintf("%s@%s", t.SSHUser, t.SSHHost)
}

// SSHExtraArgs returns the per-test SSH flags that pin to the fixture's port,
// key, and known_hosts file. These are passed to /usr/bin/ssh as additional
// argv before the remote command. The fixture deliberately keeps strict
// host-key checking ON — the known_hosts file makes that workable while
// still proving the production code path doesn't disable security.
func (t *Topology) SSHExtraArgs() []string {
	return []string{
		"-p", strconv.Itoa(t.SSHPort),
		"-i", t.SSHKeyPath,
		"-o", "UserKnownHostsFile=" + t.SSHKnownHosts,
		"-o", "StrictHostKeyChecking=yes",
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=10",
	}
}

// StartPG18WithSSH launches the combined container and returns the topology.
// Cleanup happens at t.Cleanup time. Wraps StartPGWithSSH for the most
// common case.
func StartPG18WithSSH(t *testing.T) *Topology {
	t.Helper()
	return StartPGWithSSH(t, 18)
}

// StartPGWithSSH builds + starts a custom postgres:<version> image with
// openssh-server preinstalled and a test-only authorized key.
func StartPGWithSSH(t *testing.T, version int) *Topology {
	t.Helper()
	if version < 13 || version > 18 {
		t.Fatalf("StartPGWithSSH: version %d not in [13..18]", version)
	}
	ctx := context.Background()

	// Generate a per-test ed25519 keypair on the host. Private key goes to
	// the test's tempdir; public key is baked into the image's authorized_keys
	// at build time. Per-test keys avoid any cross-test contamination.
	pubBytes, privPath := generateKeyPair(t)
	tdir := t.TempDir()

	// Build context with the Dockerfile and the public key file.
	buildDir := filepath.Join(tdir, "build")
	if err := os.MkdirAll(buildDir, 0o750); err != nil {
		t.Fatalf("mkdir build: %v", err)
	}
	if err := os.WriteFile(filepath.Join(buildDir, "authorized_keys"),
		pubBytes, 0o600); err != nil {
		t.Fatalf("write authorized_keys: %v", err)
	}
	if err := os.WriteFile(filepath.Join(buildDir, "Dockerfile"),
		[]byte(dockerfileFor(version)), 0o600); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	// Shared POSIX storage backend: caller and worker both write into
	// this directory. The bind-mount targets the SAME absolute path on both
	// sides so the operator-facing config can carry one storage.path value
	// (host caller and container worker resolve it identically). This
	// matches what a real same-host hybrid-parallel deployment looks like:
	// the caller and PG see the same path because they're on the same
	// filesystem.
	//
	// The cluster's WAL archive lives at <storage>/wal — the layout pgsafe
	// expects from PG's archive_command (storageWALDir in cmd/pgsafe). The
	// integration tests still receive the path via topo.WALArchive, so they
	// don't care where it sits; the operator-flow demo benefits because a
	// single storage.path key in YAML now covers both base backups and WAL.
	hostStorage := filepath.Join(tdir, "storage")
	walArchive := filepath.Join(hostStorage, "wal")
	if err := os.MkdirAll(walArchive, 0o777); err != nil { //nolint:gosec
		t.Fatalf("mkdir wal-archive: %v", err)
	}
	if err := os.Chmod(hostStorage, 0o777); err != nil { //nolint:gosec
		t.Fatalf("chmod storage: %v", err)
	}
	if err := os.Chmod(walArchive, 0o777); err != nil { //nolint:gosec
		t.Fatalf("chmod wal-archive: %v", err)
	}
	containerStorage := hostStorage

	// hba.conf script as in pgtest.
	hbaScript := filepath.Join(tdir, "00-pgsafe-replication-hba.sh")
	// The container's docker-entrypoint sources this script as the
	// postgres user (UID 999), but the host bind-mount carries the host
	// user's UID. We need world-read+exec so the container's postgres
	// user can read the file. gosec's G306 wants <=0o600; we suppress
	// because this is a genuinely-executable test fixture.
	hbaScriptMode := os.FileMode(0o755)
	if err := os.WriteFile(hbaScript, []byte(
		"#!/bin/sh\nset -e\n"+
			"echo 'host replication all all trust' >> \"$PGDATA/pg_hba.conf\"\n"+
			"echo 'host all all all trust' >> \"$PGDATA/pg_hba.conf\"\n"),
		hbaScriptMode); err != nil { //nolint:gosec
		t.Fatalf("write hba script: %v", err)
	}

	// PG postgres -c args (mirrors pgtest). archive_command writes into
	// walArchive/<TLI>/<seg>-<sha256-hex> matching pgsafe's
	// archive.SegmentKey layout (same shape pgbackrest uses, with
	// SHA-256 instead of SHA-1). The caller's WAL-wait
	// FindSegment lists wal/<TLI>/ and matches the prefix.
	archiveCmd := "f=%f; tli=$(echo $f | cut -c1-8); sha=$(sha256sum %p | cut -d' ' -f1); mkdir -p " + walArchive + "/$tli && chmod 0777 " + walArchive + "/$tli && test ! -f " + walArchive + "/$tli/$f-$sha && cp %p " + walArchive + "/$tli/$f-$sha && chmod 0666 " + walArchive + "/$tli/$f-$sha"
	pgArgs := []string{
		"postgres",
		"-c", "wal_level=replica",
		"-c", "archive_mode=on",
		"-c", "archive_command=" + archiveCmd,
	}
	if version >= 17 {
		pgArgs = append(pgArgs, "-c", "summarize_wal=on")
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    buildDir,
			Dockerfile: "Dockerfile",
			KeepImage:  true, // image rebuilds across tests; cache it
		},
		ExposedPorts: []string{"5432/tcp", "22/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       "postgres",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			// --allow-group-access makes $PGDATA mode 0750 so the
			// hybrid-parallel worker (member of the postgres group) can
			// read PG's files. PG 11+ supports this flag explicitly.
			"POSTGRES_INITDB_ARGS": "--encoding=UTF8 --locale=C --allow-group-access",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds,
				// walArchive lives inside hostStorage now — one bind-mount
				// covers both. Two overlapping mounts would duplicate work
				// and risk Docker mount-order surprises.
				hostStorage+":"+containerStorage+":rw",
				hbaScript+":/docker-entrypoint-initdb.d/00-pgsafe-replication-hba.sh:ro",
			)
		},
		Cmd: pgArgs,
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
			wait.ForListeningPort("22/tcp").
				WithStartupTimeout(60*time.Second),
		),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start PG+sshd container (PG %d): %v", version, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	// Resolve the host-side ports.
	pgHost, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("Host: %v", err)
	}
	pgPort, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("MappedPort 5432: %v", err)
	}
	sshPort, err := c.MappedPort(ctx, "22/tcp")
	if err != nil {
		t.Fatalf("MappedPort 22: %v", err)
	}

	superDSN := fmt.Sprintf("postgresql://postgres:postgres@%s:%s/postgres?sslmode=disable",
		pgHost, pgPort.Port())

	// Set up the pgsafe replication user (matches pgtest).
	conn, err := pgx.Connect(ctx, superDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		`CREATE USER pgsafe WITH PASSWORD 'pgsafe' REPLICATION LOGIN`,
		`GRANT pg_read_server_files TO pgsafe`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil &&
			!strings.Contains(err.Error(), "already exists") {
			t.Fatalf("setup: %s: %v", sql, err)
		}
	}
	backupDSN := strings.Replace(superDSN, "postgres:postgres@", "pgsafe:pgsafe@", 1)

	// Pre-populate known_hosts so /usr/bin/ssh accepts the container's host
	// key without prompting. Dial the SSH port and capture the server's host
	// key, then write it in known_hosts format.
	knownHosts := filepath.Join(tdir, "known_hosts")
	if err := writeKnownHosts(pgHost, int(sshPort.Num()), knownHosts); err != nil {
		t.Fatalf("known_hosts capture: %v", err)
	}

	return &Topology{
		Version:       version,
		SuperDSN:      superDSN,
		DSN:           backupDSN,
		WALArchive:    walArchive,
		SSHHost:       pgHost,
		SSHPort:       int(sshPort.Num()),
		SSHUser:       "pgsafe",
		SSHKeyPath:    privPath,
		SSHKnownHosts: knownHosts,
		// Note: SSHUser changed from "pgsafe" to "postgres" so the worker
		// has read access to $PGDATA (owned by postgres:postgres in the
		// container). The shipped binary lands at /var/lib/postgresql/pgsafe.
		HostStoragePath:      hostStorage,
		ContainerStoragePath: containerStorage,
	}
}

// Updated: ssh user is now `postgres` (matches PG's own UID, has read on
// $PGDATA). The SSH key landing path moved accordingly.

// dockerfileFor returns the Dockerfile body for a given PG major version.
// We install openssh-server, drop the test-supplied authorized_keys for the
// `pgsafe` user, and rewrite the entrypoint to start sshd alongside PG.
func dockerfileFor(version int) string {
	return fmt.Sprintf(`FROM postgres:%d

USER root

# openssh-server runs alongside PG inside the same container. The base PG
# entrypoint is preserved; we wrap it so sshd starts in the background just
# before the PG entrypoint takes the foreground.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openssh-server sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /run/sshd && \
    useradd -m -s /bin/bash -G postgres pgsafe && \
    mkdir -p /home/pgsafe/.ssh && \
    chmod 700 /home/pgsafe/.ssh && \
    echo 'pgsafe ALL=(postgres) NOPASSWD: ALL' > /etc/sudoers.d/pgsafe && \
    chmod 440 /etc/sudoers.d/pgsafe

COPY authorized_keys /home/pgsafe/.ssh/authorized_keys
RUN chmod 600 /home/pgsafe/.ssh/authorized_keys && \
    chown -R pgsafe:pgsafe /home/pgsafe/.ssh && \
    ssh-keygen -A

# Allow PasswordAuthentication off; PubkeyAuthentication on (defaults are
# usually fine but we lock them in for test reproducibility).
RUN sed -i 's/^#*PasswordAuthentication.*$/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    sed -i 's/^#*PubkeyAuthentication.*$/PubkeyAuthentication yes/' /etc/ssh/sshd_config

# Wrapper entrypoint: launch sshd, then exec the original PG entrypoint.
RUN printf '#!/bin/sh\nset -e\n/usr/sbin/sshd -D &\nexec docker-entrypoint.sh "$@"\n' > /usr/local/bin/pgsafe-entry.sh && \
    chmod 755 /usr/local/bin/pgsafe-entry.sh

ENTRYPOINT ["/usr/local/bin/pgsafe-entry.sh"]
CMD ["postgres"]
`, version)
}

// generateKeyPair writes a new ed25519 private key to the test's tempdir
// (mode 0600) and returns the OpenSSH-format authorized_keys line for it.
func generateKeyPair(t *testing.T) (authorizedKeys []byte, privPath string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	// Marshal the private key in OpenSSH PEM format.
	privBlock, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatalf("ssh.MarshalPrivateKey: %v", err)
	}
	tdir := t.TempDir()
	privPath = filepath.Join(tdir, "id_ed25519")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(privBlock), 0o600); err != nil {
		t.Fatalf("write priv: %v", err)
	}

	// authorized_keys line.
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatalf("ssh.NewPublicKey: %v", err)
	}
	return ssh.MarshalAuthorizedKey(sshPub), privPath
}

// writeKnownHosts dials the SSH port, captures the server's host key from the
// initial KEX, and writes it as a known_hosts line so subsequent /usr/bin/ssh
// invocations accept the host with StrictHostKeyChecking=yes.
func writeKnownHosts(host string, port int, dest string) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	cfg := &ssh.ClientConfig{
		User: "probe",
		Auth: []ssh.AuthMethod{ssh.Password("ignored")},
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
			line := ssh.MarshalAuthorizedKey(key)
			entry := fmt.Sprintf("[%s]:%d %s", host, port, strings.TrimSpace(string(line)))
			return os.WriteFile(dest, []byte(entry+"\n"), 0o600)
		},
		Timeout: 10 * time.Second,
	}
	// We expect auth to fail; we only need the host key, captured by the
	// callback above.
	conn, err := ssh.Dial("tcp", addr, cfg)
	if err == nil {
		_ = conn.Close()
	}
	if _, statErr := os.Stat(dest); statErr != nil {
		return fmt.Errorf("known_hosts not written: %w (dial err: %w)", statErr, err)
	}
	return nil
}
