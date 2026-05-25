//go:build integration_hybrid

package backup_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/backup"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/filter/pagechecksum"
	"github.com/vyruss/pgsafe/internal/pg"
	"github.com/vyruss/pgsafe/internal/pg/conn"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/transport/sshtest"
)

// TestPGSafeWorkerSFTPViaCaller exercises storage_reach=via_caller for
// SFTP storage. The single sshtest container plays two roles: PG-host
// worker (we ssh in to spawn `pgsafe worker stdio`) and the SFTP
// storage server (its sshd's built-in sftp subsystem accepts writes
// from the worker via key auth).
//
// The byte path with via_caller engaged:
//
//	worker (in container) → 127.0.0.1:remote_port (in container)
//	  → ssh tunnel back to host (caller)
//	  → host opens TCP to 127.0.0.1:SSHPort
//	  → container's sshd (same container as the worker)
//	  → sftp subsystem writes /home/pgsafe/storage/<backupID>/...
//
// Bytes leave the container, traverse the host's network stack, then
// re-enter the container. That round-trip is the whole point of
// caller-proxy mode and is what we're verifying actually works.
func TestPGSafeWorkerSFTPViaCaller(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	if _, err := exec.LookPath("scp"); err != nil {
		t.Skip("scp not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)
	ctx := context.Background()

	// Build + ship the pgsafe binary so the worker side can exec it.
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "pgsafe")
	build := exec.Command("go", "build", "-o", binPath, "../../cmd/pgsafe")
	cleanEnv := make([]string, 0, len(os.Environ()))
	for _, e := range os.Environ() {
		if len(e) > 5 && (strings.HasPrefix(e, "GOOS=") || strings.HasPrefix(e, "GOARCH=")) {
			continue
		}
		cleanEnv = append(cleanEnv, e)
	}
	build.Env = append([]string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"}, cleanEnv...)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build pgsafe: %v\n%s", err, out)
	}
	const remoteBin = "/tmp/pgsafe"
	if err := scpUploadHybrid(topo, binPath, remoteBin); err != nil {
		t.Fatalf("scp upload: %v", err)
	}
	if err := chmodRemoteHybrid(topo, remoteBin, "0755"); err != nil {
		t.Fatalf("chmod remote: %v", err)
	}

	// SFTP base path lives in /home/pgsafe (owned by the pgsafe user
	// the worker authenticates AS to the SFTP server, regardless of
	// which OS user the worker process runs as).
	const repoPath = "/home/pgsafe/storage"
	mkdirArgs := append([]string{}, topo.SSHExtraArgs()...)
	mkdirArgs = append(mkdirArgs, topo.SSHTarget(), "mkdir", "-p", repoPath)
	if out, err := exec.Command("ssh", mkdirArgs...).CombinedOutput(); err != nil {
		t.Fatalf("mkdir remote storage: %v\n%s", err, out)
	}

	// Seed dataset.
	conn2, err := conn.Connect(ctx, topo.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn2.Close()
	for _, sql := range []string{
		`CREATE TABLE hp_sftpvia_demo (id int primary key, body text)`,
		`INSERT INTO hp_sftpvia_demo SELECT g, repeat('z', 80) FROM generate_series(1, 200) g`,
		`CHECKPOINT`,
	} {
		if _, err := conn2.Exec(ctx, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	cluster, err := pg.Open(ctx, topo.SuperDSN)
	if err != nil {
		t.Fatalf("pg.Open: %v", err)
	}
	defer cluster.Close()

	pool, err := conn.Connect(ctx, topo.SuperDSN)
	if err != nil {
		t.Fatalf("Connect pool: %v", err)
	}
	defer pool.Close()

	id, _ := age.GenerateX25519Identity()
	chain, err := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})
	if err != nil {
		t.Fatalf("filter.NewChain: %v", err)
	}

	// Storage config from the CALLER's perspective: SFTP at the
	// container's host-mapped SSH port. With storage_reach=via_caller,
	// pgsafe will rewrite the worker-side host/port to loopback +
	// ephemeral and add `ssh -R` so worker traffic loops back here.
	sftpCfg := &config.SFTPConfig{
		Host:                  topo.SSHHost,
		Port:                  topo.SSHPort,
		Username:              "pgsafe",
		PrivateKeyFile:        topo.SSHKeyPath,
		BasePath:              repoPath,
		InsecureIgnoreHostKey: true,
	}
	storage := config.StorageConfig{Type: "sftp", SFTP: sftpCfg}

	// Worker writes data + manifest via the SFTP tunnel into its own
	// backend, but the caller still reads opts.Backend for the
	// WAL probe + WAL-wait + sidecar hashing. Point it at the
	// fixture's hostStorage so the WAL archive (written by PG's
	// archive_command into walArchive=hostStorage/wal/<TLI>) is
	// visible to the caller.
	hostBackend, err := posix.New(posix.Options{Root: topo.HostStoragePath})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := hostBackend.Open(ctx); err != nil {
		t.Fatalf("hostBackend Open: %v", err)
	}

	res, err := backup.Run(ctx, backup.Options{
		Cluster:     cluster,
		Backend:     hostBackend,
		Filter:      chain,
		Pool:        pool,
		Mode:        backup.ModeWorker,
		Server:      "demo",
		Label:       "pgsafe-hp-sftpvia-test",
		WALTimeout:  60 * time.Second,
		Recipients:  []string{id.Recipient().String()},
		Compression: "gzip:0",
		Now:         time.Now,
		Worker: backup.WorkerOptions{
			Pool:                 pool,
			PGVersion:            topo.Version,
			SSHTarget:            topo.SSHTarget(),
			SSHExtraArgs:         topo.SSHExtraArgs(),
			RemoteCommand:        []string{"sudo", "-u", "postgres", "-E", remoteBin, "worker", "stdio"},
			Storage:              storage,
			PageChecksumMode:     pagechecksum.ModeOff,
			WorkerWritesDirectly: true,
			StorageReach:         "via_caller",
		},
	})
	if err != nil {
		t.Fatalf("backup.Run hybrid (sftp via_caller): %v", err)
	}
	if res.Files < 50 {
		t.Errorf("Files = %d, want at least 50", res.Files)
	}

	// Verify expected files landed in the (in-container) SFTP storage by
	// listing them over the same ssh transport.
	for _, want := range []string{"backup_manifest", "Storage-Metadata.json", "PG_VERSION", "global/pg_control", "backup_label"} {
		args := append([]string{}, topo.SSHExtraArgs()...)
		args = append(args, topo.SSHTarget(), "test", "-f", filepath.Join(repoPath, res.BackupID, want))
		if err := exec.Command("ssh", args...).Run(); err != nil {
			t.Errorf("%s missing in SFTP storage (via_caller path): %v", want, err)
		}
	}

	t.Logf("pgsafe-worker + sftp via_caller backup OK: id=%s files=%d bytes=%d duration=%s",
		res.BackupID, res.Files, res.Bytes, res.Duration)
}
