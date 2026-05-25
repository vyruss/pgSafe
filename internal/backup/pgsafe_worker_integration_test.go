//go:build integration_hybrid

package backup_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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

// TestPGSafeWorkerEndToEnd is the load-bearing Cycle-4 gate. The
// caller on the test host SSHs into the sshtest container, runs the
// freshly-built `pgsafe worker stdio`, and drives a full pgSafe-mode
// backup against a POSIX shared backend. The test asserts the same
// post-conditions as the libpq callers — manifest committed
// atomically, sidecar present, recognizable cluster files in the storage.
func TestPGSafeWorkerEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	if _, err := exec.LookPath("scp"); err != nil {
		t.Skip("scp not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)
	ctx := context.Background()

	// Build a Linux/amd64 pgsafe binary and copy it into the container so
	// the caller can exec it via SSH as the remote command.
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "pgsafe")
	build := exec.Command("go", "build", "-o", binPath, "../../cmd/pgsafe")
	hostEnv := os.Environ()
	cleanEnv := make([]string, 0, len(hostEnv))
	for _, e := range hostEnv {
		if len(e) > 5 && (e[:5] == "GOOS=" || e[:7] == "GOARCH=") {
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

	// Seed a small dataset so the worker's filter chain processes real
	// page-checksum-bearing heap bytes.
	pgInst := topo // same fixture; reuse
	ctx2 := context.Background()
	conn2, err := conn.Connect(ctx2, pgInst.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn2.Close()
	for _, sql := range []string{
		`CREATE TABLE hp_demo (id int primary key, body text)`,
		`INSERT INTO hp_demo SELECT g, repeat('x', 80) FROM generate_series(1, 200) g`,
		`CHECKPOINT`,
	} {
		if _, err := conn2.Exec(ctx2, sql); err != nil {
			t.Fatalf("setup SQL %q: %v", sql, err)
		}
	}

	// Caller-side cluster + bracket pool.
	cluster, err := pg.Open(ctx, pgInst.SuperDSN)
	if err != nil {
		t.Fatalf("pg.Open: %v", err)
	}
	defer cluster.Close()

	pool, err := conn.Connect(ctx, pgInst.SuperDSN)
	if err != nil {
		t.Fatalf("Connect pool: %v", err)
	}
	defer pool.Close()

	// Caller-side POSIX backend at the HOST view of the shared dir.
	hostBackend, err := posix.New(posix.Options{Root: topo.HostStoragePath})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := hostBackend.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}

	id, _ := age.GenerateX25519Identity()
	chain, err := filter.NewChain(filter.Options{
		Codec:      "gzip",
		Recipients: []age.Recipient{id.Recipient()},
	})
	if err != nil {
		t.Fatalf("filter.NewChain: %v", err)
	}

	res, err := backup.Run(ctx, backup.Options{
		Cluster:     cluster,
		Backend:     hostBackend,
		Filter:      chain,
		Pool:        pool, // Invariant #5 reachability probe
		Mode:        backup.ModeWorker,
		Server:      "demo",
		Label:       "pgsafe-hp-test",
		WALTimeout:  60 * time.Second,
		Recipients:  []string{id.Recipient().String()},
		Compression: "gzip:0",
		Now:         time.Now,
		Worker: backup.WorkerOptions{
			Pool:         pool,
			PGVersion:    pgInst.Version,
			SSHTarget:    pgInst.SSHTarget(),
			SSHExtraArgs: pgInst.SSHExtraArgs(),
			// sudo -u postgres so the worker runs as $PGDATA's owner; the
			// `-E` flag preserves env (we don't actually need any, but it
			// avoids surprises if grows env-driven knobs).
			RemoteCommand: []string{"sudo", "-u", "postgres", "-E", remoteBin, "worker", "stdio"},
			Storage: config.StorageConfig{
				Type: "posix",
				Path: pgInst.ContainerStoragePath,
			},
			PageChecksumMode:     pagechecksum.ModeOff,
			WorkerWritesDirectly: true, // worker-side POSIX backend (the path this test exists to exercise)
		},
	})
	if err != nil {
		t.Fatalf("backup.Run hybrid: %v", err)
	}
	if res.Files < 50 {
		t.Errorf("Files = %d, want at least 50", res.Files)
	}

	// The container's worker wrote everything as the postgres user (mode
	// 0640 inside 0750 directories), so the test host user can't stat the
	// files directly — verify via SSH inside the container, where the
	// postgres user owns the tree.
	containerBackupDir := topo.ContainerStoragePath + "/" + res.BackupID
	for _, want := range []string{"backup_manifest", "Storage-Metadata.json", "PG_VERSION", "global/pg_control", "backup_label"} {
		full := containerBackupDir + "/" + want
		args := append([]string{}, topo.SSHExtraArgs()...)
		args = append(args, topo.SSHTarget(),
			"sudo", "-u", "postgres", "test", "-f", full)
		if err := exec.Command("ssh", args...).Run(); err != nil {
			t.Errorf("%s missing in container: %v", want, err)
		}
	}
	// backup_manifest.tmp must NOT exist (Invariant #2).
	args := append([]string{}, topo.SSHExtraArgs()...)
	args = append(args, topo.SSHTarget(),
		"sudo", "-u", "postgres", "test", "!", "-f",
		containerBackupDir+"/backup_manifest.tmp")
	if err := exec.Command("ssh", args...).Run(); err != nil {
		t.Errorf("backup_manifest.tmp leaked after Commit: %v", err)
	}

	t.Logf("hybrid-parallel backup OK: id=%s files=%d bytes=%d duration=%s",
		res.BackupID, res.Files, res.Bytes, res.Duration)

	// The worker created files as postgres (mode 0640 in 0750 dirs), which
	// blocks t.TempDir's RemoveAll cleanup at host-user perms. chmod-r-recursive
	// from inside the container (where postgres has authority) before
	// the test exits.
	chmodArgs := append([]string{}, topo.SSHExtraArgs()...)
	chmodArgs = append(chmodArgs, topo.SSHTarget(),
		"sudo", "-u", "postgres", "chmod", "-R", "0777", topo.ContainerStoragePath)
	_ = exec.Command("ssh", chmodArgs...).Run()
}

// scpUploadHybrid copies localPath onto the sshtest container at remotePath.
func scpUploadHybrid(topo *sshtest.Topology, localPath, remotePath string) error {
	args := []string{
		"-P", topoPortHybrid(topo),
		"-i", topo.SSHKeyPath,
		"-o", "UserKnownHostsFile=" + topo.SSHKnownHosts,
		"-o", "StrictHostKeyChecking=yes",
		"-o", "BatchMode=yes",
		localPath,
		topo.SSHTarget() + ":" + remotePath,
	}
	cmd := exec.Command("scp", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("scp %v: %w\nargs=%v\noutput=%s", err, err, args, out)
	}
	_ = io.Discard
	return nil
}

func chmodRemoteHybrid(topo *sshtest.Topology, remotePath, mode string) error {
	args := append([]string{}, topo.SSHExtraArgs()...)
	args = append(args, topo.SSHTarget(), "chmod", mode, remotePath)
	return exec.Command("ssh", args...).Run()
}

func topoPortHybrid(topo *sshtest.Topology) string {
	for i, a := range topo.SSHExtraArgs() {
		if a == "-p" && i+1 < len(topo.SSHExtraArgs()) {
			return topo.SSHExtraArgs()[i+1]
		}
	}
	return "22"
}
