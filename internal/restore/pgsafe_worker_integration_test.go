//go:build integration_hybrid

package restore_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/backup"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/filter/pagechecksum"
	"github.com/vyruss/pgsafe/internal/pg"
	"github.com/vyruss/pgsafe/internal/pg/conn"
	"github.com/vyruss/pgsafe/internal/restore"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/transport/sshtest"
)

// TestPGSafeWorkerRestoreEndToEnd takes a pgSafe-worker backup against
// the sshtest fixture, then runs the same caller-side dispatch
// for restore to materialise the backup on the worker's local
// filesystem (under the bind-mounted host storage so the test process
// can verify the result without SSH gymnastics). Asserts the standard
// cluster files round-trip cleanly.
func TestPGSafeWorkerRestoreEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	if _, err := exec.LookPath("scp"); err != nil {
		t.Skip("scp not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)
	ctx := context.Background()

	// Build pgsafe linux/amd64 and ship into the container — same
	// shape as the backup integration tests.
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "pgsafe")
	build := exec.Command("go", "build", "-o", binPath, "../../cmd/pgsafe")
	cleanEnv := make([]string, 0, len(os.Environ()))
	for _, e := range os.Environ() {
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
	if err := scpUpload(topo, binPath, remoteBin); err != nil {
		t.Fatalf("scp upload: %v", err)
	}
	if err := chmodRemote(topo, remoteBin, "0755"); err != nil {
		t.Fatalf("chmod remote: %v", err)
	}

	// Seed.
	conn2, err := conn.Connect(ctx, topo.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn2.Close()
	for _, sql := range []string{
		`CREATE TABLE hp_restore_demo (id int primary key, body text)`,
		`INSERT INTO hp_restore_demo SELECT g, repeat('z', 80) FROM generate_series(1, 200) g`,
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

	// Step 1: hybrid backup.
	backupRes, err := backup.Run(ctx, backup.Options{
		Cluster:     cluster,
		Backend:     hostBackend,
		Filter:      chain,
		Pool:        pool,
		Mode:        backup.ModeWorker,
		Server:      "demo",
		Label:       "pgsafe-hp-restore-test",
		WALTimeout:  60 * time.Second,
		Recipients:  []string{id.Recipient().String()},
		Compression: "gzip:0",
		Now:         time.Now,
		Worker: backup.WorkerOptions{
			Pool:             pool,
			PGVersion:        topo.Version,
			SSHTarget:        topo.SSHTarget(),
			SSHExtraArgs:     topo.SSHExtraArgs(),
			RemoteCommand:    []string{"sudo", "-u", "postgres", "-E", remoteBin, "worker", "stdio"},
			PageChecksumMode: pagechecksum.ModeOff,
			Storage: config.StorageConfig{
				Type: "posix",
				Path: topo.ContainerStoragePath,
			},
			WorkerWritesDirectly: false,
		},
	})
	if err != nil {
		t.Fatalf("backup.Run: %v", err)
	}
	t.Logf("backup OK: id=%s files=%d bytes=%d", backupRes.BackupID, backupRes.Files, backupRes.Bytes)

	// Step 2: hybrid restore. Worker writes the cluster files itself
	// — we don't pre-create the target on the host because the
	// worker runs as postgres uid (via sudo) and would hit a
	// permission error against a host-user-owned dir. The bind-mount
	// at topo.ContainerStoragePath is mode 0777, so a fresh
	// subdirectory created BY the worker lands fine.
	restoreTarget := filepath.Join(topo.ContainerStoragePath, "restored-"+backupRes.BackupID)
	defer func() {
		// Chmod -R 0777 from inside the container so the host user
		// can RemoveAll it; otherwise t.TempDir cleanup fights the
		// postgres-owned files.
		args := append([]string{}, topo.SSHExtraArgs()...)
		args = append(args, topo.SSHTarget(),
			"sudo", "-u", "postgres", "chmod", "-R", "0777", restoreTarget)
		_ = exec.Command("ssh", args...).Run()
		_ = os.RemoveAll(restoreTarget)
	}()

	restoreRes, err := restore.Run(ctx, restore.Options{
		Target:     restoreTarget,
		Identities: []age.Identity{id},
		BackupID:   backupRes.BackupID,
		Worker: &restore.WorkerOptions{
			SSHTarget:    topo.SSHTarget(),
			SSHExtraArgs: topo.SSHExtraArgs(),
			RemoteCommand: []string{
				"sudo", "-u", "postgres", "-E", remoteBin, "worker", "stdio",
			},
			CallerStorage: config.StorageConfig{
				Type: "posix",
				Path: topo.HostStoragePath,
			},
		},
	})
	if err != nil {
		t.Fatalf("restore.Run: %v", err)
	}
	t.Logf("restore OK: id=%s files=%d bytes=%d", restoreRes.BackupID, restoreRes.Files, restoreRes.Bytes)
	if restoreRes.Files < 50 {
		t.Errorf("Restore.Files = %d, want at least 50", restoreRes.Files)
	}

	// Verify standard cluster files materialised on the worker's
	// filesystem. Worker wrote them as postgres-uid in container
	// land; we verify via SSH+sudo (same pattern as the worker-side
	// backup integration test).
	containerRestoreDir := filepath.Join(topo.ContainerStoragePath, "restored-"+backupRes.BackupID)
	for _, want := range []string{"PG_VERSION", "global/pg_control", "backup_label", "backup_manifest"} {
		full := filepath.Join(containerRestoreDir, want)
		args := append([]string{}, topo.SSHExtraArgs()...)
		args = append(args, topo.SSHTarget(),
			"sudo", "-u", "postgres", "test", "-f", full)
		if err := exec.Command("ssh", args...).Run(); err != nil {
			t.Errorf("%s missing in restored cluster: %v", want, err)
		}
	}
}

func scpUpload(topo *sshtest.Topology, localPath, remotePath string) error {
	args := []string{
		"-P", strconv.Itoa(topo.SSHPort),
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
		return fmt.Errorf("scp %v: %s", err, out)
	}
	_ = io.Discard
	return nil
}

func chmodRemote(topo *sshtest.Topology, remotePath, mode string) error {
	args := append([]string{}, topo.SSHExtraArgs()...)
	args = append(args, topo.SSHTarget(), "chmod", mode, remotePath)
	return exec.Command("ssh", args...).Run()
}
