//go:build integration_hybrid

package backup_test

import (
	"context"
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

// TestPGSafeWorkerWalgrab is the load-bearing test for WALSourceWalgrab:
// the worker on the PG host reads $PGDATA/pg_wal/<bracket-segs> directly
// after pg_backup_stop and ships them through the same StreamFile RPC
// pipeline as data files. The result lands at
// <storage>/<backup-id>/pg_wal/<seg> — exactly where restore looks
// first.
//
// What this test proves end-to-end:
//   - The validation in backup.Run accepts WALSource=walgrab + Mode=worker.
//   - AcquireBracketWAL skips the archive poll (this test would hang
//     forever otherwise — the worker has its own archive_command but
//     pgsafe doesn't depend on it landing).
//   - The worker's StreamFile path-shape carve-out (isPostStopWALPath)
//     accepts pg_wal/<segname> even though the segment is not in
//     Configure's file list.
//   - The bracket WAL ends up in the storage at the right path.
//
// Without this test, the walgrab path could regress silently — every
// previous step has unit coverage but the integration of "worker
// receives unfamiliar path; ships bytes; they land at <backup-id>/pg_wal/"
// only fires on a real backup run.
func TestPGSafeWorkerWalgrab(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	if _, err := exec.LookPath("scp"); err != nil {
		t.Skip("scp not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)
	ctx := context.Background()

	// chmod the storage tree before t.TempDir cleanup runs. The
	// worker writes <backup-id>/pg_wal/<seg> as the postgres user
	// (uid 999) with mode 0750 — the test host user can't traverse
	// it, so go's t.TempDir RemoveAll fails with "permission denied"
	// and the test goes red even though the assertions all passed.
	// Registering this cleanup AFTER sshtest sets up topo means it
	// runs FIRST (LIFO), with the container still alive.
	t.Cleanup(func() {
		args := append([]string{}, topo.SSHExtraArgs()...)
		args = append(args, topo.SSHTarget(),
			"sudo", "-u", "postgres", "chmod", "-R", "a+rwX", topo.ContainerStoragePath)
		_ = exec.Command("ssh", args...).Run()
	})

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
	if err := scpUploadHybrid(topo, binPath, remoteBin); err != nil {
		t.Fatalf("scp upload: %v", err)
	}
	if err := chmodRemoteHybrid(topo, remoteBin, "0755"); err != nil {
		t.Fatalf("chmod remote: %v", err)
	}

	// Tiny seed dataset — walgrab is independent of cluster size; we just
	// need a real backup to flow.
	conn2, err := conn.Connect(ctx, topo.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn2.Close()
	for _, sql := range []string{
		`CREATE TABLE walgrab_demo (id int primary key, body text)`,
		`INSERT INTO walgrab_demo SELECT g, 'r-'||g FROM generate_series(1, 50) g`,
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

	res, err := backup.Run(ctx, backup.Options{
		Cluster:     cluster,
		Backend:     hostBackend,
		Filter:      chain,
		Pool:        pool,
		Mode:        backup.ModeWorker,
		WALSource:   backup.WALSourceWalgrab,
		Server:      "demo",
		Label:       "pgsafe-walgrab-test",
		WALTimeout:  60 * time.Second,
		Recipients:  []string{id.Recipient().String()},
		Compression: "gzip:0",
		Now:         time.Now,
		Worker: backup.WorkerOptions{
			Pool:          pool,
			PGVersion:     topo.Version,
			SSHTarget:     topo.SSHTarget(),
			SSHExtraArgs:  topo.SSHExtraArgs(),
			RemoteCommand: []string{"sudo", "-u", "postgres", "-E", remoteBin, "worker", "stdio"},
			Storage: config.StorageConfig{
				Type: "posix",
				Path: topo.ContainerStoragePath,
			},
			PageChecksumMode:     pagechecksum.ModeOff,
			WorkerWritesDirectly: true,
		},
	})
	if err != nil {
		t.Fatalf("backup.Run walgrab: %v", err)
	}

	// The bracket segment must exist at <backup-id>/pg_wal/<seg> in
	// the storage, written by the worker via StreamFile. We assert
	// at least one entry exists (count depends on bracket-spanning
	// behavior; a tiny test cluster usually produces 1).
	containerBackupDir := topo.ContainerStoragePath + "/" + res.BackupID
	args := append([]string{}, topo.SSHExtraArgs()...)
	args = append(args, topo.SSHTarget(),
		"sudo", "-u", "postgres", "ls", containerBackupDir+"/pg_wal/")
	out, err := exec.Command("ssh", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("walgrab: pg_wal/ missing or unreadable in storage: %v\n%s", err, out)
	}
	if len(out) == 0 {
		t.Fatalf("walgrab: <backup>/pg_wal/ empty — worker did not ship bracket segments")
	}
	t.Logf("walgrab backup OK: id=%s files=%d bytes=%d duration=%s; pg_wal entries:\n%s",
		res.BackupID, res.Files, res.Bytes, res.Duration, out)
}
