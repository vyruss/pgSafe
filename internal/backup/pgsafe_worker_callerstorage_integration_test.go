//go:build integration_hybrid

package backup_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/backup"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/filter/pagechecksum"
	"github.com/vyruss/pgsafe/internal/pg"
	"github.com/vyruss/pgsafe/internal/pg/conn"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/transport/sshtest"
)

// TestPGSafeWorkerCallerStorageEndToEnd is the caller-storage
// counterpart of TestPGSafeWorkerEndToEnd. The worker on the PG host
// is a pure filter service; encrypted bytes flow back over the RPC's
// gob channel and the caller writes them to its OWN local POSIX
// backend (topo.HostStoragePath, owned by the host user — readable
// without SSH gymnastics).
//
// Topology mirrors the existing pgSafe-worker test (sshtest fixture):
//   - PG + sshd in the container
//   - host-side test process is the caller
//   - worker: ssh + sudo -u postgres + /tmp/pgsafe worker stdio
//
// What's new: WorkerOptions.WorkerWritesDirectly=false. The worker has
// no Backend; StreamChunk RPC carries encrypted bytes back to the
// caller over the dual codec's gob channel.
func TestPGSafeWorkerCallerStorageEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH")
	}
	if _, err := exec.LookPath("scp"); err != nil {
		t.Skip("scp not on PATH")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)
	ctx := context.Background()

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

	// Seed dataset.
	conn2, err := conn.Connect(ctx, topo.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn2.Close()
	for _, sql := range []string{
		`CREATE TABLE hp_orch_demo (id int primary key, body text)`,
		`INSERT INTO hp_orch_demo SELECT g, repeat('y', 80) FROM generate_series(1, 200) g`,
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

	// Caller-side POSIX backend. In caller-storage mode
	// this is THE backend — the worker has none. Lives on the host
	// filesystem at topo.HostStoragePath; bytes the worker filters land
	// here via RPC.
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

	// Capture caller stderr so we can assert the topology log
	// doesn't claim "worker→storage probe: UNREACHABLE" — that line
	// used to appear in this test before the ProbeStorage gating fix
	// because the caller unconditionally probed even when
	// WorkerWritesDirectly=false (worker has no backend role to
	// probe). UNREACHABLE-as-a-no-op was a footgun for operators.
	var stderrBuf bytes.Buffer
	res, err := backup.Run(ctx, backup.Options{
		Cluster:     cluster,
		Backend:     hostBackend,
		Filter:      chain,
		Pool:        pool,
		Mode:        backup.ModeWorker,
		Server:      "demo",
		Label:       "pgsafe-hp-orch-test",
		WALTimeout:  60 * time.Second,
		Recipients:  []string{id.Recipient().String()},
		Compression: "gzip:0",
		Stderr:      io.MultiWriter(&stderrBuf, os.Stderr),
		Now:         time.Now,
		Worker: backup.WorkerOptions{
			Pool:                 pool,
			PGVersion:            topo.Version,
			SSHTarget:            topo.SSHTarget(),
			SSHExtraArgs:         topo.SSHExtraArgs(),
			RemoteCommand:        []string{"sudo", "-u", "postgres", "-E", remoteBin, "worker", "stdio"},
			PageChecksumMode:     pagechecksum.ModeOff,
			WorkerWritesDirectly: false, // ← the new path under test
		},
	})
	if err != nil {
		t.Fatalf("backup.Run hybrid (orch storage): %v", err)
	}
	if res.Files < 50 {
		t.Errorf("Files = %d, want at least 50", res.Files)
	}

	// The caller wrote every file as the host user. Host-side
	// stat works without SSH chmod gymnastics — that's the whole point
	// of caller-storage mode.
	hostBackupDir := filepath.Join(topo.HostStoragePath, res.BackupID)
	for _, want := range []string{"backup_manifest", "Storage-Metadata.json", "PG_VERSION", "global/pg_control", "backup_label"} {
		full := filepath.Join(hostBackupDir, want)
		if _, err := os.Stat(full); err != nil {
			t.Errorf("%s missing in caller-side storage: %v", want, err)
		}
	}
	if _, err := os.Stat(filepath.Join(hostBackupDir, "backup_manifest.tmp")); !os.IsNotExist(err) {
		t.Errorf("backup_manifest.tmp leaked after Commit: %v", err)
	}

	// Topology-log assertions. In --direct-write=false mode the worker
	// has no backend role; the caller MUST NOT issue ProbeStorage
	// (would ship empty creds, surface as misleading UNREACHABLE).
	stderrTxt := stderrBuf.String()
	if strings.Contains(stderrTxt, "worker→storage probe") {
		t.Errorf("topology log contains worker→storage probe line in --direct-write=false mode (probe should be skipped):\n%s",
			stderrTxt)
	}
	// Belt-and-braces: never UNREACHABLE in this happy-path test.
	if strings.Contains(stderrTxt, "UNREACHABLE") {
		t.Errorf("topology log contains UNREACHABLE — probe should not have been called or should have succeeded:\n%s",
			stderrTxt)
	}

	t.Logf("caller-storage pgsafe-worker backup OK: id=%s files=%d bytes=%d duration=%s",
		res.BackupID, res.Files, res.Bytes, res.Duration)
}
