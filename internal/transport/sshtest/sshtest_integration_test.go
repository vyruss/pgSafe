//go:build integration_hybrid

package sshtest_test

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/vyruss/pgsafe/internal/transport/sshtest"
)

// TestStartPGWithSSHSpinsUp confirms the combined PG+sshd container starts,
// PG accepts libpq, and /usr/bin/ssh can run a remote command using the
// per-test key+known_hosts.
func TestStartPGWithSSHSpinsUp(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("/usr/bin/ssh not on PATH; integration_hybrid skipped")
	}
	t.Parallel()
	topo := sshtest.StartPG18WithSSH(t)
	ctx := context.Background()

	// PG side reachable.
	conn, err := pgx.Connect(ctx, topo.SuperDSN)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var verText string
	if err := conn.QueryRow(ctx, "SHOW server_version_num").Scan(&verText); err != nil {
		t.Fatalf("server_version_num: %v", err)
	}
	// server_version_num is e.g. "180001" for 18.x; major = 180001/10000 = 18.
	if !strings.HasPrefix(verText, strconv.Itoa(topo.Version)) {
		t.Errorf("server_version_num = %q, want to start with %d", verText, topo.Version)
	}

	// SSH side reachable. Use /usr/bin/ssh directly with the topology's
	// extra args; the production transport package will wrap this
	// identically in
	args := append(topo.SSHExtraArgs(), topo.SSHTarget(), "echo", "hello")
	out, err := exec.Command("ssh", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("ssh remote echo: %v\nargs=%v\noutput=%s", err, args, out)
	}
	if !strings.Contains(string(out), "hello") {
		t.Errorf("ssh stdout = %q, want to contain \"hello\"", out)
	}
}
