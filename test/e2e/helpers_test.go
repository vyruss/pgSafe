//go:build e2e || e2e_hybrid

package e2e_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// pgsafeBinary builds (or returns the cached path to) the pgsafe binary the
// E2E tests will exec.
func pgsafeBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "pgsafe")
	cmd := exec.Command("go", "build", "-o", bin, "../../cmd/pgsafe")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build pgsafe: %v", err)
	}
	return bin
}

// runPgsafe runs the pgsafe binary with the given args and streams its
// stdout+stderr into the test log line by line. Streaming (vs. buffer-then-
// dump) is critical when pgsafe hangs — the test log fills up to the point
// of the deadlock so we can see what was happening.
func runPgsafe(t *testing.T, bin string, args ...string) (string, string) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var stdout, stderr strings.Builder
	cmd.Stdout = io.MultiWriter(&stdout, testLogWriter{t, "stdout"})
	cmd.Stderr = io.MultiWriter(&stderr, testLogWriter{t, "stderr"})
	if err := cmd.Run(); err != nil {
		t.Fatalf("pgsafe %v: %v", args, err)
	}
	return stdout.String(), stderr.String()
}

// testLogWriter is an io.Writer that splits its input into lines and emits
// each as a t.Log line; useful for streaming subprocess stderr/stdout into
// the test log in real time.
type testLogWriter struct {
	t     *testing.T
	label string
}

func (w testLogWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if line != "" {
			w.t.Logf("[%s] %s", w.label, line)
		}
	}
	return len(p), nil
}

// runPgVerifybackup executes pg_verifybackup against dir, in a fresh
// postgres:18 container with dir bind-mounted at /backup. Same wrapper
// the simple-mode roundtrip uses; the hybrid-parallel demo lands the
// final verification through the same gate so any divergence between
// modes shows up immediately.
func runPgVerifybackup(t *testing.T, dir string) error {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		return errors.New("docker not on PATH")
	}
	if err := os.Chmod(dir, 0o755); err != nil { //nolint:gosec
		return err
	}
	cmd := exec.Command("docker", "run", "--rm",
		"--user", "0:0",
		"-v", dir+":/backup:ro",
		"postgres:18",
		"pg_verifybackup", "--no-parse-wal", "/backup",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_verifybackup failed: %w\n%s", err, out)
	}
	if !strings.Contains(string(out), "verified") {
		return fmt.Errorf("pg_verifybackup output unexpected: %s", out)
	}
	return nil
}
