// Package smoke_test holds harness-level smoke tests that prove the binary
// builds and answers --help. It deliberately depends on no internal/* code
// so it stays valid throughout as modules churn underneath.
package smoke_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestBinaryHelpExitsZero builds cmd/pgsafe into a tempdir, invokes it with
// --help, and asserts exit code 0. This is the load-bearing Cycle-0 smoke.
func TestBinaryHelpExitsZero(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bin := filepath.Join(dir, "pgsafe")

	build := exec.Command("go", "build", "-o", bin, "../../cmd/pgsafe")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("go build cmd/pgsafe: %v", err)
	}

	for _, arg := range []string{"--help", "-h", "help"} {
		t.Run(arg, func(t *testing.T) {
			cmd := exec.Command(bin, arg)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s --help failed: %v\noutput:\n%s", bin, err, out)
			}
			if len(out) == 0 {
				t.Fatalf("%s %s produced no output", bin, arg)
			}
		})
	}
}

// TestBinaryVersionExitsZero asserts the version flag exits 0 and prints
// something. The exact version string is not asserted (it changes per release).
func TestBinaryVersionExitsZero(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	bin := filepath.Join(dir, "pgsafe")

	build := exec.Command("go", "build", "-o", bin, "../../cmd/pgsafe")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("go build cmd/pgsafe: %v", err)
	}

	cmd := exec.Command(bin, "--version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s --version failed: %v\noutput:\n%s", bin, err, out)
	}
	if len(out) == 0 {
		t.Fatalf("%s --version produced no output", bin)
	}
}
