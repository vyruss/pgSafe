//go:build faults

// Package faults_test holds fault-injection tests for the ten-invariant
// rulebook. Each test crashes a real operation at a named step boundary and
// asserts the post-crash storage state is one of the documented valid
// configurations — never a half-final or otherwise corrupted state.
//
//	DoD #11 (Invariant #6) and §5
//
// traceability — POSIX fsync ordering. Every layer runs in CI; this layer
// runs under -tags=faults via run-ci-local.sh.
package faults_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/posix"
)

// payload is the content every test writes through Put. Tests that observe a
// final file assert the file contains exactly this payload.
var payload = []byte("the quick brown fox jumps over the lazy dog\n")

// allPutSteps is every fault boundary inside the Put.Close 7-step sequence
// of
var allPutSteps = []string{
	posix.StepWriteTemp,
	posix.StepFsyncFile,
	posix.StepCloseFile,
	posix.StepOpenDir,
	posix.StepFsyncDirPre,
	posix.StepRename,
	posix.StepFsyncDirPost,
}

// allCommitSteps is every fault boundary inside Commit.
var allCommitSteps = []string{
	posix.StepCommitRename,
	posix.StepCommitFsync,
}

// TestPutInvariant6_PerStep is the load-bearing Invariant #6 test: kill at
// each of the seven boundaries inside Put.Close and assert the resulting
// storage is in one of the two valid states — never half-final.
func TestPutInvariant6_PerStep(t *testing.T) {
	for _, step := range allPutSteps {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "storage")
			r, err := posix.New(posix.Options{
				Root: root,
				Fault: func(s string) error {
					if s == step {
						return fmt.Errorf("injected: %s", s)
					}
					return nil
				},
			})
			if err != nil {
				t.Fatalf("posix.New: %v", err)
			}
			if err := r.Open(context.Background()); err != nil {
				t.Fatalf("Open: %v", err)
			}

			runFaultedPut(t, r)

			// Inspect post-crash filesystem.
			final := filepath.Join(root, "f.bin")
			tmp := final + ".pgsafe-tmp"
			finalExists := fileExists(t, final)
			tmpExists := fileExists(t, tmp)

			// Invariant #6: never both present.
			if finalExists && tmpExists {
				t.Errorf("BOTH tmp and final present after fault at %s", step)
			}

			// If final is present, it must contain the exact payload — no
			// half-written or empty file may pose as "final".
			if finalExists {
				got, err := os.ReadFile(final) //nolint:gosec
				if err != nil {
					t.Fatalf("ReadFile(final): %v", err)
				}
				if string(got) != string(payload) {
					t.Errorf("final file content = %q, want %q", got, payload)
				}
			}
		})
	}
}

// runFaultedPut runs a Put that may abort at any of the 7 steps. Errors are
// expected; we just want the operation to stop wherever the fault fires so
// the test can inspect on-disk state afterwards.
func runFaultedPut(t *testing.T, r *posix.Backend) {
	t.Helper()
	wc, err := r.Put(context.Background(), "f.bin")
	if err != nil {
		// Fault triggered during Put itself (StepWriteTemp).
		return
	}
	if _, err := wc.Write(payload); err != nil {
		_ = wc.Close()
		return
	}
	_ = wc.Close()
}

// TestCommitInvariant6_PerStep does the same for Commit's two boundaries.
// The rename happens before either hook fires, so any fault here leaves the
// storage with final present (and tmp absent) — never the other way round.
func TestCommitInvariant6_PerStep(t *testing.T) {
	for _, step := range allCommitSteps {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "storage")

			// First put a tmp file with no faulting; then commit with a fault.
			r, err := posix.New(posix.Options{Root: root})
			if err != nil {
				t.Fatalf("posix.New: %v", err)
			}
			if err := r.Open(context.Background()); err != nil {
				t.Fatalf("Open: %v", err)
			}
			wc, err := r.Put(context.Background(), "manifest.tmp")
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if _, err := wc.Write(payload); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := wc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Now reopen with the fault hook for Commit.
			r2, err := posix.New(posix.Options{
				Root: root,
				Fault: func(s string) error {
					if s == step {
						return fmt.Errorf("injected: %s", s)
					}
					return nil
				},
			})
			if err != nil {
				t.Fatalf("posix.New (faulted): %v", err)
			}
			if err := r2.Open(context.Background()); err != nil {
				t.Fatalf("Open: %v", err)
			}
			_ = r2.Commit(context.Background(), "manifest.tmp", "manifest")

			final := filepath.Join(root, "manifest")
			tmp := filepath.Join(root, "manifest.tmp")

			// Both StepCommitRename and StepCommitFsync fire AFTER the rename
			// hits the filesystem, so final must exist and tmp must not.
			if !fileExists(t, final) {
				t.Errorf("final absent after fault at %s; expected post-rename state", step)
			}
			if fileExists(t, tmp) {
				t.Errorf("tmp still present after fault at %s; rename did not run", step)
			}
		})
	}
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	t.Fatalf("Stat(%s): %v", path, err)
	return false
}
