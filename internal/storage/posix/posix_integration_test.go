//go:build integration

package posix_test

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/vyruss/pgsafe/internal/storage/posix"
)

// TestPutFsyncOrderingExact asserts the §3.2.3 7-step sequence runs in the
// documented order. The hook is purely observational here (it always returns
// nil); the §6 fault tests use it to inject failures at each step.
func TestPutFsyncOrderingExact(t *testing.T) {
	var steps []string
	root := filepath.Join(t.TempDir(), "storage")
	r, err := posix.New(posix.Options{
		Root: root,
		Fault: func(step string) error {
			steps = append(steps, step)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := r.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Reset after Open (which doesn't go through the fault hook anyway, but
	// belt-and-braces in case future versions wire it).
	steps = steps[:0]

	wc, err := r.Put(context.Background(), "x.bin")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := wc.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	want := []string{
		posix.StepWriteTemp,
		posix.StepFsyncFile,
		posix.StepCloseFile,
		posix.StepOpenDir,
		posix.StepFsyncDirPre,
		posix.StepRename,
		posix.StepFsyncDirPost,
	}
	if !slices.Equal(steps, want) {
		t.Errorf("step sequence:\n got %v\nwant %v", steps, want)
	}
}

// TestCommitFsyncOrderingExact asserts Commit invokes its two hooks in order.
func TestCommitFsyncOrderingExact(t *testing.T) {
	var steps []string
	root := filepath.Join(t.TempDir(), "storage")
	r, err := posix.New(posix.Options{
		Root: root,
		Fault: func(step string) error {
			steps = append(steps, step)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := r.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	wc, _ := r.Put(context.Background(), "manifest.tmp")
	_, _ = wc.Write([]byte("m"))
	_ = wc.Close()
	steps = steps[:0]

	if err := r.Commit(context.Background(), "manifest.tmp", "manifest"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	want := []string{
		posix.StepCommitRename,
		posix.StepCommitFsync,
	}
	if !slices.Equal(steps, want) {
		t.Errorf("step sequence:\n got %v\nwant %v", steps, want)
	}
}
