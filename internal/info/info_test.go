package info_test

import (
	"context"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

func newStorage(t *testing.T) *posix.Backend {
	t.Helper()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return b
}

func TestListEmpty(t *testing.T) {
	t.Parallel()
	b := newStorage(t)
	out, warnings, err := info.List(context.Background(), b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(out) != 0 || len(warnings) != 0 {
		t.Errorf("empty storage: out=%v warnings=%v", out, warnings)
	}
}

func TestListChain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newStorage(t)
	bb := backuptest.New(ctx, b, "demo")

	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	full1 := bb.AddFull(t0, "")
	incr1 := bb.AddIncremental(full1, t0.Add(2*time.Hour), "RC1")
	full2 := bb.AddFull(t0.Add(24*time.Hour), "second-day")

	got, warnings, err := info.List(ctx, b)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	if len(got) != 3 {
		t.Fatalf("len(records) = %d, want 3", len(got))
	}

	// Records are sorted by ID; verify the chain is present and the
	// sidecar fields landed.
	byID := map[string]info.BackupRecord{}
	for _, r := range got {
		byID[r.BackupID] = r
	}
	if r, ok := byID[full1]; !ok {
		t.Errorf("missing full1 %q", full1)
	} else {
		if r.Type != "full" {
			t.Errorf("full1.Type = %q", r.Type)
		}
		if r.Server != "demo" {
			t.Errorf("full1.Server = %q", r.Server)
		}
	}
	if r, ok := byID[incr1]; !ok {
		t.Errorf("missing incr1 %q", incr1)
	} else {
		if r.Type != "incremental" {
			t.Errorf("incr1.Type = %q", r.Type)
		}
		if r.ParentBackupID != full1 {
			t.Errorf("incr1.ParentBackupID = %q, want %q", r.ParentBackupID, full1)
		}
		if r.Annotation != "RC1" {
			t.Errorf("incr1.Annotation = %q", r.Annotation)
		}
	}
	if r, ok := byID[full2]; !ok {
		t.Errorf("missing full2 %q", full2)
	} else if r.Annotation != "second-day" {
		t.Errorf("full2.Annotation = %q", r.Annotation)
	}
}
