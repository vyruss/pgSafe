package retention_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/retention"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

// TestPruneRemovesExpirableBackupsAndKeepsSurvivors: build 3 fulls,
// keep 1, run Prune, assert the 2 oldest are gone (List shows them
// missing) and the most-recent is intact.
func TestPruneRemovesExpirableBackupsAndKeepsSurvivors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	bb := backuptest.New(ctx, b, "demo")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")
	bb.AddFull(t0.Add(24*time.Hour), "")
	bb.AddFull(t0.Add(48*time.Hour), "")

	recs, _, err := info.List(ctx, b)
	if err != nil {
		t.Fatalf("info.List: %v", err)
	}
	plan, err := retention.Evaluate(recs, retention.Policy{KeepFulls: 1, Now: t0.Add(72 * time.Hour)})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(plan.ExpirableBackupIDs) != 2 {
		t.Fatalf("expected 2 expirable, got %d", len(plan.ExpirableBackupIDs))
	}

	res, err := retention.Prune(ctx, b, plan, false)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(res.BackupsDeleted) != 2 {
		t.Errorf("BackupsDeleted len = %d, want 2", len(res.BackupsDeleted))
	}
	for _, id := range plan.ExpirableBackupIDs {
		if _, err := b.Stat(ctx, id+"/PG_VERSION"); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expirable backup %s/PG_VERSION still exists; err=%v", id, err)
		}
	}
	for _, id := range plan.KeptBackupIDs {
		if _, err := b.Stat(ctx, id+"/PG_VERSION"); err != nil {
			t.Errorf("kept backup %s/PG_VERSION missing: %v", id, err)
		}
	}
}

// TestPruneDryRunReportsButPreservesEverything: --dry-run produces
// the same BackupsDeleted/FilesDeleted summary but no files are
// removed.
func TestPruneDryRunReportsButPreservesEverything(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	bb := backuptest.New(ctx, b, "demo")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")
	bb.AddFull(t0.Add(24*time.Hour), "")

	recs, _, err := info.List(ctx, b)
	if err != nil {
		t.Fatalf("info.List: %v", err)
	}
	plan, err := retention.Evaluate(recs, retention.Policy{KeepFulls: 1, Now: t0.Add(48 * time.Hour)})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	res, err := retention.Prune(ctx, b, plan, true)
	if err != nil {
		t.Fatalf("Prune dry-run: %v", err)
	}
	if len(res.BackupsDeleted) == 0 {
		t.Errorf("dry-run BackupsDeleted should be non-empty (the would-be plan)")
	}
	for _, id := range plan.ExpirableBackupIDs {
		if _, err := b.Stat(ctx, id+"/PG_VERSION"); err != nil {
			t.Errorf("dry-run touched expirable %s/PG_VERSION; err=%v", id, err)
		}
	}
}

// TestPruneWALRespectsOldestNeededLSN: WAL segments with end-LSN
// older than OldestNeededLSN are deleted; segments at or beyond it
// survive.
func TestPruneWALRespectsOldestNeededLSN(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	bb := backuptest.New(ctx, b, "demo")
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	bb.AddFull(t0, "")
	survivor := bb.AddFull(t0.Add(48*time.Hour), "")

	// Inject 3 WAL segments. The synthetic builder's nextLSN starts
	// at 0x3000028, so segments at LSN ranges far below survivor's
	// StartLSN should be pruned. Timeline=1; the "future" segment
	// has a lex-greater name than the cutoff and must survive.
	bb.AddWALSegment(1, "000000010000000000000001", []byte("seg1")) // very old
	bb.AddWALSegment(1, "000000010000000000000002", []byte("seg2")) // old
	bb.AddWALSegment(1, "00000001FFFFFFFFFFFFFFFF", []byte("seg3")) // way in the future

	recs, _, err := info.List(ctx, b)
	if err != nil {
		t.Fatalf("info.List: %v", err)
	}
	plan, err := retention.Evaluate(recs, retention.Policy{KeepFulls: 1, Now: t0.Add(72 * time.Hour)})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if uint64(plan.OldestNeededLSN) == 0 {
		t.Fatalf("OldestNeededLSN = 0; expected non-zero (kept chain rooted at %s)", survivor)
	}

	res, err := retention.Prune(ctx, b, plan, false)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if len(res.WALSegmentsDeleted) == 0 {
		t.Errorf("WALSegmentsDeleted = 0; expected the very-old segment to be pruned")
	}
	// The "way in the future" segment must survive.
	if _, err := b.Stat(ctx, "wal/00000001/00000001FFFFFFFFFFFFFFFF"); err != nil {
		t.Errorf("future WAL segment unexpectedly deleted: %v", err)
	}
}

func ExampleEvaluate() {
	t := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	recs := []info.BackupRecord{
		{BackupID: "20260401T100000F", Type: "full", Server: "demo", StopTime: t},
		{BackupID: "20260402T100000F", Type: "full", Server: "demo", StopTime: t.Add(24 * time.Hour)},
	}
	plan, _ := retention.Evaluate(recs, retention.Policy{KeepFulls: 1, Now: t.Add(48 * time.Hour)})
	fmt.Printf("expirable=%d kept=%d\n", len(plan.ExpirableBackupIDs), len(plan.KeptBackupIDs))
	// Output: expirable=1 kept=1
}
