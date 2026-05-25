package retention_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/retention"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

func recordsFromSynth(t *testing.T, build func(bb *backuptest.Builder)) []info.BackupRecord {
	t.Helper()
	root := t.TempDir()
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}
	if err := b.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	bb := backuptest.New(context.Background(), b, "demo")
	build(bb)
	recs, _, err := info.List(context.Background(), b)
	if err != nil {
		t.Fatalf("info.List: %v", err)
	}
	return recs
}

func TestEvaluateEmptyPolicy(t *testing.T) {
	t.Parallel()
	_, err := retention.Evaluate(nil, retention.Policy{})
	if !errors.Is(err, retention.ErrEmptyPolicy) {
		t.Errorf("Evaluate empty policy: got %v, want ErrEmptyPolicy", err)
	}
}

func TestEvaluateKeepFullsKeepsMostRecent(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	recs := recordsFromSynth(t, func(bb *backuptest.Builder) {
		bb.AddFull(t0, "")
		bb.AddFull(t0.Add(24*time.Hour), "")
		bb.AddFull(t0.Add(48*time.Hour), "")
	})
	plan, err := retention.Evaluate(recs, retention.Policy{KeepFulls: 2, Now: t0.Add(72 * time.Hour)})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(plan.KeptBackupIDs) != 2 {
		t.Errorf("KeptBackupIDs len = %d, want 2", len(plan.KeptBackupIDs))
	}
	if len(plan.ExpirableBackupIDs) != 1 {
		t.Errorf("ExpirableBackupIDs len = %d, want 1", len(plan.ExpirableBackupIDs))
	}
	// Oldest (t0) should be expirable; t0+24h and t0+48h kept.
	if plan.ExpirableBackupIDs[0] != "20260401T100000F" {
		t.Errorf("ExpirableBackupIDs[0] = %q, want oldest", plan.ExpirableBackupIDs[0])
	}
}

func TestEvaluateKeepFullsKeepsChainTogether(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	var f1, f2 string
	recs := recordsFromSynth(t, func(bb *backuptest.Builder) {
		f1 = bb.AddFull(t0, "")
		bb.AddIncremental(f1, t0.Add(2*time.Hour), "")
		bb.AddIncremental(f1, t0.Add(4*time.Hour), "")
		f2 = bb.AddFull(t0.Add(24*time.Hour), "")
	})
	plan, err := retention.Evaluate(recs, retention.Policy{KeepFulls: 1, Now: t0.Add(48 * time.Hour)})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	// Only f2's chain should be kept (1 most-recent full); f1 + its
	// 2 incrementals all expirable together.
	if !contains(plan.KeptBackupIDs, f2) {
		t.Errorf("most-recent full %s should be kept; KeptBackupIDs=%v", f2, plan.KeptBackupIDs)
	}
	if !contains(plan.ExpirableBackupIDs, f1) {
		t.Errorf("oldest full %s should be expirable; ExpirableBackupIDs=%v", f1, plan.ExpirableBackupIDs)
	}
	expirableCount := 0
	for _, id := range plan.ExpirableBackupIDs {
		if id == f1 || hasPrefix(id, f1+"_") {
			expirableCount++
		}
	}
	if expirableCount != 3 {
		t.Errorf("f1's chain should expire as a unit (full + 2 incrementals); got %d entries", expirableCount)
	}
}

func TestEvaluateKeepFullAge(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	recs := recordsFromSynth(t, func(bb *backuptest.Builder) {
		bb.AddFull(t0, "")                     // 5d old at Now
		bb.AddFull(t0.Add(24*time.Hour), "")   // 4d old
		bb.AddFull(t0.Add(4*24*time.Hour), "") // 1d old
	})
	plan, err := retention.Evaluate(recs, retention.Policy{
		KeepFullAge: 2 * 24 * time.Hour,
		Now:         t0.Add(5 * 24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(plan.KeptBackupIDs) != 1 {
		t.Errorf("KeptBackupIDs len = %d, want 1 (only the 1d-old full)", len(plan.KeptBackupIDs))
	}
	if len(plan.ExpirableBackupIDs) != 2 {
		t.Errorf("ExpirableBackupIDs len = %d, want 2", len(plan.ExpirableBackupIDs))
	}
}

func TestEvaluateKeepDailyOneSlotPerDay(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	// Two backups same day, one backup the next day. KeepDaily=2
	// should keep one chain per day for the past 2 days = 2 chains
	// (one per day, picking the newer-in-bucket one for the
	// duplicate day).
	recs := recordsFromSynth(t, func(bb *backuptest.Builder) {
		bb.AddFull(t0, "early-day1")
		bb.AddFull(t0.Add(2*time.Hour), "late-day1")
		bb.AddFull(t0.Add(28*time.Hour), "day2")
	})
	plan, err := retention.Evaluate(recs, retention.Policy{
		KeepDaily: 2,
		Now:       t0.Add(28 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(plan.KeptBackupIDs) != 2 {
		t.Errorf("KeptBackupIDs len = %d, want 2 (one per kept day); got %v", len(plan.KeptBackupIDs), plan.KeptBackupIDs)
	}
	if len(plan.ExpirableBackupIDs) != 1 {
		t.Errorf("ExpirableBackupIDs len = %d, want 1; got %v", len(plan.ExpirableBackupIDs), plan.ExpirableBackupIDs)
	}
}

func TestEvaluateOldestNeededLSN(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	recs := recordsFromSynth(t, func(bb *backuptest.Builder) {
		bb.AddFull(t0, "a")
		bb.AddFull(t0.Add(24*time.Hour), "b")
		bb.AddFull(t0.Add(48*time.Hour), "c")
	})
	plan, err := retention.Evaluate(recs, retention.Policy{KeepFulls: 1, Now: t0.Add(72 * time.Hour)})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	// Only the most recent chain ("c") survives; OldestNeededLSN is
	// that chain's full.StartLSN, which should be non-zero.
	if uint64(plan.OldestNeededLSN) == 0 {
		t.Errorf("OldestNeededLSN = 0, want non-zero (kept chain has a real Start-LSN)")
	}
}

func TestEvaluateDeterministic(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	recs := recordsFromSynth(t, func(bb *backuptest.Builder) {
		bb.AddFull(t0, "")
		bb.AddFull(t0.Add(24*time.Hour), "")
		bb.AddFull(t0.Add(48*time.Hour), "")
	})
	policy := retention.Policy{KeepFulls: 1, Now: t0.Add(72 * time.Hour)}
	first, _ := retention.Evaluate(recs, policy)
	second, _ := retention.Evaluate(append([]info.BackupRecord{}, recs...), policy)
	if !sliceEqualSorted(first.ExpirableBackupIDs, second.ExpirableBackupIDs) {
		t.Errorf("Evaluate is not deterministic:\nfirst=%v\nsecond=%v", first.ExpirableBackupIDs, second.ExpirableBackupIDs)
	}
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func hasPrefix(s, prefix string) bool {
	if len(prefix) > len(s) {
		return false
	}
	return s[:len(prefix)] == prefix
}

func sliceEqualSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a2 := append([]string(nil), a...)
	b2 := append([]string(nil), b...)
	sort.Strings(a2)
	sort.Strings(b2)
	for i := range a2 {
		if a2[i] != b2[i] {
			return false
		}
	}
	return true
}
