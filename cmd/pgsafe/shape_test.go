package main

import (
	"testing"

	"github.com/vyruss/pgsafe/internal/backup"
)

// TestInferBackupMode pins the dispatch matrix that replaces --mode.
// The cases here are the same matrix ARCHITECTURE.md "Wire architecture"
// "Scenario reference" table covers — every operator invocation has to
// resolve to exactly one of these three. workersExplicit guards against
// surprise wire-shape changes when an operator hasn't explicitly opted
// in to parallel libpq: the safe default for a no-pg-host config is
// single-connection simple mode.
func TestInferBackupMode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name            string
		sshTarget       string
		workers         int
		workersExplicit bool
		want            backup.Mode
	}{
		{"no ssh, workers=1 (default) → simple", "", 1, false, backup.ModeSimple},
		{"no ssh, workers=4 default → simple (not explicit)", "", 4, false, backup.ModeSimple},
		{"no ssh, workers=4 explicit → remote-parallel", "", 4, true, backup.ModeRemoteParallel},
		{"no ssh, workers=2 explicit → remote-parallel", "", 2, true, backup.ModeRemoteParallel},
		{"no ssh, workers=1 explicit → simple (not >1)", "", 1, true, backup.ModeSimple},
		{"ssh target, workers=1 → pgSafe (worker on PG host, single)", "pgsafe@pg-prod", 1, false, backup.ModeWorker},
		{"ssh target, workers=4 → pgSafe (worker on PG host, parallel)", "pgsafe@pg-prod", 4, false, backup.ModeWorker},
		{"localhost ssh target → pgSafe (same-host subprocess)", "localhost", 4, false, backup.ModeWorker},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := inferBackupMode(tc.sshTarget, tc.workers, tc.workersExplicit)
			if got != tc.want {
				t.Errorf("inferBackupMode(%q, %d, %v) = %v, want %v",
					tc.sshTarget, tc.workers, tc.workersExplicit, got, tc.want)
			}
		})
	}
}
