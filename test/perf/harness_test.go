//go:build perf

package perf

import "testing"

// TestSkeleton is the Cycle-0 placeholder. It pins the build-tag wiring
// (the perf step in run-ci-local.sh exercises this package) and asserts
// the Mode constants compile. replaces it with the actual
// pgsafe-vs-pgbackrest comparison runs.
func TestSkeleton(t *testing.T) {
	for _, m := range []Mode{ModeSimple, ModeRemoteParallel, ModeHybridParallel} {
		if m == "" {
			t.Errorf("empty mode constant")
		}
	}
}
