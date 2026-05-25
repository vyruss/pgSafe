//go:build perf

// Package perf is the performance-comparison harness for pgSafe
// vs. pgBackRest. The headline numbers it produces (backup wall-time,
// restore wall-time, storage size per mode per scale) ship into
// test/perf/RESULTS.md and from there into ARCHITECTURE.md's
// "Performance" section.
//
//	is the spec. ships
//
// only this skeleton so the run-ci-local.sh "perf" step has somewhere
// to dispatch under -tags=perf. fills in cluster provisioning,
// pgbench data generation, and the side-by-side runs.
package perf

// Mode names the operating mode of a single perf run. Mirrors the
// caller's mode names so test/perf reports line up with the
// modes documented in ARCHITECTURE.md.
type Mode string

const (
	ModeSimple         Mode = "simple"
	ModeRemoteParallel Mode = "remote-parallel"
	ModeHybridParallel Mode = "hybrid-parallel"
)

// Result is one row of the JSON report committed to test/perf/RESULTS.md.
// All durations are wall-clock; all sizes are storage on-disk bytes
// (post-compression, post-encryption).
type Result struct {
	Mode              Mode    `json:"mode"`
	ClusterScaleGiB   int     `json:"cluster_scale_gib"`
	BackupWallSec     float64 `json:"backup_wall_sec"`
	RestoreWallSec    float64 `json:"restore_wall_sec"`
	RepoBytes         int64   `json:"repo_bytes"`
	PgsafeVersion     string  `json:"pgsafe_version"`
	PgbackrestVersion string  `json:"pgbackrest_version"`
	HardwareNote      string  `json:"hardware_note"`
}
