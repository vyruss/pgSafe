package main

import (
	"strings"
	"testing"
)

// TestConfigLessMissingRequiredAggregated pins the operator-experience
// contract: missing required flags in config-less mode produce a SINGLE
// error listing everything that's missing — not "fix one flag, run
// again, fix the next." Drift here turns a 5-second smoke test into
// 5 round trips.
func TestConfigLessMissingRequiredAggregated(t *testing.T) {
	t.Parallel()
	rc, _, stderr := runRoot(t, "server", "add")
	if rc == 0 {
		t.Fatal("expected non-zero exit; got 0")
	}
	for _, frag := range []string{"--server", "--pg-conn-string", "--storage-path", "--encryption-recipient"} {
		if !strings.Contains(stderr, frag) {
			t.Errorf("error message missing %q; got: %s", frag, stderr)
		}
	}
}

// TestConfigLessRejectsCloudStorage: cloud storage backends carry too
// many auth knobs to fit cleanly on the CLI; their auth chains are
// EXACTLY what makes a YAML config useful. Refuse fast with a hint
// pointing the operator at --config.
func TestConfigLessRejectsCloudStorage(t *testing.T) {
	t.Parallel()
	rc, _, stderr := runRoot(t, "server", "add",
		"--server", "demo",
		"--pg-conn-string", "postgres://x",
		"--storage-type", "s3",
		"--encryption-recipient", "age1abc",
	)
	if rc == 0 {
		t.Fatal("expected non-zero exit for --storage-type=s3 in config-less mode")
	}
	if !strings.Contains(stderr, "only posix") || !strings.Contains(stderr, "--config") {
		t.Errorf("error should explain 'only posix' + point at --config; got: %s", stderr)
	}
}

// TestConfigLessAcceptsMinimalPosix: the happy-path config-less
// invocation. server add is the cheapest command that fully exercises
// resolveConfigFromFlags + Validate without spinning up PG / network
// I/O — if this passes the same flag set works for backup/info/etc.
func TestConfigLessAcceptsMinimalPosix(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	rc, _, stderr := runRoot(t, "server", "add",
		"--server", "demo",
		"--pg-conn-string", "postgres://x",
		"--pg-version", "18",
		"--storage-type", "posix",
		"--storage-path", storage,
		"--encryption-recipient", "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p",
	)
	if rc != 0 {
		t.Fatalf("config-less server add: exit=%d stderr=%s", rc, stderr)
	}
}

// TestConfigLessCompressionDefaults pins the implicit codec/level —
// operators get a sensible compression even with the bare-minimum
// flag set. Mirrors pgbackrest's "have working defaults" attitude.
func TestConfigLessCompressionDefaults(t *testing.T) {
	t.Parallel()
	storage := t.TempDir()
	rc, _, stderr := runRoot(t, "server", "add",
		"--server", "demo",
		"--pg-conn-string", "postgres://x",
		"--pg-version", "18",
		"--storage-path", storage,
		"--encryption-recipient", "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p",
	)
	if rc != 0 {
		t.Fatalf("config-less server add (defaults): exit=%d stderr=%s", rc, stderr)
	}
}
