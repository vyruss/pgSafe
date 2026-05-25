package basebackup

import (
	"slices"
	"strings"
	"testing"
)

// TestBuildArgsDefaults pins the legacy/no-WAL pg_basebackup invocation:
// archive-tied callers (the pre-WALSource baseline) get --wal-method=none
// and --no-manifest. Catches accidental flag-set drift.
func TestBuildArgsDefaults(t *testing.T) {
	t.Parallel()
	args, err := buildArgs(Options{DSN: "postgres://x", Label: "demo"})
	if err != nil {
		t.Fatalf("buildArgs: %v", err)
	}
	want := []string{
		"-d", "postgres://x",
		"--pgdata=-",
		"--format=tar",
		"--wal-method=none",
		"--checkpoint=fast",
		"--label=demo",
		"--no-manifest",
	}
	if !slices.Equal(args, want) {
		t.Errorf("buildArgs default:\n got %q\nwant %q", args, want)
	}
}

// TestBuildArgsFetch pins the stream-source invocation: --wal-method=fetch
// instructs PG to pack the bracket WAL into the data tar's pg_wal/.
// Backup becomes self-contained — no archive dependency for THIS backup.
func TestBuildArgsFetch(t *testing.T) {
	t.Parallel()
	args, err := buildArgs(Options{DSN: "postgres://x", Label: "demo", WALMethod: "fetch"})
	if err != nil {
		t.Fatalf("buildArgs: %v", err)
	}
	if !slices.Contains(args, "--wal-method=fetch") {
		t.Errorf("expected --wal-method=fetch in args; got %q", args)
	}
	if slices.Contains(args, "--wal-method=none") {
		t.Errorf("--wal-method=none should not appear when fetch is requested; got %q", args)
	}
}

// TestBuildArgsRejectsStream pins pg_basebackup's hard constraint:
// --wal-method=stream is incompatible with --pgdata=-/--format=tar.
// pgsafe converts that into a fail-fast error before invoking the
// subprocess; otherwise the operator sees a confusing PG error mid-backup.
func TestBuildArgsRejectsStream(t *testing.T) {
	t.Parallel()
	_, err := buildArgs(Options{DSN: "postgres://x", Label: "demo", WALMethod: "stream"})
	if err == nil {
		t.Fatal("expected error for --wal-method=stream; got nil")
	}
	if !strings.Contains(err.Error(), "stream") {
		t.Errorf("error %q should mention stream", err)
	}
}

// TestBuildArgsUnknownMethod surfaces operator typos (e.g. "--wal=fetch")
// loudly instead of forwarding garbage to pg_basebackup.
func TestBuildArgsUnknownMethod(t *testing.T) {
	t.Parallel()
	_, err := buildArgs(Options{DSN: "postgres://x", Label: "demo", WALMethod: "bogus"})
	if err == nil {
		t.Fatal("expected error for unknown WAL method; got nil")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Errorf("error %q should say 'unknown'", err)
	}
}

// TestBuildArgsIncremental pins the incremental-mode args: --incremental
// is added, --no-manifest is NOT (pg_basebackup's own manifest is
// canonical for pg_combinebackup).
func TestBuildArgsIncremental(t *testing.T) {
	t.Parallel()
	args, err := buildArgs(Options{
		DSN:                     "postgres://x",
		Label:                   "demo",
		IncrementalManifestPath: "/tmp/parent.manifest",
	})
	if err != nil {
		t.Fatalf("buildArgs: %v", err)
	}
	if !slices.Contains(args, "--incremental=/tmp/parent.manifest") {
		t.Errorf("expected --incremental in args; got %q", args)
	}
	if slices.Contains(args, "--no-manifest") {
		t.Errorf("--no-manifest must not appear in incremental mode; got %q", args)
	}
}
