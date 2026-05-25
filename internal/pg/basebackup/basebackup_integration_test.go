//go:build integration

package basebackup_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/pg/basebackup"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestBaseBackupRoundTrip is the load-bearing Cycle-5 gate: take a real
// basebackup of a PG 18 container, parse the tar stream entry-by-entry,
// extract files into a tmpdir, and verify recognizable cluster files exist.
func TestBaseBackupRoundTrip(t *testing.T) {
	if _, err := exec.LookPath("pg_basebackup"); err != nil {
		t.Skip("pg_basebackup not on PATH; basebackup integration test skipped")
	}

	pg := pgtest.StartPG18(t)
	ctx := context.Background()

	dst := t.TempDir()

	stream, err := basebackup.Start(ctx, basebackup.Options{
		DSN:   pg.DSN,
		Label: "pgsafe-cycle5-roundtrip",
	})
	if err != nil {
		t.Fatalf("basebackup.Start: %v", err)
	}

	var (
		fileCount  int
		bytesTotal int64
		sawPGVer   bool
		sawPGCtl   bool
	)
	for {
		hdr, r, err := stream.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v\nstderr:\n%s", err, stream.Stderr())
		}
		if hdr.Typeflag != 0 && hdr.Typeflag != '0' {
			// directories etc. — skip
			continue
		}
		out := filepath.Join(dst, filepath.FromSlash(hdr.Name))
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil { //nolint:gosec
			t.Fatalf("MkdirAll: %v", err)
		}
		f, err := os.Create(out) //nolint:gosec
		if err != nil {
			t.Fatalf("create %s: %v", out, err)
		}
		n, err := io.Copy(f, r)
		_ = f.Close()
		if err != nil {
			t.Fatalf("copy %s: %v", hdr.Name, err)
		}
		bytesTotal += n
		fileCount++

		switch {
		case hdr.Name == "PG_VERSION":
			sawPGVer = true
		case strings.HasSuffix(hdr.Name, "/pg_control") || hdr.Name == "global/pg_control":
			sawPGCtl = true
		}
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if fileCount < 10 {
		t.Errorf("file count = %d; expected at least the standard PG cluster files (~50+)", fileCount)
	}
	if !sawPGVer {
		t.Errorf("PG_VERSION not found in tar stream")
	}
	if !sawPGCtl {
		t.Errorf("global/pg_control not found in tar stream")
	}

	// Sanity: extracted PG_VERSION should be "18" (PG 18).
	verBytes, err := os.ReadFile(filepath.Join(dst, "PG_VERSION")) //nolint:gosec
	if err != nil {
		t.Fatalf("read PG_VERSION: %v", err)
	}
	ver := strings.TrimSpace(string(verBytes))
	if ver != "18" {
		t.Errorf("PG_VERSION = %q; want %q", ver, "18")
	}

	t.Logf("basebackup round-trip: %d files, %d bytes; PG_VERSION=%s", fileCount, bytesTotal, ver)
}

// TestBaseBackupFetchEmbedsBracketWAL pins the load-bearing
// behavior of WALMethod="fetch": pg_basebackup packs the bracket WAL
// into the data tar's pg_wal/ entries, so the resulting backup is
// self-contained — no external archive required to restore.
//
// This test is the gate for WALSourceStream. If this regresses, the
// pgsafe-mode "just take a backup" path silently produces backups
// missing the bracket WAL, which restore would then notice as a PG
// recovery failure (but only at restore time — too late). Catching
// at backup time saves operators a round of "why won't this restore?"
func TestBaseBackupFetchEmbedsBracketWAL(t *testing.T) {
	if _, err := exec.LookPath("pg_basebackup"); err != nil {
		t.Skip("pg_basebackup not on PATH")
	}
	pg := pgtest.StartPG18(t)
	ctx := context.Background()

	stream, err := basebackup.Start(ctx, basebackup.Options{
		DSN:       pg.DSN,
		Label:     "pgsafe-fetch-walembed",
		WALMethod: "fetch",
	})
	if err != nil {
		t.Fatalf("basebackup.Start: %v", err)
	}
	var (
		sawPGCtl bool
		walSegs  int
	)
	for {
		hdr, r, err := stream.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v\nstderr:\n%s", err, stream.Stderr())
		}
		if hdr.Typeflag != 0 && hdr.Typeflag != '0' {
			continue
		}
		// Drain the entry so the tar reader advances.
		if _, err := io.Copy(io.Discard, r); err != nil {
			t.Fatalf("drain %s: %v", hdr.Name, err)
		}
		switch {
		case strings.HasSuffix(hdr.Name, "/pg_control") || hdr.Name == "global/pg_control":
			sawPGCtl = true
		case strings.HasPrefix(hdr.Name, "pg_wal/"):
			// PG names a 24-hex segment plus optional metadata files
			// (history, partial). The bracket-spanning segment is what
			// makes this backup restorable; count any pg_wal/ entry as
			// evidence that fetch mode is doing its job.
			walSegs++
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !sawPGCtl {
		t.Error("global/pg_control not in tar — basebackup did not produce a real cluster snapshot")
	}
	if walSegs == 0 {
		t.Error("no pg_wal/ entries in tar — --wal-method=fetch did not embed bracket WAL")
	}
	t.Logf("fetch-mode tar contained %d pg_wal entries", walSegs)
}

// TestBaseBackupRejectsStreamMethod is a unit-shaped guard for the
// pg_basebackup constraint we enforce client-side: --wal-method=stream
// is incompatible with --pgdata=-/--format=tar. The error must surface
// at Start() rather than mid-stream — operators get a clean failure
// instead of a confusing partial backup.
func TestBaseBackupRejectsStreamMethod(t *testing.T) {
	if _, err := exec.LookPath("pg_basebackup"); err != nil {
		t.Skip("pg_basebackup not on PATH")
	}
	pg := pgtest.StartPG18(t)
	_, err := basebackup.Start(context.Background(), basebackup.Options{
		DSN:       pg.DSN,
		Label:     "pgsafe-stream-rejected",
		WALMethod: "stream",
	})
	if err == nil {
		t.Fatal("expected error for WALMethod=stream; got nil")
	}
	if !strings.Contains(err.Error(), "stream") {
		t.Errorf("error %q should mention stream", err)
	}
}
