package check_test

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/backup/backuptest"
	"github.com/vyruss/pgsafe/internal/check"
	"github.com/vyruss/pgsafe/internal/storage/posix"
)

func newBackend(t *testing.T) *posix.Backend {
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

// TestRunEmptyStorage: a freshly-opened backend passes the storage-side
// probes; PG-gated probes are recorded as skipped.
func TestRunEmptyStorage(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	r := check.Run(context.Background(), check.Options{Backend: b})
	if !r.AllOK() {
		t.Errorf("empty storage should pass all probes; got %+v", r)
	}
	for _, p := range r.Probes {
		if p.Name == "archive_command" || p.Name == "standby_coordination" {
			if !strings.Contains(p.Detail, "skipped") {
				t.Errorf("%s on empty storage should be skipped; got %q", p.Name, p.Detail)
			}
		}
	}
}

// TestChainIntegrityFailsOnOrphan: an incremental whose parent is
// missing surfaces as a chain_integrity FAIL.
func TestChainIntegrityFailsOnOrphan(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	bb := backuptest.New(context.Background(), b, "demo")
	bb.AddOrphanedIncremental(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "20990101T000000F")

	r := check.Run(context.Background(), check.Options{Backend: b})
	for _, p := range r.Probes {
		if p.Name == "chain_integrity" {
			if p.OK {
				t.Errorf("chain_integrity should fail on orphaned incremental; got OK=%v detail=%q", p.OK, p.Detail)
			}
			return
		}
	}
	t.Errorf("chain_integrity probe missing from report")
}

// TestInfoDecodableFailsOnCorruptSidecar: a corrupt sidecar produces
// an info_decodable FAIL with a non-empty detail.
func TestInfoDecodableFailsOnCorruptSidecar(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	bb := backuptest.New(context.Background(), b, "demo")
	id := bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	// Overwrite the sidecar with garbage.
	wc, err := b.Put(context.Background(), path.Join(id, "Storage-Metadata.json")+".tmp")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := wc.Write([]byte("not valid json")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Delete(context.Background(), path.Join(id, "Storage-Metadata.json")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := b.Commit(context.Background(), path.Join(id, "Storage-Metadata.json")+".tmp", path.Join(id, "Storage-Metadata.json")); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	r := check.Run(context.Background(), check.Options{Backend: b})
	for _, p := range r.Probes {
		if p.Name == "info_decodable" {
			if p.OK {
				t.Errorf("info_decodable should fail on corrupt sidecar; got OK=%v detail=%q", p.OK, p.Detail)
			}
			return
		}
	}
	t.Errorf("info_decodable probe missing from report")
}

// TestWALExpectedFailsWhenBackupsButNoWAL: backups present but
// wal/ subdir is empty surfaces as wal_expected FAIL.
func TestWALExpectedFailsWhenBackupsButNoWAL(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	bb := backuptest.New(context.Background(), b, "demo")
	bb.AddFull(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC), "")

	r := check.Run(context.Background(), check.Options{Backend: b})
	for _, p := range r.Probes {
		if p.Name == "wal_expected" {
			if p.OK {
				t.Errorf("wal_expected should fail when backups exist but wal/ is empty; got OK detail=%q", p.Detail)
			}
			return
		}
	}
	t.Errorf("wal_expected probe missing from report")
}

// TestProbesPresentInOrder: the report carries the probes in the
// documented order, regardless of which fail. Stable order is
// load-bearing for monitoring integrations that rely on probe
// indices.
func TestProbesPresentInOrder(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	r := check.Run(context.Background(), check.Options{Backend: b})
	want := []string{
		"storage_reachable",
		"info_decodable",
		"chain_integrity",
		"wal_expected",
		"archive_command",
		"standby_coordination",
	}
	if len(r.Probes) != len(want) {
		t.Fatalf("Probes len = %d, want %d", len(r.Probes), len(want))
	}
	for i, w := range want {
		if r.Probes[i].Name != w {
			t.Errorf("Probes[%d].Name = %q, want %q", i, r.Probes[i].Name, w)
		}
	}
}
