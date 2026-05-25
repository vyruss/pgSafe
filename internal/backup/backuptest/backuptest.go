// Package backuptest builds synthetic backup storages WITHOUT running
// real PG backups. retention/info/prune logic has the most
// edge-case-rich test surface in the project; using real PG fixtures for
// combinatorial testing (orphaned chains, deeply nested histories,
// expire-window boundaries, partial replication) would be prohibitively
// slow. This helper makes those tests run at unit speed.
//
// Outputs match the shape `internal/info` and `internal/retention` expect:
// per-backup directories `<id>F` / `<parent>_<ts>I` containing a
// `Storage-Metadata.json` sidecar and a tiny `backup_manifest` with one
// or more File entries plus a WAL-Ranges header.
package backuptest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path"
	"time"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// Builder synthesizes backups one at a time. Each Add* call writes the
// per-backup directory into the supplied storage.Backend.
type Builder struct {
	ctx     context.Context
	backend storage.Backend
	server  string

	// nextLSN is the WAL position the next backup starts at (incremented
	// per-backup so chains have realistic monotonic LSNs).
	nextLSN uint64
}

// New returns a Builder that writes into `b`. The backend must already be
// Open()-ed.
func New(ctx context.Context, b storage.Backend, server string) *Builder {
	return &Builder{ctx: ctx, backend: b, server: server, nextLSN: 0x3000028}
}

// AddFull writes one full backup to the backend with a sidecar timestamped
// at `at` and the supplied annotation. Returns the backup ID.
func (b *Builder) AddFull(at time.Time, annotation string) string {
	id := at.UTC().Format("20060102T150405") + "F"
	b.write(id, manifest.BackupTypeFull, "", at, annotation)
	return id
}

// AddIncremental writes one incremental backup chained to parent.
func (b *Builder) AddIncremental(parent string, at time.Time, annotation string) string {
	id := parent + "_" + at.UTC().Format("20060102T150405") + "I"
	b.write(id, manifest.BackupTypeIncremental, parent, at, annotation)
	return id
}

// AddOrphanedIncremental writes an incremental whose parent was never
// added. Used by retention tests that need to assert the orphaned-chain
// case is detected.
func (b *Builder) AddOrphanedIncremental(at time.Time, fakeParent string) string {
	return b.AddIncremental(fakeParent, at, "")
}

// AddWALSegment writes a sentinel WAL segment file at wal/<TLI>/<seg>
// with the given byte contents. Used by retention tests that check
// WAL pruning past OldestNeededLSN.
func (b *Builder) AddWALSegment(timeline uint32, segName string, body []byte) {
	key := fmt.Sprintf("wal/%08X/%s", timeline, segName)
	wc, err := b.backend.Put(b.ctx, key)
	if err != nil {
		panic(fmt.Sprintf("backuptest: Put %s: %v", key, err))
	}
	if _, err := wc.Write(body); err != nil {
		_ = wc.Close()
		panic(fmt.Sprintf("backuptest: Write %s: %v", key, err))
	}
	if err := wc.Close(); err != nil {
		panic(fmt.Sprintf("backuptest: Close %s: %v", key, err))
	}
}

// write does the actual sidecar+manifest emit for one backup.
func (b *Builder) write(id, typ, parent string, at time.Time, annotation string) {
	startLSN := b.nextLSN
	stopLSN := startLSN + 0x100000
	b.nextLSN = stopLSN + 0x100000

	// One synthetic data file (PG_VERSION-shaped) so the manifest has a
	// non-empty Files array. Tests that need richer file lists call
	// AddFile directly post-construction.
	body := []byte("18\n")
	if err := putBytes(b.ctx, b.backend, path.Join(id, "PG_VERSION"), body); err != nil {
		panic(fmt.Sprintf("backuptest: PG_VERSION: %v", err))
	}

	// backup_manifest: a minimal but parseable PG-native v2 manifest. Just
	// enough fields for `internal/info.readManifestHeader` to succeed.
	mb := manifest.NewBuilder(manifest.BackupStartInfo{
		SystemIdentifier: 1234567890,
		Timeline:         1,
		StartLSN:         manifest.LSN(startLSN),
		StartTime:        at.UTC(),
	})
	sum := sha256.Sum256(body)
	mb.AddFile("PG_VERSION", int64(len(body)), sum, at.UTC())
	manifestBytes, err := mb.Finalize(manifest.BackupStopInfo{
		StopLSN:  manifest.LSN(stopLSN),
		StopTime: at.UTC().Add(time.Minute),
	})
	if err != nil {
		panic(fmt.Sprintf("backuptest: manifest finalize: %v", err))
	}
	if err := putBytes(b.ctx, b.backend, path.Join(id, "backup_manifest"), manifestBytes); err != nil {
		panic(fmt.Sprintf("backuptest: backup_manifest: %v", err))
	}

	// Sidecar. Compression="none" and empty Recipients so the bytes
	// stored above (plaintext PG_VERSION) match what the manifest's
	// SHA-256 records — this makes the synthetic storage verifiable by
	// `internal/verify` without us having to actually run age + zstd
	// inside the test helper. Tests that need a real filter-chain
	// fixture build it via the hybrid-parallel integration
	// path instead.
	sc := manifest.Sidecar{
		Version:              manifest.SidecarVersion,
		Server:               b.server,
		Compression:          "none",
		StorageLayoutVersion: 1,
		Type:                 typ,
		ParentBackupID:       parent,
		Annotation:           annotation,
	}
	scBytes, err := manifest.MarshalSidecar(sc)
	if err != nil {
		panic(fmt.Sprintf("backuptest: marshal sidecar: %v", err))
	}
	if err := putBytes(b.ctx, b.backend, path.Join(id, "Storage-Metadata.json"), scBytes); err != nil {
		panic(fmt.Sprintf("backuptest: Storage-Metadata: %v", err))
	}
}

func putBytes(ctx context.Context, b storage.Backend, key string, body []byte) error {
	wc, err := b.Put(ctx, key)
	if err != nil {
		return err
	}
	if _, err := wc.Write(body); err != nil {
		_ = wc.Close()
		return err
	}
	return wc.Close()
}
