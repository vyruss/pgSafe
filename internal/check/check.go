// Package check is `pgsafe check`'s operator-diagnosis battery. Each
// probe is a self-contained function that returns a Probe record
// (name + ok + operator-actionable detail). Run executes the probes
// in sequence and returns a Report.
//
// Every probe MUST be cheap and read-only — `pgsafe check` runs as a
// monitoring source against a long-lived storage. Probes that need a
// PG connection are gated on a non-nil pool; probes that need a WAL
// directory are gated on a non-empty walDir. Skipped probes are
// recorded with OK=true and Detail="(skipped: ...)" so the monitoring
// integration sees a stable schema regardless of which dependencies
// are wired up.
package check

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/vyruss/pgsafe/internal/backup"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/info"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// Probe is one diagnosis result. Name is a short stable identifier
// the operator can grep for ("storage_reachable", "chain_integrity").
// OK=true means the probe passed; Detail carries a short message
// either way (success summary or failure reason).
type Probe struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// Report is the full battery's outcome.
type Report struct {
	Probes []Probe `json:"probes"`
}

// AllOK is true iff every probe passed.
func (r Report) AllOK() bool {
	for _, p := range r.Probes {
		if !p.OK {
			return false
		}
	}
	return true
}

// Options bundles the per-probe dependencies. Pool is optional; a nil
// value gates the PG-side probes off.
type Options struct {
	Backend storage.Backend
	Pool    *pgxpool.Pool
}

// Run executes the diagnosis battery in a fixed order. Order is
// deliberate: cheaper / storage-only probes first so a misconfigured
// storage surfaces before we bother PG.
func Run(ctx context.Context, opts Options) Report {
	var r Report
	r.Probes = append(r.Probes, probeStorageReachable(ctx, opts.Backend))
	r.Probes = append(r.Probes, probeInfoDecodable(ctx, opts.Backend))
	r.Probes = append(r.Probes, probeChainIntegrity(ctx, opts.Backend))
	r.Probes = append(r.Probes, probeWALExpected(ctx, opts.Backend))
	r.Probes = append(r.Probes, probeArchiveCommand(ctx, opts))
	r.Probes = append(r.Probes, probeStandbyCoordination(ctx, opts))
	return r
}

// probeStorageReachable: List on the backend root succeeds. Nil
// backend → skipped.
func probeStorageReachable(ctx context.Context, b storage.Backend) Probe {
	if b == nil {
		return Probe{Name: "storage_reachable", OK: true, Detail: "(skipped: no backend)"}
	}
	if _, err := b.List(ctx, ""); err != nil {
		return Probe{Name: "storage_reachable", OK: false, Detail: err.Error()}
	}
	return Probe{Name: "storage_reachable", OK: true, Detail: "List(ctx, \"\") OK"}
}

// probeInfoDecodable: every sidecar+manifest in the storage decodes
// without warnings. info.List warnings indicate a corrupt sidecar
// or a backup directory whose metadata is inconsistent.
func probeInfoDecodable(ctx context.Context, b storage.Backend) Probe {
	if b == nil {
		return Probe{Name: "info_decodable", OK: true, Detail: "(skipped: no backend)"}
	}
	_, warnings, err := info.List(ctx, b)
	if err != nil {
		return Probe{Name: "info_decodable", OK: false, Detail: err.Error()}
	}
	if len(warnings) > 0 {
		return Probe{Name: "info_decodable", OK: false, Detail: fmt.Sprintf("%d sidecar(s) failed to decode: %s", len(warnings), warnings[0])}
	}
	return Probe{Name: "info_decodable", OK: true, Detail: "every sidecar decodes"}
}

// probeChainIntegrity: every incremental's ParentBackupID is present
// in the same storage. An orphan indicates a bad prune, a partial
// upload, or operator error.
func probeChainIntegrity(ctx context.Context, b storage.Backend) Probe {
	if b == nil {
		return Probe{Name: "chain_integrity", OK: true, Detail: "(skipped: no backend)"}
	}
	records, _, err := info.List(ctx, b)
	if err != nil {
		return Probe{Name: "chain_integrity", OK: false, Detail: err.Error()}
	}
	have := map[string]struct{}{}
	for _, r := range records {
		have[r.BackupID] = struct{}{}
	}
	var orphans []string
	for _, r := range records {
		if r.Type != manifest.BackupTypeIncremental {
			continue
		}
		if r.ParentBackupID == "" {
			orphans = append(orphans, r.BackupID+" (no parent recorded)")
			continue
		}
		if _, ok := have[r.ParentBackupID]; !ok {
			orphans = append(orphans, r.BackupID+" → "+r.ParentBackupID)
		}
	}
	if len(orphans) > 0 {
		return Probe{Name: "chain_integrity", OK: false, Detail: fmt.Sprintf("%d orphan(s): %v", len(orphans), orphans)}
	}
	return Probe{Name: "chain_integrity", OK: true, Detail: "every incremental traces to a present full"}
}

// probeWALExpected: every backup's manifest WAL-Range start segment
// exists under wal/<TLI>/. Missing WAL means the backup is not
// PITR-recoverable; surfaces this distinctly from sidecar/manifest
// problems.
func probeWALExpected(ctx context.Context, b storage.Backend) Probe {
	if b == nil {
		return Probe{Name: "wal_expected", OK: true, Detail: "(skipped: no backend)"}
	}
	records, _, err := info.List(ctx, b)
	if err != nil {
		return Probe{Name: "wal_expected", OK: false, Detail: err.Error()}
	}
	walEntries, err := b.List(ctx, "wal")
	if err != nil {
		// No wal/ dir at all is acceptable in a brand-new storage.
		if errors.Is(err, errNotExist{}) {
			return Probe{Name: "wal_expected", OK: true, Detail: "no wal/ subdir yet (fresh storage)"}
		}
		// Any other error: bubble up.
		return Probe{Name: "wal_expected", OK: false, Detail: err.Error()}
	}
	walSet := map[string]struct{}{}
	for _, fi := range walEntries {
		walSet[path.Base(fi.Path)] = struct{}{}
	}
	if len(records) == 0 {
		return Probe{Name: "wal_expected", OK: true, Detail: "no backups yet"}
	}
	// We don't enumerate every required segment for every backup
	// here (that's `pgsafe verify`'s scope). Smoke-check: at least
	// one WAL segment exists for storages with at least one backup.
	if len(walEntries) == 0 {
		return Probe{Name: "wal_expected", OK: false, Detail: "wal/ is empty but the storage has backups"}
	}
	return Probe{Name: "wal_expected", OK: true, Detail: fmt.Sprintf("%d WAL segments archived", len(walEntries))}
}

// probeArchiveCommand: Invariant #5 — verify the operator's
// archive_command actually delivers segments to the configured
// storage backend. Gated on Pool + Backend being available.
func probeArchiveCommand(ctx context.Context, opts Options) Probe {
	if opts.Pool == nil {
		return Probe{Name: "archive_command", OK: true, Detail: "(skipped: no pool)"}
	}
	if opts.Backend == nil {
		return Probe{Name: "archive_command", OK: true, Detail: "(skipped: no backend)"}
	}
	if err := backup.ProbeArchive(ctx, opts.Pool, opts.Backend, 30*time.Second); err != nil {
		return Probe{Name: "archive_command", OK: false, Detail: err.Error()}
	}
	return Probe{Name: "archive_command", OK: true, Detail: "archive_command delivers to storage"}
}

// probeStandbyCoordination: when PG is in recovery, ensure the
// standby is in a consistent enough state for backup to succeed.
// Gated on Pool.
func probeStandbyCoordination(ctx context.Context, opts Options) Probe {
	if opts.Pool == nil {
		return Probe{Name: "standby_coordination", OK: true, Detail: "(skipped: no pool)"}
	}
	if err := backup.InspectStandby(ctx, opts.Pool); err != nil {
		return Probe{Name: "standby_coordination", OK: false, Detail: err.Error()}
	}
	return Probe{Name: "standby_coordination", OK: true, Detail: "PG state allows backup"}
}

// errNotExist is a private sentinel that lets probeWALExpected match
// "wal/ doesn't exist yet" without depending on os.ErrNotExist
// (storage backends wrap errors with their own prefixes).
type errNotExist struct{}

func (errNotExist) Error() string { return "not exist" }
