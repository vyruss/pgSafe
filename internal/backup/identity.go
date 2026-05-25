package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"

	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
)

// ErrClusterIdentityMismatch is returned when a backup's source PG
// cluster (system_identifier) doesn't match the most recent backup
// already in the storage. Indicates the operator is pointing the same
// storage + server name at a different cluster — split brain or a
// misconfiguration that would silently clobber the existing backup
// chain.
var ErrClusterIdentityMismatch = errors.New("backup: cluster identity mismatch with existing storage")

// verifyClusterIdentity walks the storage for the most recent committed
// backup and compares its sidecar's SystemIdentifier against
// currentSysID. Returns nil when:
//
//   - The storage has no committed backups yet (fresh storage).
//   - The most recent sidecar has SystemIdentifier == 0 (pre-#10
//     sidecar, written before this check existed — we trust the
//     caller for backwards-compat).
//   - The identifiers match.
//
// Returns ErrClusterIdentityMismatch wrapped with a diagnostic when
// the identifiers differ. The check uses the most recent committed
// backup only; older sidecars predate the cluster-identity field and
// would force-fail every legacy storage otherwise.
//
// Surfaces backend errors directly so callers can distinguish
// "couldn't list" from "cluster mismatch".
func verifyClusterIdentity(ctx context.Context, b storage.Backend, currentSysID uint64) error {
	if currentSysID == 0 {
		return nil // caller couldn't fetch identity; nothing to check against
	}
	infos, err := b.List(ctx, "")
	if err != nil {
		return fmt.Errorf("backup: list storage for identity check: %w", err)
	}
	// Collect committed backup IDs: directories whose `backup_manifest`
	// exists. With the FormatBackupID layout each manifest path is
	// "<id>/backup_manifest"; sort descending picks the latest.
	var manifestPaths []string
	for _, fi := range infos {
		base := path.Base(fi.Path)
		if base != "backup_manifest" {
			continue
		}
		manifestPaths = append(manifestPaths, fi.Path)
	}
	if len(manifestPaths) == 0 {
		return nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(manifestPaths)))

	// Read the sidecar of the most recent backup.
	latestID := path.Dir(manifestPaths[0])
	sidecarPath := path.Join(latestID, "Storage-Metadata.json")
	rc, err := b.Get(ctx, sidecarPath)
	if err != nil {
		return fmt.Errorf("backup: read sidecar of latest backup %s for identity check: %w", latestID, err)
	}
	defer func() { _ = rc.Close() }()
	buf, err := io.ReadAll(io.LimitReader(rc, 1<<20))
	if err != nil {
		return fmt.Errorf("backup: read sidecar bytes: %w", err)
	}
	sc, err := manifest.UnmarshalSidecar(buf)
	if err != nil {
		return fmt.Errorf("backup: parse sidecar of latest backup %s: %w", latestID, err)
	}
	if sc.SystemIdentifier == 0 {
		return nil // legacy sidecar
	}
	if sc.SystemIdentifier != currentSysID {
		return fmt.Errorf("%w: latest storage backup %s has SystemIdentifier=%d, current cluster has %d",
			ErrClusterIdentityMismatch, latestID, sc.SystemIdentifier, currentSysID)
	}
	return nil
}
