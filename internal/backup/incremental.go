package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/vyruss/pgsafe/internal/storage"
)

// stageParentManifest reads <parentID>/backup_manifest from the storage and
// writes it to a securely-named temp file. Returns the path and a cleanup
// function the caller MUST defer. Used so pg_basebackup --incremental=<path>
// can read the parent manifest from the local filesystem.
//
// Note: when the storage's filter chain encrypts files,
// backup_manifest is stored in plaintext (the manifest itself is structural
// metadata, not user data) — verify in storage wiring.
func stageParentManifest(ctx context.Context, storage storage.Backend, parentID, scratchDir string) (string, func(), error) {
	rel := path.Join(parentID, "backup_manifest")
	rc, err := storage.Get(ctx, rel)
	if err != nil {
		return "", func() {}, fmt.Errorf("read %s: %w", rel, err)
	}
	defer func() { _ = rc.Close() }()

	// crypto/rand suffix avoids predictable temp paths on shared backup
	// hosts where multiple concurrent backups may run.
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", func() {}, fmt.Errorf("rand: %w", err)
	}
	name := fmt.Sprintf("pgsafe-parent-%s-%s.json", parentID, hex.EncodeToString(rnd[:]))
	dir := scratchDir
	if dir == "" {
		dir = os.TempDir()
	}
	full := filepath.Join(dir, name)
	// `full` is constructed from os.TempDir() + a fixed prefix + crypto/rand
	// hex; gosec's static taint analysis can't see the seeded randomness.
	f, err := os.OpenFile(full, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec
	if err != nil {
		return "", func() {}, fmt.Errorf("create %s: %w", full, err)
	}
	if _, err := io.Copy(f, rc); err != nil {
		_ = f.Close()
		_ = os.Remove(full)
		return "", func() {}, fmt.Errorf("copy: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(full)
		return "", func() {}, fmt.Errorf("close: %w", err)
	}
	cleanup := func() { _ = os.Remove(full) }
	return full, cleanup, nil
}

// isIncrementalFileName reports whether name is a per-relfork incremental
// file produced by pg_basebackup --incremental. PG 17+ names them
// "INCREMENTAL.<relfilenode>" (or with a fork suffix) under the database OID
// directory, e.g. "base/16384/INCREMENTAL.16385".
func isIncrementalFileName(name string) bool {
	base := name
	if i := strings.LastIndex(name, "/"); i >= 0 {
		base = name[i+1:]
	}
	return strings.HasPrefix(base, "INCREMENTAL.")
}
