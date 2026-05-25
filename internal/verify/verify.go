// Package verify re-validates stored backups by re-hashing every file
// recorded in backup_manifest and comparing against the recorded
// SHA-256. Cycle-3 deliverable; reused by `pgsafe verify`,
// `pgsafe check` (single-backup probe), and operator-side post-restore
// audits.
//
// The chain mirrors restore in reverse: backend.Get → decrypt
// (age) → decompress (codec from sidecar) → SHA-256, compared to the
// PG-native checksum the caller recorded at backup time. A
// single-byte corruption in any stored file surfaces as a mismatch
// with the path and both hashes.
package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"runtime"
	"strings"
	"sync"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/filter/compression"
	"github.com/vyruss/pgsafe/internal/filter/encryption"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/storage"
	"golang.org/x/sync/errgroup"
)

// Options configures one Verify call.
type Options struct {
	// BackupID names a single backup to verify. Empty verifies every
	// backup found by walking the backend.
	BackupID string

	// Workers caps the in-flight per-file verifications across all
	// backups. Zero defaults to runtime.NumCPU().
	Workers int

	// Identities decrypts age-encrypted files. Required when the storage
	// was written with non-empty `encryption.recipients`; ignored when
	// the sidecar shows no recipients.
	Identities []age.Identity
}

// Mismatch records one file whose recomputed hash didn't match the
// manifest's recorded value. Path is the relative-in-backup path;
// Expected and Actual are SHA-256 hex strings.
type Mismatch struct {
	BackupID string `json:"backup_id"`
	Path     string `json:"path"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Reason   string `json:"reason,omitempty"`
}

// Result is the per-backup outcome.
type Result struct {
	BackupID            string     `json:"backup_id"`
	FilesOK             int        `json:"files_ok"`
	FilesMismatched     int        `json:"files_mismatched"`
	Mismatches          []Mismatch `json:"mismatches,omitempty"`
	ManifestChecksumOK  bool       `json:"manifest_checksum_ok"`
	ManifestChecksumErr string     `json:"manifest_checksum_err,omitempty"`
}

// AllOK is true iff every per-file SHA matched and the manifest
// checksum verified.
func (r Result) AllOK() bool {
	return r.FilesMismatched == 0 && r.ManifestChecksumOK
}

// Verify re-hashes every file in each requested backup. Returns one
// Result per backup. Caller-side error means the storage backend or
// sidecar/manifest is unreadable; per-backup mismatches surface in
// Result.Mismatches and do NOT cause an error return.
func Verify(ctx context.Context, b storage.Backend, opts Options) ([]Result, error) {
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	ids, err := selectBackupIDs(ctx, b, opts.BackupID)
	if err != nil {
		return nil, err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	var (
		mu      sync.Mutex
		results = make([]Result, len(ids))
	)
	for i, id := range ids {
		i, id := i, id
		results[i].BackupID = id
		body, err := readBlob(ctx, b, path.Join(id, "backup_manifest"))
		if err != nil {
			results[i].ManifestChecksumErr = err.Error()
			continue
		}
		results[i].ManifestChecksumOK = manifestChecksumOK(body)
		sc, err := readSidecar(ctx, b, id)
		if err != nil {
			results[i].ManifestChecksumErr = "read sidecar: " + err.Error()
			continue
		}
		// codec=nil means "stored bytes are plaintext" — sidecar was
		// emitted with Compression="" or "none". Verify treats those
		// equivalently rather than asking the compression registry to
		// recognise a passthrough codec it shouldn't have to.
		var codec compression.Codec
		codecName := codecFromSidecar(sc)
		if codecName != "" && codecName != "none" {
			codec, err = compression.Get(codecName)
			if err != nil {
				results[i].ManifestChecksumErr = "compression.Get: " + err.Error()
				continue
			}
		}
		files, err := parseManifestFiles(body)
		if err != nil {
			results[i].ManifestChecksumErr = "parse manifest: " + err.Error()
			continue
		}
		for _, f := range files {
			f := f
			g.Go(func() error {
				if err := gctx.Err(); err != nil {
					return err
				}
				m, ok := verifyOne(gctx, b, codec, opts.Identities, id, f)
				mu.Lock()
				if ok {
					results[i].FilesOK++
				} else {
					results[i].FilesMismatched++
					results[i].Mismatches = append(results[i].Mismatches, m)
				}
				mu.Unlock()
				return nil
			})
		}
	}
	if err := g.Wait(); err != nil {
		return results, err
	}
	return results, nil
}

// verifyOne reads a single stored file, runs it through decrypt +
// decompress (each step skipped when not in use), hashes the plaintext,
// compares to the manifest's recorded SHA-256. codec=nil means the
// stored bytes are plaintext. ids=nil/empty means the stored bytes
// are not age-encrypted.
func verifyOne(ctx context.Context, b storage.Backend, codec compression.Codec, ids []age.Identity, backupID string, f manifestFile) (Mismatch, bool) {
	rc, err := b.Get(ctx, path.Join(backupID, f.Path))
	if err != nil {
		return Mismatch{BackupID: backupID, Path: f.Path, Expected: f.SHA256, Reason: "Get: " + err.Error()}, false
	}
	defer func() { _ = rc.Close() }()

	var src io.Reader = rc
	if len(ids) > 0 {
		dec, derr := encryption.NewReader(rc, ids)
		if derr != nil {
			return Mismatch{BackupID: backupID, Path: f.Path, Expected: f.SHA256, Reason: "decrypt: " + derr.Error()}, false
		}
		src = dec
	}
	if codec != nil {
		zr, err := codec.NewReader(src)
		if err != nil {
			return Mismatch{BackupID: backupID, Path: f.Path, Expected: f.SHA256, Reason: "decompress: " + err.Error()}, false
		}
		defer func() { _ = zr.Close() }()
		src = zr
	}

	h := sha256.New()
	if _, err := io.Copy(h, src); err != nil {
		return Mismatch{BackupID: backupID, Path: f.Path, Expected: f.SHA256, Reason: "hash copy: " + err.Error()}, false
	}
	actual := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(actual, f.SHA256) {
		return Mismatch{BackupID: backupID, Path: f.Path, Expected: f.SHA256, Actual: actual, Reason: "sha256 mismatch"}, false
	}
	return Mismatch{}, true
}

// manifestChecksumOK recomputes SHA-256 over every byte before the
// closing `"Manifest-Checksum"` field and compares to the recorded
// hex value after `"Manifest-Checksum": "`. Returns false on any
// structural inconsistency (missing field, bad hex, length mismatch).
func manifestChecksumOK(body []byte) bool {
	mark := []byte(`"Manifest-Checksum"`)
	idx := bytes.Index(body, mark)
	if idx < 0 {
		return false
	}
	prefix := []byte(`"Manifest-Checksum": "`)
	hexStart := bytes.Index(body, prefix)
	if hexStart < 0 {
		return false
	}
	hexStart += len(prefix)
	hexEnd := bytes.IndexByte(body[hexStart:], '"')
	if hexEnd <= 0 {
		return false
	}
	got := string(body[hexStart : hexStart+hexEnd])
	want := sha256.Sum256(body[:idx])
	return strings.EqualFold(got, hex.EncodeToString(want[:]))
}

// manifestFile is the minimum we need to verify one file: path, size,
// recorded SHA-256.
type manifestFile struct {
	Path   string
	Size   int64
	SHA256 string
}

func parseManifestFiles(body []byte) ([]manifestFile, error) {
	var raw struct {
		Files []struct {
			Path     string `json:"Path"`
			Size     int64  `json:"Size"`
			Checksum string `json:"Checksum"`
		} `json:"Files"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	out := make([]manifestFile, 0, len(raw.Files))
	for _, f := range raw.Files {
		if f.Checksum == "" {
			continue
		}
		out = append(out, manifestFile{Path: f.Path, Size: f.Size, SHA256: f.Checksum})
	}
	return out, nil
}

func selectBackupIDs(ctx context.Context, b storage.Backend, only string) ([]string, error) {
	if only != "" {
		return []string{only}, nil
	}
	all, err := b.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("verify: List: %w", err)
	}
	seen := map[string]struct{}{}
	for _, fi := range all {
		first, _, ok := strings.Cut(fi.Path, "/")
		if !ok {
			continue
		}
		if !strings.HasSuffix(first, "F") && !strings.HasSuffix(first, "I") {
			continue
		}
		seen[first] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids, nil
}

func readBlob(ctx context.Context, b storage.Backend, p string) ([]byte, error) {
	rc, err := b.Get(ctx, p)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

func readSidecar(ctx context.Context, b storage.Backend, backupID string) (manifest.Sidecar, error) {
	body, err := readBlob(ctx, b, path.Join(backupID, "Storage-Metadata.json"))
	if err != nil {
		return manifest.Sidecar{}, err
	}
	return manifest.UnmarshalSidecar(body)
}

func codecFromSidecar(sc manifest.Sidecar) string {
	if sc.Compression == "" {
		return "none"
	}
	if i := strings.Index(sc.Compression, ":"); i > 0 {
		return sc.Compression[:i]
	}
	return sc.Compression
}

// ErrManifestUnreadable is returned via Result.ManifestChecksumErr.
// Surfaced as a sentinel for callers (cmd/pgsafe/verify) that want to
// distinguish "manifest unreadable" from "files mismatched."
var ErrManifestUnreadable = errors.New("verify: manifest unreadable")
