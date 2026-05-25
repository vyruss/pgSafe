// Package worker is the PG-host side of pgSafe mode. It implements
// rpc.WorkerService — the caller on the backup host calls Hello,
// Configure, StreamFile (N times in parallel), Done over a single SSH
// stdio JSON-RPC channel, and the worker:
//
//   - Reads each requested $PGDATA-relative file from the local filesystem.
//   - Validates PG page checksums client-side (heap files only).
//   - Streams through the filter chain (hash → compress → encrypt).
//   - Uploads the result DIRECTLY to the storage backend using the
//     in-memory scoped credentials it received in Configure.
//
// The credentials never touch disk on the PG host — they live in this
// process's heap and are GC'd at process exit. Tenet 3.
package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crypto/sha256"

	"filippo.io/age"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/filter/pagechecksum"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/storage/posix"
	"github.com/vyruss/pgsafe/internal/transport/creds"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
	"github.com/vyruss/pgsafe/internal/wal/archive"
)

// openWorkerPOSIX is the POSIX-backend equivalent of
// creds.OpenBackendFromCredential — POSIX has no credentials, so we just
// open a posix.Backend rooted at the worker-supplied path.
func openWorkerPOSIX(root string) (storage.Backend, func(), error) {
	b, err := posix.New(posix.Options{Root: root})
	if err != nil {
		return nil, func() {}, err
	}
	if err := b.Open(context.Background()); err != nil {
		return nil, func() {}, err
	}
	return b, func() {}, nil
}

// Worker is the production rpc.WorkerService implementation. Construct via
// New; callers pass the result to rpc.Serve.
type Worker struct {
	version    string
	pgDataPath string

	mu sync.Mutex
	// state established by Configure:
	configured           bool
	backupID             string
	chain                *filter.Chain
	backend              storage.Backend
	cleanup              func()
	pageMode             pagechecksum.Mode
	files                map[string]rpc.FileSpec // path → spec for Configure-time validation
	workerWritesDirectly bool                    // false = StreamChunk path; true = StreamFile path

	// counters updated by StreamFile under atomic ops:
	fileCount  int64
	bytesTotal int64
}

// New constructs a Worker. version is the pgsafe binary version (used in
// the Hello response); pgDataPath is the resolved $PGDATA directory the
// worker reads from.
func New(version, pgDataPath string) *Worker {
	return &Worker{
		version:    version,
		pgDataPath: pgDataPath,
	}
}

// Hello reports the worker's identity. Called once at the start of every
// pgSafe-mode backup; the caller uses ProtocolVersion to refuse
// version-skewed connections.
func (w *Worker) Hello(_ *rpc.HelloRequest, resp *rpc.HelloResponse) error {
	resp.WorkerVersion = w.version
	resp.ProtocolVersion = rpc.Version
	resp.OS = runtime.GOOS
	resp.NumCPU = runtime.NumCPU()
	resp.PGDataPath = w.pgDataPath
	return nil
}

// Configure parses the credential payload, opens the storage backend, and
// builds the filter chain. Subsequent StreamFile calls reuse this state.
//
// Tenet 3: the credential payload is consumed in-memory only; we never
// persist it. The filter-chain encryption uses the caller-supplied
// age recipients list (Invariant #9 reduces to "recipients are identical
// across the backup" because age uses asymmetric encryption — there's no
// symmetric "data key" to share).
func (w *Worker) Configure(req *rpc.ConfigureRequest, resp *rpc.ConfigureResponse) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.configured {
		resp.Error = "Configure called twice on the same worker"
		return nil
	}

	// Caller-proxy for cloud backends: when the caller decided
	// (storage_reach=via_caller / auto-fallback) that the worker can't
	// reach the cloud directly, it added `ssh -R <port>` to the SSH
	// session so the worker has a dynamic SOCKS5 listener on its own
	// loopback. Cloud SDKs (AWS, Azure, GCS) all honour HTTPS_PROXY /
	// HTTP_PROXY; setting them here is the single bottleneck that
	// routes every SDK call back through the caller. Worker process is
	// short-lived (one backup), so the env-var mutation has no
	// cross-call leakage.
	if req.HTTPSProxy != "" {
		_ = os.Setenv("HTTPS_PROXY", req.HTTPSProxy)
		_ = os.Setenv("HTTP_PROXY", req.HTTPSProxy)
	}

	// Two storage modes (per req.WorkerWritesDirectly):
	//   true  — worker constructs its own Backend and writes locally.
	//           StreamFile / WriteBlob / Commit run against w.backend.
	//   false — worker has no Backend. StreamChunk reads, filters, and
	//           returns the encrypted bytes for the caller to write
	//           to ITS local backend. StorageType/Path/Credentials ignored.
	if req.WorkerWritesDirectly {
		// Parse credentials (no disk).
		cred, err := creds.Unmarshal(req.Credentials)
		if err != nil {
			resp.Error = fmt.Sprintf("creds: %v", err)
			return nil
		}

		// Open backend. POSIX takes the path from the ConfigureRequest (no
		// credentials per Tenet 3 — the worker's filesystem mount governs).
		// Cloud backends use the in-memory scoped credential.
		switch req.StorageType {
		case "posix":
			if req.StoragePath == "" {
				resp.Error = "StorageType=posix requires StoragePath"
				return nil
			}
			backend, cleanup, err := openWorkerPOSIX(req.StoragePath)
			if err != nil {
				resp.Error = fmt.Sprintf("open posix: %v", err)
				return nil
			}
			w.backend = backend
			w.cleanup = cleanup
		case "s3", "azure", "gcs", "sftp":
			backend, cleanup, err := creds.OpenBackendFromCredential(context.Background(), cred)
			if err != nil {
				resp.Error = fmt.Sprintf("open backend: %v", err)
				return nil
			}
			w.backend = backend
			w.cleanup = cleanup
		default:
			resp.Error = fmt.Sprintf("unsupported StorageType %q for pgsafe-worker", req.StorageType)
			return nil
		}
	}
	w.workerWritesDirectly = req.WorkerWritesDirectly

	// Build filter chain. Recipients come over the wire as age-string-
	// encoded public keys; we re-parse them on this side.
	rcpts, err := parseRecipients(req.AgeRecipients)
	if err != nil {
		resp.Error = fmt.Sprintf("parse age recipients: %v", err)
		return nil
	}
	chain, err := filter.NewChain(filter.Options{
		Codec:      req.CompressionCodec,
		Level:      req.CompressionLevel,
		Recipients: rcpts,
	})
	if err != nil {
		resp.Error = fmt.Sprintf("filter chain: %v", err)
		return nil
	}
	w.chain = chain
	w.backupID = req.BackupID
	w.pageMode = pagechecksum.Mode(req.PageChecksumMode)
	if req.PGDataPath != "" {
		w.pgDataPath = req.PGDataPath
	}

	// Build a quick lookup so StreamFile rejects requests outside the list.
	w.files = make(map[string]rpc.FileSpec, len(req.Files))
	for _, f := range req.Files {
		w.files[f.Path] = f
	}

	// Sanity: kill any zero/sentinel data-key intent. v1 is age-
	// asymmetric; if introduces a symmetric mode this check
	// updates with it.
	_ = req.EncryptionDataKey

	w.configured = true
	return nil
}

// StreamFile handles one file from $PGDATA. The worker:
//
//  1. Opens the local file.
//  2. Wraps with pagechecksum.Validator for heap files.
//  3. Pipes through the filter chain into a Put against the backend.
//  4. Returns plaintext SHA-256 + on-the-wire byte count for the manifest.
//
// Concurrency: the caller is allowed to fan StreamFile out to N
// goroutines; net/rpc's serve loop runs one call at a time per connection
// in JSON-RPC mode (the codec is single-threaded), so these arrive
// serialized. The fan-out happens BACKGROUND-side via the caller's
// errgroup talking to multiple Workers (one per backup). For v1
// that's one worker process per backup; widening to N parallel
// in-process workers is a Cycle-7 optimization.
func (w *Worker) StreamFile(req *rpc.StreamFileRequest, resp *rpc.StreamFileResponse) error {
	w.mu.Lock()
	if !w.configured {
		w.mu.Unlock()
		return errors.New("StreamFile before Configure")
	}
	spec, ok := w.files[req.Path]
	chain := w.chain
	backend := w.backend
	backupID := w.backupID
	pageMode := w.pageMode
	w.mu.Unlock()
	if !ok {
		// Bracket WAL segments (WALSourceWalgrab) are not in the
		// Configure file list — their names depend on stop_lsn, which
		// is post-Configure. Allow them through if the path is a
		// well-formed pg_wal/<archivable> that lives inside $PGDATA.
		if !isPostStopWALPath(req.Path) {
			return fmt.Errorf("StreamFile: path %q not in Configure's file list", req.Path)
		}
		spec = rpc.FileSpec{Path: req.Path}
	}

	// Defense-in-depth path validation.
	if strings.Contains(req.Path, "..") || strings.HasPrefix(req.Path, "/") {
		return fmt.Errorf("StreamFile: path %q escapes $PGDATA", req.Path)
	}

	full := filepath.Join(w.pgDataPath, filepath.FromSlash(req.Path))
	f, err := os.Open(full) //nolint:gosec // path validated above; $PGDATA is operator-supplied
	if err != nil {
		return fmt.Errorf("open %s: %w", full, err)
	}
	defer func() { _ = f.Close() }()

	var src io.Reader = f
	if isHeapFile(req.Path) {
		src = pagechecksum.New(f, pageMode, 0, req.Path)
	}

	repoPath := filepath.ToSlash(filepath.Join(backupID, req.Path))
	wc, err := backend.Put(context.Background(), repoPath)
	if err != nil {
		return fmt.Errorf("backend.Put %s: %w", repoPath, err)
	}
	chainW, res, err := chain.Wrap(wc)
	if err != nil {
		return fmt.Errorf("filter.Wrap: %w", err)
	}
	if _, err := io.Copy(chainW, src); err != nil {
		_ = chainW.Close()
		return fmt.Errorf("copy %s: %w", req.Path, err)
	}
	if err := chainW.Close(); err != nil {
		return fmt.Errorf("close %s: %w", req.Path, err)
	}

	atomic.AddInt64(&w.fileCount, 1)
	atomic.AddInt64(&w.bytesTotal, res.Bytes)

	resp.Path = req.Path
	resp.Bytes = res.Bytes
	resp.SHA256 = res.SHA256
	resp.ModTime = time.Now().UTC()
	_ = spec
	return nil
}

// StreamChunk is the caller-storage path: read the file, run it
// through the filter chain into a memory buffer, and return the encrypted
// bytes for the caller to write to its own local backend. The worker
// holds no Backend in this mode.
//
// Returns the same plaintext SHA-256 / byte count metadata as StreamFile
// alongside the encrypted Body. Concurrency-safe; the dual codec carries
// the Body field over the gob discriminator so binary data flies through
// without JSON+base64 inflation.
func (w *Worker) StreamChunk(req *rpc.StreamChunkRequest, resp *rpc.StreamChunkResponse) error {
	w.mu.Lock()
	if !w.configured {
		w.mu.Unlock()
		return errors.New("StreamChunk before Configure")
	}
	if w.workerWritesDirectly {
		w.mu.Unlock()
		return errors.New("StreamChunk called but Configure said WorkerWritesDirectly=true")
	}
	spec, ok := w.files[req.Path]
	chain := w.chain
	pageMode := w.pageMode
	w.mu.Unlock()
	if !ok {
		// Same WALSourceWalgrab carve-out as StreamFile — see comment there.
		if !isPostStopWALPath(req.Path) {
			return fmt.Errorf("StreamChunk: path %q not in Configure's file list", req.Path)
		}
		spec = rpc.FileSpec{Path: req.Path}
	}
	if strings.Contains(req.Path, "..") || strings.HasPrefix(req.Path, "/") {
		return fmt.Errorf("StreamChunk: path %q escapes $PGDATA", req.Path)
	}

	full := filepath.Join(w.pgDataPath, filepath.FromSlash(req.Path))
	f, err := os.Open(full) //nolint:gosec // path validated above
	if err != nil {
		return fmt.Errorf("open %s: %w", full, err)
	}
	defer func() { _ = f.Close() }()

	var src io.Reader = f
	if isHeapFile(req.Path) {
		src = pagechecksum.New(f, pageMode, 0, req.Path)
	}

	// Buffer the filter-chain output in memory; PG segments cap at 1 GiB
	// uncompressed, so post-filter sizes are bounded by that. The buffer
	// is released as soon as the caller's response handler copies
	// it to its backend.
	buf := newWriteCloseBuffer()
	chainW, res, err := chain.Wrap(buf)
	if err != nil {
		return fmt.Errorf("filter.Wrap: %w", err)
	}
	if _, err := io.Copy(chainW, src); err != nil {
		_ = chainW.Close()
		return fmt.Errorf("copy %s: %w", req.Path, err)
	}
	if err := chainW.Close(); err != nil {
		return fmt.Errorf("close %s: %w", req.Path, err)
	}

	atomic.AddInt64(&w.fileCount, 1)
	atomic.AddInt64(&w.bytesTotal, res.Bytes)

	resp.Path = req.Path
	resp.Bytes = res.Bytes
	resp.SHA256 = res.SHA256
	resp.ModTime = time.Now().UTC()
	resp.Body = buf.Bytes()
	_ = spec
	return nil
}

// writeCloseBuffer is a bytes.Buffer that satisfies io.WriteCloser. The
// filter chain wants a Closer (it propagates Close to its underlying
// writer to flush the encryption stream); a bytes.Buffer alone doesn't.
type writeCloseBuffer struct {
	buf bytes.Buffer
}

func newWriteCloseBuffer() *writeCloseBuffer {
	return &writeCloseBuffer{}
}

func (w *writeCloseBuffer) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *writeCloseBuffer) Close() error                { return nil }
func (w *writeCloseBuffer) Bytes() []byte               { return w.buf.Bytes() }

// WriteBlob writes in-memory bytes (backup_label, tablespace_map, manifest,
// sidecar) into the backend. When Filtered=true, the bytes go through the
// filter chain (encrypt+compress); when false, they're written plaintext
// (the manifest and sidecar are not encrypted in v1).
func (w *Worker) WriteBlob(req *rpc.WriteBlobRequest, resp *rpc.WriteBlobResponse) error {
	w.mu.Lock()
	if !w.configured {
		w.mu.Unlock()
		return errors.New("WriteBlob before Configure")
	}
	chain := w.chain
	backend := w.backend
	w.mu.Unlock()

	wc, err := backend.Put(context.Background(), req.RepoPath)
	if err != nil {
		return fmt.Errorf("WriteBlob backend.Put %s: %w", req.RepoPath, err)
	}
	if !req.Filtered {
		// Plaintext write — used for manifest + sidecar.
		if _, err := wc.Write(req.Body); err != nil {
			_ = wc.Close()
			return fmt.Errorf("WriteBlob plaintext write %s: %w", req.RepoPath, err)
		}
		if err := wc.Close(); err != nil {
			return fmt.Errorf("WriteBlob close %s: %w", req.RepoPath, err)
		}
		// SHA-256 the plaintext for the manifest entry. (Manifest itself is
		// the plaintext write; backup_label is the filtered case below.)
		sum := sha256OfBytes(req.Body)
		resp.Bytes = int64(len(req.Body))
		resp.SHA256 = sum
		return nil
	}
	chainW, res, err := chain.Wrap(wc)
	if err != nil {
		return fmt.Errorf("WriteBlob filter.Wrap %s: %w", req.RepoPath, err)
	}
	if _, err := chainW.Write(req.Body); err != nil {
		_ = chainW.Close()
		return fmt.Errorf("WriteBlob write %s: %w", req.RepoPath, err)
	}
	if err := chainW.Close(); err != nil {
		return fmt.Errorf("WriteBlob close %s: %w", req.RepoPath, err)
	}
	resp.Bytes = res.Bytes
	resp.SHA256 = res.SHA256
	atomic.AddInt64(&w.fileCount, 1)
	atomic.AddInt64(&w.bytesTotal, res.Bytes)
	return nil
}

// Commit promotes Tmp → Final atomically on the backend. Used by the
// caller after WriteBlob(Filtered=false) of the manifest's .tmp.
func (w *Worker) Commit(req *rpc.CommitRequest, _ *rpc.CommitResponse) error {
	w.mu.Lock()
	if !w.configured {
		w.mu.Unlock()
		return errors.New("Commit before Configure")
	}
	backend := w.backend
	w.mu.Unlock()
	if err := backend.Commit(context.Background(), req.Tmp, req.Final); err != nil {
		return fmt.Errorf("Commit %s→%s: %w", req.Tmp, req.Final, err)
	}
	return nil
}

// ProbeStorage performs a one-shot reachability check against the
// storage backend the caller is about to ship via Configure.
// Opens the backend and immediately closes it; success means open+
// stat succeeded without error. The caller surfaces the
// result in the topology log per ARCHITECTURE.md "Wire architecture"
// "Operator footgun: accidental proxying."
func (w *Worker) ProbeStorage(req *rpc.ProbeStorageRequest, resp *rpc.ProbeStorageResponse) error {
	t0 := time.Now()
	defer func() {
		resp.DurationMS = time.Since(t0).Milliseconds()
	}()
	// Empty StorageType is the "worker has no backend role" signal
	// the caller sends in --storage-on=caller mode. Treat
	// it as a no-op success rather than fall through to the default
	// "unsupported" branch (which would surface as misleading
	// UNREACHABLE in the topology log). The caller gate at
	// pgsafe_worker.go skips the call entirely; this defends against
	// older callers that didn't gate.
	if req.StorageType == "" {
		resp.Reachable = true
		return nil
	}
	switch req.StorageType {
	case "posix":
		if req.StoragePath == "" {
			resp.Error = "StorageType=posix requires StoragePath"
			return nil
		}
		b, cleanup, err := openWorkerPOSIX(req.StoragePath)
		if err != nil {
			resp.Error = err.Error()
			return nil
		}
		cleanup()
		_ = b
		resp.Reachable = true
		return nil
	case "s3", "azure", "gcs", "sftp":
		cred, err := creds.Unmarshal(req.Credentials)
		if err != nil {
			resp.Error = "creds: " + err.Error()
			return nil
		}
		b, cleanup, err := creds.OpenBackendFromCredential(context.Background(), cred)
		if err != nil {
			resp.Error = err.Error()
			return nil
		}
		cleanup()
		_ = b
		resp.Reachable = true
		return nil
	default:
		resp.Error = "unsupported StorageType " + req.StorageType
		return nil
	}
}

// Restore runs a full restore on the worker's local filesystem.
// The caller points the worker at its source storage via
// Credentials (typically an SFTP-over-tunnel cred minted by
// sftptunnel), ships age identities and recovery knobs, and the
// worker calls restore.Run against TargetPath. Errors come back
// as a string in resp.Error per the the RPC pattern.
func (w *Worker) Restore(req *rpc.RestoreRequest, resp *rpc.RestoreResponse) error {
	cred, err := creds.Unmarshal(req.Credentials)
	if err != nil {
		resp.Error = fmt.Sprintf("creds: %v", err)
		return nil
	}
	ctx := context.Background()
	backend, cleanup, err := creds.OpenBackendFromCredential(ctx, cred)
	if err != nil {
		resp.Error = fmt.Sprintf("open backend: %v", err)
		return nil
	}
	defer cleanup()

	identities, err := parseIdentitiesPEM(req.AgeIdentities)
	if err != nil {
		resp.Error = fmt.Sprintf("parse identities: %v", err)
		return nil
	}

	// restore.Options is in the restore package; constructing it
	// here would create an import cycle (worker → restore → ...).
	// The runRestoreFn variable is set by package init in
	// internal/worker/restore_glue.go to the actual runner.
	if runRestoreFn == nil {
		resp.Error = "worker: runRestoreFn not initialised"
		return nil
	}
	res, err := runRestoreFn(ctx, backend, req, identities)
	if err != nil {
		resp.Error = err.Error()
		return nil
	}
	resp.BackupID = res.BackupID
	resp.Files = res.Files
	resp.WAL = res.WAL
	resp.Bytes = res.Bytes
	return nil
}

// RestoreResult mirrors restore.Result without the import-cycle
// problem. The glue file populates this from the real type.
type RestoreResult struct {
	BackupID string
	Files    int
	WAL      int
	Bytes    int64
}

// runRestoreFn is set by an init in package main (cmd/pgsafe) or by
// internal/worker/restore_glue.go to break the worker→restore cycle.
// nil during early Worker.Hello-only tests.
var runRestoreFn func(ctx context.Context, backend storage.Backend, req *rpc.RestoreRequest, identities []age.Identity) (RestoreResult, error)

// SetRunRestore wires the restore.Run-equivalent into Worker.Restore.
// Called from the cmd/pgsafe binary's init so test builds without the
// restore package can still use the worker for backup-only tests.
func SetRunRestore(fn func(ctx context.Context, backend storage.Backend, req *rpc.RestoreRequest, identities []age.Identity) (RestoreResult, error)) {
	runRestoreFn = fn
}

// parseIdentitiesPEM walks a slice of PEM/age-secret-key byte slices
// and returns the parsed age.Identity values. Each entry is parsed
// independently so a mix of formats can be supplied.
func parseIdentitiesPEM(blobs [][]byte) ([]age.Identity, error) {
	out := make([]age.Identity, 0, len(blobs))
	for _, b := range blobs {
		ids, err := age.ParseIdentities(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		out = append(out, ids...)
	}
	return out, nil
}

// Done returns the aggregate counters and tears down the backend. The
// worker's serve loop terminates on the next stdin EOF (from the
// caller's Session.Close).
func (w *Worker) Done(_ *rpc.DoneRequest, resp *rpc.DoneResponse) error {
	w.mu.Lock()
	cleanup := w.cleanup
	resp.FileCount = atomic.LoadInt64(&w.fileCount)
	resp.BytesTotal = atomic.LoadInt64(&w.bytesTotal)
	w.cleanup = nil
	w.mu.Unlock()
	if cleanup != nil {
		cleanup()
	}
	return nil
}

// parseRecipients converts the wire-format string slice back into
// age.Recipient values.
func parseRecipients(strs []string) ([]age.Recipient, error) {
	out := make([]age.Recipient, 0, len(strs))
	for i, s := range strs {
		r, err := age.ParseX25519Recipient(s)
		if err != nil {
			return nil, fmt.Errorf("recipient[%d] %q: %w", i, s, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// sha256OfBytes returns the SHA-256 of b as a fixed-size array.
func sha256OfBytes(b []byte) [32]byte {
	return sha256.Sum256(b)
}

// isHeapFile mirrors internal/backup/remoteparallel.isHeapFile. Heap files
// have PG 8 KiB page format; everything else (PG_VERSION, configs) does not.
func isHeapFile(rel string) bool {
	return strings.HasPrefix(rel, "base/") ||
		strings.HasPrefix(rel, "global/") ||
		strings.HasPrefix(rel, "pg_tblspc/")
}

// isPostStopWALPath reports whether rel is a pg_wal/<archivable> path —
// the shape WALSourceWalgrab uses to ship bracket segments after
// pg_backup_stop. The names depend on stop_lsn so they're not in the
// Configure file list; we let them through by shape, with the same
// "..-and-leading-/" guard StreamFile applies to every path.
func isPostStopWALPath(rel string) bool {
	if !strings.HasPrefix(rel, "pg_wal/") {
		return false
	}
	if strings.Contains(rel, "..") {
		return false
	}
	return archive.IsArchivableFile(strings.TrimPrefix(rel, "pg_wal/"))
}
