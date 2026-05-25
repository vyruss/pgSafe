// Package rpc defines the JSON-RPC 2.0 control plane the caller on the
// backup host uses to drive a worker on the PG host. Wire format is
// net/rpc/jsonrpc (stdlib); we never define a custom codec.
//
// Only control-plane traffic flows over the SSH stdio channel — never bulk
// file data. The PG-host worker uploads bytes directly to the storage
// backend using the in-memory credentials it received in Configure.
//
// Method-naming convention: WorkerService.<Verb>, where <Verb> is one of:
//
//	Hello       — handshake: caller-version vs worker-version match.
//	Configure   — deliver scoped credentials, encryption data key, file list.
//	StreamFile  — instruct the worker to stream one $PGDATA file through
//	              the filter chain into the storage backend.
//	Done        — collect aggregate stats and signal a clean exit on EOF.
//
// All RPC types are JSON-serializable; net/rpc reflects on them.
package rpc

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Version is the wire-protocol version. Bumped together with the pgsafe
// binary on hard-incompatible changes. See HelloResponse for the handshake.
const Version = "phase3-v1"

// HelloRequest is the caller's first call. Carries the caller's
// pgsafe version + protocol version so the worker can refuse mismatches.
type HelloRequest struct {
	CallerVersion   string // pgsafe binary version (e.g. v0.3.0)
	ProtocolVersion string // must equal Version
}

// HelloResponse echoes the worker's identity. The caller asserts
// ProtocolVersion equality before sending Configure.
type HelloResponse struct {
	WorkerVersion   string // pgsafe binary version on the PG host
	ProtocolVersion string // must equal Version
	OS              string // runtime.GOOS for diagnostics
	NumCPU          int    // workers default to min(NumCPU, 8)
	PGDataPath      string // resolved $PGDATA — caller may sanity-check
}

// ConfigureRequest delivers everything the worker needs to do its job.
// only specifies the schema; wires the actual semantics.
type ConfigureRequest struct {
	BackupID string

	// StorageType is one of "posix", "s3", "azure", "gcs", "sftp". The
	// worker picks the matching driver from internal/storage/<type> and
	// initializes it with the scoped credentials below.
	StorageType string

	// StoragePath is the worker-side root for type=posix. Cloud backends
	// derive the bucket/container/etc from the credential payload. POSIX
	// has no credentials per Tenet 3 (mount perms govern), so the path
	// comes through here. The caller and worker must agree on a
	// path that exists on the worker's filesystem (typically a shared
	// network mount or a per-test bind-mount).
	StoragePath string

	// PGDataPath is the worker-side absolute $PGDATA. The caller
	// queries PG for `data_directory` and ships the result so the worker
	// doesn't depend on the SSH-spawned shell having PGDATA in its env
	// (sshd doesn't pass through container ENTRYPOINT env vars).
	PGDataPath string

	// Credentials carries the Tenet-3 scoped, write-only, in-memory
	// credential payload. The exact shape depends on StorageType; see
	// internal/transport/creds. JSON-marshalable.
	Credentials []byte

	// EncryptionDataKey is the per-backup data key (Invariant #9). Workers
	// derive their filter-chain encryption from this and never generate
	// their own. 32 bytes.
	EncryptionDataKey []byte

	// AgeRecipients is the operator's age public-key list. The filter
	// chain's age stage encrypts to all of them; the data key is the inner
	// envelope. (Invariant #9 will refactor whether we keep age
	// per-recipient or move to a single-data-key model; v1 keeps the age
	// envelope and adds the per-backup key as a salt.)
	AgeRecipients []string

	// CompressionCodec / Level — the operator's filter-chain settings.
	CompressionCodec string
	CompressionLevel int

	// PageChecksumMode — pagechecksum.Mode encoded as int. The worker
	// validates heap pages before they hit the filter chain.
	PageChecksumMode int

	// Files is the list of $PGDATA-relative paths the worker must stream.
	// Built on the caller side from a `pg_ls_dir` walk so the
	// exclusion list is centralized.
	Files []FileSpec

	// WorkerWritesDirectly selects which storage path the worker uses:
	//
	//   true  — worker constructs its own Backend from StorageType +
	//           StoragePath + Credentials and writes encrypted bytes to it
	//           directly (e.g. cloud bucket the PG host can reach, or a
	//           POSIX path mounted on the PG host). StreamFile / WriteBlob
	//           / Commit RPCs land bytes via that backend.
	//
	//   false — worker has no backend. StreamChunk reads $PGDATA, runs the
	//           filter chain, and returns the encrypted bytes in the RPC
	//           response. The caller owns the storage and writes the
	//           bytes to ITS local backend. StorageType / StoragePath /
	//           Credentials are ignored on the worker side.
	WorkerWritesDirectly bool

	// HTTPSProxy carries a SOCKS5 proxy URL the worker must route cloud
	// backend traffic through. Set by the caller when caller-proxy
	// mode is engaged for cloud storage (via_caller / auto-fallback +
	// s3/azure/gcs): caller adds `ssh -R <port>` for a dynamic SOCKS5
	// listener on the worker side, and the worker exports HTTPS_PROXY
	// before opening the cloud SDK so SDK requests tunnel back through
	// the caller. Empty for direct reach. Format: "socks5h://127.0.0.1:<port>".
	HTTPSProxy string
}

// FileSpec is one entry in ConfigureRequest.Files.
type FileSpec struct {
	Path string // server-relative, e.g. "global/pg_control"
	Size int64
}

// ConfigureResponse is the worker's ack. Empty on success; an Error message
// is non-empty when the worker can't satisfy the request (bad creds, unknown
// codec, etc.).
type ConfigureResponse struct {
	Error string
}

// StreamFileRequest names one file the caller wants streamed.
type StreamFileRequest struct {
	Path string // must match a FileSpec.Path from Configure
}

// StreamFileResponse is the per-file result. Bytes is the on-the-wire
// (compressed+encrypted) byte count; SHA256 is the plaintext digest as
// returned by the filter chain (so the caller can record it in the
// PG-native backup_manifest).
type StreamFileResponse struct {
	Path    string
	Bytes   int64
	SHA256  [32]byte
	ModTime time.Time
}

// SHA256Hex is a convenience for log lines.
func (r *StreamFileResponse) SHA256Hex() string {
	return hex.EncodeToString(r.SHA256[:])
}

// WriteBlobRequest names a file the caller wants written from
// in-memory bytes (rather than streamed from $PGDATA). Used for
// backup_label and tablespace_map (which come from pg_backup_stop, not
// from the filesystem) and for the manifest+sidecar at backup-end.
type WriteBlobRequest struct {
	// RepoPath is the destination relative to the backend root.
	RepoPath string

	// Body is the plaintext bytes to write. The worker's filter chain
	// re-applies if the caller sets Filtered=true; otherwise the
	// bytes go through Put unwrapped.
	Body []byte

	// Filtered selects whether the worker pipes through the filter chain
	// (true → encrypt+compress) or writes plaintext (false → for the
	// final manifest and sidecar, which are not encrypted).
	Filtered bool
}

// WriteBlobResponse echoes the per-blob hash + on-the-wire byte count.
type WriteBlobResponse struct {
	Bytes  int64
	SHA256 [32]byte
}

// StreamChunkRequest names one $PGDATA-relative file the worker should
// read, run through the filter chain, and **return as bytes** so the
// caller can write them to its own local backend. This is the
// "caller-host owns the storage" path — the worker is purely a
// filter service in this mode.
//
// Goes over the gob discriminator on the dual codec; the response Body
// is `[]byte` and would inflate ~33% on JSON.
type StreamChunkRequest struct {
	Path string // must match a FileSpec.Path from Configure
}

// StreamChunkResponse carries the encrypted+compressed payload back to
// the caller along with the per-file manifest fields.
type StreamChunkResponse struct {
	Path    string
	Bytes   int64    // on-the-wire size; equals len(Body)
	SHA256  [32]byte // plaintext digest, for the manifest
	ModTime time.Time
	Body    []byte // encrypted+compressed bytes; caller writes this
}

// CommitRequest atomic-renames Tmp → Final on the backend (the worker's
// backend, which is the only one with credentials in hybrid mode).
type CommitRequest struct {
	Tmp   string
	Final string
}

// CommitResponse is empty on success.
type CommitResponse struct{}

// DoneRequest signals the caller is finished dispatching files. The
// worker drains in-flight uploads and replies with the aggregate.
type DoneRequest struct{}

// DoneResponse is the worker-side aggregate. The caller cross-checks
// against its own (it built the manifest from per-StreamFile responses).
type DoneResponse struct {
	FileCount  int64
	BytesTotal int64
}

// RestoreRequest carries everything a worker needs to run a full
// restore locally. The caller points the worker at its source
// storage via Credentials (typically an SFTP-over-tunnel credential
// generated by sftptunnel + creds.SFTPKey), ships the age identities
// and PG-recovery knobs, and the worker calls restore.Run against
// its local TargetPath.
type RestoreRequest struct {
	BackupID       string
	StorageType    string // "sftp" for hybrid restore over a tunnel
	Credentials    []byte // creds.Credential JSON
	AgeIdentities  [][]byte
	TargetPath     string // worker-local $PGDATA absolute path
	Workers        int
	StandbyMode    bool
	RestoreCommand string
}

// RestoreResponse mirrors restore.Result. Error is non-empty on
// failure and follows the the RPC pattern (errors travel as
// strings rather than as Go errors so net/rpc serialises them).
type RestoreResponse struct {
	BackupID string
	Files    int
	WAL      int
	Bytes    int64
	Error    string
}

// ProbeStorageRequest asks the worker to perform a one-shot
// reachability check against a storage backend without opening it
// for the duration of a backup. Used at session start so the
// caller can surface "worker→storage reachable / unreachable"
// in the topology log per ARCHITECTURE.md "Wire architecture".
type ProbeStorageRequest struct {
	StorageType string // "posix" | "s3" | "azure" | "gcs" | "sftp"
	StoragePath string // POSIX root if StorageType == "posix"
	Credentials []byte // creds.Credential JSON; cloud / sftp only
}

// ProbeStorageResponse reports whether the worker could reach the
// storage backend. Reachable=true means open + close succeeded;
// Error captures the failure on Reachable=false. DurationMS is the
// wall time of the open+close call, useful for sanity-checking the
// connection's latency before trusting it.
type ProbeStorageResponse struct {
	Reachable  bool
	Error      string
	DurationMS int64
}

// IntegrityHash builds the canonical hash over a HelloResponse — used by the
// caller to bind the worker version into a backup record
// will use this for forensics if a backup turns out to be corrupted).
func (h HelloResponse) IntegrityHash() [32]byte {
	s := h.WorkerVersion + "|" + h.ProtocolVersion + "|" + h.OS
	return sha256.Sum256([]byte(s))
}
