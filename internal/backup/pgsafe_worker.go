package backup

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/filter"
	"github.com/vyruss/pgsafe/internal/filter/pagechecksum"
	"github.com/vyruss/pgsafe/internal/manifest"
	"github.com/vyruss/pgsafe/internal/pg/bracket"
	"github.com/vyruss/pgsafe/internal/pg/readbinary"
	"github.com/vyruss/pgsafe/internal/storage"
	"github.com/vyruss/pgsafe/internal/transport"
	"github.com/vyruss/pgsafe/internal/transport/creds"
	"github.com/vyruss/pgsafe/internal/transport/local"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
	"github.com/vyruss/pgsafe/internal/transport/sftptunnel"
	"github.com/vyruss/pgsafe/internal/transport/ssh"
	"golang.org/x/sync/errgroup"
)

// ModeWorker selects the pgSafe-mode backup caller: a worker
// process on the PG host (SSH-spawned cross-host, or local subprocess
// when the caller already runs there). Bulk bytes flow worker→storage
// directly; SSH stdio carries the control plane only.
const ModeWorker Mode = "worker"

// WorkerOptions extends Options for ModeWorker. Embedded into
// backup.Options so Run can dispatch.
type WorkerOptions struct {
	// Pool is the caller-side pgxpool.Pool used for bracket only.
	// The PG-host worker uses ITS OWN local filesystem reads — it never
	// goes through libpq for bulk data.
	Pool *pgxpool.Pool

	// PGVersion is the PG major version (13–18). Picks the bracket dialect.
	PGVersion int

	// SSHTarget is what /usr/bin/ssh receives — typically "user@host".
	// Empty string selects the same-host transport: the worker is
	// spawned as a local subprocess of the caller (no SSH). This
	// is mandatory for v1 — same-host pgsafe-worker must work without
	// any SSH involvement.
	SSHTarget string

	// SSHExtraArgs are passed to /usr/bin/ssh before the target. Use this
	// for IdentityFile pinning, ProxyJump, ConnectTimeout, etc. Ignored
	// when SSHTarget is empty (same-host mode).
	SSHExtraArgs []string

	// RemoteCommand is the argv the worker runs. SSH mode: defaults to
	// {"pgsafe", "worker", "stdio"} resolved on the remote $PATH.
	// Same-host mode: defaults to {os.Executable(), "worker", "stdio"}
	// so the caller and worker run from the SAME binary, ruling
	// out version-mismatch surprises. Tests can inject a freshly-built
	// binary path either way.
	RemoteCommand []string

	// Storage carries the backend-specific configuration the worker uses
	// to construct its own client. The caller mints scoped
	// credentials from this and ships them inside ConfigureRequest.
	Storage config.StorageConfig

	// PageChecksumMode selects the worker's heap-page validation mode.
	PageChecksumMode pagechecksum.Mode

	// Workers is the maximum number of in-flight StreamFile RPCs over
	// the (single) JSON-RPC connection. Zero defaults to
	// runtime.NumCPU(). The worker dispatches one goroutine per
	// incoming call automatically (net/rpc behavior); the caller
	// caps fan-out via errgroup.SetLimit so we don't queue up the
	// entire $PGDATA file list at once.
	Workers int

	// WorkerWritesDirectly chooses where the encrypted bytes land:
	//
	//   true  — worker constructs its own Backend (cloud SDK, or a POSIX
	//           path mounted on the PG host) from Storage + scoped
	//           credentials and writes directly. The caller sees
	//           per-file metadata only; no bulk bytes flow back.
	//
	//   false — worker has no Backend. Each StreamChunk RPC reads $PGDATA,
	//           runs the filter chain, and returns the encrypted bytes to
	//           the caller over the dual codec's gob channel. The
	//           caller writes them to ITS local backend
	//           (opts.Backend / opts.Backends). Use when the caller
	//           host owns the storage and the PG host has no path or
	//           credentials of its own.
	//
	// Tenet 3 holds either way: in mode (true) credentials live only in
	// the worker's heap; in mode (false) the worker has no credentials at
	// all (caller owns the storage end).
	WorkerWritesDirectly bool

	// StorageReach mirrors cfg.PG.StorageReach — controls how pgsafe
	// decides whether to let the worker reach storage natively or to
	// proxy through the caller via ssh -R / ssh -D.
	//   ""           same as "auto"
	//   "auto"       probe; on UNREACHABLE, fall back to caller-proxy
	//   "native_only" require direct reach; abort if probe fails
	//   "via_caller" skip probe; force caller-proxy
	// Only consulted when WorkerWritesDirectly is true and storage
	// type is sftp or a cloud (s3 / azure / gcs).
	StorageReach string

	// ConfirmProxy makes auto-mode pre-probe failures interactive: when
	// the worker can't reach storage directly and storage_reach=auto,
	// pgsafe prompts on stderr ("fall back to caller-proxy? [y/N]") and
	// reads a y/n answer from Stdin. Only meaningful at a terminal —
	// cron and systemd will see EOF on Stdin and decline (matches the
	// "n" path: abort the backup). Default false: silently fall back,
	// matching the unattended cron use case.
	ConfirmProxy bool
}

// dialWorker selects the transport based on SSHTarget. Cross-host (SSH)
// path uses /usr/bin/ssh + the operator-supplied target; same-host path
// (SSHTarget=="") spawns the caller's own binary as a subprocess
// via exec.Cmd. Both end in a transport.Session over which a single
// rpc.Client multiplexes parallel calls (5: one connection,
// N goroutines, real parallelism).
func dialWorker(ctx context.Context, wOpts WorkerOptions) (transport.Session, error) {
	if wOpts.SSHTarget != "" {
		cmd := wOpts.RemoteCommand
		if len(cmd) == 0 {
			cmd = []string{"pgsafe", "worker", "stdio"}
		}
		return ssh.Dial(ctx, ssh.Options{
			Target:    wOpts.SSHTarget,
			ExtraArgs: wOpts.SSHExtraArgs,
			Command:   cmd,
		})
	}
	cmd := wOpts.RemoteCommand
	if len(cmd) == 0 {
		exe, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("pgsafe-worker local: os.Executable: %w", err)
		}
		cmd = []string{exe, "worker", "stdio"}
	}
	return local.Dial(ctx, local.Options{Command: cmd})
}

// runWorkerBackup implements ModeWorker. Step ordering matches
// the simple/remote-parallel callers: bracket.Start → ship file list +
// scoped creds via RPC → worker streams files directly to backend →
// bracket.Stop → WAL-wait → manifest commit.
//
// Invariant #1 (WAL-wait after every file is durable) holds because the
// worker's StreamFile completes only after Backend.Put.Close, which is the
// per-backend durability point (POSIX fsync, S3 multipart-complete, etc.).
//
// Invariant #9 (encryption key consistency) reduces here to "shipped age
// recipients are identical across all worker calls" — true by construction
// because Configure runs once at the top.
func runWorkerBackup(ctx context.Context, opts Options, wOpts WorkerOptions) (Result, error) {
	if wOpts.Pool == nil {
		return Result{}, errors.New("backup: pgsafe-worker requires Pool")
	}
	if wOpts.PGVersion < 13 {
		return Result{}, errors.New("backup: pgsafe-worker requires PGVersion >= 13")
	}
	workers := wOpts.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	startedAt := opts.Now()
	backupID, err := ChooseBackupID(ctx, opts.Backend, opts.Type, startedAt)
	if err != nil {
		return Result{}, err
	}

	logf := func(format string, args ...any) {
		_, _ = fmt.Fprintf(stderrFor(opts), "pgsafe backup: "+format+"\n", args...)
	}

	// Pre-probe gate for storage_reach=auto. Resolves the operator's
	// "auto" intent into a concrete direct-vs-caller-proxy shape BEFORE
	// bracket.Start, so a UNREACHABLE worker can fall back without
	// leaving an open backup bracket on the PG cluster. Only paid in
	// auto mode against sftp/cloud storage with ssh transport — the
	// cases where caller-proxy is actually a possibility.
	if wOpts.WorkerWritesDirectly && wOpts.SSHTarget != "" &&
		wOpts.StorageReach == "auto" &&
		(wOpts.Storage.Type == "sftp" || isCloudStorageType(wOpts.Storage.Type)) {
		reachable, err := preProbeStorage(ctx, wOpts, opts.Label, logf)
		if err != nil {
			return Result{}, fmt.Errorf("backup: pre-probe: %w", err)
		}
		switch {
		case reachable:
			logf("storage_reach=auto: pre-probe REACHABLE; using direct worker→storage")
		case wOpts.ConfirmProxy:
			ok, err := confirmProxyFallback(os.Stdin, os.Stderr)
			if err != nil {
				return Result{}, fmt.Errorf("backup: confirm-proxy: %w", err)
			}
			if !ok {
				return Result{}, errors.New("backup: storage_reach=auto + --confirm-proxy declined; aborting (use --confirm-proxy=false or storage_reach=via_caller to force fallback)")
			}
			logf("storage_reach=auto: pre-probe UNREACHABLE; operator confirmed caller-proxy fallback")
			wOpts.StorageReach = "via_caller"
		default:
			logf("storage_reach=auto: pre-probe UNREACHABLE; falling back to caller-proxy")
			wOpts.StorageReach = "via_caller"
		}
	}

	// Topology log: every backup prints the resolved shape so operators
	// scanning cron output can see what mode is in use, where the
	// worker landed, and how bytes flow. See ARCHITECTURE.md "Wire architecture"
	// "Operator footgun: accidental proxying" for the rationale.
	workerLoc := "same-host (local subprocess)"
	if wOpts.SSHTarget != "" {
		workerLoc = "ssh " + wOpts.SSHTarget
	}
	reach := "direct (worker → storage)"
	switch {
	case !wOpts.WorkerWritesDirectly && wOpts.Storage.Type == "posix":
		reach = "via caller (worker → caller → POSIX storage)"
	case wantsSFTPProxy(wOpts.StorageReach, wOpts.SSHTarget) &&
		wOpts.WorkerWritesDirectly && wOpts.Storage.Type == "sftp":
		reach = "via caller (worker → caller → SFTP storage)"
	case wantsCloudProxy(wOpts.StorageReach, wOpts.SSHTarget) &&
		wOpts.WorkerWritesDirectly && isCloudStorageType(wOpts.Storage.Type):
		reach = "via caller (worker → caller → cloud, SOCKS5)"
	}
	storageDesc := describeStorage(wOpts.Storage)
	_, _ = fmt.Fprintf(stderrFor(opts), "pgsafe backup: topology\n"+
		"  mode    = pgSafe (worker on %s)\n"+
		"  storage = %s\n"+
		"  reach   = %s\n"+
		"  workers = %d\n"+
		"  backup-id = %s\n",
		workerLoc, storageDesc, reach, workers, backupID)
	if wOpts.StorageReach == "via_caller" && wOpts.SSHTarget == "" {
		logf("storage_reach=via_caller has no effect on same-host transport (no ssh tunnel); reaching storage directly")
	}

	// Invariant #5 — verify the operator's archive_command before
	// bothering PG with bracket.Start.
	if wOpts.Pool != nil {
		if err := ProbeArchive(ctx, wOpts.Pool, opts.Backend, opts.WALTimeout); err != nil {
			return Result{}, fmt.Errorf("backup: %w", err)
		}
		logf("WAL archive reachability probe: OK")
	}

	// Cluster identity (system identifier, WAL segment size, timeline).
	id, err := opts.Cluster.Identity(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("backup: identity: %w", err)
	}
	if err := verifyClusterIdentity(ctx, opts.Backend, id.SystemIdentifier); err != nil {
		return Result{}, err
	}

	// Invariant #8 — backup-from-standby coordination.
	if err := InspectStandby(ctx, wOpts.Pool); err != nil {
		return Result{}, fmt.Errorf("backup: %w", err)
	}

	// Resume discovery + clean (RESUME.md). Identical mechanics to
	// runSimple — no wire change to the worker; the caller-side
	// per-file loop below skips StreamFile for paths whose prior
	// repo bytes are still intact on storage.
	resumedFrom, _ := tryResume(ctx, opts, id.SystemIdentifier)
	var resumePlan *reusablePlan
	if resumedFrom != nil {
		logf("resuming backup id=%s from %d-file checkpoint at %s",
			resumedFrom.BackupID, len(resumedFrom.Files), resumedFrom.CheckpointedAt.Format(time.RFC3339))
		backupID = resumedFrom.BackupID
		resumePlan = cleanResumable(ctx, opts.Backend, resumedFrom, logf)
	}

	// Bracket: pg_backup_start.
	br, err := bracket.New(wOpts.Pool, wOpts.PGVersion)
	if err != nil {
		return Result{}, fmt.Errorf("backup: bracket: %w", err)
	}
	startInfo, err := br.Start(ctx, opts.Label, true)
	if err != nil {
		return Result{}, fmt.Errorf("backup: bracket.Start: %w", err)
	}
	logf("bracket.Start: lsn=%s timeline=%d", startInfo.LSN, startInfo.Timeline)

	// File-list discovery from the PG side. Same exclusion list as the
	// remote-parallel caller — the worker reads its local $PGDATA
	// and the caller only needs the list of paths.
	files, err := readbinary.ListPGData(ctx, wOpts.Pool)
	if err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: ListPGData: %w", err)
	}
	logf("discovered %d files in $PGDATA", len(files))

	// Empty runtime dirs (pg_notify, pg_replslot, …) — restore needs the
	// list because they have no files for parent-dir creation to cover.
	pgDataDirs, err := readbinary.ListPGDataDirs(ctx, wOpts.Pool)
	if err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: ListPGDataDirs: %w", err)
	}

	// Ask PG for its data_directory; the worker uses this as its read root.
	// SSH-spawned commands don't inherit the docker-entrypoint's PGDATA
	// env, so we ship it explicitly.
	var pgDataPath string
	if err := wOpts.Pool.QueryRow(ctx,
		`SELECT current_setting('data_directory')`).Scan(&pgDataPath); err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: query data_directory: %w", err)
	}
	logf("worker pgdata: %s", pgDataPath)

	// Caller-side I/O for POSIX storage: the worker can't reach
	// caller's local POSIX directly, so the caller spins up an
	// in-process SFTP server bound to its own loopback, sets up an SSH
	// reverse port-forward into the worker's session, and reconfigures
	// the worker to write its bytes through that tunnel as if it were
	// a normal SFTP storage backend. After this transformation the
	// "WorkerWritesDirectly=false + posix" combination becomes
	// "WorkerWritesDirectly=true + sftp" — single byte path.
	var (
		credBytes        []byte
		workerHTTPSProxy string
	)
	switch {
	case !wOpts.WorkerWritesDirectly && wOpts.Storage.Type == "posix":
		sftpd, err := sftptunnel.StartEphemeralServer(ctx, wOpts.Storage.Path)
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: sftptunnel start: %w", err)
		}
		defer func() { _ = sftpd.Close() }()

		remotePort, err := sftptunnel.PickRemotePort()
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: pick remote port: %w", err)
		}

		fwdArgs := sftptunnel.ReverseForwardArgs(remotePort, sftpd.LocalPort())
		wOpts.SSHExtraArgs = append(append([]string{}, fwdArgs...), wOpts.SSHExtraArgs...)
		logf("sftp-tunnel: caller serving %s on 127.0.0.1:%d, worker dials its loopback:%d",
			wOpts.Storage.Path, sftpd.LocalPort(), remotePort)

		basePath := wOpts.Storage.Path
		wOpts.Storage = config.StorageConfig{
			Type: "sftp",
			SFTP: &config.SFTPConfig{
				Host:                  "127.0.0.1",
				Port:                  remotePort,
				Username:              "pgsafe",
				BasePath:              basePath,
				InsecureIgnoreHostKey: true,
			},
		}
		wOpts.WorkerWritesDirectly = true

		credBytes, err = (creds.Credential{
			Type: creds.TypeSFTPKey,
			SFTPKey: &creds.SFTPKeyCredential{
				Host:                  "127.0.0.1",
				Port:                  remotePort,
				Username:              "pgsafe",
				PrivateKeyPEM:         sftpd.ClientPrivateKeyPEM(),
				BasePath:              basePath,
				InsecureIgnoreHostKey: true,
			},
		}).Marshal()
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: marshal sftp tunnel creds: %w", err)
		}
	case wOpts.WorkerWritesDirectly && isCloudStorageType(wOpts.Storage.Type) &&
		wantsCloudProxy(wOpts.StorageReach, wOpts.SSHTarget):
		// Cloud-via-caller: cloud SDKs (S3 / Azure / GCS) all honour
		// HTTPS_PROXY, so we don't need a TCP-level rewrite — just
		// stand up a dynamic SOCKS5 listener on the worker side via
		// ssh -R <port> (no destination = OpenSSH dynamic forward),
		// and tell the worker to set HTTPS_PROXY=socks5h://... before
		// opening the SDK. Bytes flow worker → caller → cloud, where
		// the SDK's own TLS terminates at the cloud endpoint (the
		// proxy carries opaque CONNECTed traffic).
		socksPort, err := sftptunnel.PickRemotePort()
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: pick socks port: %w", err)
		}
		fwdArgs := []string{
			"-o", "ExitOnForwardFailure=yes",
			"-R", fmt.Sprintf("%d", socksPort),
		}
		wOpts.SSHExtraArgs = append(append([]string{}, fwdArgs...), wOpts.SSHExtraArgs...)
		workerHTTPSProxy = fmt.Sprintf("socks5h://127.0.0.1:%d", socksPort)
		logf("cloud-proxy: caller offering dynamic SOCKS5 on worker:%d (HTTPS_PROXY=%s)",
			socksPort, workerHTTPSProxy)

		cred, err := mintForBackend(ctx, wOpts.Storage)
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: mint cloud-proxy creds: %w", err)
		}
		credBytes, err = cred.Marshal()
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: marshal cloud-proxy creds: %w", err)
		}
	case wOpts.WorkerWritesDirectly && wOpts.Storage.Type == "sftp" &&
		wantsSFTPProxy(wOpts.StorageReach, wOpts.SSHTarget):
		// SFTP-via-caller: the worker can't (or shouldn't) reach the
		// SFTP storage server directly. The caller adds an ssh -R
		// reverse port-forward into the worker's session; the worker
		// dials 127.0.0.1:remotePort and the bytes are tunneled back
		// through the caller, which opens a TCP connection to the
		// real SFTP server. The SSH session inside the SFTP backend
		// is end-to-end between worker and storage server, so HostKey
		// pinning still applies.
		if wOpts.Storage.SFTP == nil {
			_, _ = br.Stop(ctx)
			return Result{}, errors.New("backup: storage_reach=via_caller for sftp requires storage.sftp config")
		}
		storagePort := wOpts.Storage.SFTP.Port
		if storagePort == 0 {
			storagePort = 22
		}
		remotePort, err := sftptunnel.PickRemotePort()
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: pick remote port: %w", err)
		}
		fwdArgs := []string{
			"-o", "ExitOnForwardFailure=yes",
			"-R", fmt.Sprintf("%d:%s:%d", remotePort, wOpts.Storage.SFTP.Host, storagePort),
		}
		wOpts.SSHExtraArgs = append(append([]string{}, fwdArgs...), wOpts.SSHExtraArgs...)
		logf("sftp-proxy: caller forwarding worker:%d → %s:%d via ssh -R",
			remotePort, wOpts.Storage.SFTP.Host, storagePort)

		// Rewrite the SFTP config so the worker dials loopback. HostKey,
		// PrivateKeyFile, Username, BasePath are preserved — the SSH
		// session inside SFTP is end-to-end.
		proxiedSFTP := *wOpts.Storage.SFTP
		proxiedSFTP.Host = "127.0.0.1"
		proxiedSFTP.Port = remotePort
		wOpts.Storage = config.StorageConfig{Type: "sftp", SFTP: &proxiedSFTP}

		cred, err := mintForBackend(ctx, wOpts.Storage)
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: mint sftp-proxy creds: %w", err)
		}
		credBytes, err = cred.Marshal()
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: marshal sftp-proxy creds: %w", err)
		}
	case wOpts.WorkerWritesDirectly:
		// Mint scoped credentials per Tenet 3 (cloud backends). POSIX
		// gets a no-op TypeNone credential; the worker sees
		// StorageType=posix and uses its local mount perms.
		cred, err := mintForBackend(ctx, wOpts.Storage)
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: mint creds: %w", err)
		}
		credBytes, err = cred.Marshal()
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: marshal creds: %w", err)
		}
	}

	// Dial the worker. SSHTarget non-empty → cross-host SSH transport;
	// SSHTarget empty → same-host local subprocess. Same JSON-RPC
	// channel either way.
	sess, err := dialWorker(ctx, wOpts)
	if err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: dialWorker: %w", err)
	}
	defer func() { _ = sess.Close() }()

	// Drain stderr in the background so the worker doesn't block on a
	// full pipe, and so any worker-side error message is visible.
	go func() {
		buf := make([]byte, 4096)
		stderr := sess.StderrReader()
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				_, _ = fmt.Fprintf(stderrFor(opts), "pgsafe worker stderr: %s", buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()

	conn := &sessionConn{sess: sess}
	cli := rpc.NewClient(conn)
	defer func() { _ = cli.Close() }()

	// Hello — confirms protocol-version match.
	hello, err := cli.Hello(rpc.HelloRequest{
		CallerVersion:   opts.Label, // "pgsafe-<server>"
		ProtocolVersion: rpc.Version,
	})
	if err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: rpc.Hello: %w", err)
	}
	logf("worker hello: version=%s os=%s pgdata=%s numcpu=%d",
		hello.WorkerVersion, hello.OS, hello.PGDataPath, hello.NumCPU)

	// Reachability probe — ask the worker if it can open the storage
	// backend the caller is about to ship via Configure. The
	// probe is meaningless when WorkerWritesDirectly=false:
	// caller-storage mode means the worker has NO backend role
	// (it streams encrypted bytes back via RPC; the caller
	// writes them). Calling ProbeStorage in that case ships an empty
	// credential to the worker, which fails to unmarshal and surfaces
	// as a misleading "UNREACHABLE: unexpected end of JSON input"
	// log line that operators can't act on.
	if wOpts.WorkerWritesDirectly {
		probe, err := cli.ProbeStorage(rpc.ProbeStorageRequest{
			StorageType: wOpts.Storage.Type,
			StoragePath: wOpts.Storage.Path,
			Credentials: credBytes,
		})
		if err != nil {
			_, _ = br.Stop(ctx)
			return Result{}, fmt.Errorf("backup: rpc.ProbeStorage: %w", err)
		}
		if probe.Reachable {
			logf("worker→storage probe: REACHABLE (%dms)", probe.DurationMS)
		} else {
			logf("worker→storage probe: UNREACHABLE (%dms): %s", probe.DurationMS, probe.Error)
			if wOpts.StorageReach == "native_only" {
				_, _ = br.Stop(ctx)
				return Result{}, fmt.Errorf("backup: storage_reach=native_only and worker→storage UNREACHABLE: %s", probe.Error)
			}
		}
	}

	// Configure — ships scoped creds + recipients + file list to the worker.
	rpcFiles := make([]rpc.FileSpec, 0, len(files))
	for _, f := range files {
		rpcFiles = append(rpcFiles, rpc.FileSpec{Path: f.Path, Size: f.Size})
	}
	if _, err := cli.Configure(rpc.ConfigureRequest{
		BackupID:             backupID,
		StorageType:          wOpts.Storage.Type,
		StoragePath:          wOpts.Storage.Path,
		PGDataPath:           pgDataPath,
		Credentials:          credBytes,
		AgeRecipients:        opts.Recipients,
		CompressionCodec:     codecFromString(opts.Compression),
		CompressionLevel:     levelFromString(opts.Compression),
		PageChecksumMode:     int(wOpts.PageChecksumMode),
		Files:                rpcFiles,
		WorkerWritesDirectly: wOpts.WorkerWritesDirectly,
		HTTPSProxy:           workerHTTPSProxy,
	}); err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, fmt.Errorf("backup: rpc.Configure: %w", err)
	}

	// Manifest builder (caller side; populated from per-StreamFile responses).
	mb := manifest.NewBuilder(manifest.BackupStartInfo{
		SystemIdentifier: id.SystemIdentifier,
		Timeline:         startInfo.Timeline,
		StartLSN:         startInfo.LSN,
		StartTime:        startedAt.UTC(),
	})

	// Drive StreamFile in parallel over the single JSON-RPC connection.
	// net/rpc.Client multiplexes concurrent calls by sequence number;
	// the worker dispatches one goroutine per incoming call. errgroup
	// caps in-flight calls at `workers` so we don't queue the entire
	// $PGDATA file list at once. mb.AddFile and the counters are
	// guarded by mbMu — the manifest builder is documented as not
	// concurrency-safe.
	var (
		mbMu       sync.Mutex
		fileCount  int
		bytesTotal int64
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, f := range files {
		f := f
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			// Resume reuse-skip: if cleanResumable validated the
			// prior bytes for this path, do NOT call StreamFile
			// (worker reads no $PGDATA bytes, no upload). Inherit
			// the prior plaintext + repo SHAs straight into the
			// new manifest. Caller-side decision; worker stays
			// dumb-and-obedient.
			if resumePlan != nil {
				if reused, ok := resumePlan.files[f.Path]; ok {
					mbMu.Lock()
					mb.AddFile(reused.Path, reused.Size, reused.SHA256, reused.ModTime)
					mb.SetLatestRepoChecksum(reused.RepoSize, reused.RepoSHA256)
					fileCount++
					bytesTotal += reused.Size
					mbMu.Unlock()
					return nil
				}
			}
			if wOpts.WorkerWritesDirectly {
				sfResp, err := cli.StreamFile(rpc.StreamFileRequest{Path: f.Path})
				if err != nil {
					return fmt.Errorf("backup: StreamFile %s: %w", f.Path, err)
				}
				mbMu.Lock()
				mb.AddFile(sfResp.Path, sfResp.Bytes, sfResp.SHA256, sfResp.ModTime)
				fileCount++
				bytesTotal += sfResp.Bytes
				mbMu.Unlock()
				return nil
			}
			// Caller-storage path: worker reads + filters, returns
			// the encrypted bytes; caller writes them to its own
			// local backend.
			scResp, err := cli.StreamChunk(rpc.StreamChunkRequest{Path: f.Path})
			if err != nil {
				return fmt.Errorf("backup: StreamChunk %s: %w", f.Path, err)
			}
			repoPath := filepath.ToSlash(filepath.Join(backupID, f.Path))
			wc, err := opts.Backend.Put(gctx, repoPath)
			if err != nil {
				return fmt.Errorf("backup: orch.Put %s: %w", repoPath, err)
			}
			if _, err := wc.Write(scResp.Body); err != nil {
				_ = wc.Close()
				return fmt.Errorf("backup: orch.Write %s: %w", repoPath, err)
			}
			if err := wc.Close(); err != nil {
				return fmt.Errorf("backup: orch.Close %s: %w", repoPath, err)
			}
			mbMu.Lock()
			mb.AddFile(scResp.Path, scResp.Bytes, scResp.SHA256, scResp.ModTime)
			fileCount++
			bytesTotal += scResp.Bytes
			mbMu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		_, _ = br.Stop(ctx)
		return Result{}, err
	}

	// (cli.Done deliberately moved past the WriteBlob+Commit phase
	// below — it tears down the worker's backend client, which we
	// still need for the manifest/sidecar/backup_label writes when
	// those route through the worker.)

	// Bracket.Stop — returns stop LSN + backup_label + tablespace_map. The
	// caller writes those two through the caller-side filter
	// chain (the worker doesn't have backup_label — it comes from
	// pg_backup_stop, which we just called).
	stopInfo, err := br.Stop(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("backup: bracket.Stop: %w", err)
	}
	logf("bracket.Stop: lsn=%s", stopInfo.LSN)

	// backup_label / tablespace_map come from pg_backup_stop (in-memory
	// blobs the worker doesn't have access to). Two write paths:
	//   - WorkerWritesDirectly: ship via WriteBlob(Filtered=true); worker
	//     runs filter chain and writes via its backend.
	//   - else: caller runs its OWN filter chain and writes to
	//     opts.Backend.
	labelPath := filepath.ToSlash(filepath.Join(backupID, "backup_label"))
	labelBytes, labelSHA, err := writeBlobByMode(ctx, wOpts.WorkerWritesDirectly, cli, opts.Backend, opts.Filter, labelPath, stopInfo.LabelFile, true)
	if err != nil {
		return Result{}, fmt.Errorf("backup: backup_label: %w", err)
	}
	mb.AddFile("backup_label", labelBytes, labelSHA, time.Now().UTC())
	if len(stopInfo.SpcMapFile) > 0 {
		mapPath := filepath.ToSlash(filepath.Join(backupID, "tablespace_map"))
		mapBytes, mapSHA, err := writeBlobByMode(ctx, wOpts.WorkerWritesDirectly, cli, opts.Backend, opts.Filter, mapPath, stopInfo.SpcMapFile, true)
		if err != nil {
			return Result{}, fmt.Errorf("backup: tablespace_map: %w", err)
		}
		mb.AddFile("tablespace_map", mapBytes, mapSHA, time.Now().UTC())
	}

	// WAL-wait — same logic as remote-parallel.
	logf("acquiring bracket WAL via source=%q", opts.WALSource)
	segments, err := AcquireBracketWAL(ctx, AcquireOptions{
		Backend:   opts.Backend,
		Timeline:  startInfo.Timeline,
		StartLSN:  startInfo.LSN,
		StopLSN:   stopInfo.LSN,
		SegSize:   id.WALSegmentSize,
		Timeout:   opts.WALTimeout,
		WALSource: opts.WALSource,
	})
	if err != nil {
		return Result{}, fmt.Errorf("backup: %w", err)
	}

	// WALSourceWalgrab: ship the bracket segments through the worker
	// AFTER pg_backup_stop. Their names depend on stop_lsn so they're
	// not in Configure's file list — the worker side allows them
	// through by shape (pg_wal/<archivable>). Each goes through the
	// same StreamFile/StreamChunk pipeline as data files; the result
	// lands at <storage>/<backup-id>/pg_wal/<seg>, exactly where
	// restore looks first. No archive plumbing involved.
	if opts.WALSource == WALSourceWalgrab {
		for _, seg := range segments {
			rel := "pg_wal/" + seg
			if wOpts.WorkerWritesDirectly {
				sfResp, err := cli.StreamFile(rpc.StreamFileRequest{Path: rel})
				if err != nil {
					return Result{}, fmt.Errorf("backup: walgrab StreamFile %s: %w", rel, err)
				}
				mb.AddFile(sfResp.Path, sfResp.Bytes, sfResp.SHA256, sfResp.ModTime)
				continue
			}
			scResp, err := cli.StreamChunk(rpc.StreamChunkRequest{Path: rel})
			if err != nil {
				return Result{}, fmt.Errorf("backup: walgrab StreamChunk %s: %w", rel, err)
			}
			repoPath := filepath.ToSlash(filepath.Join(backupID, rel))
			wc, err := opts.Backend.Put(ctx, repoPath)
			if err != nil {
				return Result{}, fmt.Errorf("backup: walgrab Put %s: %w", repoPath, err)
			}
			if _, err := wc.Write(scResp.Body); err != nil {
				_ = wc.Close()
				return Result{}, fmt.Errorf("backup: walgrab write %s: %w", repoPath, err)
			}
			if err := wc.Close(); err != nil {
				return Result{}, fmt.Errorf("backup: walgrab close %s: %w", repoPath, err)
			}
			mb.AddFile(scResp.Path, scResp.Bytes, scResp.SHA256, scResp.ModTime)
		}
	}

	var walRecords []manifest.WALSegmentRecord
	if walRecordsNeeded(opts.WALSource) {
		walRecords, err = hashWALSegments(ctx, opts.Backend, startInfo.Timeline, segments)
		if err != nil {
			return Result{}, fmt.Errorf("backup: hash WAL: %w", err)
		}
	}

	// Manifest + sidecar commit — also routed through the worker. Plaintext
	// (Filtered=false) because the manifest is structural metadata, not
	// user data.
	manifestBytes, err := mb.Finalize(manifest.BackupStopInfo{
		StopLSN:  stopInfo.LSN,
		StopTime: opts.Now().UTC(),
	})
	if err != nil {
		return Result{}, fmt.Errorf("backup: finalize manifest: %w", err)
	}
	manifestRel := filepath.ToSlash(filepath.Join(backupID, "backup_manifest"))
	tmpRel := manifestRel + ".tmp"
	if _, _, err := writeBlobByMode(ctx, wOpts.WorkerWritesDirectly, cli, opts.Backend, opts.Filter, tmpRel, manifestBytes, false); err != nil {
		return Result{}, fmt.Errorf("backup: manifest.tmp: %w", err)
	}
	sc := manifest.Sidecar{
		Version:              manifest.SidecarVersion,
		Server:               opts.Server,
		EncryptionRecipients: opts.Recipients,
		Compression:          opts.Compression,
		StorageLayoutVersion: 1,
		WALSegments:          walRecords,
		Directories:          pgDataDirs,
		SystemIdentifier:     id.SystemIdentifier,
	}
	scBytes, err := manifest.MarshalSidecar(sc)
	if err != nil {
		return Result{}, fmt.Errorf("backup: marshal sidecar: %w", err)
	}
	scPath := filepath.ToSlash(filepath.Join(backupID, "Storage-Metadata.json"))
	if _, _, err := writeBlobByMode(ctx, wOpts.WorkerWritesDirectly, cli, opts.Backend, opts.Filter, scPath, scBytes, false); err != nil {
		return Result{}, fmt.Errorf("backup: sidecar: %w", err)
	}
	if wOpts.WorkerWritesDirectly {
		if err := cli.Commit(tmpRel, manifestRel); err != nil {
			return Result{}, fmt.Errorf("backup: rpc.Commit manifest: %w", err)
		}
	} else {
		if err := opts.Backend.Commit(ctx, tmpRel, manifestRel); err != nil {
			return Result{}, fmt.Errorf("backup: orch.Commit manifest: %w", err)
		}
	}

	// Done — worker tears down its backend client. Called after all
	// writes (data files, backup_label, tablespace_map, manifest,
	// sidecar) have completed and the manifest has been atomically
	// committed.
	if _, err := cli.Done(); err != nil {
		return Result{}, fmt.Errorf("backup: rpc.Done: %w", err)
	}

	return Result{
		BackupID: backupID,
		StartLSN: startInfo.LSN,
		StopLSN:  stopInfo.LSN,
		Timeline: startInfo.Timeline,
		Files:    fileCount,
		Bytes:    bytesTotal,
		Duration: time.Since(startedAt),
	}, nil
}

// writeBlobByMode writes an in-memory blob to the storage, picking the right
// path for the storage mode:
//
//   - workerWritesDirectly=true: ship via cli.WriteBlob; the worker holds
//     the backend and runs its own filter chain when filtered=true.
//   - workerWritesDirectly=false: write to backend.Put on the caller,
//     optionally piping through chain (for filtered=true). The worker has
//     no backend in this mode.
//
// Returns the on-the-wire byte count and the plaintext SHA-256 (the value
// the manifest expects).
func writeBlobByMode(
	ctx context.Context,
	workerWritesDirectly bool,
	cli *rpc.Client,
	backend storage.Backend,
	chain *filter.Chain,
	repoPath string,
	body []byte,
	filtered bool,
) (int64, [32]byte, error) {
	if workerWritesDirectly {
		resp, err := cli.WriteBlob(rpc.WriteBlobRequest{
			RepoPath: repoPath,
			Body:     body,
			Filtered: filtered,
		})
		if err != nil {
			return 0, [32]byte{}, err
		}
		return resp.Bytes, resp.SHA256, nil
	}
	wc, err := backend.Put(ctx, repoPath)
	if err != nil {
		return 0, [32]byte{}, fmt.Errorf("orch.Put: %w", err)
	}
	if filtered {
		chainW, res, err := chain.Wrap(wc)
		if err != nil {
			_ = wc.Close()
			return 0, [32]byte{}, fmt.Errorf("filter.Wrap: %w", err)
		}
		if _, err := chainW.Write(body); err != nil {
			_ = chainW.Close()
			return 0, [32]byte{}, fmt.Errorf("orch.filtered.Write: %w", err)
		}
		if err := chainW.Close(); err != nil {
			return 0, [32]byte{}, fmt.Errorf("orch.filtered.Close: %w", err)
		}
		return res.Bytes, res.SHA256, nil
	}
	// Plaintext path (manifest, sidecar). Hash + size measured here since
	// no filter chain is on the path.
	h := sha256.New()
	mw := io.MultiWriter(wc, h)
	n, err := mw.Write(body)
	if err != nil {
		_ = wc.Close()
		return 0, [32]byte{}, fmt.Errorf("orch.plain.Write: %w", err)
	}
	if err := wc.Close(); err != nil {
		return 0, [32]byte{}, fmt.Errorf("orch.plain.Close: %w", err)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return int64(n), sum, nil
}
