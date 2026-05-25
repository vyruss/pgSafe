package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/vyruss/pgsafe/internal/config"
	"github.com/vyruss/pgsafe/internal/transport/creds"
	"github.com/vyruss/pgsafe/internal/transport/rpc"
)

// mintForBackend dispatches to the right creds.MintXxx for the storage
// backend. POSIX returns TypeNone (no scoped credential needed; the
// worker's filesystem mount governs).
//
// S3 needs PGSAFE_S3_ROLE_ARN env var; GCS needs
// PGSAFE_GCS_TARGET_SA env var. will promote these to YAML.
func mintForBackend(ctx context.Context, sc config.StorageConfig) (creds.Credential, error) {
	switch sc.Type {
	case "posix":
		return creds.Credential{Type: creds.TypeNone}, nil
	case "s3":
		roleARN := os.Getenv("PGSAFE_S3_ROLE_ARN")
		if roleARN == "" {
			return creds.Credential{}, errors.New("PGSAFE_S3_ROLE_ARN env var required for pgsafe-worker + s3")
		}
		return creds.MintS3STS(ctx, sc.S3, roleARN, creds.DefaultLifetime)
	case "azure":
		return creds.MintAzureSAS(ctx, sc.Azure, creds.DefaultLifetime)
	case "gcs":
		targetSA := os.Getenv("PGSAFE_GCS_TARGET_SA")
		if targetSA == "" {
			return creds.Credential{}, errors.New("PGSAFE_GCS_TARGET_SA env var required for pgsafe-worker + gcs")
		}
		return creds.MintGCSToken(ctx, sc.GCS, targetSA, creds.DefaultLifetime)
	case "sftp":
		return creds.MintSFTPKey(sc.SFTP)
	default:
		return creds.Credential{}, fmt.Errorf("pgsafe-worker: unsupported storage type %q", sc.Type)
	}
}

// confirmProxyFallback prompts on prompt for a y/N answer and returns
// whether the operator agreed to caller-proxy fallback. EOF (cron /
// systemd, no terminal) reads as "no" — operators who set
// --confirm-proxy on a non-interactive run almost certainly meant to
// abort rather than silently fall back, so EOF mirrors the explicit-no
// behavior. Comparison is case-insensitive on the leading character.
func confirmProxyFallback(in io.Reader, prompt io.Writer) (bool, error) {
	if _, err := fmt.Fprint(prompt,
		"pgsafe: worker→storage UNREACHABLE; fall back to caller-proxy? [y/N]: "); err != nil {
		return false, fmt.Errorf("write prompt: %w", err)
	}
	buf := make([]byte, 1)
	n, err := in.Read(buf)
	if err == io.EOF {
		_, _ = fmt.Fprintln(prompt)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read answer: %w", err)
	}
	if n < 1 {
		return false, nil
	}
	c := buf[0]
	if c == 'y' || c == 'Y' {
		return true, nil
	}
	return false, nil
}

// preProbeStorage dials a short-lived worker session, runs Hello +
// ProbeStorage with the original (non-tunneled) credentials, and
// reports whether the worker can reach storage natively. Used by the
// auto-fallback path before bracket.Start so a UNREACHABLE result can
// flip storage_reach to via_caller without an open backup bracket.
//
// The session is torn down before return — the main backup pipeline
// dials its own session afterwards (with possibly-rewritten ssh args
// for the proxy case).
func preProbeStorage(
	ctx context.Context,
	wOpts WorkerOptions,
	label string,
	logf func(string, ...any),
) (bool, error) {
	cred, err := mintForBackend(ctx, wOpts.Storage)
	if err != nil {
		return false, fmt.Errorf("mint: %w", err)
	}
	credBytes, err := cred.Marshal()
	if err != nil {
		return false, fmt.Errorf("marshal: %w", err)
	}
	sess, err := dialWorker(ctx, wOpts)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		_, _ = io.Copy(io.Discard, sess.StderrReader())
	}()
	cleanup := func() {
		_ = sess.Close()
		<-stderrDone
	}
	conn := &sessionConn{sess: sess}
	cli := rpc.NewClient(conn)
	if _, err := cli.Hello(rpc.HelloRequest{
		CallerVersion:   label,
		ProtocolVersion: rpc.Version,
	}); err != nil {
		_ = cli.Close()
		cleanup()
		return false, fmt.Errorf("rpc.Hello: %w", err)
	}
	resp, probeErr := cli.ProbeStorage(rpc.ProbeStorageRequest{
		StorageType: wOpts.Storage.Type,
		StoragePath: wOpts.Storage.Path,
		Credentials: credBytes,
	})
	_ = cli.Close()
	cleanup()
	if probeErr != nil {
		return false, fmt.Errorf("ProbeStorage: %w", probeErr)
	}
	if resp.Reachable {
		logf("worker→storage pre-probe: REACHABLE (%dms)", resp.DurationMS)
	} else {
		logf("worker→storage pre-probe: UNREACHABLE (%dms): %s", resp.DurationMS, resp.Error)
	}
	return resp.Reachable, nil
}

// wantsSFTPProxy reports whether the operator's storage_reach setting
// should engage the SFTP-via-caller proxy (ssh -R). Same-host transport
// has no SSH session to attach a reverse forward to, so via_caller is a
// no-op there and we fall back to direct reach (the worker shares the
// caller's network anyway, since it's a local subprocess).
func wantsSFTPProxy(reach, sshTarget string) bool {
	if sshTarget == "" {
		return false
	}
	return reach == "via_caller"
}

// wantsCloudProxy mirrors wantsSFTPProxy for cloud backends. Same
// same-host short-circuit (no ssh session = no tunnel possible).
func wantsCloudProxy(reach, sshTarget string) bool {
	if sshTarget == "" {
		return false
	}
	return reach == "via_caller"
}

// isCloudStorageType reports whether a StorageConfig.Type names one of
// the cloud backends that supports HTTPS_PROXY-based tunneling. POSIX
// uses filesystem perms (no network call); SFTP has its own ssh -R
// proxy path (wantsSFTPProxy + the dispatch in runWorkerBackup).
func isCloudStorageType(t string) bool {
	switch t {
	case "s3", "azure", "gcs":
		return true
	}
	return false
}

// describeStorage formats a config.StorageConfig for the topology
// log. Returns "<type>://<location>" — concise enough for one log
// line, descriptive enough that operators can spot a misconfigured
// backend at a glance.
func describeStorage(sc config.StorageConfig) string {
	switch sc.Type {
	case "posix":
		return "posix://" + sc.Path
	case "s3":
		if sc.S3 != nil {
			return "s3://" + sc.S3.Bucket
		}
	case "azure":
		if sc.Azure != nil {
			return "azure://" + sc.Azure.AccountName + "/" + sc.Azure.Container
		}
	case "gcs":
		if sc.GCS != nil {
			return "gs://" + sc.GCS.Bucket
		}
	case "sftp":
		if sc.SFTP != nil {
			return fmt.Sprintf("sftp://%s@%s%s", sc.SFTP.Username, sc.SFTP.Host, sc.SFTP.BasePath)
		}
	}
	return sc.Type
}
