package pgtest

// pg_verifybackup integration for the test matrix and invariant
// suite. Every backup test asserts the manifest pgSafe stores would be
// accepted by PG's own verifier — free third-party validation we get for
// using the PG-native backup_manifest format.
//
//  is the spec. The wrapper runs
// the verifier inside the same per-PG-version container that produced
// the data, so we don't need 6 PG-client packages on the host.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Exec runs cmd inside the PG container and returns (exit_code, combined_output, error).
// error is non-nil only if the exec mechanism itself failed (container gone,
// docker daemon unreachable); a non-zero exit code is returned via the int
// alongside a nil error so callers can decide whether the failure is a test
// fail or expected.
func (p *PG) Exec(ctx context.Context, cmd ...string) (int, []byte, error) {
	if p.container == nil {
		return -1, nil, errors.New("pgtest.Exec: container handle nil (already terminated?)")
	}
	// Capture combined stdout+stderr so failure messages survive into test logs.
	code, reader, err := p.container.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return -1, nil, fmt.Errorf("pgtest.Exec %v: %w", cmd, err)
	}
	var buf bytes.Buffer
	if reader != nil {
		_, _ = io.Copy(&buf, reader)
	}
	return code, buf.Bytes(), nil
}

// PgVerifybackup runs `pg_verifybackup` against backupDir inside the PG
// container's filesystem. backupDir must be a path the container can read
// — typically a bind-mounted host directory passed via testcontainers'
// Binds modifier when the container is started, or a temporary path
// inside the container created by an earlier Exec call.
//
// We pass --no-parse-wal because the matrix tests verify the manifest's
// file checksums but don't necessarily ship the matching WAL into the
// directory. WAL replayability is verified separately by the round-trip's
// post-restore `pg_amcheck`.
//
// Returns nil iff pg_verifybackup exits 0. On non-zero exit, the captured
// output is included in the error so the test log shows what mismatched.
func (p *PG) PgVerifybackup(ctx context.Context, backupDir string, extraArgs ...string) error {
	args := append([]string{"pg_verifybackup", "--no-parse-wal", backupDir}, extraArgs...)
	code, out, err := p.Exec(ctx, args...)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("pg_verifybackup exited %d:\n%s", code, out)
	}
	return nil
}
