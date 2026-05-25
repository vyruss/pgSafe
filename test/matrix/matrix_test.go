//go:build matrix

// Package matrix_test runs per-PG-version axis batteries plus
// two interaction batteries on PG 18, structured as one TestMatrix
// function with t.Run subtests for every cell.
//
//	"Matrix structure" is the spec.
//
// Local CI: PGSAFE_MATRIX_PG is unset; all six PG versions run
// concurrently in one process via t.Parallel on the outer pg=N subtest.
// Future GHA: matrix job per PG version sets PGSAFE_MATRIX_PG=N; the
// same TestMatrix runs but only tests that one version.
//
// connects to the pooled PG and runs SELECT 1. fills the bodies
// with backup → restore → pg_amcheck round-trips.
package matrix_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestMain owns the pool's lifetime — pooled containers live across all
// subtests and are torn down once at the end of the run.
func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.CleanupPool()
	os.Exit(code)
}

func TestMatrix(t *testing.T) {
	for _, pgv := range pgVersionsFromEnv(t) {
		pgv := pgv
		t.Run(fmt.Sprintf("pg=%d", pgv), func(t *testing.T) {
			t.Parallel()
			pg := pgtest.StartPGPooled(t, pgv)

			t.Run("battery=backend", func(t *testing.T) {
				for _, v := range []string{"posix", "s3", "azure", "gcs", "sftp"} {
					v := v
					t.Run("value="+v, func(t *testing.T) {
						t.Parallel()
						stubCell(t, pg, "backend", v)
					})
				}
			})

			t.Run("battery=codec", func(t *testing.T) {
				for _, v := range []string{"gzip", "lz4", "zstd", "bzip2"} {
					v := v
					t.Run("value="+v, func(t *testing.T) {
						t.Parallel()
						stubCell(t, pg, "codec", v)
					})
				}
			})

			t.Run("battery=encrypt", func(t *testing.T) {
				for _, v := range []string{"age", "none"} {
					v := v
					t.Run("value="+v, func(t *testing.T) {
						t.Parallel()
						stubCell(t, pg, "encrypt", v)
					})
				}
			})

			t.Run("battery=mode", func(t *testing.T) {
				for _, v := range []string{"simple", "remote-parallel", "hybrid-parallel"} {
					v := v
					t.Run("value="+v, func(t *testing.T) {
						t.Parallel()
						if v == "hybrid-parallel" && pg.Version < 17 {
							t.Skip("hybrid-parallel uses incremental backups (PG 17+ only)")
						}
						stubCell(t, pg, "mode", v)
					})
				}
			})

			// Interaction batteries — PG 18 only. The Tenet-3 cred plumbing
			// and Invariant #9 worker-key code is PG-version-agnostic, so
			// running these on every PG version would just duplicate CPU.
			if pgv != 18 {
				return
			}

			t.Run("interaction=backend×mode", func(t *testing.T) {
				for _, b := range []string{"posix", "s3", "azure", "gcs", "sftp"} {
					for _, m := range []string{"simple", "remote-parallel", "hybrid-parallel"} {
						b, m := b, m
						t.Run(fmt.Sprintf("value=%s,%s", b, m), func(t *testing.T) {
							t.Parallel()
							stubCell(t, pg, "backend×mode", b+","+m)
						})
					}
				}
			})

			t.Run("interaction=encrypt×mode", func(t *testing.T) {
				for _, e := range []string{"age", "none"} {
					for _, m := range []string{"simple", "remote-parallel", "hybrid-parallel"} {
						e, m := e, m
						t.Run(fmt.Sprintf("value=%s,%s", e, m), func(t *testing.T) {
							t.Parallel()
							stubCell(t, pg, "encrypt×mode", e+","+m)
						})
					}
				}
			})
		})
	}
}

// stubCell is the Cycle-0 placeholder: connect to the per-cell database
// and prove the cluster is alive. replaces this with the actual
// backup → restore → pg_amcheck round-trip parameterised by axis/value.
func stubCell(t *testing.T, pg *pgtest.PG, axis, value string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pg.DSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if one != 1 {
		t.Errorf("SELECT 1 returned %d, want 1", one)
	}
	t.Logf("stub cell PG %d %s=%s OK", pg.Version, axis, value)
}

// pgVersionsFromEnv returns the slice of PG versions TestMatrix should
// iterate. The contract:
//
//   - unset (the default for local development): [18]. Running 6 PG
//     containers + cloud emulators on a dev laptop is slow and burns
//     resources for little signal during iteration.
//   - PGSAFE_MATRIX_PG=N: just that version. Used by future GHA matrix
//     jobs (each runner picks one version) and by developers who want
//     to spot-check an older PG against a current change.
//   - PGSAFE_MATRIX_PG=all: every supported version. The explicit
//     opt-in for "I really want to fan out across all six locally."
func pgVersionsFromEnv(t *testing.T) []int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("PGSAFE_MATRIX_PG"))
	switch raw {
	case "":
		return []int{18}
	case "all":
		return pgtest.SupportedVersions
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf(`PGSAFE_MATRIX_PG=%q: want an integer in %v or "all"`, raw, pgtest.SupportedVersions)
	}
	supported := false
	for _, s := range pgtest.SupportedVersions {
		if s == v {
			supported = true
			break
		}
	}
	if !supported {
		t.Fatalf("PGSAFE_MATRIX_PG=%d: not in supported set %v", v, pgtest.SupportedVersions)
	}
	return []int{v}
}
