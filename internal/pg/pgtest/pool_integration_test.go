//go:build integration

package pgtest_test

import (
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestPoolReusesContainer is the Cycle-0 DoD anchor: the second
// StartPGPooled(version) call reuses the existing container rather
// than spinning a fresh one. We assert this structurally: both calls
// return DSNs whose host:port portion is identical (same container,
// just a different database in it).
//
// This test owns the pool's lifetime — TestMain calls CleanupPool at
// the end so other integration tests in the same package see a clean
// slate.
func TestPoolReusesContainer(t *testing.T) {
	defer pgtest.CleanupPool()

	pg1 := pgtest.StartPGPooled(t, 18)
	pg2 := pgtest.StartPGPooled(t, 18)

	host1 := hostPort(pg1.SuperDSN)
	host2 := hostPort(pg2.SuperDSN)
	if host1 != host2 {
		t.Errorf("second StartPGPooled spun up a fresh container: host1=%q host2=%q", host1, host2)
	}

	if pg1.DSN == pg2.DSN {
		t.Errorf("expected fresh DB per call, got identical DSNs: %q", pg1.DSN)
	}
}

// hostPort extracts the host:port segment from a postgres:// URL DSN.
// We don't need the rest for this assertion.
func hostPort(dsn string) string {
	// postgres://user:pw@host:port/db?query
	at := strings.Index(dsn, "@")
	if at < 0 {
		return dsn
	}
	rest := dsn[at+1:]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return rest
	}
	return rest[:slash]
}
