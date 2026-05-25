//go:build integration

package pgtest_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestStartPGAcrossVersions probes every supported PG version in parallel,
// asserting the container starts, the backup user works, and the version
// reported by the server matches the request. This is the Cycle-0
// fixture-validation gate.
func TestStartPGAcrossVersions(t *testing.T) {
	for _, v := range pgtest.SupportedVersions {
		v := v
		t.Run(v2name(v), func(t *testing.T) {
			t.Parallel()
			pg := pgtest.StartPG(t, v)
			if pg.Version != v {
				t.Errorf("Version field = %d, want %d", pg.Version, v)
			}

			ctx := context.Background()
			conn, err := pgx.Connect(ctx, pg.SuperDSN)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }()

			var serverVersion string
			if err := conn.QueryRow(ctx, `SELECT current_setting('server_version_num')`).Scan(&serverVersion); err != nil {
				t.Fatalf("server_version_num: %v", err)
			}
			// server_version_num for PG 13.x is 13xxxx; for PG 18.x is 18xxxx.
			// The first two digits encode the major version.
			if len(serverVersion) < 2 {
				t.Fatalf("unexpected server_version_num: %q", serverVersion)
			}
			majorPrefix := serverVersion[:2]
			expected := v2name(v)
			if majorPrefix != expected {
				t.Errorf("server reports major %q, want %q (full = %s)", majorPrefix, expected, serverVersion)
			}
		})
	}
}

func v2name(v int) string {
	if v < 10 {
		return "0" + string(rune('0'+v))
	}
	return string([]byte{'0' + byte(v/10), '0' + byte(v%10)})
}
