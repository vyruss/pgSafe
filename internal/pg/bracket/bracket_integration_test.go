//go:build integration

package bracket_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vyruss/pgsafe/internal/pg/bracket"
	"github.com/vyruss/pgsafe/internal/pg/conn"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
)

// TestBracketStartStopAcrossVersions exercises the full bracket cycle on
// every supported PG version. The bracket package needs to work
// against both the modern (PG 15+) and legacy (PG 13/14) SQL APIs.
func TestBracketStartStopAcrossVersions(t *testing.T) {
	for _, v := range pgtest.SupportedVersions {
		v := v
		t.Run(versionName(v), func(t *testing.T) {
			t.Parallel()
			pg := pgtest.StartPG(t, v)
			ctx := context.Background()

			pool, err := conn.Connect(ctx, pg.SuperDSN)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer pool.Close()

			b, err := bracket.New(pool, v)
			if err != nil {
				t.Fatalf("bracket.New: %v", err)
			}

			start, err := b.Start(ctx, "pgsafe-bracket-test", true)
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if start.LSN == 0 {
				t.Errorf("Start LSN is zero")
			}
			if start.Timeline == 0 {
				t.Errorf("Start Timeline is zero")
			}

			stop, err := b.Stop(ctx)
			if err != nil {
				t.Fatalf("Stop: %v", err)
			}
			if stop.LSN == 0 {
				t.Errorf("Stop LSN is zero")
			}
			if stop.LSN < start.LSN {
				t.Errorf("Stop LSN %s precedes Start LSN %s", stop.LSN, start.LSN)
			}
			// Label file should contain "START WAL LOCATION".
			if !strings.Contains(string(stop.LabelFile), "START WAL LOCATION") {
				t.Errorf("LabelFile missing START WAL LOCATION; got %q",
					stop.LabelFile)
			}
		})
	}
}

func TestBracketRejectsUnsupportedVersion(t *testing.T) {
	t.Parallel()
	if _, err := bracket.New(nil, 12); err == nil {
		t.Fatal("New(nil, 12): want error")
	}
	if _, err := bracket.New(nil, 19); err == nil {
		t.Fatal("New(nil, 19): want error")
	}
}

func versionName(v int) string {
	if v < 10 {
		return "0" + string(rune('0'+v))
	}
	return string([]byte{'0' + byte(v/10), '0' + byte(v%10)})
}
