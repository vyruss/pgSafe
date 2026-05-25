//go:build integration

package walsummary_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vyruss/pgsafe/internal/pg/conn"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
	"github.com/vyruss/pgsafe/internal/pg/walsummary"
)

// TestSummarizerStateOnPG18 confirms pg_get_wal_summarizer_state returns
// non-trivial values once summarize_wal=on (set by pgtest for PG 17+).
func TestSummarizerStateOnPG18(t *testing.T) {
	t.Parallel()
	pg := pgtest.StartPG18(t)
	ctx := context.Background()

	pool, err := conn.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	s, err := walsummary.New(pool, 18)
	if err != nil {
		t.Fatalf("walsummary.New: %v", err)
	}

	// Drive a few transactions so the summarizer has something to digest,
	// then poll until SummarizedLSN advances past zero.
	if _, err := pool.Exec(ctx, `CREATE TABLE summ_demo (id int)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO summ_demo SELECT generate_series(1, 100)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_switch_wal()`); err != nil {
		t.Fatalf("pg_switch_wal: %v", err)
	}

	// Poll for up to 30s. The summarizer ticks every few seconds; the
	// `pg_switch_wal` above gives it a sealed segment to digest.
	deadline := time.Now().Add(30 * time.Second)
	var st walsummary.SummarizerState
	for time.Now().Before(deadline) {
		st, err = s.State(ctx)
		if err != nil {
			t.Fatalf("State: %v", err)
		}
		if st.SummarizedLSN > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if st.SummarizedTLI == 0 {
		t.Errorf("SummarizedTLI is zero — summarizer never ran")
	}
	if st.SummarizedLSN == 0 {
		t.Errorf("SummarizedLSN is zero after 30s — summarizer never advanced")
	}
}

// TestNewRejectsPG13ByVersion checks the version dispatch.
// (We don't need to start a real PG 13 cluster — the version arg is the gate.)
func TestNewRejectsPG13ByVersion(t *testing.T) {
	t.Parallel()
	pg := pgtest.StartPG18(t)
	ctx := context.Background()
	pool, err := conn.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	_, err = walsummary.New(pool, 13)
	if !errors.Is(err, walsummary.ErrUnsupported) {
		t.Errorf("New(_, 13): want ErrUnsupported, got %v", err)
	}
}
