//go:build integration

package readbinary_test

import (
	"context"
	"io"
	"testing"

	"github.com/vyruss/pgsafe/internal/pg/conn"
	"github.com/vyruss/pgsafe/internal/pg/pgtest"
	"github.com/vyruss/pgsafe/internal/pg/readbinary"
)

func TestReadBinaryRoundTrip(t *testing.T) {
	t.Parallel()
	pg := pgtest.StartPG18(t)
	ctx := context.Background()

	pool, err := conn.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	// PG_VERSION is the simplest cluster file: 3 bytes ("18\n").
	r := readbinary.NewReader(pool, "PG_VERSION", 3, 0)
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "18\n" {
		t.Errorf("PG_VERSION content = %q, want %q", got, "18\n")
	}
}

func TestReadBinaryChunkedLargeFile(t *testing.T) {
	t.Parallel()
	pg := pgtest.StartPG18(t)
	ctx := context.Background()

	pool, err := conn.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	// global/pg_control is exactly 8192 bytes — perfect for the chunked
	// reader with a small chunkSize.
	r := readbinary.NewReader(pool, "global/pg_control", 8192, 1024)
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 8192 {
		t.Errorf("pg_control size = %d, want 8192", len(got))
	}
}

func TestListPGData(t *testing.T) {
	t.Parallel()
	pg := pgtest.StartPG18(t)
	ctx := context.Background()

	pool, err := conn.Connect(ctx, pg.SuperDSN)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pool.Close()

	files, err := readbinary.ListPGData(ctx, pool)
	if err != nil {
		t.Fatalf("ListPGData: %v", err)
	}
	if len(files) < 50 {
		t.Errorf("expected at least 50 files in $PGDATA, got %d", len(files))
	}

	var sawPGVersion, sawPGControl bool
	for _, f := range files {
		if f.Path == "PG_VERSION" {
			sawPGVersion = true
		}
		if f.Path == "global/pg_control" {
			sawPGControl = true
		}
		// pg_wal must be excluded.
		if len(f.Path) >= 6 && f.Path[:6] == "pg_wal" {
			t.Errorf("pg_wal entry leaked into ListPGData: %s", f.Path)
		}
	}
	if !sawPGVersion {
		t.Errorf("PG_VERSION missing from ListPGData")
	}
	if !sawPGControl {
		t.Errorf("global/pg_control missing from ListPGData")
	}
}
