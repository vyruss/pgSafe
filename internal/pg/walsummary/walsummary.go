// Package walsummary wraps PG 17+ WAL-summarizer SQL surface used by
// incremental-backup mode.
//
//		pg_get_wal_summarizer_state()    — current LSN frontier the summarizer has
//		                                    digested. We poll this so we never trust
//		                                    a summary range whose RHS is past the
//		                                    frontier.
//		pg_available_wal_summaries()     — (timeline, start_lsn, end_lsn) ranges for
//		                                    which on-disk summaries exist; gap
//		                                    detection between the parent backup's
//		                                    stop LSN and "now".
//		pg_wal_summary_contents(t, s, e) — per-(relfilenode, fork, blockno) change
//		                                    records covering [s, e].
//
//	 Version dispatch via New(pool, ver):
//
// New errors with ErrUnsupported on PG <17.
package walsummary

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/manifest"
)

// ErrUnsupported is returned by New when the cluster predates PG 17 (when the
// WAL summarizer was introduced).
var ErrUnsupported = errors.New("walsummary: PG <17 has no WAL summarizer")

// SummarizerState is the output of pg_get_wal_summarizer_state.
type SummarizerState struct {
	// SummarizedTLI is the timeline ID the summarizer is currently digesting.
	SummarizedTLI uint32
	// SummarizedLSN is the high-water mark up to which the summarizer has
	// committed summaries to disk. Anything <=SummarizedLSN is safe to query
	// via Contents; anything > may not yet be on disk.
	SummarizedLSN manifest.LSN
}

// SummaryRange is one row from pg_available_wal_summaries.
type SummaryRange struct {
	Timeline uint32
	StartLSN manifest.LSN
	EndLSN   manifest.LSN
}

// ChangedBlock describes one (relfilenode, fork, blockno) tuple that changed
// in the requested LSN window. ForkNumber follows PG's fork numbering:
//
//	0 = main, 1 = fsm, 2 = vm, 3 = init.
type ChangedBlock struct {
	RelFileNode uint64
	ForkNumber  uint8
	BlockNo     uint32
}

// Summarizer is the abstract handle. Production builds via New(pool, ver);
// tests use a hand-written mock satisfying this interface.
type Summarizer interface {
	State(ctx context.Context) (SummarizerState, error)
	Available(ctx context.Context) ([]SummaryRange, error)
	Contents(ctx context.Context, timeline uint32, start, end manifest.LSN) ([]ChangedBlock, error)
}

// New returns a Summarizer for the given PG major version. Returns
// ErrUnsupported for PG <17.
func New(pool *pgxpool.Pool, pgVersion int) (Summarizer, error) {
	// Version check runs before pool validation so callers that probe support
	// (without yet connecting) get ErrUnsupported instead of a generic error.
	if pgVersion < 17 {
		return nil, ErrUnsupported
	}
	if pool == nil {
		return nil, errors.New("walsummary: pool is required")
	}
	return &summarizer{pool: pool}, nil
}

type summarizer struct {
	pool *pgxpool.Pool
}

func (s *summarizer) State(ctx context.Context) (SummarizerState, error) {
	var (
		tli     uint32
		lsnText string
	)
	row := s.pool.QueryRow(ctx,
		`SELECT summarized_tli, summarized_lsn::text FROM pg_get_wal_summarizer_state()`)
	if err := row.Scan(&tli, &lsnText); err != nil {
		return SummarizerState{}, fmt.Errorf("walsummary: pg_get_wal_summarizer_state: %w", err)
	}
	lsn, err := manifest.ParseLSN(lsnText)
	if err != nil {
		return SummarizerState{}, fmt.Errorf("walsummary: parse summarized_lsn %q: %w", lsnText, err)
	}
	return SummarizerState{SummarizedTLI: tli, SummarizedLSN: lsn}, nil
}

func (s *summarizer) Available(ctx context.Context) ([]SummaryRange, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT tli, start_lsn::text, end_lsn::text FROM pg_available_wal_summaries() ORDER BY tli, start_lsn`)
	if err != nil {
		return nil, fmt.Errorf("walsummary: pg_available_wal_summaries: %w", err)
	}
	defer rows.Close()
	var out []SummaryRange
	for rows.Next() {
		var (
			tli       uint32
			startText string
			endText   string
		)
		if err := rows.Scan(&tli, &startText, &endText); err != nil {
			return nil, fmt.Errorf("walsummary: scan: %w", err)
		}
		startLSN, err := manifest.ParseLSN(startText)
		if err != nil {
			return nil, fmt.Errorf("walsummary: parse start %q: %w", startText, err)
		}
		endLSN, err := manifest.ParseLSN(endText)
		if err != nil {
			return nil, fmt.Errorf("walsummary: parse end %q: %w", endText, err)
		}
		out = append(out, SummaryRange{Timeline: tli, StartLSN: startLSN, EndLSN: endLSN})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walsummary: rows.Err: %w", err)
	}
	return out, nil
}

// Contents queries pg_wal_summary_contents for the requested timeline-bounded
// LSN window. The PG function is variadic-by-position, so the caller pre-locks
// the window via Available() to avoid feeding it ranges that don't have
// matching summary files.
func (s *summarizer) Contents(ctx context.Context, timeline uint32, start, end manifest.LSN) ([]ChangedBlock, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT relfilenode, forknum, blocknum
		   FROM pg_wal_summary_contents($1, $2::pg_lsn, $3::pg_lsn)
		   ORDER BY relfilenode, forknum, blocknum`,
		timeline, start.String(), end.String())
	if err != nil {
		return nil, fmt.Errorf("walsummary: pg_wal_summary_contents: %w", err)
	}
	defer rows.Close()
	var out []ChangedBlock
	for rows.Next() {
		var (
			rel  uint64
			fork int16
			blk  int64
		)
		if err := rows.Scan(&rel, &fork, &blk); err != nil {
			return nil, fmt.Errorf("walsummary: scan: %w", err)
		}
		if fork < 0 || fork > 3 {
			return nil, fmt.Errorf("walsummary: unexpected forknum %d", fork)
		}
		if blk < 0 || blk > int64(^uint32(0)) {
			return nil, fmt.Errorf("walsummary: blocknum %d out of uint32 range", blk)
		}
		out = append(out, ChangedBlock{
			RelFileNode: rel,
			ForkNumber:  uint8(fork), //nolint:gosec // bounds-checked above
			BlockNo:     uint32(blk), //nolint:gosec // bounds-checked above
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walsummary: rows.Err: %w", err)
	}
	return out, nil
}
