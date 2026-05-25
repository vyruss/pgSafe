package bracket

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/manifest"
)

// bracketLegacy targets PG 13/14. The non-exclusive form is
// `pg_start_backup(label, fast, exclusive=false)` and the corresponding
// `pg_stop_backup(exclusive=false)`; the exclusive form was removed in
// PG 15. PG 14 EOLs Nov 2026; this branch is short-lived.
//
// Same session-scoping rule as bracketModern: the backend running
// pg_start_backup must also run pg_stop_backup, so we hold a dedicated
// *pgxpool.Conn between Start and Stop.
type bracketLegacy struct {
	pool *pgxpool.Pool
	conn *pgxpool.Conn
}

func (b *bracketLegacy) Start(ctx context.Context, label string, fast bool) (StartInfo, error) {
	if b.conn != nil {
		return StartInfo{}, errors.New("bracket(legacy): Start already called")
	}
	c, err := b.pool.Acquire(ctx)
	if err != nil {
		return StartInfo{}, fmt.Errorf("bracket(legacy): acquire: %w", err)
	}
	var (
		lsnText  string
		timeline uint32
	)
	row := c.QueryRow(ctx,
		`SELECT pg_start_backup($1, $2, false)::text, timeline_id
		 FROM pg_control_checkpoint()`,
		label, fast)
	if err := row.Scan(&lsnText, &timeline); err != nil {
		c.Release()
		return StartInfo{}, fmt.Errorf("bracket(legacy): pg_start_backup: %w", err)
	}
	lsn, err := manifest.ParseLSN(lsnText)
	if err != nil {
		c.Release()
		return StartInfo{}, fmt.Errorf("bracket(legacy): parse start LSN %q: %w", lsnText, err)
	}
	b.conn = c
	return StartInfo{LSN: lsn, Timeline: timeline}, nil
}

func (b *bracketLegacy) Stop(ctx context.Context) (StopInfo, error) {
	if b.conn == nil {
		return StopInfo{}, errors.New("bracket(legacy): Stop without Start")
	}
	defer func() {
		b.conn.Release()
		b.conn = nil
	}()
	var (
		lsnText    string
		labelText  string
		spcMapText string
	)
	row := b.conn.QueryRow(ctx,
		`SELECT lsn::text, labelfile, spcmapfile FROM pg_stop_backup(false)`)
	if err := row.Scan(&lsnText, &labelText, &spcMapText); err != nil {
		return StopInfo{}, fmt.Errorf("bracket(legacy): pg_stop_backup: %w", err)
	}
	lsn, err := manifest.ParseLSN(lsnText)
	if err != nil {
		return StopInfo{}, fmt.Errorf("bracket(legacy): parse stop LSN %q: %w", lsnText, err)
	}
	return StopInfo{
		LSN:        lsn,
		LabelFile:  []byte(labelText),
		SpcMapFile: []byte(spcMapText),
	}, nil
}
