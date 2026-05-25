package bracket

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vyruss/pgsafe/internal/manifest"
)

// bracketModern targets PG 15+. The exclusive form was removed in PG 15;
// `pg_backup_start(label, fast)` and `pg_backup_stop()` are the only
// SQL-level brackets remaining.
//
// The non-exclusive bracket is session-scoped: the same backend that ran
// pg_backup_start MUST run pg_backup_stop, otherwise PG errors with
// "non-exclusive backup is not in progress". We hold a dedicated *pgxpool.Conn
// from Start through Stop and release it only after Stop.
type bracketModern struct {
	pool *pgxpool.Pool
	conn *pgxpool.Conn
}

func (b *bracketModern) Start(ctx context.Context, label string, fast bool) (StartInfo, error) {
	if b.conn != nil {
		return StartInfo{}, errors.New("bracket: Start already called")
	}
	c, err := b.pool.Acquire(ctx)
	if err != nil {
		return StartInfo{}, fmt.Errorf("bracket: acquire: %w", err)
	}
	var (
		lsnText  string
		timeline uint32
	)
	row := c.QueryRow(ctx,
		`SELECT pg_backup_start($1, $2)::text, timeline_id
		 FROM pg_control_checkpoint()`,
		label, fast)
	if err := row.Scan(&lsnText, &timeline); err != nil {
		c.Release()
		return StartInfo{}, fmt.Errorf("bracket: pg_backup_start: %w", err)
	}
	lsn, err := manifest.ParseLSN(lsnText)
	if err != nil {
		c.Release()
		return StartInfo{}, fmt.Errorf("bracket: parse start LSN %q: %w", lsnText, err)
	}
	b.conn = c
	return StartInfo{LSN: lsn, Timeline: timeline}, nil
}

func (b *bracketModern) Stop(ctx context.Context) (StopInfo, error) {
	if b.conn == nil {
		return StopInfo{}, errors.New("bracket: Stop without Start")
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
	row := b.conn.QueryRow(ctx, `SELECT lsn::text, labelfile, spcmapfile FROM pg_backup_stop()`)
	if err := row.Scan(&lsnText, &labelText, &spcMapText); err != nil {
		return StopInfo{}, fmt.Errorf("bracket: pg_backup_stop: %w", err)
	}
	lsn, err := manifest.ParseLSN(lsnText)
	if err != nil {
		return StopInfo{}, fmt.Errorf("bracket: parse stop LSN %q: %w", lsnText, err)
	}
	return StopInfo{
		LSN:        lsn,
		LabelFile:  []byte(labelText),
		SpcMapFile: []byte(spcMapText),
	}, nil
}
