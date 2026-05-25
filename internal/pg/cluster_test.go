package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vyruss/pgsafe/internal/pg"
	"github.com/vyruss/pgsafe/internal/pg/basebackup"
	"github.com/vyruss/pgsafe/internal/pg/identity"
)

// mockCluster is the hand-written mock that internal/backup tests will use.
// Its presence here verifies that pg.Cluster is implementable by code
// outside the pg package.
type mockCluster struct {
	identity   identity.Identity
	identityFn func() (identity.Identity, error)
	closed     bool
}

func (m *mockCluster) Identity(_ context.Context) (identity.Identity, error) {
	if m.identityFn != nil {
		return m.identityFn()
	}
	return m.identity, nil
}

func (m *mockCluster) BaseBackup(_ context.Context, _ basebackup.Options) (pg.BaseBackupStream, error) {
	return nil, errors.New("mockCluster: BaseBackup not implemented in this mock")
}

func (m *mockCluster) Close() { m.closed = true }

func TestMockClusterSatisfiesInterface(t *testing.T) {
	t.Parallel()
	var _ pg.Cluster = (*mockCluster)(nil)
}

func TestMockClusterIdentity(t *testing.T) {
	t.Parallel()
	want := identity.Identity{SystemIdentifier: 42, Timeline: 1}
	mc := &mockCluster{identity: want}
	got, err := mc.Identity(context.Background())
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}
	if got != want {
		t.Errorf("Identity = %+v, want %+v", got, want)
	}
}

func TestMockClusterClose(t *testing.T) {
	t.Parallel()
	mc := &mockCluster{}
	mc.Close()
	if !mc.closed {
		t.Errorf("Close did not set closed=true")
	}
}
