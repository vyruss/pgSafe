package main

import (
	"testing"

	"github.com/vyruss/pgsafe/internal/config"
)

func TestServerLockPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		cfg    config.StorageConfig
		server string
		want   string
	}{
		{
			name:   "posix lives next to the storage",
			cfg:    config.StorageConfig{Type: "posix", Path: "/var/lib/pgsafe/storage"},
			server: "prod-db",
			want:   "/var/lib/pgsafe/storage/.pgsafe-server-prod-db.lock",
		},
		{
			name:   "s3 falls back to /tmp",
			cfg:    config.StorageConfig{Type: "s3"},
			server: "prod-db",
			want:   "/tmp/pgsafe-server-prod-db.lock",
		},
		{
			name:   "azure falls back to /tmp",
			cfg:    config.StorageConfig{Type: "azure"},
			server: "demo",
			want:   "/tmp/pgsafe-server-demo.lock",
		},
		{
			name:   "gcs falls back to /tmp",
			cfg:    config.StorageConfig{Type: "gcs"},
			server: "demo",
			want:   "/tmp/pgsafe-server-demo.lock",
		},
		{
			name:   "sftp falls back to /tmp",
			cfg:    config.StorageConfig{Type: "sftp"},
			server: "demo",
			want:   "/tmp/pgsafe-server-demo.lock",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := serverLockPath(tc.cfg, tc.server); got != tc.want {
				t.Errorf("serverLockPath(%v, %q) = %q, want %q", tc.cfg, tc.server, got, tc.want)
			}
		})
	}
}
