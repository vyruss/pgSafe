package worker

import "testing"

// TestIsPostStopWALPath pins the WALSourceWalgrab path-shape carve-out:
// StreamFile/StreamChunk reject paths that aren't in Configure's file
// list EXCEPT for pg_wal/<archivable>, which the caller computes
// post-stop_lsn and ships through the same RPC. If this regresses, the
// walgrab loop in pgsafe_worker.go gets "path not in file list"
// errors at runtime — silent for the operator until a backup fails.
func TestIsPostStopWALPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		// Bracket segment shapes that walgrab actually ships.
		{"pg_wal/000000010000000000000003", true},
		{"pg_wal/0000000100000B38000000F7", true},
		{"pg_wal/0000000100000B38000000F7.partial", true},
		{"pg_wal/0000000100000B38000000E1.00000028.backup", true},
		{"pg_wal/00000001.history", true},

		// Reject everything else loudly. These are the categories the
		// carve-out MUST NOT widen into:
		{"", false},                           // empty
		{"pg_wal/", false},                    // missing segname
		{"pg_wal/../etc/passwd", false},       // traversal — defense
		{"../pg_wal/00000001.history", false}, // ..-anywhere — defense
		{"PG_VERSION", false},                 // data file — must be in file list
		{"base/16384/2613", false},            // heap file — must be in file list
		{"pg_wal/notarealsegment", false},     // doesn't match archive shape
		{"pg_wal/000000010000000000000003.bogus", false},
	}
	for _, c := range cases {
		got := isPostStopWALPath(c.name)
		if got != c.want {
			t.Errorf("isPostStopWALPath(%q) = %v; want %v", c.name, got, c.want)
		}
	}
}
