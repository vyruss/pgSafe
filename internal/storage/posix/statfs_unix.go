//go:build unix

package posix

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// statfsAvailBytes returns the bytes available to a non-root user on
// the filesystem containing path. Uses statfs(2)'s f_bavail field
// (NOT f_bfree, which counts root-reserved blocks). Caller-side
// pre-flight check; the real backup-time write errors are still
// authoritative.
func statfsAvailBytes(path string) (uint64, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return 0, fmt.Errorf("posix: statfs %s: %w", path, err)
	}
	//nolint:gosec // Bsize is platform-defined; converting to uint64 is the canonical way to multiply.
	return s.Bavail * uint64(s.Bsize), nil
}
