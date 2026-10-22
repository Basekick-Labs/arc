//go:build !(linux || darwin || freebsd || windows)

package diskspace

import "errors"

// Space is not available on this platform. Callers fail open: the WAL disk
// pressure gate does not refuse on free space (#676), the standby store cap
// falls back to 1 GiB and its reserve is not checked (#1266).
func Space(string) (total, avail uint64, err error) {
	return 0, 0, errors.New("filesystem free space is not available on this platform")
}
