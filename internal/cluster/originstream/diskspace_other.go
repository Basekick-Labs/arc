//go:build !(linux || darwin || freebsd)

package originstream

import "errors"

// DiskSpace is not available on this platform: the auto cap falls back to
// 1 GiB and the free-space reserve is not checked (#1266).
func DiskSpace(string) (total, avail uint64, err error) {
	return 0, 0, errors.New("filesystem size not available on this platform")
}
