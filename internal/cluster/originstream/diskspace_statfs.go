//go:build linux || darwin || freebsd

package originstream

import "golang.org/x/sys/unix"

// DiskSpace returns the total size of the filesystem holding path and the
// bytes available to an unprivileged writer there (#1266).
func DiskSpace(path string) (total, avail uint64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bs := uint64(blockSize(&st))
	return uint64(st.Blocks) * bs, uint64(st.Bavail) * bs, nil
}
