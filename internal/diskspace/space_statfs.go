//go:build linux || darwin || freebsd

package diskspace

import "golang.org/x/sys/unix"

// Space returns the total size of the filesystem holding path and the
// bytes available there to an unprivileged writer (#676, #1266). Arc runs as a
// non-root user in its image, so Bavail is what it can write; for root it is
// conservative (the root-reserved blocks are not counted).
func Space(path string) (total, avail uint64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bs := uint64(statfsBlockSize(&st))
	return uint64(st.Blocks) * bs, uint64(st.Bavail) * bs, nil
}
