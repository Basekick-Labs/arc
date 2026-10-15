//go:build darwin || freebsd

package diskspace

import "golang.org/x/sys/unix"

// statfsBlockSize is the unit of Blocks and Bavail: the fundamental block
// size on darwin and FreeBSD.
func statfsBlockSize(st *unix.Statfs_t) int64 { return int64(st.Bsize) }
