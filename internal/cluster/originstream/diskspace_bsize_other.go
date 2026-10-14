//go:build darwin || freebsd

package originstream

import "golang.org/x/sys/unix"

func blockSize(st *unix.Statfs_t) int64 { return int64(st.Bsize) }
