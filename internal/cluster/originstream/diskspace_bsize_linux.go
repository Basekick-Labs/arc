//go:build linux

package originstream

import "golang.org/x/sys/unix"

func blockSize(st *unix.Statfs_t) int64 { return st.Frsize }
