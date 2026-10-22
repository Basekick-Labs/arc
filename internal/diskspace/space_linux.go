//go:build linux

package diskspace

import "golang.org/x/sys/unix"

// statfsBlockSize is the unit of Blocks and Bavail: the fragment size on
// Linux, which can differ from the preferred I/O size in Bsize.
func statfsBlockSize(st *unix.Statfs_t) int64 { return st.Frsize }
