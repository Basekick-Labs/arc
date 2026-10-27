//go:build linux || darwin || freebsd

package wal

import (
	"os"
	"syscall"
)

// allocatedBytes is the space a file's blocks occupy (st_blocks is in
// 512-byte units on every platform here).
func allocatedBytes(info os.FileInfo) (int64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int64(st.Blocks) * 512, true
}
