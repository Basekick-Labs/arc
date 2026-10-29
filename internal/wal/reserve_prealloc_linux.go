//go:build linux

package wal

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate allocates size bytes of blocks for f and sets its size.
func preallocate(f *os.File, size int64) error {
	return unix.Fallocate(int(f.Fd()), 0, 0, size)
}
