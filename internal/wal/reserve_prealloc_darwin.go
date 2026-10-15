//go:build darwin

package wal

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocate allocates size bytes of blocks for f and sets its size.
// F_PREALLOCATE allocates without moving the end of file, so the truncate
// sets the size over the allocated blocks.
func preallocate(f *os.File, size int64) error {
	st := unix.Fstore_t{Flags: unix.F_ALLOCATEALL, Posmode: unix.F_PEOFPOSMODE, Offset: 0, Length: size}
	if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &st); err != nil {
		return err
	}
	return f.Truncate(size)
}
