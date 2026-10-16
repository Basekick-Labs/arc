//go:build linux

package wal

import (
	"os"
	"syscall"
)

// dataSyncSupported reports whether dataSync performs a real fdatasync(2)
// rather than falling back to a full fsync.
const dataSyncSupported = true

// dataSync flushes file data without waiting for a metadata-only journal
// commit, using fdatasync(2).
//
// Note this is not free of metadata work: the WAL file grows on every append,
// and fdatasync must still persist a changed file size before returning. The
// saving is the inode's other metadata (timestamps), so the win over fsync is
// real but modest — larger on rotational and network-backed storage than on
// NVMe.
//
// It runs through SyscallConn().Control, not f.Fd(), because the WAL syncs on
// its own goroutine while the writer rotates (#1246). Fd() hands back the raw
// descriptor number without holding the file open, so a Close on the writer
// goroutine could release the number and a later open (the next WAL file, a
// Parquet temp file) could reuse it, and fdatasync would then flush the wrong
// file and report success. Control holds a reference for the duration of the
// call, and on a file already closed it returns os.ErrClosed instead.
func dataSync(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	if err := rc.Control(func(fd uintptr) {
		// Repeat on EINTR: a signal can interrupt fdatasync before it
		// completes, and returning early would report durability that was not
		// achieved.
		for {
			syncErr = syscall.Fdatasync(int(fd))
			if syncErr != syscall.EINTR {
				return
			}
		}
	}); err != nil {
		return err
	}
	return syncErr
}
