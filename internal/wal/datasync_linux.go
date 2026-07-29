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
func dataSync(f *os.File) error {
	// Repeat on EINTR: a signal can interrupt fdatasync before it completes,
	// and returning early would report durability that was not achieved.
	for {
		err := syscall.Fdatasync(int(f.Fd()))
		if err != syscall.EINTR {
			return err
		}
	}
}
