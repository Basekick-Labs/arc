//go:build !(linux || darwin || freebsd)

package wal

import "os"

// allocatedBytes: this platform does not report allocated blocks.
func allocatedBytes(os.FileInfo) (int64, bool) { return 0, false }
