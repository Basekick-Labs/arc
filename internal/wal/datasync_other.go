//go:build !linux

package wal

import "os"

// dataSyncSupported reports whether dataSync performs a real fdatasync(2)
// rather than falling back to a full fsync.
const dataSyncSupported = false

// dataSync falls back to a full fsync on platforms where Go does not expose
// fdatasync(2) — notably darwin and windows, where syscall has no Fdatasync.
//
// The fallback is correct but slower: it flushes metadata the caller did not
// ask for. Operators selecting the fdatasync sync mode on these platforms get
// fsync semantics, which the WAL logs once at startup so the choice is not
// silently misrepresented.
//
// On darwin, Go's Sync() issues fcntl(F_FULLFSYNC), which also flushes the
// drive's own write cache, so both sync modes there are a full cache flush:
// stronger than fsync(2), and slower, which is where running syncs beside the
// writer (#1246) helps most.
func dataSync(f *os.File) error {
	return f.Sync()
}
