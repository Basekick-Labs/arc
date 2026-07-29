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
// On darwin, note that Sync() maps to fsync(2), which flushes to the drive but
// does not force the drive's own write cache — F_FULLFSYNC would be required
// for that. The durability ladder is softer there regardless of sync mode.
func dataSync(f *os.File) error {
	return f.Sync()
}
