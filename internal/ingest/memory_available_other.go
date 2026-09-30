//go:build !windows

package ingest

func platformAvailableMemoryBytes() (uint64, bool) {
	return 0, false
}
