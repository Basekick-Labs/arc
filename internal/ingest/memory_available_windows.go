//go:build windows

package ingest

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

func platformAvailableMemoryBytes() (uint64, bool) {
	var status windows.MemoryStatusEx
	status.Length = uint32(unsafe.Sizeof(status))
	if err := windows.GlobalMemoryStatusEx(&status); err != nil {
		return 0, false
	}
	return status.AvailPhys, true
}
