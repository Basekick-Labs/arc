//go:build windows

package diskspace

import "golang.org/x/sys/windows"

// Space returns the total size of the volume holding path and the bytes
// available to this process's user there (quotas included) (#676).
func Space(path string) (total, avail uint64, err error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var free, size, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(p, &free, &size, &totalFree); err != nil {
		return 0, 0, err
	}
	return size, free, nil
}
