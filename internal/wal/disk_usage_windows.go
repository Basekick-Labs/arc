//go:build windows

package wal

import "golang.org/x/sys/windows"

func filesystemUsage(path string) (totalBytes, availableBytes uint64, err error) {
	directory, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var totalFreeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(directory, &availableBytes, &totalBytes, &totalFreeBytes); err != nil {
		return 0, 0, err
	}
	return totalBytes, availableBytes, nil
}
