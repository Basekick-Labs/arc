//go:build unix

package wal

import "golang.org/x/sys/unix"

func filesystemUsage(path string) (totalBytes, availableBytes uint64, err error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	blockSize := uint64(stat.Bsize)
	return uint64(stat.Blocks) * blockSize, uint64(stat.Bavail) * blockSize, nil
}
