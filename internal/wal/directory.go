package wal

import (
	"errors"
	"io/fs"
	"os"
	"strings"
)

// DirectoryBytes reports the bytes occupied by WAL and quarantined WAL files.
// Quarantined files still consume disk and remain available for operator
// recovery, so they are included in the gauge.
func DirectoryBytes(walDir string) (int64, error) {
	entries, err := os.ReadDir(walDir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	var total int64
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".wal") && !strings.HasSuffix(name, ".failed") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
	}
	return total, nil
}
