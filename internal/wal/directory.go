package wal

import (
	"errors"
	"io/fs"
	"os"
)

// DirectoryBytes reports the bytes occupied by every file in the WAL directory.
//
// Every file, not a suffix list. The names that are not `*.wal` — `.wal.recovery`
// attempt sidecars, `.wal-recovery-*` temporaries leaked by a crash, legacy
// `.wal.recovered` files — are exactly what accumulates when recovery is
// failing, which is the state this gauge's capacity alert exists for. A suffix
// list also has to be revisited every time the WAL grows a new artifact name,
// and was not.
//
// It therefore measures DIRECTORY occupancy. On a volume dedicated to the WAL,
// which is how `wal.directory` is meant to be pointed, that is the figure an
// operator wants; aimed at a shared path it will include whatever else is there.
func DirectoryBytes(walDir string) (int64, error) {
	entries, err := os.ReadDir(walDir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return sumRegularFiles(entries)
}

// sumRegularFiles is split out so the vanished-entry case can be driven with a
// fake fs.DirEntry: it is a race against live writers that a real directory
// cannot stage deterministically.
func sumRegularFiles(entries []os.DirEntry) (int64, error) {
	var total int64
	for _, entry := range entries {
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			// The walk races every writer in this directory: a sidecar write
			// temporary's own deferred remove, the recovery-artifact sweep, and
			// the purges. While this counted only `*.wal` and `*.failed` the
			// suffix filter ran FIRST and hid the race; now it does not.
			// Failing the sample would leave the gauge STALE, which is worse
			// than a figure a few hundred bytes light — and it is the gauge the
			// capacity alert reads.
			continue
		}
		if err != nil {
			return 0, err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
	}
	return total, nil
}
