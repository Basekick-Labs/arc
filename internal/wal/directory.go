package wal

import (
	"errors"
	"io/fs"
	"os"
	"strings"
)

// DirectoryBytes reports the bytes occupied by every file in the WAL directory
// except the reserve file (ReserveFileName, #676).
//
// Every file but the reserve, not a suffix list. The names that are not `*.wal` — `.wal.recovery`
// attempt sidecars, `.wal-recovery-*` temporaries leaked by a crash, legacy
// `.wal.recovered` files — are exactly what accumulates when recovery is
// failing, which is the state this gauge's capacity alert exists for. A suffix
// list also has to be revisited every time the WAL grows a new artifact name,
// and was not.
//
// It therefore measures DIRECTORY occupancy. On a volume dedicated to the WAL,
// which is how `wal.directory` is meant to be pointed, that is the figure an
// operator wants; aimed at a shared path it will include whatever else is there.
//
// The reserve file is left out: it is a fixed 64 MiB held for a start on a
// full volume, not WAL growth, and wal.max_directory_mb is enforced on this
// figure — counting the reserve would spend the cap on the space set aside for
// the case the cap exists to prevent.
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

// capDirectoryBytes is what wal.max_directory_mb is enforced on (#676): the
// directory's bytes except the reserve file and quarantined `*.wal*.failed`
// files. Quarantined files are kept for an operator and nothing deletes them,
// so counting them could hold the gate on for good.
func capDirectoryBytes(walDir string) (int64, error) {
	entries, err := os.ReadDir(walDir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	kept := entries[:0]
	for _, e := range entries {
		if !isQuarantinedName(e.Name()) {
			kept = append(kept, e)
		}
	}
	return sumRegularFiles(kept)
}

// isQuarantinedName matches a file recovery quarantined: `*.wal*.failed`.
func isQuarantinedName(name string) bool {
	return strings.HasSuffix(name, ".failed") && strings.Contains(name, walFileSuffix)
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
		if info.Mode().IsRegular() && !isReserveName(info.Name()) {
			total += info.Size()
		}
	}
	return total, nil
}
