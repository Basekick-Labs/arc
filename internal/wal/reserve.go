package wal

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"

	"github.com/basekick-labs/arc/internal/diskspace"
	"github.com/rs/zerolog"
)

// The WAL reserve file (#676): space held on the WAL volume so a node whose
// volume filled up can still start. A full volume made the first WAL header
// write fail with ENOSPC, and the pod crash-looped; releasing the reserve at
// boot gives the start, and recovery, room to free space.
const (
	// ReserveFileName is the reserve's name in the WAL directory. It must
	// never end in ".wal": every WAL listing, the purge and recovery match
	// that suffix, and the reserve is not a WAL file.
	ReserveFileName = ".arc-wal-reserve"
	// ReserveFileSize is how much the reserve holds.
	ReserveFileSize = int64(64) << 20
	// ReserveBootMinFree: a start that finds less free space than this on the
	// WAL volume removes the reserve before anything writes there.
	ReserveBootMinFree = uint64(32) << 20
	// reserveTempSuffix names the reserve while it is being allocated.
	reserveTempSuffix = ".tmp"
)

// ReserveFilePath is the reserve file's path in walDir.
func ReserveFilePath(walDir string) string {
	return filepath.Join(walDir, ReserveFileName)
}

// isReserveName reports a name the reserve file uses, final or temporary.
func isReserveName(name string) bool {
	return name == ReserveFileName || name == ReserveFileName+reserveTempSuffix
}

// diskSpaceFn is diskspace.Space; a variable so tests can fake a full volume.
var diskSpaceFn = diskspace.Space

// PrepareReserveAtBoot runs before anything writes to the WAL volume at
// start (#676). It keeps an existing reserve file, and removes it when the
// volume has less than ReserveBootMinFree free, logging an Error: the start
// then has the reserve's room for the WAL header, the registration journal
// and SQLite. It never creates the reserve (the disk pressure monitor does,
// once there is room) and never refuses a start: a statfs error or a failed
// remove is logged and the start goes on. It reports whether it released.
func PrepareReserveAtBoot(walDir string, logger zerolog.Logger) bool {
	if err := os.MkdirAll(walDir, 0700); err != nil {
		logger.Warn().Err(err).Str("dir", walDir).Msg("Cannot create the WAL directory to check its volume's free space at start")
		return false
	}
	// A temporary left by a crash mid-allocation holds space for nothing.
	_ = os.Remove(ReserveFilePath(walDir) + reserveTempSuffix)
	path := ReserveFilePath(walDir)
	if _, err := os.Stat(path); err != nil {
		return false
	}
	_, avail, err := diskSpaceFn(walDir)
	if err != nil {
		logger.Warn().Err(err).Str("dir", walDir).Msg("Cannot read the WAL volume's free space at start; the WAL reserve file is kept")
		return false
	}
	if avail >= ReserveBootMinFree {
		return false
	}
	if err := os.Remove(path); err != nil {
		logger.Error().Err(err).Str("file", path).Uint64("free_bytes", avail).
			Msg("The WAL volume is full and its reserve file could not be removed to make room for the start")
		return false
	}
	logger.Error().Str("file", path).Uint64("free_bytes", avail).Int64("released_bytes", ReserveFileSize).
		Msg("The WAL volume is full: released the WAL reserve file so Arc can start. Writes are refused (503) until free space returns; free space on the volume. The reserve is recreated once there is room")
	return true
}

// createReserveFile allocates the reserve at path: preallocated where the
// platform can (fallocate on Linux, F_PREALLOCATE on darwin), otherwise, or
// when the allocation does not show in st_blocks, by writing incompressible
// bytes. It is built under a temporary name and renamed, so a reserve file
// always holds its full size. On error nothing is left behind.
func createReserveFile(path string, size int64) (err error) {
	tmp := path + reserveTempSuffix
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if perr := preallocateFn(f, size); perr != nil || !allocatedAtLeast(f, size) {
		if err = f.Truncate(0); err != nil {
			return err
		}
		if err = writeIncompressible(f, size); err != nil {
			return err
		}
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if !allocatedAtLeast(f, size) {
		return errors.New("the reserve file's blocks are not allocated (a compressing or deduplicating filesystem?)")
	}
	if err = f.Close(); err != nil {
		f = nil
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

// preallocateFn is preallocate; a variable so tests can force the fallback.
var preallocateFn = preallocate

// writeIncompressible fills f with size pseudo-random bytes: a compressing
// filesystem stores them at full size, so the blocks really are held.
func writeIncompressible(f *os.File, size int64) error {
	var seed [32]byte
	for i := range seed {
		seed[i] = byte(rand.Uint32())
	}
	src := rand.NewChaCha8(seed)
	buf := make([]byte, 1<<20)
	for left := size; left > 0; {
		n := min(int64(len(buf)), left)
		_, _ = src.Read(buf[:n])
		if _, err := f.Write(buf[:n]); err != nil {
			return fmt.Errorf("writing the WAL reserve file: %w", err)
		}
		left -= n
	}
	return nil
}

// allocatedAtLeast reports whether f holds at least size bytes of allocated
// blocks; true where the platform does not report blocks.
func allocatedAtLeast(f *os.File, size int64) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	blocks, ok := allocatedBytes(info)
	return !ok || blocks >= size
}
