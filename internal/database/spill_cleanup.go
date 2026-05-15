package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// spillFilePrefix is the DuckDB-internal filename prefix for query spill
// files written when intermediate state exceeds memory_limit (HASH_GROUP_BY
// overflow, large sorts, joins). Files are named like
// "duckdb_temp_storage_S32K-3.tmp" — the suffix encodes block size and an
// index. Validated against DuckDB 1.5.1. If a future bump renames the
// prefix, this sweep silently no-ops and orphans return.
const spillFilePrefix = "duckdb_temp_storage_"

// spillFileLiveThreshold is the minimum age a spill file must have before
// CleanupOrphanedSpillFiles will delete it. Anything younger may belong to
// a concurrent DuckDB process. Coarse safety net; a per-instance
// temp_directory is the durable fix and is the documented contract.
const spillFileLiveThreshold = 60 * time.Second

// CleanupOrphanedSpillFiles deletes leftover DuckDB temp-spill files from a
// previous run. On graceful shutdown DuckDB unlinks these itself, but on
// kill -9, OOM-kill, or crash they survive — they are not O_TMPFILE — and
// accumulate forever across restarts. Safe to call before opening the
// database; DuckDB recreates spill files lazily on first overflow.
//
// Files modified within spillFileLiveThreshold are skipped to protect a
// concurrent DuckDB process. The caller is still responsible for ensuring
// no other arc instance writes to this directory (no lockfile primitive
// today — see plan).
//
// Empty or non-existent tempDir is a no-op. Per-file failures are counted
// and summarized in a single log line — best-effort cleanup is the right
// semantics here since the alternative is unbounded disk growth. Only
// regular files matching the spill prefix are considered; subdirectories
// are ignored.
//
// Tests in spill_cleanup_test.go assert these contracts.
func CleanupOrphanedSpillFiles(tempDir string, logger zerolog.Logger) error {
	if tempDir == "" {
		return nil
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read temp dir %q: %w", tempDir, err)
	}

	now := time.Now()
	var (
		removed        int
		bytesFreed     int64
		skippedRecent  int
		statFailures   int
		removeFailures int
		firstErr       error
		firstErrFile   string
	)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, spillFilePrefix) || !strings.HasSuffix(name, ".tmp") {
			continue
		}

		info, statErr := entry.Info()
		if statErr != nil {
			// Race with a concurrent unlink (DuckDB itself, or another sweep) —
			// expected on a hot directory. Count and continue; one summary
			// Warn at the end covers the whole batch.
			statFailures++
			if firstErr == nil {
				firstErr = statErr
				firstErrFile = name
			}
			continue
		}
		if now.Sub(info.ModTime()) < spillFileLiveThreshold {
			skippedRecent++
			continue
		}

		path := filepath.Join(tempDir, name)
		size := info.Size()
		// os.Remove on a symlink unlinks the symlink, not the target — safe
		// under TOCTOU on Linux/macOS/Windows.
		if rmErr := os.Remove(path); rmErr != nil {
			removeFailures++
			if firstErr == nil {
				firstErr = rmErr
				firstErrFile = name
			}
			continue
		}
		removed++
		bytesFreed += size
	}

	// Pick the right level once: Warn if anything went wrong, Info if
	// there was work to summarize (so operators see the leak recovery),
	// Debug if the sweep was a no-op (so healthy clusters with frequent
	// restarts don't spam logs). Only one zerolog event is acquired from
	// the pool — branching the assignment avoids leaking an orphaned
	// event that never gets Msg'd.
	failures := statFailures + removeFailures
	var logEvt *zerolog.Event
	switch {
	case failures > 0:
		logEvt = logger.Warn().
			Err(firstErr).
			Str("first_failure_file", firstErrFile).
			Int("stat_failures", statFailures).
			Int("remove_failures", removeFailures)
	case removed > 0 || skippedRecent > 0:
		logEvt = logger.Info()
	default:
		logEvt = logger.Debug()
	}
	logEvt.
		Str("temp_directory", tempDir).
		Int("removed", removed).
		Int64("bytes_freed", bytesFreed).
		Int("skipped_recent", skippedRecent).
		Msg("Cleaned orphaned DuckDB spill files")

	return nil
}
