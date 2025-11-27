package wal

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/rs/zerolog"
)

// RecoveryCallback is called for each batch of records during recovery
type RecoveryCallback func(ctx context.Context, records []map[string]interface{}) error

// RecoveryStats holds statistics about WAL recovery
type RecoveryStats struct {
	RecoveredFiles    int
	RecoveredBatches  int
	RecoveredEntries  int
	CorruptedEntries  int
	RecoveryDuration  time.Duration
}

// Recovery manages WAL recovery operations
type Recovery struct {
	walDir string
	logger zerolog.Logger
}

// NewRecovery creates a new WAL recovery manager
func NewRecovery(walDir string, logger zerolog.Logger) *Recovery {
	return &Recovery{
		walDir: walDir,
		logger: logger.With().Str("component", "wal-recovery").Logger(),
	}
}

// Recover scans the WAL directory and replays all WAL files
func (r *Recovery) Recover(ctx context.Context, callback RecoveryCallback) (*RecoveryStats, error) {
	startTime := time.Now()
	stats := &RecoveryStats{}

	// Check if WAL directory exists
	if _, err := os.Stat(r.walDir); os.IsNotExist(err) {
		r.logger.Info().Msg("No WAL directory found, skipping recovery")
		return stats, nil
	}

	// Find all WAL files (not .recovered)
	walFiles, err := r.findWALFiles()
	if err != nil {
		return nil, err
	}

	if len(walFiles) == 0 {
		r.logger.Info().Msg("No WAL files found, skipping recovery")
		return stats, nil
	}

	r.logger.Info().Int("files", len(walFiles)).Msg("WAL recovery started")

	// Process each WAL file
	for _, walFile := range walFiles {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		r.logger.Info().Str("file", filepath.Base(walFile)).Msg("Recovering WAL file")

		reader := NewReader(walFile, r.logger)
		entries, err := reader.ReadAll()
		if err != nil {
			r.logger.Error().Err(err).Str("file", walFile).Msg("Failed to read WAL file")
			continue
		}

		// Replay entries
		for _, entry := range entries {
			if err := callback(ctx, entry.Records); err != nil {
				r.logger.Error().Err(err).Msg("Failed to replay WAL entry")
				continue
			}
			stats.RecoveredBatches++
			stats.RecoveredEntries += len(entry.Records)
		}

		stats.CorruptedEntries += int(reader.CorruptedEntries)
		stats.RecoveredFiles++

		// Archive recovered WAL file
		archivePath := walFile + ".recovered"
		if err := os.Rename(walFile, archivePath); err != nil {
			r.logger.Error().Err(err).Str("file", walFile).Msg("Failed to archive WAL file")
		} else {
			r.logger.Info().Str("file", filepath.Base(archivePath)).Msg("WAL file archived")
		}
	}

	stats.RecoveryDuration = time.Since(startTime)

	r.logger.Info().
		Int("files", stats.RecoveredFiles).
		Int("batches", stats.RecoveredBatches).
		Int("entries", stats.RecoveredEntries).
		Int("corrupted", stats.CorruptedEntries).
		Dur("duration", stats.RecoveryDuration).
		Msg("WAL recovery complete")

	return stats, nil
}

// findWALFiles finds all WAL files in the directory, sorted by modification time
func (r *Recovery) findWALFiles() ([]string, error) {
	pattern := filepath.Join(r.walDir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	// Filter out .recovered files and sort by modification time
	var walFiles []string
	for _, match := range matches {
		// Skip .recovered files
		if filepath.Ext(match) == ".recovered" {
			continue
		}
		walFiles = append(walFiles, match)
	}

	// Sort by modification time (oldest first)
	sort.Slice(walFiles, func(i, j int) bool {
		infoI, _ := os.Stat(walFiles[i])
		infoJ, _ := os.Stat(walFiles[j])
		if infoI == nil || infoJ == nil {
			return walFiles[i] < walFiles[j]
		}
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	return walFiles, nil
}

// CleanupOldWALs removes recovered WAL files older than the specified age
func (r *Recovery) CleanupOldWALs(maxAge time.Duration) (int, int64, error) {
	pattern := filepath.Join(r.walDir, "*.wal.recovered")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, 0, err
	}

	now := time.Now()
	deletedCount := 0
	freedBytes := int64(0)

	for _, file := range matches {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}

		age := now.Sub(info.ModTime())
		if age > maxAge {
			size := info.Size()
			if err := os.Remove(file); err != nil {
				r.logger.Error().Err(err).Str("file", file).Msg("Failed to delete old WAL file")
				continue
			}
			deletedCount++
			freedBytes += size
			r.logger.Debug().Str("file", filepath.Base(file)).Msg("Deleted old WAL file")
		}
	}

	if deletedCount > 0 {
		r.logger.Info().
			Int("deleted", deletedCount).
			Int64("freed_bytes", freedBytes).
			Msg("Cleaned up old WAL files")
	}

	return deletedCount, freedBytes, nil
}

// ListWALFiles lists all WAL files in the directory
func (r *Recovery) ListWALFiles() (active []string, recovered []string, err error) {
	// Active WAL files
	activePattern := filepath.Join(r.walDir, "*.wal")
	activeMatches, err := filepath.Glob(activePattern)
	if err != nil {
		return nil, nil, err
	}

	// Filter out .recovered from active matches
	for _, match := range activeMatches {
		if filepath.Ext(match) != ".recovered" {
			active = append(active, match)
		}
	}

	// Recovered WAL files
	recoveredPattern := filepath.Join(r.walDir, "*.wal.recovered")
	recovered, err = filepath.Glob(recoveredPattern)
	if err != nil {
		return nil, nil, err
	}

	return active, recovered, nil
}
