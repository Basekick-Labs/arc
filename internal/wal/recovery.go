package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// RecoveryCallback is called for each batch of records during recovery (row format)
type RecoveryCallback func(ctx context.Context, records []map[string]interface{}) error

// TrackedRecoveryCallback receives the original WAL entry identity so the
// eventual buffer flush can checkpoint the same entry if the recovery barrier
// fails and the file must be replayed again.
type TrackedRecoveryCallback func(ctx context.Context, records []map[string]interface{}, walIdentity string) error

// ColumnarRecoveryCallback is called for columnar WAL entries during recovery.
//
// walIdentity is the identity of the entry being replayed, so the re-buffered
// batch can inherit it and have its eventual flush checkpoint the ORIGINAL
// entry — which is what stops a later pass replaying it again. It is empty for
// an entry that carries no tracked identity.
type ColumnarRecoveryCallback func(ctx context.Context, database, measurement string, columns map[string][]interface{}, walIdentity string) error

// RecoveryStats holds statistics about WAL recovery
type RecoveryStats struct {
	RecoveredFiles   int
	RecoveredBatches int
	RecoveredEntries int
	CorruptedEntries int
	SkippedFiles     int
	KeptFiles        int
	BarrierFailures  int
	QuarantinedFiles int
	RecoveryDuration time.Duration
}

// RecoveryOptions configures WAL recovery behavior
type RecoveryOptions struct {
	// SkipActiveFile is the path to the currently active WAL file that should be skipped
	// during periodic recovery (to avoid reading a file being actively written)
	SkipActiveFile string

	// AdditionalCheckpointHashes contains checkpoints read safely from the
	// active file, which is intentionally excluded from recovery scans.
	AdditionalCheckpointHashes []string

	// BatchSize limits how many records are replayed per callback invocation
	// This provides backpressure during mass recovery after prolonged outages
	// 0 means no limit (all records in an entry replayed at once)
	BatchSize int

	// ColumnarCallback handles columnar WAL entries from the zero-copy write path
	ColumnarCallback ColumnarRecoveryCallback

	// TrackedRowCallback handles row-format entries while preserving the entry's
	// identity through the buffer flush. When configured, it receives the full
	// WAL entry at once so a single identity is not split across independent
	// flush tasks.
	TrackedRowCallback TrackedRecoveryCallback

	// MinFileAge, when > 0, skips WAL files modified more recently than this.
	// Defense against the #594 class beyond the SkipActiveFile name match:
	// the periodic recovery reads CurrentFile() and then scans — a rotation
	// landing between those instants would put the NEW active (header-only)
	// file in the scan, and deleting it re-creates the unlinked-inode data
	// loss. Production call sites pass a few seconds; a genuinely
	// recoverable young file is picked up by the next pass. Zero disables
	// the guard (tests recover freshly-written files).
	MinFileAge time.Duration

	// MinFileAgeExemptFiles are known-closed files that may be recovered even
	// when their modification time is recent. The periodic recovery path uses
	// this for the file that Writer.Rotate just closed; MinFileAge still
	// protects files that rotate while recovery is scanning.
	MinFileAgeExemptFiles []string

	// BeforeDelete is an epoch barrier that must make every replayed entry in a
	// batch durable before the corresponding WAL files are removed. The
	// callback runs after BarrierBatchFiles files or BarrierBatchRows records
	// have replayed, whichever threshold is reached first. A nil callback means
	// the recovery callback itself provides synchronous durability.
	BeforeDelete func(context.Context) error

	// BarrierBatchFiles bounds how many successfully replayed files are covered
	// by one BeforeDelete call. Defaults to 16 when BeforeDelete is configured.
	BarrierBatchFiles int

	// BarrierBatchRows optionally triggers BeforeDelete after this many rows
	// have replayed, allowing callers to align barriers with max_buffer_size.
	BarrierBatchRows int

	// MaxReplayFailures is the number of failed replay passes before a poison
	// WAL file is quarantined as .wal.failed. Defaults to 3.
	MaxReplayFailures int
}

// Recovery manages WAL recovery operations
type Recovery struct {
	walDir string
	logger zerolog.Logger

	failuresMu sync.Mutex
	failures   map[string]int
}

// NewRecovery creates a new WAL recovery manager
func NewRecovery(walDir string, logger zerolog.Logger) *Recovery {
	return &Recovery{
		walDir:   walDir,
		logger:   logger.With().Str("component", "wal-recovery").Logger(),
		failures: make(map[string]int),
	}
}

// Recover scans the WAL directory and replays all WAL files
func (r *Recovery) Recover(ctx context.Context, callback RecoveryCallback) (*RecoveryStats, error) {
	return r.RecoverWithOptions(ctx, callback, nil)
}

// RecoverWithOptions scans the WAL directory and replays WAL files with configurable options
func (r *Recovery) RecoverWithOptions(ctx context.Context, callback RecoveryCallback, opts *RecoveryOptions) (*RecoveryStats, error) {
	startTime := time.Now()
	stats := &RecoveryStats{}

	if opts == nil {
		opts = &RecoveryOptions{}
	}
	barrierBatchFiles := opts.BarrierBatchFiles
	if barrierBatchFiles <= 0 {
		barrierBatchFiles = 16
	}
	maxReplayFailures := opts.MaxReplayFailures
	if maxReplayFailures <= 0 {
		maxReplayFailures = 3
	}
	minAgeExempt := make(map[string]struct{}, len(opts.MinFileAgeExemptFiles))
	for _, path := range opts.MinFileAgeExemptFiles {
		minAgeExempt[filepath.Clean(path)] = struct{}{}
	}

	// Check if WAL directory exists
	if _, err := os.Stat(r.walDir); os.IsNotExist(err) {
		r.logger.Info().Msg("No WAL directory found, skipping recovery")
		return stats, nil
	}

	// Find all pending WAL files
	walFiles, err := r.findWALFiles()
	if err != nil {
		return nil, err
	}

	if len(walFiles) == 0 {
		r.logger.Info().Msg("No WAL files found, skipping recovery")
		return stats, nil
	}

	r.logger.Info().Int("files", len(walFiles)).Msg("WAL recovery started")

	flushed := make(map[string]struct{})
	for _, hash := range opts.AdditionalCheckpointHashes {
		flushed[hash] = struct{}{}
	}

	type recoveredWALFile struct {
		path    string
		entries int
		batches int
	}
	var pendingDelete []recoveredWALFile
	pendingRows := 0
	var recoveryErr error
	flushPending := func() bool {
		if len(pendingDelete) == 0 {
			return true
		}
		if opts.BeforeDelete != nil {
			if err := opts.BeforeDelete(ctx); err != nil {
				stats.BarrierFailures++
				stats.KeptFiles += len(pendingDelete)
				recoveryErr = fmt.Errorf("WAL recovery flush barrier: %w", err)
				r.logger.Error().Err(err).
					Int("files", len(pendingDelete)).
					Msg("WAL recovery flush barrier failed; keeping replayed files")
				pendingDelete = nil
				pendingRows = 0
				return false
			}
		}
		for index, recovered := range pendingDelete {
			if err := os.Remove(recovered.path); err != nil {
				if !os.IsNotExist(err) {
					stats.KeptFiles += len(pendingDelete) - index
					recoveryErr = fmt.Errorf("delete durably recovered WAL file %q: %w", recovered.path, err)
					r.logger.Error().Err(err).Str("file", recovered.path).Msg("Failed to delete durably recovered WAL file")
					pendingDelete = nil
					pendingRows = 0
					return false
				}
			}
			stats.RecoveredFiles++
			stats.RecoveredBatches += recovered.batches
			stats.RecoveredEntries += recovered.entries
			r.logger.Info().
				Str("file", filepath.Base(recovered.path)).
				Int("entries", recovered.entries).
				Msg("WAL file recovered, flushed, and deleted")
		}
		pendingDelete = nil
		pendingRows = 0
		return true
	}
	recordReplayFailure := func(path string, cause error) bool {
		quarantined, quarantineErr := r.noteReplayFailure(path, maxReplayFailures)
		if quarantineErr != nil {
			stats.KeptFiles++
			r.logger.Error().Err(quarantineErr).Str("file", path).Msg("Failed to quarantine repeatedly failing WAL file")
			return false
		}
		if quarantined {
			stats.QuarantinedFiles++
			r.logger.Error().Err(cause).
				Str("file", filepath.Base(path)).
				Int("attempts", maxReplayFailures).
				Msg("Quarantined WAL file after repeated recovery failures")
			return true
		}
		stats.KeptFiles++
		return false
	}

	// Quarantined data is not replayed, but its checkpoints remain durable
	// proof for entries in earlier retained files, including after a restart.
	// Include the collision suffix used by noteReplayFailure as well.
	quarantinedFiles, err := filepath.Glob(filepath.Join(r.walDir, "*.wal*.failed"))
	if err != nil {
		return stats, fmt.Errorf("find quarantined WAL checkpoints: %w", err)
	}
	checkpointFiles := append(append([]string(nil), walFiles...), quarantinedFiles...)

	// Scan non-active files for checkpoints before invoking callbacks. A flush
	// checkpoint can land in the next WAL file after rotation, while the data
	// entry remains in the previous file. Recently rotated files are scanned for
	// checkpoints too, even though the replay pass below skips them.
	for _, walFile := range checkpointFiles {
		select {
		case <-ctx.Done():
			stats.KeptFiles += len(pendingDelete)
			return stats, ctx.Err()
		default:
		}

		// Skip the active WAL file if specified (prevents reading file being written)
		if opts.SkipActiveFile != "" && walFile == opts.SkipActiveFile {
			r.logger.Debug().Str("file", filepath.Base(walFile)).Msg("Skipping active WAL file")
			stats.SkippedFiles++
			continue
		}

		reader := NewReader(walFile, r.logger)
		checkpointHashes, err := reader.ReadCheckpointHashes()
		if err != nil {
			r.logger.Error().Err(err).Str("file", walFile).Msg("Failed to scan WAL checkpoints")
		}
		// A later damaged entry must not erase earlier checksum-validated
		// checkpoints returned by the reader alongside its scan error.
		for _, hash := range checkpointHashes {
			flushed[hash] = struct{}{}
		}
	}

	// Process each WAL file
	for fileIndex, walFile := range walFiles {
		select {
		case <-ctx.Done():
			stats.KeptFiles += len(pendingDelete)
			return stats, ctx.Err()
		default:
		}
		if opts.SkipActiveFile != "" && walFile == opts.SkipActiveFile {
			stats.KeptFiles += len(walFiles) - fileIndex - 1
			break
		}
		if opts.MinFileAge > 0 {
			_, exempt := minAgeExempt[filepath.Clean(walFile)]
			if info, statErr := os.Stat(walFile); !exempt && statErr == nil && time.Since(info.ModTime()) < opts.MinFileAge {
				r.logger.Debug().Str("file", filepath.Base(walFile)).Msg("Skipping too-recent WAL file (possible fresh rotation)")
				stats.SkippedFiles++
				stats.KeptFiles++
				stats.KeptFiles += len(walFiles) - fileIndex - 1
				break
			}
		}

		reader := NewReader(walFile, r.logger)
		entries, err := reader.ReadAll()
		if err != nil {
			r.logger.Error().Err(err).Str("file", walFile).Msg("Failed to read WAL file")
			if !recordReplayFailure(walFile, err) {
				stats.KeptFiles += len(walFiles) - fileIndex - 1
				break
			}
			continue
		}
		r.logger.Info().Str("file", filepath.Base(walFile)).Msg("Recovering WAL file")

		// Replay entries - track if all succeed
		allEntriesSucceeded := true
		fileRecoveredBatches := 0
		fileRecoveredEntries := 0

		for _, entry := range entries {
			if len(entry.CheckpointHashes) > 0 {
				continue
			}
			if _, ok := flushed[entry.PayloadHash]; ok {
				r.logger.Debug().Str("payload_hash", entry.PayloadHash).Msg("Skipping WAL entry covered by flush checkpoint")
				continue
			}
			// Dispatch based on entry format
			if entry.ColumnarData != nil && opts.ColumnarCallback != nil {
				// Columnar entry from zero-copy AppendRaw path
				if err := opts.ColumnarCallback(ctx, entry.ColumnarData.Database, entry.ColumnarData.Measurement, entry.ColumnarData.Columns, entry.PayloadHash); err != nil {
					// #590: continue with the remaining entries instead of
					// abandoning the rest of the file — one poisoned entry
					// (e.g. a payload the write path rejects) must not
					// discard every durable entry after it. The file is not
					// deleted by THIS recovery pass (allEntriesSucceeded=
					// false); repeated callback failures eventually quarantine
					// the file rather than age-purging its remaining data.
					r.logger.Error().Err(err).
						Str("database", entry.ColumnarData.Database).
						Str("measurement", entry.ColumnarData.Measurement).
						Msg("Failed to replay columnar WAL entry; continuing with remaining entries")
					allEntriesSucceeded = false
					stats.CorruptedEntries++
					continue
				}
				fileRecoveredBatches++
				// Count rows from first column length
				for _, col := range entry.ColumnarData.Columns {
					fileRecoveredEntries += len(col)
					break
				}
			} else if entry.Records != nil {
				// Row-format entry from Append path
				if opts.TrackedRowCallback != nil {
					if err := opts.TrackedRowCallback(ctx, entry.Records, entry.PayloadHash); err != nil {
						r.logger.Error().Err(err).Msg("Failed to replay tracked WAL entry")
						allEntriesSucceeded = false
						break
					}
					fileRecoveredBatches++
					fileRecoveredEntries += len(entry.Records)
				} else if opts.BatchSize > 0 && len(entry.Records) > opts.BatchSize {
					for i := 0; i < len(entry.Records); i += opts.BatchSize {
						end := i + opts.BatchSize
						if end > len(entry.Records) {
							end = len(entry.Records)
						}
						batch := entry.Records[i:end]
						if err := callback(ctx, batch); err != nil {
							r.logger.Error().Err(err).Msg("Failed to replay WAL entry batch")
							allEntriesSucceeded = false
							break
						}
						fileRecoveredBatches++
						fileRecoveredEntries += len(batch)
					}
					if !allEntriesSucceeded {
						break
					}
				} else {
					if err := callback(ctx, entry.Records); err != nil {
						// Deliberate asymmetry with the columnar branch's
						// continue: row-format callbacks apply records one
						// by one, so a mid-entry failure leaves an unknown
						// prefix applied — continuing to the next entry
						// would need per-record granularity to be
						// meaningful. Row entries are the rare non-msgpack
						// fallback; keep the conservative break here.
						r.logger.Error().Err(err).Msg("Failed to replay WAL entry")
						allEntriesSucceeded = false
						break
					}
					fileRecoveredBatches++
					fileRecoveredEntries += len(entry.Records)
				}
			}
		}

		stats.CorruptedEntries += int(reader.CorruptedEntries)
		if reader.CorruptedEntries > 0 {
			allEntriesSucceeded = false
		}

		// Only delete WAL file if ALL entries were successfully replayed
		if allEntriesSucceeded && len(entries) > 0 {
			r.clearReplayFailure(walFile)
			if fileRecoveredBatches == 0 || opts.BeforeDelete == nil {
				pendingDelete = append(pendingDelete, recoveredWALFile{path: walFile, entries: fileRecoveredEntries, batches: fileRecoveredBatches})
				if !flushPending() {
					stats.KeptFiles += len(walFiles) - fileIndex - 1
					break
				}
			} else {
				pendingDelete = append(pendingDelete, recoveredWALFile{path: walFile, entries: fileRecoveredEntries, batches: fileRecoveredBatches})
				pendingRows += fileRecoveredEntries
				rowsReached := opts.BarrierBatchRows > 0 && pendingRows >= opts.BarrierBatchRows
				if len(pendingDelete) >= barrierBatchFiles || rowsReached {
					if !flushPending() {
						stats.KeptFiles += len(walFiles) - fileIndex - 1
						break
					}
				}
			}
		} else if allEntriesSucceeded && len(entries) == 0 {
			// Empty WAL file (header-only, 7 bytes) — safe to delete
			if err := os.Remove(walFile); err != nil {
				if !os.IsNotExist(err) {
					stats.KeptFiles++
					stats.KeptFiles += len(walFiles) - fileIndex - 1
					recoveryErr = fmt.Errorf("delete empty WAL file %q: %w", walFile, err)
					break
				}
			}
			r.logger.Debug().Str("file", filepath.Base(walFile)).Msg("Deleted empty WAL file")
			stats.RecoveredFiles++
			r.clearReplayFailure(walFile)
		} else if !allEntriesSucceeded {
			quarantined := recordReplayFailure(walFile, fmt.Errorf("one or more entries could not be replayed"))
			r.logger.Warn().
				Str("file", filepath.Base(walFile)).
				Int("recovered_entries", fileRecoveredEntries).
				Int("total_entries", len(entries)).
				Msg("WAL file replay failed; keeping it or quarantining after repeated failures")
			if !quarantined {
				stats.KeptFiles += len(walFiles) - fileIndex - 1
				break
			}
		}
	}
	if recoveryErr == nil {
		flushPending()
	}

	stats.RecoveryDuration = time.Since(startTime)

	r.logger.Info().
		Int("files", stats.RecoveredFiles).
		Int("batches", stats.RecoveredBatches).
		Int("entries", stats.RecoveredEntries).
		Int("corrupted", stats.CorruptedEntries).
		Int("skipped", stats.SkippedFiles).
		Int("kept", stats.KeptFiles).
		Int("barrier_failures", stats.BarrierFailures).
		Int("quarantined", stats.QuarantinedFiles).
		Dur("duration", stats.RecoveryDuration).
		Msg("WAL recovery complete")

	return stats, recoveryErr
}

// noteReplayFailure counts consecutive failed replay passes for a WAL file and
// moves it out of the recovery glob after the configured threshold. The
// quarantined file remains on disk for operator inspection; it is never
// discarded as part of recovery.
func (r *Recovery) noteReplayFailure(path string, maxAttempts int) (bool, error) {
	r.failuresMu.Lock()
	if r.failures == nil {
		r.failures = make(map[string]int)
	}
	r.failures[path]++
	attempts := r.failures[path]
	r.failuresMu.Unlock()
	if attempts < maxAttempts {
		return false, nil
	}

	quarantinePath := path + ".failed"
	if _, err := os.Lstat(quarantinePath); err == nil {
		quarantinePath = fmt.Sprintf("%s.%d.failed", path, time.Now().UnixNano())
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		return false, err
	}

	r.failuresMu.Lock()
	delete(r.failures, path)
	r.failuresMu.Unlock()
	return true, nil
}

func (r *Recovery) clearReplayFailure(path string) {
	r.failuresMu.Lock()
	delete(r.failures, path)
	r.failuresMu.Unlock()
}

// findWALFiles finds all WAL files in the directory, sorted by modification time
func (r *Recovery) findWALFiles() ([]string, error) {
	pattern := filepath.Join(r.walDir, "*.wal")
	walFiles, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
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

// CleanupOldWALs removes legacy .recovered WAL files older than the specified age.
// Note: As of the current implementation, WAL files are deleted immediately after
// successful recovery, so this function is primarily for cleaning up legacy files
// from previous versions that renamed files to .recovered instead of deleting them.
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

// ListWALFiles lists all WAL files in the directory.
// Returns active (pending) WAL files and legacy .recovered files.
// Note: As of the current implementation, WAL files are deleted immediately after
// successful recovery, so the recovered list will typically be empty or contain
// only legacy files from previous versions.
func (r *Recovery) ListWALFiles() (active []string, recovered []string, err error) {
	// Active WAL files (pending recovery)
	activePattern := filepath.Join(r.walDir, "*.wal")
	active, err = filepath.Glob(activePattern)
	if err != nil {
		return nil, nil, err
	}

	// Legacy recovered WAL files (from previous versions)
	recoveredPattern := filepath.Join(r.walDir, "*.wal.recovered")
	recovered, err = filepath.Glob(recoveredPattern)
	if err != nil {
		return nil, nil, err
	}

	return active, recovered, nil
}
