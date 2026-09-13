package tiering

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"
)

// Migrator handles file migration between tiers
type Migrator struct {
	manager       *Manager
	maxConcurrent int
	batchSize     int
	logger        zerolog.Logger
}

// MigratorConfig holds configuration for creating a migrator
type MigratorConfig struct {
	Manager       *Manager
	MaxConcurrent int
	BatchSize     int
	Logger        zerolog.Logger
}

// ErrCandidateQuarantined is returned by MigrateFile when the candidate's
// storage key turned out to be permanently unusable and the file index row
// was marked so it is never selected again (#758). It wraps the backend's
// ErrInvalidPath, so errors.Is works for either. Callers count it as a failed
// migration for the cycle that discovered it, but it is not a retryable one.
var ErrCandidateQuarantined = errors.New("tiering: candidate quarantined, its storage key is permanently unusable")

// quarantineReasonInvalidPath is what the file index row records. It is a
// fixed string rather than the wrapped error so the column stays greppable
// and does not carry the per-backend spelling of the same condition.
const quarantineReasonInvalidPath = "storage key is permanently unusable by every storage backend (storage.ErrInvalidPath)"

// NewMigrator creates a new migrator
func NewMigrator(cfg *MigratorConfig) *Migrator {
	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	return &Migrator{
		manager:       cfg.Manager,
		maxConcurrent: maxConcurrent,
		batchSize:     batchSize,
		logger:        cfg.Logger.With().Str("component", "tiering-migrator").Logger(),
	}
}

// MigrateTier migrates eligible files from one tier to another
// Returns the number of files migrated and the number of errors
func (m *Migrator) MigrateTier(ctx context.Context, fromTier, toTier Tier) (int, int) {
	m.logger.Info().
		Str("from_tier", string(fromTier)).
		Str("to_tier", string(toTier)).
		Msg("Starting tier migration")

	// Find candidates for migration
	candidates, err := m.FindCandidates(ctx, fromTier, toTier)
	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to find migration candidates")
		return 0, 1
	}

	if len(candidates) == 0 {
		m.logger.Info().Msg("No candidates for migration")
		return 0, 0
	}

	m.logger.Info().Int("candidates", len(candidates)).Msg("Found migration candidates")

	// Process in batches
	migrated := 0
	failed := 0

	for i := 0; i < len(candidates); i += m.batchSize {
		end := i + m.batchSize
		if end > len(candidates) {
			end = len(candidates)
		}

		batch := candidates[i:end]
		batchMigrated, batchFailed := m.MigrateBatch(ctx, batch)
		migrated += batchMigrated
		failed += batchFailed
	}

	return migrated, failed
}

// FindCandidates finds files eligible for migration from one tier to another
func (m *Migrator) FindCandidates(ctx context.Context, fromTier, toTier Tier) ([]MigrationCandidate, error) {
	// Only support Hot -> Cold migration in 2-tier system
	if fromTier != TierHot || toTier != TierCold {
		return nil, fmt.Errorf("unsupported migration: %s -> %s (only hot -> cold supported)", fromTier, toTier)
	}

	// Hot -> Cold: files older than hot_max_age_days
	maxAge := time.Duration(m.manager.config.DefaultHotMaxAgeDays) * 24 * time.Hour

	// Get files older than max age in the source tier
	files, err := m.manager.metadata.GetFilesOlderThan(ctx, fromTier, maxAge)
	if err != nil {
		return nil, fmt.Errorf("failed to get old files: %w", err)
	}

	// Filter by per-database policies
	var candidates []MigrationCandidate
	now := time.Now().UTC()

	for _, file := range files {
		// Check if database is excluded from tiering
		if m.manager.IsHotOnly(ctx, file.Database) {
			continue
		}

		// Get effective policy for this database
		policy := m.manager.GetEffectivePolicy(ctx, file.Database)

		// Calculate age threshold based on policy
		ageThreshold := time.Duration(policy.HotMaxAgeDays) * 24 * time.Hour

		// Check if file is old enough based on its policy
		fileAge := now.Sub(file.PartitionTime)
		if fileAge < ageThreshold {
			continue
		}

		// Only migrate daily-compacted files to cold tier
		if !strings.HasSuffix(file.Path, "_daily.parquet") {
			continue
		}

		// Spoke-namespace files register for visibility but do NOT migrate
		// yet: legacy spoke-side compacted files sync once and carry
		// sync_received receipts, and deleting their hot copy would make
		// confirmPresent forget the receipt — the spoke then re-offers the
		// file and the hub re-accepts a duplicate next to the cold copy (the
		// #611 hazard class, #687). The path shape decides, via the same
		// parser that registers files: content-based heuristics (numeric
		// spoke measurement names) and metadata-based reconstruction
		// (synthetic or legacy PartitionTime values) both misclassify.
		// An unparseable path is skipped conservatively.
		info, err := m.manager.parseFilePath(file.Path)
		if err != nil {
			m.manager.logger.Debug().Str("path", file.Path).
				Msg("Skipping unrecognized file path for cold migration")
			continue
		}
		if info.SpokeNamespaced && !m.manager.hasHotFileRemovalHook() {
			// Without receipt marking wired, deleting a spoke file's hot copy
			// would make confirmPresent forget its sync receipt and re-accept
			// a duplicate upload (#687).
			m.manager.logger.Debug().Str("path", file.Path).
				Msg("Skipping spoke-namespace file for cold migration (receipt marking not wired, #687)")
			continue
		}

		candidates = append(candidates, MigrationCandidate{
			Path:          file.Path,
			Database:      file.Database,
			Measurement:   file.Measurement,
			PartitionTime: file.PartitionTime,
			SizeBytes:     file.SizeBytes,
			CurrentTier:   fromTier,
			TargetTier:    toTier,
			Age:           fileAge,
		})
	}

	return candidates, nil
}

// MigrateBatch migrates a batch of files concurrently
func (m *Migrator) MigrateBatch(ctx context.Context, candidates []MigrationCandidate) (int, int) {
	if len(candidates) == 0 {
		return 0, 0
	}
	// Mark sync receipts for the WHOLE batch before any file work (#687):
	// one chunked UPDATE on the shared sync DB instead of per-file calls
	// from concurrent goroutines. Ordering is load-bearing — marking must
	// precede each file's tier flip and delete, and pre-marking the batch
	// satisfies that for every member; a marked receipt whose file does not
	// migrate (later copy failure) is documented harmless. A mark failure
	// aborts the batch so no file's hot copy can be removed unmarked.
	paths := make([]string, len(candidates))
	for i, c := range candidates {
		paths[i] = c.Path
	}
	if err := m.manager.notifyHotFilesRemoved(paths); err != nil {
		m.logger.Warn().Err(err).Int("candidates", len(candidates)).
			Msg("Could not mark sync receipts; aborting migration batch")
		return 0, len(candidates)
	}

	if len(candidates) == 0 {
		return 0, 0
	}

	sem := semaphore.NewWeighted(int64(m.maxConcurrent))
	var wg sync.WaitGroup
	var migrated, failed int64
	var mu sync.Mutex

	for _, candidate := range candidates {
		if err := sem.Acquire(ctx, 1); err != nil {
			m.logger.Error().Err(err).Msg("Failed to acquire semaphore")
			break
		}

		wg.Add(1)
		go func(c MigrationCandidate) {
			defer wg.Done()
			defer sem.Release(1)

			if err := m.MigrateFile(ctx, c); err != nil {
				// A quarantined candidate already logged its own, definitive
				// line at Error; repeating it here would read as a second
				// failure of the same file. It still counts as failed for this
				// cycle: nothing was migrated, and the cycle summary should
				// say so the one time it happens.
				if !errors.Is(err, ErrCandidateQuarantined) {
					m.logger.Error().
						Err(err).
						Str("path", c.Path).
						Str("from", string(c.CurrentTier)).
						Str("to", string(c.TargetTier)).
						Msg("Failed to migrate file")
				}

				mu.Lock()
				failed++
				mu.Unlock()
			} else {
				mu.Lock()
				migrated++
				mu.Unlock()
			}
		}(candidate)
	}

	wg.Wait()
	return int(migrated), int(failed)
}

// MigrateFile migrates a single file from one tier to another
func (m *Migrator) MigrateFile(ctx context.Context, candidate MigrationCandidate) error {
	// .UTC() so StartedAt persists in a stable timezone (matches the
	// rest of internal/tiering/metadata.go). time.Since(startTime) is
	// location-independent so the elapsed-duration math is unaffected.
	// Issue #460.
	startTime := time.Now().UTC()

	// Get source and destination backends
	srcBackend := m.manager.GetBackendForTier(candidate.CurrentTier)
	dstBackend := m.manager.GetBackendForTier(candidate.TargetTier)

	if srcBackend == nil {
		return fmt.Errorf("source backend not available for tier: %s", candidate.CurrentTier)
	}
	if dstBackend == nil {
		return fmt.Errorf("destination backend not available for tier: %s", candidate.TargetTier)
	}

	// Record migration start
	record := &MigrationRecord{
		FilePath:  candidate.Path,
		Database:  candidate.Database,
		FromTier:  candidate.CurrentTier,
		ToTier:    candidate.TargetTier,
		SizeBytes: candidate.SizeBytes,
		StartedAt: startTime,
	}
	migrationID, err := m.manager.metadata.RecordMigration(ctx, record)
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to record migration start")
	}

	// Perform the migration using streaming to avoid loading entire files into memory
	migrationErr := m.copyFileStreaming(ctx, srcBackend, dstBackend, candidate.Path, candidate.SizeBytes)

	if migrationErr != nil {
		// Record failure
		if migrationID > 0 {
			m.manager.metadata.CompleteMigration(ctx, migrationID, migrationErr)
		}
		if errors.Is(migrationErr, storage.ErrInvalidPath) {
			return m.quarantineCandidate(ctx, candidate, migrationErr)
		}
		return migrationErr
	}

	// Update tier metadata. Sync receipts were already marked for the whole
	// batch by MigrateBatch before any file work began (#687) — marking must
	// precede this flip, and a marked receipt for a file that ends up not
	// migrating is documented harmless by the receive path.
	if err := m.manager.metadata.UpdateTier(ctx, candidate.Path, candidate.TargetTier); err != nil {
		// Rollback: delete from destination
		if delErr := dstBackend.Delete(ctx, candidate.Path); delErr != nil {
			m.logger.Error().Err(delErr).Str("path", candidate.Path).Msg("Failed to rollback destination file")
		}
		if migrationID > 0 {
			m.manager.metadata.CompleteMigration(ctx, migrationID, err)
		}
		return fmt.Errorf("failed to update tier metadata: %w", err)
	}

	// Delete from source tier
	if err := srcBackend.Delete(ctx, candidate.Path); err != nil {
		m.logger.Warn().Err(err).Str("path", candidate.Path).Msg("Failed to delete source file after migration")
		// Don't fail the migration - file is in destination, just source cleanup failed
	} else {
		// Clean up empty parent directories after successful delete
		m.CleanupEmptyDirectories(ctx, candidate.Path)
	}

	// Record success
	if migrationID > 0 {
		m.manager.metadata.CompleteMigration(ctx, migrationID, nil)
	}

	duration := time.Since(startTime)
	m.logger.Debug().
		Str("path", candidate.Path).
		Str("from", string(candidate.CurrentTier)).
		Str("to", string(candidate.TargetTier)).
		Int64("size_bytes", candidate.SizeBytes).
		Dur("duration", duration).
		Msg("File migrated successfully")

	return nil
}

// quarantineCandidate handles a migration that failed because no backend can
// address the candidate's key (#758). The failure is permanent: the same key
// is refused by every backend on every attempt, so leaving the row in the hot
// tier would make FindCandidates re-select it next cycle, write another
// failed-migration row, and log the same error, forever.
//
// The row is marked rather than deleted or re-tiered. On local storage the
// file is a real data file inside the partition glob, so the query path still
// serves it and the index should keep saying it exists in hot. What changes is
// that the two work-set queries stop returning it. The marked row is the
// operator's record; the only remedy is renaming the object, after which the
// next scan registers the new key as a fresh row.
//
// If the mark itself cannot be persisted the original error is returned as a
// plain failure, so the candidate IS retried next cycle. That is right: the
// persistence failure is the transient one, and a retry of it is a single
// UPDATE, not a copy.
func (m *Migrator) quarantineCandidate(ctx context.Context, candidate MigrationCandidate, cause error) error {
	if err := m.manager.metadata.QuarantineFile(ctx, candidate.Path, quarantineReasonInvalidPath); err != nil {
		m.logger.Error().Err(err).
			Str("path", candidate.Path).
			AnErr("cause", cause).
			Msg("Migration failed on a permanently unusable storage key and the quarantine mark could not be persisted; the candidate will be re-selected next cycle")
		return cause
	}
	metrics.Get().IncStorageInvalidPathQuarantined()
	m.logger.Error().Err(cause).
		Str("path", candidate.Path).
		Str("from", string(candidate.CurrentTier)).
		Str("to", string(candidate.TargetTier)).
		Msg("Migration candidate has a permanently unusable storage key; quarantined so it is never selected again. Its tier is unchanged and the file is not deleted. Rename the object by hand to make it migratable")
	return fmt.Errorf("%w: %w", ErrCandidateQuarantined, cause)
}

// copyFile copies a file from source to destination backend
func (m *Migrator) copyFile(ctx context.Context, src, dst interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
}, path string, expectedSize int64) error {

	// For now, use simple read/write
	// TODO: Use streaming (ReadTo/WriteReader) for large files

	data, err := src.Read(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to read from source: %w", err)
	}

	// Verify size matches
	if int64(len(data)) != expectedSize && expectedSize > 0 {
		return fmt.Errorf("size mismatch: expected %d, got %d", expectedSize, len(data))
	}

	if err := dst.Write(ctx, path, data); err != nil {
		return fmt.Errorf("failed to write to destination: %w", err)
	}

	return nil
}

// StreamingBackend is an interface for backends that support streaming
type StreamingBackend interface {
	ReadTo(ctx context.Context, path string, w io.Writer) error
	WriteReader(ctx context.Context, path string, r io.Reader, size int64) error
}

// copyFileStreaming copies a file using streaming for memory efficiency
// This is used for large files to avoid loading them entirely into memory
func (m *Migrator) copyFileStreaming(ctx context.Context, src, dst StreamingBackend, path string, size int64) error {
	// Create a pipe to stream data from source to destination
	pr, pw := io.Pipe()

	errCh := make(chan error, 2)

	// Read from source in a goroutine
	go func() {
		err := src.ReadTo(ctx, path, pw)
		pw.CloseWithError(err)
		errCh <- err
	}()

	// Write to destination in main goroutine
	go func() {
		err := dst.WriteReader(ctx, path, pr, size)
		pr.CloseWithError(err)
		errCh <- err
	}()

	// Wait for both operations to complete. The first error to arrive is the
	// one reported, except that a permanent ErrInvalidPath wins over whatever
	// arrived first: which side fails first is a goroutine race (the pipe
	// closes with the reader's error, and the writer may report that or its
	// own), and the caller branches on errors.Is to quarantine the candidate
	// (#758), so the classification must not depend on the ordering.
	var firstErr error
	for i := 0; i < 2; i++ {
		err := <-errCh
		if err == nil {
			continue
		}
		if firstErr == nil || (errors.Is(err, storage.ErrInvalidPath) && !errors.Is(firstErr, storage.ErrInvalidPath)) {
			firstErr = err
		}
	}

	if firstErr != nil {
		return fmt.Errorf("streaming copy failed: %w", firstErr)
	}

	return nil
}

// ReconcileOrphanedFiles finds and deletes files that exist in hot storage
// but are tracked as cold in metadata (orphaned after failed hot deletion during migration).
// Only checks files migrated within the last 48 hours to limit I/O.
func (m *Migrator) ReconcileOrphanedFiles(ctx context.Context) (orphansFound, deleted, failed int) {
	const reconcileWindow = 48 * time.Hour

	coldFiles, err := m.manager.metadata.GetRecentlyMigratedFiles(ctx, TierCold, reconcileWindow)
	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to get recently migrated files for reconciliation")
		return 0, 0, 1
	}

	if len(coldFiles) == 0 {
		return 0, 0, 0
	}

	hotBackend := m.manager.GetBackendForTier(TierHot)
	if hotBackend == nil {
		m.logger.Error().Msg("Hot backend not available for orphaned file reconciliation")
		return 0, 0, 1
	}

	for _, file := range coldFiles {
		select {
		case <-ctx.Done():
			return orphansFound, deleted, failed
		default:
		}

		exists, err := hotBackend.Exists(ctx, file.Path)
		if errors.Is(err, storage.ErrInvalidPath) {
			// Permanent: Exists fails identically on every cycle until the
			// 48-hour window closes, and Delete would fail the same way, so
			// there is nothing this loop can ever do for the row (#758). Mark
			// it so GetRecentlyMigratedFiles stops returning it. The cold
			// row is left as cold: this sweep only ever removes hot copies,
			// and it cannot even establish whether one exists here.
			//
			// A persistence failure is counted and retried next cycle, which
			// is a retry of one UPDATE, not a storm.
			qErr := m.manager.metadata.QuarantineFile(ctx, file.Path, quarantineReasonInvalidPath)
			if qErr != nil {
				m.logger.Error().Err(qErr).Str("path", file.Path).AnErr("cause", err).
					Msg("Cold file's storage key is permanently unusable and the quarantine mark could not be persisted; reconciliation will retry it next cycle")
				failed++
				continue
			}
			metrics.Get().IncStorageInvalidPathQuarantined()
			m.logger.Error().Err(err).Str("path", file.Path).
				Msg("Cold file's storage key is permanently unusable, so its hot copy can be neither checked nor removed; quarantined so reconciliation stops retrying it. Remove any hot copy by hand")
			failed++
			continue
		}
		if err != nil {
			m.logger.Warn().Err(err).Str("path", file.Path).Msg("Failed to check hot existence during reconciliation")
			failed++
			continue
		}

		if !exists {
			continue
		}

		// Orphan found — file is in both hot and cold
		orphansFound++
		m.logger.Info().
			Str("path", file.Path).
			Str("database", file.Database).
			Str("measurement", file.Measurement).
			Int64("size_bytes", file.SizeBytes).
			Msg("Found orphaned hot file (metadata says cold), deleting from hot")

		// Mark sync receipts before this delete too (#687). Normally a no-op
		// (MigrateFile marked before flipping the tier), but reconciliation
		// is the crash-recovery path and must uphold the same invariant.
		if err := m.manager.notifyHotFilesRemoved([]string{file.Path}); err != nil {
			m.logger.Warn().Err(err).Str("path", file.Path).
				Msg("Could not mark sync receipts; keeping orphaned hot file for the next cycle")
			failed++
			continue
		}

		if err := hotBackend.Delete(ctx, file.Path); err != nil {
			m.logger.Warn().Err(err).Str("path", file.Path).Msg("Failed to delete orphaned hot file")
			failed++
			continue
		}

		deleted++
		m.CleanupEmptyDirectories(ctx, file.Path)
	}

	return orphansFound, deleted, failed
}

// CleanupEmptyDirectories removes empty directories after file migration
// Walks up from the file's hour directory, removing empty dirs until hitting a non-empty one
// Path format: {database}/{measurement}/{year}/{month}/{day}/{hour}/
func (m *Migrator) CleanupEmptyDirectories(ctx context.Context, filePath string) {
	// Get DirectoryRemover interface from hot backend
	dirRemover, ok := m.manager.hotBackend.(storage.DirectoryRemover)
	if !ok {
		return // Backend doesn't support directory removal
	}

	// Extract directory path from file path
	dir := filepath.Dir(filePath)

	// Walk up the tree: hour -> day -> month -> year -> measurement -> database
	// Stop when we hit a non-empty directory or reach the root
	// Maximum depth of 6 prevents accidentally climbing too far
	for depth := 0; depth < 6; depth++ {
		if dir == "" || dir == "." {
			break
		}

		// Try to remove - will fail silently if not empty (os.Remove only removes empty dirs)
		err := dirRemover.RemoveDirectory(ctx, dir)
		if err != nil {
			// Directory not empty or other error - stop climbing
			break
		}

		m.logger.Debug().Str("dir", dir).Msg("Removed empty directory")
		dir = filepath.Dir(dir)
	}
}
