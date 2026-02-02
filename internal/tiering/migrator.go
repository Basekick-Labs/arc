package tiering

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	errors := 0

	for i := 0; i < len(candidates); i += m.batchSize {
		end := i + m.batchSize
		if end > len(candidates) {
			end = len(candidates)
		}

		batch := candidates[i:end]
		batchMigrated, batchErrors := m.MigrateBatch(ctx, batch)
		migrated += batchMigrated
		errors += batchErrors
	}

	return migrated, errors
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

	sem := semaphore.NewWeighted(int64(m.maxConcurrent))
	var wg sync.WaitGroup
	var migrated, errors int64
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
				m.logger.Error().
					Err(err).
					Str("path", c.Path).
					Str("from", string(c.CurrentTier)).
					Str("to", string(c.TargetTier)).
					Msg("Failed to migrate file")

				mu.Lock()
				errors++
				mu.Unlock()
			} else {
				mu.Lock()
				migrated++
				mu.Unlock()
			}
		}(candidate)
	}

	wg.Wait()
	return int(migrated), int(errors)
}

// MigrateFile migrates a single file from one tier to another
func (m *Migrator) MigrateFile(ctx context.Context, candidate MigrationCandidate) error {
	startTime := time.Now()

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

	// Perform the migration
	migrationErr := m.copyFile(ctx, srcBackend, dstBackend, candidate.Path, candidate.SizeBytes)

	if migrationErr != nil {
		// Record failure
		if migrationID > 0 {
			m.manager.metadata.CompleteMigration(ctx, migrationID, migrationErr)
		}
		return migrationErr
	}

	// Update tier metadata
	if err := m.manager.metadata.UpdateTier(ctx, candidate.Path, candidate.TargetTier); err != nil {
		// Rollback: delete from destination
		if delErr := dstBackend.Delete(ctx, candidate.Path); delErr != nil {
			m.logger.Error().Err(delErr).Str("path", candidate.Path).Msg("Failed to rollback destination file")
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

	// Wait for both operations to complete
	var readErr, writeErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			if readErr == nil {
				readErr = err
			} else {
				writeErr = err
			}
		}
	}

	if readErr != nil {
		return fmt.Errorf("streaming read failed: %w", readErr)
	}
	if writeErr != nil {
		return fmt.Errorf("streaming write failed: %w", writeErr)
	}

	return nil
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
