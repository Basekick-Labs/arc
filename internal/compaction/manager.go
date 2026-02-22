package compaction

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ErrCycleAlreadyRunning is returned when attempting to start a compaction cycle while one is already in progress
var ErrCycleAlreadyRunning = errors.New("compaction cycle already running")

// Manager orchestrates compaction jobs across all measurements
type Manager struct {
	StorageBackend  storage.Backend
	LockManager     *LockManager
	ManifestManager *ManifestManager

	// Configuration
	MinAgeHours   int
	MinFiles      int
	TargetSizeMB  int
	MaxConcurrent int
	TempDirectory string // Temp directory for compaction files
	MemoryLimit   string // DuckDB memory limit for subprocess (e.g., "8GB")

	// Sort key configuration (from ingest config)
	SortKeysConfig  map[string][]string // measurement -> sort keys
	DefaultSortKeys []string            // default sort keys

	// Tiers
	Tiers []Tier

	// Job history (jobs run in subprocess, so no activeJobs tracking)
	jobHistory []map[string]interface{}

	// Cycle management - prevents concurrent compaction cycles
	cycleRunning atomic.Bool
	cycleID      atomic.Int64

	// Metrics
	TotalJobsCompleted   int
	TotalJobsFailed      int
	TotalFilesCompacted  int
	TotalBytesSaved      int64
	TotalManifestsRecov  int // Number of manifests recovered

	// Callback invoked after a successful compaction job (in parent process).
	// Used to invalidate DuckDB and query caches after files are deleted.
	onCompactionComplete func()

	logger zerolog.Logger
	mu     sync.Mutex
}

// ManagerConfig holds configuration for creating a compaction manager
type ManagerConfig struct {
	StorageBackend  storage.Backend
	LockManager     *LockManager
	MinAgeHours     int
	MinFiles        int
	TargetSizeMB    int
	MaxConcurrent   int
	TempDirectory   string              // Temp directory for compaction files
	MemoryLimit     string              // DuckDB memory limit for subprocess (e.g., "8GB")
	SortKeysConfig  map[string][]string // Per-measurement sort keys from ingest config
	DefaultSortKeys []string            // Default sort keys from ingest config
	Tiers           []Tier
	Logger          zerolog.Logger
}

// NewManager creates a new compaction manager
func NewManager(cfg *ManagerConfig) *Manager {
	// Set defaults
	if cfg.MinAgeHours == 0 {
		cfg.MinAgeHours = 1
	}
	if cfg.MinFiles == 0 {
		cfg.MinFiles = 10
	}
	if cfg.TargetSizeMB == 0 {
		cfg.TargetSizeMB = 512
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 2
	}
	if cfg.TempDirectory == "" {
		cfg.TempDirectory = "./data/compaction"
	}

	// Set default sort keys if not provided
	sortKeysConfig := cfg.SortKeysConfig
	if sortKeysConfig == nil {
		sortKeysConfig = make(map[string][]string)
	}

	defaultSortKeys := cfg.DefaultSortKeys
	if defaultSortKeys == nil {
		defaultSortKeys = []string{"time"} // Default to time-only sorting
	}

	logger := cfg.Logger.With().Str("component", "compaction-manager").Logger()

	m := &Manager{
		StorageBackend:  cfg.StorageBackend,
		LockManager:     cfg.LockManager,
		ManifestManager: NewManifestManager(cfg.StorageBackend, logger),
		MinAgeHours:     cfg.MinAgeHours,
		MinFiles:        cfg.MinFiles,
		TargetSizeMB:    cfg.TargetSizeMB,
		MaxConcurrent:   cfg.MaxConcurrent,
		TempDirectory:   cfg.TempDirectory,
		MemoryLimit:     cfg.MemoryLimit,
		SortKeysConfig:  sortKeysConfig,
		DefaultSortKeys: defaultSortKeys,
		Tiers:           cfg.Tiers,
		jobHistory:      make([]map[string]interface{}, 0),
		logger:          logger,
	}

	// Log tier information
	if len(m.Tiers) > 0 {
		var enabledTiers []string
		for _, tier := range m.Tiers {
			if tier.IsEnabled() {
				enabledTiers = append(enabledTiers, tier.GetTierName())
			}
		}
		m.logger.Info().
			Strs("tiers", enabledTiers).
			Msg("Compaction manager initialized with tiers")
	} else {
		m.logger.Info().Msg("Compaction manager initialized (no tiers)")
	}

	return m
}

// SetOnCompactionComplete sets the callback invoked after each successful compaction job.
// This is used to invalidate DuckDB and query caches in the parent process after
// the compaction subprocess deletes old parquet files.
func (m *Manager) SetOnCompactionComplete(fn func()) {
	m.onCompactionComplete = fn
}

// FindCandidates finds partitions that are candidates for compaction across all databases
func (m *Manager) FindCandidates(ctx context.Context) ([]Candidate, error) {
	var candidates []Candidate

	m.logger.Info().Msg("Scanning for compaction candidates")

	// Discover all databases
	databases, err := m.listDatabases(ctx)
	if err != nil {
		return nil, err
	}

	m.logger.Info().Strs("databases", databases).Msg("Discovered databases for compaction")

	// Process each database
	for _, database := range databases {
		// List all measurements in this database
		measurements, err := m.listMeasurements(ctx, database)
		if err != nil {
			m.logger.Error().Err(err).Str("database", database).Msg("Failed to list measurements")
			continue
		}

		// Find candidates for each measurement across all tiers
		for _, meas := range measurements {
			for _, tier := range m.Tiers {
				if !tier.IsEnabled() {
					continue
				}

				tierCandidates, err := tier.FindCandidates(ctx, database, meas)
				if err != nil {
					m.logger.Error().Err(err).
						Str("database", database).
						Str("measurement", meas).
						Str("tier", tier.GetTierName()).
						Msg("Failed to find candidates")
					continue
				}

				candidates = append(candidates, tierCandidates...)
			}
		}
	}

	m.logger.Info().Int("candidates", len(candidates)).Msg("Found compaction candidates")
	return candidates, nil
}

// CompactPartition compacts a single partition using subprocess isolation.
// Running compaction in a subprocess ensures that DuckDB's jemalloc memory
// is fully released when the subprocess exits, preventing memory retention.
func (m *Manager) CompactPartition(ctx context.Context, candidate Candidate) error {
	lockKey := filepath.Join(candidate.Database, candidate.PartitionPath)

	// Try to acquire lock
	if !m.LockManager.AcquireLock(lockKey) {
		m.logger.Info().Str("partition", lockKey).Msg("Partition already locked, skipping")
		return nil
	}
	defer m.LockManager.ReleaseLock(lockKey)

	// Build subprocess config
	config := &SubprocessJobConfig{
		Database:      candidate.Database,
		Measurement:   candidate.Measurement,
		PartitionPath: candidate.PartitionPath,
		Files:         candidate.Files,
		Tier:          candidate.Tier,
		TargetSizeMB:  m.TargetSizeMB,
		TempDirectory: m.TempDirectory,
		MemoryLimit:   m.MemoryLimit,
		SortKeys:      m.GetSortKeys(candidate.Measurement),
		StorageType:   m.StorageBackend.Type(),
		StorageConfig: m.StorageBackend.ConfigJSON(),
	}

	// Build extra environment variables for subprocess (storage credentials)
	var extraEnv []string
	if azureBackend, ok := m.StorageBackend.(*storage.AzureBlobBackend); ok {
		if key := azureBackend.GetAccountKey(); key != "" {
			extraEnv = append(extraEnv, "AZURE_STORAGE_KEY="+key)
		}
	}
	if s3Backend, ok := m.StorageBackend.(*storage.S3Backend); ok {
		if accessKey := s3Backend.GetAccessKey(); accessKey != "" {
			extraEnv = append(extraEnv, "AWS_ACCESS_KEY_ID="+accessKey)
		}
		if secretKey := s3Backend.GetSecretKey(); secretKey != "" {
			extraEnv = append(extraEnv, "AWS_SECRET_ACCESS_KEY="+secretKey)
		}
	}

	// Run compaction in subprocess for memory isolation
	result, err := RunJobInSubprocess(ctx, config, m.logger, extraEnv...)

	// Always clean up temp directories for this partition after subprocess completes.
	// The subprocess has its own defer cleanup, but if it crashes or gets OOM-killed,
	// the defer never runs. This ensures cleanup happens from the parent process.
	// Job temp dirs are named: {database}_{partition_path_with_underscores}_{timestamp}
	partitionPrefix := candidate.Database + "_" + strings.ReplaceAll(candidate.PartitionPath, "/", "_") + "_"
	if entries, readErr := os.ReadDir(config.TempDirectory); readErr == nil {
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), partitionPrefix) {
				dirPath := filepath.Join(config.TempDirectory, entry.Name())
				if removeErr := os.RemoveAll(dirPath); removeErr != nil {
					m.logger.Debug().Err(removeErr).Str("dir", entry.Name()).Msg("Failed to cleanup subprocess temp directory")
				} else {
					m.logger.Debug().Str("dir", entry.Name()).Msg("Cleaned up subprocess temp directory")
				}
			}
		}
	}

	// Update metrics
	shouldInvalidateCache := err == nil && result.Success

	m.mu.Lock()
	if shouldInvalidateCache {
		m.TotalJobsCompleted++
		m.TotalFilesCompacted += result.FilesCompacted
		m.TotalBytesSaved += (result.BytesBefore - result.BytesAfter)
	} else {
		m.TotalJobsFailed++
	}

	// Build job stats for history
	jobStats := map[string]interface{}{
		"database":       candidate.Database,
		"measurement":    candidate.Measurement,
		"partition_path": candidate.PartitionPath,
		"tier":           candidate.Tier,
	}

	if result != nil {
		jobStats["files_compacted"] = result.FilesCompacted
		jobStats["bytes_before"] = result.BytesBefore
		jobStats["bytes_after"] = result.BytesAfter
		jobStats["success"] = result.Success
		if result.BytesBefore > 0 {
			jobStats["compression_ratio"] = 1 - float64(result.BytesAfter)/float64(result.BytesBefore)
		}
		if result.Error != "" {
			jobStats["error"] = result.Error
		}
	}
	if err != nil {
		jobStats["error"] = err.Error()
		jobStats["success"] = false
	}

	// Add to history
	m.jobHistory = append(m.jobHistory, jobStats)
	// Keep last 100 jobs
	if len(m.jobHistory) > 100 {
		m.jobHistory = m.jobHistory[len(m.jobHistory)-100:]
	}
	m.mu.Unlock()

	// Invalidate caches outside the lock — the callback performs IO (DuckDB Exec)
	// and should not block stat reads or other concurrent compaction goroutines.
	// The subprocess deleted old parquet files from storage, but DuckDB's
	// cache_httpfs and parquet_metadata_cache still reference them.
	if shouldInvalidateCache && m.onCompactionComplete != nil {
		m.onCompactionComplete()
	}

	// Return error if subprocess failed
	if err != nil {
		return err
	}
	if result != nil && !result.Success {
		return errors.New(result.Error)
	}
	return nil
}

// compactFilesAdaptively attempts compaction with adaptive batch sizing.
// If compaction fails with a recoverable error (segfault, OOM), it splits the batch
// in half and retries each half recursively. This allows large compactions to succeed
// by automatically finding a batch size that fits in memory.
//
// Parameters:
//   - ctx: context for cancellation
//   - candidate: the original candidate (used for metadata)
//   - files: the current subset of files to compact
//   - depth: recursion depth (0 = original batch, 1+ = split retries)
//   - stderr: stderr output from last failed attempt (for error classification)
//
// The algorithm:
//  1. Try to compact all files in the batch
//  2. If it fails with a recoverable error and batch size > minBatchSize:
//     - Split files in half
//     - Recursively compact each half
//  3. If batch size <= minBatchSize and still failing, give up
func (m *Manager) compactFilesAdaptively(ctx context.Context, candidate Candidate, files []string, depth int, stderr string) error {
	const maxDepth = 4     // Maximum split depth: 30 → 15 → 7 → 3 → minimum
	const minBatchSize = 2 // Don't split below 2 files

	// Check context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Safety check: too deep or too few files
	if depth > maxDepth {
		return fmt.Errorf("compaction failed: exceeded max retry depth %d", maxDepth)
	}
	if len(files) < minBatchSize {
		return fmt.Errorf("compaction failed: batch size %d below minimum %d", len(files), minBatchSize)
	}

	// Create candidate for this batch
	batchCandidate := candidate
	batchCandidate.Files = files

	// Log retry attempts
	if depth > 0 {
		m.logger.Info().
			Int("depth", depth).
			Int("file_count", len(files)).
			Str("partition", candidate.PartitionPath).
			Msg("Retrying compaction with reduced batch size")
	}

	// Attempt compaction
	err := m.CompactPartition(ctx, batchCandidate)
	if err == nil {
		// Success!
		if depth > 0 {
			m.logger.Info().
				Int("depth", depth).
				Int("file_count", len(files)).
				Str("partition", candidate.PartitionPath).
				Msg("Compaction succeeded after batch size reduction")
		}
		return nil
	}

	// Extract stderr from error message for classification
	// Error format from RunJobInSubprocess: "subprocess failed: %w (stderr: %s)"
	errStderr := stderr
	if errStr := err.Error(); strings.Contains(errStr, "(stderr:") {
		parts := strings.SplitN(errStr, "(stderr:", 2)
		if len(parts) > 1 {
			errStderr = strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")
		}
	}

	// Classify the error
	recoverable, reason := ClassifySubprocessError(err, errStderr)

	if !recoverable {
		m.logger.Error().
			Err(err).
			Str("reason", reason).
			Str("partition", candidate.PartitionPath).
			Msg("Compaction failed with non-recoverable error")
		return err
	}

	// Check if we can split further
	if len(files) <= minBatchSize {
		m.logger.Error().
			Err(err).
			Str("reason", reason).
			Int("batch_size", len(files)).
			Str("partition", candidate.PartitionPath).
			Msg("Compaction failed at minimum batch size, cannot split further")
		return err
	}

	// Split batch in half and try each half
	mid := len(files) / 2
	firstHalf := files[:mid]
	secondHalf := files[mid:]

	m.logger.Warn().
		Int("depth", depth).
		Int("original_size", len(files)).
		Int("first_half", len(firstHalf)).
		Int("second_half", len(secondHalf)).
		Str("reason", reason).
		Str("partition", candidate.PartitionPath).
		Msg("Splitting batch after recoverable failure")

	// Try first half
	if err := m.compactFilesAdaptively(ctx, candidate, firstHalf, depth+1, errStderr); err != nil {
		return fmt.Errorf("first half failed: %w", err)
	}

	// Try second half
	if err := m.compactFilesAdaptively(ctx, candidate, secondHalf, depth+1, errStderr); err != nil {
		return fmt.Errorf("second half failed: %w", err)
	}

	return nil
}

// RunCompactionCycle runs one compaction cycle for all enabled tiers.
// Returns the cycle ID and an error if the cycle couldn't be started.
// Returns ErrCycleAlreadyRunning if a cycle is already in progress.
func (m *Manager) RunCompactionCycle(ctx context.Context) (int64, error) {
	// Collect all enabled tier names
	var tierNames []string
	for _, tier := range m.Tiers {
		if tier.IsEnabled() {
			tierNames = append(tierNames, tier.GetTierName())
		}
	}
	return m.RunCompactionCycleForTiers(ctx, tierNames)
}

// RunCompactionCycleForTiers runs a complete compaction cycle for specific tiers across all databases.
// tierNames must be non-empty - specify which tiers to run explicitly.
func (m *Manager) RunCompactionCycleForTiers(ctx context.Context, tierNames []string) (int64, error) {
	return m.runCycleInternal(ctx, nil, tierNames)
}

// RunCompactionCycleForDatabase runs a compaction cycle for a single database.
// tierNames must be non-empty - specify which tiers to run explicitly.
func (m *Manager) RunCompactionCycleForDatabase(ctx context.Context, database string, tierNames []string) (int64, error) {
	return m.runCycleInternal(ctx, []string{database}, tierNames)
}

// runCycleInternal is the shared implementation for compaction cycles.
// If filterDatabases is non-nil, only those databases are compacted; otherwise all databases are discovered.
func (m *Manager) runCycleInternal(ctx context.Context, filterDatabases []string, tierNames []string) (int64, error) {
	// Prevent concurrent compaction cycles using atomic compare-and-swap
	if !m.cycleRunning.CompareAndSwap(false, true) {
		m.logger.Warn().Msg("Compaction cycle already running, skipping")
		return 0, ErrCycleAlreadyRunning
	}
	defer m.cycleRunning.Store(false)

	cycleID := m.cycleID.Add(1)

	// Require explicit tier names
	if len(tierNames) == 0 {
		m.logger.Debug().Int64("cycle_id", cycleID).Msg("No tiers specified, skipping cycle")
		return cycleID, nil
	}

	logEvent := m.logger.Info().
		Int64("cycle_id", cycleID).
		Strs("tiers", tierNames)
	if filterDatabases != nil {
		logEvent = logEvent.Strs("databases", filterDatabases)
	}
	logEvent.Msg("Starting compaction cycle")

	// Run manifest recovery before starting new compactions
	// This ensures interrupted compactions from previous cycles are completed
	if m.ManifestManager != nil {
		recovered, err := m.ManifestManager.RecoverOrphanedManifests(ctx)
		if err != nil {
			m.logger.Warn().Err(err).Msg("Manifest recovery encountered errors")
		}
		if recovered > 0 {
			m.mu.Lock()
			m.TotalManifestsRecov += recovered
			m.mu.Unlock()
			m.logger.Info().Int("recovered", recovered).Msg("Recovered orphaned compaction manifests")
		}
	}

	// Build tier filter map for quick lookup
	tierFilter := make(map[string]bool)
	for _, name := range tierNames {
		tierFilter[name] = true
	}

	// Determine databases to compact
	var databases []string
	if filterDatabases != nil {
		databases = filterDatabases
	} else {
		// Pre-discover databases ONCE before processing tiers
		// This avoids redundant storage API calls when multiple tiers are enabled
		var err error
		databases, err = m.listDatabases(ctx)
		if err != nil {
			m.logger.Error().Err(err).Msg("Failed to list databases for compaction cycle")
			return cycleID, err
		}
	}

	// Build database -> measurements map to avoid repeated lookups
	dbMeasurements := make(map[string][]string)
	for _, database := range databases {
		measurements, err := m.listMeasurements(ctx, database)
		if err != nil {
			m.logger.Error().Err(err).
				Str("database", database).
				Msg("Failed to list measurements")
			continue
		}
		dbMeasurements[database] = measurements
	}

	// Process tiers sequentially to maintain hierarchy (hourly -> daily)
	// This ensures lower tiers complete before higher tiers run
	totalCandidates := 0
	totalErrors := 0

	for _, tier := range m.Tiers {
		if !tier.IsEnabled() {
			continue
		}

		tierName := tier.GetTierName()

		// Skip tier if not in filter
		if !tierFilter[tierName] {
			m.logger.Debug().
				Int64("cycle_id", cycleID).
				Str("tier", tierName).
				Msg("Skipping tier (not in filter)")
			continue
		}
		m.logger.Info().
			Int64("cycle_id", cycleID).
			Str("tier", tierName).
			Msg("Processing tier")

		// MEMORY OPTIMIZATION: Process candidates as they're found instead of accumulating all.
		// This prevents unbounded memory growth when there are millions of files.
		// Each measurement's candidates are processed and then eligible for GC.
		sem := make(chan struct{}, m.MaxConcurrent)
		var wg sync.WaitGroup
		var tierCandidateCount int
		var errCount int
		var errMu sync.Mutex

		for _, database := range databases {
			measurements := dbMeasurements[database]

			for _, meas := range measurements {
				// Check for cancellation between measurements
				select {
				case <-ctx.Done():
					m.logger.Info().
						Int64("cycle_id", cycleID).
						Str("tier", tierName).
						Msg("Tier processing cancelled")
					wg.Wait()
					return cycleID, ctx.Err()
				default:
				}

				candidates, err := tier.FindCandidates(ctx, database, meas)
				if err != nil {
					m.logger.Error().Err(err).
						Str("database", database).
						Str("measurement", meas).
						Str("tier", tierName).
						Msg("Failed to find candidates")
					continue
				}

				// Process this measurement's candidates immediately
				for _, candidate := range candidates {
					// Filter out files that are tracked by manifests (pending compaction)
					filteredCandidate, shouldProcess := m.filterCandidateFiles(ctx, candidate)
					if !shouldProcess {
						m.logger.Debug().
							Str("partition", candidate.PartitionPath).
							Msg("Skipping candidate: all files are tracked by manifests")
						continue
					}

					// Split large candidates into batches to prevent DuckDB segfaults
					// when processing too many files in a single read_parquet() call
					batches := SplitCandidateIntoBatches(filteredCandidate)
					if len(batches) > 1 {
						m.logger.Info().
							Str("partition", filteredCandidate.PartitionPath).
							Int("total_files", len(filteredCandidate.Files)).
							Int("batches", len(batches)).
							Msg("Splitting large candidate into batches")
					}

					tierCandidateCount += len(batches)

					wg.Add(1)
					sem <- struct{}{} // Acquire semaphore

					// Run all batches for the same partition sequentially within a single goroutine.
					// This prevents race conditions where batch N tries to compact files that were
					// already deleted by batch N-1. Different partitions can still run in parallel.
					go func(partitionBatches []Candidate, partition string) {
						defer wg.Done()
						defer func() { <-sem }() // Release semaphore

						for _, batch := range partitionBatches {
							// Use adaptive compaction with automatic batch splitting on failure
							if err := m.compactFilesAdaptively(ctx, batch, batch.Files, 0, ""); err != nil {
								m.logger.Error().Err(err).
									Str("partition", batch.PartitionPath).
									Str("tier", tierName).
									Int("batch", batch.BatchNumber).
									Int("total_batches", batch.TotalBatches).
									Int64("cycle_id", cycleID).
									Msg("Compaction failed")
								errMu.Lock()
								errCount++
								errMu.Unlock()
								// Continue with next batch - don't fail entire partition
							}
						}
					}(batches, candidate.PartitionPath)
				}
				// candidates slice is now eligible for GC after this iteration
			}
		}

		// Wait for all jobs in this tier to complete before moving to next tier
		wg.Wait()

		if tierCandidateCount == 0 {
			m.logger.Info().
				Int64("cycle_id", cycleID).
				Str("tier", tierName).
				Msg("No candidates found for tier")
			continue
		}

		totalCandidates += tierCandidateCount
		totalErrors += errCount

		m.logger.Info().
			Int64("cycle_id", cycleID).
			Str("tier", tierName).
			Int("total", tierCandidateCount).
			Int("succeeded", tierCandidateCount-errCount).
			Int("failed", errCount).
			Msg("Tier processing complete")
	}

	m.logger.Info().
		Int64("cycle_id", cycleID).
		Int("total_candidates", totalCandidates).
		Int("total_succeeded", totalCandidates-totalErrors).
		Int("total_failed", totalErrors).
		Msg("Compaction cycle complete")

	return cycleID, nil
}

// IsCycleRunning returns true if a compaction cycle is currently in progress
func (m *Manager) IsCycleRunning() bool {
	return m.cycleRunning.Load()
}

// GetCurrentCycleID returns the current or most recent cycle ID
func (m *Manager) GetCurrentCycleID() int64 {
	return m.cycleID.Load()
}

// listDatabases discovers all databases in storage
func (m *Manager) listDatabases(ctx context.Context) ([]string, error) {
	// MEMORY OPTIMIZATION: Use ListDirectories instead of List to avoid loading all file paths.
	// ListDirectories only reads top-level directory entries, not all files recursively.
	// This reduces memory from O(millions of files) to O(number of databases).
	if dirLister, ok := m.StorageBackend.(storage.DirectoryLister); ok {
		dirs, err := dirLister.ListDirectories(ctx, "")
		if err != nil {
			return nil, err
		}

		// Filter out hidden directories and special directories
		databases := make([]string, 0, len(dirs))
		for _, dir := range dirs {
			if dir != "" && !strings.HasPrefix(dir, ".") && dir != "compaction" {
				databases = append(databases, dir)
			}
		}
		return databases, nil
	}

	// Fallback for backends that don't implement DirectoryLister
	objects, err := m.StorageBackend.List(ctx, "")
	if err != nil {
		return nil, err
	}

	// Extract unique database names from paths
	databaseSet := make(map[string]struct{})
	for _, obj := range objects {
		// Path format: database/measurement/year/month/day/hour/file.parquet
		parts := strings.Split(obj, "/")
		if len(parts) >= 2 {
			database := parts[0]
			// Skip hidden directories and special files
			if database != "" && !strings.HasPrefix(database, ".") && database != "compaction" {
				databaseSet[database] = struct{}{}
			}
		}
	}

	// Convert to slice
	databases := make([]string, 0, len(databaseSet))
	for db := range databaseSet {
		databases = append(databases, db)
	}

	return databases, nil
}

// listMeasurements lists all measurements in storage for a given database
func (m *Manager) listMeasurements(ctx context.Context, database string) ([]string, error) {
	// MEMORY OPTIMIZATION: Use ListDirectories instead of List to avoid loading all file paths.
	// ListDirectories only reads directory entries at one level, not all files recursively.
	// This reduces memory from O(files in database) to O(number of measurements).
	if dirLister, ok := m.StorageBackend.(storage.DirectoryLister); ok {
		measurements, err := dirLister.ListDirectories(ctx, database+"/")
		if err != nil {
			return nil, err
		}

		m.logger.Debug().
			Str("database", database).
			Strs("measurements", measurements).
			Msg("Found measurements")

		return measurements, nil
	}

	// Fallback for backends that don't implement DirectoryLister
	prefix := database + "/"
	objects, err := m.StorageBackend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// Extract unique measurement names from paths
	measurementSet := make(map[string]struct{})
	for _, obj := range objects {
		// Path format: database/measurement/year/month/day/hour/file.parquet
		parts := strings.Split(obj, "/")
		if len(parts) >= 6 {
			// parts[0] is database, parts[1] is measurement
			measurement := parts[1]
			if measurement != "" && measurement != "." {
				measurementSet[measurement] = struct{}{}
			}
		}
	}

	// Convert to slice
	measurements := make([]string, 0, len(measurementSet))
	for meas := range measurementSet {
		measurements = append(measurements, meas)
	}

	m.logger.Debug().
		Str("database", database).
		Strs("measurements", measurements).
		Msg("Found measurements")

	return measurements, nil
}

// GetSortKeys returns sort keys for a measurement.
// Checks measurement-specific config first, then falls back to default.
func (m *Manager) GetSortKeys(measurement string) []string {
	// Check measurement-specific config
	if keys, exists := m.SortKeysConfig[measurement]; exists {
		return keys
	}

	// Use default (e.g., ["time"])
	return m.DefaultSortKeys
}

// filterCandidateFiles removes files that are tracked by manifests from a candidate.
// Returns the filtered candidate and whether it should still be processed.
func (m *Manager) filterCandidateFiles(ctx context.Context, candidate Candidate) (Candidate, bool) {
	if m.ManifestManager == nil {
		return candidate, len(candidate.Files) > 0
	}

	filesInManifests, err := m.ManifestManager.GetFilesInManifests(ctx)
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to get files in manifests, skipping partition to avoid re-compaction")
		return candidate, false
	}

	if len(filesInManifests) == 0 {
		return candidate, len(candidate.Files) > 0
	}

	// Filter out files that are in manifests
	filteredFiles := make([]string, 0, len(candidate.Files))
	var skipped int
	for _, f := range candidate.Files {
		if _, inManifest := filesInManifests[f]; inManifest {
			skipped++
			continue
		}
		filteredFiles = append(filteredFiles, f)
	}

	if skipped > 0 {
		m.logger.Debug().
			Str("partition", candidate.PartitionPath).
			Int("skipped", skipped).
			Int("remaining", len(filteredFiles)).
			Msg("Filtered files tracked by manifests")
	}

	candidate.Files = filteredFiles
	candidate.FileCount = len(filteredFiles)

	return candidate, len(filteredFiles) > 0
}

// Stats returns compaction statistics
func (m *Manager) Stats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := map[string]interface{}{
		"total_jobs_completed":    m.TotalJobsCompleted,
		"total_jobs_failed":       m.TotalJobsFailed,
		"total_files_compacted":   m.TotalFilesCompacted,
		"total_bytes_saved":       m.TotalBytesSaved,
		"total_bytes_saved_mb":    float64(m.TotalBytesSaved) / 1024 / 1024,
		"total_manifests_recover": m.TotalManifestsRecov,
		"cycle_running":           m.cycleRunning.Load(),
		"current_cycle_id":        m.cycleID.Load(),
	}

	// Add recent jobs (last 10)
	recentJobs := m.jobHistory
	if len(recentJobs) > 10 {
		recentJobs = recentJobs[len(recentJobs)-10:]
	}
	stats["recent_jobs"] = recentJobs

	// Add tier stats
	if len(m.Tiers) > 0 {
		tierStats := make([]map[string]interface{}, len(m.Tiers))
		for i, tier := range m.Tiers {
			tierStats[i] = tier.GetStats()
		}
		stats["tiers"] = tierStats
	}

	return stats
}

// CleanupOrphanedTempDirs removes orphaned temp directories from previous runs.
// This handles cleanup after pod crashes where the defer cleanup didn't run.
// Call this on startup before running compaction cycles.
func (m *Manager) CleanupOrphanedTempDirs() error {
	entries, err := os.ReadDir(m.TempDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet
		}
		return fmt.Errorf("failed to read temp directory: %w", err)
	}

	var cleaned int
	for _, entry := range entries {
		if entry.IsDir() {
			path := filepath.Join(m.TempDirectory, entry.Name())
			if err := os.RemoveAll(path); err != nil {
				m.logger.Warn().Err(err).Str("dir", entry.Name()).Msg("Failed to cleanup orphaned temp directory")
			} else {
				m.logger.Info().Str("dir", entry.Name()).Msg("Cleaned up orphaned temp directory")
				cleaned++
			}
		}
	}

	if cleaned > 0 {
		m.logger.Info().Int("count", cleaned).Msg("Orphaned temp directories cleaned")
	}
	return nil
}

// LockManager manages locks for compaction partitions
type LockManager struct {
	locks map[string]bool
	mu    sync.Mutex
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]bool),
	}
}

// AcquireLock attempts to acquire a lock for a partition
func (l *LockManager) AcquireLock(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locks[key] {
		return false // Already locked
	}

	l.locks[key] = true
	return true
}

// ReleaseLock releases a lock for a partition
func (l *LockManager) ReleaseLock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.locks, key)
}

// IsLocked checks if a partition is locked
func (l *LockManager) IsLocked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.locks[key]
}
