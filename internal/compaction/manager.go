package compaction

import (
	"context"
	"errors"
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
	StorageBackend storage.Backend
	LockManager    *LockManager

	// Configuration
	MinAgeHours   int
	MinFiles      int
	TargetSizeMB  int
	MaxConcurrent int
	TempDirectory string // Temp directory for compaction files

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
	TotalJobsCompleted  int
	TotalJobsFailed     int
	TotalFilesCompacted int
	TotalBytesSaved     int64

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

	m := &Manager{
		StorageBackend:  cfg.StorageBackend,
		LockManager:     cfg.LockManager,
		MinAgeHours:     cfg.MinAgeHours,
		MinFiles:        cfg.MinFiles,
		TargetSizeMB:    cfg.TargetSizeMB,
		MaxConcurrent:   cfg.MaxConcurrent,
		TempDirectory:   cfg.TempDirectory,
		SortKeysConfig:  sortKeysConfig,
		DefaultSortKeys: defaultSortKeys,
		Tiers:           cfg.Tiers,
		jobHistory:      make([]map[string]interface{}, 0),
		logger:          cfg.Logger.With().Str("component", "compaction-manager").Logger(),
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
		SortKeys:      m.GetSortKeys(candidate.Measurement),
		StorageType:   m.StorageBackend.Type(),
		StorageConfig: m.StorageBackend.ConfigJSON(),
	}

	// Run compaction in subprocess for memory isolation
	result, err := RunJobInSubprocess(ctx, config, m.logger)

	// Update metrics
	m.mu.Lock()
	defer m.mu.Unlock()

	if err == nil && result.Success {
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

	// Return error if subprocess failed
	if err != nil {
		return err
	}
	if result != nil && !result.Success {
		return errors.New(result.Error)
	}
	return nil
}

// RunCompactionCycle runs one compaction cycle.
// Returns the cycle ID and an error if the cycle couldn't be started.
// Returns ErrCycleAlreadyRunning if a cycle is already in progress.
func (m *Manager) RunCompactionCycle(ctx context.Context) (int64, error) {
	// Prevent concurrent compaction cycles using atomic compare-and-swap
	if !m.cycleRunning.CompareAndSwap(false, true) {
		m.logger.Warn().Msg("Compaction cycle already running, skipping")
		return 0, ErrCycleAlreadyRunning
	}
	defer m.cycleRunning.Store(false)

	cycleID := m.cycleID.Add(1)
	m.logger.Info().Int64("cycle_id", cycleID).Msg("Starting compaction cycle")

	// Process tiers sequentially to maintain hierarchy (hourly -> daily)
	// This ensures lower tiers complete before higher tiers run
	totalCandidates := 0
	totalErrors := 0

	for _, tier := range m.Tiers {
		if !tier.IsEnabled() {
			continue
		}

		tierName := tier.GetTierName()
		m.logger.Info().
			Int64("cycle_id", cycleID).
			Str("tier", tierName).
			Msg("Processing tier")

		// Find candidates for this tier
		databases, err := m.listDatabases(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("tier", tierName).Msg("Failed to list databases")
			continue
		}

		var tierCandidates []Candidate
		for _, database := range databases {
			measurements, err := m.listMeasurements(ctx, database)
			if err != nil {
				m.logger.Error().Err(err).
					Str("database", database).
					Str("tier", tierName).
					Msg("Failed to list measurements")
				continue
			}

			for _, meas := range measurements {
				candidates, err := tier.FindCandidates(ctx, database, meas)
				if err != nil {
					m.logger.Error().Err(err).
						Str("database", database).
						Str("measurement", meas).
						Str("tier", tierName).
						Msg("Failed to find candidates")
					continue
				}
				tierCandidates = append(tierCandidates, candidates...)
			}
		}

		if len(tierCandidates) == 0 {
			m.logger.Info().
				Int64("cycle_id", cycleID).
				Str("tier", tierName).
				Msg("No candidates found for tier")
			continue
		}

		m.logger.Info().
			Int64("cycle_id", cycleID).
			Str("tier", tierName).
			Int("candidates", len(tierCandidates)).
			Msg("Found candidates for tier")

		// Process tier candidates with concurrency limit
		sem := make(chan struct{}, m.MaxConcurrent)
		var wg sync.WaitGroup
		var errCount int
		var errMu sync.Mutex

		for _, candidate := range tierCandidates {
			select {
			case <-ctx.Done():
				m.logger.Info().
					Int64("cycle_id", cycleID).
					Str("tier", tierName).
					Msg("Tier processing cancelled")
				wg.Wait() // Wait for running jobs
				return cycleID, ctx.Err()
			default:
			}

			wg.Add(1)
			sem <- struct{}{} // Acquire semaphore

			go func(c Candidate) {
				defer wg.Done()
				defer func() { <-sem }() // Release semaphore

				if err := m.CompactPartition(ctx, c); err != nil {
					m.logger.Error().Err(err).
						Str("partition", c.PartitionPath).
						Str("tier", tierName).
						Int64("cycle_id", cycleID).
						Msg("Compaction failed")
					errMu.Lock()
					errCount++
					errMu.Unlock()
				}
			}(candidate)
		}

		// Wait for all jobs in this tier to complete before moving to next tier
		wg.Wait()

		totalCandidates += len(tierCandidates)
		totalErrors += errCount

		m.logger.Info().
			Int64("cycle_id", cycleID).
			Str("tier", tierName).
			Int("total", len(tierCandidates)).
			Int("succeeded", len(tierCandidates)-errCount).
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
	// List all top-level objects
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
	// List from the database prefix
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

// Stats returns compaction statistics
func (m *Manager) Stats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := map[string]interface{}{
		"total_jobs_completed":  m.TotalJobsCompleted,
		"total_jobs_failed":     m.TotalJobsFailed,
		"total_files_compacted": m.TotalFilesCompacted,
		"total_bytes_saved":     m.TotalBytesSaved,
		"total_bytes_saved_mb":  float64(m.TotalBytesSaved) / 1024 / 1024,
		"cycle_running":         m.cycleRunning.Load(),
		"current_cycle_id":      m.cycleID.Load(),
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
