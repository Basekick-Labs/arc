package tiering

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Manager orchestrates tiered storage operations
type Manager struct {
	// Storage backends
	hotBackend  storage.Backend
	coldBackend storage.Backend

	// Data stores
	metadata *MetadataStore
	policies *PolicyStore

	// Configuration
	config *config.TieredStorageConfig

	// License client for feature gating
	licenseClient *license.Client

	// Components
	migrator  *Migrator
	scheduler *Scheduler
	router    *Router

	// State
	running atomic.Bool
	stopCh  chan struct{}

	logger zerolog.Logger
	mu     sync.RWMutex
}

// ManagerConfig holds configuration for creating a tiering manager
type ManagerConfig struct {
	// Storage backends
	HotBackend  storage.Backend // Required: local storage for hot tier
	ColdBackend storage.Backend // Optional: S3/Azure for cold tier

	// Database connection for metadata
	DB *sql.DB

	// Configuration
	Config *config.TieredStorageConfig

	// License client
	LicenseClient *license.Client

	// Logger
	Logger zerolog.Logger
}

// NewManager creates a new tiering manager
func NewManager(cfg *ManagerConfig) (*Manager, error) {
	logger := cfg.Logger.With().Str("component", "tiering-manager").Logger()

	// Validate configuration
	if cfg.HotBackend == nil {
		return nil, fmt.Errorf("hot backend is required")
	}
	if cfg.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("configuration is required")
	}

	// Validate license
	if cfg.LicenseClient == nil {
		return nil, fmt.Errorf("license client is required for tiered storage")
	}
	if !cfg.LicenseClient.CanUseTieredStorage() {
		return nil, fmt.Errorf("valid license with tiered_storage feature required")
	}

	// Create metadata store
	metadata, err := NewMetadataStore(cfg.DB, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	// Create policy store
	policies, err := NewPolicyStore(cfg.DB, cfg.Config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create policy store: %w", err)
	}

	m := &Manager{
		hotBackend:    cfg.HotBackend,
		coldBackend:   cfg.ColdBackend,
		metadata:      metadata,
		policies:      policies,
		config:        cfg.Config,
		licenseClient: cfg.LicenseClient,
		stopCh:        make(chan struct{}),
		logger:        logger,
	}

	// Create migrator
	m.migrator = NewMigrator(&MigratorConfig{
		Manager:       m,
		MaxConcurrent: cfg.Config.MigrationMaxConcurrent,
		BatchSize:     cfg.Config.MigrationBatchSize,
		Logger:        logger,
	})

	// Create scheduler
	m.scheduler = NewScheduler(&SchedulerConfig{
		Manager:  m,
		Schedule: cfg.Config.MigrationSchedule,
		Logger:   logger,
	})

	// Create router for query routing across tiers
	m.router = NewRouter(m, logger)

	logger.Info().
		Bool("cold_enabled", cfg.ColdBackend != nil && cfg.Config.Cold.Enabled).
		Str("schedule", cfg.Config.MigrationSchedule).
		Msg("Tiering manager created")

	return m, nil
}

// Start starts the tiering manager and scheduler
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running.Load() {
		return fmt.Errorf("tiering manager already running")
	}

	// Verify license before starting
	if !m.licenseClient.CanUseTieredStorage() {
		m.logger.Warn().Msg("Valid license required for tiered storage - not starting scheduler")
		return nil
	}

	// Start scheduler
	if err := m.scheduler.Start(); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	m.running.Store(true)
	m.logger.Info().Msg("Tiering manager started")
	return nil
}

// Stop stops the tiering manager and scheduler
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running.Load() {
		return nil
	}

	close(m.stopCh)
	m.scheduler.Stop()
	m.running.Store(false)

	m.logger.Info().Msg("Tiering manager stopped")
	return nil
}

// IsRunning returns true if the manager is running
func (m *Manager) IsRunning() bool {
	return m.running.Load()
}

// RunMigrationCycle runs a single migration cycle
func (m *Manager) RunMigrationCycle(ctx context.Context) error {
	// Check license before each cycle
	if !m.licenseClient.CanUseTieredStorage() {
		m.logger.Warn().Msg("Valid license required - skipping migration cycle")
		return nil
	}

	m.logger.Info().Msg("Starting migration cycle")
	startTime := time.Now()

	// Scan and register any new files before migration
	scanResult, err := m.ScanAndRegisterFiles(ctx)
	if err != nil {
		m.logger.Warn().Err(err).Msg("File scan failed, continuing with existing metadata")
	} else {
		m.logger.Info().
			Int("scanned", scanResult.FilesScanned).
			Int("registered", scanResult.FilesRegistered).
			Msg("Pre-migration scan completed")
	}

	var totalMigrated int
	var totalErrors int

	// Hot -> Cold migrations (2-tier system)
	if m.coldBackend != nil && m.config.Cold.Enabled {
		migrated, errors := m.migrator.MigrateTier(ctx, TierHot, TierCold)
		totalMigrated += migrated
		totalErrors += errors
	}

	// Reconcile orphaned hot files (files tracked as cold but still in hot storage)
	orphansFound, orphansDeleted, orphanErrors := m.migrator.ReconcileOrphanedFiles(ctx)
	totalErrors += orphanErrors
	if orphansFound > 0 || orphanErrors > 0 {
		m.logger.Info().
			Int("found", orphansFound).
			Int("deleted", orphansDeleted).
			Int("errors", orphanErrors).
			Msg("Orphaned hot file reconciliation completed")
	}

	duration := time.Since(startTime)
	m.logger.Info().
		Int("migrated", totalMigrated).
		Int("errors", totalErrors).
		Dur("duration", duration).
		Msg("Migration cycle completed")

	return nil
}

// TriggerMigration triggers a manual migration cycle
func (m *Manager) TriggerMigration(ctx context.Context) error {
	return m.RunMigrationCycle(ctx)
}

// GetBackendForTier returns the storage backend for a tier
func (m *Manager) GetBackendForTier(tier Tier) storage.Backend {
	switch tier {
	case TierHot:
		return m.hotBackend
	case TierCold:
		return m.coldBackend
	default:
		return m.hotBackend
	}
}

// GetMetadata returns the metadata store
func (m *Manager) GetMetadata() *MetadataStore {
	return m.metadata
}

// GetPolicies returns the policy store
func (m *Manager) GetPolicies() *PolicyStore {
	return m.policies
}

// GetConfig returns the tiered storage configuration
func (m *Manager) GetConfig() *config.TieredStorageConfig {
	return m.config
}

// GetRouter returns the tier router for query routing
func (m *Manager) GetRouter() *Router {
	return m.router
}

// RecordNewFile records a newly ingested file in the hot tier
func (m *Manager) RecordNewFile(ctx context.Context, file *FileMetadata) error {
	file.Tier = TierHot
	if file.CreatedAt.IsZero() {
		file.CreatedAt = time.Now().UTC()
	}
	return m.metadata.RecordFile(ctx, file)
}

// DeleteFile removes a file from tier metadata (called when file is deleted)
func (m *Manager) DeleteFile(ctx context.Context, path string) error {
	return m.metadata.DeleteFile(ctx, path)
}

// GetStatus returns the current tiering status
func (m *Manager) GetStatus(ctx context.Context) (*StatusResponse, error) {
	status := &StatusResponse{
		Enabled:      m.config.Enabled,
		LicenseValid: m.licenseClient.CanUseTieredStorage(),
	}

	if !status.LicenseValid {
		status.Reason = "license required"
		return status, nil
	}

	// Get tier stats
	tierStats, err := m.metadata.GetTierStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tier stats: %w", err)
	}

	status.Tiers = make(map[string]TierStats)

	// Hot tier
	hotStats := tierStats[TierHot]
	hotStats.Enabled = true
	hotStats.Backend = "local"
	status.Tiers["hot"] = hotStats

	// Cold tier
	coldStats := tierStats[TierCold]
	coldStats.Enabled = m.config.Cold.Enabled && m.coldBackend != nil
	coldStats.Backend = m.config.Cold.Backend
	status.Tiers["cold"] = coldStats

	// Scheduler status
	status.Scheduler = m.scheduler.Status()

	return status, nil
}

// GetEffectivePolicy returns the effective policy for a database
func (m *Manager) GetEffectivePolicy(ctx context.Context, database string) *EffectivePolicy {
	return m.policies.GetEffective(ctx, database)
}

// IsHotOnly returns true if the database should stay in hot tier only
func (m *Manager) IsHotOnly(ctx context.Context, database string) bool {
	return m.policies.IsHotOnly(ctx, database)
}

// ScanResult holds the results of a file scan operation
type ScanResult struct {
	FilesScanned    int `json:"files_scanned"`
	FilesRegistered int `json:"files_registered"`
	FilesSkipped    int `json:"files_skipped"`
	Errors          int `json:"errors"`
}

// ScanAndRegisterFiles scans the hot tier storage and registers all existing parquet files
// Path format: {database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}.parquet
func (m *Manager) ScanAndRegisterFiles(ctx context.Context) (*ScanResult, error) {
	result := &ScanResult{}

	// Check if hot backend supports ListObjects
	objectLister, ok := m.hotBackend.(storage.ObjectLister)
	if !ok {
		return nil, fmt.Errorf("hot backend does not support ListObjects")
	}

	m.logger.Info().Msg("Starting file scan for tiering registration")

	// List all objects in the storage root
	objects, err := objectLister.ListObjects(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	for _, obj := range objects {
		// Only process parquet files
		if !strings.HasSuffix(obj.Path, ".parquet") {
			continue
		}

		result.FilesScanned++

		// Parse the path to extract database, measurement, and partition time
		// Format: {database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}.parquet
		fileInfo, err := m.parseFilePath(obj.Path)
		if err != nil {
			m.logger.Warn().
				Str("path", obj.Path).
				Err(err).
				Msg("Failed to parse file path, skipping")
			result.Errors++
			continue
		}

		// Create file metadata
		file := &FileMetadata{
			Path:          obj.Path,
			Database:      fileInfo.Database,
			Measurement:   fileInfo.Measurement,
			PartitionTime: fileInfo.PartitionTime,
			Tier:          TierHot,
			SizeBytes:     obj.Size,
			CreatedAt:     obj.LastModified,
		}

		// Record the file (uses UPSERT, so safe to re-run)
		if err := m.metadata.RecordFile(ctx, file); err != nil {
			m.logger.Warn().
				Str("path", obj.Path).
				Err(err).
				Msg("Failed to record file, skipping")
			result.Errors++
			continue
		}

		result.FilesRegistered++

		// Log progress every 100 files
		if result.FilesScanned%100 == 0 {
			m.logger.Info().
				Int("scanned", result.FilesScanned).
				Int("registered", result.FilesRegistered).
				Msg("Scan progress")
		}
	}

	m.logger.Info().
		Int("scanned", result.FilesScanned).
		Int("registered", result.FilesRegistered).
		Int("skipped", result.FilesSkipped).
		Int("errors", result.Errors).
		Msg("File scan completed")

	return result, nil
}

// filePathInfo holds parsed information from a file path
type filePathInfo struct {
	Database      string
	Measurement   string
	PartitionTime time.Time
}

// parseFilePath parses a storage path to extract database, measurement, and partition time
// Expected format: {database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}.parquet
func (m *Manager) parseFilePath(path string) (*filePathInfo, error) {
	// Normalize path separators
	path = filepath.ToSlash(path)
	parts := strings.Split(path, "/")

	// Need at least: database/measurement/year/month/day/hour/filename.parquet
	if len(parts) < 7 {
		return nil, fmt.Errorf("path too short: %s", path)
	}

	database := parts[0]
	measurement := parts[1]

	// Validate no path traversal in database or measurement names
	if strings.Contains(database, "..") || strings.ContainsAny(database, "/\\") {
		return nil, fmt.Errorf("invalid database name in path: %s", database)
	}
	if strings.Contains(measurement, "..") || strings.ContainsAny(measurement, "/\\") {
		return nil, fmt.Errorf("invalid measurement name in path: %s", measurement)
	}

	// Parse year, month, day, hour
	year, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid year in path: %s", parts[2])
	}

	month, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, fmt.Errorf("invalid month in path: %s", parts[3])
	}

	day, err := strconv.Atoi(parts[4])
	if err != nil {
		return nil, fmt.Errorf("invalid day in path: %s", parts[4])
	}

	hour, err := strconv.Atoi(parts[5])
	if err != nil {
		return nil, fmt.Errorf("invalid hour in path: %s", parts[5])
	}

	// Construct partition time (UTC)
	partitionTime := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)

	return &filePathInfo{
		Database:      database,
		Measurement:   measurement,
		PartitionTime: partitionTime,
	}, nil
}
