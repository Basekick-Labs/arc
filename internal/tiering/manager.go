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

	// onMigrationComplete, when set, runs after a migration cycle that moved
	// or deleted at least one tier file. The query layer uses it to invalidate
	// pruner and SQL transform caches, whose cached partition paths go stale
	// the moment a file changes tier (a cached hot hour glob that no longer
	// matches any file makes DuckDB error on the next query within the cache
	// TTL). Guarded by callbackMu: it is wired from main after the scheduler
	// goroutine already runs.
	callbackMu          sync.Mutex
	onMigrationComplete func()

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

	// Cleanup old migration history records
	if err := m.cleanupOldMigrations(ctx); err != nil {
		m.logger.Warn().Err(err).Msg("Migration history cleanup failed")
	}

	duration := time.Since(startTime)
	m.logger.Info().
		Int("migrated", totalMigrated).
		Int("errors", totalErrors).
		Dur("duration", duration).
		Msg("Migration cycle completed")

	m.notifyMigrationComplete(totalMigrated, orphansDeleted)

	return nil
}

// notifyMigrationComplete fires the registered callback when a cycle changed
// which files live on which tier. Orphan deletions count: crash-recovery
// reconciliation can delete hot files in a cycle that migrated nothing, and a
// cached pruned hot glob matching zero files makes DuckDB error until TTL.
func (m *Manager) notifyMigrationComplete(migrated, orphansDeleted int) {
	if migrated <= 0 && orphansDeleted <= 0 {
		return
	}
	m.callbackMu.Lock()
	fn := m.onMigrationComplete
	m.callbackMu.Unlock()
	if fn != nil {
		fn()
	}
}

// SetOnMigrationComplete registers a callback invoked after any migration
// cycle that moved or deleted tier files. See the field comment for why the
// query caches need it. Safe to call after Start: the scheduler goroutine
// reads the callback under the same lock.
func (m *Manager) SetOnMigrationComplete(fn func()) {
	m.callbackMu.Lock()
	m.onMigrationComplete = fn
	m.callbackMu.Unlock()
}

// cleanupOldMigrations deletes migration history records older than the configured retention.
//
// This layer owns retention *policy*: a value of 0 (unset) defaults to 90 days,
// and a negative value explicitly disables cleanup. MetadataStore.CleanupOldMigrations
// applies its own <= 0 guard as a defensive contract, but by the time we call it here
// retentionDays is always > 0.
func (m *Manager) cleanupOldMigrations(ctx context.Context) error {
	retentionDays := m.config.MigrationHistoryRetentionDays
	if retentionDays == 0 {
		retentionDays = 90 // Default: keep 90 days of migration history
	} else if retentionDays < 0 {
		return nil // Negative value explicitly disables cleanup
	}

	deleted, err := m.metadata.CleanupOldMigrations(ctx, retentionDays)
	if err != nil {
		return err
	}

	if deleted > 0 {
		m.logger.Debug().
			Int64("deleted", deleted).
			Int("retention_days", retentionDays).
			Msg("Migration history cleanup completed")
	}

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

	// One query for the cold path set instead of a point lookup per scanned
	// file: the scan walks every hot file, while the cold set holds only
	// migrated daily files. Fail the scan on error rather than risk the
	// downgrade the check exists to prevent.
	coldFiles, err := m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		return nil, fmt.Errorf("failed to load cold tier paths: %w", err)
	}
	coldPaths := make(map[string]bool, len(coldFiles))
	for _, f := range coldFiles {
		coldPaths[f.Path] = true
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

		// Never downgrade a cold row back to hot (#683): the scan lists the
		// HOT backend, and a hot file whose row already says cold is exactly
		// the orphan that ReconcileOrphanedFiles deletes after a failed
		// post-migration cleanup. Re-registering it as hot would reset
		// migrated_at, hide it from reconciliation, and re-upload it to cold
		// every cycle.
		if coldPaths[obj.Path] {
			result.FilesSkipped++
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

// parseFilePath parses a storage path to extract database, measurement, and
// partition time. Accepted shapes (hour- and day-level, plain and
// spoke-namespaced): {db}/{meas}/Y/M/D[/H]/{file}.parquet and
// {spoke}/{db}/{meas}/Y/M/D[/H]/{file}.parquet.
func (m *Manager) parseFilePath(path string) (*filePathInfo, error) {
	// Normalize path separators
	path = filepath.ToSlash(path)
	parts := strings.Split(path, "/")

	// Parse by TAIL shape, not absolute segment counts (#619 precedent:
	// compaction's isHourLevelFile) — a spoke-namespace pseudo-database adds
	// a path level, so hour-level files have 7 parts plain and 8 under a
	// spoke, day-level 6 and 7. Validation thresholds mirror
	// internal/compaction/daily.go isHourLevelFile; keep them in sync.
	//   hour-level tail: {year}/{month}/{day}/{hour}/{file}.parquet
	//   day-level tail:  {year}/{month}/{day}/{file}.parquet
	validDate := func(y, mo, d string) (time.Time, bool) {
		yn, err := strconv.Atoi(y)
		if err != nil || len(y) != 4 {
			return time.Time{}, false
		}
		mn, err := strconv.Atoi(mo)
		if err != nil || mn < 1 || mn > 12 {
			return time.Time{}, false
		}
		dn, err := strconv.Atoi(d)
		if err != nil || dn < 1 || dn > 31 {
			return time.Time{}, false
		}
		return time.Date(yn, time.Month(mn), dn, 0, 0, 0, 0, time.UTC), true
	}

	var partitionTime time.Time
	var prefix []string
	n := len(parts)
	if n >= 7 {
		if day, ok := validDate(parts[n-5], parts[n-4], parts[n-3]); ok {
			if hn, err := strconv.Atoi(parts[n-2]); err == nil && hn >= 0 && hn <= 23 {
				partitionTime = day.Add(time.Duration(hn) * time.Hour)
				prefix = parts[:n-5]
			}
		}
	}
	if prefix == nil && n >= 6 {
		if day, ok := validDate(parts[n-4], parts[n-3], parts[n-2]); ok {
			// Day-level files carry no hour segment; their partition time is
			// the start of the day, matching hourly rows' hour-start convention.
			partitionTime = day
			prefix = parts[:n-4]
		}
	}
	if prefix == nil {
		return nil, fmt.Errorf("no year/month/day[/hour] partition tail in path: %s", path)
	}

	// The prefix is {database}/{measurement} (2 parts) or a spoke namespace
	// {spoke}/{db}/{measurement} (3 parts). For spoke files, register
	// (database=spoke, measurement=spoke-db): that is the split the QUERY
	// layer produces for spoke data (FROM "rocket-01".telemetry globs
	// rocket-01/telemetry/**), so tier metadata stays query-visible. Deeper
	// nesting is not a feature (edge-sync forbids relaying); reject it.
	var database, measurement string
	switch len(prefix) {
	case 2:
		database, measurement = prefix[0], prefix[1]
	case 3:
		database, measurement = prefix[0], prefix[1]
	default:
		return nil, fmt.Errorf("unsupported path depth (%d prefix segments): %s", len(prefix), path)
	}

	// Validate no path traversal in database or measurement names
	if strings.Contains(database, "..") || strings.ContainsAny(database, "\\") {
		return nil, fmt.Errorf("invalid database name in path: %s", database)
	}
	if strings.Contains(measurement, "..") || strings.ContainsAny(measurement, "\\") {
		return nil, fmt.Errorf("invalid measurement name in path: %s", measurement)
	}

	return &filePathInfo{
		Database:      database,
		Measurement:   measurement,
		PartitionTime: partitionTime,
	}, nil
}
