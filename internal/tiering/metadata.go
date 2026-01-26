package tiering

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// tierCacheEntry holds cached tier information with expiration
type tierCacheEntry struct {
	tiers     map[Tier]bool
	expiresAt time.Time
}

// tierCacheTTL is how long tier lookups are cached (30 seconds)
const tierCacheTTL = 30 * time.Second

// MetadataStore manages tier file metadata in SQLite
type MetadataStore struct {
	db     *sql.DB
	logger zerolog.Logger
	mu     sync.RWMutex

	// Cache for GetTiersForMeasurement - keyed by "database/measurement"
	tierCache   map[string]*tierCacheEntry
	tierCacheMu sync.RWMutex
}

// NewMetadataStore creates a new metadata store using the provided SQLite connection
func NewMetadataStore(db *sql.DB, logger zerolog.Logger) (*MetadataStore, error) {
	store := &MetadataStore{
		db:        db,
		logger:    logger.With().Str("component", "tiering-metadata").Logger(),
		tierCache: make(map[string]*tierCacheEntry),
	}

	if err := store.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize tiering schema: %w", err)
	}

	return store, nil
}

// initSchema creates the required tables if they don't exist
func (s *MetadataStore) initSchema() error {
	schema := `
	-- File tier tracking
	CREATE TABLE IF NOT EXISTS tier_files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		path TEXT UNIQUE NOT NULL,
		database TEXT NOT NULL,
		measurement TEXT NOT NULL,
		partition_time TIMESTAMP NOT NULL,
		tier TEXT NOT NULL DEFAULT 'hot',
		size_bytes INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		migrated_at TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_tier_files_database ON tier_files(database);
	CREATE INDEX IF NOT EXISTS idx_tier_files_tier ON tier_files(tier);
	CREATE INDEX IF NOT EXISTS idx_tier_files_partition ON tier_files(partition_time);
	CREATE INDEX IF NOT EXISTS idx_tier_files_database_tier ON tier_files(database, tier);

	-- Migration history
	CREATE TABLE IF NOT EXISTS tier_migrations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_path TEXT NOT NULL,
		database TEXT NOT NULL,
		from_tier TEXT NOT NULL,
		to_tier TEXT NOT NULL,
		size_bytes INTEGER,
		started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		completed_at TIMESTAMP,
		error TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_tier_migrations_database ON tier_migrations(database);
	CREATE INDEX IF NOT EXISTS idx_tier_migrations_started ON tier_migrations(started_at);
	`

	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create tiering tables: %w", err)
	}

	s.logger.Info().Msg("Tiering metadata schema initialized")
	return nil
}

// invalidateTierCache removes the cache entry for a database/measurement pair.
// Call this after any operation that might change which tiers have data.
func (s *MetadataStore) invalidateTierCache(database, measurement string) {
	cacheKey := database + "/" + measurement
	s.tierCacheMu.Lock()
	delete(s.tierCache, cacheKey)
	s.tierCacheMu.Unlock()
}

// RecordFile records a new file in the metadata store
func (s *MetadataStore) RecordFile(ctx context.Context, file *FileMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		INSERT INTO tier_files (path, database, measurement, partition_time, tier, size_bytes, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			tier = excluded.tier,
			size_bytes = excluded.size_bytes,
			migrated_at = CASE WHEN tier_files.tier != excluded.tier THEN CURRENT_TIMESTAMP ELSE tier_files.migrated_at END
	`

	createdAt := file.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	_, err := s.db.ExecContext(ctx, query,
		file.Path,
		file.Database,
		file.Measurement,
		file.PartitionTime.UTC(),
		string(file.Tier),
		file.SizeBytes,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("failed to record file: %w", err)
	}

	// Invalidate tier cache for this database/measurement
	s.invalidateTierCache(file.Database, file.Measurement)

	return nil
}

// GetFile retrieves file metadata by path
func (s *MetadataStore) GetFile(ctx context.Context, path string) (*FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
		SELECT id, path, database, measurement, partition_time, tier, size_bytes, created_at, migrated_at
		FROM tier_files
		WHERE path = ?
	`

	row := s.db.QueryRowContext(ctx, query, path)
	return s.scanFile(row)
}

// GetFilesInTier retrieves all files in a specific tier
func (s *MetadataStore) GetFilesInTier(ctx context.Context, tier Tier) ([]FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
		SELECT id, path, database, measurement, partition_time, tier, size_bytes, created_at, migrated_at
		FROM tier_files
		WHERE tier = ?
		ORDER BY partition_time ASC
	`

	rows, err := s.db.QueryContext(ctx, query, string(tier))
	if err != nil {
		return nil, fmt.Errorf("failed to query files in tier: %w", err)
	}
	defer rows.Close()

	return s.scanFiles(rows)
}

// GetFilesOlderThan retrieves files in a tier older than the specified age
func (s *MetadataStore) GetFilesOlderThan(ctx context.Context, tier Tier, maxAge time.Duration) ([]FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-maxAge)

	query := `
		SELECT id, path, database, measurement, partition_time, tier, size_bytes, created_at, migrated_at
		FROM tier_files
		WHERE tier = ? AND partition_time < ?
		ORDER BY partition_time ASC
	`

	rows, err := s.db.QueryContext(ctx, query, string(tier), cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query old files: %w", err)
	}
	defer rows.Close()

	return s.scanFiles(rows)
}

// GetFilesByDatabase retrieves all files for a specific database
func (s *MetadataStore) GetFilesByDatabase(ctx context.Context, database string) ([]FileMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
		SELECT id, path, database, measurement, partition_time, tier, size_bytes, created_at, migrated_at
		FROM tier_files
		WHERE database = ?
		ORDER BY partition_time DESC
	`

	rows, err := s.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to query files by database: %w", err)
	}
	defer rows.Close()

	return s.scanFiles(rows)
}

// GetAllDatabases returns all unique database names from the tier metadata.
// This includes databases that may only have data in cold storage.
func (s *MetadataStore) GetAllDatabases(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT DISTINCT database FROM tier_files ORDER BY database`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, fmt.Errorf("failed to scan database: %w", err)
		}
		databases = append(databases, db)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating databases: %w", err)
	}

	return databases, nil
}

// GetMeasurementsByDatabase returns all unique measurements for a database from the tier metadata.
// This includes measurements that may only have data in cold storage.
func (s *MetadataStore) GetMeasurementsByDatabase(ctx context.Context, database string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `SELECT DISTINCT measurement FROM tier_files WHERE database = ? ORDER BY measurement`

	rows, err := s.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to query measurements: %w", err)
	}
	defer rows.Close()

	var measurements []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return nil, fmt.Errorf("failed to scan measurement: %w", err)
		}
		measurements = append(measurements, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating measurements: %w", err)
	}

	return measurements, nil
}

// UpdateTier updates the tier for a file
func (s *MetadataStore) UpdateTier(ctx context.Context, path string, newTier Tier) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get database/measurement for cache invalidation
	var database, measurement string
	lookupQuery := `SELECT database, measurement FROM tier_files WHERE path = ?`
	_ = s.db.QueryRowContext(ctx, lookupQuery, path).Scan(&database, &measurement)

	query := `
		UPDATE tier_files
		SET tier = ?, migrated_at = CURRENT_TIMESTAMP
		WHERE path = ?
	`

	result, err := s.db.ExecContext(ctx, query, string(newTier), path)
	if err != nil {
		return fmt.Errorf("failed to update tier: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("file not found: %s", path)
	}

	// Invalidate tier cache if we found the database/measurement
	if database != "" && measurement != "" {
		s.invalidateTierCache(database, measurement)
	}

	return nil
}

// DeleteFile removes a file from the metadata store
func (s *MetadataStore) DeleteFile(ctx context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get database/measurement for cache invalidation before delete
	var database, measurement string
	lookupQuery := `SELECT database, measurement FROM tier_files WHERE path = ?`
	_ = s.db.QueryRowContext(ctx, lookupQuery, path).Scan(&database, &measurement)

	query := `DELETE FROM tier_files WHERE path = ?`
	_, err := s.db.ExecContext(ctx, query, path)
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	// Invalidate tier cache if we found the database/measurement
	if database != "" && measurement != "" {
		s.invalidateTierCache(database, measurement)
	}

	return nil
}

// RecordMigration records a migration attempt
func (s *MetadataStore) RecordMigration(ctx context.Context, record *MigrationRecord) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		INSERT INTO tier_migrations (file_path, database, from_tier, to_tier, size_bytes, started_at, completed_at, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

	result, err := s.db.ExecContext(ctx, query,
		record.FilePath,
		record.Database,
		string(record.FromTier),
		string(record.ToTier),
		record.SizeBytes,
		record.StartedAt,
		record.CompletedAt,
		record.Error,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to record migration: %w", err)
	}

	return result.LastInsertId()
}

// CompleteMigration marks a migration as completed
func (s *MetadataStore) CompleteMigration(ctx context.Context, migrationID int64, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errorMsg *string
	if err != nil {
		msg := err.Error()
		errorMsg = &msg
	}

	query := `
		UPDATE tier_migrations
		SET completed_at = CURRENT_TIMESTAMP, error = ?
		WHERE id = ?
	`

	_, execErr := s.db.ExecContext(ctx, query, errorMsg, migrationID)
	if execErr != nil {
		return fmt.Errorf("failed to complete migration: %w", execErr)
	}

	return nil
}

// GetTierStats returns statistics for each tier
func (s *MetadataStore) GetTierStats(ctx context.Context) (map[Tier]TierStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
		SELECT tier, COUNT(*) as file_count, COALESCE(SUM(size_bytes), 0) as total_bytes
		FROM tier_files
		GROUP BY tier
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get tier stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[Tier]TierStats)
	for rows.Next() {
		var tierStr string
		var fileCount int64
		var totalBytes int64

		if err := rows.Scan(&tierStr, &fileCount, &totalBytes); err != nil {
			return nil, fmt.Errorf("failed to scan tier stats: %w", err)
		}

		tier := TierFromString(tierStr)
		stats[tier] = TierStats{
			Tier:        tier,
			FileCount:   fileCount,
			TotalSizeMB: totalBytes / (1024 * 1024),
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tier stats: %w", err)
	}

	return stats, nil
}

// GetTiersForMeasurement returns which tiers have data for a specific database/measurement.
// Returns a map of tier -> bool indicating presence of data in that tier.
// Results are cached for 30 seconds to reduce SQLite query overhead during query execution.
func (s *MetadataStore) GetTiersForMeasurement(ctx context.Context, database, measurement string) (map[Tier]bool, error) {
	cacheKey := database + "/" + measurement

	// Check cache first (with separate lock to avoid blocking other operations)
	s.tierCacheMu.RLock()
	if entry, ok := s.tierCache[cacheKey]; ok && time.Now().Before(entry.expiresAt) {
		// Cache hit - return a copy to avoid mutation
		result := make(map[Tier]bool, len(entry.tiers))
		for k, v := range entry.tiers {
			result[k] = v
		}
		s.tierCacheMu.RUnlock()
		return result, nil
	}
	s.tierCacheMu.RUnlock()

	// Cache miss - query database
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
		SELECT DISTINCT tier
		FROM tier_files
		WHERE database = ? AND measurement = ?
	`

	rows, err := s.db.QueryContext(ctx, query, database, measurement)
	if err != nil {
		return nil, fmt.Errorf("failed to get tiers for measurement: %w", err)
	}
	defer rows.Close()

	tiers := make(map[Tier]bool)
	for rows.Next() {
		var tierStr string
		if err := rows.Scan(&tierStr); err != nil {
			return nil, fmt.Errorf("failed to scan tier: %w", err)
		}
		tiers[TierFromString(tierStr)] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tiers: %w", err)
	}

	// Update cache
	s.tierCacheMu.Lock()
	s.tierCache[cacheKey] = &tierCacheEntry{
		tiers:     tiers,
		expiresAt: time.Now().Add(tierCacheTTL),
	}
	s.tierCacheMu.Unlock()

	// Return a copy
	result := make(map[Tier]bool, len(tiers))
	for k, v := range tiers {
		result[k] = v
	}
	return result, nil
}

// GetRecentMigrations returns recent migration records
func (s *MetadataStore) GetRecentMigrations(ctx context.Context, limit int) ([]MigrationRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
		SELECT id, file_path, database, from_tier, to_tier, size_bytes, started_at, completed_at, error
		FROM tier_migrations
		ORDER BY started_at DESC
		LIMIT ?
	`

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent migrations: %w", err)
	}
	defer rows.Close()

	var records []MigrationRecord
	for rows.Next() {
		var record MigrationRecord
		var fromTier, toTier string
		var completedAt sql.NullTime
		var errorMsg sql.NullString

		if err := rows.Scan(
			&record.ID,
			&record.FilePath,
			&record.Database,
			&fromTier,
			&toTier,
			&record.SizeBytes,
			&record.StartedAt,
			&completedAt,
			&errorMsg,
		); err != nil {
			return nil, fmt.Errorf("failed to scan migration record: %w", err)
		}

		record.FromTier = TierFromString(fromTier)
		record.ToTier = TierFromString(toTier)
		if completedAt.Valid {
			record.CompletedAt = &completedAt.Time
		}
		if errorMsg.Valid {
			record.Error = errorMsg.String
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migrations: %w", err)
	}

	return records, nil
}

// helper functions

func (s *MetadataStore) scanFile(row *sql.Row) (*FileMetadata, error) {
	var file FileMetadata
	var tierStr string
	var migratedAt sql.NullTime

	err := row.Scan(
		&file.ID,
		&file.Path,
		&file.Database,
		&file.Measurement,
		&file.PartitionTime,
		&tierStr,
		&file.SizeBytes,
		&file.CreatedAt,
		&migratedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan file: %w", err)
	}

	file.Tier = TierFromString(tierStr)
	if migratedAt.Valid {
		file.MigratedAt = &migratedAt.Time
	}

	return &file, nil
}

func (s *MetadataStore) scanFiles(rows *sql.Rows) ([]FileMetadata, error) {
	var files []FileMetadata

	for rows.Next() {
		var file FileMetadata
		var tierStr string
		var migratedAt sql.NullTime

		err := rows.Scan(
			&file.ID,
			&file.Path,
			&file.Database,
			&file.Measurement,
			&file.PartitionTime,
			&tierStr,
			&file.SizeBytes,
			&file.CreatedAt,
			&migratedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file: %w", err)
		}

		file.Tier = TierFromString(tierStr)
		if migratedAt.Valid {
			file.MigratedAt = &migratedAt.Time
		}

		files = append(files, file)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating files: %w", err)
	}

	return files, nil
}
