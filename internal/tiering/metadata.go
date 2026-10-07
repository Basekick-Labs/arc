package tiering

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// tierCacheEntry holds cached tier information with expiration
type tierCacheEntry struct {
	tiers     map[Tier]bool
	expiresAt time.Time
}

// pruneExpiredTierCache removes expired entries. The caller must hold tierCacheMu.
func (s *MetadataStore) pruneExpiredTierCache(now time.Time) {
	for key, entry := range s.tierCache {
		if !now.Before(entry.expiresAt) {
			delete(s.tierCache, key)
		}
	}
}

// tierCacheTTL is how long tier lookups are cached (30 seconds)
const tierCacheTTL = 30 * time.Second

// MetadataStore manages tier file metadata in SQLite
type MetadataStore struct {
	db     *sql.DB
	logger zerolog.Logger

	// Cache for GetTiersForMeasurement - keyed by "database/measurement"
	tierCache    map[string]*tierCacheEntry
	tierCacheGen uint64
	tierCacheMu  sync.RWMutex
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
		migrated_at TIMESTAMP,
		quarantined_at TIMESTAMP,
		quarantine_reason TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_tier_files_database ON tier_files(database);
	CREATE INDEX IF NOT EXISTS idx_tier_files_tier ON tier_files(tier);
	CREATE INDEX IF NOT EXISTS idx_tier_files_partition ON tier_files(partition_time);
	CREATE INDEX IF NOT EXISTS idx_tier_files_database_tier ON tier_files(database, tier);
	-- GetTiersForMeasurement's SELECT DISTINCT tier WHERE database = ? AND
	-- measurement = ? is the query path's routing read and, uncached, the
	-- replication drainer's before/after tier-set read. Without this it walks
	-- every row of the database through idx_tier_files_database_tier and
	-- filters on measurement. tier is included so the index COVERS the read:
	-- the planner picks a two-column (database, measurement) index only once
	-- ANALYZE has run, and nothing in Arc runs ANALYZE.
	CREATE INDEX IF NOT EXISTS idx_tier_files_database_measurement_tier ON tier_files(database, measurement, tier);

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
	-- Expression index on the normalized timestamp so the one-time MAX(id)
	-- cutoff lookup in CleanupOldMigrations is index-backed. The plain
	-- started_at index cannot serve datetime(started_at) comparisons.
	CREATE INDEX IF NOT EXISTS idx_tier_migrations_started_normalized ON tier_migrations(datetime(started_at));
	`

	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create tiering tables: %w", err)
	}

	// CREATE TABLE IF NOT EXISTS leaves an existing table untouched, so a
	// tier_files table created before the quarantine columns existed (#758)
	// keeps its old shape. SQLite has no ADD COLUMN IF NOT EXISTS; a
	// duplicate-column error is the expected outcome on an up-to-date
	// database and is not a failure. Rows that predate the column read back
	// as NULL, which is the not-quarantined state.
	for _, col := range []string{
		"ALTER TABLE tier_files ADD COLUMN quarantined_at TIMESTAMP",
		"ALTER TABLE tier_files ADD COLUMN quarantine_reason TEXT",
	} {
		if _, err := s.db.Exec(col); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add tier_files quarantine columns: %w", err)
		}
	}

	s.logger.Info().Msg("Tiering metadata schema initialized")
	return nil
}

// tierFileColumns is the SELECT list every tier_files read shares, in the
// order scanFile and scanFiles consume it.
const tierFileColumns = "id, path, database, measurement, partition_time, tier, size_bytes, created_at, migrated_at, quarantined_at, quarantine_reason"

// invalidateTierCache removes the cache entry for a database/measurement pair.
// Call this after any operation that might change which tiers have data.
func (s *MetadataStore) invalidateTierCache(database, measurement string) {
	cacheKey := database + "/" + measurement
	s.tierCacheMu.Lock()
	delete(s.tierCache, cacheKey)
	s.tierCacheGen++
	s.tierCacheMu.Unlock()
}

// The three write helpers below do NOT invalidate the tier cache; their
// caller is the replication-registration drainer, which applies a whole batch
// and then invalidates once per distinct database/measurement.
//
// That split is deliberate rather than tidy. invalidateTierCache bumps a
// single process-wide tierCacheGen, and storeTierCacheIfUnchanged discards any
// cache fill whose generation moved — so invalidating per row would make a
// catch-up burst of thousands of files, or one compaction sweep's manifest
// deletes, throw away concurrent GetTiersForMeasurement fills for every
// unrelated measurement, leaving the query path to re-run SELECT DISTINCT tier
// on the one shared SQLite connection. Per batch the bump count is the number
// of measurements touched, not the number of files.
//
// Skipping invalidation entirely is not an option in either direction: a
// concurrent GetTiersForMeasurement that read tierCacheGen and ran its query
// BEFORE one of these writes must not be allowed to store a tier set missing
// the tier just added, or it serves that set for the full TTL — for a
// measurement whose only other row is cold, exactly the cold-only read these
// helpers exist to prevent.

// recordHotFileIfNotCold records a hot file without ever moving an existing
// row out of the cold tier. It is the registration path for a file this node
// did not write itself — one pulled from a peer by cluster file replication —
// where plain RecordFile would be wrong: RecordFile's upsert sets
// `tier = excluded.tier` unconditionally, which is why ScanAndRegisterFiles
// needs its own cold-path pre-check before calling it (#683). A hot file whose
// row already says cold is the orphan ReconcileOrphanedFiles deletes after a
// failed post-migration cleanup; re-registering it as hot would reset
// migrated_at, hide it from reconciliation, and re-upload it every cycle.
//
// One statement, so there is no read-then-write window: the conflict branch is
// gated on the stored row still being hot, and a cold or quarantined row is
// left exactly as it is.
//
// Reports whether a row was written. A conflict the WHERE excluded, and a
// re-record of an identical row, both report false, so a reconciliation walk
// over files this node already holds writes nothing.
func (s *MetadataStore) recordHotFileIfNotCold(ctx context.Context, file *FileMetadata) (bool, error) {
	createdAt := file.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	// Bound in UTC, here and in every other writer of created_at: the column
	// is compared in SQL (DeleteFileInTier's createdBefore), go-sqlite3 stores
	// a time.Time as text in whatever zone the value carries, and the scan
	// passes a local backend's mtime, which is in the host zone. Two zones in
	// one column make that comparison a string compare across offsets.
	//
	// A quarantined row is matched by the conflict target but excluded from the
	// update: its key is permanently unusable, and the row records that.
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO tier_files (path, database, measurement, partition_time, tier, size_bytes, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET size_bytes = excluded.size_bytes
			WHERE tier_files.tier = ?
			  AND tier_files.quarantined_at IS NULL
			  AND tier_files.size_bytes != excluded.size_bytes
	`,
		file.Path,
		file.Database,
		file.Measurement,
		file.PartitionTime.UTC(),
		string(TierHot),
		file.SizeBytes,
		createdAt.UTC(),
		string(TierHot),
	)
	if err != nil {
		return false, fmt.Errorf("failed to record hot file: %w", err)
	}

	n, _ := res.RowsAffected()
	return n > 0, nil
}

// markFileCold records that a path is readable in the cold tier and no longer
// in hot, for a node that learned it from its own storage rather than by
// performing the migration: the primary writer migrates and then removes the
// hot copy cluster-wide through the Raft manifest, and without this the
// receiving node would not read the cold copy until its next cold-tier
// metadata sync — up to a full migration interval.
//
// An upsert, not an update: the hot row may never have been written (a
// registration dropped under load), and an update that matched nothing would
// leave the file with no row at all — unlinked locally, absent from this
// node's cold reads, and invisible until the sync. The conflict branch still
// refuses to touch a cold or quarantined row.
//
// migrated_at is deliberately left NULL on insert and untouched on update.
// Stamping it would put every path this node unlinks into orphan
// reconciliation's recently-migrated window, which HEADs each row — see
// RecordColdFile, which stamps from the cold object's own timestamp for that
// reason.
//
// It stays NULL for good, and that is the intent rather than an oversight:
// syncColdTierMetadata skips any path it already holds a cold row for, so it
// never reaches RecordColdFile for one of these, and RecordColdFile keeps an
// existing stamp when the tier is unchanged in any case. Nothing needs it.
// Both readers of migrated_at — GetRecentlyMigratedFiles, for orphan
// reconciliation, and the manifest sweep — look for work on a file whose hot
// copy is still present and whose manifest entry still stands, and by the
// time this runs neither is true.
//
// Reports whether a row was written.
func (s *MetadataStore) markFileCold(ctx context.Context, file *FileMetadata) (bool, error) {
	createdAt := file.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	res, err := s.db.ExecContext(ctx, `
		INSERT INTO tier_files (path, database, measurement, partition_time, tier, size_bytes, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET tier = excluded.tier
			WHERE tier_files.tier = ?
			  AND tier_files.quarantined_at IS NULL
	`,
		file.Path,
		file.Database,
		file.Measurement,
		file.PartitionTime.UTC(),
		string(TierCold),
		file.SizeBytes,
		createdAt.UTC(),
		string(TierHot),
	)
	if err != nil {
		return false, fmt.Errorf("failed to mark file cold: %w", err)
	}

	n, _ := res.RowsAffected()
	return n > 0, nil
}

// retireHotRow removes a path's row only if it is still hot, for a node that
// has just unlinked its local copy for a reason that does not put the file in
// cold — a compaction that consumed it, a retention or operator delete. The
// tier condition is the same one DeleteFileInTier applies: a row that reached
// cold meanwhile is not this caller's to remove.
//
// Hands back the row's database and measurement so the caller can invalidate
// the tier cache for the batch; they are read before the delete because the
// row is gone afterwards. Reports whether a row was removed.
func (s *MetadataStore) retireHotRow(ctx context.Context, path string) (bool, string, string, error) {
	// One statement with RETURNING rather than SELECT-then-DELETE: the pair
	// leaves a window in which a row inserted between them is deleted while
	// the cache key reads empty, and it would have to discard the SELECT's
	// error to stay readable.
	var database, measurement string
	err := s.db.QueryRowContext(ctx,
		`DELETE FROM tier_files WHERE path = ? AND tier = ?
		 RETURNING database, measurement`,
		path, string(TierHot),
	).Scan(&database, &measurement)
	if errors.Is(err, sql.ErrNoRows) {
		// No hot row for this path: already cold, quarantined away, or never
		// registered. Not an error.
		return false, "", "", nil
	}
	if err != nil {
		return false, "", "", fmt.Errorf("failed to retire hot row: %w", err)
	}
	return true, database, measurement, nil
}

// RecordFile records a new file in the metadata store
func (s *MetadataStore) RecordFile(ctx context.Context, file *FileMetadata) error {
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
		createdAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to record file: %w", err)
	}

	// Invalidate tier cache for this database/measurement
	s.invalidateTierCache(file.Database, file.Measurement)

	return nil
}

// sqliteTimestampLayout is the text form CURRENT_TIMESTAMP writes. migrated_at
// is compared and ordered as text, so a Go-side stamp must use the same
// layout or the two formats sort against each other ('T' vs ' ').
const sqliteTimestampLayout = "2006-01-02 15:04:05"

// RecordColdFile records a file that is present in cold storage, as found by
// the cold-tier metadata sync. Unlike RecordFile it stamps migrated_at from
// the cold object's own timestamp rather than now: the sync discovers moves
// after the fact, on every node, and orphan reconciliation walks every row
// migrated in the last 48 hours with one HEAD each — stamping now on a
// fresh node would make it HEAD the whole cold tier for two cycles. A
// same-tier conflict keeps the row's migrated_at.
func (s *MetadataStore) RecordColdFile(ctx context.Context, file *FileMetadata, migratedAt time.Time) error {
	if migratedAt.IsZero() {
		migratedAt = time.Now()
	}
	createdAt := file.CreatedAt
	if createdAt.IsZero() {
		createdAt = migratedAt
	}

	query := `
		INSERT INTO tier_files (path, database, measurement, partition_time, tier, size_bytes, created_at, migrated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET
			tier = excluded.tier,
			size_bytes = excluded.size_bytes,
			migrated_at = CASE WHEN tier_files.tier != excluded.tier THEN excluded.migrated_at ELSE tier_files.migrated_at END
	`
	_, err := s.db.ExecContext(ctx, query,
		file.Path,
		file.Database,
		file.Measurement,
		file.PartitionTime.UTC(),
		string(TierCold),
		file.SizeBytes,
		createdAt.UTC(),
		migratedAt.UTC().Format(sqliteTimestampLayout),
	)
	if err != nil {
		return fmt.Errorf("failed to record cold file: %w", err)
	}

	s.invalidateTierCache(file.Database, file.Measurement)
	return nil
}

// GetFile retrieves file metadata by path
func (s *MetadataStore) GetFile(ctx context.Context, path string) (*FileMetadata, error) {
	query := `
		SELECT ` + tierFileColumns + `
		FROM tier_files
		WHERE path = ?
	`

	row := s.db.QueryRowContext(ctx, query, path)
	return s.scanFile(row)
}

// GetFilesInTier retrieves all files in a specific tier
func (s *MetadataStore) GetFilesInTier(ctx context.Context, tier Tier) ([]FileMetadata, error) {
	query := `
		SELECT ` + tierFileColumns + `
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

// CountFilesInTierByDatabase counts the rows a tier holds, grouped by
// database, in ONE query (#1085 stage B3). It backs the backup manifest's
// cold_files_excluded marker: how many files a backup is NOT carrying because
// they have been migrated out of hot storage.
//
// Grouped rather than one count per database on purpose. The set a caller
// wants is "every database with rows in this tier", and that set is not the
// backup inventory: a fully cold database has no hot files at all, so it is
// absent from a backup listing and from the manifest inventory while still
// holding the rows this counts. Returning the group means the caller never has
// to discover the set first, and it cannot half-fail the way N point queries
// can.
//
// Quarantined rows are excluded, as in GetFilesOlderThan: a quarantined file
// is one tiering has established it can never act on (#758), so it is not a
// file a backup is missing. Defensive today — quarantineCandidate leaves the
// row in the HOT tier (migrator.go) — and correct if that ever changes.
//
// Measured, not assumed, on this schema at 200k rows with a third of them
// cold, before and after ANALYZE (which changes nothing, and which nothing in
// Arc runs anyway):
//
//	SEARCH tier_files USING INDEX idx_tier_files_tier (tier=?)
//	USE TEMP B-TREE FOR GROUP BY                          ~21 ms
//
// idx_tier_files_database_tier is not picked, in either state: tier is that
// index's SECOND column, so tier = ? cannot seek it. Forcing it with INDEXED BY
// gives a full SCAN of the index — no temp b-tree, since the index is already
// in database order, but every row visited including hot and a table lookup for
// quarantined_at on each match, which no index covers. It loses by ~1.5x.
//
// This is NOT the fastest plan available, and the comment says so rather than
// claiming optimality, because the next person to read it will otherwise
// conclude nothing better exists. A covering index on
// (tier, database, quarantined_at) seeks on the leading column, needs no temp
// b-tree and no table lookups, is picked WITHOUT ANALYZE, and is ~6x faster
// (~21 ms down to ~3 ms). It is deliberately not added: it costs ~7% of the
// database file and a sixth b-tree to maintain on tier_files, which the ingest
// flush path writes to on every file registration, and it buys ~18 ms ONCE per
// backup run — on an operation that copies gigabytes over minutes. Wrong trade
// today.
//
// The scale at which to revisit it: the read is linear in matched rows, about
// 330 ns each, and it holds the shared SQLite connection for its duration
// (the pool is SetMaxOpenConns(1)), so it stalls auth, audit and tier
// registration for as long as it runs. At 1M cold rows that is ~330 ms once
// per backup, which is where the covering index stops being a bad trade.
func (s *MetadataStore) CountFilesInTierByDatabase(ctx context.Context, tier Tier) (map[string]int64, error) {
	query := `
		SELECT database, COUNT(*)
		FROM tier_files
		WHERE tier = ? AND quarantined_at IS NULL
		GROUP BY database
	`

	rows, err := s.db.QueryContext(ctx, query, string(tier))
	if err != nil {
		return nil, fmt.Errorf("failed to count files by database in tier: %w", err)
	}
	defer rows.Close()

	counts := map[string]int64{}
	for rows.Next() {
		var database string
		var n int64
		if err := rows.Scan(&database, &n); err != nil {
			return nil, fmt.Errorf("failed to scan tier file count: %w", err)
		}
		counts[database] = n
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tier file counts: %w", err)
	}

	return counts, nil
}

// GetFilesOlderThan retrieves files in a tier older than the specified age.
// Quarantined rows are excluded: this is the migration candidate query, and a
// quarantined file is one tiering has established it can never act on (#758).
func (s *MetadataStore) GetFilesOlderThan(ctx context.Context, tier Tier, maxAge time.Duration) ([]FileMetadata, error) {
	cutoff := time.Now().UTC().Add(-maxAge)

	query := `
		SELECT ` + tierFileColumns + `
		FROM tier_files
		WHERE tier = ? AND partition_time < ? AND quarantined_at IS NULL
		ORDER BY partition_time ASC
	`

	rows, err := s.db.QueryContext(ctx, query, string(tier), cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query old files: %w", err)
	}
	defer rows.Close()

	return s.scanFiles(rows)
}

// GetRecentlyMigratedFiles retrieves files in a tier that were migrated within the given window.
// Used by reconciliation to limit the working set to recently-migrated files.
// Quarantined rows are excluded for the same reason as in GetFilesOlderThan.
func (s *MetadataStore) GetRecentlyMigratedFiles(ctx context.Context, tier Tier, window time.Duration) ([]FileMetadata, error) {
	cutoff := time.Now().UTC().Add(-window)

	query := `
		SELECT ` + tierFileColumns + `
		FROM tier_files
		WHERE tier = ? AND migrated_at IS NOT NULL AND migrated_at >= ? AND quarantined_at IS NULL
		ORDER BY migrated_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, string(tier), cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query recently migrated files: %w", err)
	}
	defer rows.Close()

	return s.scanFiles(rows)
}

// GetFilesByDatabase retrieves all files for a specific database
func (s *MetadataStore) GetFilesByDatabase(ctx context.Context, database string) ([]FileMetadata, error) {
	query := `
		SELECT ` + tierFileColumns + `
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

// GetFilesForQuery retrieves files for a database, optionally filtered by
// measurement and/or time range. Filters are pushed into the SQL WHERE
// clause instead of being applied client-side, so SQLite's indexes on
// database/tier/partition_time can prune rows before they reach Go.
func (s *MetadataStore) GetFilesForQuery(ctx context.Context, database, measurement string, startTime, endTime *time.Time) ([]FileMetadata, error) {
	query := `
		SELECT ` + tierFileColumns + `
		FROM tier_files
		WHERE database = ?
	`
	args := []any{database}

	if measurement != "" {
		query += " AND measurement = ?"
		args = append(args, measurement)
	}
	if startTime != nil {
		query += " AND partition_time >= ?"
		args = append(args, startTime.UTC())
	}
	if endTime != nil {
		query += " AND partition_time <= ?"
		args = append(args, endTime.UTC())
	}

	query += " ORDER BY partition_time DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query files: %w", err)
	}
	defer rows.Close()

	return s.scanFiles(rows)
}

// GetAllDatabases returns all unique database names from the tier metadata.
// This includes databases that may only have data in cold storage.
func (s *MetadataStore) GetAllDatabases(ctx context.Context) ([]string, error) {
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

// GetTiersForDatabase returns which tiers have data for a specific database.
// Returns a slice of tier names (e.g., ["hot"], ["cold"], or ["hot", "cold"]).
func (s *MetadataStore) GetTiersForDatabase(ctx context.Context, database string) ([]string, error) {
	query := `SELECT DISTINCT tier FROM tier_files WHERE database = ? ORDER BY tier`

	rows, err := s.db.QueryContext(ctx, query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to query tiers: %w", err)
	}
	defer rows.Close()

	var tiers []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("failed to scan tier: %w", err)
		}
		tiers = append(tiers, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tiers: %w", err)
	}

	return tiers, nil
}

// GetMeasurementsByDatabase returns all unique measurements for a database from the tier metadata.
// This includes measurements that may only have data in cold storage.
func (s *MetadataStore) GetMeasurementsByDatabase(ctx context.Context, database string) ([]string, error) {
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

// DeleteFileInTier removes a file's row only if it is still in the given
// tier, so a decision taken from a tier listing cannot delete a row that
// changed tier meanwhile.
//
// createdBefore extends that to the row's age: a non-zero value deletes only
// a row at least as old as the caller's evidence. It covers one narrow case —
// a row the replication drainer DELETED and a later pull re-INSERTED between
// the caller's snapshot and this call carries a fresh created_at, and the
// decision was not taken about that row. It does NOT cover a row that was
// merely re-registered: no conflict branch in this file touches created_at,
// so a refreshed row is as old as the one the snapshot judged, and the caller
// has to re-check storage for that (retireVanishedHotRows stats the path).
//
// The comparison is on text — go-sqlite3 binds a time.Time in the zone it
// carries — so every writer of created_at binds UTC and so does this.
//
// Reports whether a row was removed.
func (s *MetadataStore) DeleteFileInTier(ctx context.Context, path string, tier Tier, createdBefore time.Time) (bool, error) {
	query := `DELETE FROM tier_files WHERE path = ? AND tier = ?`
	args := []any{path, string(tier)}
	if !createdBefore.IsZero() {
		query += ` AND created_at < ?`
		args = append(args, createdBefore.UTC())
	}
	query += ` RETURNING database, measurement`

	var database, measurement string
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&database, &measurement)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to delete file: %w", err)
	}
	if database != "" && measurement != "" {
		s.invalidateTierCache(database, measurement)
	}
	return true, nil
}

// QuarantineFile marks a file index row as one tiering must never act on
// again (#758). The row stays, with its tier unchanged, so the query path and
// the status endpoints keep describing what is actually on disk; only the
// work-set queries (GetFilesOlderThan, GetRecentlyMigratedFiles) exclude it.
//
// Persisted rather than held in memory because both work sets are recomputed
// from this table every cycle, so an in-memory skip list would be forgotten
// on restart and the retry storm would resume. Nothing clears the mark: the
// only remedy for a permanently unusable key is renaming the object, and a
// renamed object has a NEW key that registers as a fresh row.
//
// Idempotent on an already quarantined row: the original timestamp and
// reason are kept, so the record says when tiering first established the
// condition rather than when it last looked.
func (s *MetadataStore) QuarantineFile(ctx context.Context, path, reason string) error {
	query := `
		UPDATE tier_files
		SET quarantined_at = COALESCE(quarantined_at, ?),
		    quarantine_reason = COALESCE(quarantine_reason, ?)
		WHERE path = ?
	`

	result, err := s.db.ExecContext(ctx, query, time.Now().UTC(), reason, path)
	if err != nil {
		return fmt.Errorf("failed to quarantine file: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("file not found: %s", path)
	}

	return nil
}

// GetQuarantinedFiles returns every file index row QuarantineFile has marked,
// oldest mark first. This is the operator's list of files tiering has given
// up on and that need a rename by hand.
func (s *MetadataStore) GetQuarantinedFiles(ctx context.Context) ([]FileMetadata, error) {
	query := `
		SELECT ` + tierFileColumns + `
		FROM tier_files
		WHERE quarantined_at IS NOT NULL
		ORDER BY quarantined_at ASC, id ASC
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query quarantined files: %w", err)
	}
	defer rows.Close()

	return s.scanFiles(rows)
}

// CountQuarantinedFiles returns how many file index rows are quarantined.
// Reported on the tiering status endpoint so the condition is visible without
// reading the metrics endpoint; it should be zero.
func (s *MetadataStore) CountQuarantinedFiles(ctx context.Context) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tier_files WHERE quarantined_at IS NOT NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("failed to count quarantined files: %w", err)
	}
	return n, nil
}

// RecordMigration records a migration attempt
func (s *MetadataStore) RecordMigration(ctx context.Context, record *MigrationRecord) (int64, error) {
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
	var errorMsg *string
	if err != nil {
		msg := err.Error()
		errorMsg = &msg
	}

	// Use Go time.Time (not SQLite CURRENT_TIMESTAMP) so completed_at uses
	// the same RFC3339 format as started_at (stored by RecordMigration).
	// Mixing CURRENT_TIMESTAMP (space-separated) with Go time.Time (RFC3339)
	// causes incorrect string comparisons (T > space in ASCII).
	now := time.Now().UTC()

	query := `
		UPDATE tier_migrations
		SET completed_at = ?, error = ?
		WHERE id = ?
	`

	_, execErr := s.db.ExecContext(ctx, query, now, errorMsg, migrationID)
	if execErr != nil {
		return fmt.Errorf("failed to complete migration: %w", execErr)
	}

	return nil
}

// GetTierStats returns statistics for each tier
func (s *MetadataStore) GetTierStats(ctx context.Context) (map[Tier]TierStats, error) {
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

	// Check cache first (with separate lock to avoid blocking other operations).
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
	cacheGen := s.tierCacheGen
	s.tierCacheMu.RUnlock()

	// Cache miss - query database
	tiers, err := s.queryTierSet(ctx, database, measurement)
	if err != nil {
		return nil, err
	}

	s.storeTierCacheIfUnchanged(cacheKey, tiers, cacheGen)

	// Return a copy
	result := make(map[Tier]bool, len(tiers))
	for _, tier := range tiers {
		result[tier] = true
	}
	return result, nil
}

// readTierSet is GetTiersForMeasurement without the cache: the tiers that have
// a row for the measurement right now, straight from SQLite. For a writer that
// needs to know whether its own writes changed the set — the cache can be up
// to tierCacheTTL behind, and the writer is about to invalidate it anyway.
func (s *MetadataStore) readTierSet(ctx context.Context, database, measurement string) (map[Tier]bool, error) {
	tiers, err := s.queryTierSet(ctx, database, measurement)
	if err != nil {
		return nil, err
	}
	result := make(map[Tier]bool, len(tiers))
	for _, tier := range tiers {
		result[tier] = true
	}
	return result, nil
}

func (s *MetadataStore) queryTierSet(ctx context.Context, database, measurement string) ([]Tier, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT tier
		FROM tier_files
		WHERE database = ? AND measurement = ?
	`, database, measurement)
	if err != nil {
		return nil, fmt.Errorf("failed to get tiers for measurement: %w", err)
	}
	defer rows.Close()

	var tiers []Tier
	for rows.Next() {
		var tierStr string
		if err := rows.Scan(&tierStr); err != nil {
			return nil, fmt.Errorf("failed to scan tier: %w", err)
		}
		tiers = append(tiers, TierFromString(tierStr))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tiers: %w", err)
	}
	return tiers, nil
}

func (s *MetadataStore) storeTierCacheIfUnchanged(key string, tiers []Tier, gen uint64) {
	s.tierCacheMu.Lock()
	defer s.tierCacheMu.Unlock()
	if s.tierCacheGen != gen {
		return
	}

	s.pruneExpiredTierCache(time.Now())
	cachedTiers := make(map[Tier]bool, len(tiers))
	for _, tier := range tiers {
		cachedTiers[tier] = true
	}
	s.tierCache[key] = &tierCacheEntry{
		tiers:     cachedTiers,
		expiresAt: time.Now().Add(tierCacheTTL),
	}
}

// GetRecentMigrations returns recent migration records
func (s *MetadataStore) GetRecentMigrations(ctx context.Context, limit int) ([]MigrationRecord, error) {
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

// CleanupOldMigrations deletes migration records older than retentionDays.
// A retentionDays of 0 or less is a no-op (keep all records).
// Uses s.db directly; SQLite handles locking between concurrent operations.
//
// The cutoff is resolved to a single MAX(id) up front, then deletes are batched
// (1000 rows per transaction) by primary key to avoid re-scanning on the
// datetime(started_at) predicate every iteration and to keep the SQLite write
// lock held only briefly. Vacuum is intentionally not run — freed pages are
// immediately reused by subsequent INSERTs, and vacuum would cause unnecessary
// write amplification.
func (s *MetadataStore) CleanupOldMigrations(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}

	// Compute cutoff in Go as time.Time. Both sides use SQLite's datetime()
	// wrapper to normalize format: Go time.Time is RFC3339 (T-separated),
	// legacy records may use space-separated format from CURRENT_TIMESTAMP.
	// datetime() accepts both and normalizes to space-separated, preventing
	// the T > space ASCII comparison bug on mixed-format tables.
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)

	// Resolve the date predicate exactly once. RecordMigration sets started_at
	// to time.Now().UTC() immediately before INSERT, so id (autoincrement) is
	// monotonic with started_at — every row with id <= maxID has started_at
	// <= the cutoff row's. That lets the batch loop below delete by primary
	// key (id <= maxID) instead of re-evaluating datetime(started_at) on every
	// iteration, which would force a full table scan per batch (the datetime()
	// wrapper defeats the started_at index).
	var maxID int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(id), 0) FROM tier_migrations WHERE datetime(started_at) < datetime(?)",
		cutoff).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("failed to find max migration id for cleanup: %w", err)
	}
	if maxID == 0 {
		return 0, nil // No records older than the cutoff.
	}

	const batchSize = 1000
	var totalDeleted int64

	for {
		if err := ctx.Err(); err != nil {
			return totalDeleted, err
		}

		result, err := s.db.ExecContext(ctx,
			"DELETE FROM tier_migrations WHERE id IN (SELECT id FROM tier_migrations WHERE id <= ? ORDER BY id ASC LIMIT ?)",
			maxID, batchSize)
		if err != nil {
			return totalDeleted, fmt.Errorf("failed to cleanup old migrations: %w", err)
		}

		deleted, err := result.RowsAffected()
		if err != nil {
			// The rows for this batch were already deleted; we just can't read
			// the count. Surface the error (the caller logs it) rather than
			// silently breaking with an under-reported total.
			return totalDeleted, fmt.Errorf("failed to retrieve rows affected by migration cleanup: %w", err)
		}

		totalDeleted += deleted

		if deleted < batchSize {
			// Fewer rows than the limit means this was the last batch.
			break
		}
	}

	// Note: we intentionally do not run PRAGMA incremental_vacuum here.
	// tier_migrations is continuously written during normal operations;
	// freed pages are immediately reused by subsequent INSERTs. Vacuuming
	// would shrink the file only for it to re-grow on the next cycle,
	// causing unnecessary write amplification. The key fix for #342 is
	// the DELETE itself — that stops unbounded growth.

	return totalDeleted, nil
}

// helper functions

func (s *MetadataStore) scanFile(row *sql.Row) (*FileMetadata, error) {
	var file FileMetadata
	var tierStr string
	var migratedAt, quarantinedAt sql.NullTime
	var quarantineReason sql.NullString

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
		&quarantinedAt,
		&quarantineReason,
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
	if quarantinedAt.Valid {
		file.QuarantinedAt = &quarantinedAt.Time
	}
	file.QuarantineReason = quarantineReason.String

	return &file, nil
}

func (s *MetadataStore) scanFiles(rows *sql.Rows) ([]FileMetadata, error) {
	var files []FileMetadata

	for rows.Next() {
		var file FileMetadata
		var tierStr string
		var migratedAt, quarantinedAt sql.NullTime
		var quarantineReason sql.NullString

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
			&quarantinedAt,
			&quarantineReason,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file: %w", err)
		}

		file.Tier = TierFromString(tierStr)
		if migratedAt.Valid {
			file.MigratedAt = &migratedAt.Time
		}
		if quarantinedAt.Valid {
			file.QuarantinedAt = &quarantinedAt.Time
		}
		file.QuarantineReason = quarantineReason.String

		files = append(files, file)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating files: %w", err)
	}

	return files, nil
}
