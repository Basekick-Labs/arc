package tiering

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// DatabasePolicy represents per-database tiering policy overrides
type DatabasePolicy struct {
	ID            int64     `json:"id"`
	Database      string    `json:"database"`
	HotOnly       bool      `json:"hot_only"`         // Exclude from tiering entirely
	HotMaxAgeDays *int      `json:"hot_max_age_days"` // nil = use global default
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// EffectivePolicy represents the resolved policy for a database
type EffectivePolicy struct {
	Database      string `json:"database"`
	HotOnly       bool   `json:"hot_only"`
	HotMaxAgeDays int    `json:"hot_max_age_days"`
	Source        string `json:"source"` // "custom" or "global"
}

// PolicyStore manages per-database tiering policies in SQLite
type PolicyStore struct {
	db       *sql.DB
	defaults *config.TieredStorageConfig
	cache    map[string]*DatabasePolicy
	logger   zerolog.Logger
	mu       sync.RWMutex
}

// NewPolicyStore creates a new policy store
func NewPolicyStore(db *sql.DB, defaults *config.TieredStorageConfig, logger zerolog.Logger) (*PolicyStore, error) {
	store := &PolicyStore{
		db:       db,
		defaults: defaults,
		cache:    make(map[string]*DatabasePolicy),
		logger:   logger.With().Str("component", "tiering-policy").Logger(),
	}

	if err := store.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize policy schema: %w", err)
	}

	// Load existing policies into cache
	if err := store.loadCache(); err != nil {
		return nil, fmt.Errorf("failed to load policy cache: %w", err)
	}

	return store, nil
}

// initSchema creates the tiering_policies table if it doesn't exist
func (s *PolicyStore) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS tiering_policies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		database TEXT UNIQUE NOT NULL,
		hot_only BOOLEAN DEFAULT FALSE,
		hot_max_age_days INTEGER,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_tiering_policies_database ON tiering_policies(database);
	`

	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create tiering_policies table: %w", err)
	}

	s.logger.Info().Msg("Tiering policy schema initialized")
	return nil
}

// loadCache loads all policies from the database into the in-memory cache
func (s *PolicyStore) loadCache() error {
	policies, err := s.listFromDB(context.Background())
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache = make(map[string]*DatabasePolicy)
	for i := range policies {
		s.cache[policies[i].Database] = &policies[i]
	}

	s.logger.Info().Int("count", len(policies)).Msg("Loaded tiering policies into cache")
	return nil
}

// Get retrieves a policy for a specific database
func (s *PolicyStore) Get(ctx context.Context, database string) (*DatabasePolicy, error) {
	s.mu.RLock()
	policy, exists := s.cache[database]
	s.mu.RUnlock()

	if exists {
		return policy, nil
	}

	// Not in cache, check database and cache the result
	policy, err := s.getFromDB(ctx, database)
	if err != nil {
		return nil, err
	}

	// Cache the result (even if nil, we cache to avoid repeated DB lookups)
	if policy != nil {
		s.mu.Lock()
		s.cache[database] = policy
		s.mu.Unlock()
	}

	return policy, nil
}

// getFromDB retrieves a policy from the database
func (s *PolicyStore) getFromDB(ctx context.Context, database string) (*DatabasePolicy, error) {
	query := `
		SELECT id, database, hot_only, hot_max_age_days, created_at, updated_at
		FROM tiering_policies
		WHERE database = ?
	`

	row := s.db.QueryRowContext(ctx, query, database)
	return s.scanPolicy(row)
}

// Set creates or updates a policy for a database
func (s *PolicyStore) Set(ctx context.Context, policy *DatabasePolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
		INSERT INTO tiering_policies (database, hot_only, hot_max_age_days, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(database) DO UPDATE SET
			hot_only = excluded.hot_only,
			hot_max_age_days = excluded.hot_max_age_days,
			updated_at = CURRENT_TIMESTAMP
	`

	_, err := s.db.ExecContext(ctx, query,
		policy.Database,
		policy.HotOnly,
		policy.HotMaxAgeDays,
	)
	if err != nil {
		return fmt.Errorf("failed to set policy: %w", err)
	}

	// Update cache
	s.cache[policy.Database] = policy

	s.logger.Info().
		Str("database", policy.Database).
		Bool("hot_only", policy.HotOnly).
		Msg("Tiering policy updated")

	return nil
}

// Delete removes a policy for a database (reverts to global defaults)
func (s *PolicyStore) Delete(ctx context.Context, database string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `DELETE FROM tiering_policies WHERE database = ?`
	result, err := s.db.ExecContext(ctx, query, database)
	if err != nil {
		return fmt.Errorf("failed to delete policy: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("policy not found for database: %s", database)
	}

	// Remove from cache
	delete(s.cache, database)

	s.logger.Info().
		Str("database", database).
		Msg("Tiering policy deleted, reverting to global defaults")

	return nil
}

// List returns all custom policies
func (s *PolicyStore) List(ctx context.Context) ([]DatabasePolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	policies := make([]DatabasePolicy, 0, len(s.cache))
	for _, policy := range s.cache {
		policies = append(policies, *policy)
	}
	return policies, nil
}

// listFromDB retrieves all policies from the database
func (s *PolicyStore) listFromDB(ctx context.Context) ([]DatabasePolicy, error) {
	query := `
		SELECT id, database, hot_only, hot_max_age_days, created_at, updated_at
		FROM tiering_policies
		ORDER BY database
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list policies: %w", err)
	}
	defer rows.Close()

	var policies []DatabasePolicy
	for rows.Next() {
		var policy DatabasePolicy
		var hotMaxAge sql.NullInt64

		err := rows.Scan(
			&policy.ID,
			&policy.Database,
			&policy.HotOnly,
			&hotMaxAge,
			&policy.CreatedAt,
			&policy.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan policy: %w", err)
		}

		if hotMaxAge.Valid {
			val := int(hotMaxAge.Int64)
			policy.HotMaxAgeDays = &val
		}

		policies = append(policies, policy)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating policies: %w", err)
	}

	return policies, nil
}

// GetEffective returns the effective policy for a database
// This resolves custom policies with global defaults
func (s *PolicyStore) GetEffective(ctx context.Context, database string) *EffectivePolicy {
	policy, _ := s.Get(ctx, database)

	effective := &EffectivePolicy{
		Database:      database,
		HotMaxAgeDays: s.defaults.DefaultHotMaxAgeDays,
		Source:        "global",
	}

	if policy != nil {
		effective.Source = "custom"
		effective.HotOnly = policy.HotOnly

		if policy.HotMaxAgeDays != nil {
			effective.HotMaxAgeDays = *policy.HotMaxAgeDays
		}
	}

	return effective
}

// IsHotOnly returns true if the database is excluded from tiering
func (s *PolicyStore) IsHotOnly(ctx context.Context, database string) bool {
	policy, err := s.Get(ctx, database)
	if err != nil || policy == nil {
		return false
	}
	return policy.HotOnly
}

// GetHotMaxAge returns the hot tier max age for a database in days
func (s *PolicyStore) GetHotMaxAge(ctx context.Context, database string) int {
	effective := s.GetEffective(ctx, database)
	return effective.HotMaxAgeDays
}

// helper functions

func (s *PolicyStore) scanPolicy(row *sql.Row) (*DatabasePolicy, error) {
	var policy DatabasePolicy
	var hotMaxAge sql.NullInt64

	err := row.Scan(
		&policy.ID,
		&policy.Database,
		&policy.HotOnly,
		&hotMaxAge,
		&policy.CreatedAt,
		&policy.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan policy: %w", err)
	}

	if hotMaxAge.Valid {
		val := int(hotMaxAge.Int64)
		policy.HotMaxAgeDays = &val
	}

	return &policy, nil
}
