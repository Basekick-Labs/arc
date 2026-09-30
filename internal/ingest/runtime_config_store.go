package ingest

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RuntimeIngestConfig contains the ingest settings that the runtime API can
// change without rebuilding the Arrow writer.
type RuntimeIngestConfig struct {
	MaxBufferSize  int `json:"max_buffer_size"`
	MaxBufferAgeMS int `json:"max_buffer_age_ms"`
}

func validateRuntimeIngestConfig(cfg RuntimeIngestConfig) error {
	if cfg.MaxBufferSize <= 0 {
		return fmt.Errorf("max_buffer_size must be greater than zero")
	}
	if cfg.MaxBufferAgeMS <= 0 {
		return fmt.Errorf("max_buffer_age_ms must be greater than zero")
	}
	maxInt64 := int64(^uint64(0) >> 1)
	if int64(cfg.MaxBufferAgeMS) > maxInt64/int64(time.Millisecond) {
		return fmt.Errorf("max_buffer_age_ms is too large")
	}
	return nil
}

// RuntimeIngestConfigStore persists the runtime override in Arc's metadata
// SQLite database. A missing row means the process should use its startup
// configuration (arc.toml, environment, or defaults).
type RuntimeIngestConfigStore struct {
	db *sql.DB
}

// NewRuntimeIngestConfigStore creates the metadata table used for the optional
// runtime override.
func NewRuntimeIngestConfigStore(db *sql.DB) (*RuntimeIngestConfigStore, error) {
	if db == nil {
		return nil, fmt.Errorf("runtime ingest config database is nil")
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS arc_runtime_ingest_config (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			max_buffer_size INTEGER NOT NULL CHECK (max_buffer_size > 0),
			max_buffer_age_ms INTEGER NOT NULL CHECK (max_buffer_age_ms > 0),
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return nil, fmt.Errorf("create runtime ingest config table: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS arc_runtime_ingest_elastic_reserve (
			id INTEGER PRIMARY KEY CHECK (id = 1),
		enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
			capacity_records INTEGER NOT NULL CHECK (capacity_records >= 0),
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return nil, fmt.Errorf("create runtime ingest elastic reserve table: %w", err)
	}
	return &RuntimeIngestConfigStore{db: db}, nil
}

// LoadElasticReserve returns the saved reserve override, if one exists.
func (s *RuntimeIngestConfigStore) LoadElasticReserve() (RuntimeElasticReserveConfig, bool, error) {
	if s == nil || s.db == nil {
		return RuntimeElasticReserveConfig{}, false, fmt.Errorf("runtime ingest config store is unavailable")
	}
	var enabled int
	var cfg RuntimeElasticReserveConfig
	err := s.db.QueryRow(`
		SELECT enabled, capacity_records
		FROM arc_runtime_ingest_elastic_reserve WHERE id = 1
	`).Scan(&enabled, &cfg.CapacityRecords)
	if errors.Is(err, sql.ErrNoRows) {
		return RuntimeElasticReserveConfig{}, false, nil
	}
	if err != nil {
		return RuntimeElasticReserveConfig{}, false, fmt.Errorf("load runtime ingest elastic reserve: %w", err)
	}
	cfg.Enabled = enabled == 1
	if err := validateRuntimeElasticReserveConfig(cfg); err != nil {
		return RuntimeElasticReserveConfig{}, false, fmt.Errorf("invalid persisted runtime ingest elastic reserve: %w", err)
	}
	return cfg, true, nil
}

// SaveElasticReserve atomically replaces the independent reserve override.
func (s *RuntimeIngestConfigStore) SaveElasticReserve(cfg RuntimeElasticReserveConfig) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("runtime ingest config store is unavailable")
	}
	if err := validateRuntimeElasticReserveConfig(cfg); err != nil {
		return err
	}
	enabled := 0
	if cfg.Enabled {
		enabled = 1
	}
	_, err := s.db.Exec(`
		INSERT INTO arc_runtime_ingest_elastic_reserve (id, enabled, capacity_records, updated_at)
		VALUES (1, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			enabled = excluded.enabled,
			capacity_records = excluded.capacity_records,
			updated_at = CURRENT_TIMESTAMP
	`, enabled, cfg.CapacityRecords)
	if err != nil {
		return fmt.Errorf("save runtime ingest elastic reserve: %w", err)
	}
	return nil
}

// DeleteElasticReserve removes only the reserve override. It is deliberately
// separate from Delete, which resets max_buffer_size and max_buffer_age_ms.
func (s *RuntimeIngestConfigStore) DeleteElasticReserve() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("runtime ingest config store is unavailable")
	}
	if _, err := s.db.Exec(`DELETE FROM arc_runtime_ingest_elastic_reserve WHERE id = 1`); err != nil {
		return fmt.Errorf("delete runtime ingest elastic reserve: %w", err)
	}
	return nil
}

// Load returns the saved override, if one exists.
func (s *RuntimeIngestConfigStore) Load() (RuntimeIngestConfig, bool, error) {
	if s == nil || s.db == nil {
		return RuntimeIngestConfig{}, false, fmt.Errorf("runtime ingest config store is unavailable")
	}
	var cfg RuntimeIngestConfig
	err := s.db.QueryRow(`
		SELECT max_buffer_size, max_buffer_age_ms
		FROM arc_runtime_ingest_config WHERE id = 1
	`).Scan(&cfg.MaxBufferSize, &cfg.MaxBufferAgeMS)
	if errors.Is(err, sql.ErrNoRows) {
		return RuntimeIngestConfig{}, false, nil
	}
	if err != nil {
		return RuntimeIngestConfig{}, false, fmt.Errorf("load runtime ingest config: %w", err)
	}
	if err := validateRuntimeIngestConfig(cfg); err != nil {
		return RuntimeIngestConfig{}, false, fmt.Errorf("invalid persisted runtime ingest config: %w", err)
	}
	return cfg, true, nil
}

// Save atomically replaces the durable override.
func (s *RuntimeIngestConfigStore) Save(cfg RuntimeIngestConfig) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("runtime ingest config store is unavailable")
	}
	if err := validateRuntimeIngestConfig(cfg); err != nil {
		return err
	}
	_, err := s.db.Exec(`
		INSERT INTO arc_runtime_ingest_config
			(id, max_buffer_size, max_buffer_age_ms, updated_at)
		VALUES (1, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			max_buffer_size = excluded.max_buffer_size,
			max_buffer_age_ms = excluded.max_buffer_age_ms,
			updated_at = CURRENT_TIMESTAMP
	`, cfg.MaxBufferSize, cfg.MaxBufferAgeMS)
	if err != nil {
		return fmt.Errorf("save runtime ingest config: %w", err)
	}
	return nil
}

// Delete removes the durable override. Arc then returns to the startup
// configuration for the current process and after subsequent restarts.
func (s *RuntimeIngestConfigStore) Delete() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("runtime ingest config store is unavailable")
	}
	if _, err := s.db.Exec(`DELETE FROM arc_runtime_ingest_config WHERE id = 1`); err != nil {
		return fmt.Errorf("delete runtime ingest config: %w", err)
	}
	return nil
}
