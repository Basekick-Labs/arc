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

// MinRuntimeIngestBufferSize is the smallest supported runtime threshold.
// Values below this can create a flush task for nearly every incoming record
// and rapidly saturate the bounded flush queue.
const MinRuntimeIngestBufferSize = 1000

func validateRuntimeIngestConfig(cfg RuntimeIngestConfig) error {
	if cfg.MaxBufferSize < MinRuntimeIngestBufferSize {
		return fmt.Errorf("max_buffer_size must be at least %d", MinRuntimeIngestBufferSize)
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
			max_buffer_size INTEGER NOT NULL CHECK (max_buffer_size >= 1000),
			max_buffer_age_ms INTEGER NOT NULL CHECK (max_buffer_age_ms > 0),
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return nil, fmt.Errorf("create runtime ingest config table: %w", err)
	}
	return &RuntimeIngestConfigStore{db: db}, nil
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
