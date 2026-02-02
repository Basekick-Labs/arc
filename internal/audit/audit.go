package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// AuditEvent represents a single auditable action
type AuditEvent struct {
	Timestamp   time.Time         `json:"timestamp"`
	EventType   string            `json:"event_type"`
	Actor       string            `json:"actor"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Database    string            `json:"database,omitempty"`
	Measurement string            `json:"measurement,omitempty"`
	StatusCode  int               `json:"status_code"`
	IPAddress   string            `json:"ip_address"`
	UserAgent   string            `json:"user_agent"`
	DurationMs  int64             `json:"duration_ms"`
	Detail      map[string]string `json:"detail,omitempty"`
}

// AuditEntry is a stored audit log entry with ID
type AuditEntry struct {
	ID          int64     `json:"id"`
	Timestamp   time.Time `json:"timestamp"`
	EventType   string    `json:"event_type"`
	Actor       string    `json:"actor"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	Database    string    `json:"database,omitempty"`
	Measurement string    `json:"measurement,omitempty"`
	StatusCode  int       `json:"status_code"`
	IPAddress   string    `json:"ip_address"`
	UserAgent   string    `json:"user_agent"`
	DurationMs  int64     `json:"duration_ms"`
	Detail      string    `json:"detail,omitempty"`
}

// QueryFilter holds filter parameters for querying audit logs
type QueryFilter struct {
	EventType string
	Actor     string
	Database  string
	Since     time.Time
	Until     time.Time
	Limit     int
	Offset    int
}

// Logger is the core audit logging service
type Logger struct {
	db     *sql.DB
	config *config.AuditLogConfig
	logger zerolog.Logger

	eventCh chan *AuditEvent
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// LoggerConfig holds configuration for creating an audit logger
type LoggerConfig struct {
	DB     *sql.DB
	Config *config.AuditLogConfig
	Logger zerolog.Logger
}

// NewLogger creates a new audit logger
func NewLogger(cfg *LoggerConfig) (*Logger, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("configuration is required")
	}

	l := &Logger{
		db:      cfg.DB,
		config:  cfg.Config,
		logger:  cfg.Logger.With().Str("component", "audit").Logger(),
		eventCh: make(chan *AuditEvent, 1000),
		stopCh:  make(chan struct{}),
	}

	if err := l.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize audit schema: %w", err)
	}

	l.logger.Info().Msg("Audit logger created")
	return l, nil
}

func (l *Logger) initSchema() error {
	// Enable incremental auto-vacuum to reclaim disk space after retention cleanup.
	// Must be set before creating tables (only takes effect on new databases).
	if _, err := l.db.Exec("PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
		l.logger.Warn().Err(err).Msg("Failed to set auto_vacuum pragma")
	}

	schema := `
	CREATE TABLE IF NOT EXISTS audit_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		event_type TEXT NOT NULL,
		actor TEXT,
		method TEXT NOT NULL,
		path TEXT NOT NULL,
		database_name TEXT,
		measurement TEXT,
		status_code INTEGER,
		ip_address TEXT,
		user_agent TEXT,
		duration_ms INTEGER,
		detail TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs(timestamp);
	CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_logs(event_type);
	CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs(actor);
	CREATE INDEX IF NOT EXISTS idx_audit_database ON audit_logs(database_name);
	`

	_, err := l.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create audit tables: %w", err)
	}

	return nil
}

// Start starts the background writer and retention cleanup goroutines
func (l *Logger) Start() error {
	l.wg.Add(1)
	go l.writerLoop()

	l.wg.Add(1)
	go l.retentionLoop()

	l.logger.Info().
		Int("retention_days", l.config.RetentionDays).
		Bool("include_reads", l.config.IncludeReads).
		Msg("Audit logger started")

	return nil
}

// Stop gracefully shuts down the audit logger
func (l *Logger) Stop() error {
	close(l.stopCh)
	l.wg.Wait()
	l.logger.Info().Msg("Audit logger stopped")
	return nil
}

// LogEvent queues an audit event for async writing
func (l *Logger) LogEvent(event *AuditEvent) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	select {
	case l.eventCh <- event:
	default:
		l.logger.Warn().Str("event_type", event.EventType).Msg("Audit event channel full, dropping event")
	}
}

// writerLoop processes events from the channel and batch-inserts them
func (l *Logger) writerLoop() {
	defer l.wg.Done()

	batch := make([]*AuditEvent, 0, 100)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event := <-l.eventCh:
			batch = append(batch, event)
			if len(batch) >= 100 {
				l.flushBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				l.flushBatch(batch)
				batch = batch[:0]
			}
		case <-l.stopCh:
			// Drain remaining events
			for {
				select {
				case event := <-l.eventCh:
					batch = append(batch, event)
				default:
					if len(batch) > 0 {
						l.flushBatch(batch)
					}
					return
				}
			}
		}
	}
}

func (l *Logger) flushBatch(batch []*AuditEvent) {
	if len(batch) == 0 {
		return
	}

	tx, err := l.db.Begin()
	if err != nil {
		l.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to begin audit batch transaction")
		return
	}

	stmt, err := tx.Prepare(`
		INSERT INTO audit_logs (timestamp, event_type, actor, method, path, database_name, measurement, status_code, ip_address, user_agent, duration_ms, detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		l.logger.Error().Err(err).Msg("Failed to prepare audit insert statement")
		return
	}
	defer stmt.Close()

	for _, event := range batch {
		var detailJSON string
		if len(event.Detail) > 0 {
			if b, err := json.Marshal(event.Detail); err == nil {
				detailJSON = string(b)
			}
		}

		_, err := stmt.Exec(
			event.Timestamp.UTC(),
			event.EventType,
			event.Actor,
			event.Method,
			event.Path,
			event.Database,
			event.Measurement,
			event.StatusCode,
			event.IPAddress,
			event.UserAgent,
			event.DurationMs,
			detailJSON,
		)
		if err != nil {
			l.logger.Error().Err(err).Str("event_type", event.EventType).Msg("Failed to insert audit event")
		}
	}

	if err := tx.Commit(); err != nil {
		l.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to commit audit batch")
	}
}

// retentionLoop periodically deletes old audit entries
func (l *Logger) retentionLoop() {
	defer l.wg.Done()

	// Run cleanup and vacuum once on startup to reclaim space from previous runs
	l.cleanupOldEntries()
	if _, err := l.db.Exec("VACUUM"); err != nil {
		l.logger.Warn().Err(err).Msg("Failed to vacuum audit database on startup")
	}

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.cleanupOldEntries()
		case <-l.stopCh:
			return
		}
	}
}

func (l *Logger) cleanupOldEntries() {
	if l.config.RetentionDays <= 0 {
		return
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -l.config.RetentionDays)

	result, err := l.db.Exec("DELETE FROM audit_logs WHERE timestamp < ?", cutoff)
	if err != nil {
		l.logger.Error().Err(err).Msg("Failed to cleanup old audit entries")
		return
	}

	if rows, _ := result.RowsAffected(); rows > 0 {
		l.logger.Info().Int64("deleted", rows).Int("retention_days", l.config.RetentionDays).Msg("Cleaned up old audit entries")

		// Reclaim disk space freed by deleted rows
		if _, err := l.db.Exec("PRAGMA incremental_vacuum"); err != nil {
			l.logger.Warn().Err(err).Msg("Failed to run incremental vacuum")
		}
	}
}

// Query retrieves audit log entries matching the given filter
func (l *Logger) Query(ctx context.Context, filter *QueryFilter) ([]AuditEntry, error) {
	query := "SELECT id, timestamp, event_type, actor, method, path, database_name, measurement, status_code, ip_address, user_agent, duration_ms, detail FROM audit_logs WHERE 1=1"
	var args []interface{}

	if filter.EventType != "" {
		query += " AND event_type = ?"
		args = append(args, filter.EventType)
	}
	if filter.Actor != "" {
		query += " AND actor = ?"
		args = append(args, filter.Actor)
	}
	if filter.Database != "" {
		query += " AND database_name = ?"
		args = append(args, filter.Database)
	}
	if !filter.Since.IsZero() {
		query += " AND timestamp >= ?"
		args = append(args, filter.Since.UTC())
	}
	if !filter.Until.IsZero() {
		query += " AND timestamp <= ?"
		args = append(args, filter.Until.UTC())
	}

	query += " ORDER BY timestamp DESC"

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	query += " LIMIT ?"
	args = append(args, limit)

	if filter.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, filter.Offset)
	}

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query audit logs: %w", err)
	}
	defer rows.Close()

	var entries []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var actor, dbName, measurement, ipAddress, userAgent, detail sql.NullString
		var statusCode, durationMs sql.NullInt64

		if err := rows.Scan(&e.ID, &e.Timestamp, &e.EventType, &actor, &e.Method, &e.Path, &dbName, &measurement, &statusCode, &ipAddress, &userAgent, &durationMs, &detail); err != nil {
			return nil, fmt.Errorf("failed to scan audit entry: %w", err)
		}

		e.Actor = actor.String
		e.Database = dbName.String
		e.Measurement = measurement.String
		e.StatusCode = int(statusCode.Int64)
		e.IPAddress = ipAddress.String
		e.UserAgent = userAgent.String
		e.DurationMs = durationMs.Int64
		e.Detail = detail.String

		entries = append(entries, e)
	}

	return entries, rows.Err()
}

// Stats returns aggregate counts by event type
func (l *Logger) Stats(ctx context.Context, since time.Time) (map[string]int64, error) {
	query := "SELECT event_type, COUNT(*) FROM audit_logs"
	var args []interface{}

	if !since.IsZero() {
		query += " WHERE timestamp >= ?"
		args = append(args, since.UTC())
	}

	query += " GROUP BY event_type ORDER BY COUNT(*) DESC"

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query audit stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]int64)
	for rows.Next() {
		var eventType string
		var count int64
		if err := rows.Scan(&eventType, &count); err != nil {
			return nil, fmt.Errorf("failed to scan audit stat: %w", err)
		}
		stats[eventType] = count
	}

	return stats, rows.Err()
}
