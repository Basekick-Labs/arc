package mqtt

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// Repository handles MQTT subscription persistence
type Repository struct {
	db        *sql.DB
	encryptor PasswordEncryptor
	logger    zerolog.Logger
}

// NewRepository creates a new MQTT subscription repository
func NewRepository(dbPath string, encryptor PasswordEncryptor, logger zerolog.Logger) (*Repository, error) {
	return NewSQLiteRepository(dbPath, nil, logger)
}

// NewSQLiteRepository creates a new MQTT subscription repository with optional encryption key
func NewSQLiteRepository(dbPath string, encryptionKey []byte, logger zerolog.Logger) (*Repository, error) {
	encryptor, err := NewPasswordEncryptor(encryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create encryptor: %w", err)
	}
	return newRepository(dbPath, encryptor, logger)
}

// newRepository is the internal constructor
func newRepository(dbPath string, encryptor PasswordEncryptor, logger zerolog.Logger) (*Repository, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Limit connections for SQLite
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	repo := &Repository{
		db:        db,
		encryptor: encryptor,
		logger:    logger.With().Str("component", "mqtt-repository").Logger(),
	}

	// Initialize schema
	if err := repo.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return repo, nil
}

// initSchema creates the subscriptions table if it doesn't exist
func (r *Repository) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS mqtt_subscriptions (
		id TEXT PRIMARY KEY,
		name TEXT UNIQUE NOT NULL,
		broker TEXT NOT NULL,
		client_id TEXT NOT NULL,
		topics TEXT NOT NULL,
		qos INTEGER DEFAULT 1,
		database TEXT NOT NULL,
		username TEXT,
		password_encrypted TEXT,
		tls_enabled INTEGER DEFAULT 0,
		tls_cert_path TEXT,
		tls_key_path TEXT,
		tls_ca_path TEXT,
		tls_insecure_skip_verify INTEGER DEFAULT 0,
		auto_start INTEGER DEFAULT 1,
		status TEXT DEFAULT 'stopped',
		error_message TEXT,
		topic_mapping TEXT,
		keep_alive_seconds INTEGER DEFAULT 60,
		connect_timeout_seconds INTEGER DEFAULT 30,
		reconnect_min_seconds INTEGER DEFAULT 1,
		reconnect_max_seconds INTEGER DEFAULT 60,
		clean_session INTEGER DEFAULT 0,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_mqtt_subscriptions_name ON mqtt_subscriptions(name);
	CREATE INDEX IF NOT EXISTS idx_mqtt_subscriptions_status ON mqtt_subscriptions(status);
	CREATE INDEX IF NOT EXISTS idx_mqtt_subscriptions_auto_start ON mqtt_subscriptions(auto_start);
	`

	if _, err := r.db.Exec(schema); err != nil {
		return err
	}

	// Run migrations for existing tables
	return r.runMigrations()
}

// runMigrations applies schema migrations for existing tables
func (r *Repository) runMigrations() error {
	// Migration: Add tls_insecure_skip_verify column if missing
	migrations := []struct {
		name  string
		check string
		apply string
	}{
		{
			name:  "add_tls_insecure_skip_verify",
			check: "SELECT tls_insecure_skip_verify FROM mqtt_subscriptions LIMIT 1",
			apply: "ALTER TABLE mqtt_subscriptions ADD COLUMN tls_insecure_skip_verify INTEGER DEFAULT 0",
		},
		{
			name:  "add_clean_session",
			check: "SELECT clean_session FROM mqtt_subscriptions LIMIT 1",
			apply: "ALTER TABLE mqtt_subscriptions ADD COLUMN clean_session INTEGER DEFAULT 0",
		},
	}

	for _, m := range migrations {
		// Check if migration is needed
		_, err := r.db.Exec(m.check)
		if err != nil {
			// Column doesn't exist, apply migration
			r.logger.Info().Str("migration", m.name).Msg("Applying schema migration")
			if _, err := r.db.Exec(m.apply); err != nil {
				return fmt.Errorf("failed to apply migration %s: %w", m.name, err)
			}
		}
	}

	return nil
}

// Close closes the database connection
func (r *Repository) Close() error {
	return r.db.Close()
}

// Create creates a new subscription
func (r *Repository) Create(ctx context.Context, sub *Subscription) error {
	// Generate ID if not set
	if sub.ID == "" {
		sub.ID = uuid.New().String()
	}

	// Set timestamps
	now := time.Now().UTC()
	sub.CreatedAt = now
	sub.UpdatedAt = now

	// Serialize topics to JSON
	topicsJSON, err := json.Marshal(sub.Topics)
	if err != nil {
		return fmt.Errorf("failed to serialize topics: %w", err)
	}

	// Serialize topic mapping to JSON
	var topicMappingJSON []byte
	if sub.TopicMapping != nil {
		topicMappingJSON, err = json.Marshal(sub.TopicMapping)
		if err != nil {
			return fmt.Errorf("failed to serialize topic mapping: %w", err)
		}
	}

	query := `
	INSERT INTO mqtt_subscriptions (
		id, name, broker, client_id, topics, qos, database,
		username, password_encrypted, tls_enabled, tls_cert_path,
		tls_key_path, tls_ca_path, tls_insecure_skip_verify, auto_start,
		status, error_message, topic_mapping, keep_alive_seconds,
		connect_timeout_seconds, reconnect_min_seconds, reconnect_max_seconds,
		clean_session, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err = r.db.ExecContext(ctx, query,
		sub.ID, sub.Name, sub.Broker, sub.ClientID, string(topicsJSON),
		sub.QoS, sub.Database, sub.Username, sub.PasswordEncrypted,
		boolToInt(sub.TLSEnabled), sub.TLSCertPath, sub.TLSKeyPath,
		sub.TLSCAPath, boolToInt(sub.TLSInsecureSkipVerify),
		boolToInt(sub.AutoStart), string(sub.Status), sub.ErrorMessage,
		nullableString(topicMappingJSON), sub.KeepAliveSeconds,
		sub.ConnectTimeoutSeconds, sub.ReconnectMinSeconds,
		sub.ReconnectMaxSeconds, boolToInt(sub.CleanSession),
		sub.CreatedAt.Format(time.RFC3339),
		sub.UpdatedAt.Format(time.RFC3339),
	)

	if err != nil {
		if isUniqueConstraintError(err) {
			return fmt.Errorf("subscription with name '%s' already exists", sub.Name)
		}
		return fmt.Errorf("failed to create subscription: %w", err)
	}

	return nil
}

// Get retrieves a subscription by ID
func (r *Repository) Get(ctx context.Context, id string) (*Subscription, error) {
	query := `
	SELECT id, name, broker, client_id, topics, qos, database,
		username, password_encrypted, tls_enabled, tls_cert_path,
		tls_key_path, tls_ca_path, tls_insecure_skip_verify, auto_start,
		status, error_message, topic_mapping, keep_alive_seconds,
		connect_timeout_seconds, reconnect_min_seconds, reconnect_max_seconds,
		clean_session, created_at, updated_at
	FROM mqtt_subscriptions
	WHERE id = ?
	`

	return r.scanSubscription(r.db.QueryRowContext(ctx, query, id))
}

// GetByName retrieves a subscription by name
func (r *Repository) GetByName(ctx context.Context, name string) (*Subscription, error) {
	query := `
	SELECT id, name, broker, client_id, topics, qos, database,
		username, password_encrypted, tls_enabled, tls_cert_path,
		tls_key_path, tls_ca_path, tls_insecure_skip_verify, auto_start,
		status, error_message, topic_mapping, keep_alive_seconds,
		connect_timeout_seconds, reconnect_min_seconds, reconnect_max_seconds,
		clean_session, created_at, updated_at
	FROM mqtt_subscriptions
	WHERE name = ?
	`

	return r.scanSubscription(r.db.QueryRowContext(ctx, query, name))
}

// List retrieves all subscriptions
func (r *Repository) List(ctx context.Context) ([]*Subscription, error) {
	query := `
	SELECT id, name, broker, client_id, topics, qos, database,
		username, password_encrypted, tls_enabled, tls_cert_path,
		tls_key_path, tls_ca_path, tls_insecure_skip_verify, auto_start,
		status, error_message, topic_mapping, keep_alive_seconds,
		connect_timeout_seconds, reconnect_min_seconds, reconnect_max_seconds,
		clean_session, created_at, updated_at
	FROM mqtt_subscriptions
	ORDER BY name
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions: %w", err)
	}
	defer rows.Close()

	var subscriptions []*Subscription
	for rows.Next() {
		sub, err := r.scanSubscriptionRow(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, sub)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating subscriptions: %w", err)
	}

	return subscriptions, nil
}

// ListAutoStart retrieves all subscriptions with auto_start enabled
func (r *Repository) ListAutoStart(ctx context.Context) ([]*Subscription, error) {
	query := `
	SELECT id, name, broker, client_id, topics, qos, database,
		username, password_encrypted, tls_enabled, tls_cert_path,
		tls_key_path, tls_ca_path, tls_insecure_skip_verify, auto_start,
		status, error_message, topic_mapping, keep_alive_seconds,
		connect_timeout_seconds, reconnect_min_seconds, reconnect_max_seconds,
		clean_session, created_at, updated_at
	FROM mqtt_subscriptions
	WHERE auto_start = 1
	ORDER BY name
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list auto-start subscriptions: %w", err)
	}
	defer rows.Close()

	var subscriptions []*Subscription
	for rows.Next() {
		sub, err := r.scanSubscriptionRow(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, sub)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating subscriptions: %w", err)
	}

	return subscriptions, nil
}

// Update updates an existing subscription
func (r *Repository) Update(ctx context.Context, sub *Subscription) error {
	sub.UpdatedAt = time.Now().UTC()

	// Serialize topics to JSON
	topicsJSON, err := json.Marshal(sub.Topics)
	if err != nil {
		return fmt.Errorf("failed to serialize topics: %w", err)
	}

	// Serialize topic mapping to JSON
	var topicMappingJSON []byte
	if sub.TopicMapping != nil {
		topicMappingJSON, err = json.Marshal(sub.TopicMapping)
		if err != nil {
			return fmt.Errorf("failed to serialize topic mapping: %w", err)
		}
	}

	query := `
	UPDATE mqtt_subscriptions SET
		name = ?, broker = ?, client_id = ?, topics = ?, qos = ?,
		database = ?, username = ?, password_encrypted = ?,
		tls_enabled = ?, tls_cert_path = ?, tls_key_path = ?,
		tls_ca_path = ?, tls_insecure_skip_verify = ?, auto_start = ?,
		status = ?, error_message = ?, topic_mapping = ?,
		keep_alive_seconds = ?, connect_timeout_seconds = ?,
		reconnect_min_seconds = ?, reconnect_max_seconds = ?,
		clean_session = ?, updated_at = ?
	WHERE id = ?
	`

	result, err := r.db.ExecContext(ctx, query,
		sub.Name, sub.Broker, sub.ClientID, string(topicsJSON),
		sub.QoS, sub.Database, sub.Username, sub.PasswordEncrypted,
		boolToInt(sub.TLSEnabled), sub.TLSCertPath, sub.TLSKeyPath,
		sub.TLSCAPath, boolToInt(sub.TLSInsecureSkipVerify),
		boolToInt(sub.AutoStart), string(sub.Status), sub.ErrorMessage,
		nullableString(topicMappingJSON), sub.KeepAliveSeconds,
		sub.ConnectTimeoutSeconds, sub.ReconnectMinSeconds,
		sub.ReconnectMaxSeconds, boolToInt(sub.CleanSession),
		sub.UpdatedAt.Format(time.RFC3339),
		sub.ID,
	)

	if err != nil {
		if isUniqueConstraintError(err) {
			return fmt.Errorf("subscription with name '%s' already exists", sub.Name)
		}
		return fmt.Errorf("failed to update subscription: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// UpdateStatus updates only the status and error message
func (r *Repository) UpdateStatus(ctx context.Context, id string, status SubscriptionStatus, errorMsg string) error {
	query := `
	UPDATE mqtt_subscriptions
	SET status = ?, error_message = ?, updated_at = ?
	WHERE id = ?
	`

	result, err := r.db.ExecContext(ctx, query, string(status), errorMsg, time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// Delete deletes a subscription by ID
func (r *Repository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM mqtt_subscriptions WHERE id = ?`

	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete subscription: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// scanSubscription scans a single row into a Subscription
func (r *Repository) scanSubscription(row *sql.Row) (*Subscription, error) {
	var sub Subscription
	var topicsJSON string
	var topicMappingJSON sql.NullString
	var createdAt, updatedAt string
	var tlsEnabled, tlsInsecureSkipVerify, autoStart, cleanSession int
	var status string

	err := row.Scan(
		&sub.ID, &sub.Name, &sub.Broker, &sub.ClientID, &topicsJSON,
		&sub.QoS, &sub.Database, &sub.Username, &sub.PasswordEncrypted,
		&tlsEnabled, &sub.TLSCertPath, &sub.TLSKeyPath, &sub.TLSCAPath,
		&tlsInsecureSkipVerify, &autoStart, &status, &sub.ErrorMessage,
		&topicMappingJSON, &sub.KeepAliveSeconds, &sub.ConnectTimeoutSeconds,
		&sub.ReconnectMinSeconds, &sub.ReconnectMaxSeconds,
		&cleanSession, &createdAt, &updatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan subscription: %w", err)
	}

	// Parse topics JSON
	if err := json.Unmarshal([]byte(topicsJSON), &sub.Topics); err != nil {
		return nil, fmt.Errorf("failed to parse topics: %w", err)
	}

	// Parse topic mapping JSON if present
	if topicMappingJSON.Valid && topicMappingJSON.String != "" {
		if err := json.Unmarshal([]byte(topicMappingJSON.String), &sub.TopicMapping); err != nil {
			return nil, fmt.Errorf("failed to parse topic mapping: %w", err)
		}
	}

	// Parse timestamps
	sub.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	sub.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)

	// Convert integers to booleans
	sub.TLSEnabled = tlsEnabled == 1
	sub.TLSInsecureSkipVerify = tlsInsecureSkipVerify == 1
	sub.AutoStart = autoStart == 1
	sub.CleanSession = cleanSession == 1
	sub.Status = SubscriptionStatus(status)
	sub.HasPassword = sub.PasswordEncrypted != ""

	return &sub, nil
}

// scanSubscriptionRow scans from *sql.Rows instead of *sql.Row
func (r *Repository) scanSubscriptionRow(rows *sql.Rows) (*Subscription, error) {
	var sub Subscription
	var topicsJSON string
	var topicMappingJSON sql.NullString
	var createdAt, updatedAt string
	var tlsEnabled, tlsInsecureSkipVerify, autoStart, cleanSession int
	var status string

	err := rows.Scan(
		&sub.ID, &sub.Name, &sub.Broker, &sub.ClientID, &topicsJSON,
		&sub.QoS, &sub.Database, &sub.Username, &sub.PasswordEncrypted,
		&tlsEnabled, &sub.TLSCertPath, &sub.TLSKeyPath, &sub.TLSCAPath,
		&tlsInsecureSkipVerify, &autoStart, &status, &sub.ErrorMessage,
		&topicMappingJSON, &sub.KeepAliveSeconds, &sub.ConnectTimeoutSeconds,
		&sub.ReconnectMinSeconds, &sub.ReconnectMaxSeconds,
		&cleanSession, &createdAt, &updatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to scan subscription row: %w", err)
	}

	// Parse topics JSON
	if err := json.Unmarshal([]byte(topicsJSON), &sub.Topics); err != nil {
		return nil, fmt.Errorf("failed to parse topics: %w", err)
	}

	// Parse topic mapping JSON if present
	if topicMappingJSON.Valid && topicMappingJSON.String != "" {
		if err := json.Unmarshal([]byte(topicMappingJSON.String), &sub.TopicMapping); err != nil {
			return nil, fmt.Errorf("failed to parse topic mapping: %w", err)
		}
	}

	// Parse timestamps
	sub.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	sub.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)

	// Convert integers to booleans
	sub.TLSEnabled = tlsEnabled == 1
	sub.TLSInsecureSkipVerify = tlsInsecureSkipVerify == 1
	sub.AutoStart = autoStart == 1
	sub.CleanSession = cleanSession == 1
	sub.Status = SubscriptionStatus(status)
	sub.HasPassword = sub.PasswordEncrypted != ""

	return &sub, nil
}

// Helper functions

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullableString(b []byte) sql.NullString {
	if len(b) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}

func isUniqueConstraintError(err error) bool {
	return err != nil && (errors.Is(err, sql.ErrNoRows) ||
		// SQLite unique constraint error
		(err.Error() != "" && (strings.Contains(err.Error(), "UNIQUE constraint failed") ||
			strings.Contains(err.Error(), "unique constraint"))))
}
