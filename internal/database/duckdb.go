package database

import (
	"database/sql"
	"fmt"
	"runtime"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

// DuckDB manages DuckDB connections and query execution
// Note: No mutex is needed here because:
// 1. *sql.DB maintains its own connection pool with internal synchronization
// 2. DuckDB handles concurrent queries internally
// 3. Adding a mutex would only add overhead without safety benefits
type DuckDB struct {
	db     *sql.DB
	logger zerolog.Logger
	config *Config
}

// Config holds DuckDB configuration
type Config struct {
	MaxConnections int
	MemoryLimit    string
	ThreadCount    int
	EnableWAL      bool
	// S3 configuration for httpfs extension
	S3Region    string
	S3AccessKey string
	S3SecretKey string
	S3Endpoint  string // Custom endpoint for MinIO or S3-compatible services
	S3UseSSL    bool
	S3PathStyle bool // Use path-style addressing (required for MinIO)
	// Azure Blob Storage configuration for azure extension
	AzureAccountName string
	AzureAccountKey  string
	AzureEndpoint    string // Custom endpoint (optional)
}

// New creates a new DuckDB instance
func New(cfg *Config, logger zerolog.Logger) (*DuckDB, error) {
	// Build connection string with configuration
	dsn := buildDSN(cfg)

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Set connection pool limits optimized for query-heavy workloads
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxConnections) // Keep all connections idle-ready to avoid acquisition overhead
	db.SetConnMaxLifetime(0)               // No lifetime limit - DuckDB handles connection health internally
	db.SetConnMaxIdleTime(10 * time.Minute) // Longer idle time to reduce connection churn

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	// Configure database settings (memory limit, threads)
	if err := configureDatabase(db, cfg, logger); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure duckdb: %w", err)
	}

	s3Enabled := cfg.S3AccessKey != "" && cfg.S3SecretKey != ""
	azureEnabled := cfg.AzureAccountName != "" && cfg.AzureAccountKey != ""
	logger.Info().
		Int("max_connections", cfg.MaxConnections).
		Str("memory_limit", cfg.MemoryLimit).
		Int("thread_count", cfg.ThreadCount).
		Bool("wal_enabled", cfg.EnableWAL).
		Bool("s3_enabled", s3Enabled).
		Str("s3_region", cfg.S3Region).
		Bool("azure_enabled", azureEnabled).
		Str("azure_account", cfg.AzureAccountName).
		Msg("DuckDB initialized")

	return &DuckDB{
		db:     db,
		logger: logger,
		config: cfg,
	}, nil
}

// buildDSN constructs the DuckDB connection string
// NOTE: DuckDB memory_limit and threads must be set via SET commands after connection
func buildDSN(_ *Config) string {
	// In-memory database - settings applied via configureDatabase()
	return ""
}

// configureDatabase sets DuckDB configuration after connection
func configureDatabase(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	// Set memory limit to prevent unbounded memory growth
	if cfg.MemoryLimit != "" {
		if _, err := db.Exec(fmt.Sprintf("SET memory_limit='%s'", cfg.MemoryLimit)); err != nil {
			return fmt.Errorf("failed to set memory_limit: %w", err)
		}
	}
	// Set thread count
	if cfg.ThreadCount > 0 {
		logger.Info().Int("threads", cfg.ThreadCount).Msg("Setting DuckDB thread count")
		if _, err := db.Exec(fmt.Sprintf("SET threads=%d", cfg.ThreadCount)); err != nil {
			return fmt.Errorf("failed to set threads: %w", err)
		}
	}

	// Configure httpfs extension for S3 access if credentials are provided
	if cfg.S3AccessKey != "" && cfg.S3SecretKey != "" {
		if err := configureS3Access(db, cfg); err != nil {
			return fmt.Errorf("failed to configure S3 access: %w", err)
		}
	}

	// Configure azure extension for Azure Blob Storage access if credentials are provided
	if cfg.AzureAccountName != "" && cfg.AzureAccountKey != "" {
		if err := configureAzureAccess(db, cfg, logger); err != nil {
			return fmt.Errorf("failed to configure Azure access: %w", err)
		}
	}

	return nil
}

// configureS3Access sets up the httpfs extension for S3 access
// Note: We use SET GLOBAL to ensure settings persist across all connections in the pool
func configureS3Access(db *sql.DB, cfg *Config) error {
	// Install and load the httpfs extension
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		return fmt.Errorf("failed to install httpfs: %w", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		return fmt.Errorf("failed to load httpfs: %w", err)
	}

	// Set S3 credentials using GLOBAL scope to persist across connections
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_access_key_id='%s'", cfg.S3AccessKey)); err != nil {
		return fmt.Errorf("failed to set s3_access_key_id: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_secret_access_key='%s'", cfg.S3SecretKey)); err != nil {
		return fmt.Errorf("failed to set s3_secret_access_key: %w", err)
	}

	// Set S3 region
	if cfg.S3Region != "" {
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_region='%s'", cfg.S3Region)); err != nil {
			return fmt.Errorf("failed to set s3_region: %w", err)
		}
	}

	// Set custom endpoint for MinIO or S3-compatible services
	if cfg.S3Endpoint != "" {
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", cfg.S3Endpoint)); err != nil {
			return fmt.Errorf("failed to set s3_endpoint: %w", err)
		}
	}

	// Set URL style (path-style for MinIO, virtual-hosted for AWS S3)
	urlStyle := "vhost"
	if cfg.S3PathStyle {
		urlStyle = "path"
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_url_style='%s'", urlStyle)); err != nil {
		return fmt.Errorf("failed to set s3_url_style: %w", err)
	}

	// Set SSL usage
	useSSL := "true"
	if !cfg.S3UseSSL {
		useSSL = "false"
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_use_ssl=%s", useSSL)); err != nil {
		return fmt.Errorf("failed to set s3_use_ssl: %w", err)
	}

	return nil
}

// configureAzureAccess sets up the azure extension for Azure Blob Storage access
// Note: We use SET GLOBAL to ensure settings persist across all connections in the pool
func configureAzureAccess(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	// Install and load the azure extension
	if _, err := db.Exec("INSTALL azure"); err != nil {
		return fmt.Errorf("failed to install azure: %w", err)
	}
	if _, err := db.Exec("LOAD azure"); err != nil {
		return fmt.Errorf("failed to load azure: %w", err)
	}

	// Set transport option to curl on Linux to resolve potential SSL certificate issues
	if runtime.GOOS == "linux" {
		if _, err := db.Exec("SET GLOBAL azure_transport_option_type = 'curl'"); err != nil {
			return fmt.Errorf("failed to set azure_transport_option_type: %w", err)
		}

		logger.Info().
			Str("azure_transport_option", "curl").
			Msg("Azure transport option set to curl for Linux")
	}

	// Create a secret for Azure Blob Storage authentication
	var secretSQL string
	if cfg.AzureAccountKey != "" {
		// Use connection string with account key
		connStr := fmt.Sprintf("AccountName=%s;AccountKey=%s", cfg.AzureAccountName, cfg.AzureAccountKey)
		secretSQL = fmt.Sprintf(`
			CREATE SECRET azure_secret (
				TYPE AZURE,
				CONNECTION_STRING '%s'
			)
		`, connStr)
	} else {
		// Fall back to credential chain if no account key
		secretSQL = fmt.Sprintf(`
			CREATE SECRET azure_secret (
				TYPE AZURE,
				PROVIDER CREDENTIAL_CHAIN,
				ACCOUNT_NAME '%s'
			)
		`, cfg.AzureAccountName)
	}

	if _, err := db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to create azure secret: %w", err)
	}

	return nil
}

// Query executes a query and returns rows
func (d *DuckDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := d.db.Query(query, args...)
	elapsed := time.Since(start)

	if err != nil {
		d.logger.Error().
			Err(err).
			Str("query", query).
			Dur("elapsed", elapsed).
			Msg("Query failed")
		return nil, fmt.Errorf("query failed: %w", err)
	}

	d.logger.Debug().
		Str("query", query).
		Dur("elapsed", elapsed).
		Msg("Query executed")

	return rows, nil
}

// Exec executes a statement without returning rows
func (d *DuckDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	result, err := d.db.Exec(query, args...)
	elapsed := time.Since(start)

	if err != nil {
		d.logger.Error().
			Err(err).
			Str("query", query).
			Dur("elapsed", elapsed).
			Msg("Exec failed")
		return nil, fmt.Errorf("exec failed: %w", err)
	}

	d.logger.Debug().
		Str("query", query).
		Dur("elapsed", elapsed).
		Msg("Exec completed")

	return result, nil
}

// Close closes the database connection
func (d *DuckDB) Close() error {
	if err := d.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	d.logger.Info().Msg("DuckDB closed")
	return nil
}

// Stats returns database statistics
func (d *DuckDB) Stats() sql.DBStats {
	return d.db.Stats()
}

// DB returns the underlying *sql.DB connection pool
// This is used for passing to components that need direct DB access (e.g., compaction)
func (d *DuckDB) DB() *sql.DB {
	return d.db
}
