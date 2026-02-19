package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

// QueryProfile contains timing breakdown for a query execution
type QueryProfile struct {
	TotalMs     float64 `json:"total_ms"`
	PlannerMs   float64 `json:"planner_ms"`
	ExecutionMs float64 `json:"execution_ms"`
	RowsScanned uint64  `json:"rows_scanned"`
	Latency     float64 `json:"latency_ms"` // DuckDB reported latency
}

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

// escapeSQLString escapes single quotes for safe use in DuckDB SQL strings.
// This prevents SQL injection when interpolating configuration values.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
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
	// Query optimization configuration
	EnableS3Cache     bool  // Enable S3 file caching via cache_httpfs extension
	S3CacheSize       int64 // Cache size in bytes
	S3CacheTTLSeconds int   // Cache entry TTL in seconds (default: 3600)
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
		Bool("s3_cache_enabled", cfg.EnableS3Cache).
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
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL memory_limit='%s'", cfg.MemoryLimit)); err != nil {
			return fmt.Errorf("failed to set memory_limit: %w", err)
		}
	}
	// Set thread count
	if cfg.ThreadCount > 0 {
		logger.Info().Int("threads", cfg.ThreadCount).Msg("Setting DuckDB thread count")
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL threads=%d", cfg.ThreadCount)); err != nil {
			return fmt.Errorf("failed to set threads: %w", err)
		}
	}

	// Cache Parquet file metadata (schema, row group info) to reduce I/O on repeated access
	if _, err := db.Exec("SET GLOBAL parquet_metadata_cache=true"); err != nil {
		logger.Warn().Err(err).Msg("Failed to enable parquet metadata cache (continuing without it)")
	}

	// Preserve insertion order for deterministic results (important for LIMIT queries)
	if _, err := db.Exec("SET GLOBAL preserve_insertion_order=true"); err != nil {
		logger.Warn().Err(err).Msg("Failed to set preserve_insertion_order")
	}


	// Configure httpfs extension for S3 access if credentials are provided
	if cfg.S3AccessKey != "" && cfg.S3SecretKey != "" {
		if err := configureS3Access(db, cfg, logger); err != nil {
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
func configureS3Access(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	// Install and load the httpfs extension
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		return fmt.Errorf("failed to install httpfs: %w", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		return fmt.Errorf("failed to load httpfs: %w", err)
	}

	// Set S3 credentials using GLOBAL scope to persist across connections
	// Note: credentials are escaped to prevent SQL injection
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_access_key_id='%s'", escapeSQLString(cfg.S3AccessKey))); err != nil {
		return fmt.Errorf("failed to set s3_access_key_id: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_secret_access_key='%s'", escapeSQLString(cfg.S3SecretKey))); err != nil {
		return fmt.Errorf("failed to set s3_secret_access_key: %w", err)
	}

	// Set S3 region
	if cfg.S3Region != "" {
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_region='%s'", escapeSQLString(cfg.S3Region))); err != nil {
			return fmt.Errorf("failed to set s3_region: %w", err)
		}
	}

	// Set custom endpoint for MinIO or S3-compatible services
	if cfg.S3Endpoint != "" {
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", escapeSQLString(cfg.S3Endpoint))); err != nil {
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

	if _, err := db.Exec("SET GLOBAL prefetch_all_parquet_files=true"); err != nil {
		logger.Warn().Err(err).Msg("Failed to set prefetch_all_parquet_files")
	}

	// Configure cache_httpfs extension for S3 file caching if enabled
	if cfg.EnableS3Cache {
		logger.Info().Msg("Enabling S3 file caching via cache_httpfs extension")
		if _, err := db.Exec("INSTALL cache_httpfs FROM community"); err != nil {
			logger.Warn().Err(err).Msg("Failed to install cache_httpfs extension, continuing without cache")
		} else if _, err := db.Exec("LOAD cache_httpfs"); err != nil {
			logger.Warn().Err(err).Msg("Failed to load cache_httpfs extension, continuing without cache")
		} else {
			if _, err := db.Exec("SET GLOBAL cache_httpfs_type='in_mem'"); err != nil {
				logger.Warn().Err(err).Msg("Failed to set cache_httpfs_type to in_mem")
			}
			// Calculate max blocks from cache size (each block is 512KB)
			if cfg.S3CacheSize > 0 {
				maxBlocks := cfg.S3CacheSize / (512 * 1024) // 512KB per block
				if maxBlocks > 0 {
					if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_max_in_mem_cache_block_count=%d", maxBlocks)); err != nil {
						logger.Warn().Err(err).Int64("max_blocks", maxBlocks).Msg("Failed to set cache_httpfs_max_in_mem_cache_block_count")
					}
				} else {
					logger.Warn().
						Int64("configured_bytes", cfg.S3CacheSize).
						Msg("S3 cache size too small (minimum 512KB), increase s3_cache_size for caching to take effect")
				}
			}
			if cfg.S3CacheTTLSeconds > 0 {
				ttlMs := cfg.S3CacheTTLSeconds * 1000
				if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_in_mem_cache_block_timeout_millisec=%d", ttlMs)); err != nil {
					logger.Warn().Err(err).Int("ttl_ms", ttlMs).Msg("Failed to set cache_httpfs_in_mem_cache_block_timeout_millisec")
				}
				// Link glob, metadata, and file handle cache TTLs to the same value.
				// Arc's parquet files are immutable — shorter default TTLs waste S3 HEAD/LIST requests.
				// Post-compaction cache invalidation handles stale entries.
				if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_glob_cache_entry_timeout_millisec=%d", ttlMs)); err != nil {
					logger.Warn().Err(err).Msg("Failed to set cache_httpfs_glob_cache_entry_timeout_millisec")
				}
				if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_metadata_cache_entry_timeout_millisec=%d", ttlMs)); err != nil {
					logger.Warn().Err(err).Msg("Failed to set cache_httpfs_metadata_cache_entry_timeout_millisec")
				}
				if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_file_handle_cache_entry_timeout_millisec=%d", ttlMs)); err != nil {
					logger.Warn().Err(err).Msg("Failed to set cache_httpfs_file_handle_cache_entry_timeout_millisec")
				}
			}
			logger.Info().
				Int64("cache_size_bytes", cfg.S3CacheSize).
				Int("ttl_seconds", cfg.S3CacheTTLSeconds).
				Msg("cache_httpfs extension loaded with in_mem mode")
		}
	}

	return nil
}

// S3Config holds S3 configuration for DuckDB httpfs extension
type S3Config struct {
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	PathStyle bool
}

// ConfigureS3 reconfigures DuckDB's S3 settings at runtime.
// This is useful when tiered storage uses different S3 credentials than the main storage.
// The httpfs extension must already be loaded.
func (d *DuckDB) ConfigureS3(s3cfg *S3Config) error {
	// Set S3 credentials (escaped to prevent SQL injection)
	if s3cfg.AccessKey != "" {
		if _, err := d.db.Exec(fmt.Sprintf("SET GLOBAL s3_access_key_id='%s'", escapeSQLString(s3cfg.AccessKey))); err != nil {
			return fmt.Errorf("failed to set s3_access_key_id: %w", err)
		}
	}
	if s3cfg.SecretKey != "" {
		if _, err := d.db.Exec(fmt.Sprintf("SET GLOBAL s3_secret_access_key='%s'", escapeSQLString(s3cfg.SecretKey))); err != nil {
			return fmt.Errorf("failed to set s3_secret_access_key: %w", err)
		}
	}

	// Set S3 region
	if s3cfg.Region != "" {
		if _, err := d.db.Exec(fmt.Sprintf("SET GLOBAL s3_region='%s'", escapeSQLString(s3cfg.Region))); err != nil {
			return fmt.Errorf("failed to set s3_region: %w", err)
		}
	}

	// Set custom endpoint for MinIO or S3-compatible services
	if s3cfg.Endpoint != "" {
		if _, err := d.db.Exec(fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", escapeSQLString(s3cfg.Endpoint))); err != nil {
			return fmt.Errorf("failed to set s3_endpoint: %w", err)
		}
	}

	// Set URL style (path-style for MinIO, virtual-hosted for AWS S3)
	urlStyle := "vhost"
	if s3cfg.PathStyle {
		urlStyle = "path"
	}
	if _, err := d.db.Exec(fmt.Sprintf("SET GLOBAL s3_url_style='%s'", urlStyle)); err != nil {
		return fmt.Errorf("failed to set s3_url_style: %w", err)
	}

	// Set SSL usage
	useSSL := "true"
	if !s3cfg.UseSSL {
		useSSL = "false"
	}
	if _, err := d.db.Exec(fmt.Sprintf("SET GLOBAL s3_use_ssl=%s", useSSL)); err != nil {
		return fmt.Errorf("failed to set s3_use_ssl: %w", err)
	}

	d.logger.Info().
		Str("region", s3cfg.Region).
		Str("endpoint", s3cfg.Endpoint).
		Bool("path_style", s3cfg.PathStyle).
		Bool("use_ssl", s3cfg.UseSSL).
		Msg("DuckDB S3 configuration updated")

	return nil
}

// ClearHTTPCache clears DuckDB's cache_httpfs and parquet_metadata_cache.
// This should be called after compaction deletes files to prevent stale cache hits
// (glob results, file metadata, and data blocks pointing to deleted parquet files).
// Safe to call even if cache_httpfs is not loaded — the error is silently ignored.
func (d *DuckDB) ClearHTTPCache() {
	// Clear cache_httpfs (glob results, data blocks, file handles, file metadata)
	if _, err := d.db.Exec("SELECT cache_httpfs_clear_cache()"); err != nil {
		d.logger.Debug().Err(err).Msg("cache_httpfs_clear_cache not available (extension may not be loaded)")
	} else {
		d.logger.Info().Msg("Cleared cache_httpfs cache after compaction")
	}

	// Reset parquet_metadata_cache by toggling off/on to clear cached schema for deleted files
	if _, err := d.db.Exec("SET GLOBAL parquet_metadata_cache=false"); err != nil {
		d.logger.Debug().Err(err).Msg("Failed to disable parquet_metadata_cache")
	} else {
		if _, err := d.db.Exec("SET GLOBAL parquet_metadata_cache=true"); err != nil {
			d.logger.Warn().Err(err).Msg("Failed to re-enable parquet_metadata_cache")
		} else {
			d.logger.Info().Msg("Reset parquet_metadata_cache after compaction")
		}
	}
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
	// Note: values are escaped to prevent SQL injection
	var secretSQL string
	if cfg.AzureAccountKey != "" {
		// Use connection string with account key
		connStr := fmt.Sprintf("AccountName=%s;AccountKey=%s",
			escapeSQLString(cfg.AzureAccountName),
			escapeSQLString(cfg.AzureAccountKey))
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
		`, escapeSQLString(cfg.AzureAccountName))
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

// QueryContext executes a query with context support for timeout/cancellation
func (d *DuckDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := d.db.QueryContext(ctx, query, args...)
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

// QueryWithProfile executes a query and returns timing breakdown using DuckDB profiling
// This is used to measure parsing/planning overhead for optimization decisions
func (d *DuckDB) QueryWithProfile(query string) (*sql.Rows, *QueryProfile, error) {
	return d.QueryWithProfileContext(context.Background(), query)
}

// QueryWithProfileContext executes a query with context support for timeout/cancellation
// and returns timing breakdown using DuckDB profiling
func (d *DuckDB) QueryWithProfileContext(ctx context.Context, query string) (*sql.Rows, *QueryProfile, error) {
	// Create a temporary file for profiling output
	tmpFile, err := os.CreateTemp("", "duckdb_profile_*.json")
	if err != nil {
		// Fall back to regular query if we can't create temp file
		rows, err := d.QueryContext(ctx, query)
		return rows, nil, err
	}
	profilePath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(profilePath)

	// Enable JSON profiling with custom metrics to capture planning time
	if _, err := d.db.Exec("PRAGMA enable_profiling='json'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to enable profiling")
	}
	if _, err := d.db.Exec(fmt.Sprintf("PRAGMA profiling_output='%s'", profilePath)); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set profiling output")
	}
	// Enable planner timing metrics
	if _, err := d.db.Exec("SET custom_profiling_settings='{\"PLANNER\": \"true\", \"PLANNER_BINDING\": \"true\", \"PHYSICAL_PLANNER\": \"true\", \"OPERATOR_TIMING\": \"true\", \"OPERATOR_CARDINALITY\": \"true\"}'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set custom profiling settings")
	}

	// Execute the query with timing and context
	start := time.Now()
	rows, err := d.db.QueryContext(ctx, query)
	totalTime := time.Since(start)

	// Disable profiling
	d.db.Exec("PRAGMA disable_profiling")

	if err != nil {
		return nil, nil, fmt.Errorf("query failed: %w", err)
	}

	// Parse the profiling output
	profile := d.parseProfileOutput(profilePath, totalTime)

	d.logger.Debug().
		Str("query", query).
		Float64("total_ms", profile.TotalMs).
		Float64("planner_ms", profile.PlannerMs).
		Float64("execution_ms", profile.ExecutionMs).
		Msg("Query profiled")

	return rows, profile, nil
}

// duckdbProfileOutput represents the JSON structure from DuckDB profiling
type duckdbProfileOutput struct {
	Latency     float64                  `json:"latency"`
	RowsScanned uint64                   `json:"operator_rows_scanned"`
	Planner     float64                  `json:"planner"`
	Children    []duckdbProfileOperator  `json:"children"`
	Timings     map[string]interface{}   `json:"timings"`
}

type duckdbProfileOperator struct {
	OperatorTiming      float64                 `json:"operator_timing"`
	OperatorCardinality uint64                  `json:"operator_cardinality"`
	OperatorRowsScanned uint64                  `json:"operator_rows_scanned"`
	Children            []duckdbProfileOperator `json:"children"`
}

// parseProfileOutput reads and parses the DuckDB profiling JSON output
func (d *DuckDB) parseProfileOutput(path string, totalTime time.Duration) *QueryProfile {
	profile := &QueryProfile{
		TotalMs: float64(totalTime.Microseconds()) / 1000.0,
	}

	data, err := os.ReadFile(path)
	if err != nil {
		d.logger.Debug().Err(err).Str("path", path).Msg("Failed to read profile output")
		return profile
	}

	// Debug: log raw JSON to understand structure
	d.logger.Debug().Str("raw_json", string(data[:min(500, len(data))])).Msg("DuckDB profile JSON")

	var output duckdbProfileOutput
	if err := json.Unmarshal(data, &output); err != nil {
		d.logger.Debug().Err(err).Str("raw", string(data[:min(200, len(data))])).Msg("Failed to parse profile JSON")
		return profile
	}

	// DuckDB reports latency in seconds, convert to ms
	profile.Latency = output.Latency * 1000.0
	profile.PlannerMs = output.Planner * 1000.0
	profile.RowsScanned = output.RowsScanned

	// Calculate execution time as latency minus planner time
	// (or estimate from operators if planner timing not available)
	if profile.PlannerMs > 0 {
		profile.ExecutionMs = profile.Latency - profile.PlannerMs
	} else {
		// Sum operator timings as execution time
		profile.ExecutionMs = sumOperatorTimings(output.Children) * 1000.0
	}

	// If DuckDB latency is available, use it; otherwise use our measured total
	if profile.Latency == 0 {
		profile.Latency = profile.TotalMs
	}

	return profile
}

// sumOperatorTimings recursively sums operator timings in seconds
func sumOperatorTimings(operators []duckdbProfileOperator) float64 {
	var total float64
	for _, op := range operators {
		total += op.OperatorTiming
		total += sumOperatorTimings(op.Children)
	}
	return total
}
