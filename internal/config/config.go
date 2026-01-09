package config

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all configuration for Arc
type Config struct {
	Server     ServerConfig
	Database   DatabaseConfig
	Storage    StorageConfig
	Ingest     IngestConfig
	Cache      CacheConfig
	Log        LogConfig
	Auth       AuthConfig
	Compaction CompactionConfig
	WAL        WALConfig
	Telemetry       TelemetryConfig
	Delete          DeleteConfig
	Retention       RetentionConfig
	ContinuousQuery ContinuousQueryConfig
	Metrics         MetricsConfig
	MQTT            MQTTConfig
	License         LicenseConfig
	Scheduler       SchedulerConfig
}

type ServerConfig struct {
	Host           string
	Port           int
	ReadTimeout    int
	WriteTimeout   int
	MaxPayloadSize int64 // Maximum request payload size in bytes (applies to both compressed and decompressed)
	// TLS Configuration
	TLSEnabled  bool   // Enable HTTPS/TLS
	TLSCertFile string // Path to TLS certificate file (PEM format)
	TLSKeyFile  string // Path to TLS private key file (PEM format)
}

type DatabaseConfig struct {
	MaxConnections int
	MemoryLimit    string
	ThreadCount    int
	EnableWAL      bool
}

type StorageConfig struct {
	Backend   string
	LocalPath string
	// S3/MinIO configuration
	S3Bucket    string
	S3Region    string
	S3Endpoint  string // Custom endpoint for MinIO (e.g., "http://localhost:9000")
	S3AccessKey string // AWS access key (or use AWS_ACCESS_KEY_ID env var)
	S3SecretKey string // AWS secret key (or use AWS_SECRET_ACCESS_KEY env var)
	S3UseSSL    bool   // Use HTTPS for S3 connections
	S3PathStyle bool   // Use path-style addressing (required for MinIO)
	// Azure Blob Storage configuration
	AzureConnectionString  string // Connection string (simplest auth method)
	AzureAccountName       string // Storage account name
	AzureAccountKey        string // Storage account key
	AzureSASToken          string // SAS token for scoped access
	AzureContainer         string // Container name
	AzureEndpoint          string // Custom endpoint (for Azurite testing)
	AzureUseManagedIdentity bool   // Use managed identity (Azure-hosted deployments)
}

type IngestConfig struct {
	MaxBufferSize     int    // Max records before flush
	MaxBufferAgeMS    int    // Max age in milliseconds before flush
	Compression       string // Parquet compression: snappy, gzip, zstd
	UseDictionary     bool   // Use dictionary encoding
	WriteStatistics   bool   // Write Parquet statistics
	DataPageVersion   string // Parquet data page version: 1.0 or 2.0
	FlushWorkers      int    // Number of workers for async flush (default: 2x CPU, min 8, max 64)
	FlushQueueSize    int    // Capacity of flush task queue (default: 4x workers, min 100)
	ShardCount        int    // Number of buffer shards for lock distribution (default: 32)
	SortKeys          []string // Per-measurement sort keys: "measurement:col1,col2,time"
	DefaultSortKeys   string   // Default sort keys for measurements not in SortKeys
}

type CacheConfig struct {
	Enabled    bool
	MaxSizeMB  int
	DefaultTTL int
}

type LogConfig struct {
	Level  string
	Format string
}

type AuthConfig struct {
	Enabled      bool
	DBPath       string // SQLite database path (shared with WAL, retention, etc.)
	CacheTTL     int    // Token cache TTL in seconds
	MaxCacheSize int    // Maximum number of cached tokens
}

type CompactionConfig struct {
	Enabled           bool   // Enable compaction
	HourlySchedule    string // Cron schedule for hourly compaction (default: "5 * * * *")
	DailySchedule     string // Cron schedule for daily compaction (default: "0 3 * * *")
	HourlyEnabled     bool   // Enable hourly tier
	DailyEnabled      bool   // Enable daily tier
	HourlyMinAgeHours int    // Minimum age for hourly compaction (default: 1)
	HourlyMinFiles    int    // Minimum files for hourly compaction (default: 10)
	DailyMinAgeHours  int    // Minimum age for daily compaction (default: 24)
	DailyMinFiles     int    // Minimum files for daily compaction (default: 12)
	MaxConcurrent     int    // Max concurrent compaction jobs (default: 2)
	TempDirectory     string // Temporary directory for compaction files (default: ./data/compaction)
}

type WALConfig struct {
	Enabled       bool   // Enable WAL for durability (default: false)
	Directory     string // WAL directory (default: ./data/wal)
	SyncMode      string // Sync mode: fsync, fdatasync, async (default: fdatasync)
	MaxSizeMB     int    // Rotate WAL when it reaches this size in MB (default: 100)
	MaxAgeSeconds int    // Rotate WAL after this many seconds (default: 3600)
}

type TelemetryConfig struct {
	Enabled         bool   // Enable telemetry (default: true)
	Endpoint        string // Telemetry endpoint URL
	IntervalSeconds int    // Reporting interval in seconds (default: 86400 = 24h)
}

type DeleteConfig struct {
	Enabled               bool // Enable delete operations (default: false for safety)
	ConfirmationThreshold int  // Require confirm=true for deletes affecting more than this many rows
	MaxRowsPerDelete      int  // Maximum rows that can be deleted in a single operation
}

type RetentionConfig struct {
	Enabled bool   // Enable retention policy management (default: true for policy CRUD, execution is manual)
	DBPath  string // SQLite database path for storing policies
}

type ContinuousQueryConfig struct {
	Enabled bool   // Enable continuous query management (default: true for CRUD, execution is manual)
	DBPath  string // SQLite database path for storing queries
}

type MetricsConfig struct {
	TimeseriesRetentionMinutes int // Retention period for timeseries data in minutes (default: 30, max: 1440)
	TimeseriesIntervalSeconds  int // Collection interval in seconds (default: 5)
}

// MQTTConfig contains MQTT feature toggle only.
// All subscription configuration is managed via the REST API and persisted in SQLite.
// See POST /api/v1/mqtt/subscriptions for creating subscriptions.
type MQTTConfig struct {
	Enabled bool // Enable MQTT subscription manager feature
}

// LicenseConfig holds configuration for enterprise license validation
type LicenseConfig struct {
	Enabled bool   // Enable license validation (default: false)
	Key     string // License key (ARC-ENT-XXXX-XXXX-XXXX-XXXX)
}

// SchedulerConfig holds configuration for automatic schedulers (Enterprise features)
// Note: CQ and retention schedulers are automatically enabled when their respective features
// are enabled (continuous_query.enabled, retention.enabled) AND a valid license is present.
type SchedulerConfig struct {
	RetentionSchedule string // Cron schedule for retention (default: "0 3 * * *" = 3am daily)
}

// Load loads configuration from environment and config file
func Load() (*Config, error) {
	v := viper.New()

	// Set defaults
	setDefaults(v)

	// Environment variables
	v.SetEnvPrefix("ARC")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Config file (optional) - TEMPORARILY DISABLED for initial testing
	// Load config file
	v.SetConfigName("arc")
	v.SetConfigType("toml")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/arc/")
	v.AddConfigPath("$HOME/.arc/")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
		// Config file not found is OK, use defaults
	}

	// Parse max payload size
	maxPayloadSize, err := ParseSize(v.GetString("server.max_payload_size"))
	if err != nil {
		return nil, fmt.Errorf("invalid server.max_payload_size: %w", err)
	}

	// Build config from Viper (which includes defaults + env vars)
	cfg := &Config{
		Server: ServerConfig{
			Host:           v.GetString("server.host"),
			Port:           v.GetInt("server.port"),
			ReadTimeout:    v.GetInt("server.read_timeout"),
			WriteTimeout:   v.GetInt("server.write_timeout"),
			MaxPayloadSize: maxPayloadSize,
			TLSEnabled:     v.GetBool("server.tls_enabled"),
			TLSCertFile:    v.GetString("server.tls_cert_file"),
			TLSKeyFile:     v.GetString("server.tls_key_file"),
		},
		Database: DatabaseConfig{
			MaxConnections: v.GetInt("database.max_connections"),
			MemoryLimit:    v.GetString("database.memory_limit"),
			ThreadCount:    v.GetInt("database.thread_count"),
			EnableWAL:      v.GetBool("database.enable_wal"),
		},
		Storage: StorageConfig{
			Backend:     v.GetString("storage.backend"),
			LocalPath:   v.GetString("storage.local_path"),
			S3Bucket:    v.GetString("storage.s3_bucket"),
			S3Region:    v.GetString("storage.s3_region"),
			S3Endpoint:  v.GetString("storage.s3_endpoint"),
			S3AccessKey: v.GetString("storage.s3_access_key"),
			S3SecretKey: v.GetString("storage.s3_secret_key"),
			S3UseSSL:    v.GetBool("storage.s3_use_ssl"),
			S3PathStyle: v.GetBool("storage.s3_path_style"),
			// Azure Blob Storage
			AzureConnectionString:   v.GetString("storage.azure_connection_string"),
			AzureAccountName:        v.GetString("storage.azure_account_name"),
			AzureAccountKey:         v.GetString("storage.azure_account_key"),
			AzureSASToken:           v.GetString("storage.azure_sas_token"),
			AzureContainer:          v.GetString("storage.azure_container"),
			AzureEndpoint:           v.GetString("storage.azure_endpoint"),
			AzureUseManagedIdentity: v.GetBool("storage.azure_use_managed_identity"),
		},
		Ingest: IngestConfig{
			MaxBufferSize:   v.GetInt("ingest.max_buffer_size"),
			MaxBufferAgeMS:  v.GetInt("ingest.max_buffer_age_ms"),
			Compression:     v.GetString("ingest.compression"),
			UseDictionary:   v.GetBool("ingest.use_dictionary"),
			WriteStatistics: v.GetBool("ingest.write_statistics"),
			DataPageVersion: v.GetString("ingest.data_page_version"),
			FlushWorkers:    v.GetInt("ingest.flush_workers"),
			FlushQueueSize:  v.GetInt("ingest.flush_queue_size"),
			ShardCount:      v.GetInt("ingest.shard_count"),
			SortKeys:        v.GetStringSlice("ingest.sort_keys"),
			DefaultSortKeys: v.GetString("ingest.default_sort_keys"),
		},
		Cache: CacheConfig{
			Enabled:    v.GetBool("cache.enabled"),
			MaxSizeMB:  v.GetInt("cache.max_size_mb"),
			DefaultTTL: v.GetInt("cache.default_ttl"),
		},
		Log: LogConfig{
			Level:  v.GetString("log.level"),
			Format: v.GetString("log.format"),
		},
		Auth: AuthConfig{
			Enabled:      v.GetBool("auth.enabled"),
			DBPath:       v.GetString("auth.db_path"),
			CacheTTL:     v.GetInt("auth.cache_ttl"),
			MaxCacheSize: v.GetInt("auth.max_cache_size"),
		},
		Compaction: CompactionConfig{
			Enabled:           v.GetBool("compaction.enabled"),
			HourlySchedule:    v.GetString("compaction.hourly_schedule"),
			DailySchedule:     v.GetString("compaction.daily_schedule"),
			HourlyEnabled:     v.GetBool("compaction.hourly_enabled"),
			DailyEnabled:      v.GetBool("compaction.daily_enabled"),
			HourlyMinAgeHours: v.GetInt("compaction.hourly_min_age_hours"),
			HourlyMinFiles:    v.GetInt("compaction.hourly_min_files"),
			DailyMinAgeHours:  v.GetInt("compaction.daily_min_age_hours"),
			DailyMinFiles:     v.GetInt("compaction.daily_min_files"),
			MaxConcurrent:     v.GetInt("compaction.max_concurrent"),
			TempDirectory:     v.GetString("compaction.temp_directory"),
		},
		WAL: WALConfig{
			Enabled:       v.GetBool("wal.enabled"),
			Directory:     v.GetString("wal.directory"),
			SyncMode:      v.GetString("wal.sync_mode"),
			MaxSizeMB:     v.GetInt("wal.max_size_mb"),
			MaxAgeSeconds: v.GetInt("wal.max_age_seconds"),
		},
		Telemetry: TelemetryConfig{
			Enabled:         v.GetBool("telemetry.enabled"),
			Endpoint:        v.GetString("telemetry.endpoint"),
			IntervalSeconds: v.GetInt("telemetry.interval_seconds"),
		},
		Delete: DeleteConfig{
			Enabled:               v.GetBool("delete.enabled"),
			ConfirmationThreshold: v.GetInt("delete.confirmation_threshold"),
			MaxRowsPerDelete:      v.GetInt("delete.max_rows_per_delete"),
		},
		Retention: RetentionConfig{
			Enabled: v.GetBool("retention.enabled"),
			DBPath:  v.GetString("retention.db_path"),
		},
		ContinuousQuery: ContinuousQueryConfig{
			Enabled: v.GetBool("continuous_query.enabled"),
			DBPath:  v.GetString("continuous_query.db_path"),
		},
		Metrics: MetricsConfig{
			TimeseriesRetentionMinutes: v.GetInt("metrics.timeseries_retention_minutes"),
			TimeseriesIntervalSeconds:  v.GetInt("metrics.timeseries_interval_seconds"),
		},
		MQTT: MQTTConfig{
			Enabled: v.GetBool("mqtt.enabled"),
		},
		License: LicenseConfig{
			Enabled: v.GetBool("license.enabled"),
			Key:     v.GetString("license.key"),
		},
		Scheduler: SchedulerConfig{
			RetentionSchedule: v.GetString("scheduler.retention_schedule"),
		},
	}

	return cfg, nil
}

func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8000)
	v.SetDefault("server.read_timeout", 30)
	v.SetDefault("server.write_timeout", 30)
	// Max payload size default - 1GB
	v.SetDefault("server.max_payload_size", "1GB")
	// TLS defaults - disabled by default for backward compatibility
	v.SetDefault("server.tls_enabled", false)
	v.SetDefault("server.tls_cert_file", "")
	v.SetDefault("server.tls_key_file", "")

	// Database defaults - dynamically calculated based on system resources
	v.SetDefault("database.max_connections", getDefaultMaxConnections())
	v.SetDefault("database.memory_limit", getDefaultMemoryLimit())
	v.SetDefault("database.thread_count", getDefaultThreadCount())
	v.SetDefault("database.enable_wal", true)

	// Storage defaults
	v.SetDefault("storage.backend", "local")
	v.SetDefault("storage.local_path", "./data/arc")
	v.SetDefault("storage.s3_region", "us-east-1")
	v.SetDefault("storage.s3_use_ssl", true)
	v.SetDefault("storage.s3_path_style", false) // Use virtual-hosted style by default (set true for MinIO)

	// Cache defaults
	v.SetDefault("cache.enabled", true)
	v.SetDefault("cache.max_size_mb", 1024)
	v.SetDefault("cache.default_ttl", 300)

	// Ingest defaults
	v.SetDefault("ingest.max_buffer_size", 50000)
	v.SetDefault("ingest.max_buffer_age_ms", 5000)
	v.SetDefault("ingest.compression", "snappy")
	v.SetDefault("ingest.use_dictionary", true)
	v.SetDefault("ingest.write_statistics", true)
	v.SetDefault("ingest.data_page_version", "2.0")
	v.SetDefault("ingest.flush_workers", getDefaultFlushWorkers())
	v.SetDefault("ingest.flush_queue_size", getDefaultFlushQueueSize())
	v.SetDefault("ingest.shard_count", 32)
	v.SetDefault("ingest.sort_keys", []string{})          // No custom sort keys by default
	v.SetDefault("ingest.default_sort_keys", "time")      // Default to time-only sorting

	// Log defaults
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")

	// Auth defaults
	v.SetDefault("auth.enabled", true)
	v.SetDefault("auth.db_path", "./data/arc.db") // Shared SQLite DB (same as Python)
	v.SetDefault("auth.cache_ttl", 300)           // 5 minutes
	v.SetDefault("auth.max_cache_size", 1000)     // Max cached tokens

	// Compaction defaults
	v.SetDefault("compaction.enabled", true)
	v.SetDefault("compaction.hourly_schedule", "5 * * * *")   // Every hour at :05
	v.SetDefault("compaction.daily_schedule", "0 3 * * *")    // 3 AM daily
	v.SetDefault("compaction.hourly_enabled", true)           // Enable hourly tier
	v.SetDefault("compaction.daily_enabled", true)            // Enable daily tier
	v.SetDefault("compaction.hourly_min_age_hours", 0)        // 0 hours min age (compact immediately)
	v.SetDefault("compaction.hourly_min_files", 10)           // 10 files minimum
	v.SetDefault("compaction.daily_min_age_hours", 24)        // 24 hours min age
	v.SetDefault("compaction.daily_min_files", 12)            // 12 files minimum
	v.SetDefault("compaction.max_concurrent", 2)              // 2 concurrent jobs
	v.SetDefault("compaction.temp_directory", "./data/compaction") // Temp directory for compaction files

	// WAL defaults
	v.SetDefault("wal.enabled", false)           // Disabled by default for backwards compatibility
	v.SetDefault("wal.directory", "./data/wal")  // WAL directory
	v.SetDefault("wal.sync_mode", "fdatasync")   // Balanced mode: fdatasync, fsync, or async
	v.SetDefault("wal.max_size_mb", 100)         // Rotate WAL at 100MB
	v.SetDefault("wal.max_age_seconds", 3600)    // Rotate WAL after 1 hour

	// Telemetry defaults
	v.SetDefault("telemetry.enabled", true)                                               // Enabled by default (opt-out)
	v.SetDefault("telemetry.endpoint", "https://telemetry.basekick.net/api/v1/telemetry") // Telemetry endpoint
	v.SetDefault("telemetry.interval_seconds", 86400)                                     // 24 hours

	// Delete defaults
	v.SetDefault("delete.enabled", false)                // Disabled by default for safety
	v.SetDefault("delete.confirmation_threshold", 10000) // Require confirm=true for > 10k rows
	v.SetDefault("delete.max_rows_per_delete", 1000000)  // Max 1M rows per delete

	// Retention policy defaults
	v.SetDefault("retention.enabled", true)             // Enable policy management by default
	v.SetDefault("retention.db_path", "./data/arc.db")  // Shared SQLite DB with auth

	// Continuous query defaults
	v.SetDefault("continuous_query.enabled", true)            // Enable CQ management by default
	v.SetDefault("continuous_query.db_path", "./data/arc.db") // Shared SQLite DB with auth

	// Metrics defaults
	v.SetDefault("metrics.timeseries_retention_minutes", 30) // 30 minutes retention
	v.SetDefault("metrics.timeseries_interval_seconds", 5)   // Collect every 5 seconds

	// MQTT defaults (subscriptions are configured via REST API, stored in SQLite)
	v.SetDefault("mqtt.enabled", false) // Feature toggle only - disabled by default

	// License defaults (Enterprise features)
	// Note: Server URL and validation interval are hardcoded in internal/license/client.go
	v.SetDefault("license.enabled", false) // Disabled by default
	v.SetDefault("license.key", "")        // Must be provided

	// Scheduler defaults (Enterprise features)
	// Note: CQ and retention schedulers are auto-enabled when their features are enabled AND license allows
	v.SetDefault("scheduler.retention_schedule", "0 3 * * *") // 3am daily
}

func getDefaultThreadCount() int {
	// Use number of CPU cores for optimal parallelism
	return runtime.NumCPU()
}

func getDefaultMaxConnections() int {
	// Use 2x CPU cores as a good default for connection pooling
	// This allows for good parallelism while avoiding excessive resource usage
	cores := runtime.NumCPU()
	maxConns := cores * 2
	if maxConns < 4 {
		return 4 // Minimum 4 connections
	}
	if maxConns > 64 {
		return 64 // Cap at 64 to avoid excessive resource usage
	}
	return maxConns
}

func getDefaultMemoryLimit() string {
	// Target ~25% of system memory for DuckDB, capped at reasonable limits
	// This is a conservative default that works well across different systems
	// Users can override via ARC_DATABASE_MEMORY_LIMIT env var or config file
	cores := runtime.NumCPU()

	// Heuristic: assume ~2GB per core as a rough estimate of available memory
	// This is conservative and works for most cloud instances
	estimatedMemGB := cores * 2

	// Use 50% of estimated memory for DuckDB
	targetMemGB := estimatedMemGB / 2

	// Apply bounds
	if targetMemGB < 1 {
		return "1GB"
	}
	if targetMemGB > 32 {
		return "32GB" // Cap at 32GB by default
	}
	return fmt.Sprintf("%dGB", targetMemGB)
}

func getDefaultFlushWorkers() int {
	// Scale flush workers with CPU cores, similar to InfluxDB's approach
	// More workers allow higher concurrent I/O to storage
	cores := runtime.NumCPU()
	workers := cores * 2
	if workers < 8 {
		return 8 // Minimum for reasonable concurrency
	}
	if workers > 64 {
		return 64 // Cap to avoid excessive resource usage
	}
	return workers
}

func getDefaultFlushQueueSize() int {
	// Queue should absorb bursts without dropping tasks
	// 4x workers provides good burst capacity
	workers := getDefaultFlushWorkers()
	queueSize := workers * 4
	if queueSize < 100 {
		return 100
	}
	return queueSize
}

// ValidateTLS validates TLS configuration when TLS is enabled.
// Returns nil if TLS is disabled or if configuration is valid.
func (cfg *ServerConfig) ValidateTLS() error {
	if !cfg.TLSEnabled {
		return nil
	}

	if cfg.TLSCertFile == "" {
		return fmt.Errorf("TLS enabled but server.tls_cert_file not specified")
	}
	if cfg.TLSKeyFile == "" {
		return fmt.Errorf("TLS enabled but server.tls_key_file not specified")
	}

	// Verify cert file exists and is accessible
	certInfo, err := os.Stat(cfg.TLSCertFile)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("TLS certificate file not found: %s", cfg.TLSCertFile)
		}
		return fmt.Errorf("cannot access TLS certificate file %s: %w", cfg.TLSCertFile, err)
	}
	if certInfo.IsDir() {
		return fmt.Errorf("TLS certificate path is a directory, not a file: %s", cfg.TLSCertFile)
	}

	// Verify key file exists and is accessible
	keyInfo, err := os.Stat(cfg.TLSKeyFile)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("TLS key file not found: %s", cfg.TLSKeyFile)
		}
		return fmt.Errorf("cannot access TLS key file %s: %w", cfg.TLSKeyFile, err)
	}
	if keyInfo.IsDir() {
		return fmt.Errorf("TLS key path is a directory, not a file: %s", cfg.TLSKeyFile)
	}

	return nil
}

// ParseSize parses a human-readable size string (e.g., "1GB", "500MB", "100KB") to bytes.
// Supports: B, KB, MB, GB (case-insensitive).
// Returns the size in bytes or an error if the format is invalid.
func ParseSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(strings.ToUpper(sizeStr))
	if sizeStr == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Define multipliers (order matters: check longer suffixes first)
	type unitInfo struct {
		suffix     string
		multiplier int64
	}
	units := []unitInfo{
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}

	// Try each suffix from longest to shortest
	for _, unit := range units {
		if strings.HasSuffix(sizeStr, unit.suffix) {
			numStr := strings.TrimSuffix(sizeStr, unit.suffix)
			numStr = strings.TrimSpace(numStr)

			// Ensure the remaining string is a valid number (no trailing non-numeric chars)
			var num float64
			var trailing string
			n, _ := fmt.Sscanf(numStr, "%f%s", &num, &trailing)
			if n == 0 {
				return 0, fmt.Errorf("invalid size number: %s", numStr)
			}
			if trailing != "" {
				// There's extra text after the number - likely an unrecognized unit like "T" in "1TB"
				return 0, fmt.Errorf("invalid size format: %s (use e.g., '1GB', '500MB', '100KB')", sizeStr)
			}
			if num < 0 {
				return 0, fmt.Errorf("size cannot be negative: %s", sizeStr)
			}
			return int64(num * float64(unit.multiplier)), nil
		}
	}

	// Try parsing as plain number (bytes)
	var num int64
	var trailing string
	n, _ := fmt.Sscanf(sizeStr, "%d%s", &num, &trailing)
	if n == 0 || trailing != "" {
		return 0, fmt.Errorf("invalid size format: %s (use e.g., '1GB', '500MB', '100KB')", sizeStr)
	}
	if num < 0 {
		return 0, fmt.Errorf("size cannot be negative: %s", sizeStr)
	}
	return num, nil
}
