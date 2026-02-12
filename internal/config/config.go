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
	Cluster         ClusterConfig
	Query           QueryConfig
	TieredStorage   TieredStorageConfig
	AuditLog        AuditLogConfig
	Backup          BackupConfig
	Governance      GovernanceConfig
	QueryManagement QueryManagementConfig
}

type ServerConfig struct {
	Host            string
	Port            int
	ReadTimeout     int
	WriteTimeout    int
	IdleTimeout     int // Connection idle timeout in seconds
	ShutdownTimeout int // Graceful shutdown timeout in seconds
	MaxPayloadSize  int64 // Maximum request payload size in bytes (applies to both compressed and decompressed)
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
	FlushWorkers        int      // Number of workers for async flush (default: 2x CPU, min 8, max 64)
	FlushQueueSize      int      // Capacity of flush task queue (default: 4x workers, min 100)
	ShardCount          int      // Number of buffer shards for lock distribution (default: 32)
	SortKeys            []string // Per-measurement sort keys: "measurement:col1,col2,time"
	DefaultSortKeys     string   // Default sort keys for measurements not in SortKeys
	FlushTimeoutSeconds int      // Timeout for storage writes during flush (default: 30s, 0 = no timeout)
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
	DailyMinFiles             int // Minimum files for daily compaction (default: 12)
	DailySkipFileAgeCheckDays int // Skip file creation time check for partitions older than N days (default: 7)
	MaxConcurrent             int // Max concurrent compaction jobs (default: 2)
	TempDirectory     string // Temporary directory for compaction files (default: ./data/compaction)
}

type WALConfig struct {
	Enabled                 bool   // Enable WAL for durability (default: false)
	Directory               string // WAL directory (default: ./data/wal)
	SyncMode                string // Sync mode: fsync, fdatasync, async (default: fdatasync)
	MaxSizeMB               int    // Rotate WAL when it reaches this size in MB (default: 100)
	MaxAgeSeconds           int    // Rotate WAL after this many seconds (default: 3600)
	RecoveryIntervalSeconds int    // Interval for periodic WAL recovery in seconds (default: 300 = 5 minutes)
	RecoveryBatchSize       int    // Max records to replay per batch during recovery (default: 10000)
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

// QueryConfig holds configuration for query execution optimizations
type QueryConfig struct {
	Timeout           int   // Query execution timeout in seconds (0 = no timeout, default: 300)
	EnableS3Cache     bool  // Enable S3 file caching for faster repeated reads (useful for CTEs/subqueries)
	S3CacheSize       int64 // Cache size in bytes (parsed from "128MB", "256MB", etc.)
	S3CacheTTLSeconds int   // Cache entry TTL in seconds (default: 3600 = 1 hour)
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

// TieredStorageConfig holds configuration for tiered storage (Enterprise feature)
// Tiered storage enables automatic data lifecycle management with hot/cold tiers.
// - Hot tier: Local storage for recent data (fast access)
// - Cold tier: S3/Azure for archived data (cost-effective archival storage)
//
// Data older than DefaultHotMaxAgeDays is automatically migrated to cold storage.
type TieredStorageConfig struct {
	Enabled              bool   // Enable tiered storage (requires enterprise license)
	MigrationSchedule    string // Cron schedule for migrations (default: "0 2 * * *" = 2am daily)
	MigrationMaxConcurrent int  // Max concurrent file migrations (default: 4)
	MigrationBatchSize   int    // Files per migration batch (default: 100)

	// Single threshold: data older than this moves from hot to cold
	DefaultHotMaxAgeDays int // Data older than this migrates to cold (default: 30)

	// Cold tier configuration (remote S3/Azure storage)
	Cold ColdTierConfig
}

// ColdTierConfig holds configuration for the cold storage tier (S3/Azure archive).
// This is the only remote tier - data moves directly from hot (local) to cold (remote).
type ColdTierConfig struct {
	Enabled bool   // Enable cold tier
	Backend string // "s3" or "azure"

	// S3 settings
	S3Bucket       string // S3 bucket for archived data
	S3Region       string // AWS region
	S3Endpoint     string // Custom endpoint for MinIO
	S3AccessKey    string // AWS access key (use env: ARC_TIERED_STORAGE_COLD_S3_ACCESS_KEY)
	S3SecretKey    string // AWS secret key (use env: ARC_TIERED_STORAGE_COLD_S3_SECRET_KEY)
	S3UseSSL       bool   // Use HTTPS for S3 connections
	S3PathStyle    bool   // Use path-style addressing (required for MinIO)
	S3StorageClass string // S3 storage class (default: "GLACIER")

	// Azure settings
	AzureContainer         string // Azure container for archived data
	AzureConnectionString  string // Connection string (simplest auth method)
	AzureAccountName       string // Storage account name
	AzureAccountKey        string // Storage account key
	AzureSASToken          string // SAS token for scoped access
	AzureEndpoint          string // Custom endpoint (for Azurite testing)
	AzureUseManagedIdentity bool   // Use managed identity (Azure-hosted deployments)
	AzureAccessTier        string // Azure access tier (default: "Archive")

	// Retrieval settings (for Glacier/Archive)
	RetrievalMode string // Glacier retrieval mode: "standard", "expedited", "bulk" (default: "standard")
}

// AuditLogConfig holds configuration for enterprise audit logging.
// When enabled, all auditable API requests are logged to SQLite for compliance.
type AuditLogConfig struct {
	Enabled       bool // Enable audit logging (requires enterprise license)
	RetentionDays int  // How long to keep audit logs (default: 90)
	IncludeReads  bool // Log read/query operations (default: false, high volume)
}

// GovernanceConfig holds configuration for query governance (Enterprise feature).
// Provides per-token rate limiting and query quotas for resource predictability.
type GovernanceConfig struct {
	Enabled                 bool // Enable query governance (requires enterprise license with query_governance feature)
	DefaultRateLimitPerMin  int  // Default rate limit per minute for all tokens (0 = unlimited)
	DefaultRateLimitPerHour int  // Default rate limit per hour for all tokens (0 = unlimited)
	DefaultMaxQueriesPerHour int // Default max queries per hour per token (0 = unlimited)
	DefaultMaxQueriesPerDay  int // Default max queries per day per token (0 = unlimited)
	DefaultMaxRowsPerQuery   int // Default max rows returned per query (0 = unlimited)
}

// QueryManagementConfig holds configuration for long-running query management (Enterprise feature).
// Provides active query tracking, cancellation, and history.
type QueryManagementConfig struct {
	Enabled     bool // Enable query management (requires enterprise license with query_management feature)
	HistorySize int  // Ring buffer size for completed query history (default: 100)
}

type BackupConfig struct {
	Enabled   bool   // Enable backup/restore API
	LocalPath string // Local directory for backups (default: "./data/backups")
}

// ClusterConfig holds configuration for Arc clustering (Enterprise feature)
// Clustering enables horizontal scaling with role-based node separation:
// - writer: Handles ingestion, WAL, flushes to shared storage
// - reader: Query-only node, reads from shared storage
// - compactor: Background compaction and maintenance
// - standalone: Single-node deployment (default, OSS-compatible)
type ClusterConfig struct {
	Enabled     bool   // Enable clustering mode (default: false for standalone)
	NodeID      string // Unique node identifier (auto-generated if empty)
	Role        string // Node role: "writer", "reader", "compactor", "standalone" (default: "standalone")
	ClusterName string // Cluster name for identification

	// Discovery configuration
	Seeds []string // Static seed nodes for initial cluster discovery (host:port)

	// Coordination
	CoordinatorAddr string // Address to bind coordinator service (default: ":9100")
	AdvertiseAddr   string // Address advertised to other nodes (auto-detected if empty)

	// Health check configuration
	HealthCheckInterval int // Health check interval in seconds (default: 5)
	HealthCheckTimeout  int // Health check timeout in seconds (default: 3)
	UnhealthyThreshold  int // Number of failed checks before marking unhealthy (default: 3)

	// Heartbeat configuration
	HeartbeatInterval int // Heartbeat interval in seconds (default: 1)
	HeartbeatTimeout  int // Heartbeat timeout before considering node dead (default: 5)

	// Raft consensus configuration (Phase 3)
	RaftDataDir          string // Directory for Raft data (default: ./data/raft)
	RaftBindAddr         string // Address to bind Raft transport (default: ":9200")
	RaftAdvertiseAddr    string // Address advertised to Raft peers (auto-detected if empty)
	RaftBootstrap        bool   // Bootstrap a new Raft cluster (only for first node)
	RaftElectionTimeout  int    // Election timeout in milliseconds (default: 1000)
	RaftHeartbeatTimeout int    // Raft heartbeat timeout in milliseconds (default: 500)
	RaftSnapshotInterval int    // Snapshot interval in seconds (default: 300)
	RaftSnapshotThreshold int   // Number of logs before snapshot (default: 10000)

	// Request routing configuration (Phase 3)
	RouteTimeout int // Timeout for forwarded requests in milliseconds (default: 5000)
	RouteRetries int // Number of retries for failed forwards (default: 3)

	// WAL Replication configuration (Phase 3.3)
	ReplicationEnabled     bool // Enable WAL replication to readers (default: false)
	ReplicationLagLimit    int  // Max acceptable lag in milliseconds before health degrades (default: 5000)
	ReplicationBufferSize  int  // Entry buffer size for replication queue (default: 10000)
	ReplicationAckInterval int  // How often readers send acks in milliseconds (default: 100)

	// Sharding configuration (Phase 4)
	ShardingEnabled           bool   // Enable sharding for horizontal write scaling (default: false)
	ShardingNumShards         int    // Number of shards (default: 3)
	ShardingShardKey          string // Shard key: "database" or "measurement" (default: "database")
	ShardingReplicationFactor int    // Number of copies per shard (default: 3)
	ShardingRouteTimeout      int    // Timeout for shard routing in milliseconds (default: 5000)

	// Writer failover configuration (Phase 3)
	FailoverEnabled        bool // Enable automatic writer failover (default: false)
	FailoverTimeoutSeconds int  // Timeout for failover operation in seconds (default: 30)
	FailoverCooldownSeconds int // Cooldown between failovers in seconds (default: 60)
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

	// Parse S3 cache size
	s3CacheSize, err := ParseSize(v.GetString("query.s3_cache_size"))
	if err != nil {
		return nil, fmt.Errorf("invalid query.s3_cache_size: %w", err)
	}

	// Build config from Viper (which includes defaults + env vars)
	cfg := &Config{
		Server: ServerConfig{
			Host:           v.GetString("server.host"),
			Port:           v.GetInt("server.port"),
			ReadTimeout:     v.GetInt("server.read_timeout"),
			WriteTimeout:    v.GetInt("server.write_timeout"),
			IdleTimeout:     v.GetInt("server.idle_timeout"),
			ShutdownTimeout: v.GetInt("server.shutdown_timeout"),
			MaxPayloadSize:  maxPayloadSize,
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
			FlushWorkers:        v.GetInt("ingest.flush_workers"),
			FlushQueueSize:      v.GetInt("ingest.flush_queue_size"),
			FlushTimeoutSeconds: v.GetInt("ingest.flush_timeout_seconds"),
			ShardCount:          v.GetInt("ingest.shard_count"),
			SortKeys:            v.GetStringSlice("ingest.sort_keys"),
			DefaultSortKeys:     v.GetString("ingest.default_sort_keys"),
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
			DailyMinFiles:             v.GetInt("compaction.daily_min_files"),
			DailySkipFileAgeCheckDays: v.GetInt("compaction.daily_skip_file_age_check_days"),
			MaxConcurrent:             v.GetInt("compaction.max_concurrent"),
			TempDirectory:     v.GetString("compaction.temp_directory"),
		},
		WAL: WALConfig{
			Enabled:                 v.GetBool("wal.enabled"),
			Directory:               v.GetString("wal.directory"),
			SyncMode:                v.GetString("wal.sync_mode"),
			MaxSizeMB:               v.GetInt("wal.max_size_mb"),
			MaxAgeSeconds:           v.GetInt("wal.max_age_seconds"),
			RecoveryIntervalSeconds: v.GetInt("wal.recovery_interval_seconds"),
			RecoveryBatchSize:       v.GetInt("wal.recovery_batch_size"),
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
		Query: QueryConfig{
			Timeout:           v.GetInt("query.timeout"),
			EnableS3Cache:     v.GetBool("query.enable_s3_cache"),
			S3CacheSize:       s3CacheSize,
			S3CacheTTLSeconds: v.GetInt("query.s3_cache_ttl_seconds"),
		},
		License: LicenseConfig{
			Enabled: v.GetBool("license.enabled"),
			Key:     v.GetString("license.key"),
		},
		Scheduler: SchedulerConfig{
			RetentionSchedule: v.GetString("scheduler.retention_schedule"),
		},
		Cluster: ClusterConfig{
			Enabled:             v.GetBool("cluster.enabled"),
			NodeID:              v.GetString("cluster.node_id"),
			Role:                v.GetString("cluster.role"),
			ClusterName:         v.GetString("cluster.cluster_name"),
			Seeds:               parseStringSlice(v.GetString("cluster.seeds")),
			CoordinatorAddr:     v.GetString("cluster.coordinator_addr"),
			AdvertiseAddr:       v.GetString("cluster.advertise_addr"),
			HealthCheckInterval: v.GetInt("cluster.health_check_interval"),
			HealthCheckTimeout:  v.GetInt("cluster.health_check_timeout"),
			UnhealthyThreshold:  v.GetInt("cluster.unhealthy_threshold"),
			HeartbeatInterval:   v.GetInt("cluster.heartbeat_interval"),
			HeartbeatTimeout:    v.GetInt("cluster.heartbeat_timeout"),
			// Raft configuration
			RaftDataDir:          v.GetString("cluster.raft_data_dir"),
			RaftBindAddr:         v.GetString("cluster.raft_bind_addr"),
			RaftAdvertiseAddr:    v.GetString("cluster.raft_advertise_addr"),
			RaftBootstrap:        v.GetBool("cluster.raft_bootstrap"),
			RaftElectionTimeout:  v.GetInt("cluster.raft_election_timeout"),
			RaftHeartbeatTimeout: v.GetInt("cluster.raft_heartbeat_timeout"),
			RaftSnapshotInterval: v.GetInt("cluster.raft_snapshot_interval"),
			RaftSnapshotThreshold: v.GetInt("cluster.raft_snapshot_threshold"),
			// Routing configuration
			RouteTimeout: v.GetInt("cluster.route_timeout"),
			RouteRetries: v.GetInt("cluster.route_retries"),
			// Replication configuration
			ReplicationEnabled:     v.GetBool("cluster.replication_enabled"),
			ReplicationLagLimit:    v.GetInt("cluster.replication_lag_limit"),
			ReplicationBufferSize:  v.GetInt("cluster.replication_buffer_size"),
			ReplicationAckInterval: v.GetInt("cluster.replication_ack_interval"),
			// Sharding configuration (Phase 4)
			ShardingEnabled:           v.GetBool("cluster.sharding_enabled"),
			ShardingNumShards:         v.GetInt("cluster.sharding_num_shards"),
			ShardingShardKey:          v.GetString("cluster.sharding_shard_key"),
			ShardingReplicationFactor: v.GetInt("cluster.sharding_replication_factor"),
			ShardingRouteTimeout:      v.GetInt("cluster.sharding_route_timeout"),
			// Writer failover configuration
			FailoverEnabled:         v.GetBool("cluster.failover_enabled"),
			FailoverTimeoutSeconds:  v.GetInt("cluster.failover_timeout"),
			FailoverCooldownSeconds: v.GetInt("cluster.failover_cooldown"),
		},
		TieredStorage: TieredStorageConfig{
			Enabled:              v.GetBool("tiered_storage.enabled"),
			MigrationSchedule:    v.GetString("tiered_storage.migration_schedule"),
			MigrationMaxConcurrent: v.GetInt("tiered_storage.migration_max_concurrent"),
			MigrationBatchSize:   v.GetInt("tiered_storage.migration_batch_size"),
			DefaultHotMaxAgeDays: v.GetInt("tiered_storage.default_hot_max_age_days"),
			Cold: ColdTierConfig{
				Enabled:               v.GetBool("tiered_storage.cold.enabled"),
				Backend:               v.GetString("tiered_storage.cold.backend"),
				S3Bucket:              v.GetString("tiered_storage.cold.s3_bucket"),
				S3Region:              v.GetString("tiered_storage.cold.s3_region"),
				S3Endpoint:            v.GetString("tiered_storage.cold.s3_endpoint"),
				S3AccessKey:           v.GetString("tiered_storage.cold.s3_access_key"),
				S3SecretKey:           v.GetString("tiered_storage.cold.s3_secret_key"),
				S3UseSSL:              v.GetBool("tiered_storage.cold.s3_use_ssl"),
				S3PathStyle:           v.GetBool("tiered_storage.cold.s3_path_style"),
				S3StorageClass:        v.GetString("tiered_storage.cold.s3_storage_class"),
				AzureContainer:        v.GetString("tiered_storage.cold.azure_container"),
				AzureConnectionString: v.GetString("tiered_storage.cold.azure_connection_string"),
				AzureAccountName:      v.GetString("tiered_storage.cold.azure_account_name"),
				AzureAccountKey:       v.GetString("tiered_storage.cold.azure_account_key"),
				AzureSASToken:         v.GetString("tiered_storage.cold.azure_sas_token"),
				AzureEndpoint:         v.GetString("tiered_storage.cold.azure_endpoint"),
				AzureUseManagedIdentity: v.GetBool("tiered_storage.cold.azure_use_managed_identity"),
				AzureAccessTier:       v.GetString("tiered_storage.cold.azure_access_tier"),
				RetrievalMode:         v.GetString("tiered_storage.cold.retrieval_mode"),
			},
		},
		AuditLog: AuditLogConfig{
			Enabled:       v.GetBool("audit_log.enabled"),
			RetentionDays: v.GetInt("audit_log.retention_days"),
			IncludeReads:  v.GetBool("audit_log.include_reads"),
		},
		Governance: GovernanceConfig{
			Enabled:                 v.GetBool("governance.enabled"),
			DefaultRateLimitPerMin:  v.GetInt("governance.default_rate_limit_per_min"),
			DefaultRateLimitPerHour: v.GetInt("governance.default_rate_limit_per_hour"),
			DefaultMaxQueriesPerHour: v.GetInt("governance.default_max_queries_per_hour"),
			DefaultMaxQueriesPerDay:  v.GetInt("governance.default_max_queries_per_day"),
			DefaultMaxRowsPerQuery:   v.GetInt("governance.default_max_rows_per_query"),
		},
		QueryManagement: QueryManagementConfig{
			Enabled:     v.GetBool("query_management.enabled"),
			HistorySize: v.GetInt("query_management.history_size"),
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
	v.SetDefault("server.idle_timeout", 120)
	v.SetDefault("server.shutdown_timeout", 30)
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
	v.SetDefault("ingest.flush_timeout_seconds", 30)      // 30s timeout for storage writes during flush

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
	v.SetDefault("compaction.daily_skip_file_age_check_days", 7) // Skip file age check for partitions older than 7 days
	v.SetDefault("compaction.max_concurrent", 2)              // 2 concurrent jobs
	v.SetDefault("compaction.temp_directory", "./data/compaction") // Temp directory for compaction files

	// WAL defaults
	v.SetDefault("wal.enabled", false)                    // Disabled by default for backwards compatibility
	v.SetDefault("wal.directory", "./data/wal")           // WAL directory
	v.SetDefault("wal.sync_mode", "fdatasync")            // Balanced mode: fdatasync, fsync, or async
	v.SetDefault("wal.max_size_mb", 100)                  // Rotate WAL at 100MB
	v.SetDefault("wal.max_age_seconds", 3600)             // Rotate WAL after 1 hour
	v.SetDefault("wal.recovery_interval_seconds", 300)    // Periodic recovery every 5 minutes
	v.SetDefault("wal.recovery_batch_size", 10000)        // Max records per recovery batch (rate limiting)

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

	// Query defaults
	v.SetDefault("query.timeout", 300)                 // 5 minute query timeout (0 = no timeout)
	v.SetDefault("query.enable_s3_cache", false)       // Disabled by default (opt-in feature)
	v.SetDefault("query.s3_cache_size", "128MB")       // 128MB cache (256 blocks × 512KB)
	v.SetDefault("query.s3_cache_ttl_seconds", 3600)   // 1 hour

	// License defaults (Enterprise features)
	// Note: Server URL and validation interval are hardcoded in internal/license/client.go
	v.SetDefault("license.enabled", false) // Disabled by default
	v.SetDefault("license.key", "")        // Must be provided

	// Scheduler defaults (Enterprise features)
	// Note: CQ and retention schedulers are auto-enabled when their features are enabled AND license allows
	v.SetDefault("scheduler.retention_schedule", "0 3 * * *") // 3am daily

	// Cluster defaults (Enterprise feature)
	v.SetDefault("cluster.enabled", false)                 // Disabled by default (standalone mode)
	v.SetDefault("cluster.node_id", "")                    // Auto-generated if empty
	v.SetDefault("cluster.role", "standalone")             // standalone, writer, reader, compactor
	v.SetDefault("cluster.cluster_name", "arc-cluster")    // Default cluster name
	v.SetDefault("cluster.seeds", []string{})              // No seeds by default
	v.SetDefault("cluster.coordinator_addr", ":9100")      // Coordinator bind address
	v.SetDefault("cluster.advertise_addr", "")             // Auto-detected if empty
	v.SetDefault("cluster.health_check_interval", 5)       // 5 seconds
	v.SetDefault("cluster.health_check_timeout", 3)        // 3 seconds
	v.SetDefault("cluster.unhealthy_threshold", 3)         // 3 failed checks
	v.SetDefault("cluster.heartbeat_interval", 1)          // 1 second
	v.SetDefault("cluster.heartbeat_timeout", 5)           // 5 seconds

	// Raft consensus defaults (Phase 3)
	v.SetDefault("cluster.raft_data_dir", "./data/raft")   // Raft data directory
	v.SetDefault("cluster.raft_bind_addr", ":9200")        // Raft transport bind address
	v.SetDefault("cluster.raft_advertise_addr", "")        // Auto-detected if empty
	v.SetDefault("cluster.raft_bootstrap", false)          // Don't bootstrap by default
	v.SetDefault("cluster.raft_election_timeout", 1000)    // 1 second election timeout
	v.SetDefault("cluster.raft_heartbeat_timeout", 500)    // 500ms heartbeat timeout
	v.SetDefault("cluster.raft_snapshot_interval", 300)    // 5 minutes snapshot interval
	v.SetDefault("cluster.raft_snapshot_threshold", 10000) // 10k logs before snapshot

	// Request routing defaults (Phase 3)
	v.SetDefault("cluster.route_timeout", 5000)            // 5 second timeout for forwards
	v.SetDefault("cluster.route_retries", 3)               // 3 retries for failed forwards

	// WAL Replication defaults (Phase 3.3)
	v.SetDefault("cluster.replication_enabled", false)     // Disabled by default
	v.SetDefault("cluster.replication_lag_limit", 5000)    // 5 second lag limit
	v.SetDefault("cluster.replication_buffer_size", 10000) // 10k entry buffer
	v.SetDefault("cluster.replication_ack_interval", 100)  // 100ms ack interval

	// Sharding defaults (Phase 4)
	v.SetDefault("cluster.sharding_enabled", false)            // Disabled by default
	v.SetDefault("cluster.sharding_num_shards", 3)             // 3 shards default
	v.SetDefault("cluster.sharding_shard_key", "database")     // Database-level sharding
	v.SetDefault("cluster.sharding_replication_factor", 3)     // RF=3 for fault tolerance
	v.SetDefault("cluster.sharding_route_timeout", 5000)       // 5 second timeout

	// Writer failover defaults (Phase 3)
	v.SetDefault("cluster.failover_enabled", false) // Disabled by default
	v.SetDefault("cluster.failover_timeout", 30)    // 30 second failover timeout
	v.SetDefault("cluster.failover_cooldown", 60)   // 60 second cooldown between failovers

	// Tiered storage defaults (Enterprise feature)
	// Simple 2-tier system: Hot (local) -> Cold (S3/Azure archive)
	v.SetDefault("tiered_storage.enabled", false)                    // Disabled by default
	v.SetDefault("tiered_storage.migration_schedule", "0 2 * * *")   // 2am daily
	v.SetDefault("tiered_storage.migration_max_concurrent", 4)       // 4 concurrent migrations
	v.SetDefault("tiered_storage.migration_batch_size", 100)         // 100 files per batch
	v.SetDefault("tiered_storage.default_hot_max_age_days", 30)      // 30 days in hot tier before archiving

	// Cold tier defaults (S3/Azure archive storage)
	v.SetDefault("tiered_storage.cold.enabled", false)               // Disabled by default
	v.SetDefault("tiered_storage.cold.backend", "s3")                // S3 by default
	v.SetDefault("tiered_storage.cold.s3_bucket", "")                // Must be configured
	v.SetDefault("tiered_storage.cold.s3_region", "us-east-1")       // Default region
	v.SetDefault("tiered_storage.cold.s3_endpoint", "")              // Empty for AWS, set for MinIO
	v.SetDefault("tiered_storage.cold.s3_access_key", "")            // Must be configured
	v.SetDefault("tiered_storage.cold.s3_secret_key", "")            // Must be configured
	v.SetDefault("tiered_storage.cold.s3_use_ssl", true)             // HTTPS by default
	v.SetDefault("tiered_storage.cold.s3_path_style", false)         // Virtual-hosted style for AWS
	v.SetDefault("tiered_storage.cold.s3_storage_class", "GLACIER")  // Glacier by default
	v.SetDefault("tiered_storage.cold.azure_container", "")          // Must be configured for Azure
	v.SetDefault("tiered_storage.cold.azure_connection_string", "")
	v.SetDefault("tiered_storage.cold.azure_account_name", "")
	v.SetDefault("tiered_storage.cold.azure_account_key", "")
	v.SetDefault("tiered_storage.cold.azure_sas_token", "")
	v.SetDefault("tiered_storage.cold.azure_endpoint", "")
	v.SetDefault("tiered_storage.cold.azure_use_managed_identity", false)
	v.SetDefault("tiered_storage.cold.azure_access_tier", "Archive") // Azure archive tier
	v.SetDefault("tiered_storage.cold.retrieval_mode", "standard")   // Standard retrieval

	// Audit log defaults (Enterprise feature)
	v.SetDefault("audit_log.enabled", false)
	v.SetDefault("audit_log.retention_days", 90)
	v.SetDefault("audit_log.include_reads", false)

	// Query governance defaults (Enterprise feature)
	v.SetDefault("governance.enabled", false)
	v.SetDefault("governance.default_rate_limit_per_min", 0)
	v.SetDefault("governance.default_rate_limit_per_hour", 0)
	v.SetDefault("governance.default_max_queries_per_hour", 0)
	v.SetDefault("governance.default_max_queries_per_day", 0)
	v.SetDefault("governance.default_max_rows_per_query", 0)

	// Query management defaults (Enterprise feature)
	v.SetDefault("query_management.enabled", false)
	v.SetDefault("query_management.history_size", 100)

	// Backup defaults
	v.SetDefault("backup.enabled", true)
	v.SetDefault("backup.local_path", "./data/backups")
}

func getDefaultThreadCount() int {
	// Use number of CPU cores for optimal parallelism
	return runtime.NumCPU()
}

// parseStringSlice parses a comma-separated string into a slice of strings.
// This is needed because Viper's GetStringSlice doesn't automatically parse
// comma-separated values from environment variables.
func parseStringSlice(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
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
