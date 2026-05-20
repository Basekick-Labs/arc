package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/memtrim"
	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver registration
	"github.com/rs/zerolog"
)

// ArrowEnabled is set to true by duckdb_arrow.go init() when compiled with the duckdb_arrow tag.
var ArrowEnabled bool

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

// quoteDuckDBIdent quotes a DuckDB identifier (table, column, setting name)
// for safe interpolation into SQL. DuckDB identifier quoting uses double
// quotes; embedded double quotes are doubled (`"foo""bar"`), matching the
// SQL standard. This is distinct from Go's %q verb, which uses Go-style
// backslash escapes that DuckDB's parser rejects.
func quoteDuckDBIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// stripURLScheme normalises an S3 endpoint into the bare "host:port" form
// that DuckDB's httpfs extension expects. The AWS SDK accepts either
// "host:port" or "scheme://host:port[/]"; DuckDB does not. Passing scheme'd
// or trailing-slashed input through verbatim produces "http://http://..."
// URLs that fail to resolve.
//
// Strips, in order:
//   - leading and trailing whitespace (paste artefacts),
//   - leading "http://" or "https://" (case-insensitive — RFC 3986 schemes
//     are case-insensitive and users routinely paste mixed-case),
//   - trailing slashes ("host:port/" → "host:port").
//
// The case of the remainder is preserved (bucket names and path components
// can be case-sensitive depending on the S3 implementation).
func stripURLScheme(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	lower := strings.ToLower(endpoint)
	switch {
	case strings.HasPrefix(lower, "https://"):
		endpoint = endpoint[len("https://"):]
	case strings.HasPrefix(lower, "http://"):
		endpoint = endpoint[len("http://"):]
	}
	return strings.TrimRight(endpoint, "/")
}

// Config holds DuckDB configuration
type Config struct {
	MaxConnections int
	MemoryLimit    string
	ThreadCount    int
	EnableWAL      bool
	// TempDirectory is where DuckDB writes query spill files (HASH_GROUP_BY
	// overflow, large sorts, joins). Empty leaves DuckDB's default
	// (CWD-relative). Orphans from a crashed previous run are swept by
	// CleanupOrphanedSpillFiles at startup.
	TempDirectory string
	// S3 configuration for httpfs extension
	S3Region    string
	S3AccessKey string
	S3SecretKey string
	S3Endpoint  string // Custom endpoint for MinIO or S3-compatible services
	S3UseSSL    bool
	S3PathStyle bool   // Use path-style addressing (required for MinIO)
	S3Bucket    string // Bucket name; used to build the allowed_directories prefix for the sandbox
	S3Prefix    string // Key prefix under the bucket; used with S3Bucket to scope sandbox access
	// Azure Blob Storage configuration for azure extension
	AzureAccountName string
	AzureAccountKey  string
	AzureEndpoint    string // Custom endpoint (optional)
	AzureContainer   string // Container name; used to build the allowed_directories prefix for the sandbox
	// Cold-tier sandbox allowlist entries. Independent from S3Bucket /
	// AzureContainer (which describe Arc's primary/hot storage) because
	// Enterprise tiered storage routinely combines hot=local with cold=S3 —
	// hot S3 fields would then be empty and a hot-only allowlist would
	// block every cold-tier query. Populated from cfg.TieredStorage.Cold
	// by cmd/arc/main.go when tiering is enabled.
	ColdS3Bucket       string
	ColdS3Prefix       string
	ColdAzureContainer string
	// LocalStorageRoot is the absolute path of the local-storage backend root,
	// used to whitelist Arc-managed files in the DuckDB sandbox. Equals
	// ArcxStorageRoot when arcx is enabled; populated independently so the
	// sandbox keeps a working entry even on deployments without arcx.
	LocalStorageRoot string
	// UploadDir is the dedicated directory the API layer uses for multipart
	// uploads (CSV/Parquet imports) and the DELETE handler's S3-rewrite
	// staging. Added to allowed_directories so DuckDB can read/write via
	// read_csv/read_parquet/COPY. Distinct from TempDirectory (DuckDB spill)
	// for clean separation; main.go usually places it under TempDirectory
	// so operators get a single config knob.
	UploadDir string
	// CompactionTempDirectory is the operator-configured base path
	// compaction jobs use to stage rewritten parquet files
	// (cfg.Compaction.TempDirectory, default ./data/compaction).
	//
	// Compaction currently runs in a subprocess (internal/compaction/
	// subprocess.go) that opens its OWN DuckDB outside this package's
	// configureDatabase, so the subprocess is NOT subject to this sandbox
	// and does not need the entry to function today. Allowlisting it
	// anyway is defensive: any future refactor moving compaction back
	// in-process would otherwise fail post-lockdown with a confusing
	// permission error on COPY ... TO. Empty disables the entry.
	CompactionTempDirectory string
	// Query optimization configuration
	EnableS3Cache     bool  // Enable S3 file caching via cache_httpfs extension
	S3CacheSize       int64 // Cache size in bytes
	S3CacheTTLSeconds int   // Cache entry TTL in seconds (default: 3600)
	// ArcxExtensionPath is the absolute path to arcx.duckdb_extension.
	// Empty disables the loader. Arc Enterprise only — the caller
	// (cmd/arc/main.go) clears this field when the license does not
	// permit arcx, so the DB layer trusts presence.
	ArcxExtensionPath string
	// ArcxStorageRoot is the filesystem root arcx's arc_partition_agg
	// table function uses to locate parquet files. Set to the local
	// storage backend's root path; ignored when ArcxExtensionPath is empty.
	ArcxStorageRoot string
}

// New creates a new DuckDB instance
func New(cfg *Config, logger zerolog.Logger) (*DuckDB, error) {
	dsn := buildDSN(cfg)

	// Open the *sql.DB. Extension registration in DuckDB is per-database
	// (ExtensionManager lives on DatabaseInstance), so a single LOAD inside
	// configureDatabase suffices for the whole pool — no connInitFn needed.
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Set connection pool limits optimized for query-heavy workloads
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxConnections)  // Keep all connections idle-ready to avoid acquisition overhead
	db.SetConnMaxLifetime(0)                // No lifetime limit - DuckDB handles connection health internally
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
func buildDSN(cfg *Config) string {
	// Loading arcx (or any unsigned extension) requires the DuckDB
	// allow_unsigned_extensions flag at connection time — it cannot be
	// flipped via SET after the connection is open. We pass it via the
	// duckdb-go driver's DSN query-string. When arcx is disabled, return
	// the empty DSN (in-memory database, default settings).
	if cfg.ArcxExtensionPath != "" {
		return "?allow_unsigned_extensions=true"
	}
	return ""
}

// arcxLoadTimeout bounds the LOAD '<path>' call so a corrupt or
// network-mounted extension file cannot hang DuckDB initialization
// indefinitely. 30s is generous for dlopen + DuckDB's Load() hook; real
// loads are tens of milliseconds.
const arcxLoadTimeout = 30 * time.Second

// arcxVerifyTimeout bounds the post-LOAD `SELECT arcx_version()` proof-
// of-life. Pure metadata read; ten seconds is generous to cover transient
// pool contention during startup while still bounding a hung DuckDB.
const arcxVerifyTimeout = 10 * time.Second

// arcxStorageRootSetting is the dotted extension-registered global setting
// arcx exposes for the partition_agg table function's filesystem root.
// SET GLOBAL "arcx.storage_root" = '<path>' propagates database-wide.
const arcxStorageRootSetting = "arcx.storage_root"

// loadArcxExtension performs a one-shot LOAD of the proprietary arcx
// extension and configures its global storage root. Extension registration
// is database-wide in DuckDB (ExtensionManager lives on DatabaseInstance),
// so a single LOAD registers arcx for every pool connection; SET GLOBAL on
// arcx-registered settings propagates the same way. Called once during
// configureDatabase. Idempotent — re-LOAD of an already-registered
// extension is a no-op success even after the sandbox lockdown.
func loadArcxExtension(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	if cfg.ArcxExtensionPath == "" {
		return nil
	}
	componentLogger := logger.With().Str("component", "duckdb").Logger()

	// filepath.ToSlash normalises Windows-style backslashes. DuckDB's LOAD
	// parses the path as a single-quoted SQL string literal where backslashes
	// are not interpreted as escapes, but Windows paths like
	// `C:\Program Files\arcx\arcx.duckdb_extension` have been observed to
	// confuse the loader on some Windows builds. Forward slashes work
	// everywhere DuckDB runs.
	path := filepath.ToSlash(cfg.ArcxExtensionPath)

	ctx, cancel := context.WithTimeout(context.Background(), arcxLoadTimeout)
	defer cancel()

	// Pinned connection: DuckDB's LOAD registers the extension on the
	// database-wide ExtensionManager, but we pin a connection anyway so
	// the LOAD and the immediately-following SET GLOBAL land on the same
	// underlying handle. Defensive against future driver changes.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire pinned connection for arcx LOAD: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD '%s'", escapeSQLString(path))); err != nil {
		return fmt.Errorf("arcx LOAD: %w", err)
	}
	if cfg.ArcxStorageRoot != "" {
		storageRoot := filepath.ToSlash(cfg.ArcxStorageRoot)
		// SET GLOBAL because arcx.storage_root is an extension-registered
		// global setting; verified empirically in Phase 0 that the value
		// propagates to fresh pool connections. Double-quoted because the
		// setting name contains a dot — bare identifiers with dots are
		// parsed as table-qualified column refs by DuckDB.
		if _, err := conn.ExecContext(ctx, "SET GLOBAL "+quoteDuckDBIdent(arcxStorageRootSetting)+" = '"+escapeSQLString(storageRoot)+"'"); err != nil {
			return fmt.Errorf("SET arcx.storage_root: %w", err)
		}
	}
	componentLogger.Info().Str("path", path).Msg("arcx extension loaded (database-wide)")
	return nil
}

// configureDatabase sets DuckDB configuration after connection
func configureDatabase(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	// Set memory limit to prevent unbounded memory growth
	if cfg.MemoryLimit != "" {
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL memory_limit='%s'", escapeSQLString(cfg.MemoryLimit))); err != nil {
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
	// Pin DuckDB's spill location so operators can place it on fast scratch
	// storage AND so CleanupOrphanedSpillFiles can sweep a known path at
	// startup. Empty leaves DuckDB's default (CWD-relative). The directory
	// must exist before DuckDB tries to write a spill file; create it with
	// 0o700 so intermediate query state is not world-readable on shared
	// hosts. escapeSQLString is sufficient defense against the path
	// reaching DuckDB's parser because Arc relies on DuckDB's default
	// standard_conforming_strings=on (single-quote doubling is the only
	// in-band escape).
	if cfg.TempDirectory != "" {
		if err := os.MkdirAll(cfg.TempDirectory, 0o700); err != nil {
			return fmt.Errorf("failed to create temp_directory %q: %w", cfg.TempDirectory, err)
		}
		logger.Info().Str("temp_directory", cfg.TempDirectory).Msg("Setting DuckDB temp directory")
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL temp_directory='%s'", escapeSQLString(cfg.TempDirectory))); err != nil {
			return fmt.Errorf("failed to set temp_directory: %w", err)
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

	// Load the proprietary arcx extension once for the whole pool. Extension
	// registration is database-wide, so a single LOAD covers every connection.
	// License gating happens upstream (cmd/arc/main.go clears
	// ArcxExtensionPath when the license does not permit it), so an empty
	// path means arcx is intentionally disabled.
	if cfg.ArcxExtensionPath != "" {
		if err := loadArcxExtension(db, cfg, logger); err != nil {
			return fmt.Errorf("failed to load arcx extension: %w", err)
		}
		if err := verifyArcxLoaded(db, cfg, logger); err != nil {
			return fmt.Errorf("failed to verify arcx extension: %w", err)
		}
	}

	// Final step: lock down DuckDB's file-access surface so user-supplied SQL
	// cannot reach arbitrary local files or remote URLs. Must run AFTER every
	// INSTALL/LOAD above (enable_external_access=false blocks future LOADs).
	if err := lockdownExternalAccess(db, cfg, logger); err != nil {
		return fmt.Errorf("failed to lock down DuckDB external access: %w", err)
	}

	return nil
}

// verifyArcxLoaded confirms the proprietary arcx DuckDB extension is
// callable on a pool connection. An empty version string signals an ABI
// mismatch or a buggy build of arcx — fail-fast rather than limping along.
//
// Pinned via db.Conn(ctx) so the verify query lands on a specific connection
// (defensive against future driver changes — extension state is currently
// database-wide on DuckDB but pinning costs nothing and survives reorgs).
func verifyArcxLoaded(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	if cfg.ArcxExtensionPath == "" {
		return nil // belt-and-suspenders; caller already guards this
	}
	componentLogger := logger.With().Str("component", "duckdb").Logger()
	ctx, cancel := context.WithTimeout(context.Background(), arcxVerifyTimeout)
	defer cancel()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire pinned connection: %w", err)
	}
	defer conn.Close()

	var ver string
	if err := conn.QueryRowContext(ctx, "SELECT arcx_version()").Scan(&ver); err != nil {
		return fmt.Errorf("arcx_version() proof-of-life: %w", err)
	}
	if strings.TrimSpace(ver) == "" {
		return fmt.Errorf("arcx_version() returned empty string (extension binary corrupt or ABI mismatch?)")
	}
	componentLogger.Info().
		Str("path", cfg.ArcxExtensionPath).
		Str("arcx_version", ver).
		Msg("arcx extension verified")
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
		if _, err := db.Exec(fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", escapeSQLString(stripURLScheme(cfg.S3Endpoint)))); err != nil {
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
					// Scale glob/metadata/file-handle cache sizes proportionally.
					// A 7-day hourly query generates ~168 glob patterns — the default
					// 64 entries causes constant eviction on large deployments.
					globEntries := max(maxBlocks/20, 64)      // ~5% of blocks, floor at default
					metadataEntries := max(maxBlocks/10, 250) // ~10% of blocks, floor at default
					fileHandleEntries := max(maxBlocks/10, 250)
					if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_glob_cache_entry_size=%d", globEntries)); err != nil {
						logger.Warn().Err(err).Msg("Failed to set cache_httpfs_glob_cache_entry_size")
					}
					if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_metadata_cache_entry_size=%d", metadataEntries)); err != nil {
						logger.Warn().Err(err).Msg("Failed to set cache_httpfs_metadata_cache_entry_size")
					}
					if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_file_handle_cache_entry_size=%d", fileHandleEntries)); err != nil {
						logger.Warn().Err(err).Msg("Failed to set cache_httpfs_file_handle_cache_entry_size")
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
				// Metadata and file handle TTLs match s3_cache_ttl_seconds — these
				// reference immutable individual parquet files.
				if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_metadata_cache_entry_timeout_millisec=%d", ttlMs)); err != nil {
					logger.Warn().Err(err).Msg("Failed to set cache_httpfs_metadata_cache_entry_timeout_millisec")
				}
				if _, err := db.Exec(fmt.Sprintf("SET GLOBAL cache_httpfs_file_handle_cache_entry_timeout_millisec=%d", ttlMs)); err != nil {
					logger.Warn().Err(err).Msg("Failed to set cache_httpfs_file_handle_cache_entry_timeout_millisec")
				}
			}
			// Glob TTL: 10s — directory listings change during compaction and S3 LIST
			// overhead is negligible. Post-compaction invalidation handles the rest.
			if _, err := db.Exec("SET GLOBAL cache_httpfs_glob_cache_entry_timeout_millisec=10000"); err != nil {
				logger.Warn().Err(err).Msg("Failed to set cache_httpfs_glob_cache_entry_timeout_millisec")
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
		if _, err := d.db.Exec(fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", escapeSQLString(stripURLScheme(s3cfg.Endpoint)))); err != nil {
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
// Call after compaction/delete/retention so subsequent queries don't hit stale
// cache entries pointing to files that no longer exist. Also asks glibc to
// release native-heap pages — debug.FreeOSMemory only covers Go-managed memory;
// CGo allocations from the DuckDB httpfs extension live outside it.
func (d *DuckDB) ClearHTTPCache() {
	if _, err := d.db.Exec("SELECT cache_httpfs_clear_cache()"); err != nil {
		d.logger.Debug().Err(err).Msg("cache_httpfs_clear_cache not available (extension may not be loaded)")
	} else {
		d.logger.Info().Msg("Cleared cache_httpfs cache")
	}

	// Toggle disable then re-enable — always attempt the re-enable even if
	// disable failed, so a transient disable error doesn't leave the cache
	// in an unintended off state on a connection.
	if _, err := d.db.Exec("SET GLOBAL parquet_metadata_cache=false"); err != nil {
		d.logger.Debug().Err(err).Msg("Failed to disable parquet_metadata_cache")
	}
	if _, err := d.db.Exec("SET GLOBAL parquet_metadata_cache=true"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to re-enable parquet_metadata_cache")
	} else {
		d.logger.Info().Msg("Reset parquet_metadata_cache")
	}

	if memtrim.ReleaseToOS() {
		d.logger.Info().Str("source", "clear_http_cache").Msg("Released glibc heap pages to OS")
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

// Close closes the database connection. DuckDB unlinks spill files in its
// own Close path; we deliberately do NOT re-sweep here. Re-review thread:
// (a) on the happy path it's a no-op; (b) the 60s mtime guard would skip
// freshly-written files anyway; (c) running it from a SIGTERM handler
// risks stalling shutdown past systemd's TimeoutStopSec. The startup
// sweep in cmd/arc/main.go covers the crash case, which is the only path
// that actually leaks.
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
//
// The caller MUST close both resources when done:
//  1. rows.Close() — releases the result set
//  2. conn.Close() — returns the pinned connection to the pool
func (d *DuckDB) QueryWithProfile(query string) (*sql.Rows, *sql.Conn, *QueryProfile, error) {
	return d.QueryWithProfileContext(context.Background(), query)
}

// QueryWithProfileContext executes a query with context support for timeout/cancellation
// and returns timing breakdown using DuckDB profiling.
// All profiling PRAGMAs and the query are pinned to a single connection to avoid
// race conditions across the connection pool.
//
// The caller MUST close both resources when done:
//  1. rows.Close() — releases the result set
//  2. conn.Close() — returns the pinned connection to the pool
func (d *DuckDB) QueryWithProfileContext(ctx context.Context, query string) (*sql.Rows, *sql.Conn, *QueryProfile, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Create a temporary file for profiling output. MUST land inside the
	// DuckDB sandbox's allowed_directories — d.config.TempDirectory is
	// always allowlisted (see buildAllowedDirectories), os.TempDir() is
	// not. An empty TempDirectory would make CreateTemp fall back to
	// os.TempDir() which the sandbox rejects, so explicitly fall through
	// to the non-profile path without even attempting the file create.
	var tmpFile *os.File
	if d.config.TempDirectory == "" {
		d.logger.Debug().Msg("Profile mode requested but TempDirectory is unset; returning result without profile data")
	} else {
		var err error
		tmpFile, err = os.CreateTemp(d.config.TempDirectory, "duckdb_profile_*.json")
		if err != nil {
			d.logger.Warn().Err(err).Str("temp_dir", d.config.TempDirectory).Msg("Failed to create profile temp file; falling back to non-profile query path")
			tmpFile = nil
		}
	}
	if tmpFile == nil {
		// No usable temp dir — return a regular query result without profile data.
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			conn.Close()
			return nil, nil, nil, err
		}
		return rows, conn, nil, nil
	}
	profilePath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(profilePath)

	// Enable JSON profiling with custom metrics to capture planning time
	// All PRAGMAs run on the same pinned connection
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_profiling='json'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to enable profiling")
	}
	// profilePath includes the operator-controlled d.config.TempDirectory
	// prefix; escape it the same way SET GLOBAL temp_directory does above
	// to neutralise any embedded single quote (operator config like
	// "/data/arc/it's-folder" would otherwise break out of the SQL literal).
	// ToSlash so Windows backslashes from os.CreateTemp match the sandbox
	// allowlist (allowed_directories stores forward-slash entries).
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA profiling_output='%s'", escapeSQLString(filepath.ToSlash(profilePath)))); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set profiling output")
	}
	// Enable planner timing metrics
	if _, err := conn.ExecContext(ctx, "SET custom_profiling_settings='{\"PLANNER\": \"true\", \"PLANNER_BINDING\": \"true\", \"PHYSICAL_PLANNER\": \"true\", \"OPERATOR_TIMING\": \"true\", \"OPERATOR_CARDINALITY\": \"true\"}'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set custom profiling settings")
	}

	// Execute the query with timing and context on the pinned connection
	start := time.Now()
	rows, err := conn.QueryContext(ctx, query)
	totalTime := time.Since(start)

	// Disable profiling on the same connection
	conn.ExecContext(ctx, "PRAGMA disable_profiling")

	if err != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("query failed: %w", err)
	}

	// Parse the profiling output
	profile := d.parseProfileOutput(profilePath, totalTime)

	d.logger.Debug().
		Str("query", query).
		Float64("total_ms", profile.TotalMs).
		Float64("planner_ms", profile.PlannerMs).
		Float64("execution_ms", profile.ExecutionMs).
		Msg("Query profiled")

	return rows, conn, profile, nil
}

// duckdbProfileOutput represents the JSON structure from DuckDB profiling
type duckdbProfileOutput struct {
	Latency     float64                 `json:"latency"`
	RowsScanned uint64                  `json:"operator_rows_scanned"`
	Planner     float64                 `json:"planner"`
	Children    []duckdbProfileOperator `json:"children"`
	Timings     map[string]interface{}  `json:"timings"`
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
