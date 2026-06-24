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

// S3 secret names. Primary storage and the cold tier get SEPARATE, SCOPE-bound
// secrets so a query against the primary bucket and a query against the cold
// bucket each resolve their own credentials. A single shared secret would let
// the runtime cold-tier ConfigureS3 overwrite the primary's credentials (the two
// tiers can use different buckets/accounts), so they must not share a name.
const (
	arcS3PrimarySecretName = "arc_s3_primary"
	arcS3ColdSecretName    = "arc_s3_cold"
)

// Azure secret names. Same rationale as the S3 names above: primary and cold
// Azure storage get separate, SCOPE-bound secrets so distinct containers/accounts
// per tier don't clobber each other.
const (
	arcAzurePrimarySecretName = "azure_secret_primary"
	arcAzureColdSecretName    = "azure_secret_cold"
)

// s3SecretParams describes one DuckDB S3 secret to create.
type s3SecretParams struct {
	name      string // secret name (must be unique per credential set)
	scope     string // s3://bucket/prefix/ this secret applies to; "" = unscoped
	accessKey string
	secretKey string
	region    string
	endpoint  string
	pathStyle bool
	useSSL    bool
}

// buildS3SecretSQL builds a `CREATE OR REPLACE SECRET <name> (TYPE S3, ...)`
// statement. Using DuckDB's secrets manager (instead of
// `SET GLOBAL s3_secret_access_key`) keeps the secret out of current_setting():
// the value is unreadable via SQL and redacted in duckdb_secrets(), closing the
// exfiltration path where any authenticated query user could
// `SELECT current_setting('s3_secret_access_key')`.
//
// The secret is TEMPORARY by default (no PERSISTENT keyword): it lives in-memory
// for the life of the DuckDB instance and is visible to every pooled connection,
// matching the old SET GLOBAL behavior. PERSISTENT must NOT be used — it would
// write the key unencrypted to ~/.duckdb/stored_secrets.
//
// SCOPE: when non-empty, the secret applies only to paths under that prefix, so
// primary and cold-tier secrets coexist and DuckDB picks the right credentials
// per read_parquet() path (longest-prefix match). An empty scope is unscoped
// (applies to all s3:// paths) — the single-tier default.
//
// Credentials are three-way:
//   - both accessKey and secretKey set → static-key secret (KEY_ID/SECRET).
//   - both empty → PROVIDER CREDENTIAL_CHAIN, so DuckDB falls back to the AWS
//     credential chain (env vars, IAM instance profile / IRSA). Verified against
//     the bundled DuckDB that CREDENTIAL_CHAIN composes with REGION/ENDPOINT/
//     URL_STYLE/USE_SSL, so custom-endpoint (MinIO-with-env-creds) still works.
//   - exactly one set → returns an error. Silently routing a half-supplied
//     credential to the credential chain would discard the provided key and
//     authenticate as a different identity (e.g. the host instance role) with no
//     signal — a misconfiguration trap, not a convenience.
//
// accessKey/secretKey/region/endpoint/scope are escaped (single quotes doubled);
// pathStyle and useSSL are program-controlled and emitted as bare enum/bool
// literals. region/endpoint/scope are only included when non-empty. The endpoint
// is scheme-stripped internally via stripURLScheme, so callers may pass a raw
// "https://host:port" value.
func buildS3SecretSQL(p s3SecretParams) (string, error) {
	hasKey, hasSecret := p.accessKey != "", p.secretKey != ""
	if hasKey != hasSecret {
		return "", fmt.Errorf("S3 credentials misconfigured for secret %q: exactly one of access key / secret key is set; provide both (static credentials) or neither (AWS credential chain)", p.name)
	}

	var b strings.Builder
	b.WriteString("CREATE OR REPLACE SECRET ")
	b.WriteString(p.name)
	b.WriteString(" (\n\tTYPE S3")
	if hasKey {
		b.WriteString(",\n\tKEY_ID '")
		b.WriteString(escapeSQLString(p.accessKey))
		b.WriteString("',\n\tSECRET '")
		b.WriteString(escapeSQLString(p.secretKey))
		b.WriteString("'")
	} else {
		// No static credentials: defer to the AWS credential chain.
		b.WriteString(",\n\tPROVIDER CREDENTIAL_CHAIN")
	}
	if p.region != "" {
		b.WriteString(",\n\tREGION '")
		b.WriteString(escapeSQLString(p.region))
		b.WriteString("'")
	}
	// Check the stripped value, not the raw endpoint: a malformed config like
	// "http://" or whitespace strips to "" and must be treated as "no endpoint"
	// rather than emitting an empty ENDPOINT '' clause.
	if stripped := stripURLScheme(p.endpoint); stripped != "" {
		b.WriteString(",\n\tENDPOINT '")
		b.WriteString(escapeSQLString(stripped))
		b.WriteString("'")
	}
	urlStyle := "vhost"
	if p.pathStyle {
		urlStyle = "path"
	}
	b.WriteString(",\n\tURL_STYLE '")
	b.WriteString(urlStyle)
	b.WriteString("',\n\tUSE_SSL ")
	if p.useSSL {
		b.WriteString("true")
	} else {
		b.WriteString("false")
	}
	if p.scope != "" {
		b.WriteString(",\n\tSCOPE '")
		b.WriteString(escapeSQLString(p.scope))
		b.WriteString("'")
	}
	b.WriteString("\n)")
	return b.String(), nil
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
	// S3IsPrimaryBackend is true when the primary/hot store is S3-compatible
	// (storage.backend in {"s3","minio"}). Decouples "a primary S3 secret must
	// exist" from "static keys are set", so IRSA / credential-chain deployments
	// (empty keys) still get a primary secret with PROVIDER CREDENTIAL_CHAIN.
	// See the gate in configureDatabase.
	S3IsPrimaryBackend bool
	// Azure Blob Storage configuration for azure extension
	AzureAccountName string
	AzureAccountKey  string
	// AzureConnectionString embeds the account identity (and key) itself. When
	// set, it is the primary auth method and AzureAccountName may be empty —
	// mirrors the Go backend's first auth case (internal/storage/azure_blob.go).
	AzureConnectionString string
	AzureEndpoint         string // Custom endpoint (optional)
	AzureContainer        string // Container name; used to build the allowed_directories prefix for the sandbox
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

	// S3 is active when the primary backend is S3 (incl. IRSA / credential-chain
	// with empty keys) or static keys are configured. Keying only off key
	// presence would log s3_enabled=false for a working IRSA deployment.
	s3Enabled := cfg.S3IsPrimaryBackend || (cfg.S3AccessKey != "" && cfg.S3SecretKey != "")
	azureEnabled := cfg.AzureAccountName != "" || cfg.AzureConnectionString != ""
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

	// Configure httpfs extension + primary S3 secret if primary storage uses S3.
	//   - S3IsPrimaryBackend (storage.backend=="s3"): always create a primary
	//     secret. With static keys → KEY_ID/SECRET; with both keys empty →
	//     PROVIDER CREDENTIAL_CHAIN, so IRSA / IAM instance role / env creds
	//     authenticate s3:// query reads (buildS3SecretSQL handles both).
	//   - Keys set without the backend signal (legacy/explicit): still handled,
	//     and a half-configured pair (exactly one key set) reaches
	//     configureS3Access and is rejected by buildS3SecretSQL at startup rather
	//     than being silently skipped.
	// Both-empty AND backend!="s3" falls through to the cold-tier branch (primary
	// storage is not S3).
	if cfg.S3IsPrimaryBackend || cfg.S3AccessKey != "" || cfg.S3SecretKey != "" {
		if err := configureS3Access(db, cfg, logger); err != nil {
			return fmt.Errorf("failed to configure S3 access: %w", err)
		}
	} else if cfg.ColdS3Bucket != "" {
		// Primary storage is not S3, but a cold tier targets S3. httpfs must be
		// loaded at startup (before the sandbox lockdown blocks INSTALL/LOAD) so
		// the runtime ConfigureS3 cold-tier secret can use the S3 secret type.
		// No primary secret is created here — cold-tier credentials are applied
		// later via ConfigureS3.
		if err := ensureHTTPFSLoaded(db); err != nil {
			return fmt.Errorf("failed to load httpfs for cold-tier S3: %w", err)
		}
	}

	// Configure azure extension for Azure Blob Storage access. Only the account
	// name is required: when AzureAccountKey is empty, configureAzureAccess
	// provisions a PROVIDER CREDENTIAL_CHAIN secret so managed identity / az-login
	// / env credentials work (mirrors the S3 credential-chain behavior).
	if cfg.AzureAccountName != "" || cfg.AzureConnectionString != "" {
		if err := configureAzureAccess(db, cfg, logger); err != nil {
			return fmt.Errorf("failed to configure Azure access: %w", err)
		}
	} else if cfg.ColdAzureContainer != "" {
		// Primary storage is not Azure, but a cold tier targets Azure. Load the
		// azure extension at startup (before the sandbox lockdown blocks
		// INSTALL/LOAD) so the runtime ConfigureAzure cold-tier secret works. No
		// primary secret is created here — cold credentials are applied later via
		// ConfigureAzure. Mirrors the cold-tier S3 branch above.
		if err := ensureAzureLoaded(db, logger); err != nil {
			return fmt.Errorf("failed to load azure extension for cold-tier Azure: %w", err)
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

// ensureHTTPFSLoaded installs and loads the httpfs extension. httpfs registers
// the S3 secret type, so it MUST be loaded before any CREATE SECRET (TYPE S3) —
// including the runtime cold-tier secret created by ConfigureS3 — and before the
// sandbox lockdown (enable_external_access=false blocks INSTALL/LOAD). Loading
// httpfs is idempotent, so calling this for both primary and cold-tier S3 is
// safe.
func ensureHTTPFSLoaded(db *sql.DB) error {
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		return fmt.Errorf("failed to install httpfs: %w", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		return fmt.Errorf("failed to load httpfs: %w", err)
	}
	return nil
}

// configureS3Access sets up the httpfs extension and the primary-storage S3
// secret. S3 credentials are stored in DuckDB's secrets manager via CREATE
// SECRET (not SET GLOBAL) so the secret key cannot be read back through the
// query API via current_setting(); see buildS3SecretSQL. The secret is
// instance-scoped and therefore visible to every connection in the pool, like
// the old SET GLOBAL.
func configureS3Access(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	if err := ensureHTTPFSLoaded(db); err != nil {
		return err
	}

	// Store S3 credentials + endpoint config in the secrets manager. Must run
	// after LOAD httpfs (which registers the S3 secret type). CREATE SECRET is a
	// catalog op and is NOT gated by enable_external_access, so order relative to
	// the sandbox lockdown is immaterial; we run it here for locality.
	//
	// Scope the primary secret to the primary bucket/prefix when known, so it
	// coexists with a separately-scoped cold-tier secret (see DuckDB.ConfigureS3)
	// instead of one clobbering the other. When no bucket is configured the scope
	// is empty (unscoped), preserving single-tier behavior.
	secretSQL, err := buildS3SecretSQL(s3SecretParams{
		name:      arcS3PrimarySecretName,
		scope:     s3SecretScope(cfg.S3Bucket, cfg.S3Prefix),
		accessKey: cfg.S3AccessKey,
		secretKey: cfg.S3SecretKey,
		region:    cfg.S3Region,
		endpoint:  cfg.S3Endpoint,
		pathStyle: cfg.S3PathStyle,
		useSSL:    cfg.S3UseSSL,
	})
	if err != nil {
		return err
	}
	if _, err := db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to create S3 secret: %w", err)
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
	// Bucket/Prefix scope the cold-tier secret to its own bucket/prefix so it
	// does not clobber the primary S3 secret. Empty Bucket → unscoped secret.
	Bucket string
	Prefix string
}

// ConfigureS3 reconfigures DuckDB's S3 settings at runtime.
// This is useful when tiered storage uses different S3 credentials than the main storage.
//
// httpfs must already be loaded: configureDatabase loads it at startup whenever
// primary OR cold storage uses S3 (see the ColdS3Bucket branch), which is before
// the sandbox lockdown blocks INSTALL/LOAD. This is the only thing that makes the
// CREATE SECRET (TYPE S3) below work on a local-primary + S3-cold deployment.
//
// Credentials go into the secrets manager via CREATE OR REPLACE SECRET under a
// DEDICATED cold-tier name (arc_s3_cold), scoped to the cold bucket/prefix. This
// must NOT reuse the primary secret name — primary and cold can use different
// buckets/accounts, and a shared secret would let cold credentials clobber the
// primary's. With distinct scoped secrets, DuckDB resolves the right credentials
// per read_parquet() path. This runs after the sandbox lockdown
// (enable_external_access=false); CREATE SECRET is a catalog op, not gated by it.
func (d *DuckDB) ConfigureS3(s3cfg *S3Config) error {
	if s3cfg == nil {
		return fmt.Errorf("ConfigureS3: s3cfg must not be nil")
	}
	secretSQL, err := buildS3SecretSQL(s3SecretParams{
		name:      arcS3ColdSecretName,
		scope:     s3SecretScope(s3cfg.Bucket, s3cfg.Prefix),
		accessKey: s3cfg.AccessKey,
		secretKey: s3cfg.SecretKey,
		region:    s3cfg.Region,
		endpoint:  s3cfg.Endpoint,
		pathStyle: s3cfg.PathStyle,
		useSSL:    s3cfg.UseSSL,
	})
	if err != nil {
		return err
	}
	if _, err := d.db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to create S3 secret: %w", err)
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

// azureSecretParams describes one DuckDB Azure secret to create.
type azureSecretParams struct {
	name  string // secret name (unique per credential set)
	scope string // azure://container/ this secret applies to; "" = unscoped
	// connectionString, when set, is the auth method (it embeds the account
	// name + key); accountName/accountKey are then ignored. Mirrors the Go
	// backend's connection-string-first precedence.
	connectionString string
	accountName      string
	accountKey       string // empty → PROVIDER CREDENTIAL_CHAIN (managed identity / env)
}

// buildAzureSecretSQL builds a `CREATE OR REPLACE SECRET <name> (TYPE AZURE, ...)`
// statement. Auth precedence mirrors the Go backend (internal/storage/azure_blob.go):
//   - an explicit connection string → CONNECTION_STRING (account name not required;
//     the connection string embeds it);
//   - account name + key → a synthesized AccountName=…;AccountKey=… connection string;
//   - account name, no key → PROVIDER CREDENTIAL_CHAIN (managed identity / az-login / env).
//
// SCOPE, when non-empty, binds the secret to one container so primary and cold-tier
// Azure secrets coexist and DuckDB resolves the right credentials per path. Values are
// escaped (single quotes doubled). Mirrors buildS3SecretSQL.
func buildAzureSecretSQL(p azureSecretParams) (string, error) {
	if p.connectionString == "" && p.accountName == "" {
		return "", fmt.Errorf("azure secret %q: account name or connection string is required", p.name)
	}
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE SECRET ")
	b.WriteString(p.name)
	b.WriteString(" (\n\tTYPE AZURE")
	switch {
	case p.connectionString != "":
		// Operator-supplied connection string (embeds account name + key/SAS).
		b.WriteString(",\n\tCONNECTION_STRING '")
		b.WriteString(escapeSQLString(p.connectionString))
		b.WriteString("'")
	case p.accountKey != "":
		// Synthesize a connection string from account name + key.
		connStr := "AccountName=" + p.accountName + ";AccountKey=" + p.accountKey
		b.WriteString(",\n\tCONNECTION_STRING '")
		b.WriteString(escapeSQLString(connStr))
		b.WriteString("'")
	default:
		// No key: defer to the Azure credential chain (managed identity / env).
		b.WriteString(",\n\tPROVIDER CREDENTIAL_CHAIN,\n\tACCOUNT_NAME '")
		b.WriteString(escapeSQLString(p.accountName))
		b.WriteString("'")
	}
	if p.scope != "" {
		b.WriteString(",\n\tSCOPE '")
		b.WriteString(escapeSQLString(p.scope))
		b.WriteString("'")
	}
	b.WriteString("\n)")
	return b.String(), nil
}

// azureScope builds the SCOPE prefix for an Azure secret from a container name,
// or "" (unscoped) when no container is configured.
func azureScope(container string) string {
	if container == "" {
		return ""
	}
	return "azure://" + container + "/"
}

// ensureAzureLoaded installs and loads the azure extension and sets the Linux
// curl transport. Like httpfs for S3, this MUST run before any
// CREATE SECRET (TYPE AZURE) — including the runtime cold-tier secret created by
// ConfigureAzure — and before the sandbox lockdown. Idempotent.
func ensureAzureLoaded(db *sql.DB, logger zerolog.Logger) error {
	if _, err := db.Exec("INSTALL azure"); err != nil {
		return fmt.Errorf("failed to install azure: %w", err)
	}
	if _, err := db.Exec("LOAD azure"); err != nil {
		return fmt.Errorf("failed to load azure: %w", err)
	}
	// Set transport option to curl on Linux to resolve potential SSL cert issues.
	if runtime.GOOS == "linux" {
		if _, err := db.Exec("SET GLOBAL azure_transport_option_type = 'curl'"); err != nil {
			return fmt.Errorf("failed to set azure_transport_option_type: %w", err)
		}
		logger.Info().Str("azure_transport_option", "curl").Msg("Azure transport option set to curl for Linux")
	}
	return nil
}

// configureAzureAccess sets up the azure extension and the PRIMARY Azure secret,
// scoped to the primary container so it coexists with a separately-scoped
// cold-tier secret (see DuckDB.ConfigureAzure) instead of clobbering it.
func configureAzureAccess(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	if err := ensureAzureLoaded(db, logger); err != nil {
		return err
	}
	secretSQL, err := buildAzureSecretSQL(azureSecretParams{
		name:             arcAzurePrimarySecretName,
		scope:            azureScope(cfg.AzureContainer),
		connectionString: cfg.AzureConnectionString,
		accountName:      cfg.AzureAccountName,
		accountKey:       cfg.AzureAccountKey,
	})
	if err != nil {
		return err
	}
	if _, err := db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to create azure secret: %w", err)
	}
	return nil
}

// AzureConfig holds Azure configuration for a runtime (cold-tier) secret.
type AzureConfig struct {
	// ConnectionString, when set, is the auth method (embeds account name+key);
	// AccountName/AccountKey are then ignored. Mirrors the primary path.
	ConnectionString string
	AccountName      string
	AccountKey       string
	Container        string // scopes the secret to this container; empty = unscoped
}

// ConfigureAzure provisions the cold-tier Azure secret at runtime, under a
// DEDICATED name (azure_secret_cold) scoped to the cold container, so it does not
// clobber the primary Azure secret. Mirrors ConfigureS3. The azure extension must
// already be loaded (configureDatabase loads it at startup whenever primary OR
// cold storage uses Azure, before the sandbox lockdown).
func (d *DuckDB) ConfigureAzure(azcfg *AzureConfig) error {
	if azcfg == nil {
		return fmt.Errorf("ConfigureAzure: azcfg must not be nil")
	}
	secretSQL, err := buildAzureSecretSQL(azureSecretParams{
		name:             arcAzureColdSecretName,
		scope:            azureScope(azcfg.Container),
		connectionString: azcfg.ConnectionString,
		accountName:      azcfg.AccountName,
		accountKey:       azcfg.AccountKey,
	})
	if err != nil {
		return err
	}
	if _, err := d.db.Exec(secretSQL); err != nil {
		return fmt.Errorf("failed to create cold-tier azure secret: %w", err)
	}
	d.logger.Info().Str("account", azcfg.AccountName).Str("container", azcfg.Container).Msg("DuckDB cold-tier Azure secret configured")
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
