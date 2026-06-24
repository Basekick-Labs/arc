package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/cluster"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/fips"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/mqtt"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/basekick-labs/arc/internal/scheduler"
	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/telemetry"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/basekick-labs/arc/internal/wal"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Version is set at build time
var Version = "dev"

// uploadSubdirName is the fixed name of the multipart-upload directory Arc
// creates beneath cfg.Database.TempDirectory. cfg.Database.TempDirectory
// always resolves to a non-empty absolute path before this is used (the
// "./.tmp" fallback runs in main() before any consumer); the directory is
// NEVER created under os.TempDir, because os.TempDir is intentionally
// outside the DuckDB sandbox allowlist. MUST stay in sync with whatever
// path the DB layer adds to allowed_directories, otherwise reads of
// uploaded files fail with a permission error.
const uploadSubdirName = "arc-uploads"

// cacheInvalidateHMACTolerance is the freshness window for HMACs on the
// cluster cache-invalidate fan-out. Aliased to the package-wide
// security.HMACTimestampTolerance so a future change to the cluster's
// HMAC freshness window propagates here automatically — Gemini round 1
// on PR #449 flagged the hardcoded constants scattered across the
// codebase. Shared between the NonceCache TTL and the receiver-side
// ValidateCacheInvalidateHMAC call so a nonce expires from the cache
// at the same instant its MAC would be rejected as stale.
const cacheInvalidateHMACTolerance = security.HMACTimestampTolerance

func main() {
	// Check for subcommands before loading full config
	if len(os.Args) > 1 && os.Args[1] == "compact" {
		runCompactSubcommand(os.Args[2:])
		return
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Validate TLS configuration before starting
	if err := cfg.Server.ValidateTLS(); err != nil {
		fmt.Fprintf(os.Stderr, "TLS configuration error: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger.Setup(cfg.Log.Level, cfg.Log.Format)
	log.Info().Str("version", Version).Bool("duckdb_arrow", database.ArrowEnabled).Bool("fips_mode", fips.Enabled()).Msg("Starting Arc...")

	// Fail closed: the arc-fips build (fips.BuildTagged) MUST run with the Go
	// Cryptographic Module actually in FIPS mode. The binary bakes in
	// GODEBUG=fips140=only (see cmd/arc/fips.go), but an operator could still
	// override it; refuse to start rather than silently run a "FIPS" binary
	// outside FIPS mode, which would defeat the posture and mislead an auditor.
	if fips.BuildTagged && !fips.Enabled() {
		log.Fatal().Msg("FIPS build started without the Go Cryptographic Module in FIPS mode (GODEBUG=fips140 disabled); refusing to start. Remove any GODEBUG=fips140=off override.")
	}

	// Validate Enterprise License early (before component initialization)
	// This allows us to apply core limits to DuckDB and ingestion workers
	var licenseClient *license.Client
	if cfg.License.Key != "" {
		log.Info().
			Str("license_key", cfg.License.Key[:min(12, len(cfg.License.Key))]+"...").
			Str("server_url", license.LicenseServerURL).
			Msg("Validating enterprise license")

		var err error
		licenseClient, err = license.NewClient(&license.ClientConfig{
			LicenseKey: cfg.License.Key,
			Logger:     logger.Get("license"),
		})
		if err != nil {
			log.Warn().Err(err).Msg("Failed to initialize license client - enterprise features disabled")
		} else {
			// Activate or verify license at startup
			lic, err := licenseClient.ActivateOrVerify(context.Background())
			if err != nil {
				log.Warn().
					Err(err).
					Str("license_key", cfg.License.Key[:min(12, len(cfg.License.Key))]+"...").
					Msg("License activation/verification failed - enterprise features disabled")
				licenseClient = nil
			} else {
				log.Info().
					Str("tier", string(lic.Tier)).
					Str("status", lic.Status).
					Int("days_remaining", lic.DaysRemaining).
					Int("max_cores", lic.MaxCores).
					Time("expires_at", lic.ExpiresAt).
					Strs("features", lic.Features).
					Msg("Enterprise license verified successfully")

				// Apply core limits from license to config
				// This ensures DuckDB and ingestion workers respect the license
				if lic.MaxCores > 0 {
					machineCores := runtime.NumCPU()

					if machineCores > lic.MaxCores {
						// Limit Go runtime to licensed cores - this is the real enforcement
						previousGOMAXPROCS := runtime.GOMAXPROCS(lic.MaxCores)
						log.Info().
							Int("machine_cores", machineCores).
							Int("licensed_cores", lic.MaxCores).
							Int("gomaxprocs_before", previousGOMAXPROCS).
							Int("gomaxprocs_after", lic.MaxCores).
							Msg("License core limit applied via GOMAXPROCS")

						// Also set DuckDB thread count to match
						cfg.Database.ThreadCount = lic.MaxCores
						log.Info().
							Int("duckdb_threads", lic.MaxCores).
							Msg("License core limit applied to DuckDB threads")

						// Limit ingestion flush workers to licensed cores
						if cfg.Ingest.FlushWorkers > lic.MaxCores {
							cfg.Ingest.FlushWorkers = lic.MaxCores
							log.Info().
								Int("flush_workers", lic.MaxCores).
								Msg("License core limit applied to ingestion flush workers")
						}
					}
				}
			}
		}
	} else {
		log.Warn().Msg("Enterprise license not configured - enterprise features disabled")
	}

	// If no working license — either none configured, or validation failed
	// above and licenseClient was reset to nil — surface a single invite
	// line so operators on the OSS edition see what they're missing.
	// Deliberately placed after both no-license branches so it fires
	// exactly once regardless of which path got us here.
	if licenseClient == nil {
		log.Info().
			Str("url", "https://basekick.net/enterprise").
			Msg("Running Arc OSS — try Arc Enterprise for tiering, clustering, RBAC, audit, and arcx")
	}

	// Initialize metrics collector
	metrics.Init(logger.Get("metrics"))

	// Initialize timeseries collector with config
	metrics.InitTimeSeriesCollector(
		cfg.Metrics.TimeseriesRetentionMinutes,
		cfg.Metrics.TimeseriesIntervalSeconds,
	)
	log.Info().
		Int("retention_minutes", cfg.Metrics.TimeseriesRetentionMinutes).
		Int("interval_seconds", cfg.Metrics.TimeseriesIntervalSeconds).
		Msg("Timeseries metrics collector initialized")

	// Initialize shutdown coordinator
	shutdownCoordinator := shutdown.New(30*time.Second, logger.Get("shutdown"))

	// Opt-in pprof on a localhost listener (no-op unless ARC_DEBUG_PPROF=1).
	// Replaces the previous behaviour where pprof was unconditionally
	// mounted on the public Fiber app and any network-reachable caller
	// could fetch heap dumps or pin a CPU core via /debug/pprof/profile.
	// See cmd/arc/debug_pprof.go for the rationale. Closes audit #2
	// (GHSA-j93g-rp6m-j32m) from 2026-05-19.
	startDebugPprofIfEnabled(shutdownCoordinator, logger.Get("debug-pprof"))

	// arcx (Arc Enterprise DuckDB extension): gate via license before
	// passing the path down to the DB layer. The extension binary is the
	// licensing perimeter, but having Arc refuse to LOAD without a valid
	// license is the operator-friendly behavior.
	arcxPath := cfg.Database.ArcxExtensionPath
	if arcxPath != "" {
		if licenseClient == nil || !licenseClient.CanUseArcx() {
			log.Warn().
				Str("arcx_extension_path", arcxPath).
				Msg("arcx extension configured but license does not include 'arcx' feature — extension will not load")
			arcxPath = ""
		}
	}

	// Initialize DuckDB
	log.Info().
		Int("thread_count", cfg.Database.ThreadCount).
		Int("max_connections", cfg.Database.MaxConnections).
		Str("memory_limit", cfg.Database.MemoryLimit).
		Bool("arcx_enabled", arcxPath != "").
		Int("machine_cpus", runtime.NumCPU()).
		Msg("Initializing DuckDB with database config")
	dbConfig := &database.Config{
		MaxConnections: cfg.Database.MaxConnections,
		MemoryLimit:    cfg.Database.MemoryLimit,
		ThreadCount:    cfg.Database.ThreadCount,
		EnableWAL:      cfg.Database.EnableWAL,
		TempDirectory:  cfg.Database.TempDirectory,
		// S3 configuration for httpfs extension (enables DuckDB to query S3 directly)
		S3Region:    cfg.Storage.S3Region,
		S3AccessKey: cfg.Storage.S3AccessKey,
		S3SecretKey: cfg.Storage.S3SecretKey,
		S3Endpoint:  cfg.Storage.S3Endpoint,
		S3UseSSL:    cfg.Storage.S3UseSSL,
		S3PathStyle: cfg.Storage.S3PathStyle,
		S3Bucket:    cfg.Storage.S3Bucket,
		S3Prefix:    cfg.Storage.S3Prefix,
		// Primary-backend signal: when the primary store is S3-compatible
		// (storage.backend is "s3" or "minio" — the same set switched on at the
		// storage-backend selection below), create a primary S3 secret even with
		// empty keys (PROVIDER CREDENTIAL_CHAIN) so IRSA / IAM role / env
		// credentials authenticate DuckDB query reads against s3://.
		S3IsPrimaryBackend: cfg.Storage.Backend == "s3" || cfg.Storage.Backend == "minio",
		// Azure Blob Storage configuration for azure extension
		AzureAccountName:      cfg.Storage.AzureAccountName,
		AzureAccountKey:       cfg.Storage.AzureAccountKey,
		AzureConnectionString: cfg.Storage.AzureConnectionString,
		AzureEndpoint:         cfg.Storage.AzureEndpoint,
		AzureContainer:        cfg.Storage.AzureContainer,
		// Cold-tier sandbox allowlist entries. The cold tier may use a
		// different bucket/container from the primary backend (commonly
		// hot=local + cold=S3 on Enterprise); the sandbox must allow both.
		// Populated unconditionally — empty values are ignored by
		// buildAllowedDirectories. License gating happens later in main.go
		// before the tiering manager actually runs.
		ColdS3Bucket:       cfg.TieredStorage.Cold.S3Bucket,
		ColdS3Prefix:       cfg.TieredStorage.Cold.S3Prefix,
		ColdAzureContainer: cfg.TieredStorage.Cold.AzureContainer,
		// Local storage root used by the DuckDB sandbox to whitelist
		// Arc-managed file paths in allowed_directories. Always populated
		// regardless of the configured backend; on S3/Azure-only deployments
		// this still covers the local spill/temp areas under the same root.
		LocalStorageRoot: cfg.Storage.LocalPath,
		// Compaction temp directory (cfg.Compaction.TempDirectory) — every
		// compaction job COPYs rewritten parquet to a subdir of this path
		// before uploading. Must be in the sandbox allowlist or every
		// compaction job fails post-lockdown.
		CompactionTempDirectory: cfg.Compaction.TempDirectory,
		// Query optimization
		EnableS3Cache:     cfg.Query.EnableS3Cache,
		S3CacheSize:       cfg.Query.S3CacheSize,
		S3CacheTTLSeconds: cfg.Query.S3CacheTTLSeconds,
		// arcx loader (cleared above when license does not permit)
		ArcxExtensionPath: arcxPath,
		// arcx storage root for the partition_agg table function. Only set
		// when the loader is enabled; the DB layer ignores it otherwise.
		ArcxStorageRoot: func() string {
			if arcxPath == "" {
				return ""
			}
			return cfg.Storage.LocalPath
		}(),
	}

	// resolveAbsPath converts an operator-supplied path (possibly relative,
	// possibly Windows-slashed) into an absolute forward-slash form suitable
	// for safe interpolation into the DuckDB sandbox allowlist. Rejects paths
	// containing control bytes — they should not appear in real config and
	// would corrupt the allowlist SQL even after escapeSQLString quote-doubling
	// (newlines are SQL-significant; nulls truncate the literal in DuckDB's
	// parser). Empty input passes through unchanged so callers can treat
	// "unset" as a sentinel.
	//
	// Matters for systemd units with WorkingDirectory=/ and docker entrypoints
	// rooted at /, where relative paths resolve differently than during
	// operator-facing local runs. Also keeps the value DuckDB stores
	// internally byte-identical to what CleanupOrphanedSpillFiles walks.
	resolveAbsPath := func(name, p string) string {
		if p == "" {
			return p
		}
		// Reject any non-printable character or Unicode formatting / line/
		// paragraph separator. Real filesystem paths never contain these;
		// their presence in operator config indicates either a typo (newline
		// at end of YAML scalar, BOM at start of file) or a paste from a
		// hostile source. The categories caught:
		//   Cc — ASCII control (\0, \n, \r, \t, etc.)
		//   Cf — format chars (LRM/RLM/LRE/RLE/PDF/LRO/RLO/LRI/RLI/FSI/PDI,
		//        ZWSP/ZWNJ/ZWJ, BOM/ZWNBSP, soft hyphen — invisible runes
		//        that can make a path look one way in logs and another in
		//        the SQL literal sent to DuckDB).
		//   Zl — line separator (U+2028)
		//   Zp — paragraph separator (U+2029)
		// unicode.IsControl covers Cc; unicode.In with the others closes
		// the bidi / invisible-character bypass surface gemini will look
		// for. Reject loudly rather than try to interpret these.
		for _, r := range p {
			if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
				log.Fatal().Str("setting", name).Str("path", p).Msg("Configured path contains control, formatting, or line/paragraph-separator characters; refusing to start")
			}
		}
		// filepath.Abs can only fail when os.Getwd fails (e.g. the CWD was
		// unlinked between exec and now). Fail-fast — a silent relative-path
		// fallback would land in the sandbox allowlist as a relative literal
		// that never matches the absolute paths Arc emits at query time, and
		// the failure mode is invisible (every query 500s, no startup log).
		abs, err := filepath.Abs(p)
		if err != nil {
			log.Fatal().Err(err).Str("setting", name).Str("path", p).Msg("Failed to resolve path to absolute; refusing to start")
		}
		return filepath.ToSlash(abs)
	}

	// Normalize every operator-supplied path that will be interpolated into
	// the DuckDB sandbox allowlist OR consumed by DuckDB directly. The
	// sandbox does prefix-match on literals — relative paths in the allowlist
	// never match the absolute paths Arc emits at query time, so every path
	// MUST be absolute before lockdown.
	//
	// If the operator explicitly cleared database.temp_directory, fall back
	// to a known relative path BEFORE Abs-resolving. Otherwise DuckDB's own
	// default (".tmp") would be relative and never match the absolute spill
	// paths inside the sandbox allowlist, breaking every query that spills.
	// Matches the config-load default at internal/config/config.go:757.
	if dbConfig.TempDirectory == "" {
		dbConfig.TempDirectory = "./.tmp"
	}
	dbConfig.TempDirectory = resolveAbsPath("database.temp_directory", dbConfig.TempDirectory)
	dbConfig.LocalStorageRoot = resolveAbsPath("storage.local_path", dbConfig.LocalStorageRoot)
	dbConfig.CompactionTempDirectory = resolveAbsPath("compaction.temp_directory", dbConfig.CompactionTempDirectory)
	if dbConfig.ArcxStorageRoot != "" {
		dbConfig.ArcxStorageRoot = resolveAbsPath("arcx.storage_root", dbConfig.ArcxStorageRoot)
	}
	// arcx.extension_path is interpolated into a `LOAD '<path>'` statement;
	// normalise it the same way every other operator-supplied path is
	// (control-char rejection + Abs + ToSlash) so the LOAD is robust against
	// CWD changes and so the path cannot smuggle SQL through escapeSQLString.
	if dbConfig.ArcxExtensionPath != "" {
		dbConfig.ArcxExtensionPath = resolveAbsPath("database.arcx_extension_path", dbConfig.ArcxExtensionPath)
	}

	// Refuse obviously-wrong storage roots that would neuter the sandbox
	// (allowing every local file). Operator owns the config so this is
	// protection against a typo (e.g. "/" instead of "/data") rather than a
	// malicious config. Covers POSIX system roots, the OS temp tree (sharing
	// /tmp with other processes is never what an operator wants for Arc
	// data), and common shared-state roots. Windows roots like C:\ are not
	// enumerated — Windows-on-server-with-Arc is an unusual deployment.
	//
	// Apply to ALL local-directory configurations that end up in the sandbox
	// allowlist. TempDirectory and CompactionTempDirectory are also added
	// verbatim to allowed_directories, so a typo there would grant the same
	// kind of broad access as a misconfigured LocalStorageRoot.
	//
	// Prefix-match (not exact-match) so a configured path like
	// "/etc/arc-data" is rejected too — its allowlist entry would be
	// "/etc/arc-data/" which is still inside /etc and would let any reader
	// drop a file under /etc/arc-data to be exfiltrated through Arc. Same
	// reasoning for /root/.ssh, /proc/<pid>/, /sys/class/, etc.
	deniedRoots := []string{
		"/etc", "/usr", "/bin", "/sbin", "/boot",
		"/proc", "/sys", "/dev",
		"/root",
	}
	// pathStartsWithRoot returns true when `path` is exactly `root`, is
	// `root` with a trailing slash, or has `root + "/"` as a prefix.
	// Anchored so "/etcd-data" is NOT matched by "/etc" — only true
	// subdirectories or the bare directory itself.
	pathStartsWithRoot := func(path, root string) bool {
		return path == root || path == root+"/" || strings.HasPrefix(path, root+"/")
	}
	for _, pair := range []struct {
		name, value string
	}{
		{"storage.local_path", dbConfig.LocalStorageRoot},
		{"database.temp_directory", dbConfig.TempDirectory},
		{"compaction.temp_directory", dbConfig.CompactionTempDirectory},
	} {
		if pair.value == "" {
			continue
		}
		// Reject the root filesystem outright — never legitimate.
		if pair.value == "/" {
			log.Fatal().Str("setting", pair.name).Str("path", pair.value).Msg("Configured path refuses to be the filesystem root; pick a dedicated data directory")
		}
		for _, root := range deniedRoots {
			if pathStartsWithRoot(pair.value, root) {
				log.Fatal().Str("setting", pair.name).Str("path", pair.value).Str("denied_root", root).Msg("Configured path is inside a system root; pick a dedicated data directory")
			}
		}
	}

	// Resolve symlinks on every local directory that lands in the sandbox
	// allowlist. filepath.Abs does NOT resolve symlinks; the kernel will,
	// so without EvalSymlinks the sandbox literal-string can mismatch the
	// real path the kernel opens (most common cause: macOS /var → /private/var,
	// Docker bind mounts, K8s subPath). Same Warn-and-substitute policy as
	// the upload-dir handling below — never hard-Fatal on a symlinked
	// ancestor; instead use the resolved path so the allowlist and the
	// kernel agree. EvalSymlinks errors only on missing paths, which is a
	// real misconfiguration we should fail on.
	resolveLocalDirSymlinks := func(name string, p *string) {
		if *p == "" {
			return
		}
		resolved, err := filepath.EvalSymlinks(*p)
		if err != nil {
			log.Fatal().Err(err).Str("setting", name).Str("path", *p).Msg("Failed to resolve configured path symlinks; refusing to start")
		}
		resolved = filepath.ToSlash(resolved)
		if resolved != *p {
			log.Warn().
				Str("setting", name).
				Str("original", *p).
				Str("resolved", resolved).
				Msg("Configured path resolves through a symlink; using the resolved path as the sandbox allowlist entry")
			*p = resolved
		}
	}
	// TempDirectory and CompactionTempDirectory must exist on disk before
	// EvalSymlinks is called — config-load defaults them to "./.tmp" and
	// "./data/compaction" respectively, neither of which exists at first
	// boot. Create them with 0o700 first so the symlink-resolution check
	// has something to resolve.
	if err := os.MkdirAll(dbConfig.TempDirectory, 0o700); err != nil {
		log.Fatal().Err(err).Str("path", dbConfig.TempDirectory).Msg("Failed to create database.temp_directory")
	}
	if dbConfig.CompactionTempDirectory != "" {
		if err := os.MkdirAll(dbConfig.CompactionTempDirectory, 0o700); err != nil {
			log.Fatal().Err(err).Str("path", dbConfig.CompactionTempDirectory).Msg("Failed to create compaction.temp_directory")
		}
	}
	if dbConfig.LocalStorageRoot != "" {
		if err := os.MkdirAll(dbConfig.LocalStorageRoot, 0o700); err != nil {
			log.Fatal().Err(err).Str("path", dbConfig.LocalStorageRoot).Msg("Failed to create storage.local_path")
		}
	}
	resolveLocalDirSymlinks("storage.local_path", &dbConfig.LocalStorageRoot)
	resolveLocalDirSymlinks("database.temp_directory", &dbConfig.TempDirectory)
	resolveLocalDirSymlinks("compaction.temp_directory", &dbConfig.CompactionTempDirectory)

	// Production safety net: if every path contributing to the sandbox
	// allowlist is empty, DuckDB will refuse every file-touching query.
	// internal/database.lockdownExternalAccess logs a Warn in that case
	// (it's library code that test fixtures and embeddings also call), but
	// for the production binary an empty allowlist is unrecoverable
	// misconfiguration — fail-fast at startup rather than serving 500s on
	// every query. main.go's "./.tmp" fallback for TempDirectory makes this
	// branch effectively unreachable today; this guard is here to catch a
	// future refactor that drops the fallback.
	if dbConfig.LocalStorageRoot == "" &&
		dbConfig.TempDirectory == "" &&
		dbConfig.CompactionTempDirectory == "" &&
		dbConfig.S3Bucket == "" &&
		dbConfig.ColdS3Bucket == "" &&
		dbConfig.AzureContainer == "" &&
		dbConfig.ColdAzureContainer == "" {
		log.Fatal().Msg("sandbox allowlist would be empty — every file-touching query would fail; check arc.toml [storage] and [database] config")
	}

	// Compute and create the import-upload directory. Lives under the
	// operator-configured TempDirectory (always non-empty by this point —
	// see the "./.tmp" fallback above). The DB sandbox whitelists exactly
	// this directory in allowed_directories so reads of uploaded files
	// succeed; nothing else under os.TempDir is reachable from user SQL.
	uploadDir := filepath.Join(dbConfig.TempDirectory, uploadSubdirName)
	if err := os.MkdirAll(uploadDir, 0o700); err != nil {
		log.Fatal().Err(err).Str("path", uploadDir).Msg("Failed to create import upload directory")
	}
	// Lstat BEFORE Chmod. os.Chmod follows symlinks (no portable Lchmod on
	// Linux), so a chmod-first ordering would silently change the perms of
	// any attacker-staged symlink target before the Lstat check fires. With
	// Lstat first, we abort startup the instant we see a symlink and the
	// target's perms remain untouched.
	if info, err := os.Lstat(uploadDir); err != nil {
		log.Fatal().Err(err).Str("path", uploadDir).Msg("Failed to stat import upload directory")
	} else if info.Mode()&os.ModeSymlink != 0 {
		log.Fatal().Str("path", uploadDir).Msg("Import upload directory is a symlink; refusing to start (security)")
	}
	// Defense in depth against an ancestor-of-uploadDir symlink (e.g.
	// dbConfig.TempDirectory itself is a symlink — filepath.Abs does not
	// resolve symlinks). If EvalSymlinks resolves to a different path, the
	// sandbox would otherwise allowlist the literal pre-resolution string
	// while the kernel actually opens files at the resolved location —
	// reads from the literal allowlisted path would fail, and writes via
	// the resolved path would land outside the allowlisted prefix.
	//
	// Hard-rejecting on any symlinked ancestor would break legitimate
	// deployments (macOS routes /var through /private/var; Docker bind
	// mounts and K8s subPath frequently traverse symlinks). Instead: log a
	// Warn so operators see the resolution happened, and use the resolved
	// path as the sandbox allowlist entry. The kernel and the allowlist
	// then agree on the same underlying directory, closing the spoofing
	// window without false-positiving common production environments.
	if resolved, err := filepath.EvalSymlinks(uploadDir); err != nil {
		log.Fatal().Err(err).Str("path", uploadDir).Msg("Failed to resolve import upload directory symlinks")
	} else if resolved != uploadDir {
		log.Warn().
			Str("original", uploadDir).
			Str("resolved", resolved).
			Msg("Import upload directory resolves through a symlink; using the resolved path as the sandbox allowlist entry")
		uploadDir = resolved
	}
	// Chmod after Lstat+EvalSymlinks — at this instant the path is a real
	// directory whose every ancestor resolves to itself. A same-host
	// attacker who can write to the parent directory still has a TOCTOU
	// window between EvalSymlinks and Chmod; that's a known constraint of
	// POSIX file APIs without O_PATH+fchmod, and an attacker with that
	// level of access has already won. MkdirAll silently accepts an
	// existing directory with looser permissions, so chmod explicitly to
	// enforce 0o700 across restarts. On Windows this is a no-op for the
	// perm bits but harmless.
	if err := os.Chmod(uploadDir, 0o700); err != nil {
		log.Fatal().Err(err).Str("path", uploadDir).Msg("Failed to chmod import upload directory to 0700")
	}
	dbConfig.UploadDir = filepath.ToSlash(uploadDir)

	// Sweep orphaned DuckDB spill files from a previous run (kill -9,
	// OOM-kill, crash). DuckDB unlinks these on graceful close, but
	// otherwise they survive and accumulate. Runs BEFORE database.New so
	// we never race with our own DuckDB process. Files younger than
	// spillFileLiveThreshold are skipped to protect any concurrent arc;
	// the durable invariant is "no two arc instances share a
	// temp_directory" — document this in the operator config.
	if err := database.CleanupOrphanedSpillFiles(dbConfig.TempDirectory, logger.Get("database")); err != nil {
		log.Warn().Err(err).Msg("Failed to sweep orphaned DuckDB spill files; continuing")
	}

	db, err := database.New(dbConfig, logger.Get("database"))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize database")
	}
	shutdownCoordinator.Register("database", db, shutdown.PriorityDatabase)

	// Test query
	log.Info().Msg("Testing DuckDB connection...")
	rows, err := db.Query("SELECT 'Hello from Arc in Go!' as message, version() as duckdb_version")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to execute test query")
	}
	defer rows.Close()

	var message, version string
	if rows.Next() {
		if err := rows.Scan(&message, &version); err != nil {
			log.Fatal().Err(err).Msg("Failed to scan result")
		}
		log.Info().
			Str("message", message).
			Str("duckdb_version", version).
			Msg("Test query successful")
	}

	// Print database stats
	stats := db.Stats()
	log.Info().
		Int("max_open", stats.MaxOpenConnections).
		Int("open", stats.OpenConnections).
		Int("in_use", stats.InUse).
		Int("idle", stats.Idle).
		Msg("Database connection pool stats")

	// Initialize storage backend
	var storageBackend storage.Backend
	switch cfg.Storage.Backend {
	case "local":
		storageBackend, err = storage.NewLocalBackend(cfg.Storage.LocalPath, logger.Get("storage"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize local storage backend")
		}
		shutdownCoordinator.Register("storage", storageBackend, shutdown.PriorityStorage)
		log.Info().
			Str("backend", "local").
			Str("path", cfg.Storage.LocalPath).
			Msg("Storage backend initialized")

	case "s3", "minio":
		// Note: an empty s3_bucket for an S3 primary backend is rejected earlier
		// in config.Load (before database.New builds the DuckDB secret), so by
		// here the bucket is guaranteed non-empty.
		s3Config := &storage.S3Config{
			Bucket:    cfg.Storage.S3Bucket,
			Region:    cfg.Storage.S3Region,
			Endpoint:  cfg.Storage.S3Endpoint,
			AccessKey: cfg.Storage.S3AccessKey,
			SecretKey: cfg.Storage.S3SecretKey,
			UseSSL:    cfg.Storage.S3UseSSL,
			PathStyle: cfg.Storage.S3PathStyle,
			Prefix:    cfg.Storage.S3Prefix,
		}
		storageBackend, err = storage.NewS3Backend(s3Config, logger.Get("storage"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize S3 storage backend")
		}
		shutdownCoordinator.Register("storage", storageBackend, shutdown.PriorityStorage)
		log.Info().
			Str("backend", cfg.Storage.Backend).
			Str("bucket", cfg.Storage.S3Bucket).
			Str("prefix", cfg.Storage.S3Prefix).
			Str("region", cfg.Storage.S3Region).
			Str("endpoint", cfg.Storage.S3Endpoint).
			Msg("Storage backend initialized")

	case "azure", "azblob":
		azureConfig := &storage.AzureBlobConfig{
			ConnectionString:   cfg.Storage.AzureConnectionString,
			AccountName:        cfg.Storage.AzureAccountName,
			AccountKey:         cfg.Storage.AzureAccountKey,
			SASToken:           cfg.Storage.AzureSASToken,
			ContainerName:      cfg.Storage.AzureContainer,
			Endpoint:           cfg.Storage.AzureEndpoint,
			UseManagedIdentity: cfg.Storage.AzureUseManagedIdentity,
		}
		storageBackend, err = storage.NewAzureBlobBackend(azureConfig, logger.Get("storage"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize Azure Blob Storage backend")
		}
		shutdownCoordinator.Register("storage", storageBackend, shutdown.PriorityStorage)
		log.Info().
			Str("backend", cfg.Storage.Backend).
			Str("container", cfg.Storage.AzureContainer).
			Str("account", cfg.Storage.AzureAccountName).
			Msg("Storage backend initialized")

	default:
		log.Fatal().Str("backend", cfg.Storage.Backend).Msg("Unsupported storage backend (use 'local', 's3', 'minio', 'azure', or 'azblob')")
	}

	// Pattern 2 shared-storage multi-writer mode startup validation.
	// Refuses to start under three conditions that would silently break
	// the multi-writer invariant:
	//
	//   (a) cluster.enabled=false — without the cluster coordinator,
	//       schedulers have a nil ClusterGate (see scheduler wiring in
	//       cmd/arc/main.go) and singleton tasks (retention, CQ, delete)
	//       run unconditionally. Two such "standalone" nodes pointed at
	//       the same bucket would each run retention against the shared
	//       data — duplicate deletes, duplicate writes. SharedStorageMode
	//       is meaningless without clustering.
	//   (b) cfg.Storage.Backend == "local" — per-node filesystems can't
	//       be shared across N writers. Writes would diverge silently.
	//   (c) license lacks the shared_storage_multi_writer feature — this
	//       is an Enterprise-tier capability that must be explicitly
	//       licensed; running without the gate would be a license bypass.
	//
	// Order matters: cluster.enabled before backend before license, so
	// the most upstream misconfig surfaces first.
	if cfg.Cluster.SharedStorageMode {
		if !cfg.Cluster.Enabled {
			log.Fatal().
				Msg("cluster.shared_storage_mode=true requires cluster.enabled=true; without the cluster coordinator there is no leader-election gate and singleton background tasks (retention, CQ, delete) would run on every node")
		}
		if cfg.Storage.Backend == "local" {
			log.Fatal().
				Str("backend", cfg.Storage.Backend).
				Msg("cluster.shared_storage_mode=true requires an object-store backend (s3, minio, azure, or azblob); local-filesystem backend cannot be shared across writers")
		}
		if licenseClient == nil || !licenseClient.CanUseSharedStorageMultiWriter() {
			log.Fatal().
				Msg("cluster.shared_storage_mode=true requires an Enterprise license with the shared_storage_multi_writer feature; contact sales@basekick.net")
		}
		log.Info().
			Str("backend", cfg.Storage.Backend).
			Msg("Pattern 2 shared-storage multi-writer mode enabled (singleton tasks gate on Raft leader; writer failover suppressed)")
	}

	// Initialize WAL writer (if enabled) - recovery happens after ArrowBuffer is ready
	var walWriter *wal.Writer
	var walRecovery *wal.Recovery
	if cfg.WAL.Enabled {
		var err error
		walWriter, err = wal.NewWriter(&wal.WriterConfig{
			WALDir:       cfg.WAL.Directory,
			SyncMode:     wal.SyncMode(cfg.WAL.SyncMode),
			MaxSizeBytes: int64(cfg.WAL.MaxSizeMB) * 1024 * 1024,
			MaxAge:       time.Duration(cfg.WAL.MaxAgeSeconds) * time.Second,
			BufferSize:   cfg.WAL.BufferSize,
			Logger:       logger.Get("wal"),
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize WAL writer")
		}
		shutdownCoordinator.Register("wal", walWriter, shutdown.PriorityWAL)

		// Prepare recovery (will run after ArrowBuffer is created)
		walRecovery = wal.NewRecovery(cfg.WAL.Directory, logger.Get("wal"))

		log.Info().
			Str("directory", cfg.WAL.Directory).
			Str("sync_mode", cfg.WAL.SyncMode).
			Int("max_size_mb", cfg.WAL.MaxSizeMB).
			Int("max_age_seconds", cfg.WAL.MaxAgeSeconds).
			Msg("WAL enabled")
	} else {
		log.Info().Msg("WAL is DISABLED - data durability relies on immediate Parquet flushes")
	}

	// Initialize Arrow buffer (optionally with WAL)
	log.Info().
		Int("flush_workers", cfg.Ingest.FlushWorkers).
		Int("shard_count", cfg.Ingest.ShardCount).
		Int("flush_queue_size", cfg.Ingest.FlushQueueSize).
		Msg("Initializing Arrow buffer with ingestion config")
	arrowBuffer := ingest.NewArrowBuffer(&cfg.Ingest, storageBackend, logger.Get("arrow"))
	if walWriter != nil {
		arrowBuffer.SetWAL(walWriter)
	}
	shutdownCoordinator.Register("arrow-buffer", arrowBuffer, shutdown.PriorityBuffer)

	// After ArrowBuffer flushes (priority 30) but before WAL closes (priority 40),
	// purge WAL files since all data has been flushed to storage.
	// This prevents recovery from replaying already-persisted data on next startup.
	if walWriter != nil {
		shutdownCoordinator.RegisterHook("wal-purge", func(ctx context.Context) error {
			deleted, err := walWriter.PurgeAll()
			if err != nil {
				log.Error().Err(err).Msg("Failed to purge WAL files on shutdown")
				return err
			}
			if deleted > 0 {
				log.Info().Int("deleted", deleted).Msg("Purged WAL files after clean buffer flush")
			}
			return nil
		}, 35) // Between PriorityBuffer(30) and PriorityWAL(40)
	}

	// Run WAL recovery NOW that ArrowBuffer is ready
	if walRecovery != nil {
		// Create shared recovery callback to avoid code duplication
		recoveryCallback := createWALRecoveryCallback(arrowBuffer, logger.Get("wal-recovery"))
		columnarCallback := createColumnarRecoveryCallback(arrowBuffer, logger.Get("wal-recovery"))

		recoveryStats, err := walRecovery.RecoverWithOptions(context.Background(), recoveryCallback, &wal.RecoveryOptions{
			BatchSize:        cfg.WAL.RecoveryBatchSize,
			ColumnarCallback: columnarCallback,
		})
		if err != nil {
			log.Error().Err(err).Msg("WAL recovery failed")
		} else if recoveryStats.RecoveredFiles > 0 {
			// Track recovery metrics
			metrics.Get().IncWALRecoveryTotal()
			metrics.Get().IncWALRecoveryRecords(int64(recoveryStats.RecoveredEntries))
			log.Info().
				Int("files", recoveryStats.RecoveredFiles).
				Int("batches", recoveryStats.RecoveredBatches).
				Int("entries", recoveryStats.RecoveredEntries).
				Int("corrupted", recoveryStats.CorruptedEntries).
				Dur("duration", recoveryStats.RecoveryDuration).
				Msg("WAL recovery complete")
		}

		// Start periodic WAL maintenance goroutine.
		// Two modes:
		//   Normal:   purge rotated WAL files older than safeAge (data already in parquet)
		//   Recovery: when a flush failure is detected (S3 outage), replay WAL files
		//             to re-buffer data that was cleared from buffers after failed flush
		walMaintenanceCtx, walMaintenanceCancel := context.WithCancel(context.Background())
		shutdownCoordinator.RegisterHook("wal-periodic-maintenance", func(ctx context.Context) error {
			walMaintenanceCancel()
			return nil
		}, shutdown.PriorityBuffer)

		// Safe age threshold: after this duration, a rotated WAL file's data MUST have
		// been flushed to parquet by the normal buffer flush cycle (MaxBufferAgeMS).
		// We use 3x margin to account for flush worker delays and clock skew.
		safeAge := time.Duration(cfg.Ingest.MaxBufferAgeMS) * time.Millisecond * 3
		if safeAge < 30*time.Second {
			safeAge = 30 * time.Second
		}

		recoveryInterval := time.Duration(cfg.WAL.RecoveryIntervalSeconds) * time.Second
		go func() {
			ticker := time.NewTicker(recoveryInterval)
			defer ticker.Stop()
			walLogger := logger.Get("wal-maintenance")

			for {
				select {
				case <-walMaintenanceCtx.Done():
					return
				case <-ticker.C:
					if arrowBuffer.HasFlushFailure() {
						// Storage failure detected — replay WAL files to recover data
						// that was cleared from buffers after failed flush
						walLogger.Info().Msg("Flush failure detected, attempting WAL recovery")

						// Purge old WAL files first (same as normal path) to avoid replaying
						// data that was already successfully flushed to parquet before the failure.
						if walWriter != nil {
							deleted, purgeErr := walWriter.PurgeOlderThan(safeAge)
							if purgeErr != nil {
								walLogger.Error().Err(purgeErr).Msg("WAL purge before recovery failed")
							} else if deleted > 0 {
								walLogger.Info().Int("deleted", deleted).Msg("Purged old WAL files before recovery")
							}
						}

						recovery := wal.NewRecovery(cfg.WAL.Directory, walLogger)
						activeFile := ""
						if walWriter != nil {
							activeFile = walWriter.CurrentFile()
						}
						stats, err := recovery.RecoverWithOptions(context.Background(), recoveryCallback, &wal.RecoveryOptions{
							SkipActiveFile:   activeFile,
							BatchSize:        cfg.WAL.RecoveryBatchSize,
							ColumnarCallback: columnarCallback,
						})
						if err != nil {
							walLogger.Error().Err(err).Msg("WAL recovery after flush failure failed")
						} else {
							if stats.RecoveredFiles > 0 {
								metrics.Get().IncWALRecoveryTotal()
								metrics.Get().IncWALRecoveryRecords(int64(stats.RecoveredEntries))
								walLogger.Info().
									Int("files", stats.RecoveredFiles).
									Int("entries", stats.RecoveredEntries).
									Msg("WAL recovery after flush failure complete")
							}
							arrowBuffer.ResetFlushFailure()
						}
					} else {
						// Normal operation — purge WAL files old enough that their data
						// has been flushed to parquet by the normal buffer flush cycle
						deleted, err := walWriter.PurgeOlderThan(safeAge)
						if err != nil {
							walLogger.Error().Err(err).Msg("Periodic WAL purge failed")
						} else if deleted > 0 {
							walLogger.Info().Int("deleted", deleted).Msg("Periodic WAL cleanup complete")
						}
					}
				}
			}
		}()
		log.Info().
			Dur("interval", recoveryInterval).
			Dur("safe_age", safeAge).
			Msg("Periodic WAL maintenance enabled")
	}

	// Initialize MQTT Subscription Manager (if enabled)
	var mqttManager mqtt.Manager
	if cfg.MQTT.Enabled {
		// Get encryption key from environment (required for subscriptions with passwords)
		encryptionKey, keyErr := mqtt.GetEncryptionKey()
		if keyErr != nil {
			log.Fatal().Err(keyErr).Msg("Invalid ARC_ENCRYPTION_KEY - must be 32 bytes (base64 or hex encoded)")
		}
		if encryptionKey == nil {
			log.Warn().Msg("ARC_ENCRYPTION_KEY not set - MQTT subscriptions with passwords will be rejected")
		}

		// Create password encryptor (will be NilEncryptor if no key - rejects passwords)
		encryptor, err := mqtt.NewPasswordEncryptor(encryptionKey)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create password encryptor")
		}

		// Create SQLite repository for MQTT subscriptions (use shared DB path from auth config)
		mqttDBPath := cfg.Auth.DBPath
		if mqttDBPath == "" {
			mqttDBPath = "./data/arc.db" // Use shared SQLite database
		}
		mqttRepo, err := mqtt.NewSQLiteRepository(mqttDBPath, encryptionKey, logger.Get("mqtt-repo"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize MQTT repository")
		}

		// Create subscription manager
		mqttManager = mqtt.NewSubscriptionManager(mqttRepo, encryptor, arrowBuffer, logger.Get("mqtt"))

		// Start manager (loads and starts auto_start subscriptions)
		if err := mqttManager.Start(context.Background()); err != nil {
			log.Warn().Err(err).Msg("Some MQTT subscriptions failed to auto-start")
		}

		// Register shutdown hook
		shutdownCoordinator.RegisterHook("mqtt-manager", func(ctx context.Context) error {
			return mqttManager.Shutdown(ctx)
		}, shutdown.PriorityIngest)

		log.Info().Bool("encryption_enabled", encryptionKey != nil).Msg("MQTT subscription manager enabled")
	} else {
		log.Debug().Msg("MQTT subscription manager is disabled")
	}

	// Initialize AuthManager (if enabled)
	var authManager *auth.AuthManager
	// Phase A: assigned inside the auth-enabled branch below; invoked
	// either immediately (OSS / non-clustered) or after the Raft proposer
	// is wired (cluster mode). Function scope so the cluster wire-up branch
	// can reach it. `bootstrapRan` tracks whether the closure has been
	// invoked, so the cluster-init-failure fallback later in main() can
	// detect "deferred but never ran" without double-banners on the
	// happy path.
	var runInitialTokenBootstrap func()
	var bootstrapRan bool
	if cfg.Auth.Enabled {
		authManager, err = auth.NewAuthManager(
			cfg.Auth.DBPath,
			time.Duration(cfg.Auth.CacheTTL)*time.Second,
			cfg.Auth.MaxCacheSize,
			logger.Get("auth"),
		)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize auth manager")
		}
		shutdownCoordinator.Register("auth", authManager, shutdown.PriorityAuth)

		// Restrict SQLite database file permissions (contains auth tokens, audit logs, etc.)
		if err := os.Chmod(cfg.Auth.DBPath, 0600); err != nil {
			log.Warn().Err(err).Str("path", cfg.Auth.DBPath).Msg("Failed to set database file permissions")
		}

		// Create initial admin token on first run.
		// ARC_AUTH_BOOTSTRAP_TOKEN: use a known token value instead of generating a random one.
		// ARC_AUTH_FORCE_BOOTSTRAP: add a recovery admin token without removing existing tokens (recovery path).
		//
		// Phase A (Cluster Auth Convergence): when the node is going to enter
		// cluster mode AND has a working Enterprise license that includes
		// clustering, defer the bootstrap until AFTER SetRaftProposer has
		// flipped the AuthManager onto the Raft path. Otherwise every node
		// independently creates its own admin token in its own SQLite (the
		// original OSS behaviour) and four banners print instead of one.
		runInitialTokenBootstrap = func() {
			bootstrapRan = true
			var bootstrapToken string
			var bootstrapErr error
			// Bound the bootstrap retry loop. 30s matches the upstream
			// WaitForLeader ceiling and leaves headroom for the inner
			// retry's worst case: 4 attempts × proposeTimeout (5s) +
			// exp backoff (250+500+1000ms ≈ 1.75s) ≈ 22s. If the cluster
			// genuinely never elects a leader the timeout cancels the
			// loop cleanly and we surface the error rather than blocking
			// startup indefinitely. Internal review #2 (post-Gemini round
			// 3) flagged the previous 10s ceiling as too tight against
			// proposeTimeout=5s.
			bootstrapCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if cfg.Auth.ForceBootstrap && cfg.Auth.BootstrapToken != "" {
				bootstrapToken, bootstrapErr = authManager.ForceAddRecoveryToken(bootstrapCtx, cfg.Auth.BootstrapToken)
			} else if cfg.Auth.BootstrapToken != "" {
				bootstrapToken, bootstrapErr = authManager.EnsureInitialTokenWithValue(bootstrapCtx, cfg.Auth.BootstrapToken)
			} else {
				bootstrapToken, bootstrapErr = authManager.EnsureInitialToken(bootstrapCtx)
			}
			if bootstrapErr != nil {
				log.Error().Err(bootstrapErr).Msg("Failed to create initial admin token")
			} else if bootstrapToken != "" {
				// Print colorized banner to stderr (bypasses structured logging)
				const (
					cyan   = "\033[96m"
					yellow = "\033[93m"
					bold   = "\033[1m"
					reset  = "\033[0m"
				)
				banner := cyan + "======================================================================" + reset
				fmt.Fprintln(os.Stderr)
				fmt.Fprintln(os.Stderr, banner)
				if cfg.Auth.ForceBootstrap {
					fmt.Fprintln(os.Stderr, cyan+bold+"  RECOVERY TOKEN ADDED - EXISTING TOKENS PRESERVED"+reset)
				} else {
					fmt.Fprintln(os.Stderr, cyan+bold+"  FIRST RUN - INITIAL ADMIN TOKEN GENERATED"+reset)
				}
				fmt.Fprintln(os.Stderr, banner)
				fmt.Fprintln(os.Stderr, yellow+bold+"  Admin API token: "+bootstrapToken+reset)
				fmt.Fprintln(os.Stderr, banner)
				fmt.Fprintln(os.Stderr, cyan+"  SAVE THIS TOKEN! It will not be shown again."+reset)
				fmt.Fprintln(os.Stderr, cyan+"  Use this token to login to the web UI or API."+reset)
				if cfg.Auth.ForceBootstrap {
					fmt.Fprintln(os.Stderr, cyan+"  Use the API to revoke any tokens you no longer need."+reset)
					fmt.Fprintln(os.Stderr, cyan+"  Remove ARC_AUTH_FORCE_BOOTSTRAP after recovery."+reset)
				} else {
					fmt.Fprintln(os.Stderr, cyan+"  You can create additional tokens after logging in."+reset)
				}
				fmt.Fprintln(os.Stderr, banner)
				fmt.Fprintln(os.Stderr)
			}
		}

		// Decide whether bootstrap can run inline or must wait for the Raft
		// proposer to be wired. We check the same preconditions the cluster
		// init block at line 1157 checks, so a "yes" here is guaranteed to
		// reach the proposer-wiring branch below.
		willEnterClusterMode := cfg.Cluster.Enabled &&
			licenseClient != nil &&
			licenseClient.GetLicense() != nil &&
			licenseClient.GetLicense().HasFeature(license.FeatureClustering)
		if !willEnterClusterMode {
			runInitialTokenBootstrap()
		} else {
			log.Info().Msg("Deferring initial token bootstrap until cluster Raft proposer is wired (Phase A)")
		}

		log.Info().
			Str("db_path", cfg.Auth.DBPath).
			Int("cache_ttl", cfg.Auth.CacheTTL).
			Int("max_cache_size", cfg.Auth.MaxCacheSize).
			Msg("Authentication enabled")
	} else {
		log.Warn().Msg("Authentication is DISABLED - all endpoints are public")
	}

	// Initialize Compaction (if enabled)
	var hourlyScheduler *compaction.Scheduler
	var dailyScheduler *compaction.Scheduler
	var compactionManager *compaction.Manager
	// Phase 4: build the compaction role gate from config. When clustering
	// is enabled, only nodes with RoleCompactor actually run compaction —
	// the gate is consulted at Scheduler.Start() and TriggerNow(). OSS and
	// standalone deployments pass a nil gate (compactionGate below) and
	// skip the role check entirely, preserving pre-Phase-4 behavior.
	var compactionGate compaction.ClusterGate
	if cfg.Cluster.Enabled {
		compactionGate = newCompactionClusterGate(cfg.Cluster.Role)
	}
	// Phase 4: completion-manifest directory used by the cluster-mode
	// subprocess → parent handoff. Empty in OSS (nil gate is what disables
	// the handoff inside job.go via clusterMode()). Populated only when
	// clustering + peer replication are BOTH enabled — that's when readers
	// actually need the Raft manifest update to pull the compacted file.
	//
	// Security: explicitly create the base TempDirectory with 0700 perms
	// BEFORE the inner .completion/pending subdirs. os.MkdirAll would
	// otherwise create the base dir with umask'd perms (typically 0755),
	// letting a co-located attacker inject completion manifests before the
	// inner dir's 0700 closes the window. This is a no-op when the dir
	// already exists with correct perms.
	completionDir := ""
	if cfg.Cluster.Enabled && cfg.Cluster.ReplicationEnabled {
		if err := os.MkdirAll(cfg.Compaction.TempDirectory, 0o700); err != nil {
			log.Fatal().Err(err).Str("dir", cfg.Compaction.TempDirectory).Msg("Failed to create compaction temp directory with 0700 perms")
		}
		// Honor an explicit compaction.completion_dir override if set;
		// otherwise derive under {temp_directory}/.completion/pending.
		if cfg.Compaction.CompletionDir != "" {
			completionDir = cfg.Compaction.CompletionDir
		} else {
			completionDir = filepath.Join(cfg.Compaction.TempDirectory, ".completion", "pending")
		}
	}
	if cfg.Compaction.Enabled {
		// Build tiers
		var tiers []compaction.Tier

		if cfg.Compaction.HourlyEnabled {
			hourlyTier := compaction.NewHourlyTier(&compaction.HourlyTierConfig{
				StorageBackend: storageBackend,
				MinAgeHours:    cfg.Compaction.HourlyMinAgeHours,
				MinFiles:       cfg.Compaction.HourlyMinFiles,
				Enabled:        true,
				Logger:         logger.Get("compaction"),
			})
			tiers = append(tiers, hourlyTier)
		}

		if cfg.Compaction.DailyEnabled {
			dailyTier := compaction.NewDailyTier(&compaction.DailyTierConfig{
				StorageBackend:       storageBackend,
				MinAgeHours:          cfg.Compaction.DailyMinAgeHours,
				MinFiles:             cfg.Compaction.DailyMinFiles,
				SkipFileAgeCheckDays: cfg.Compaction.DailySkipFileAgeCheckDays,
				Enabled:              true,
				Logger:               logger.Get("compaction"),
			})
			tiers = append(tiers, dailyTier)
		}

		// Create lock manager
		lockManager := compaction.NewLockManager()

		// Parse sort keys from ingest config for compaction
		// Compaction needs to maintain the same sort order as ingested files
		sortKeysConfig, defaultSortKeys, err := config.ParseSortKeys(cfg.Ingest)
		if err != nil {
			log.Warn().Err(err).Msg("Invalid sort keys config for compaction, using defaults")
			sortKeysConfig = make(map[string][]string)
			defaultSortKeys = []string{"time"}
		}

		// Create compaction manager (discovers all databases dynamically)
		// Compaction jobs run in subprocesses for memory isolation
		compactionManager = compaction.NewManager(&compaction.ManagerConfig{
			StorageBackend: storageBackend,
			LockManager:    lockManager,
			MaxConcurrent:  cfg.Compaction.MaxConcurrent,
			MemoryLimit:    cfg.Database.MemoryLimit, // Use same limit as main DuckDB
			CompletionDir:  completionDir,            // Phase 4: empty in OSS, set in cluster mode
			// Pass the SAME absolute-resolved value the DB-layer sandbox
			// allowlist references — dbConfig.CompactionTempDirectory has
			// already been through resolveAbsPath. Using the raw
			// cfg.Compaction.TempDirectory here would leave the manager
			// holding a relative path while the sandbox sees the absolute
			// resolution; the parent-side orphan-cleanup walker would then
			// look at a different filesystem location than the allowlist.
			TempDirectory:   dbConfig.CompactionTempDirectory,
			SortKeysConfig:  sortKeysConfig,
			DefaultSortKeys: defaultSortKeys,
			Tiers:           tiers,
			Logger:          logger.Get("compaction"),
		})

		// Cleanup orphaned temp directories from previous runs (e.g., pod crashes).
		// CleanupOrphanedTempDirs skips the Phase 4 reserved ".completion"
		// subdirectory so a crash that left pending completion manifests on
		// disk doesn't silently lose them on restart.
		if err := compactionManager.CleanupOrphanedTempDirs(); err != nil {
			log.Warn().Err(err).Msg("Failed to cleanup orphaned compaction temp directories")
		}
		// Phase 4: sweep completion manifests that are stuck in writing_output
		// from a subprocess that crashed mid-upload. Manifests in later
		// states (output_written, sources_deleted) are left alone so the
		// watcher can still process them once it starts.
		if completionDir != "" {
			orphanTimeout := time.Duration(cfg.Compaction.CompletionOrphanTimeoutMS) * time.Millisecond
			if err := compactionManager.CleanupOrphanedCompletionManifests(orphanTimeout); err != nil {
				log.Warn().Err(err).Msg("Failed to cleanup orphaned completion manifests")
			}
		}

		// Create hourly scheduler (if hourly tier is enabled)
		if cfg.Compaction.HourlyEnabled {
			hourlyScheduler, err = compaction.NewScheduler(&compaction.SchedulerConfig{
				Manager:     compactionManager,
				Schedule:    cfg.Compaction.HourlySchedule,
				TierNames:   []string{"hourly"},
				Enabled:     true,
				ClusterGate: compactionGate, // Phase 4: nil in OSS, role-check in cluster mode
				Logger:      logger.Get("compaction-hourly"),
			})
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create hourly compaction scheduler")
			}

			if err := hourlyScheduler.Start(); err != nil {
				log.Fatal().Err(err).Msg("Failed to start hourly compaction scheduler")
			}

			shutdownCoordinator.RegisterHook("hourly-compaction-scheduler", func(ctx context.Context) error {
				hourlyScheduler.Stop()
				return nil
			}, shutdown.PriorityCompaction)

			log.Info().
				Str("schedule", cfg.Compaction.HourlySchedule).
				Msg("Hourly compaction scheduler started")
		}

		// Create daily scheduler (if daily tier is enabled)
		if cfg.Compaction.DailyEnabled {
			dailyScheduler, err = compaction.NewScheduler(&compaction.SchedulerConfig{
				Manager:     compactionManager,
				Schedule:    cfg.Compaction.DailySchedule,
				TierNames:   []string{"daily"},
				Enabled:     true,
				ClusterGate: compactionGate, // Phase 4: nil in OSS, role-check in cluster mode
				Logger:      logger.Get("compaction-daily"),
			})
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create daily compaction scheduler")
			}

			if err := dailyScheduler.Start(); err != nil {
				log.Fatal().Err(err).Msg("Failed to start daily compaction scheduler")
			}

			shutdownCoordinator.RegisterHook("daily-compaction-scheduler", func(ctx context.Context) error {
				dailyScheduler.Stop()
				return nil
			}, shutdown.PriorityCompaction)

			log.Info().
				Str("schedule", cfg.Compaction.DailySchedule).
				Msg("Daily compaction scheduler started")
		}

		log.Info().
			Bool("hourly_enabled", cfg.Compaction.HourlyEnabled).
			Str("hourly_schedule", cfg.Compaction.HourlySchedule).
			Bool("daily_enabled", cfg.Compaction.DailyEnabled).
			Str("daily_schedule", cfg.Compaction.DailySchedule).
			Int("max_concurrent", cfg.Compaction.MaxConcurrent).
			Msg("Compaction enabled")
	} else {
		log.Info().Msg("Compaction is DISABLED")
	}

	// Initialize Telemetry (if enabled)
	var telemetryCollector *telemetry.Collector
	if cfg.Telemetry.Enabled {
		telemetryCfg := &telemetry.Config{
			Enabled:  true,
			Endpoint: cfg.Telemetry.Endpoint,
			Interval: time.Duration(cfg.Telemetry.IntervalSeconds) * time.Second,
			DataDir:  "./data",
		}
		telemetryCollector, err = telemetry.New(telemetryCfg, Version, logger.Get("telemetry"))
		if err != nil {
			log.Warn().Err(err).Msg("Failed to initialize telemetry (continuing without it)")
		} else {
			telemetryCollector.Start()
			shutdownCoordinator.RegisterHook("telemetry", func(ctx context.Context) error {
				telemetryCollector.Stop()
				return nil
			}, shutdown.PriorityTelemetry)

			log.Info().
				Str("instance_id", telemetryCollector.GetInstanceID()).
				Dur("interval", time.Duration(cfg.Telemetry.IntervalSeconds)*time.Second).
				Msg("Telemetry enabled")
		}
	} else {
		log.Info().Msg("Telemetry is DISABLED (opt-out via ARC_TELEMETRY_ENABLED=false)")
	}

	// Start periodic license validation (if license was validated earlier)
	if licenseClient != nil {
		licenseClient.StartPeriodicValidation(license.ValidationInterval)
		shutdownCoordinator.RegisterHook("license-client", func(ctx context.Context) error {
			licenseClient.Stop()
			return nil
		}, shutdown.PriorityTelemetry)
	}

	// Initialize Cluster Coordinator (Enterprise feature)
	// Clustering enables role-based node separation: writer, reader, compactor
	var clusterCoordinator *cluster.Coordinator
	if cfg.Cluster.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Clustering requires enterprise license - running in standalone mode")
		} else {
			lic := licenseClient.GetLicense()
			if lic == nil || !lic.HasFeature(license.FeatureClustering) {
				log.Warn().Msg("License does not include clustering feature - running in standalone mode")
			} else {
				// Determine API address for this node. Use api.ListenAddr
				// so IPv6 literals are bracketed correctly (the previous
				// fmt.Sprintf("%s:%d", …) produced "::1:8000" which is
				// not a valid address). The 0.0.0.0 special-case keeps
				// the historical advertise-address shape (":<port>") for
				// operators who explicitly bound the IPv4 wildcard.
				apiAddr := fmt.Sprintf(":%d", cfg.Server.Port)
				if cfg.Server.Host != "" && cfg.Server.Host != "0.0.0.0" {
					_, apiAddr = api.ListenAddr(cfg.Server.Host, cfg.Server.Port)
				}

				// Clustering requires shared-secret auth on the coordinator
				// protocol. Without it, the coordinator validates no HMAC on
				// join/heartbeat/leave messages (the checks are gated on a
				// non-empty secret), so any host that can reach the coordinator
				// port could join as a trusted node. Fail closed rather than
				// run an unauthenticated cluster (GHSA-p378-jp5r-gpgw). This
				// guard is only reachable by a clustering-licensed instance
				// (the standalone-fallback branches above handle the unlicensed
				// case), so it never affects single-node deployments.
				if cfg.Cluster.SharedSecret == "" {
					log.Error().Msg("cluster.enabled requires ARC_CLUSTER_SHARED_SECRET to be set (cluster join and heartbeat messages must be authenticated)")
					log.Error().Msg("Set ARC_CLUSTER_SHARED_SECRET to a value shared by all cluster nodes (generate with: openssl rand -hex 32), or disable cluster.enabled to continue")
					os.Exit(1)
				}
				// Note: this guard subsumes the previous replication-only check
				// (cluster.replication_enabled with an empty secret) — an empty
				// secret now fails closed regardless of which cluster features
				// are enabled.

				var err error
				clusterCoordinator, err = cluster.NewCoordinator(&cluster.CoordinatorConfig{
					Config:        &cfg.Cluster,
					LicenseClient: licenseClient,
					Version:       Version,
					APIAddress:    apiAddr,
					Logger:        logger.Get("cluster"),
					// Mirror the Fiber listener's TLS posture so the
					// internal request Router uses https:// when peers
					// serve TLS. All cluster nodes are expected to be
					// configured identically.
					ServerTLSEnabled: cfg.Server.TLSEnabled,
					// Phase 4: surface the "no compactor elected" and
					// "multiple compactors elected" warnings only when
					// this deployment actually needs a compactor (cluster
					// + peer replication + compaction all enabled).
					WarnIfNoCompactor: cfg.Cluster.ReplicationEnabled && cfg.Compaction.Enabled,
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to initialize cluster coordinator - running in standalone mode")
				} else {
					// Peer replication Phase 2 needs the storage backend handle so the
					// fetch handler can serve local bytes and the puller can write
					// received bytes. Must be set before Start — the puller is
					// constructed inside Start when ReplicationEnabled is true.
					clusterCoordinator.SetStorageBackend(storageBackend)

					if err := clusterCoordinator.Start(); err != nil {
						log.Error().Err(err).Msg("Failed to start cluster coordinator - running in standalone mode")
						clusterCoordinator = nil
					} else {
						shutdownCoordinator.RegisterHook("cluster-coordinator", func(ctx context.Context) error {
							return clusterCoordinator.Stop()
						}, shutdown.PriorityCompaction) // Stop before compaction

						localNode := clusterCoordinator.GetLocalNode()
						capabilities := localNode.GetCapabilities()
						log.Info().
							Str("node_id", localNode.ID).
							Str("role", string(localNode.Role)).
							Str("cluster", cfg.Cluster.ClusterName).
							Bool("can_ingest", capabilities.CanIngest).
							Bool("can_query", capabilities.CanQuery).
							Bool("can_compact", capabilities.CanCompact).
							Msg("Cluster coordinator started")

						// Phase A: Cluster Auth Convergence — wire the AuthManager
						// into the cluster's Raft FSM so that every CreateToken /
						// RevokeToken / etc. propagates cluster-wide instead of
						// staying in this node's local SQLite.
						//
						// Two halves:
						//   1. SetRaftProposer flips AuthManager's write methods
						//      from direct-SQLite to Raft-propose. From this point
						//      forward every API-driven token mutation goes through
						//      the FSM apply path on every node.
						//   2. SetAuthCallbacks gives the FSM the per-node
						//      materialise hooks so each node's local SQLite
						//      mirrors the cluster-authoritative state. The
						//      callbacks fire on the runFSM goroutine after the
						//      in-memory tokens map has been mutated.
						//
						// Order matters: install callbacks BEFORE flipping the
						// proposer, so the very first cluster-wide CreateToken
						// (typically the bootstrap admin token on the next
						// EnsureInitialToken call) has its callback wired and
						// materialises into SQLite on this node.
						if authManager != nil {
							if fsm := clusterCoordinator.GetRaftFSM(); fsm != nil {
								fsm.SetAuthCallbacks(
									func(e *clusterraft.TokenEntry) {
										if err := authManager.ApplyCreateToken(cluster.ToAuthTokenEntry(e)); err != nil {
											log.Error().Err(err).Int64("token_id", e.ID).Msg("Failed to materialise CreateToken into local SQLite")
										}
									},
									func(e *clusterraft.TokenEntry) {
										if err := authManager.ApplyUpdateToken(cluster.ToAuthTokenEntry(e)); err != nil {
											log.Error().Err(err).Int64("token_id", e.ID).Msg("Failed to materialise UpdateToken into local SQLite")
										}
									},
									func(id int64) {
										if err := authManager.ApplyRevokeToken(id); err != nil {
											log.Error().Err(err).Int64("token_id", id).Msg("Failed to materialise RevokeToken into local SQLite")
										}
									},
									func(id int64) {
										if err := authManager.ApplyDeleteToken(id); err != nil {
											log.Error().Err(err).Int64("token_id", id).Msg("Failed to materialise DeleteToken into local SQLite")
										}
									},
									func(id int64, newHash, newPrefix string, lsn uint64) {
										if err := authManager.ApplyRotateToken(id, newHash, newPrefix); err != nil {
											log.Error().Err(err).Int64("token_id", id).Msg("Failed to materialise RotateToken into local SQLite")
										}
									},
								)
								proposer := cluster.NewCoordinatorAuthProposer(clusterCoordinator)
								if proposer != nil {
									authManager.SetRaftProposer(proposer)
									log.Info().Msg("Cluster auth state replication enabled — token writes now propagate via Raft")

									// Phase A: NOW run the deferred bootstrap.
									// The proposer is wired, the FSM has its
									// auth callbacks, and a forwardApplyToLeader
									// path exists for follower proposals.
									// Every node calls EnsureInitialToken with
									// its own randomly-generated value; only
									// the Raft leader's proposal lands, the
									// rest get "token name already exists" and
									// silently return empty-string (no banner).
									//
									// First wait for a Raft leader to be
									// observed. On followers this prevents
									// forwardApplyToLeader from racing the
									// election window and returning
									// ErrNoLeaderKnown — that error would
									// surface as a non-idempotent bootstrap
									// failure even though the cluster will
									// elect a leader within ~5s.
									if err := clusterCoordinator.WaitForLeader(30 * time.Second); err != nil {
										log.Warn().Err(err).Msg("Cluster auth bootstrap: leader not observed within 30s; proceeding (call will likely retry via Raft semantics)")
									}
									if runInitialTokenBootstrap != nil {
										runInitialTokenBootstrap()
									}
								}
							}
						}

						// Wire up WAL replication if enabled
						if cfg.Cluster.ReplicationEnabled && walWriter != nil {
							clusterCoordinator.SetWAL(walWriter)
							clusterCoordinator.SetIngestBuffer(arrowBuffer)
							if err := clusterCoordinator.StartReplication(); err != nil {
								log.Warn().Err(err).Msg("Failed to start WAL replication")
							} else {
								log.Info().
									Bool("is_writer", capabilities.CanIngest).
									Msg("WAL replication started")
							}
						}

						// Wire up peer file replication (Enterprise Phase 1 + Phase 2).
						//
						// Phase 1 — the registrar is non-blocking: it enqueues file
						// registrations from the flush path and a background worker
						// appends them to the Raft manifest.
						//
						// Phase 2 — the puller is constructed inside coordinator.Start()
						// when ReplicationEnabled is true. It watches the FSM for new
						// file registrations and pulls the bytes from the origin peer
						// over the coordinator TCP protocol. It's owned by the
						// coordinator and stopped from coordinator.Stop(), so it has
						// no separate shutdown hook here.
						//
						// OSS deployments never reach this block (no coordinator).
						//
						// Shutdown ordering (lower priority runs first, see shutdown.go):
						//   HTTPServer (10)  — stop accepting client requests
						//   Ingest     (20)  — drain ingest/flush
						//   Buffer     (30)  — file-registrar drains queue into Raft
						//   Compaction (50)  — cluster-coordinator stops:
						//                        puller.Stop()  (first)
						//                        raftNode.Stop()  (second)
						//
						// This sequence ensures final file announcements land in Raft
						// while Raft is still alive, and pending peer pulls are
						// cancelled promptly when Raft is about to go away.
						fileRegistrar := cluster.NewCoordinatorFileRegistrar(clusterCoordinator, logger.Get("file-registrar"))
						fileRegistrar.Start(context.Background())
						arrowBuffer.SetFileRegistrar(fileRegistrar)
						shutdownCoordinator.RegisterHook("file-registrar", func(ctx context.Context) error {
							fileRegistrar.Stop()
							return nil
						}, shutdown.PriorityBuffer)
						log.Info().Msg("Cluster file manifest registrar enabled")

						// Phase 5: wire the dynamic compaction gate to the coordinator
						// so CanCompact() checks the FSM's active compactor lease
						// when failover is configured.
						if compactionGate != nil {
							if gate, ok := compactionGate.(*compactionClusterGate); ok {
								gate.setCoordinator(clusterCoordinator)
							}
						}

						// Phase 4+5: start the compaction completion watcher on
						// nodes that can potentially compact. With Phase 5 failover,
						// any node may become the active compactor at runtime, so we
						// construct the watcher and schedulers on every node but only
						// start them when the node holds the compactor lease.
						//
						// Shutdown ordering: watcher stops at PriorityCompaction - 1
						// so it drains any pending manifests BEFORE the coordinator
						// tears down Raft.
						if completionDir != "" && cfg.Compaction.Enabled {
							bridge := cluster.NewCompactionBridge(clusterCoordinator)
							pollInterval := time.Duration(cfg.Compaction.CompletionWatcherIntervalMS) * time.Millisecond
							if pollInterval <= 0 {
								pollInterval = 1 * time.Second
							}
							watcher, werr := compaction.NewCompletionWatcher(compaction.CompletionWatcherConfig{
								Dir:          completionDir,
								Bridge:       bridge,
								PollInterval: pollInterval,
								ApplyTimeout: 5 * time.Second,
								Logger:       logger.Get("compaction-watcher"),
							})
							if werr != nil {
								log.Warn().Err(werr).Msg("Failed to construct compaction completion watcher")
							} else {
								// If this node is already the active compactor (static
								// RoleCompactor with no failover, or failover already
								// assigned us), start immediately. Otherwise the FSM
								// callback will start it dynamically.
								if capabilities.CanCompact || clusterCoordinator.IsActiveCompactor() {
									watcher.Start(context.Background())
									log.Info().
										Str("completion_dir", completionDir).
										Dur("poll_interval", pollInterval).
										Msg("Phase 4 compaction completion watcher started")
								}

								shutdownCoordinator.RegisterHook("compaction-completion-watcher", func(ctx context.Context) error {
									watcher.Stop()
									return nil
								}, shutdown.PriorityCompaction-1)

								// Phase 5: wire OnBecomeCompactor / OnLoseCompactor
								// callbacks so the scheduler and watcher activate/deactivate
								// dynamically when the compactor lease moves between nodes.
								// Dedicated compactor nodes (CanCompact=true) keep the watcher
								// running regardless of lease state to process orphaned manifests.
								isDedicatedCompactor := capabilities.CanCompact
								clusterCoordinator.SetCompactorCallbacks(
									func() {
										// OnBecomeCompactor: start scheduler + watcher
										log.Info().Msg("Phase 5: this node became the active compactor — starting compaction")
										if hourlyScheduler != nil {
											if err := hourlyScheduler.Start(); err != nil {
												log.Error().Err(err).Msg("Failed to start hourly scheduler after failover")
											}
										}
										if dailyScheduler != nil {
											if err := dailyScheduler.Start(); err != nil {
												log.Error().Err(err).Msg("Failed to start daily scheduler after failover")
											}
										}
										if !isDedicatedCompactor {
											watcher.Start(context.Background())
										}
									},
									func() {
										// OnLoseCompactor: stop scheduler + watcher
										log.Info().Msg("Phase 5: this node lost the active compactor lease — stopping compaction")
										if hourlyScheduler != nil {
											hourlyScheduler.Stop()
										}
										if dailyScheduler != nil {
											dailyScheduler.Stop()
										}
										if !isDedicatedCompactor {
											watcher.Stop()
										}
									},
								)
							}
						}
					}
				}
			}
		}
	}

	// Phase A: fallback bootstrap. If we deferred the initial-token
	// bootstrap above on the expectation that cluster mode would wire
	// the Raft proposer, but the proposer never landed (cluster
	// coordinator failed to start; coordinator started but raftFSM is
	// nil because RaftDataDir was empty; SetRaftProposer branch was
	// otherwise skipped), the operator would have no admin token.
	// Run it now in OSS-fallthrough mode so the system stays usable.
	// The closure itself sets bootstrapRan, and ensureFirstToken's
	// underlying SQLite path is idempotent — re-running on subsequent
	// boots is a no-op.
	if authManager != nil && runInitialTokenBootstrap != nil && !bootstrapRan {
		log.Warn().Msg("Cluster auth proposer was never wired (e.g. cluster init failed, or RaftDataDir empty); running initial token bootstrap in standalone fallback mode")
		runInitialTokenBootstrap()
	}

	// Determine node capabilities (for role-based component initialization)
	nodeRole := cluster.RoleStandalone
	if clusterCoordinator != nil {
		nodeRole = clusterCoordinator.GetRole()
	}
	nodeCapabilities := nodeRole.GetCapabilities()

	// Log node role and capabilities (useful for debugging cluster deployments)
	if nodeRole != cluster.RoleStandalone {
		log.Info().
			Str("role", string(nodeRole)).
			Bool("can_ingest", nodeCapabilities.CanIngest).
			Bool("can_query", nodeCapabilities.CanQuery).
			Bool("can_compact", nodeCapabilities.CanCompact).
			Msg("Node running with cluster role")
	}

	// Auto-sync: ensure HTTP write timeout can accommodate query timeout
	if cfg.Query.Timeout > 0 && cfg.Server.WriteTimeout < cfg.Query.Timeout {
		log.Warn().
			Int("write_timeout", cfg.Server.WriteTimeout).
			Int("query_timeout", cfg.Query.Timeout).
			Msg("HTTP write_timeout is less than query timeout - adjusting to match")
		cfg.Server.WriteTimeout = cfg.Query.Timeout
	}

	// Initialize HTTP server
	serverConfig := &api.ServerConfig{
		Host:            cfg.Server.Host,
		Port:            cfg.Server.Port,
		ReadTimeout:     time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout:    time.Duration(cfg.Server.WriteTimeout) * time.Second,
		IdleTimeout:     time.Duration(cfg.Server.IdleTimeout) * time.Second,
		ShutdownTimeout: time.Duration(cfg.Server.ShutdownTimeout) * time.Second,
		MaxPayloadSize:  cfg.Server.MaxPayloadSize,
		TLSEnabled:      cfg.Server.TLSEnabled,
		TLSCertFile:     cfg.Server.TLSCertFile,
		TLSKeyFile:      cfg.Server.TLSKeyFile,
	}

	server := api.NewServer(serverConfig, logger.Get("server"))

	// Register base routes
	server.RegisterRoutes()

	// Apply auth middleware if enabled
	var rbacManager *auth.RBACManager
	if authManager != nil {
		middlewareConfig := auth.DefaultMiddlewareConfig()
		middlewareConfig.AuthManager = authManager
		// Add public routes that don't need auth
		// Note: /api/v1/internal/cache/invalidate is public because cluster peers
		// call it without an auth token after compaction. Access is gated by
		// HMAC-SHA256 validation in the handler (see handleCacheInvalidate); the
		// receiver refuses every request when cluster.shared_secret is empty.
		middlewareConfig.PublicRoutes = append(middlewareConfig.PublicRoutes, "/health", "/ready", "/api/v1/auth/verify", api.CacheInvalidatePath)
		// /metrics stays public — Prometheus scrapers expect it. It is
		// already in auth.DefaultMiddlewareConfig().PublicPrefixes, so no
		// further append is needed here. /debug/pprof is intentionally NOT
		// here: pprof is no longer mounted on the public Fiber app
		// (internal/api/server.go). The opt-in localhost pprof listener
		// runs on a separate port; see startDebugPprofIfEnabled.
		//
		// The JSON metrics endpoints (/api/v1/metrics and its sub-paths) are
		// deliberately public, equivalent to the existing Prometheus /metrics
		// surface: aggregate operational counters plus Go runtime/GC stats and
		// host-arch fingerprinting (go_version/os/arch). They expose no queries,
		// database/measurement names, file paths, or credentials, and monitoring
		// dashboards poll them without a token. Whitelist them explicitly so
		// their public status is an auditable decision, not an accident of
		// registration order (they were previously reachable only because
		// RegisterRoutes runs before this middleware; see GHSA-m3qr-fvp4-78xj).
		// /api/v1/logs, by contrast, is NOT public — it is registered with admin
		// auth below.
		middlewareConfig.PublicPrefixes = append(middlewareConfig.PublicPrefixes, "/api/v1/metrics")
		server.GetApp().Use(auth.NewMiddleware(middlewareConfig))

		// Register the admin-authenticated logs route AFTER the auth
		// middleware so it sits later in the Fiber stack and is actually
		// gated (GHSA-m3qr-fvp4-78xj).
		server.RegisterLogsRoute(authManager)

		// Initialize RBAC Manager (Enterprise feature)
		rbacManager = auth.NewRBACManager(&auth.RBACManagerConfig{
			DB:            authManager.GetDB(),
			LicenseClient: licenseClient,
			Logger:        logger.Get("rbac"),
			// Phase A.2 Item 2: cascade-on-delete soft cap.
			// Default is 50000 (set by viper); operators can override
			// via cluster.rbac.max_cascade_descendants in arc.toml or
			// ARC_CLUSTER_RBAC_MAX_CASCADE_DESCENDANTS env var. 0 = disabled.
			MaxCascadeDescendants: cfg.Cluster.RBACMaxCascadeDescendants,
		})
		shutdownCoordinator.Register("rbac", rbacManager, shutdown.PriorityAuth)
		if rbacManager.IsRBACEnabled() {
			log.Info().Msg("Enterprise RBAC enabled")
		}

		// Register auth routes
		authHandler := api.NewAuthHandler(authManager, logger.Get("auth"))
		authHandler.SetRBACManager(rbacManager)
		authHandler.RegisterRoutes(server.GetApp())
		authHandler.RegisterTokenMembershipRoutes(server.GetApp())

		// Register RBAC routes (Enterprise feature)
		rbacHandler := api.NewRBACHandler(authManager, rbacManager, logger.Get("rbac"))
		rbacHandler.RegisterRoutes(server.GetApp())

		// Phase A.1: Cluster Auth Convergence (RBAC). Wire the RBACManager
		// into the cluster FSM so every node materialises Raft-replicated
		// RBAC state into local SQLite. Mirrors the Phase A token wire-up
		// at the cluster initialisation block above, but lives here
		// because rbacManager is constructed after the cluster block.
		//
		// Order matters (mirrors Phase A):
		//   1. SetRBACCallbacks gives the FSM the per-node materialise
		//      hooks BEFORE flipping the proposer, so the first
		//      cluster-wide CreateOrganization has its callback wired
		//      and lands in SQLite on this node.
		//   2. SetRaftProposer flips RBACManager's write methods from
		//      direct-SQLite to Raft-propose.
		//   3. SeedRBACFromLocalSQLite runs leader-only, proposing
		//      Create<X> for every pre-existing RBAC row so post-upgrade
		//      clusters have their state replicated to followers.
		if clusterCoordinator != nil && rbacManager != nil {
			if fsm := clusterCoordinator.GetRaftFSM(); fsm != nil {
				fsm.SetRBACCallbacks(
					func(e *clusterraft.OrganizationEntry) {
						if err := rbacManager.ApplyCreateOrganization(cluster.ToAuthOrganizationEntry(e)); err != nil {
							log.Error().Err(err).Int64("organization_id", e.ID).Msg("Failed to materialise CreateOrganization into local SQLite")
						}
					},
					func(e *clusterraft.OrganizationEntry) {
						if err := rbacManager.ApplyUpdateOrganization(cluster.ToAuthOrganizationEntry(e)); err != nil {
							log.Error().Err(err).Int64("organization_id", e.ID).Msg("Failed to materialise UpdateOrganization into local SQLite")
						}
					},
					func(id int64) {
						if err := rbacManager.ApplyDeleteOrganization(id); err != nil {
							log.Error().Err(err).Int64("organization_id", id).Msg("Failed to materialise DeleteOrganization into local SQLite")
						}
					},
					func(e *clusterraft.TeamEntry) {
						if err := rbacManager.ApplyCreateTeam(cluster.ToAuthTeamEntry(e)); err != nil {
							log.Error().Err(err).Int64("team_id", e.ID).Msg("Failed to materialise CreateTeam into local SQLite")
						}
					},
					func(e *clusterraft.TeamEntry) {
						if err := rbacManager.ApplyUpdateTeam(cluster.ToAuthTeamEntry(e)); err != nil {
							log.Error().Err(err).Int64("team_id", e.ID).Msg("Failed to materialise UpdateTeam into local SQLite")
						}
					},
					func(id int64) {
						if err := rbacManager.ApplyDeleteTeam(id); err != nil {
							log.Error().Err(err).Int64("team_id", id).Msg("Failed to materialise DeleteTeam into local SQLite")
						}
					},
					func(e *clusterraft.RoleEntry) {
						if err := rbacManager.ApplyCreateRole(cluster.ToAuthRoleEntry(e)); err != nil {
							log.Error().Err(err).Int64("role_id", e.ID).Msg("Failed to materialise CreateRole into local SQLite")
						}
					},
					func(e *clusterraft.RoleEntry) {
						if err := rbacManager.ApplyUpdateRole(cluster.ToAuthRoleEntry(e)); err != nil {
							log.Error().Err(err).Int64("role_id", e.ID).Msg("Failed to materialise UpdateRole into local SQLite")
						}
					},
					func(id int64) {
						if err := rbacManager.ApplyDeleteRole(id); err != nil {
							log.Error().Err(err).Int64("role_id", id).Msg("Failed to materialise DeleteRole into local SQLite")
						}
					},
					func(e *clusterraft.MeasurementPermissionEntry) {
						if err := rbacManager.ApplyCreateMeasurementPermission(cluster.ToAuthMeasurementPermissionEntry(e)); err != nil {
							log.Error().Err(err).Int64("measurement_permission_id", e.ID).Msg("Failed to materialise CreateMeasurementPermission into local SQLite")
						}
					},
					func(id int64) {
						if err := rbacManager.ApplyDeleteMeasurementPermission(id); err != nil {
							log.Error().Err(err).Int64("measurement_permission_id", id).Msg("Failed to materialise DeleteMeasurementPermission into local SQLite")
						}
					},
					func(e *clusterraft.TokenMembershipEntry) {
						if err := rbacManager.ApplyAddTokenToTeam(cluster.ToAuthTokenMembershipEntry(e)); err != nil {
							log.Error().Err(err).Int64("membership_id", e.ID).Msg("Failed to materialise AddTokenToTeam into local SQLite")
						}
					},
					func(tokenID, teamID int64) {
						if err := rbacManager.ApplyRemoveTokenFromTeam(tokenID, teamID); err != nil {
							log.Error().Err(err).Int64("token_id", tokenID).Int64("team_id", teamID).Msg("Failed to materialise RemoveTokenFromTeam into local SQLite")
						}
					},
				)
				rbacProposer := cluster.NewCoordinatorAuthProposer(clusterCoordinator)
				if rbacProposer != nil {
					rbacManager.SetRaftProposer(rbacProposer)
					log.Info().Msg("Cluster RBAC replication enabled — RBAC writes now propagate via Raft")

					// Phase A.1: run the upgrade-seed AFTER the proposer
					// is wired and AFTER the leader is observed. Idempotent
					// — followers skip via IsLeader(). Under a 30s ceiling
					// to keep startup bounded.
					//
					// Run in a background goroutine so the HTTP server can
					// start listening immediately instead of blocking up to
					// 30s on the WaitForLeader call. On a cold start or
					// rolling upgrade the leader may take seconds to elect;
					// blocking startup would cause k8s liveness / readiness
					// probes to time out and the container to restart-loop.
					// The seed is leader-only and idempotent on re-run, so
					// completing it asynchronously is safe — followers
					// don't reach the seed body at all (IsLeader check),
					// and a re-elected leader will pick it up on its own
					// startup. Gemini PR #458 round 7 G23.
					//
					// Wire shutdown signal into the goroutine via a
					// cancellable seedCtx that a shutdown hook fires. If
					// the app receives SIGTERM during startup (operator
					// kills a restart-looping container, k8s rolls a
					// pod), we cancel mid-seed instead of fighting the
					// database close. The cancel is also called in defer
					// for the success path so the parent ctx never
					// leaks. Gemini PR #458 round 9 G30.
					seedCtx, seedCancel := context.WithCancel(context.Background())
					// Fire before everything else (priority < PriorityHTTPServer=10)
					// so the seed goroutine bails as soon as shutdown begins,
					// freeing the RBACManager connections + the cluster
					// coordinator's WaitForLeader before they get torn down
					// in the higher-priority hooks.
					shutdownCoordinator.RegisterHook("rbac-seed-cancel", func(ctx context.Context) error {
						seedCancel()
						return nil
					}, 5)
					go func() {
						defer seedCancel()
						if err := clusterCoordinator.WaitForLeader(30 * time.Second); err != nil {
							log.Warn().Err(err).Msg("Cluster RBAC seed: leader not observed within 30s; skipping (will retry on next restart)")
							return
						}
						// Bound the seed under a 30s timeout AND the
						// app-shutdown cancellation; whichever fires first
						// wins. context.WithTimeout chains off seedCtx so
						// either source of cancellation propagates.
						timedCtx, timedCancel := context.WithTimeout(seedCtx, 30*time.Second)
						defer timedCancel()
						if seedErr := rbacManager.SeedRBACFromLocalSQLite(timedCtx); seedErr != nil {
							log.Warn().Err(seedErr).Msg("Cluster RBAC seed: partial failure (cluster is still operable; missing rows can be re-issued by an operator)")
						}
					}()
				}
			}
		}
	} else {
		// Authentication disabled: register the logs route unguarded so the
		// endpoint still works for no-auth deployments. (When auth is enabled
		// it is registered with admin auth inside the block above, after the
		// middleware — GHSA-m3qr-fvp4-78xj.)
		server.RegisterLogsRoute(nil)
	}

	// Initialize Audit Logging (Enterprise feature - requires valid license)
	// Must be registered before API routes so the middleware captures all requests
	var auditLogger *audit.Logger
	if cfg.AuditLog.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Audit logging requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseAuditLogging() {
			log.Warn().Msg("License does not include audit_logging feature - feature disabled")
		} else {
			auditDB, err := sql.Open("sqlite3", cfg.Auth.DBPath)
			if err != nil {
				log.Error().Err(err).Msg("Failed to open audit database - feature disabled")
			} else {
				auditLogger, err = audit.NewLogger(&audit.LoggerConfig{
					DB:     auditDB,
					Config: &cfg.AuditLog,
					Logger: logger.Get("audit"),
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to create audit logger - feature disabled")
				} else {
					auditLogger.Start()
					shutdownCoordinator.RegisterHook("audit", func(ctx context.Context) error {
						auditLogger.Stop()
						return nil
					}, shutdown.PriorityCompaction)

					// Register audit middleware BEFORE API routes
					server.GetApp().Use(audit.Middleware(auditLogger, cfg.AuditLog.IncludeReads))

					log.Info().
						Int("retention_days", cfg.AuditLog.RetentionDays).
						Bool("include_reads", cfg.AuditLog.IncludeReads).
						Msg("Audit logging enabled")
				}
			}
		}
	}

	// Register MessagePack handler with Arrow buffer
	msgpackHandler := api.NewMsgPackHandler(logger.Get("msgpack"), arrowBuffer, server.GetMaxPayloadSize())
	if authManager != nil && rbacManager != nil {
		msgpackHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	msgpackHandler.RegisterRoutes(server.GetApp())

	// Register Line Protocol handler
	lineProtocolHandler := api.NewLineProtocolHandler(arrowBuffer, logger.Get("lineprotocol"))
	if authManager != nil && rbacManager != nil {
		lineProtocolHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	lineProtocolHandler.RegisterRoutes(server.GetApp())

	// Register TLE handler (streaming TLE ingestion)
	tleHandler := api.NewTLEHandler(arrowBuffer, logger.Get("tle"))
	if authManager != nil && rbacManager != nil {
		tleHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	tleHandler.RegisterRoutes(server.GetApp())

	// Register Import handler (CSV, Parquet, Line Protocol, TLE bulk import).
	// All formats parse in-process and ingest through the ArrowBuffer pipeline;
	// no import path issues DuckDB queries against the uploaded file, so the
	// handler no longer needs the sandbox-allowlisted upload directory.
	// (dbConfig.UploadDir is still allowlisted for delete.go's S3 COPY ... TO
	// staging — see NewDeleteHandler below.)
	importHandler := api.NewImportHandler(logger.Get("import"))
	importHandler.SetArrowBuffer(arrowBuffer)
	if authManager != nil && rbacManager != nil {
		importHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	importHandler.RegisterRoutes(server.GetApp())

	// Register Query handler with dedicated query timeout and slow query logging
	queryHandler := api.NewQueryHandler(db, storageBackend, logger.Get("query"), cfg.Query.Timeout, cfg.Query.SlowQueryThresholdMs)
	if authManager != nil && rbacManager != nil {
		queryHandler.SetAuthAndRBAC(authManager, rbacManager)
	}
	queryHandler.RegisterRoutes(server.GetApp())
	// Start the handler's background workers (currently the partition
	// pruner cache janitor — sweeps expired globCache / partitionCache
	// entries so they don't accumulate over the process lifetime).
	// Matches the WAL maintenance pattern at line 730: ad-hoc cancel
	// context registered with the shutdown coordinator. Runs in the
	// HTTPServer priority band (the earliest tier) because the janitor
	// has nothing to flush — it just owns a ticker and an in-memory
	// map. Stopping it early frees the goroutine without blocking any
	// downstream shutdown hook.
	queryWorkersCtx, queryWorkersCancel := context.WithCancel(context.Background())
	shutdownCoordinator.RegisterHook("query-handler-workers", func(_ context.Context) error {
		queryWorkersCancel()
		return nil
	}, shutdown.PriorityHTTPServer)
	queryHandler.StartBackgroundWorkers(queryWorkersCtx)

	// Wire up cluster router to handlers for request forwarding
	// This enables reader nodes to forward writes to writers, and
	// compactor nodes to forward queries to readers/writers
	if clusterCoordinator != nil {
		router := clusterCoordinator.GetRouter()
		if router != nil {
			msgpackHandler.SetRouter(router)
			lineProtocolHandler.SetRouter(router)
			tleHandler.SetRouter(router)
			queryHandler.SetRouter(router)
			log.Info().Msg("Cluster router wired to API handlers for request forwarding")
		}

		// Wire the catch-up gate (#392). When cfg.Cluster.QueryGateOnCatchup is
		// true, user-facing read endpoints will 503 until the catch-up walker's
		// batch has fully settled. The flag is off by default; the SetCluster
		// call is always made so the handler has a coordinator reference for
		// any future cluster-aware behavior even when the gate is off.
		//
		// Validate the gate × catch-up-walker combination at startup. If the
		// walker is disabled (replication_catchup_enabled=false, the emergency
		// off-switch for pathologically large manifests), the gate has nothing
		// to wait for and would 503 forever. Auto-disable the gate with a WARN
		// rather than panicking — the operator's intent is clear: they don't
		// want catch-up bootstrapping, so they implicitly don't want a gate
		// that depends on it.
		gateEnabled := cfg.Cluster.QueryGateOnCatchup
		if gateEnabled && !cfg.Cluster.ReplicationCatchUpEnabled {
			log.Warn().Msg("cluster.query_gate_on_catchup=true requires cluster.replication_catchup_enabled=true; the catch-up walker is disabled, so the gate would never clear. Auto-disabling the gate.")
			gateEnabled = false
		}
		queryHandler.SetCluster(clusterCoordinator, gateEnabled)
		if gateEnabled {
			log.Info().Msg("Query catch-up gate enabled — read endpoints will return 503 until the startup catch-up batch settles")
		}

		// Wire the post-compaction cache-invalidate endpoint, conditionally.
		// The endpoint is registered ONLY when cluster.shared_secret is set —
		// the only legitimate sender is a cluster peer doing post-compaction
		// fan-out (see SetOnCompactionComplete below), which by definition
		// needs the secret to compute the MAC. Without the secret there is no
		// caller, so we don't register the route at all (Fiber returns 404).
		// This eliminates a runtime check on every request and makes the
		// "not configured" state impossible to confuse with a misauth.
		// Tolerance matches the project default for HMAC freshness windows.
		if cfg.Cluster.SharedSecret != "" {
			localNode := clusterCoordinator.GetRegistry().Local()
			if localNode == nil {
				log.Fatal().Msg("cluster.shared_secret is configured but local node missing from registry — coordinator wiring is broken")
			}
			cacheInvalidateHandler := api.NewCacheInvalidateHandler(
				cfg.Cluster.SharedSecret,
				cfg.Cluster.ClusterName,
				localNode.ID,
				security.NewNonceCache(cacheInvalidateHMACTolerance),
				cacheInvalidateHMACTolerance,
				func() {
					db.ClearHTTPCache()
					queryHandler.InvalidateCaches()
				},
				log.With().Str("component", "cache-invalidate").Logger(),
			)
			cacheInvalidateHandler.Register(server.GetApp())
		}
	}

	// Initialize Query Governance (Enterprise feature - requires valid license)
	if cfg.Governance.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Query governance requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseQueryGovernance() {
			log.Warn().Msg("License does not include query_governance feature - feature disabled")
		} else {
			governanceDBPath := cfg.Auth.DBPath
			if governanceDBPath == "" {
				governanceDBPath = "./data/arc.db"
			}
			governanceDB, err := sql.Open("sqlite3", governanceDBPath)
			if err != nil {
				log.Error().Err(err).Msg("Failed to open governance database - feature disabled")
			} else {
				governanceManager, err := governance.NewManager(&governance.ManagerConfig{
					DB:     governanceDB,
					Config: &cfg.Governance,
					Logger: logger.Get("governance"),
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to create governance manager - feature disabled")
				} else {
					governanceManager.Start()
					shutdownCoordinator.RegisterHook("governance", func(ctx context.Context) error {
						return governanceManager.Stop()
					}, shutdown.PriorityCompaction)

					// Wire governance to query handler
					queryHandler.SetGovernance(governanceManager, licenseClient)

					// Register governance API handler
					governanceHandler := api.NewGovernanceHandler(governanceManager, authManager, licenseClient, logger.Get("governance-api"))
					governanceHandler.RegisterRoutes(server.GetApp())

					log.Info().
						Bool("enabled", true).
						Int("default_rate_limit_per_min", cfg.Governance.DefaultRateLimitPerMin).
						Int("default_rate_limit_per_hour", cfg.Governance.DefaultRateLimitPerHour).
						Int("default_max_queries_per_hour", cfg.Governance.DefaultMaxQueriesPerHour).
						Int("default_max_queries_per_day", cfg.Governance.DefaultMaxQueriesPerDay).
						Int("default_max_rows_per_query", cfg.Governance.DefaultMaxRowsPerQuery).
						Msg("Query governance enabled")
				}
			}
		}
	}

	// Initialize Query Management (Enterprise feature - requires valid license)
	if cfg.QueryManagement.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Query management requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseQueryManagement() {
			log.Warn().Msg("License does not include query_management feature - feature disabled")
		} else {
			qmRegistry := queryregistry.NewRegistry(&queryregistry.RegistryConfig{
				HistorySize: cfg.QueryManagement.HistorySize,
			}, logger.Get("query-registry"))

			// Wire registry to query handler
			queryHandler.SetQueryRegistry(qmRegistry)

			// Register query management API handler
			qmHandler := api.NewQueryManagementHandler(qmRegistry, authManager, licenseClient, logger.Get("query-mgmt-api"))
			qmHandler.RegisterRoutes(server.GetApp())

			log.Info().
				Bool("enabled", true).
				Int("history_size", cfg.QueryManagement.HistorySize).
				Msg("Query management enabled")
		}
	}

	// Register Compaction handler (if compaction is enabled)
	if compactionManager != nil {
		compactionHandler := api.NewCompactionHandler(compactionManager, hourlyScheduler, dailyScheduler, authManager, logger.Get("compaction"))
		compactionHandler.RegisterRoutes(server.GetApp())

		// Wire post-compaction cache invalidation.
		// After compaction deletes old parquet files, DuckDB's cache_httpfs still holds
		// cached glob results (directory listings) pointing to deleted files, causing 404s.
		// This callback clears all relevant caches in the parent process after each
		// successful compaction job. See: https://github.com/Basekick-Labs/arc/issues/204
		// Logged exactly once per process when cluster mode is up but
		// cluster.shared_secret is empty — the per-compaction Warn would
		// otherwise spam the log every compaction (hourly + daily).
		var fanOutSkipLogOnce sync.Once

		// Build a single shared *http.Client for the cache-invalidate
		// fan-out so connection pooling actually reuses sockets across
		// compactions. Only constructed when cluster mode is on —
		// compaction itself works in OSS/standalone, but the fan-out
		// path is cluster-only (the per-call `if clusterCoordinator
		// != nil` guard inside the callback is what actually decides
		// whether to send). We reuse the coordinator's already-loaded
		// *tls.Config rather than re-reading cert/key/CA from disk;
		// when cluster.tls_enabled is false the getter returns nil
		// and the transport falls back to plaintext (or system roots
		// if the URL is https://). No Client.Timeout: the per-request
		// context.WithTimeout below is the policy bound; an extra
		// Client.Timeout would be redundant dead config.
		var clusterHTTPClient *http.Client
		var clusterScheme string
		if clusterCoordinator != nil {
			clusterHTTPClient = &http.Client{
				Transport: security.NewClusterHTTPTransport(clusterCoordinator.ClusterTLSConfig()),
			}
			clusterScheme = security.SchemeForServer(cfg.Server.TLSEnabled)
			log.Info().
				Str("scheme", clusterScheme).
				Bool("cluster_tls", cfg.Cluster.TLSEnabled).
				Msg("Post-compaction cache-invalidate fan-out transport initialised")
		}

		compactionManager.SetOnCompactionComplete(func() {
			// Local invalidation
			db.ClearHTTPCache()
			queryHandler.InvalidateCaches()

			// Distributed invalidation (enterprise clustering)
			// Notify all query-capable nodes to clear their caches too
			if clusterCoordinator != nil {
				registry := clusterCoordinator.GetRegistry()
				localNode := registry.Local()
				if localNode == nil {
					return
				}

				// Skip the HTTP fan-out entirely when no cluster shared
				// secret is configured. The receiving end refuses every
				// request in that state (see handleCacheInvalidate), so
				// sending would be a waste. Wrapped in sync.Once so the
				// operator sees the explanation exactly once at first
				// compaction, not on every subsequent run.
				if cfg.Cluster.SharedSecret == "" {
					fanOutSkipLogOnce.Do(func() {
						log.Warn().Msg("post-compaction cluster cache invalidation skipped: cluster.shared_secret is not configured; each node's local in-process invalidation already ran. Set cluster.shared_secret to enable fan-out.")
					})
					return
				}

				targets := registry.GetReaders()
				targets = append(targets, registry.GetWriters()...)

				for _, node := range targets {
					if node.ID == localNode.ID {
						continue
					}
					go func(n *cluster.Node) {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()

						// Compute the HMAC once per request: a fresh nonce
						// (32 random bytes, hex-encoded) binds the MAC to a
						// single attempt; the timestamp binds it to a
						// 5-minute freshness window. Receiver replay-checks
						// (nonce, sender nodeID) against its NonceCache.
						nonce, err := security.GenerateNonce()
						if err != nil {
							log.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to generate nonce for cache invalidation")
							return
						}
						ts := time.Now().Unix()
						mac := security.ComputeCacheInvalidateHMAC(
							cfg.Cluster.SharedSecret, nonce, localNode.ID, cfg.Cluster.ClusterName, ts,
						)

						// Build via *url.URL so an IPv6 literal in
						// n.APIAddress (e.g. "[::1]:8000") survives
						// unmangled. CacheInvalidatePath is a known-safe
						// constant path, so we don't need to escape it.
						peerURL := (&url.URL{
							Scheme: clusterScheme,
							Host:   n.APIAddress,
							Path:   api.CacheInvalidatePath,
						}).String()
						req, err := http.NewRequestWithContext(ctx, http.MethodPost, peerURL, nil)
						if err != nil {
							log.Warn().Err(err).Str("node_id", n.ID).Msg("Failed to create cache invalidation request")
							return
						}
						req.Header.Set("X-Arc-Node-ID", localNode.ID)
						req.Header.Set("X-Arc-Cluster", cfg.Cluster.ClusterName)
						req.Header.Set("X-Arc-Nonce", nonce)
						req.Header.Set("X-Arc-Timestamp", strconv.FormatInt(ts, 10))
						req.Header.Set("X-Arc-HMAC", mac)

						resp, err := clusterHTTPClient.Do(req)
						if err != nil {
							log.Warn().Err(err).Str("node_id", n.ID).Str("address", n.APIAddress).
								Msg("Failed to invalidate cache on remote node")
							return
						}
						defer resp.Body.Close()
						_, _ = io.Copy(io.Discard, resp.Body)
						if resp.StatusCode != http.StatusNoContent {
							log.Warn().Int("status", resp.StatusCode).Str("node_id", n.ID).
								Msg("Unexpected status from cache invalidation")
						} else {
							log.Debug().Str("node_id", n.ID).Msg("Remote cache invalidated after compaction")
						}
					}(node)
				}
			}
		})
	}

	// Register Delete handler
	// DELETE on S3-backed deployments stages the rewritten parquet locally
	// before uploading; the temp file MUST land inside the DuckDB sandbox's
	// allowed_directories. Reuse the same dir as the import handler — it's
	// already allowlisted and lifecycle-managed (the file is unlinked after
	// upload). Cross-handler reuse is fine because the filenames are unique
	// via os.CreateTemp.
	deleteHandler := api.NewDeleteHandler(db, storageBackend, &cfg.Delete, authManager, dbConfig.UploadDir, logger.Get("delete"))
	deleteHandler.RegisterRoutes(server.GetApp())
	if clusterCoordinator != nil {
		deleteHandler.SetCoordinator(clusterCoordinator)
	}
	if cfg.Delete.Enabled {
		log.Info().
			Int("confirmation_threshold", cfg.Delete.ConfirmationThreshold).
			Int("max_rows_per_delete", cfg.Delete.MaxRowsPerDelete).
			Msg("Delete operations enabled")
	} else {
		log.Info().Msg("Delete operations DISABLED (set delete.enabled=true in arc.toml to enable)")
	}

	// Register Databases handler
	databasesHandler := api.NewDatabasesHandler(storageBackend, &cfg.Delete, authManager, logger.Get("databases"))
	databasesHandler.RegisterRoutes(server.GetApp())

	// Register Debug handler — admin-auth memory diagnostics
	debugHandler := api.NewDebugHandler(db, authManager, logger.Get("debug"))
	debugHandler.RegisterRoutes(server.GetApp())

	// Register Retention handler
	var retentionHandler *api.RetentionHandler
	if cfg.Retention.Enabled {
		var err error
		retentionHandler, err = api.NewRetentionHandler(storageBackend, db, &cfg.Retention, licenseClient, authManager, logger.Get("retention"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize retention handler")
		}
		retentionHandler.RegisterRoutes(server.GetApp())
		if clusterCoordinator != nil {
			retentionHandler.SetCoordinator(clusterCoordinator)
		}
		shutdownCoordinator.RegisterHook("retention", func(ctx context.Context) error {
			return retentionHandler.Close()
		}, shutdown.PriorityDatabase)
		log.Info().Str("db_path", cfg.Retention.DBPath).Msg("Retention policies enabled")
	} else {
		log.Info().Msg("Retention policies DISABLED")
	}

	// Register Continuous Query handler
	var cqHandler *api.ContinuousQueryHandler
	if cfg.ContinuousQuery.Enabled {
		var err error
		cqHandler, err = api.NewContinuousQueryHandler(db, storageBackend, arrowBuffer, &cfg.ContinuousQuery, authManager, logger.Get("cq"))
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to initialize continuous query handler")
		}
		if clusterCoordinator != nil {
			cqHandler.SetCoordinator(clusterCoordinator)
		}
		cqHandler.RegisterRoutes(server.GetApp())
		shutdownCoordinator.RegisterHook("continuous-query", func(ctx context.Context) error {
			return cqHandler.Close()
		}, shutdown.PriorityDatabase)
		log.Info().Str("db_path", cfg.ContinuousQuery.DBPath).Msg("Continuous queries enabled")
	} else {
		log.Info().Msg("Continuous queries DISABLED")
	}

	// Initialize CQ Scheduler (Enterprise feature - requires valid license)
	// Scheduler is enabled when continuous_query.enabled=true AND license allows it
	var cqScheduler *scheduler.CQScheduler
	if cfg.ContinuousQuery.Enabled && cqHandler != nil {
		if licenseClient != nil && licenseClient.CanUseCQScheduler() {
			var err error
			var cqGate scheduler.WriterGate
			if clusterCoordinator != nil {
				cqGate = newWriterClusterGate(clusterCoordinator)
			}
			cqScheduler, err = scheduler.NewCQScheduler(&scheduler.CQSchedulerConfig{
				CQHandler:     cqHandler,
				LicenseClient: licenseClient,
				ClusterGate:   cqGate,
				Logger:        logger.Get("cq-scheduler"),
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to create CQ scheduler")
			} else {
				if err := cqScheduler.Start(); err != nil {
					log.Error().Err(err).Msg("Failed to start CQ scheduler")
				} else {
					shutdownCoordinator.RegisterHook("cq-scheduler", func(ctx context.Context) error {
						cqScheduler.Stop()
						return nil
					}, shutdown.PriorityCompaction)
					cqHandler.SetScheduler(cqScheduler)
					log.Info().Int("job_count", cqScheduler.JobCount()).Msg("CQ scheduler started")
				}
			}
		} else if licenseClient == nil {
			log.Info().Msg("CQ automatic scheduling requires enterprise license")
		} else {
			log.Info().Msg("CQ automatic scheduling not included in license tier")
		}
	}

	// Initialize Retention Scheduler (Enterprise feature - requires valid license)
	// Scheduler is enabled when retention.enabled=true AND license allows it
	var retentionScheduler *scheduler.RetentionScheduler
	if cfg.Retention.Enabled && retentionHandler != nil {
		if licenseClient != nil && licenseClient.CanUseRetentionScheduler() {
			var err error
			var retentionGate scheduler.WriterGate
			if clusterCoordinator != nil {
				retentionGate = newWriterClusterGate(clusterCoordinator)
			}
			retentionScheduler, err = scheduler.NewRetentionScheduler(&scheduler.RetentionSchedulerConfig{
				RetentionHandler: retentionHandler,
				LicenseClient:    licenseClient,
				ClusterGate:      retentionGate,
				Schedule:         cfg.Scheduler.RetentionSchedule,
				Logger:           logger.Get("retention-scheduler"),
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to create retention scheduler")
			} else {
				if err := retentionScheduler.Start(); err != nil {
					log.Error().Err(err).Msg("Failed to start retention scheduler")
				} else {
					shutdownCoordinator.RegisterHook("retention-scheduler", func(ctx context.Context) error {
						retentionScheduler.Stop()
						return nil
					}, shutdown.PriorityCompaction)
					log.Info().Str("schedule", cfg.Scheduler.RetentionSchedule).Msg("Retention scheduler started")
				}
			}
		} else if licenseClient == nil {
			log.Info().Msg("Retention automatic scheduling requires enterprise license")
		} else {
			log.Info().Msg("Retention automatic scheduling not included in license tier")
		}
	}

	// Register Scheduler status handler (always register, shows status even if schedulers not running)
	// Note: We must explicitly pass nil interfaces when schedulers are nil, because
	// a nil *CQScheduler passed to an interface is not a nil interface (Go quirk)
	var cqSchedulerInterface api.CQSchedulerInterface
	if cqScheduler != nil {
		cqSchedulerInterface = cqScheduler
	}
	var retentionSchedulerInterface api.RetentionSchedulerInterface
	if retentionScheduler != nil {
		retentionSchedulerInterface = retentionScheduler
	}
	schedulerHandler := api.NewSchedulerHandler(cqSchedulerInterface, retentionSchedulerInterface, licenseClient, authManager, logger.Get("scheduler-api"))
	schedulerHandler.RegisterRoutes(server.GetApp())

	// Initialize Reconciliation (Phase 5 manifest-vs-storage drift cleanup).
	// Enterprise feature riding on FeatureClustering — no separate license
	// flag, since standalone Arc has no manifest. Off by default; once
	// enabled, runs on cron with conservative grace window + blast cap.
	var reconciliationScheduler *reconciliation.Scheduler
	if cfg.Reconciliation.Enabled && clusterCoordinator != nil {
		gate := newReconciliationClusterGate(clusterCoordinator, cfg.Storage.Backend)
		recCfg := reconciliation.Config{
			Enabled:                  true,
			BackendKind:              reconciliationBackendKind(cfg.Storage.Backend),
			LocalNodeID:              cfg.Cluster.NodeID,
			GraceWindow:              time.Duration(cfg.Reconciliation.GraceWindowSeconds) * time.Second,
			ClockSkewAllowance:       time.Duration(cfg.Reconciliation.ClockSkewAllowanceSeconds) * time.Second,
			PerPrefixTimeout:         time.Duration(cfg.Reconciliation.PerPrefixTimeoutSeconds) * time.Second,
			MaxRunDuration:           time.Duration(cfg.Reconciliation.MaxRunDurationSeconds) * time.Second,
			MaxManifestSize:          cfg.Reconciliation.MaxManifestSize,
			MaxDeletesPerRun:         cfg.Reconciliation.MaxDeletesPerRun,
			BatchSize:                cfg.Reconciliation.BatchSize,
			DeletePreManifestOrphans: cfg.Reconciliation.DeletePreManifestOrphans,
			ManifestOnlyDryRun:       cfg.Reconciliation.ManifestOnlyDryRun,
			SamplePathsCap:           cfg.Reconciliation.SamplePathsCap,
			MaxRootWalkDatabases:     cfg.Reconciliation.MaxRootWalkDatabases,
			RecheckConcurrency:       cfg.Reconciliation.RecheckConcurrency,
		}
		reconciler, err := reconciliation.NewReconciler(recCfg, clusterCoordinator, storageBackend, gate, auditLogger, logger.Get("reconciliation"))
		if err != nil {
			log.Error().Err(err).Msg("Failed to create reconciler — feature disabled")
		} else {
			reconciliationScheduler, err = reconciliation.NewScheduler(reconciliation.SchedulerConfig{
				Reconciler: reconciler,
				Schedule:   cfg.Reconciliation.Schedule,
				Logger:     logger.Get("reconciliation-scheduler"),
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to create reconciliation scheduler")
				reconciliationScheduler = nil
			} else if err := reconciliationScheduler.Start(); err != nil {
				log.Error().Err(err).Msg("Failed to start reconciliation scheduler")
				reconciliationScheduler = nil
			} else {
				shutdownCoordinator.RegisterHook("reconciliation-scheduler", func(ctx context.Context) error {
					reconciliationScheduler.Stop()
					return nil
				}, shutdown.PriorityCompaction)
				log.Info().Str("schedule", cfg.Reconciliation.Schedule).
					Str("backend_kind", string(recCfg.BackendKind)).
					Bool("dry_run_only", cfg.Reconciliation.ManifestOnlyDryRun).
					Msg("Reconciliation scheduler started")
			}
		}
	} else if cfg.Reconciliation.Enabled && clusterCoordinator == nil {
		log.Warn().Msg("Reconciliation requires Enterprise clustering (cluster.enabled=true) — feature disabled")
	}

	// Always register the handler so /api/v1/reconciliation/status reports
	// the disabled state when the scheduler isn't running.
	var reconciliationSchedulerInterface api.ReconciliationSchedulerInterface
	if reconciliationScheduler != nil {
		reconciliationSchedulerInterface = reconciliationScheduler
	}
	reconciliationHandler := api.NewReconciliationHandler(reconciliationSchedulerInterface, licenseClient, authManager, logger.Get("reconciliation-api"))
	reconciliationHandler.RegisterRoutes(server.GetApp())

	// Register Cluster handler (always register, shows status even if clustering not enabled)
	clusterHandler := api.NewClusterHandler(clusterCoordinator, authManager, licenseClient, logger.Get("cluster-api"))
	clusterHandler.RegisterRoutes(server.GetApp())

	// MQTT API handlers are registered unconditionally. Both MQTTHandler
	// (stats/health) and MQTTSubscriptionHandler (CRUD/lifecycle) nil-guard
	// every endpoint when manager is nil, returning a stable, documented shape
	// instead of letting some routes 404 and others 503. Most return 503 with
	// "MQTT subsystem disabled" — except MQTTHandler.handleHealth, which
	// returns 200 with `{"status":"disabled","healthy":false}` so uptime
	// monitors don't page operators about a configured-off subsystem.
	// Regression tests in internal/api/mqtt_test.go and
	// internal/api/mqtt_subscriptions_test.go pin the disabled-response shape.
	mqttHandler := api.NewMQTTHandler(mqttManager, authManager, logger.Get("mqtt-api"))
	mqttHandler.RegisterRoutes(server.GetApp())

	mqttSubHandler := api.NewMQTTSubscriptionHandler(mqttManager, authManager, logger.Get("mqtt-subscriptions-api"))
	mqttSubHandler.RegisterRoutes(server.GetApp())

	// Initialize Tiered Storage (Enterprise feature - requires valid license)
	// 2-tier system: Hot (local) -> Cold (S3/Azure archive)
	var tieringManager *tiering.Manager
	if cfg.TieredStorage.Enabled {
		if licenseClient == nil {
			log.Warn().Msg("Tiered storage requires enterprise license - feature disabled")
		} else if !licenseClient.CanUseTieredStorage() {
			log.Warn().Msg("License does not include tiered_storage feature - feature disabled")
		} else {
			// Open SQLite database for tiering metadata (shared with other features)
			tieringDBPath := cfg.Auth.DBPath // Use shared SQLite database
			tieringDB, err := sql.Open("sqlite3", tieringDBPath)
			if err != nil {
				log.Error().Err(err).Msg("Failed to open tiering database - feature disabled")
			} else {
				// Create cold tier backend (S3 or Azure)
				var coldBackend storage.Backend
				cold := cfg.TieredStorage.Cold

				if cold.Enabled {
					switch cold.Backend {
					case "s3":
						s3Config := &storage.S3Config{
							Region:    cold.S3Region,
							Bucket:    cold.S3Bucket,
							Endpoint:  cold.S3Endpoint,
							AccessKey: cold.S3AccessKey,
							SecretKey: cold.S3SecretKey,
							UseSSL:    cold.S3UseSSL,
							PathStyle: cold.S3PathStyle,
							Prefix:    cold.S3Prefix,
						}
						coldBackend, err = storage.NewS3Backend(s3Config, logger.Get("tiering-cold-s3"))
						if err != nil {
							log.Error().Err(err).Msg("Failed to create cold tier S3 backend for tiering")
						}

					case "azure":
						azureConfig := &storage.AzureBlobConfig{
							ConnectionString:   cold.AzureConnectionString,
							AccountName:        cold.AzureAccountName,
							AccountKey:         cold.AzureAccountKey,
							SASToken:           cold.AzureSASToken,
							ContainerName:      cold.AzureContainer,
							Endpoint:           cold.AzureEndpoint,
							UseManagedIdentity: cold.AzureUseManagedIdentity,
						}
						coldBackend, err = storage.NewAzureBlobBackend(azureConfig, logger.Get("tiering-cold-azure"))
						if err != nil {
							log.Error().Err(err).Msg("Failed to create cold tier Azure backend for tiering")
						}
					}
				}

				// Create tiering manager
				tieringManager, err = tiering.NewManager(&tiering.ManagerConfig{
					HotBackend:    storageBackend,
					ColdBackend:   coldBackend,
					DB:            tieringDB,
					Config:        &cfg.TieredStorage,
					LicenseClient: licenseClient,
					Logger:        logger.Get("tiering"),
				})
				if err != nil {
					log.Error().Err(err).Msg("Failed to create tiering manager - feature disabled")
				} else {
					// Start tiering manager
					if err := tieringManager.Start(); err != nil {
						log.Error().Err(err).Msg("Failed to start tiering manager")
					} else {
						shutdownCoordinator.RegisterHook("tiering", func(ctx context.Context) error {
							return tieringManager.Stop()
						}, shutdown.PriorityCompaction)

						log.Info().
							Str("schedule", cfg.TieredStorage.MigrationSchedule).
							Bool("cold_enabled", cfg.TieredStorage.Cold.Enabled).
							Int("default_hot_days", cfg.TieredStorage.DefaultHotMaxAgeDays).
							Msg("Tiered storage enabled")
					}
				}
			}
		}
	}

	// Register Tiering API handlers (always register, handlers check if manager is nil)
	if tieringManager != nil {
		tieringHandler := api.NewTieringHandler(tieringManager, authManager, licenseClient, logger.Get("tiering-api"))
		tieringHandler.RegisterRoutes(server.GetApp())

		tieringPoliciesHandler := api.NewTieringPoliciesHandler(tieringManager, authManager, licenseClient, logger.Get("tiering-policies-api"))
		tieringPoliciesHandler.RegisterRoutes(server.GetApp())

		// Wire tiering manager to query handler for multi-tier query routing
		queryHandler.SetTieringManager(tieringManager)
		log.Info().Msg("Tiering manager wired to query handler for multi-tier queries")

		// Wire tiering manager to databases handler for cold-tier database/measurement listing
		databasesHandler.SetTieringManager(tieringManager)
		log.Info().Msg("Tiering manager wired to databases handler for cold-tier listing")

		// Wire tiering manager to arrow buffer for automatic file registration
		arrowBuffer.SetTieringManager(tieringManager)
		log.Info().Msg("Tiering manager wired to arrow buffer for auto-registration")

		// Configure DuckDB with cold tier S3 credentials for direct S3 queries
		// This is needed because DuckDB's httpfs extension needs credentials to query S3 directly
		cold := cfg.TieredStorage.Cold
		// Configure DuckDB for cold-tier S3 whenever cold tiering targets S3.
		// Empty access/secret keys are valid: ConfigureS3 then provisions a
		// CREDENTIAL_CHAIN secret so IAM roles / IRSA / env credentials work for
		// tiered queries. A half-supplied key pair is rejected by ConfigureS3.
		if cold.Enabled && cold.Backend == "s3" {
			if err := db.ConfigureS3(&database.S3Config{
				Region:    cold.S3Region,
				Endpoint:  cold.S3Endpoint,
				AccessKey: cold.S3AccessKey,
				SecretKey: cold.S3SecretKey,
				UseSSL:    cold.S3UseSSL,
				PathStyle: cold.S3PathStyle,
				Bucket:    cold.S3Bucket,
				Prefix:    cold.S3Prefix,
			}); err != nil {
				log.Warn().Err(err).Msg("Failed to configure DuckDB with cold tier S3 credentials")
			} else {
				log.Info().Msg("DuckDB configured with cold tier S3 credentials for multi-tier queries")
			}
		}

		// Configure DuckDB for cold-tier Azure (scoped secret, separate from the
		// primary Azure secret). Empty account key → credential chain (managed
		// identity / env). Mirrors the cold-tier S3 path above.
		if cold.Enabled && cold.Backend == "azure" {
			if err := db.ConfigureAzure(&database.AzureConfig{
				AccountName: cold.AzureAccountName,
				AccountKey:  cold.AzureAccountKey,
				Container:   cold.AzureContainer,
			}); err != nil {
				log.Warn().Err(err).Msg("Failed to configure DuckDB with cold tier Azure credentials")
			} else {
				log.Info().Msg("DuckDB configured with cold tier Azure credentials for multi-tier queries")
			}
		}
	}

	// Initialize Backup/Restore (OSS feature — no license required)
	if cfg.Backup.Enabled {
		backupManager, err := backup.NewManager(&backup.ManagerConfig{
			DataStorage:  storageBackend,
			BackupPath:   cfg.Backup.LocalPath,
			SQLiteDBPath: cfg.Auth.DBPath,
			ConfigPath:   "arc.toml",
			Logger:       logger.Get("backup"),
		})
		if err != nil {
			log.Error().Err(err).Msg("Failed to initialize backup manager")
		} else {
			backupHandler := api.NewBackupHandler(backupManager, authManager, logger.Get("backup-api"))
			backupHandler.RegisterRoutes(server.GetApp())
			log.Info().Str("backup_path", cfg.Backup.LocalPath).Msg("Backup/restore enabled")
		}
	}

	// Register audit API routes (audit logger initialized earlier, before API routes)
	if auditLogger != nil {
		auditHandler := api.NewAuditHandler(auditLogger, authManager, licenseClient, logger.Get("audit-api"))
		auditHandler.RegisterRoutes(server.GetApp())
	}

	// Mark /ready=503 BEFORE the HTTP listener drain begins so the load
	// balancer (Pattern 2 multi-writer) stops routing new requests here
	// before the listener actually closes. Priority 5 < PriorityHTTPServer=10
	// so this hook fires first.
	//
	// The sleep is load-bearing: shutdown hooks run sequentially with no
	// inter-hook wait, so without it the next hook (http-server, priority
	// 10) would call app.Shutdown() within microseconds — before the LB
	// has had time to poll /ready and observe the 503. The 10-second
	// default matches the typical LB health-check cycle (HAProxy + nginx
	// + Traefik default to ~5-10s polls). Operators with faster or
	// slower polls should configure their LB termination grace
	// accordingly; a future PR may expose this as an env var
	// (ARC_SHUTDOWN_READY_DRAIN_SECONDS) if customers ask.
	//
	// In-flight requests that arrived BEFORE MarkNotReady drain via Fiber's
	// normal shutdown handling — they complete; only NEW requests get
	// rejected by the LB once it observes the 503.
	// Drain grace: 10s in clustered mode gives the LB one poll cycle to
	// observe /ready=503 and stop routing before the listener closes.
	// In standalone mode there's no LB / no peer cluster — the grace
	// just delays every shutdown for no benefit (local dev, integration
	// tests). Skip it entirely. (Gemini PR #463 round 6.)
	readyDrainGrace := 10 * time.Second
	if !cfg.Cluster.Enabled {
		readyDrainGrace = 0
	}
	shutdownCoordinator.RegisterHook("ready-flag-off", func(ctx context.Context) error {
		server.MarkNotReady()
		if readyDrainGrace == 0 {
			return nil
		}
		// time.NewTimer + defer Stop instead of time.After: if ctx.Done
		// wins the select, time.After would leak the underlying timer
		// until the deadline expires. Bounded leak in a shutdown hook,
		// but using the standard idiom anyway.
		timer := time.NewTimer(readyDrainGrace)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			// Coordinator timeout — yield immediately so http-server hook
			// can still run within the remaining budget. Better an
			// undrained close than a deadlocked shutdown.
		}
		return nil
	}, 5)

	// Register HTTP server shutdown hook (first to stop accepting new
	// requests). The 30-second arg is the HTTP server's INTERNAL drain
	// timeout — independent of shutdownCoordinator's own 30-second
	// budget. If the coordinator ctx expires before this hook completes,
	// the coordinator skips remaining hooks; the internal timeout is a
	// best-effort upper bound on this hook's slice of that budget. The
	// debug-pprof hook (registered earlier in main, same priority) uses
	// srv.Close() instead of Shutdown(ctx) to avoid letting a long
	// /debug/pprof/profile?seconds=N capture starve this hook.
	shutdownCoordinator.RegisterHook("http-server", func(ctx context.Context) error {
		return server.Shutdown(30 * time.Second)
	}, shutdown.PriorityHTTPServer)

	// Mark /ready=200 so the load balancer (Pattern 2 multi-writer) starts
	// routing traffic here. By this point WAL recovery has completed
	// (cmd/arc/main.go:701-724), the Arrow buffer is initialised, the
	// cluster coordinator is up, and all background schedulers are running.
	// In single-writer deployments this is also the signal to Kubernetes
	// readiness probes / any LB doing httpchk that the node is healthy.
	server.MarkReady()

	// Start server
	if err := server.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start HTTP server")
	}

	protocol := "HTTP"
	if cfg.Server.TLSEnabled {
		protocol = "HTTPS"
	}
	log.Info().
		Int("port", cfg.Server.Port).
		Str("protocol", protocol).
		Str("version", Version).
		Bool("fips_mode", fips.Enabled()).
		Msg("Arc is ready!")

	// Wait for shutdown signal
	sig := shutdownCoordinator.WaitForSignal()
	log.Info().Str("signal", sig.String()).Msg("Initiating graceful shutdown...")

	// Perform graceful shutdown of all components
	if err := shutdownCoordinator.Shutdown(); err != nil {
		log.Error().Err(err).Msg("Shutdown completed with errors")
		os.Exit(1)
	}

	log.Info().Msg("Arc shutdown complete")
}

// createWALRecoveryCallback creates a reusable WAL recovery callback function.
// This callback replays recovered WAL records through the ArrowBuffer for re-ingestion.
func createWALRecoveryCallback(arrowBuffer *ingest.ArrowBuffer, walLogger zerolog.Logger) wal.RecoveryCallback {
	return func(ctx context.Context, records []map[string]interface{}) error {
		if len(records) == 0 {
			return nil
		}
		for _, rec := range records {
			// Extract measurement from recovered record
			// WAL row format uses underscore-prefixed keys: _measurement, _database
			measurement, _ := rec["_measurement"].(string)
			if measurement == "" {
				measurement, _ = rec["measurement"].(string)
			}
			if measurement == "" {
				measurement, _ = rec["m"].(string)
			}
			if measurement == "" {
				continue // Skip records without measurement
			}

			database, _ := rec["_database"].(string)
			if database == "" {
				database, _ = rec["database"].(string)
			}
			if database == "" {
				database = "default"
			}

			// Build columnar record from recovered data
			columns := make(map[string][]interface{})
			for key, value := range rec {
				if key == "_measurement" || key == "measurement" || key == "m" || key == "_database" || key == "database" {
					continue
				}
				columns[key] = []interface{}{value}
			}

			if err := arrowBuffer.WriteColumnarDirectNoWAL(ctx, database, measurement, columns); err != nil {
				walLogger.Error().Err(err).Str("measurement", measurement).Msg("Failed to replay WAL record")
				return err
			}
		}
		walLogger.Info().Int("records", len(records)).Msg("WAL recovery: replayed records")
		return nil
	}
}

// createColumnarRecoveryCallback creates a WAL recovery callback for columnar entries
// written via the zero-copy AppendRaw path.
func createColumnarRecoveryCallback(arrowBuffer *ingest.ArrowBuffer, walLogger zerolog.Logger) wal.ColumnarRecoveryCallback {
	return func(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
		if database == "" {
			database = "default"
		}
		if err := arrowBuffer.WriteColumnarDirectNoWAL(ctx, database, measurement, columns); err != nil {
			walLogger.Error().Err(err).Str("database", database).Str("measurement", measurement).Msg("Failed to replay columnar WAL entry")
			return err
		}
		rowCount := 0
		for _, col := range columns {
			rowCount = len(col)
			break
		}
		walLogger.Info().Str("database", database).Str("measurement", measurement).Int("rows", rowCount).Msg("WAL recovery: replayed columnar entry")
		return nil
	}
}

// runCompactSubcommand handles the "compact" subcommand for subprocess-based compaction.
// This is invoked by the parent process to run compaction jobs in isolation,
// ensuring DuckDB memory is fully released when the subprocess exits.
func runCompactSubcommand(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	jobJSON := fs.String("job", "", "Job configuration as JSON (deprecated, use --job-stdin)")
	jobStdin := fs.Bool("job-stdin", false, "Read job configuration from stdin")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	var configData []byte
	var err error

	if *jobStdin {
		// Read config from stdin (preferred - avoids argument list too long errors)
		configData, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: failed to read config from stdin: %v\n", err)
			os.Exit(1)
		}
	} else if *jobJSON != "" {
		// Legacy: read from command line argument
		configData = []byte(*jobJSON)
	} else {
		fmt.Fprintln(os.Stderr, "error: --job-stdin or --job flag required")
		os.Exit(1)
	}

	var cfg compaction.SubprocessJobConfig
	if err := json.Unmarshal(configData, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid job config: %v\n", err)
		os.Exit(1)
	}

	// Run compaction job
	result, err := compaction.RunSubprocessJob(&cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Output result as JSON to stdout for parent process to parse
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to encode result: %v\n", err)
		os.Exit(1)
	}
}

// writerClusterGate implements scheduler.WriterGate (satisfies both
// scheduler.RetentionClusterGate and scheduler.CQClusterGate aliases).
// Only the primary writer node runs scheduled mutations (retention, CQ) to
// prevent duplicate writes. Lives in main.go to avoid a compile-time
// dependency between the scheduler and cluster packages.
type writerClusterGate struct {
	coordinator *cluster.Coordinator
}

func newWriterClusterGate(c *cluster.Coordinator) *writerClusterGate {
	return &writerClusterGate{coordinator: c}
}

func (g *writerClusterGate) IsPrimaryWriter() bool {
	return g.coordinator.IsPrimaryWriter()
}

func (g *writerClusterGate) Role() string {
	return string(g.coordinator.GetRole())
}

// compactionClusterGate implements compaction.ClusterGate. When the
// coordinator has a compactor failover manager (Phase 5), it checks the
// FSM's active compactor lease instead of the static role. When failover
// is not configured (no coordinator, or coordinator without failover),
// it falls back to the static role check from Phase 4.
//
// This type sits in main.go so the compaction package has no compile-time
// dependency on the cluster package.
type compactionClusterGate struct {
	role         cluster.NodeRole
	capabilities cluster.RoleCapabilities
	// coordinator is non-nil when clustering is enabled. When the
	// coordinator has a compactor failover manager, CanCompact checks
	// the FSM lease instead of the static role.
	coordinator *cluster.Coordinator
}

func newCompactionClusterGate(roleString string) *compactionClusterGate {
	role := cluster.ParseRole(roleString)
	return &compactionClusterGate{
		role:         role,
		capabilities: role.GetCapabilities(),
	}
}

// setCoordinator enables the dynamic FSM lease check for Phase 5 failover.
// Called after the coordinator is created and started.
func (g *compactionClusterGate) setCoordinator(c *cluster.Coordinator) {
	g.coordinator = c
}

// CanCompact reports whether this node should run compaction.
//
// Phase 5 (failover enabled): checks the FSM's active compactor lease.
// Phase 4 (no failover): checks the static role capability.
func (g *compactionClusterGate) CanCompact() bool {
	if g.coordinator != nil && g.coordinator.GetActiveCompactorID() != "" {
		// Failover system is active — use the FSM lease.
		return g.coordinator.IsActiveCompactor()
	}
	// No failover or no lease assigned yet — fall back to static role.
	return g.capabilities.CanCompact
}

// Role returns the node's role string for log messages.
func (g *compactionClusterGate) Role() string {
	return string(g.role)
}

// reconciliationClusterGate implements reconciliation.Gate. The two halves
// of a reconcile run (storage scan, manifest sweep) have different gating
// rules depending on the storage backend topology:
//
//   - Shared storage (S3, Azure, MinIO): one node sweeps the bucket. Both
//     halves gate on IsActiveCompactor — reuses the failover-managed
//     compactor lease as a single-sweeper election with no new state.
//   - Local storage: every node walks its own disks; the per-file
//     OriginNodeID filter inside the reconciler handles scoping.
//     BatchFileOpsInManifest leader-forwards on its own, so the
//     manifest-sweep gate is also "always".
//
// This type lives in main.go so the reconciliation package has no
// compile-time dependency on the cluster package.
type reconciliationClusterGate struct {
	coordinator *cluster.Coordinator
	shared      bool // true for s3/azure backends — one-node-sweeps semantics
}

func newReconciliationClusterGate(c *cluster.Coordinator, backendKind string) *reconciliationClusterGate {
	return &reconciliationClusterGate{
		coordinator: c,
		shared:      isSharedBackend(backendKind),
	}
}

func (g *reconciliationClusterGate) ShouldRunStorageScan() bool {
	if g.shared {
		return g.coordinator.IsActiveCompactor()
	}
	return true
}

func (g *reconciliationClusterGate) ShouldRunManifestSweep() bool {
	if g.shared {
		return g.coordinator.IsActiveCompactor()
	}
	return true
}

func (g *reconciliationClusterGate) Role() string {
	return string(g.coordinator.GetRole())
}

// reconciliationBackendKind maps the config storage backend string to
// the reconciliation package's typed BackendKind.
func reconciliationBackendKind(backend string) reconciliation.BackendKind {
	if isSharedBackend(backend) {
		return reconciliation.BackendShared
	}
	return reconciliation.BackendLocal
}

// isSharedBackend reports whether the configured storage backend is
// shared across cluster nodes (one source of truth on disk).
func isSharedBackend(backend string) bool {
	switch backend {
	case "s3", "minio", "azure":
		return true
	default:
		return false
	}
}
