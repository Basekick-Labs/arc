package database

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// This file owns Arc's DuckDB sandbox: an `enable_external_access=false`
// lockdown scoped to a per-deployment `allowed_directories` list. Closes
// the I/O-function denylist bypass from the 2026-05-19 security audit
// (read_csv_auto, read_json, read_text, glob, etc. were callable by any
// authenticated token against arbitrary local files).
//
// Strategy:
//  1. Enumerate every directory prefix Arc legitimately needs at query time
//     (local storage root, DuckDB spill dir, dedicated import-upload subdir,
//     compaction temp dir, S3/Azure bucket+prefix for hot AND cold tiers).
//  2. SET GLOBAL allowed_directories = [...]
//  3. SET GLOBAL enable_external_access = false  (one-way, cannot be undone)
//  4. Verify the flip actually took effect.
//
// Every INSTALL / LOAD Arc needs MUST have completed before step 3. After
// step 3, DuckDB rejects further INSTALL/LOAD. Already-loaded extensions
// (httpfs, azure, cache_httpfs, arcx) remain fully callable because
// enable_external_access is checked at LOAD time, not at function-invocation
// time. File access checks happen at file-open time against the current
// allowed_directories.

// sandboxLockdownTimeout bounds the SET GLOBAL pair plus the read-back
// verification at startup. Five seconds is generous to cover transient
// pool contention while still bounding a hung DuckDB so startup fails
// rather than blocking indefinitely. Applied via a single context that
// spans the SET / SET / SELECT sequence so the budget is shared.
const sandboxLockdownTimeout = 5 * time.Second

// lockdownExternalAccess seals DuckDB's filesystem access surface so that
// user-supplied SQL cannot reach arbitrary local files or remote URLs.
// Called once at the end of configureDatabase, after every INSTALL/LOAD
// and every SET GLOBAL Arc needs to perform.
func lockdownExternalAccess(db *sql.DB, cfg *Config, logger zerolog.Logger) error {
	componentLogger := logger.With().Str("component", "duckdb-sandbox").Logger()
	dirs := buildAllowedDirectories(cfg)

	// Empty allowlist + enable_external_access=false locks DuckDB out of
	// every file it might legitimately need (spill, profile output, manifest
	// reads). Warn loudly so misconfigured deployments fail fast and visibly
	// rather than silently breaking every query. The Warn enumerates every
	// Config field that contributes to the allowlist so operators see exactly
	// which knob is unset.
	if len(dirs) == 0 {
		componentLogger.Warn().Msg("sandbox allowlist is empty — every file-touching query will fail; check Config.LocalStorageRoot / TempDirectory / UploadDir / CompactionTempDirectory / S3Bucket / ColdS3Bucket / AzureContainer / ColdAzureContainer wiring")
	}

	ctx, cancel := context.WithTimeout(context.Background(), sandboxLockdownTimeout)
	defer cancel()

	// Compose the allowed_directories SET. DuckDB accepts a list literal.
	// Each entry is single-quoted; embedded single quotes are doubled by
	// escapeSQLString. Local paths were already forward-slashed in
	// buildAllowedDirectories via withTrailingSlash; S3 and Azure URIs use
	// forward slashes natively.
	quoted := make([]string, len(dirs))
	for i, d := range dirs {
		quoted[i] = "'" + escapeSQLString(d) + "'"
	}
	setDirs := "SET GLOBAL allowed_directories = [" + strings.Join(quoted, ", ") + "]"
	if _, err := db.ExecContext(ctx, setDirs); err != nil {
		return fmt.Errorf("set allowed_directories: %w", err)
	}

	if _, err := db.ExecContext(ctx, "SET GLOBAL enable_external_access = false"); err != nil {
		return fmt.Errorf("set enable_external_access=false: %w", err)
	}

	// Read-back guard: a future DuckDB release could reject the flip silently
	// (e.g., behind a feature flag), or a misconfiguration could leave the
	// setting in a state we did not expect. Verify the flag actually flipped
	// before declaring the sandbox active.
	var got bool
	if err := db.QueryRowContext(ctx, "SELECT current_setting('enable_external_access')::BOOLEAN").Scan(&got); err != nil {
		return fmt.Errorf("read back enable_external_access: %w", err)
	}
	if got {
		return fmt.Errorf("sandbox lockdown did not take effect: enable_external_access is still true")
	}

	componentLogger.Info().Strs("allowed_directories", dirs).Msg("DuckDB external access locked down (sandbox active)")
	return nil
}

// buildAllowedDirectories enumerates every directory prefix Arc legitimately
// needs DuckDB to read from or write to after lockdown. Returns trailing-
// slashed, forward-slash entries ready to interpolate into the
// `allowed_directories` SET. Order is informational only — DuckDB matches
// by prefix in any order.
//
// Paths are passed through verbatim by this function; the caller in main.go
// is responsible for absolute-resolution before populating Config. Empty
// cfg fields are skipped so test fixtures can construct a minimal Config
// without producing nonsensical allowlist entries.
func buildAllowedDirectories(cfg *Config) []string {
	dirs := make([]string, 0, 8)

	if cfg.LocalStorageRoot != "" {
		dirs = append(dirs, withTrailingSlash(cfg.LocalStorageRoot))
	}
	if cfg.TempDirectory != "" {
		dirs = append(dirs, withTrailingSlash(cfg.TempDirectory))
	}
	// Dedicated import-upload directory (also used by the DELETE handler for
	// S3-rewrite staging). Narrower than os.TempDir(): only this single
	// directory is reachable, so other processes' /tmp files stay outside
	// the DuckDB sandbox. main.go creates it under TempDirectory at startup.
	if cfg.UploadDir != "" {
		dirs = append(dirs, withTrailingSlash(cfg.UploadDir))
	}
	// Compaction's own temp directory (cfg.Compaction.TempDirectory, default
	// ./data/compaction). Today compaction runs in a subprocess (see
	// internal/compaction/subprocess.go) that opens its OWN DuckDB without
	// going through configureDatabase, so the subprocess is not subject to
	// this sandbox and does not need this entry. Allowlisting it anyway is
	// defensive: any future parent-side code path that COPY-rewrites parquet
	// to this dir (e.g., a refactor moving compaction back in-process) would
	// otherwise fail post-lockdown with a confusing permission error.
	if cfg.CompactionTempDirectory != "" {
		dirs = append(dirs, withTrailingSlash(cfg.CompactionTempDirectory))
	}
	// Primary S3 backend (when storage.backend = "s3"). The query rewriter
	// emits read_parquet('s3://<bucket>/<prefix>/...') for both ingest reads
	// and queries.
	var hotS3 string
	if cfg.S3Bucket != "" {
		hotS3 = s3PrefixURI(cfg.S3Bucket, cfg.S3Prefix)
		dirs = append(dirs, hotS3)
	}
	// Cold-tier S3 (Enterprise tiered storage). internal/tiering/router.go
	// emits read_parquet('s3://<cold-bucket>/<cold-prefix>/...'). The hot=local
	// + cold=S3 topology is canonical, but operators ALSO run hot=S3 with
	// shared-bucket-different-prefix cold tiers (e.g. bucket=warehouse with
	// hot/ and cold/ prefixes). Dedupe on the FULL URI, not just the bucket
	// name — a same-bucket-different-prefix cold tier MUST get its own
	// allowlist entry.
	if cfg.ColdS3Bucket != "" {
		coldS3 := s3PrefixURI(cfg.ColdS3Bucket, cfg.ColdS3Prefix)
		if coldS3 != hotS3 {
			dirs = append(dirs, coldS3)
		}
	}
	// Primary Azure backend (when storage.backend = "azure"). Arc's config
	// does not currently expose a per-deployment Azure key prefix, so the
	// allowlist scopes to the whole container.
	var hotAzure string
	if cfg.AzureContainer != "" {
		hotAzure = "azure://" + cfg.AzureContainer + "/"
		dirs = append(dirs, hotAzure)
	}
	// Cold-tier Azure (Enterprise). Mirrors the S3 cold-tier full-URI dedupe.
	if cfg.ColdAzureContainer != "" {
		coldAzure := "azure://" + cfg.ColdAzureContainer + "/"
		if coldAzure != hotAzure {
			dirs = append(dirs, coldAzure)
		}
	}
	return dirs
}

// withTrailingSlash normalises a non-empty directory-like path to a single
// trailing forward slash. ToSlash is folded in so callers don't need to
// remember to pre-normalize. Callers gate on the empty-string case before
// calling, so this helper assumes non-empty input.
func withTrailingSlash(p string) string {
	p = filepath.ToSlash(p)
	if strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

// s3PrefixURI builds a normalized "s3://<bucket>/<prefix>/" allowlist entry.
// Uses path.Clean (NOT filepath.Clean — the latter rewrites slashes on
// Windows) to collapse interior runs of slashes (`tenant-a//sub` →
// `tenant-a/sub`) AND to canonicalise `.` segments. Leading slashes are
// stripped via TrimLeft before Clean so `//tenant-a` becomes `tenant-a`.
//
// `..` segments are rejected outright: path.Clean preserves a leading `..`
// (Clean("..") → ".." and Clean("../foo") → "../foo"), so without an
// explicit reject the helper would emit nonsensical entries like
// `s3://bucket/../`. An operator that supplies `..` is misconfigured;
// fall back to the bare-bucket prefix and let DuckDB reject the actual
// read at query time with a meaningful error.
//
// Always trailing-slashed so DuckDB's prefix matcher treats the entry as
// a directory.
func s3PrefixURI(bucket, prefix string) string {
	prefix = strings.TrimLeft(prefix, "/")
	if prefix == "" {
		return "s3://" + bucket + "/"
	}
	prefix = path.Clean(prefix)
	// path.Clean leaves "." (when input was "./" or "."), and preserves any
	// leading ".." segments. Both shapes are operator-misconfiguration; the
	// safe fallback is the bare bucket. A query that legitimately needs an
	// out-of-prefix path will surface a clearer permission error than
	// shipping a nonsensical allowlist entry.
	if prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") {
		return "s3://" + bucket + "/"
	}
	return "s3://" + bucket + "/" + prefix + "/"
}

// s3SecretScope builds the SCOPE prefix for a DuckDB S3 secret. It reuses
// s3PrefixURI so the secret's scope matches the sandbox allowlist entry exactly
// (same bucket/prefix normalization), but returns "" when no bucket is
// configured so the caller emits an UNSCOPED secret rather than a bogus
// "s3:///" scope.
func s3SecretScope(bucket, prefix string) string {
	if bucket == "" {
		return ""
	}
	return s3PrefixURI(bucket, prefix)
}
