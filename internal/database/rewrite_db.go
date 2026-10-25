package database

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

// EffectiveLimits reports the memory limit (bytes) and thread count the main
// DuckDB instance is running with. They are read back from DuckDB rather than
// taken from Config: since #1026 both default to empty/0 in Config, and DuckDB
// then picks its own cgroup-aware values, which only current_setting shows.
func (d *DuckDB) EffectiveLimits(ctx context.Context) (memoryBytes int64, threads int, err error) {
	var mem string
	var th int64
	if err := d.db.QueryRowContext(ctx, "SELECT current_setting('memory_limit'), current_setting('threads')").Scan(&mem, &th); err != nil {
		return 0, 0, fmt.Errorf("read effective memory_limit and threads: %w", err)
	}
	memoryBytes, err = ParseDuckDBSize(mem)
	if err != nil {
		return 0, 0, err
	}
	return memoryBytes, int(th), nil
}

// ParseDuckDBSize parses a size as DuckDB prints it from
// current_setting('memory_limit'), e.g. "3.7 GiB" or "512.0 MiB". Decimal
// units (KB, MB, GB, TB) are accepted too.
func ParseDuckDBSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	i := 0
	for i < len(t) && (t[i] == '.' || (t[i] >= '0' && t[i] <= '9')) {
		i++
	}
	n, err := strconv.ParseFloat(t[:i], 64)
	if err != nil || i == 0 {
		return 0, fmt.Errorf("parse DuckDB size %q", s)
	}
	units := map[string]float64{
		"": 1, "B": 1, "BYTES": 1,
		"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
		"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "TIB": 1 << 40,
	}
	m, ok := units[strings.ToUpper(strings.TrimSpace(t[i:]))]
	if !ok {
		return 0, fmt.Errorf("parse DuckDB size %q: unknown unit", s)
	}
	return int64(n * m), nil
}

// RewriteDBOptions configures OpenRewriteDB.
type RewriteDBOptions struct {
	// MemoryLimitBytes is the private instance's memory_limit.
	MemoryLimitBytes int64
	// SpillDir is the private instance's temp_directory. It MUST NOT be the
	// main instance's: two instances in one process name their spill files
	// alike from 0, overwrite each other's and the one that closes first
	// deletes the other's. The caller creates it and removes it.
	SpillDir string
	// AllowedDirs are the only directories the private instance can read or
	// write once locked down: the source file's, the output's and SpillDir.
	AllowedDirs []string
	// Extensions are loaded, never installed, before the lockdown: the
	// main instance's database.extensions (#440), so a WHERE that uses one
	// runs on both instances. The main instance installed them.
	Extensions []string
}

// OpenRewriteDB opens a private, single-threaded, sandboxed DuckDB for one
// memory-heavy parquet rewrite (a DELETE of a very wide file).
//
// Why a separate instance: a parquet COPY holds, per thread in flight, a
// decoded row group (columns x rows x ~9 B, however sparse; the source's and
// the one being written) and the writer's per-column state (~150 KB per
// column), part of it outside DuckDB's memory_limit. On a 46-core node a 3,100-column file
// therefore runs the process out of memory. The only lever is the thread
// count, which DuckDB sets per instance, never per session.
//
// The instance has no secrets and no remote access: the caller brings the
// source to local disk. Of the main instance's extensions it loads only the
// operator's database.extensions (not arcx, not httpfs). The user's WHERE clause runs in it, so the
// same lockdown as the main instance applies, with a narrower allowlist.
func OpenRewriteDB(ctx context.Context, opts RewriteDBOptions, logger zerolog.Logger) (*sql.DB, error) {
	if opts.SpillDir == "" || len(opts.AllowedDirs) == 0 {
		return nil, fmt.Errorf("rewrite database needs a spill directory and an allowlist")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open rewrite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	settings := []string{
		// Same UTC contract as the main instance (#682): the DELETE WHERE
		// compares naive literals against time, and the main instance counted
		// the rows it removes in UTC. GLOBAL: a plain SET is session-scoped
		// and a reconnect would read in the host's zone.
		"SET GLOBAL TimeZone='UTC'",
		"SET threads=1",
		fmt.Sprintf("SET memory_limit='%dB'", opts.MemoryLimitBytes),
		fmt.Sprintf("SET temp_directory='%s'", escapeSQLString(opts.SpillDir)),
		// One thread keeps the source order anyway; set so a DuckDB default
		// change cannot reorder the rewritten file.
		"SET preserve_insertion_order=true",
		// The file is read once; caching its blocks only costs memory.
		"SET enable_external_file_cache=false",
		// No extension is loaded or installed on demand: the WHERE is the
		// user's, and the sandbox should not rest on the extension directory
		// being outside the allowlist.
		"SET autoload_known_extensions=false",
		"SET autoinstall_known_extensions=false",
	}
	for _, s := range settings {
		if _, err := db.ExecContext(ctx, s); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure rewrite database (%s): %w", s, err)
		}
	}
	if err := validateExtensions(opts.Extensions); err != nil {
		db.Close()
		return nil, err
	}
	for _, name := range opts.Extensions {
		if _, err := db.ExecContext(ctx, "LOAD "+quoteDuckDBIdent(name)); err != nil {
			db.Close()
			return nil, fmt.Errorf("load DuckDB extension %q in the rewrite database: %w", name, err)
		}
	}
	// lockdownDirs logs the allowlist at Info, which reads like a database
	// restart when it happens per DELETE: only its warnings are kept, and the
	// allowlist goes to Debug below.
	dirs := make([]string, len(opts.AllowedDirs))
	for i, d := range opts.AllowedDirs {
		dirs[i] = withTrailingSlash(d)
	}
	if err := lockdownDirs(ctx, db, dirs, logger.Level(zerolog.WarnLevel)); err != nil {
		db.Close()
		return nil, err
	}
	logger.Debug().Strs("allowed_directories", dirs).Int64("memory_limit_bytes", opts.MemoryLimitBytes).Msg("Opened single-threaded rewrite database")
	return db, nil
}
