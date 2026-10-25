package database

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// Each field that makes configureDatabase load httpfs or azure for storage
// makes the node a possible remote reader, alone (#1372).
func TestMayReadRemoteStorage(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  Config
		want bool
	}{
		{"local only", Config{LocalStorageRoot: "/data"}, false},
		{"primary bucket named, backend local", Config{S3Bucket: "arc"}, false},
		{"primary container named, backend local", Config{AzureContainer: "arc"}, false},
		{"S3 primary", Config{S3IsPrimaryBackend: true}, true},
		{"S3 access key", Config{S3AccessKey: "k"}, true},
		{"S3 secret key", Config{S3SecretKey: "s"}, true},
		{"cold S3 bucket", Config{ColdS3Bucket: "cold"}, true},
		{"Azure primary", Config{AzureIsPrimaryBackend: true}, true},
		{"cold Azure container", Config{ColdAzureContainer: "cold"}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := mayReadRemoteStorage(&c.cfg); got != c.want {
				t.Fatalf("mayReadRemoteStorage = %v, want %v", got, c.want)
			}
		})
	}
}

func newFileCacheDB(t *testing.T, cfg Config) (*DuckDB, error) {
	t.Helper()
	tmp := t.TempDir()
	root := filepath.Join(tmp, "data")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cfg.MaxConnections, cfg.MemoryLimit, cfg.LocalStorageRoot, cfg.TempDirectory = 2, "256MB", root, tmp
	db, err := New(&cfg, zerolog.Nop())
	if err == nil {
		t.Cleanup(func() { db.Close() })
	}
	return db, err
}

// The setting DuckDB ends up with, read from DuckDB on a pool connection,
// matches the mode and the node's storage, and ExternalFileCacheEnabled
// reports it.
func TestExternalFileCacheSetting(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  Config
		want bool
	}{
		{"auto (empty), local only", Config{}, false},
		{"auto, local only", Config{ExternalFileCache: "auto"}, false},
		{"auto, cold S3 bucket", Config{ExternalFileCache: "auto", ColdS3Bucket: "cold"}, true},
		{"auto, S3 primary", Config{ExternalFileCache: "auto", S3IsPrimaryBackend: true, S3Bucket: "arc", S3Region: "us-east-1", S3AccessKey: "k", S3SecretKey: "s"}, true},
		{"auto, Azure primary", Config{ExternalFileCache: "auto", AzureIsPrimaryBackend: true, AzureContainer: "arc", AzureAccountName: "acct", AzureAccountKey: "a2V5"}, true},
		{"true, local only", Config{ExternalFileCache: "true"}, true},
		{"false, cold S3 bucket", Config{ExternalFileCache: "false", ColdS3Bucket: "cold"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, err := newFileCacheDB(t, c.cfg)
			if err != nil {
				if mayReadRemoteStorage(&c.cfg) && (strings.Contains(err.Error(), "httpfs") || strings.Contains(err.Error(), "azure")) {
					t.Skipf("extension unavailable (offline?): %v", err)
				}
				t.Fatalf("New: %v", err)
			}
			var got bool
			if err := db.DB().QueryRow("SELECT current_setting('enable_external_file_cache')::BOOLEAN").Scan(&got); err != nil {
				t.Fatalf("read setting: %v", err)
			}
			if got != c.want {
				t.Fatalf("enable_external_file_cache = %v, want %v", got, c.want)
			}
			if on, known := db.ExternalFileCacheEnabled(); !known || on != got {
				t.Fatalf("ExternalFileCacheEnabled() = %v (known %v), DuckDB says %v", on, known, got)
			}
		})
	}
}

func TestExternalFileCacheRejectsUnknownMode(t *testing.T) {
	_, err := newFileCacheDB(t, Config{ExternalFileCache: "sometimes"})
	if err == nil || !strings.Contains(err.Error(), "database.external_file_cache") {
		t.Fatalf("New with an unknown mode: err = %v, want a database.external_file_cache error", err)
	}
}

// ClearHTTPCache runs after every compaction job on every node, so it must
// leave the file cache alone: on a node that may read S3 or Azure it is the
// remote read cache. A flush (SET false, then true) would end with the same
// setting, so the test reads the cache's bytes, not only the setting.
func TestClearHTTPCacheLeavesTheFileCache(t *testing.T) {
	for _, mode := range []string{"true", "false"} {
		t.Run(mode, func(t *testing.T) {
			db, err := newFileCacheDB(t, Config{ExternalFileCache: mode})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			f := filepath.Join(db.config.LocalStorageRoot, "x.parquet")
			if _, err := db.DB().Exec("COPY (SELECT range AS v FROM range(100000)) TO '" + escapeSQLString(f) + "' (FORMAT parquet)"); err != nil {
				t.Fatalf("write parquet: %v", err)
			}
			if _, err := db.DB().Exec("SELECT sum(v) FROM read_parquet('" + escapeSQLString(f) + "')"); err != nil {
				t.Fatalf("read parquet: %v", err)
			}
			cached := func() int64 {
				var b int64
				if err := db.DB().QueryRow("SELECT coalesce(sum(memory_usage_bytes), 0) FROM duckdb_memory() WHERE tag = 'EXTERNAL_FILE_CACHE'").Scan(&b); err != nil {
					t.Fatalf("duckdb_memory: %v", err)
				}
				return b
			}
			before := cached()
			if (mode == "true") != (before > 0) {
				t.Fatalf("EXTERNAL_FILE_CACHE before ClearHTTPCache = %d bytes with mode %s", before, mode)
			}
			db.ClearHTTPCache()
			if after := cached(); after < before {
				t.Fatalf("ClearHTTPCache freed the file cache: %d -> %d bytes", before, after)
			}
			var got bool
			if err := db.DB().QueryRow("SELECT current_setting('enable_external_file_cache')::BOOLEAN").Scan(&got); err != nil {
				t.Fatalf("read setting: %v", err)
			}
			if want := mode == "true"; got != want {
				t.Fatalf("after ClearHTTPCache enable_external_file_cache = %v, want %v", got, want)
			}
		})
	}
}
