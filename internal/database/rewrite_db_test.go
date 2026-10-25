package database

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestParseDuckDBSize(t *testing.T) {
	for in, want := range map[string]int64{
		"3.5 GiB":   7 << 29,
		"512.0 MiB": 512 << 20,
		"4GB":       4e9,
		"100 KiB":   100 << 10,
		"0 bytes":   0,
		"1.5 TiB":   3 << 39,
	} {
		got, err := ParseDuckDBSize(in)
		if err != nil || got != want {
			t.Errorf("%q: %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "GiB", "3.7 parsecs", "-1 GB"} {
		if _, err := ParseDuckDBSize(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

// With the defaults since #1026 Config leaves both empty; the limits must
// come from DuckDB, never read as zero.
func TestEffectiveLimitsWithDefaultConfig(t *testing.T) {
	d, err := New(&Config{MaxConnections: 1, LocalStorageRoot: t.TempDir()}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	mem, threads, err := d.EffectiveLimits(context.Background())
	if err != nil || mem <= 0 || threads <= 0 {
		t.Fatalf("memory %d, threads %d, %v", mem, threads, err)
	}
	d2, err := New(&Config{MaxConnections: 1, MemoryLimit: "300MB", ThreadCount: 3, LocalStorageRoot: t.TempDir()}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	mem, threads, err = d2.EffectiveLimits(context.Background())
	// DuckDB reports 300 MB as "286.1 MiB".
	if err != nil || mem < 299e6 || mem > 301e6 || threads != 3 {
		t.Fatalf("memory %d, threads %d, %v; want ~300 MB and 3", mem, threads, err)
	}
}

func TestOpenRewriteDBIsPrivateSingleThreadedAndSandboxed(t *testing.T) {
	allowed := t.TempDir()
	spill := filepath.Join(allowed, "spill")
	if err := os.Mkdir(spill, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	for _, dir := range []string{allowed, outside} {
		if err := os.WriteFile(filepath.Join(dir, "x.csv"), []byte("a\n1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	db, err := OpenRewriteDB(ctx, RewriteDBOptions{MemoryLimitBytes: 300 << 20, SpillDir: spill, AllowedDirs: []string{allowed}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var threads int64
	var mem, temp, tz string
	var order, fileCache bool
	if err := db.QueryRow(`SELECT current_setting('threads'), current_setting('memory_limit'), current_setting('temp_directory'),
		current_setting('TimeZone'), current_setting('preserve_insertion_order'), current_setting('enable_external_file_cache')`).
		Scan(&threads, &mem, &temp, &tz, &order, &fileCache); err != nil {
		t.Fatal(err)
	}
	if threads != 1 || temp != spill || tz != "UTC" || !order || fileCache {
		t.Fatalf("threads %d, temp_directory %q, TimeZone %q, preserve_insertion_order %v, file cache %v", threads, temp, tz, order, fileCache)
	}
	if got, _ := ParseDuckDBSize(mem); got != 300<<20 {
		t.Fatalf("memory_limit %q", mem)
	}
	var autoload, autoinstall bool
	if err := db.QueryRow("SELECT current_setting('autoload_known_extensions'), current_setting('autoinstall_known_extensions')").Scan(&autoload, &autoinstall); err != nil || autoload || autoinstall {
		t.Fatalf("autoload %v, autoinstall %v, %v", autoload, autoinstall, err)
	}
	// The zone holds on a new connection too, not only the one configured.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	conn.Close()
	if err := db.QueryRow("SELECT current_setting('TimeZone')").Scan(&tz); err != nil || tz != "UTC" {
		t.Fatalf("TimeZone on a new connection %q, %v", tz, err)
	}

	var n int
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_csv('%s')", filepath.Join(allowed, "x.csv"))).Scan(&n); err != nil || n != 1 {
		t.Fatalf("allowed file: %d, %v", n, err)
	}
	err = db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_csv('%s')", filepath.Join(outside, "x.csv"))).Scan(&n)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "permission") {
		t.Fatalf("file outside the allowlist: %v", err)
	}
	if _, err := db.Exec("SET enable_external_access=true"); err == nil {
		t.Fatal("sandbox could be lifted")
	}
}

func TestOpenRewriteDBRefusesWithoutSpillOrAllowlist(t *testing.T) {
	for _, o := range []RewriteDBOptions{{SpillDir: t.TempDir()}, {AllowedDirs: []string{t.TempDir()}}} {
		if db, err := OpenRewriteDB(context.Background(), o, zerolog.Nop()); err == nil {
			db.Close()
			t.Fatalf("opened with %+v", o)
		}
	}
}

// The rewrite instance loads only a supported extension's bare name, the
// same rule as the main instance (#440): never a path.
func TestOpenRewriteDBRefusesAnUnsupportedExtension(t *testing.T) {
	dir := t.TempDir()
	o := RewriteDBOptions{SpillDir: dir, AllowedDirs: []string{dir}, Extensions: []string{filepath.Join(dir, "x.duckdb_extension")}}
	db, err := OpenRewriteDB(context.Background(), o, zerolog.Nop())
	if err == nil {
		db.Close()
		t.Fatal("opened with an extension path")
	}
	if !strings.Contains(err.Error(), "not a supported DuckDB extension") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}
