package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

// TestDatabaseThreadCountDefaultsToZero pins that Arc does not set DuckDB's
// thread count.
//
// DuckDB reads cpu.max and so gets the container's CPU quota. Arc used to
// override that with runtime.NumCPU(), which reflects cpuset/affinity but NOT a
// CFS quota — so a 2-CPU pod on a 64-core host produced SET GLOBAL threads=64
// (#1026). Zero means configureDatabase skips the SET entirely and DuckDB's own
// value stands. The licensed-core cap in main.go sets this explicitly and is
// unaffected.
func TestDatabaseThreadCountDefaultsToZero(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	if got := v.GetInt("database.thread_count"); got != 0 {
		t.Errorf("database.thread_count default = %d, want 0 so DuckDB's cgroup-aware value is left alone", got)
	}
}

// TestEffectiveCores covers the helper every quota-derived default is built on.
//
// Table-driven over the pure form rather than driven through
// runtime.GOMAXPROCS(n): that is process-global, would slow every other test in
// the binary, and the values worth testing (0, 128 on a smaller box) are ones a
// test has no business installing process-wide.
func TestEffectiveCores(t *testing.T) {
	cases := []struct {
		name           string
		numCPU, gomaxp int
		want           int
	}{
		// The ordinary container case: NumCPU cannot see the CFS quota, GOMAXPROCS
		// can. This row is #1030.
		{"quota below machine", 64, 2, 2},
		{"no quota", 8, 8, 8},
		{"cpuset only", 2, 2, 2},
		// GOMAXPROCS env has no clamp in the runtime, so it can exceed the machine.
		// Verified: GOMAXPROCS=128 on an 8-CPU box reports 128.
		{"gomaxprocs raised above machine", 8, 128, 8},
		// An operator-raised GOMAXPROCS below the machine size is honoured. A
		// deliberate residual, documented on EffectiveCores.
		{"gomaxprocs between quota and machine", 64, 32, 32},
		// Degenerate inputs must not yield 0 — a 0 would make the compaction
		// threads default 0, which means "unset" to the subprocess.
		{"zero gomaxprocs", 8, 0, 8},
		{"zero both", 0, 0, 1},
		{"negative", -1, -1, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := effectiveCores(c.numCPU, c.gomaxp); got != c.want {
				t.Errorf("effectiveCores(%d, %d) = %d, want %d", c.numCPU, c.gomaxp, got, c.want)
			}
		})
	}
}

// TestEffectiveCores_MatchesRuntime pins that the exported form reads both
// runtime values rather than only one of them.
func TestEffectiveCores_MatchesRuntime(t *testing.T) {
	want := effectiveCores(runtime.NumCPU(), runtime.GOMAXPROCS(0))
	if got := EffectiveCores(); got != want {
		t.Errorf("EffectiveCores() = %d, want %d", got, want)
	}
}

func TestDefaultCompactionThreads(t *testing.T) {
	cases := []struct{ cores, want int }{
		{1, 1}, // floor: never 0, which the subprocess reads as "unset"
		{2, 1}, // the headline 2-CPU pod
		{3, 1},
		{8, 4},
		{64, 32},
		{0, 1},
	}
	for _, c := range cases {
		if got := defaultCompactionThreads(c.cores); got != c.want {
			t.Errorf("defaultCompactionThreads(%d) = %d, want %d", c.cores, got, c.want)
		}
	}
}

func TestDefaultMaxConnections(t *testing.T) {
	cases := []struct{ cores, want int }{
		{1, 4}, {2, 4}, {8, 16}, {64, 64}, {128, 64},
	}
	for _, c := range cases {
		if got := defaultMaxConnections(c.cores); got != c.want {
			t.Errorf("defaultMaxConnections(%d) = %d, want %d", c.cores, got, c.want)
		}
	}
}

// TestGetDefaultMaxConnections_StaysMachineDerived pins the #1030 decision that
// max_connections is NOT quota-derived: it bounds statements in flight, not CPU
// work, and Arc queries are frequently S3-I/O-bound, so a 2-CPU pod dropping
// from 64 pool slots to 4 would convert concurrency into client timeouts.
func TestGetDefaultMaxConnections_StaysMachineDerived(t *testing.T) {
	if got, want := getDefaultMaxConnections(), defaultMaxConnections(runtime.NumCPU()); got != want {
		t.Errorf("getDefaultMaxConnections() = %d, want %d (machine cores, deliberately not the CPU quota)", got, want)
	}
	if got := getDefaultMaxConnections(); got < 4 || got > 64 {
		t.Errorf("getDefaultMaxConnections() = %d, want within 4..64", got)
	}
}

// TestDatabaseMemoryLimitDefaultsToEmpty pins that Arc does not set DuckDB's
// memory limit either.
//
// Empty does NOT mean unlimited: DuckDB performs its own cgroup-aware detection
// and defaults to 80% of what it finds (measured — 28.7 GiB on a 36 GiB host,
// 409.5 MiB in a --memory=512m container). Arc used to overwrite that with
// min(NumCPU, 32) GB, which is 32 GB inside a 2Gi pod on a big host and 14 GB on
// a 36 GiB workstation: wrong in both directions depending on memory-per-core.
func TestDatabaseMemoryLimitDefaultsToEmpty(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	if got := v.GetString("database.memory_limit"); got != "" {
		t.Errorf("database.memory_limit default = %q, want \"\" so DuckDB's cgroup-aware value is left alone", got)
	}
}

func TestDefaultFlushWorkers(t *testing.T) {
	cases := []struct{ cores, want int }{
		// The floor is why a quota-derived value would have been a no-op for any
		// quota of 4 cores or fewer: 2 cores and 4 cores both yield 8.
		{1, 8}, {2, 8}, {4, 8}, {8, 16}, {64, 64}, {128, 64},
	}
	for _, c := range cases {
		if got := defaultFlushWorkers(c.cores); got != c.want {
			t.Errorf("defaultFlushWorkers(%d) = %d, want %d", c.cores, got, c.want)
		}
	}
}

// TestGetDefaultFlushWorkers_StaysMachineDerived pins the #1030 decision that
// flush_workers is NOT quota-derived. Measured, ABAB, with the Go path held to
// 2 cores and the same offered load per arm: against a 500ms-per-upload sink, 8
// workers pushed 61% as many rows to storage as 64 did over the same window,
// both arms at their upload-concurrency ceiling; against a 1ms sink the two were
// indistinguishable and both ingest-limited. The pool is bound by concurrent
// uploads, not by cores.
func TestGetDefaultFlushWorkers_StaysMachineDerived(t *testing.T) {
	if got, want := getDefaultFlushWorkers(), defaultFlushWorkers(runtime.NumCPU()); got != want {
		t.Errorf("getDefaultFlushWorkers() = %d, want %d (machine cores, deliberately not the CPU quota)", got, want)
	}
	if got := getDefaultFlushWorkers(); got < 8 || got > 64 {
		t.Errorf("getDefaultFlushWorkers() = %d, want within 8..64", got)
	}
}

// TestGetDefaultFlushQueueSize_FollowsWorkers keeps the wiring assertion the
// pure table above cannot make: the queue is sized from the RESOLVED worker
// count, so a change to one default moves the other.
func TestGetDefaultFlushQueueSize_FollowsWorkers(t *testing.T) {
	if got, want := getDefaultFlushQueueSize(), defaultFlushQueueSize(getDefaultFlushWorkers()); got != want {
		t.Errorf("getDefaultFlushQueueSize() = %d, want %d (4x the resolved worker count, floor 100)", got, want)
	}
	if got := getDefaultFlushQueueSize(); got < 100 {
		t.Errorf("getDefaultFlushQueueSize() = %d, want at least 100", got)
	}
}

func TestDefaultFlushQueueSize(t *testing.T) {
	cases := []struct{ workers, want int }{
		{8, 100}, {24, 100}, {25, 100}, {26, 104}, {64, 256},
	}
	for _, c := range cases {
		if got := defaultFlushQueueSize(c.workers); got != c.want {
			t.Errorf("defaultFlushQueueSize(%d) = %d, want %d", c.workers, got, c.want)
		}
	}
}

func TestLoad_DefaultsFromSystem(t *testing.T) {
	// Create a temp dir without config file to test defaults
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Change to temp dir so no config file is found
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify dynamic defaults are applied
	if cfg.Database.ThreadCount != 0 {
		t.Errorf("Database.ThreadCount = %d, want 0 (DuckDB's own cgroup-aware value, #1026)", cfg.Database.ThreadCount)
	}

	expectedConns := getDefaultMaxConnections()
	if cfg.Database.MaxConnections != expectedConns {
		t.Errorf("Database.MaxConnections = %d, want %d", cfg.Database.MaxConnections, expectedConns)
	}

	if cfg.Database.MemoryLimit != "" {
		t.Errorf("Database.MemoryLimit = %q, want \"\" (DuckDB's own cgroup-aware value, #1026)", cfg.Database.MemoryLimit)
	}

	// Ingest dictionary defaults (26.09.1): no dictionary encoding at ingest —
	// compaction re-encodes via DuckDB anyway. A silent revert of either
	// SetDefault would ship unnoticed otherwise: every ingest test constructs
	// IngestConfig by hand and the bool zero values coincide with these.
	if cfg.Ingest.UseDictionary {
		t.Error("Ingest.UseDictionary default = true, want false (26.09.1 default flip)")
	}
	if cfg.Ingest.NumericDictionary {
		t.Error("Ingest.NumericDictionary default = true, want false")
	}

	// preserve_insertion_order (26.09.1): SQL-standard unordered results.
	if cfg.Database.PreserveInsertionOrder {
		t.Error("Database.PreserveInsertionOrder default = true, want false (26.09.1 default flip)")
	}

	// Verify ingest defaults are applied
	expectedFlushWorkers := getDefaultFlushWorkers()
	if cfg.Ingest.FlushWorkers != expectedFlushWorkers {
		t.Errorf("Ingest.FlushWorkers = %d, want %d", cfg.Ingest.FlushWorkers, expectedFlushWorkers)
	}

	expectedFlushQueueSize := getDefaultFlushQueueSize()
	if cfg.Ingest.FlushQueueSize != expectedFlushQueueSize {
		t.Errorf("Ingest.FlushQueueSize = %d, want %d", cfg.Ingest.FlushQueueSize, expectedFlushQueueSize)
	}

	if cfg.Ingest.ShardCount != 32 {
		t.Errorf("Ingest.ShardCount = %d, want 32", cfg.Ingest.ShardCount)
	}
}

func TestLoad_BackupConfigPopulated(t *testing.T) {
	// Regression: cfg.Backup was never populated in Load(), so the backup API never enabled
	// regardless of the [backup] section. Assert defaults land and env overrides apply.
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Defaults: enabled=true, local_path set.
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if !cfg.Backup.Enabled {
		t.Error("Backup.Enabled = false, want true (default)")
	}
	if cfg.Backup.LocalPath == "" {
		t.Error("Backup.LocalPath is empty, want the default path")
	}

	// Env override.
	os.Setenv("ARC_BACKUP_ENABLED", "false")
	os.Setenv("ARC_BACKUP_LOCAL_PATH", "/custom/backups")
	defer func() {
		os.Unsetenv("ARC_BACKUP_ENABLED")
		os.Unsetenv("ARC_BACKUP_LOCAL_PATH")
	}()
	cfg2, err := Load()
	if err != nil {
		t.Fatalf("Load() with env: %v", err)
	}
	if cfg2.Backup.Enabled {
		t.Error("Backup.Enabled = true, want false (from env)")
	}
	if cfg2.Backup.LocalPath != "/custom/backups" {
		t.Errorf("Backup.LocalPath = %q, want /custom/backups (from env)", cfg2.Backup.LocalPath)
	}
}

func TestLoad_IcebergRejectsColdTiering(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// iceberg + cold-tier tiering must be rejected (cold files would be deleted from the table).
	os.Setenv("ARC_ICEBERG_ENABLED", "true")
	os.Setenv("ARC_TIERED_STORAGE_ENABLED", "true")
	os.Setenv("ARC_TIERED_STORAGE_COLD_ENABLED", "true")
	os.Setenv("ARC_TIERED_STORAGE_COLD_BACKEND", "s3")
	os.Setenv("ARC_TIERED_STORAGE_COLD_S3_BUCKET", "b")
	defer func() {
		os.Unsetenv("ARC_ICEBERG_ENABLED")
		os.Unsetenv("ARC_TIERED_STORAGE_ENABLED")
		os.Unsetenv("ARC_TIERED_STORAGE_COLD_ENABLED")
		os.Unsetenv("ARC_TIERED_STORAGE_COLD_BACKEND")
		os.Unsetenv("ARC_TIERED_STORAGE_COLD_S3_BUCKET")
	}()

	if _, err := Load(); err == nil {
		t.Fatal("expected error for iceberg.enabled + cold-tier tiering, got nil")
	}
}

func TestLoad_IcebergRequiresLocalBackend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// iceberg + non-local primary backend must be rejected (local-only in v1).
	os.Setenv("ARC_ICEBERG_ENABLED", "true")
	os.Setenv("ARC_STORAGE_BACKEND", "s3")
	os.Setenv("ARC_STORAGE_S3_BUCKET", "b")
	defer func() {
		os.Unsetenv("ARC_ICEBERG_ENABLED")
		os.Unsetenv("ARC_STORAGE_BACKEND")
		os.Unsetenv("ARC_STORAGE_S3_BUCKET")
	}()

	if _, err := Load(); err == nil {
		t.Fatal("expected error for iceberg.enabled + non-local backend, got nil")
	}
}

func TestLoad_IcebergReconcileInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval string
		want     int
		wantErr  bool
	}{
		{name: "default", want: 300},
		{name: "one second", interval: "1", want: 1},
		{name: "five minutes", interval: "300", want: 300},
		{name: "one hour", interval: "3600", want: 3600},
		{name: "zero", interval: "0", wantErr: true},
		{name: "negative", interval: "-1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ARC_ICEBERG_ENABLED", "true")
			t.Setenv("ARC_ICEBERG_RECONCILE_INTERVAL", tt.interval)
			t.Chdir(t.TempDir())

			cfg, err := Load()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected invalid reconcile interval to fail config loading")
				}
				if !strings.Contains(err.Error(), "iceberg.reconcile_interval") {
					t.Errorf("Load() error = %v, want reconcile interval validation error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Iceberg.ReconcileInterval != tt.want {
				t.Errorf("Iceberg.ReconcileInterval = %d, want %d", cfg.Iceberg.ReconcileInterval, tt.want)
			}
		})
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	// Create a temp dir without config file
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Set env vars to override
	os.Setenv("ARC_DATABASE_MAX_CONNECTIONS", "42")
	os.Setenv("ARC_DATABASE_MEMORY_LIMIT", "16GB")
	os.Setenv("ARC_DATABASE_THREAD_COUNT", "8")
	defer func() {
		os.Unsetenv("ARC_DATABASE_MAX_CONNECTIONS")
		os.Unsetenv("ARC_DATABASE_MEMORY_LIMIT")
		os.Unsetenv("ARC_DATABASE_THREAD_COUNT")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Database.MaxConnections != 42 {
		t.Errorf("Database.MaxConnections = %d, want 42 (from env)", cfg.Database.MaxConnections)
	}
	if cfg.Database.MemoryLimit != "16GB" {
		t.Errorf("Database.MemoryLimit = %s, want '16GB' (from env)", cfg.Database.MemoryLimit)
	}
	if cfg.Database.ThreadCount != 8 {
		t.Errorf("Database.ThreadCount = %d, want 8 (from env)", cfg.Database.ThreadCount)
	}
}

func TestLoad_MetricsDefaults(t *testing.T) {
	// Create a temp dir without config file to test defaults
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify metrics defaults
	if cfg.Metrics.TimeseriesRetentionMinutes != 30 {
		t.Errorf("Metrics.TimeseriesRetentionMinutes = %d, want 30", cfg.Metrics.TimeseriesRetentionMinutes)
	}
	if cfg.Metrics.TimeseriesIntervalSeconds != 5 {
		t.Errorf("Metrics.TimeseriesIntervalSeconds = %d, want 5", cfg.Metrics.TimeseriesIntervalSeconds)
	}
}

func TestLoad_MetricsEnvOverride(t *testing.T) {
	// Create a temp dir without config file
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Set env vars to override
	os.Setenv("ARC_METRICS_TIMESERIES_RETENTION_MINUTES", "60")
	os.Setenv("ARC_METRICS_TIMESERIES_INTERVAL_SECONDS", "10")
	defer func() {
		os.Unsetenv("ARC_METRICS_TIMESERIES_RETENTION_MINUTES")
		os.Unsetenv("ARC_METRICS_TIMESERIES_INTERVAL_SECONDS")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Metrics.TimeseriesRetentionMinutes != 60 {
		t.Errorf("Metrics.TimeseriesRetentionMinutes = %d, want 60 (from env)", cfg.Metrics.TimeseriesRetentionMinutes)
	}
	if cfg.Metrics.TimeseriesIntervalSeconds != 10 {
		t.Errorf("Metrics.TimeseriesIntervalSeconds = %d, want 10 (from env)", cfg.Metrics.TimeseriesIntervalSeconds)
	}
}

// TLS Configuration Tests

func TestServerConfig_ValidateTLS_Disabled(t *testing.T) {
	cfg := &ServerConfig{TLSEnabled: false}
	if err := cfg.ValidateTLS(); err != nil {
		t.Errorf("ValidateTLS() with TLS disabled should not error: %v", err)
	}
}

func TestServerConfig_ValidateTLS_MissingCertFile(t *testing.T) {
	cfg := &ServerConfig{
		TLSEnabled:  true,
		TLSCertFile: "",
		TLSKeyFile:  "/some/key.pem",
	}
	err := cfg.ValidateTLS()
	if err == nil {
		t.Error("ValidateTLS() should error when cert file is empty")
	}
	if !strings.Contains(err.Error(), "tls_cert_file") {
		t.Errorf("Error should mention tls_cert_file: %v", err)
	}
}

func TestServerConfig_ValidateTLS_MissingKeyFile(t *testing.T) {
	cfg := &ServerConfig{
		TLSEnabled:  true,
		TLSCertFile: "/some/cert.pem",
		TLSKeyFile:  "",
	}
	err := cfg.ValidateTLS()
	if err == nil {
		t.Error("ValidateTLS() should error when key file is empty")
	}
	if !strings.Contains(err.Error(), "tls_key_file") {
		t.Errorf("Error should mention tls_key_file: %v", err)
	}
}

func TestServerConfig_ValidateTLS_CertFileNotFound(t *testing.T) {
	cfg := &ServerConfig{
		TLSEnabled:  true,
		TLSCertFile: "/nonexistent/path/cert.pem",
		TLSKeyFile:  "/nonexistent/path/key.pem",
	}
	err := cfg.ValidateTLS()
	if err == nil {
		t.Error("ValidateTLS() should error when cert file doesn't exist")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Error should mention file not found: %v", err)
	}
}

func TestServerConfig_ValidateTLS_KeyFileNotFound(t *testing.T) {
	// Create a temp cert file but not key file
	tmpDir, err := os.MkdirTemp("", "arc-tls-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	certPath := filepath.Join(tmpDir, "cert.pem")
	if err := os.WriteFile(certPath, []byte("fake cert"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &ServerConfig{
		TLSEnabled:  true,
		TLSCertFile: certPath,
		TLSKeyFile:  "/nonexistent/key.pem",
	}
	err = cfg.ValidateTLS()
	if err == nil {
		t.Error("ValidateTLS() should error when key file doesn't exist")
	}
	if !strings.Contains(err.Error(), "key file not found") {
		t.Errorf("Error should mention key file not found: %v", err)
	}
}

func TestServerConfig_ValidateTLS_CertIsDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-tls-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &ServerConfig{
		TLSEnabled:  true,
		TLSCertFile: tmpDir, // Directory, not a file
		TLSKeyFile:  "/some/key.pem",
	}
	err = cfg.ValidateTLS()
	if err == nil {
		t.Error("ValidateTLS() should error when cert path is a directory")
	}
	if !strings.Contains(err.Error(), "directory") {
		t.Errorf("Error should mention directory: %v", err)
	}
}

func TestServerConfig_ValidateTLS_ValidFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-tls-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	certPath := filepath.Join(tmpDir, "cert.pem")
	keyPath := filepath.Join(tmpDir, "key.pem")

	if err := os.WriteFile(certPath, []byte("fake cert"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, []byte("fake key"), 0600); err != nil {
		t.Fatal(err)
	}

	cfg := &ServerConfig{
		TLSEnabled:  true,
		TLSCertFile: certPath,
		TLSKeyFile:  keyPath,
	}
	err = cfg.ValidateTLS()
	if err != nil {
		t.Errorf("ValidateTLS() should not error with valid files: %v", err)
	}
}

func TestLoad_TLSDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify TLS defaults
	if cfg.Server.TLSEnabled != false {
		t.Error("Server.TLSEnabled should default to false")
	}
	if cfg.Server.TLSCertFile != "" {
		t.Errorf("Server.TLSCertFile should default to empty, got %s", cfg.Server.TLSCertFile)
	}
	if cfg.Server.TLSKeyFile != "" {
		t.Errorf("Server.TLSKeyFile should default to empty, got %s", cfg.Server.TLSKeyFile)
	}
}

func TestLoad_TLSEnvOverride(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Set env vars to enable TLS
	os.Setenv("ARC_SERVER_TLS_ENABLED", "true")
	os.Setenv("ARC_SERVER_TLS_CERT_FILE", "/path/to/cert.pem")
	os.Setenv("ARC_SERVER_TLS_KEY_FILE", "/path/to/key.pem")
	defer func() {
		os.Unsetenv("ARC_SERVER_TLS_ENABLED")
		os.Unsetenv("ARC_SERVER_TLS_CERT_FILE")
		os.Unsetenv("ARC_SERVER_TLS_KEY_FILE")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if !cfg.Server.TLSEnabled {
		t.Error("Server.TLSEnabled should be true from env")
	}
	if cfg.Server.TLSCertFile != "/path/to/cert.pem" {
		t.Errorf("Server.TLSCertFile = %s, want /path/to/cert.pem", cfg.Server.TLSCertFile)
	}
	if cfg.Server.TLSKeyFile != "/path/to/key.pem" {
		t.Errorf("Server.TLSKeyFile = %s, want /path/to/key.pem", cfg.Server.TLSKeyFile)
	}
}

// ParseSize Tests

func TestParseSize_ValidSizes(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		// Bytes
		{"100B", 100},
		{"1024B", 1024},
		// Kilobytes
		{"1KB", 1024},
		{"100KB", 100 * 1024},
		{"512KB", 512 * 1024},
		// Megabytes
		{"1MB", 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
		{"512MB", 512 * 1024 * 1024},
		// Gigabytes
		{"1GB", 1024 * 1024 * 1024},
		{"2GB", 2 * 1024 * 1024 * 1024},
		// Case insensitivity
		{"1gb", 1024 * 1024 * 1024},
		{"1Gb", 1024 * 1024 * 1024},
		{"100mb", 100 * 1024 * 1024},
		{"100Mb", 100 * 1024 * 1024},
		// With spaces
		{" 1GB ", 1024 * 1024 * 1024},
		{"100 MB", 100 * 1024 * 1024},
		// Fractional values
		{"1.5GB", int64(1.5 * 1024 * 1024 * 1024)},
		{"0.5MB", int64(0.5 * 1024 * 1024)},
		// Plain numbers (bytes)
		{"1024", 1024},
		{"1048576", 1048576},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseSize(tt.input)
			if err != nil {
				t.Errorf("ParseSize(%q) error = %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("ParseSize(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseSize_InvalidSizes(t *testing.T) {
	tests := []struct {
		input string
		desc  string
	}{
		{"", "empty string"},
		{"   ", "whitespace only"},
		{"-1GB", "negative size"},
		{"-100MB", "negative size"},
		{"1TB", "unsupported unit"},
		{"1PB", "unsupported unit"},
		{"abc", "non-numeric"},
		{"GB", "no number"},
		{"MB100", "reversed format"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := ParseSize(tt.input)
			if err == nil {
				t.Errorf("ParseSize(%q) should error for %s", tt.input, tt.desc)
			}
		})
	}
}

func TestLoad_MaxPayloadSizeDefault(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Default should be 1GB
	expectedSize := int64(1024 * 1024 * 1024)
	if cfg.Server.MaxPayloadSize != expectedSize {
		t.Errorf("Server.MaxPayloadSize = %d, want %d (1GB)", cfg.Server.MaxPayloadSize, expectedSize)
	}
}

func TestLoad_MaxPayloadSizeEnvOverride(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Set env var to 2GB
	os.Setenv("ARC_SERVER_MAX_PAYLOAD_SIZE", "2GB")
	defer os.Unsetenv("ARC_SERVER_MAX_PAYLOAD_SIZE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	expectedSize := int64(2 * 1024 * 1024 * 1024)
	if cfg.Server.MaxPayloadSize != expectedSize {
		t.Errorf("Server.MaxPayloadSize = %d, want %d (2GB)", cfg.Server.MaxPayloadSize, expectedSize)
	}
}

func TestLoad_MaxPayloadSizeInvalid(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Set env var to invalid value
	os.Setenv("ARC_SERVER_MAX_PAYLOAD_SIZE", "invalid")
	defer os.Unsetenv("ARC_SERVER_MAX_PAYLOAD_SIZE")

	_, err = Load()
	if err == nil {
		t.Error("Load() should error with invalid max_payload_size")
	}
	if !strings.Contains(err.Error(), "max_payload_size") {
		t.Errorf("Error should mention max_payload_size: %v", err)
	}
}

// Cluster Seeds Tests

func TestParseStringSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{"empty", "", nil},
		{"single", "host1:9100", []string{"host1:9100"}},
		{"multiple", "host1:9100,host2:9100", []string{"host1:9100", "host2:9100"}},
		{"with spaces", "host1:9100, host2:9100 , host3:9100", []string{"host1:9100", "host2:9100", "host3:9100"}},
		{"trailing comma", "host1:9100,host2:9100,", []string{"host1:9100", "host2:9100"}},
		{"leading comma", ",host1:9100,host2:9100", []string{"host1:9100", "host2:9100"}},
		{"empty elements", "host1:9100,,host2:9100", []string{"host1:9100", "host2:9100"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseStringSlice(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("parseStringSlice(%q) = %v (len %d), want %v (len %d)",
					tt.input, result, len(result), tt.expected, len(tt.expected))
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("parseStringSlice(%q)[%d] = %q, want %q", tt.input, i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestLoad_ClusterSeedsEnvOverride(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	// Set env var for cluster seeds
	os.Setenv("ARC_CLUSTER_SEEDS", "host1:9100,host2:9100,host3:9100")
	defer os.Unsetenv("ARC_CLUSTER_SEEDS")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	expected := []string{"host1:9100", "host2:9100", "host3:9100"}
	if len(cfg.Cluster.Seeds) != len(expected) {
		t.Errorf("Cluster.Seeds = %v (len %d), want %v (len %d)",
			cfg.Cluster.Seeds, len(cfg.Cluster.Seeds), expected, len(expected))
		return
	}
	for i, v := range cfg.Cluster.Seeds {
		if v != expected[i] {
			t.Errorf("Cluster.Seeds[%d] = %q, want %q", i, v, expected[i])
		}
	}
}

func TestLoad_ClusterReplicationReconciliationInterval(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Cluster.ReplicationReconciliationIntervalSeconds != 300 {
		t.Errorf("default reconciliation interval = %d, want 300", cfg.Cluster.ReplicationReconciliationIntervalSeconds)
	}

	os.Setenv("ARC_CLUSTER_REPLICATION_RECONCILIATION_INTERVAL_SECONDS", "17")
	defer os.Unsetenv("ARC_CLUSTER_REPLICATION_RECONCILIATION_INTERVAL_SECONDS")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() with env override error = %v", err)
	}
	if cfg.Cluster.ReplicationReconciliationIntervalSeconds != 17 {
		t.Errorf("env reconciliation interval = %d, want 17", cfg.Cluster.ReplicationReconciliationIntervalSeconds)
	}
}

// QueryConfig Tests

func TestQueryConfig_Defaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Test defaults
	if cfg.Query.EnableS3Cache != false {
		t.Errorf("Query.EnableS3Cache default = %v, want false", cfg.Query.EnableS3Cache)
	}
	expectedSize := int64(128 * 1024 * 1024) // 128MB in bytes
	if cfg.Query.S3CacheSize != expectedSize {
		t.Errorf("Query.S3CacheSize default = %d, want %d (128MB)", cfg.Query.S3CacheSize, expectedSize)
	}
	if cfg.Query.S3CacheTTLSeconds != 3600 {
		t.Errorf("Query.S3CacheTTLSeconds default = %d, want 3600", cfg.Query.S3CacheTTLSeconds)
	}
}

func TestQueryConfig_EnvOverride(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	os.Setenv("ARC_QUERY_ENABLE_S3_CACHE", "true")
	os.Setenv("ARC_QUERY_S3_CACHE_SIZE", "256MB")
	os.Setenv("ARC_QUERY_S3_CACHE_TTL_SECONDS", "7200")
	defer func() {
		os.Unsetenv("ARC_QUERY_ENABLE_S3_CACHE")
		os.Unsetenv("ARC_QUERY_S3_CACHE_SIZE")
		os.Unsetenv("ARC_QUERY_S3_CACHE_TTL_SECONDS")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Query.EnableS3Cache != true {
		t.Errorf("Query.EnableS3Cache = %v, want true", cfg.Query.EnableS3Cache)
	}
	expectedSize := int64(256 * 1024 * 1024) // 256MB in bytes
	if cfg.Query.S3CacheSize != expectedSize {
		t.Errorf("Query.S3CacheSize = %d, want %d (256MB)", cfg.Query.S3CacheSize, expectedSize)
	}
	if cfg.Query.S3CacheTTLSeconds != 7200 {
		t.Errorf("Query.S3CacheTTLSeconds = %d, want 7200", cfg.Query.S3CacheTTLSeconds)
	}
}

func TestWALConfig_Defaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Test WAL defaults
	if cfg.WAL.RecoveryIntervalSeconds != 300 {
		t.Errorf("WAL.RecoveryIntervalSeconds default = %d, want 300", cfg.WAL.RecoveryIntervalSeconds)
	}
	if cfg.WAL.RecoveryBatchSize != 10000 {
		t.Errorf("WAL.RecoveryBatchSize default = %d, want 10000", cfg.WAL.RecoveryBatchSize)
	}
	if cfg.WAL.DiskHighWatermarkPercent != 90 {
		t.Errorf("WAL.DiskHighWatermarkPercent default = %d, want 90", cfg.WAL.DiskHighWatermarkPercent)
	}
	if cfg.WAL.DiskMinFreeMB != 512 {
		t.Errorf("WAL.DiskMinFreeMB default = %d, want 512", cfg.WAL.DiskMinFreeMB)
	}
}

func TestWALConfig_EnvOverride(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	os.Setenv("ARC_WAL_RECOVERY_INTERVAL_SECONDS", "600")
	os.Setenv("ARC_WAL_RECOVERY_BATCH_SIZE", "5000")
	os.Setenv("ARC_WAL_DISK_HIGH_WATERMARK_PERCENT", "85")
	os.Setenv("ARC_WAL_DISK_MIN_FREE_MB", "256")
	defer func() {
		os.Unsetenv("ARC_WAL_RECOVERY_INTERVAL_SECONDS")
		os.Unsetenv("ARC_WAL_RECOVERY_BATCH_SIZE")
		os.Unsetenv("ARC_WAL_DISK_HIGH_WATERMARK_PERCENT")
		os.Unsetenv("ARC_WAL_DISK_MIN_FREE_MB")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.WAL.RecoveryIntervalSeconds != 600 {
		t.Errorf("WAL.RecoveryIntervalSeconds = %d, want 600", cfg.WAL.RecoveryIntervalSeconds)
	}
	if cfg.WAL.RecoveryBatchSize != 5000 {
		t.Errorf("WAL.RecoveryBatchSize = %d, want 5000", cfg.WAL.RecoveryBatchSize)
	}
	if cfg.WAL.DiskHighWatermarkPercent != 85 {
		t.Errorf("WAL.DiskHighWatermarkPercent = %d, want 85", cfg.WAL.DiskHighWatermarkPercent)
	}
	if cfg.WAL.DiskMinFreeMB != 256 {
		t.Errorf("WAL.DiskMinFreeMB = %d, want 256", cfg.WAL.DiskMinFreeMB)
	}
}

// TestLoad_StorageBackendValidation covers the startup guards added for
// S3/Azure primary backends and the cold tier: a misconfigured backend must
// fail fast at config load rather than at first query.
func TestLoad_StorageBackendValidation(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		wantError bool
	}{
		{
			name:      "invalid primary backend -> error",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "gcs"},
			wantError: true,
		},
		{
			name:      "local primary backend -> ok",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "local"},
			wantError: false,
		},
		{
			name:      "s3 primary without bucket -> error",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "s3"},
			wantError: true,
		},
		{
			name:      "minio primary without bucket -> error",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "minio"},
			wantError: true,
		},
		{
			name:      "s3 primary with bucket -> ok",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b"},
			wantError: false,
		},
		{
			name:      "backend case/space normalized -> bucket guard still applies",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "  S3  "},
			wantError: true,
		},
		{
			name:      "azure primary without account name or conn string -> error",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "azure", "ARC_STORAGE_AZURE_CONTAINER": "c"},
			wantError: true,
		},
		{
			name:      "azure primary with connection string, no account name -> ok",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "azure", "ARC_STORAGE_AZURE_CONTAINER": "c", "ARC_STORAGE_AZURE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=k"},
			wantError: false,
		},
		{
			name:      "azure primary without container -> error",
			env:       map[string]string{"ARC_STORAGE_BACKEND": "azure", "ARC_STORAGE_AZURE_ACCOUNT_NAME": "a"},
			wantError: true,
		},
		{
			name:      "cold tier enabled s3 without bucket -> error",
			env:       map[string]string{"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "s3"},
			wantError: true,
		},
		{
			name:      "cold tier enabled s3 with bucket -> ok",
			env:       map[string]string{"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "s3", "ARC_TIERED_STORAGE_COLD_S3_BUCKET": "b"},
			wantError: false,
		},
		{
			name:      "cold tier enabled invalid backend -> error",
			env:       map[string]string{"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "gcs"},
			wantError: true,
		},
		{
			// Regression for the gate-vs-runtime divergence: with the PARENT tier
			// disabled, the runtime ignores the cold tier entirely, so an invalid
			// cold config must NOT block startup (previously it did).
			name:      "parent tiering disabled + bad cold config -> ok (runtime ignores it)",
			env:       map[string]string{"ARC_TIERED_STORAGE_ENABLED": "false", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "gcs"},
			wantError: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "arc-config-validate")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)
			oldWd, _ := os.Getwd()
			os.Chdir(tmpDir)
			defer os.Chdir(oldWd)

			for k, v := range tc.env {
				os.Setenv(k, v)
			}
			defer func() {
				for k := range tc.env {
					os.Unsetenv(k)
				}
			}()

			_, err = Load()
			if tc.wantError && err == nil {
				t.Errorf("Load() expected an error, got nil")
			}
			if !tc.wantError && err != nil {
				t.Errorf("Load() unexpected error: %v", err)
			}
		})
	}
}

// TestLoad_StorageValuesTrimmed verifies storage identifiers are trimmed
// in-place at load, so stray copy-paste whitespace can't reach the DuckDB
// secret SCOPE / sandbox allowlist / cloud clients.
func TestLoad_StorageValuesTrimmed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "arc-config-trim")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	env := map[string]string{
		"ARC_STORAGE_BACKEND":               "s3",
		"ARC_STORAGE_S3_BUCKET":             "  my-bucket  ",
		"ARC_STORAGE_S3_PREFIX":             "  hot/  ",
		"ARC_TIERED_STORAGE_ENABLED":        "true",
		"ARC_TIERED_STORAGE_COLD_ENABLED":   "true",
		"ARC_TIERED_STORAGE_COLD_BACKEND":   "s3",
		"ARC_TIERED_STORAGE_COLD_S3_BUCKET": "  cold-bucket  ",
	}
	for k, v := range env {
		os.Setenv(k, v)
	}
	defer func() {
		for k := range env {
			os.Unsetenv(k)
		}
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Storage.S3Bucket != "my-bucket" {
		t.Errorf("S3Bucket = %q, want trimmed %q", cfg.Storage.S3Bucket, "my-bucket")
	}
	if cfg.Storage.S3Prefix != "hot/" {
		t.Errorf("S3Prefix = %q, want trimmed %q", cfg.Storage.S3Prefix, "hot/")
	}
	if cfg.TieredStorage.Cold.S3Bucket != "cold-bucket" {
		t.Errorf("Cold.S3Bucket = %q, want trimmed %q (pointer mutation must persist)", cfg.TieredStorage.Cold.S3Bucket, "cold-bucket")
	}
}

func TestLoad_CompactionMaxFilesPerBatch(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "arc-config-test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)

		oldWd, _ := os.Getwd()
		os.Chdir(tmpDir)
		defer os.Chdir(oldWd)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}

		if cfg.Compaction.MaxFilesPerBatch != 30 {
			t.Errorf("Compaction.MaxFilesPerBatch = %d, want 30 (default)", cfg.Compaction.MaxFilesPerBatch)
		}
	})

	t.Run("env override", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "arc-config-test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)

		oldWd, _ := os.Getwd()
		os.Chdir(tmpDir)
		defer os.Chdir(oldWd)

		// The edge/constrained-link case: smaller batches produce smaller,
		// independently-transferable compacted outputs.
		os.Setenv("ARC_COMPACTION_MAX_FILES_PER_BATCH", "5")
		defer os.Unsetenv("ARC_COMPACTION_MAX_FILES_PER_BATCH")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}

		if cfg.Compaction.MaxFilesPerBatch != 5 {
			t.Errorf("Compaction.MaxFilesPerBatch = %d, want 5 (from env)", cfg.Compaction.MaxFilesPerBatch)
		}
	})
}
