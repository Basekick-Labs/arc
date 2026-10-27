package config

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/syscpu"
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

// TestEffectiveCores_ForwardsToSyscpu pins that config's exported form is the
// same answer as the package that owns the implementation. The table-driven
// coverage of the pure form lives in internal/syscpu with the function.
//
// Deliberately weak, and worth saying so rather than overselling it: this
// compares two live reads, so a forwarder reimplemented as runtime.NumCPU()
// would still pass on any machine without a CPU quota. What actually holds
// that line is cmd/arc's TestApplyLicenseCoreLimits_LoadResolvesAutoBeforeLicense,
// which drives the seam with injected values.
func TestEffectiveCores_ForwardsToSyscpu(t *testing.T) {
	if got, want := EffectiveCores(), syscpu.EffectiveCores(); got != want {
		t.Errorf("EffectiveCores() = %d, want %d (syscpu.EffectiveCores)", got, want)
	}
}

func TestDefaultCompactionThreads(t *testing.T) {
	cases := []struct {
		name                 string
		cores, maxConcurrent int
		want                 int
	}{
		{"default concurrency keeps half-core behavior", 8, 2, 4},
		{"two-core limit at default concurrency", 2, 2, 1},
		{"raised concurrency divides available cores", 16, 4, 4},
		{"raised concurrency on constrained process", 8, 4, 2},
		{"zero concurrency uses default", 8, 0, 4},
		{"negative concurrency uses default", 8, -1, 4},
		{"minimum one thread", 1, 8, 1},
		{"zero cores", 0, 2, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := defaultCompactionThreads(c.cores, c.maxConcurrent); got != c.want {
				t.Errorf("defaultCompactionThreads(%d, %d) = %d, want %d", c.cores, c.maxConcurrent, got, c.want)
			}
		})
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
	if len(cfg.Database.Extensions) != 0 {
		t.Errorf("Database.Extensions default = %v, want an empty list", cfg.Database.Extensions)
	}

	// Verify ingest defaults are applied
	// These settings drive the flush cadence described alongside hourly
	// compaction's independent file floor.
	if cfg.Ingest.MaxBufferSize != 50000 {
		t.Errorf("Ingest.MaxBufferSize = %d, want 50000", cfg.Ingest.MaxBufferSize)
	}
	if cfg.Ingest.MaxBufferAgeMS != 5000 {
		t.Errorf("Ingest.MaxBufferAgeMS = %d, want 5000", cfg.Ingest.MaxBufferAgeMS)
	}
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
	if cfg.Compaction.HourlyMinFiles != 10 {
		t.Errorf("Compaction.HourlyMinFiles = %d, want 10", cfg.Compaction.HourlyMinFiles)
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

func TestLoad_IcebergRejectsDottedNamespacePrefix(t *testing.T) {
	// Arc builds ONE Iceberg namespace component per database, as <prefix>_<database>. A dot in
	// the prefix puts a dot in every one of them, and iceberg-go v0.7.0 addresses a namespace with
	// a dotted component by a JSON-encoded catalog key instead of the plain dotted string. The
	// warehouse directory then becomes __iceberg_namespace_v1__:[...].db, which Arc's own
	// warehouse-directory test does not recognise and would walk back in as a user database, and
	// the percent-encoded metadata location leaves no version-hint.text for directory readers.
	for _, tc := range []struct {
		prefix  string
		wantErr bool
	}{
		{"arc", false},
		{"arc_wh", false},
		{"my-warehouse", false},
		{"my.warehouse", true},
		{".arc", true},
		{"arc.", true},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_ICEBERG_ENABLED", "true")
			t.Setenv("ARC_STORAGE_BACKEND", "local")
			t.Setenv("ARC_ICEBERG_NAMESPACE_PREFIX", tc.prefix)

			_, err := Load()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("namespace_prefix %q was accepted; it would publish unreadable tables", tc.prefix)
				}
				if !strings.Contains(err.Error(), "must not contain a dot") {
					t.Errorf("error does not name the cause: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("namespace_prefix %q rejected: %v", tc.prefix, err)
			}
		})
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
	os.Setenv("ARC_DATABASE_EXTENSIONS", " Spatial ,vss,,spatial")
	defer func() {
		os.Unsetenv("ARC_DATABASE_MAX_CONNECTIONS")
		os.Unsetenv("ARC_DATABASE_MEMORY_LIMIT")
		os.Unsetenv("ARC_DATABASE_THREAD_COUNT")
		os.Unsetenv("ARC_DATABASE_EXTENSIONS")
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
	// Split on commas (GetStringSlice kept the whole value as ONE entry),
	// trimmed, lower-cased, empties and repeats dropped. Compared as a
	// slice: a joined string cannot tell ["spatial","vss"] from
	// ["spatial,vss"].
	if got, want := cfg.Database.Extensions, []string{"spatial", "vss"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Database.Extensions = %q, want %q (from env)", got, want)
	}
}

func TestLoad_IcebergNamespaceMigrationDryRun(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_ICEBERG_ENABLED", "true")
	t.Setenv("ARC_STORAGE_BACKEND", "local")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() default: %v", err)
	}
	if !cfg.Iceberg.NamespaceMigrationDryRun {
		t.Fatal("Iceberg.NamespaceMigrationDryRun default = false, want true")
	}

	t.Setenv("ARC_ICEBERG_NAMESPACE_MIGRATION_DRY_RUN", "false")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() apply mode: %v", err)
	}
	if cfg.Iceberg.NamespaceMigrationDryRun {
		t.Fatal("Iceberg.NamespaceMigrationDryRun = true, want false from environment")
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
	if !cfg.Query.CancelOnClientDisconnect {
		t.Error("Query.CancelOnClientDisconnect default = false, want true")
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
	os.Setenv("ARC_QUERY_CANCEL_ON_CLIENT_DISCONNECT", "false")
	defer func() {
		os.Unsetenv("ARC_QUERY_ENABLE_S3_CACHE")
		os.Unsetenv("ARC_QUERY_S3_CACHE_SIZE")
		os.Unsetenv("ARC_QUERY_S3_CACHE_TTL_SECONDS")
		os.Unsetenv("ARC_QUERY_CANCEL_ON_CLIENT_DISCONNECT")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Query.CancelOnClientDisconnect {
		t.Error("Query.CancelOnClientDisconnect env override = true, want false")
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
	defer func() {
		os.Unsetenv("ARC_WAL_RECOVERY_INTERVAL_SECONDS")
		os.Unsetenv("ARC_WAL_RECOVERY_BATCH_SIZE")
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
			env:       map[string]string{"ARC_STORAGE_BACKEND": "gcp"},
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
			env:       map[string]string{"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "gcp"},
			wantError: true,
		},
		{
			// Regression for the gate-vs-runtime divergence: with the PARENT tier
			// disabled, the runtime ignores the cold tier entirely, so an invalid
			// cold config must NOT block startup (previously it did).
			name:      "parent tiering disabled + bad cold config -> ok (runtime ignores it)",
			env:       map[string]string{"ARC_TIERED_STORAGE_ENABLED": "false", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "gcp"},
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

// #1207: stream takeover timings are Go durations with a floor.
func TestLoad_StreamTakeoverTimings(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.StreamTakeoverGrace != 60*time.Second || cfg.Cluster.StreamTakeoverDepartedTTL != 24*time.Hour {
		t.Fatalf("defaults %v %v", cfg.Cluster.StreamTakeoverGrace, cfg.Cluster.StreamTakeoverDepartedTTL)
	}
	for _, tc := range []struct{ key, val string }{
		{"ARC_CLUSTER_STREAM_TAKEOVER_GRACE", "5s"},
		{"ARC_CLUSTER_STREAM_TAKEOVER_GRACE", "soon"},
		{"ARC_CLUSTER_STREAM_TAKEOVER_DEPARTED_TTL", "30s"},
	} {
		t.Setenv(tc.key, tc.val)
		if _, err := Load(); err == nil {
			t.Errorf("%s=%s accepted", tc.key, tc.val)
		}
		os.Unsetenv(tc.key)
	}
}

// A breaker fraction that is not a number above 0 is refused: NaN or 0 would
// disable or misread the stale-local sweep's breaker (#1071).
func TestLoad_StaleLocalSweepMaxFraction(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)
	for _, v := range []string{"0", "-1", "NaN"} {
		t.Setenv("ARC_CLUSTER_STALE_LOCAL_SWEEP_MAX_FRACTION", v)
		if _, err := Load(); err == nil {
			t.Fatalf("%s accepted", v)
		}
	}
	t.Setenv("ARC_CLUSTER_STALE_LOCAL_SWEEP_MAX_FRACTION", "2")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.StaleLocalSweepMaxFraction != 2 || !cfg.Cluster.StaleLocalSweep {
		t.Fatalf("%+v", cfg.Cluster)
	}
}

// Raft pre-vote is on by default and can be turned off for a rolling upgrade
// from a release without it (#1272).
func TestLoad_RaftPreVote(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Cluster.RaftPreVote {
		t.Fatal("cluster.raft_pre_vote defaults to off")
	}
	t.Setenv("ARC_CLUSTER_RAFT_PRE_VOTE", "false")
	if cfg, err = Load(); err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.RaftPreVote {
		t.Fatal("ARC_CLUSTER_RAFT_PRE_VOTE=false ignored")
	}
}

// A forwarded import is bounded by a 1 h backstop by default, settable, 0 =
// no bound (#1070).
func TestLoad_ImportForwardTimeout(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(oldWd)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.ImportForwardTimeoutMs != 3600000 {
		t.Fatalf("cluster.import_forward_timeout_ms default %d, want 3600000", cfg.Cluster.ImportForwardTimeoutMs)
	}
	t.Setenv("ARC_CLUSTER_IMPORT_FORWARD_TIMEOUT_MS", "0")
	if cfg, err = Load(); err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.ImportForwardTimeoutMs != 0 {
		t.Fatalf("ARC_CLUSTER_IMPORT_FORWARD_TIMEOUT_MS=0 gave %d", cfg.Cluster.ImportForwardTimeoutMs)
	}
}

// TestFileTimePruningDefaultsOn pins the 27.01.1 default (#659) and that both
// ways an operator turns it off still work.
func TestFileTimePruningDefaultsOn(t *testing.T) {
	// A developer shell's value must not decide the default case; t.Setenv
	// restores it afterwards.
	t.Setenv("ARC_QUERY_FILE_TIME_PRUNING", "")
	os.Unsetenv("ARC_QUERY_FILE_TIME_PRUNING")
	v := viper.New()
	setDefaults(v)
	if !v.GetBool("query.file_time_pruning") {
		t.Fatal("query.file_time_pruning default = false, want true (#659)")
	}
	if got := v.GetInt("query.file_time_pruning_margin_seconds"); got != 300 {
		t.Errorf("query.file_time_pruning_margin_seconds default = %d, want 300", got)
	}

	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Query.FileTimePruning {
		t.Error("Load() with no config: Query.FileTimePruning = false, want true")
	}

	t.Setenv("ARC_QUERY_FILE_TIME_PRUNING", "false")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Query.FileTimePruning {
		t.Error("ARC_QUERY_FILE_TIME_PRUNING=false: Query.FileTimePruning = true, want false")
	}
	os.Unsetenv("ARC_QUERY_FILE_TIME_PRUNING")

	if err := os.WriteFile(filepath.Join(tmpDir, "arc.toml"), []byte("[query]\nfile_time_pruning = false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Query.FileTimePruning {
		t.Error("arc.toml file_time_pruning = false: Query.FileTimePruning = true, want false")
	}
}

// database.external_file_cache defaults to auto, takes true/false (any case,
// surrounding space) from the environment, and refuses anything else (#1372).
func TestExternalFileCacheKey(t *testing.T) {
	t.Setenv("ARC_DATABASE_EXTERNAL_FILE_CACHE", "")
	os.Unsetenv("ARC_DATABASE_EXTERNAL_FILE_CACHE")
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Database.ExternalFileCache != "auto" {
		t.Fatalf("default database.external_file_cache = %q, want auto", cfg.Database.ExternalFileCache)
	}
	for env, want := range map[string]string{"false": "false", " TRUE ": "true", "Auto": "auto"} {
		t.Setenv("ARC_DATABASE_EXTERNAL_FILE_CACHE", env)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("ARC_DATABASE_EXTERNAL_FILE_CACHE=%q: Load() error = %v", env, err)
		}
		if cfg.Database.ExternalFileCache != want {
			t.Errorf("ARC_DATABASE_EXTERNAL_FILE_CACHE=%q: got %q, want %q", env, cfg.Database.ExternalFileCache, want)
		}
	}
	for _, bad := range []string{"off", "yes", "0"} {
		t.Setenv("ARC_DATABASE_EXTERNAL_FILE_CACHE", bad)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "database.external_file_cache") {
			t.Errorf("ARC_DATABASE_EXTERNAL_FILE_CACHE=%q: Load() error = %v, want a database.external_file_cache error", bad, err)
		}
	}
	os.Unsetenv("ARC_DATABASE_EXTERNAL_FILE_CACHE")
	if err := os.WriteFile(filepath.Join(tmpDir, "arc.toml"), []byte("[database]\nexternal_file_cache = \"false\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() with arc.toml error = %v", err)
	}
	if cfg.Database.ExternalFileCache != "false" {
		t.Errorf("arc.toml external_file_cache = \"false\": got %q", cfg.Database.ExternalFileCache)
	}
}

// The TOML array spelling of database.extensions keeps each element whole
// and is normalised like the environment spelling (#440).
func TestLoad_DatabaseExtensionsTOMLArray(t *testing.T) {
	tmpDir := t.TempDir()
	oldWd, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd)
	if err := os.WriteFile(filepath.Join(tmpDir, "arc.toml"), []byte("[database]\nextensions = [\"Spatial\", \"spatial\", \" vss \"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.Database.Extensions, []string{"spatial", "vss"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Database.Extensions = %q, want %q", got, want)
	}
}

// TestLoad_GCSPrimaryValidation (#1105): storage.backend="gcs" is accepted with
// a bucket, and each refusal names the key at fault.
func TestLoad_GCSPrimaryValidation(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantErr string // "" = must load
	}{
		{"gcs with bucket", map[string]string{"ARC_STORAGE_GCS_BUCKET": "b"}, ""},
		{"gcs with every key at a non-default value", map[string]string{
			"ARC_STORAGE_GCS_BUCKET": " b ", "ARC_STORAGE_GCS_PREFIX": "inst/a/",
			"ARC_STORAGE_GCS_CREDENTIALS_FILE": "/etc/arc/key.json",
			"ARC_STORAGE_GCS_ENDPOINT":         "https://storage-psc.p.googleapis.com",
		}, ""},
		{"gcs endpoint as a bare host", map[string]string{"ARC_STORAGE_GCS_BUCKET": "b", "ARC_STORAGE_GCS_ENDPOINT": "storage.googleapis.com"}, ""},
		{"gcs without bucket", map[string]string{}, "storage.gcs_bucket is empty"},
		{"gcs bad prefix", map[string]string{"ARC_STORAGE_GCS_BUCKET": "b", "ARC_STORAGE_GCS_PREFIX": "../x"}, "storage.gcs_prefix"},
		{"gcs http endpoint", map[string]string{"ARC_STORAGE_GCS_BUCKET": "b", "ARC_STORAGE_GCS_ENDPOINT": "http://localhost:4443"}, "must be an https URL"},
		{"gcs unusable endpoint", map[string]string{"ARC_STORAGE_GCS_BUCKET": "b", "ARC_STORAGE_GCS_ENDPOINT": "ftp://x"}, "storage.gcs_endpoint"},
		// Lifted in part 3: a GCS primary takes a cold tier.
		{"gcs with an S3 cold tier", map[string]string{
			"ARC_STORAGE_GCS_BUCKET": "b", "ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true",
			"ARC_TIERED_STORAGE_COLD_BACKEND": "s3", "ARC_TIERED_STORAGE_COLD_S3_BUCKET": "c",
		}, ""},
		{"gcs with a leftover cold tier, tiering off", map[string]string{
			"ARC_STORAGE_GCS_BUCKET": "b", "ARC_TIERED_STORAGE_ENABLED": "false", "ARC_TIERED_STORAGE_COLD_ENABLED": "true",
		}, ""},
		{"gcs with iceberg", map[string]string{"ARC_STORAGE_GCS_BUCKET": "b", "ARC_ICEBERG_ENABLED": "true"}, "iceberg.enabled=true requires storage.backend=\"local\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_STORAGE_BACKEND", "gcs")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Load() error = %v", err)
				}
				if cfg.Storage.GCSBucket != "b" {
					t.Fatalf("bucket = %q, want trimmed \"b\"", cfg.Storage.GCSBucket)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load() error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestLoad_GCSColdTierValidation (#1105 part 3): tiered_storage.cold.backend
// "gcs" with every key, its refusals naming the key, and the overlap refusal
// between cold and primary storage, which applies to every backend.
func TestLoad_GCSColdTierValidation(t *testing.T) {
	cold := func(kv ...string) map[string]string {
		m := map[string]string{
			"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true",
			"ARC_TIERED_STORAGE_COLD_BACKEND": "gcs",
		}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	cases := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"local hot, gcs cold, every key non-default", cold(
			"ARC_TIERED_STORAGE_COLD_GCS_BUCKET", " c ", "ARC_TIERED_STORAGE_COLD_GCS_PREFIX", "cold/a",
			"ARC_TIERED_STORAGE_COLD_GCS_CREDENTIALS_FILE", "/etc/arc/cold.json",
			"ARC_TIERED_STORAGE_COLD_GCS_ENDPOINT", "https://storage.googleapis.com"), ""},
		{"gcs cold without bucket", cold(), "tiered_storage.cold.gcs_bucket is empty"},
		{"gcs cold bad prefix", cold("ARC_TIERED_STORAGE_COLD_GCS_BUCKET", "c", "ARC_TIERED_STORAGE_COLD_GCS_PREFIX", "../x"), "tiered_storage.cold.gcs_prefix"},
		{"gcs cold http endpoint", cold("ARC_TIERED_STORAGE_COLD_GCS_BUCKET", "c", "ARC_TIERED_STORAGE_COLD_GCS_ENDPOINT", "http://x"), "tiered_storage.cold.gcs_endpoint"},
		{"gcs hot + gcs cold, sibling prefixes", cold("ARC_STORAGE_BACKEND", "gcs", "ARC_STORAGE_GCS_BUCKET", "b", "ARC_STORAGE_GCS_PREFIX", "hot",
			"ARC_TIERED_STORAGE_COLD_GCS_BUCKET", "b", "ARC_TIERED_STORAGE_COLD_GCS_PREFIX", "hot-other"), ""},
		{"gcs hot + gcs cold, same prefix", cold("ARC_STORAGE_BACKEND", "gcs", "ARC_STORAGE_GCS_BUCKET", "b", "ARC_STORAGE_GCS_PREFIX", "p",
			"ARC_TIERED_STORAGE_COLD_GCS_BUCKET", "b", "ARC_TIERED_STORAGE_COLD_GCS_PREFIX", "p"), "overlaps primary storage"},
		{"gcs hot at the bucket root, cold nested", cold("ARC_STORAGE_BACKEND", "gcs", "ARC_STORAGE_GCS_BUCKET", "b",
			"ARC_TIERED_STORAGE_COLD_GCS_BUCKET", "b", "ARC_TIERED_STORAGE_COLD_GCS_PREFIX", "cold"), "overlaps primary storage"},
		{"gcs hot nested in the cold prefix", cold("ARC_STORAGE_BACKEND", "gcs", "ARC_STORAGE_GCS_BUCKET", "b", "ARC_STORAGE_GCS_PREFIX", "c/hot",
			"ARC_TIERED_STORAGE_COLD_GCS_BUCKET", "b", "ARC_TIERED_STORAGE_COLD_GCS_PREFIX", "c"), "overlaps primary storage"},
		{"gcs hot vs S3-interop cold on the same bucket", map[string]string{
			"ARC_STORAGE_BACKEND": "gcs", "ARC_STORAGE_GCS_BUCKET": "b",
			"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "s3",
			"ARC_TIERED_STORAGE_COLD_S3_BUCKET": "b", "ARC_TIERED_STORAGE_COLD_S3_PREFIX": "cold",
			"ARC_TIERED_STORAGE_COLD_S3_ENDPOINT": "https://storage.googleapis.com",
		}, "overlaps primary storage"},
		// The cold endpoint is part of its identity: the same bucket name on
		// a store that is not Google's is another bucket.
		{"gcs hot + gcs cold on another store, same bucket and prefix", cold("ARC_STORAGE_BACKEND", "gcs", "ARC_STORAGE_GCS_BUCKET", "b", "ARC_STORAGE_GCS_PREFIX", "p",
			"ARC_TIERED_STORAGE_COLD_GCS_BUCKET", "b", "ARC_TIERED_STORAGE_COLD_GCS_PREFIX", "p",
			"ARC_TIERED_STORAGE_COLD_GCS_ENDPOINT", "https://gcs.example.internal"), ""},
		// Every backend, not only GCS: an S3 hot and S3 cold in one prefix.
		{"s3 hot + s3 cold, same prefix", map[string]string{
			"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b", "ARC_STORAGE_S3_PREFIX": "p",
			"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "s3",
			"ARC_TIERED_STORAGE_COLD_S3_BUCKET": "b", "ARC_TIERED_STORAGE_COLD_S3_PREFIX": "p",
		}, "overlaps primary storage"},
		{"s3 hot + s3 cold, other bucket", map[string]string{
			"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b",
			"ARC_TIERED_STORAGE_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "s3",
			"ARC_TIERED_STORAGE_COLD_S3_BUCKET": "c",
		}, ""},
		// Gated like the cold validation: tiering off ignores the cold block.
		{"overlap with tiering off is ignored", map[string]string{
			"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b",
			"ARC_TIERED_STORAGE_ENABLED": "false", "ARC_TIERED_STORAGE_COLD_ENABLED": "true", "ARC_TIERED_STORAGE_COLD_BACKEND": "s3",
			"ARC_TIERED_STORAGE_COLD_S3_BUCKET": "b",
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Load() error = %v", err)
				}
				if cfg.TieredStorage.Cold.Backend == "gcs" && cfg.TieredStorage.Cold.GCSBucket != strings.TrimSpace(tc.env["ARC_TIERED_STORAGE_COLD_GCS_BUCKET"]) {
					t.Fatalf("cold bucket = %q", cfg.TieredStorage.Cold.GCSBucket)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load() error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
