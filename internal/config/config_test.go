package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestGetDefaultThreadCount(t *testing.T) {
	expected := runtime.NumCPU()
	actual := getDefaultThreadCount()
	if actual != expected {
		t.Errorf("getDefaultThreadCount() = %d, want %d", actual, expected)
	}
}

func TestGetDefaultMaxConnections(t *testing.T) {
	cores := runtime.NumCPU()
	expected := cores * 2
	if expected < 4 {
		expected = 4
	}
	if expected > 64 {
		expected = 64
	}

	actual := getDefaultMaxConnections()
	if actual != expected {
		t.Errorf("getDefaultMaxConnections() = %d, want %d", actual, expected)
	}
}

func TestGetDefaultMaxConnections_Bounds(t *testing.T) {
	actual := getDefaultMaxConnections()
	if actual < 4 {
		t.Errorf("getDefaultMaxConnections() = %d, should be at least 4", actual)
	}
	if actual > 64 {
		t.Errorf("getDefaultMaxConnections() = %d, should be at most 64", actual)
	}
}

func TestGetDefaultMemoryLimit(t *testing.T) {
	result := getDefaultMemoryLimit()
	if result == "" {
		t.Error("getDefaultMemoryLimit() returned empty string")
	}
	// Should end with "GB"
	if len(result) < 3 || result[len(result)-2:] != "GB" {
		t.Errorf("getDefaultMemoryLimit() = %s, should end with 'GB'", result)
	}
}

func TestGetDefaultFlushWorkers(t *testing.T) {
	cores := runtime.NumCPU()
	expected := cores * 2
	if expected < 8 {
		expected = 8
	}
	if expected > 64 {
		expected = 64
	}

	actual := getDefaultFlushWorkers()
	if actual != expected {
		t.Errorf("getDefaultFlushWorkers() = %d, want %d", actual, expected)
	}
}

func TestGetDefaultFlushWorkers_Bounds(t *testing.T) {
	actual := getDefaultFlushWorkers()
	if actual < 8 {
		t.Errorf("getDefaultFlushWorkers() = %d, should be at least 8", actual)
	}
	if actual > 64 {
		t.Errorf("getDefaultFlushWorkers() = %d, should be at most 64", actual)
	}
}

func TestGetDefaultFlushQueueSize(t *testing.T) {
	workers := getDefaultFlushWorkers()
	expected := workers * 4
	if expected < 100 {
		expected = 100
	}

	actual := getDefaultFlushQueueSize()
	if actual != expected {
		t.Errorf("getDefaultFlushQueueSize() = %d, want %d", actual, expected)
	}
}

func TestGetDefaultFlushQueueSize_Bounds(t *testing.T) {
	actual := getDefaultFlushQueueSize()
	if actual < 100 {
		t.Errorf("getDefaultFlushQueueSize() = %d, should be at least 100", actual)
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
	expectedThreads := runtime.NumCPU()
	if cfg.Database.ThreadCount != expectedThreads {
		t.Errorf("Database.ThreadCount = %d, want %d", cfg.Database.ThreadCount, expectedThreads)
	}

	expectedConns := getDefaultMaxConnections()
	if cfg.Database.MaxConnections != expectedConns {
		t.Errorf("Database.MaxConnections = %d, want %d", cfg.Database.MaxConnections, expectedConns)
	}

	expectedMem := getDefaultMemoryLimit()
	if cfg.Database.MemoryLimit != expectedMem {
		t.Errorf("Database.MemoryLimit = %s, want %s", cfg.Database.MemoryLimit, expectedMem)
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
