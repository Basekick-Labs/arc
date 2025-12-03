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
