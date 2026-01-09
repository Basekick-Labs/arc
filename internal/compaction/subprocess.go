package compaction

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	// Import DuckDB driver for subprocess
	_ "github.com/duckdb/duckdb-go/v2"
)

// SubprocessJobConfig is the serializable configuration passed to the subprocess.
// It contains all information needed to run a compaction job in isolation.
type SubprocessJobConfig struct {
	Database      string   `json:"database"`
	Measurement   string   `json:"measurement"`
	PartitionPath string   `json:"partition_path"`
	Files         []string `json:"files"`
	Tier          string   `json:"tier"`
	TargetSizeMB  int      `json:"target_size_mb"`
	TempDirectory string   `json:"temp_directory"`
	SortKeys      []string `json:"sort_keys"`    // Sort keys for ORDER BY in compaction
	MemoryLimit   string   `json:"memory_limit"` // DuckDB memory limit (e.g., "8GB")

	// Storage configuration
	StorageType   string `json:"storage_type"`   // "local" or "s3"
	StorageConfig string `json:"storage_config"` // JSON-encoded storage-specific config
}

// SubprocessJobResult is returned by the subprocess via stdout as JSON.
// It contains the outcome of the compaction job.
type SubprocessJobResult struct {
	Success        bool   `json:"success"`
	Error          string `json:"error,omitempty"`
	FilesCompacted int    `json:"files_compacted"`
	BytesBefore    int64  `json:"bytes_before"`
	BytesAfter     int64  `json:"bytes_after"`
	OutputFile     string `json:"output_file,omitempty"`
}

// RunSubprocessJob is called from the subprocess to execute compaction.
// It creates a new DuckDB connection, runs the job, and returns the result.
// When this function returns and the subprocess exits, all DuckDB memory is released.
//
// Memory profiling: Set ARC_COMPACTION_PROFILE=1 to enable heap profiling.
// Profiles are written to /tmp/arc_compaction_heap_before.pprof and /tmp/arc_compaction_heap_after.pprof
func RunSubprocessJob(config *SubprocessJobConfig) (*SubprocessJobResult, error) {
	// Setup logging to stderr (stdout is reserved for result JSON)
	logger := zerolog.New(os.Stderr).With().Timestamp().
		Str("component", "compaction-subprocess").
		Str("partition", config.PartitionPath).
		Logger()

	// Check if profiling is enabled
	profilingEnabled := os.Getenv("ARC_COMPACTION_PROFILE") == "1"
	if profilingEnabled {
		logger.Info().Msg("Memory profiling enabled - writing heap profiles to /tmp/")
		logMemStats(logger, "BEFORE compaction")
	}

	logger.Info().Msg("Starting compaction subprocess")

	// Create storage backend from config
	backend, err := createStorageBackendFromConfig(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage backend: %w", err)
	}
	defer backend.Close()

	// Create temporary DuckDB connection (in-memory)
	// This connection will be fully released when the subprocess exits
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer db.Close()

	// Set DuckDB memory limit from config.
	// This prevents OOM on servers without swap by forcing DuckDB to spill to disk.
	if config.MemoryLimit != "" {
		if _, err := db.Exec(fmt.Sprintf("SET memory_limit='%s'", config.MemoryLimit)); err != nil {
			logger.Warn().Err(err).Str("limit", config.MemoryLimit).Msg("Failed to set DuckDB memory limit")
		} else {
			logger.Info().Str("limit", config.MemoryLimit).Msg("DuckDB memory limit configured")
		}
	}

	// Create and run job
	job := NewJob(&JobConfig{
		Database:       config.Database,
		Measurement:    config.Measurement,
		PartitionPath:  config.PartitionPath,
		Files:          config.Files,
		StorageBackend: backend,
		TargetSizeMB:   config.TargetSizeMB,
		Tier:           config.Tier,
		TempDirectory:  config.TempDirectory,
		SortKeys:       config.SortKeys,
		Logger:         logger,
		DB:             db,
	})

	ctx := context.Background()
	err = job.Run(ctx)

	result := &SubprocessJobResult{
		Success:        err == nil,
		FilesCompacted: job.FilesCompacted,
		BytesBefore:    job.BytesBefore,
		BytesAfter:     job.BytesAfter,
	}
	if err != nil {
		result.Error = err.Error()
	}

	logger.Info().
		Bool("success", result.Success).
		Int("files_compacted", result.FilesCompacted).
		Int64("bytes_before", result.BytesBefore).
		Int64("bytes_after", result.BytesAfter).
		Msg("Compaction subprocess completed")

	// Write heap profile if profiling is enabled
	if profilingEnabled {
		logMemStats(logger, "AFTER compaction")
		writeHeapProfile(logger, "/tmp/arc_compaction_heap.pprof")
	}

	return result, nil
}

// logMemStats logs current memory statistics
func logMemStats(logger zerolog.Logger, label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logger.Info().
		Str("label", label).
		Uint64("alloc_mb", m.Alloc/1024/1024).
		Uint64("total_alloc_mb", m.TotalAlloc/1024/1024).
		Uint64("sys_mb", m.Sys/1024/1024).
		Uint64("heap_alloc_mb", m.HeapAlloc/1024/1024).
		Uint64("heap_sys_mb", m.HeapSys/1024/1024).
		Uint64("heap_inuse_mb", m.HeapInuse/1024/1024).
		Uint32("num_gc", m.NumGC).
		Msg("Memory stats")
}

// writeHeapProfile writes a heap profile to the specified path
func writeHeapProfile(logger zerolog.Logger, path string) {
	f, err := os.Create(path)
	if err != nil {
		logger.Error().Err(err).Str("path", path).Msg("Failed to create heap profile")
		return
	}
	defer f.Close()

	// Force GC before capturing profile for accurate view
	runtime.GC()

	if err := pprof.WriteHeapProfile(f); err != nil {
		logger.Error().Err(err).Msg("Failed to write heap profile")
		return
	}
	logger.Info().Str("path", path).Msg("Heap profile written")
}

// RunJobInSubprocess executes compaction in a subprocess for memory isolation.
// The subprocess runs the same binary with the "compact" subcommand.
// When the subprocess exits, all DuckDB memory (including jemalloc arenas) is released.
// extraEnv can be used to pass additional environment variables (e.g., credentials).
func RunJobInSubprocess(ctx context.Context, config *SubprocessJobConfig, logger zerolog.Logger, extraEnv ...string) (*SubprocessJobResult, error) {
	// Serialize config to JSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize job config: %w", err)
	}

	// Get path to current executable
	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to get executable path: %w", err)
	}

	// Build command: arc compact --job-stdin
	// Pass config via stdin to avoid "argument list too long" errors with many files
	cmd := exec.CommandContext(ctx, execPath, "compact", "--job-stdin")

	// Pass config via stdin
	cmd.Stdin = bytes.NewReader(configJSON)

	// Set extra environment variables (e.g., Azure credentials)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}

	// Capture stdout and stderr separately
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	logger.Info().
		Str("partition", config.PartitionPath).
		Int("files", len(config.Files)).
		Int("config_size_kb", len(configJSON)/1024).
		Msg("Starting compaction subprocess")

	// Run subprocess
	err = cmd.Run()

	// Log stderr (contains subprocess logs) - forward at INFO level for visibility
	if stderr.Len() > 0 {
		// Log each line separately for better formatting
		for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
			if line != "" {
				logger.Info().
					Str("subprocess", "compaction").
					Msg(line)
			}
		}
	}

	if err != nil {
		// Check if it's a context cancellation
		if ctx.Err() != nil {
			return nil, fmt.Errorf("subprocess cancelled: %w", ctx.Err())
		}
		return nil, fmt.Errorf("subprocess failed: %w (stderr: %s)", err, stderr.String())
	}

	// Parse result from stdout
	var result SubprocessJobResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("failed to parse subprocess result: %w (stdout: %s)", err, stdout.String())
	}

	logger.Debug().
		Bool("success", result.Success).
		Int("files_compacted", result.FilesCompacted).
		Msg("Subprocess completed")

	return &result, nil
}

// createStorageBackendFromConfig creates a storage backend from subprocess config
func createStorageBackendFromConfig(config *SubprocessJobConfig, logger zerolog.Logger) (storage.Backend, error) {
	switch config.StorageType {
	case "local":
		var localConfig struct {
			BasePath string `json:"base_path"`
		}
		if err := json.Unmarshal([]byte(config.StorageConfig), &localConfig); err != nil {
			return nil, fmt.Errorf("failed to parse local storage config: %w", err)
		}
		return storage.NewLocalBackend(localConfig.BasePath, logger)

	case "s3":
		var s3Config struct {
			Bucket    string `json:"bucket"`
			Region    string `json:"region"`
			Endpoint  string `json:"endpoint"`
			PathStyle bool   `json:"path_style"`
		}
		if err := json.Unmarshal([]byte(config.StorageConfig), &s3Config); err != nil {
			return nil, fmt.Errorf("failed to parse S3 storage config: %w", err)
		}
		return storage.NewS3Backend(&storage.S3Config{
			Bucket:    s3Config.Bucket,
			Region:    s3Config.Region,
			Endpoint:  s3Config.Endpoint,
			PathStyle: s3Config.PathStyle,
			// Credentials come from environment variables
		}, logger)

	case "azure":
		var azureConfig struct {
			Container   string `json:"container"`
			AccountName string `json:"account_name"`
			Endpoint    string `json:"endpoint"`
		}
		if err := json.Unmarshal([]byte(config.StorageConfig), &azureConfig); err != nil {
			return nil, fmt.Errorf("failed to parse Azure storage config: %w", err)
		}
		// Read credentials from environment variables (set by parent process)
		accountKey := os.Getenv("AZURE_STORAGE_KEY")
		return storage.NewAzureBlobBackend(&storage.AzureBlobConfig{
			ContainerName:      azureConfig.Container,
			AccountName:        azureConfig.AccountName,
			AccountKey:         accountKey,
			Endpoint:           azureConfig.Endpoint,
			UseManagedIdentity: accountKey == "", // Only use managed identity if no key provided
		}, logger)

	default:
		return nil, fmt.Errorf("unsupported storage type: %s", config.StorageType)
	}
}
