package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// escapeSQLPath escapes single quotes in file paths for safe SQL interpolation in DuckDB queries.
// This prevents SQL injection attacks from malicious filenames containing quotes.
func escapeSQLPath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

// validateParquetFile checks if a file is a valid Parquet file by checking magic bytes.
// This is a lightweight validation that doesn't load the file into memory (unlike DuckDB read_parquet).
// Parquet files must have "PAR1" magic bytes at both the start and end of the file.
func validateParquetFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Parquet files need at least 12 bytes (4 byte header + 4 byte footer + 4 byte metadata length)
	if stat.Size() < 12 {
		return fmt.Errorf("file too small to be valid parquet (%d bytes)", stat.Size())
	}

	// Check magic bytes "PAR1" at start
	magic := make([]byte, 4)
	if _, err := file.Read(magic); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	if string(magic) != "PAR1" {
		return fmt.Errorf("invalid parquet magic header: got %q", magic)
	}

	// Check magic bytes "PAR1" at end
	if _, err := file.Seek(-4, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to footer: %w", err)
	}
	if _, err := file.Read(magic); err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}
	if string(magic) != "PAR1" {
		return fmt.Errorf("invalid parquet magic footer: got %q", magic)
	}

	return nil
}

// buildOrderByClause builds an ORDER BY clause from sort keys.
// Returns an empty string if no sort keys, or "ORDER BY col1, col2, ..." if sort keys exist.
// Column names are quoted to handle special characters.
func buildOrderByClause(sortKeys []string) string {
	if len(sortKeys) == 0 {
		return ""
	}

	var quotedKeys []string
	for _, key := range sortKeys {
		// Quote column names to handle special characters and reserved words
		quotedKeys = append(quotedKeys, fmt.Sprintf(`"%s"`, key))
	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(quotedKeys, ", "))
}

// JobStatus represents the status of a compaction job
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
)

// Job represents a single compaction job for a partition
type Job struct {
	// Configuration
	Measurement    string
	PartitionPath  string
	Files          []string // Original files to be compacted
	StorageBackend storage.Backend
	Database       string
	TargetSizeMB   int
	Tier           string
	TempDirectory  string   // Base temp directory for compaction files
	SortKeys       []string // Sort keys for this measurement (for ORDER BY in compaction)

	// Job metadata
	JobID       string
	StartedAt   *time.Time
	CompletedAt *time.Time
	Status      JobStatus
	Error       error

	// Metrics
	FilesCompacted  int
	BytesBefore     int64
	BytesAfter      int64
	DurationSeconds float64

	// Internal
	logger         zerolog.Logger
	mu             sync.Mutex
	db             *sql.DB  // Shared DuckDB connection
	compactedFiles []string // Files that were actually compacted (valid files only)
}

// JobConfig holds configuration for creating a compaction job
type JobConfig struct {
	Measurement    string
	PartitionPath  string
	Files          []string
	StorageBackend storage.Backend
	Database       string
	TargetSizeMB   int
	Tier           string
	TempDirectory  string   // Base temp directory for compaction files (default: ./data/compaction)
	SortKeys       []string // Sort keys for this measurement (for ORDER BY in compaction)
	Logger         zerolog.Logger
	DB             *sql.DB // Shared DuckDB connection (avoids memory retention from temp connections)
}

// NewJob creates a new compaction job
func NewJob(cfg *JobConfig) *Job {
	// Generate unique job ID including database to prevent collisions
	// across different databases with same partition paths
	jobID := fmt.Sprintf("%s_%s_%d",
		cfg.Database,
		strings.ReplaceAll(cfg.PartitionPath, "/", "_"),
		time.Now().UnixNano(),
	)

	// Use default temp directory if not specified
	tempDir := cfg.TempDirectory
	if tempDir == "" {
		tempDir = "./data/compaction"
	}

	// Use default sort keys if not provided
	sortKeys := cfg.SortKeys
	if sortKeys == nil {
		sortKeys = []string{"time"} // Default to time-only sorting
	}

	return &Job{
		Measurement:    cfg.Measurement,
		PartitionPath:  cfg.PartitionPath,
		Files:          cfg.Files,
		StorageBackend: cfg.StorageBackend,
		Database:       cfg.Database,
		TargetSizeMB:   cfg.TargetSizeMB,
		Tier:           cfg.Tier,
		TempDirectory:  tempDir,
		SortKeys:       sortKeys,
		JobID:          jobID,
		Status:         JobStatusPending,
		logger:         cfg.Logger.With().Str("job_id", jobID).Logger(),
		db:             cfg.DB,
	}
}

// Run executes the compaction job
func (j *Job) Run(ctx context.Context) error {
	j.mu.Lock()
	now := time.Now()
	j.StartedAt = &now
	j.Status = JobStatusRunning
	j.mu.Unlock()

	j.logger.Info().
		Str("database", j.Database).
		Str("partition", j.PartitionPath).
		Int("file_count", len(j.Files)).
		Msg("Starting compaction job")

	// Create temp directory for this job using configured base path
	tempDir := filepath.Join(j.TempDirectory, j.JobID)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return j.fail(fmt.Errorf("failed to create temp directory: %w", err))
	}
	defer j.cleanupTemp(tempDir)

	// Download files to temp directory
	downloadedFiles, err := j.downloadFiles(ctx, tempDir)
	if err != nil {
		return j.fail(fmt.Errorf("failed to download files: %w", err))
	}

	if len(downloadedFiles) == 0 {
		j.logger.Info().Msg("All files already compacted, skipping")
		return j.complete()
	}

	j.logger.Info().
		Int("file_count", len(downloadedFiles)).
		Int64("total_bytes", j.BytesBefore).
		Msg("Downloaded files for compaction")

	// Compact using DuckDB - this will set j.compactedFiles with only the valid files
	compactedFile, err := j.compactFiles(ctx, downloadedFiles, tempDir)

	// MEMORY OPTIMIZATION: Clear downloadedFiles slice after compaction.
	// This allows GC to reclaim memory from the file metadata before upload/delete phases.
	downloadedFiles = nil

	if err != nil {
		return j.fail(fmt.Errorf("failed to compact files: %w", err))
	}

	// Get compacted file size
	info, err := os.Stat(compactedFile)
	if err != nil {
		return j.fail(fmt.Errorf("failed to stat compacted file: %w", err))
	}
	j.BytesAfter = info.Size()

	j.logger.Info().
		Str("file", filepath.Base(compactedFile)).
		Int64("bytes", j.BytesAfter).
		Msg("Compacted file created")

	// Upload compacted file
	compactedKey := filepath.Join(j.PartitionPath, filepath.Base(compactedFile))
	if err := j.uploadFile(ctx, compactedFile, compactedKey); err != nil {
		return j.fail(fmt.Errorf("failed to upload compacted file: %w", err))
	}

	// Delete old files from storage
	if err := j.deleteOldFiles(ctx); err != nil {
		j.logger.Warn().Err(err).Msg("Failed to delete some old files")
		// Don't fail the job for deletion errors
	}

	// Cleanup empty directories (best-effort, local storage only)
	j.cleanupEmptyDirectories(ctx)

	return j.complete()
}

// downloadedFile tracks a downloaded file with its original storage key and local path
type downloadedFile struct {
	storageKey string // Original storage key
	localPath  string // Local file path after download
	size       int64  // File size in bytes
}

// downloadTask represents a file download task for parallel processing
type downloadTask struct {
	index   int
	fileKey string
}

// downloadResult represents the result of a file download task
type downloadResult struct {
	index   int
	file    *downloadedFile
	skipped bool
	err     error
}

// downloadWorkers is the number of concurrent download workers
const downloadWorkers = 4

// downloadFiles downloads files from storage to the temp directory using parallel workers.
// Returns downloaded files info and any error encountered.
func (j *Job) downloadFiles(ctx context.Context, tempDir string) ([]downloadedFile, error) {
	if len(j.Files) == 0 {
		return nil, nil
	}

	// Create channels for task distribution and result collection
	tasks := make(chan downloadTask, len(j.Files))
	results := make(chan downloadResult, len(j.Files))

	// Determine number of workers (don't use more workers than files)
	numWorkers := downloadWorkers
	if len(j.Files) < numWorkers {
		numWorkers = len(j.Files)
	}

	// Start download workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				result := j.downloadSingleFile(ctx, tempDir, task.index, task.fileKey)
				results <- result
			}
		}()
	}

	// Send tasks to workers
	for i, fileKey := range j.Files {
		tasks <- downloadTask{index: i, fileKey: fileKey}
	}
	close(tasks)

	// Wait for all workers to finish and close results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results, maintaining order
	downloadedFiles := make([]*downloadedFile, len(j.Files))
	var skippedCount int
	var firstError error

	for result := range results {
		if result.err != nil && firstError == nil {
			firstError = result.err
			// Continue collecting results to let workers finish
			continue
		}
		if result.skipped {
			skippedCount++
			continue
		}
		if result.file != nil {
			downloadedFiles[result.index] = result.file
		}
	}

	// Return first error encountered
	if firstError != nil {
		return nil, firstError
	}

	// Filter out nil entries (skipped files) and calculate total size
	var finalFiles []downloadedFile
	var totalSize int64
	for _, df := range downloadedFiles {
		if df != nil {
			finalFiles = append(finalFiles, *df)
			totalSize += df.size
		}
	}

	j.BytesBefore = totalSize

	if skippedCount > 0 {
		j.logger.Info().Int("skipped", skippedCount).Msg("Skipped already-compacted files")
	}

	return finalFiles, nil
}

// downloadSingleFile downloads a single file from storage
func (j *Job) downloadSingleFile(ctx context.Context, tempDir string, index int, fileKey string) downloadResult {
	// Check for cancellation
	select {
	case <-ctx.Done():
		return downloadResult{index: index, err: ctx.Err()}
	default:
	}

	localPath := filepath.Join(tempDir, filepath.Base(fileKey))

	// Read file from storage
	data, err := j.StorageBackend.Read(ctx, fileKey)
	if err != nil {
		// Check if file doesn't exist (already compacted)
		exists, checkErr := j.StorageBackend.Exists(ctx, fileKey)
		if checkErr == nil && !exists {
			j.logger.Debug().Str("file", fileKey).Msg("File not found (already compacted), skipping")
			return downloadResult{index: index, skipped: true}
		}
		return downloadResult{index: index, err: fmt.Errorf("failed to read %s: %w", fileKey, err)}
	}

	// Write to local file
	if err := os.WriteFile(localPath, data, 0644); err != nil {
		return downloadResult{index: index, err: fmt.Errorf("failed to write %s: %w", localPath, err)}
	}

	return downloadResult{
		index: index,
		file: &downloadedFile{
			storageKey: fileKey,
			localPath:  localPath,
			size:       int64(len(data)),
		},
	}
}

// compactFiles merges multiple Parquet files into one using DuckDB.
// It validates each file and only compacts valid ones, storing the list of
// successfully compacted files' storage keys in j.compactedFiles.
func (j *Job) compactFiles(ctx context.Context, files []downloadedFile, tempDir string) (string, error) {
	// Generate output filename with tier-specific suffix
	timestamp := time.Now().Format("20060102_150405")
	suffix := "compacted"
	if j.Tier != "hourly" {
		suffix = j.Tier
	}
	outputFile := filepath.Join(tempDir, fmt.Sprintf("%s_%s_%s.parquet", j.Measurement, timestamp, suffix))

	// Use the shared DuckDB connection instead of creating a new one
	// This prevents memory retention from DuckDB's jemalloc not releasing memory on Close()
	db := j.db
	if db == nil {
		return "", fmt.Errorf("no DuckDB connection provided for compaction")
	}

	// Validate each file first and track which ones are valid
	// MEMORY OPTIMIZATION: Use lightweight parquet magic byte check instead of DuckDB read_parquet().
	// DuckDB's read_parquet() loads the entire file into memory for validation, which causes
	// massive memory consumption when validating hundreds of files. Magic byte check only reads 8 bytes.
	var validLocalPaths []string
	var validStorageKeys []string
	for _, df := range files {
		if err := validateParquetFile(df.localPath); err != nil {
			j.logger.Error().Err(err).Str("file", filepath.Base(df.localPath)).Msg("Skipping corrupted file")
			continue
		}
		validLocalPaths = append(validLocalPaths, df.localPath)
		validStorageKeys = append(validStorageKeys, df.storageKey)
	}

	if len(validLocalPaths) == 0 {
		return "", fmt.Errorf("no valid parquet files found")
	}

	j.logger.Info().
		Int("valid", len(validLocalPaths)).
		Int("total", len(files)).
		Msg("Validated files for compaction")

	// Build file list for DuckDB with escaped paths to prevent SQL injection
	var fileListSQL string
	if len(validLocalPaths) == 1 {
		fileListSQL = fmt.Sprintf("'%s'", escapeSQLPath(validLocalPaths[0]))
	} else {
		fileListSQL = "["
		for i, f := range validLocalPaths {
			if i > 0 {
				fileListSQL += ", "
			}
			fileListSQL += fmt.Sprintf("'%s'", escapeSQLPath(f))
		}
		fileListSQL += "]"
	}

	// Build ORDER BY clause from sort keys
	// This ensures compacted files maintain the same sort order as ingested files
	orderByClause := buildOrderByClause(j.SortKeys)

	// Execute compaction query
	// Uses union_by_name=true to handle schema evolution
	// Sorts by configured keys to maintain compression benefits
	escapedOutputFile := escapeSQLPath(outputFile)
	query := fmt.Sprintf(`
		COPY (
			SELECT * FROM read_parquet(%s, union_by_name=true)
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, fileListSQL, orderByClause, escapedOutputFile)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return "", fmt.Errorf("failed to execute compaction query: %w", err)
	}

	// Store the list of files that were actually compacted (for safe deletion)
	j.compactedFiles = validStorageKeys
	j.FilesCompacted = len(validLocalPaths)
	return outputFile, nil
}

// uploadFile uploads a file to storage
func (j *Job) uploadFile(ctx context.Context, localPath, key string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("failed to read local file: %w", err)
	}
	return j.StorageBackend.Write(ctx, key, data)
}

// deleteOldFiles removes only the files that were actually compacted from storage.
// This ensures we don't delete files that were skipped due to corruption or other issues.
func (j *Job) deleteOldFiles(ctx context.Context) error {
	if len(j.compactedFiles) == 0 {
		j.logger.Debug().Msg("No files to delete (none were compacted)")
		return nil
	}

	var lastErr error
	var deleted, failed int
	for _, fileKey := range j.compactedFiles {
		if err := j.StorageBackend.Delete(ctx, fileKey); err != nil {
			j.logger.Warn().Err(err).Str("file", fileKey).Msg("Failed to delete old file")
			lastErr = err
			failed++
		} else {
			j.logger.Debug().Str("file", fileKey).Msg("Deleted old file")
			deleted++
		}
	}

	j.logger.Info().
		Int("deleted", deleted).
		Int("failed", failed).
		Int("total", len(j.compactedFiles)).
		Msg("Completed deletion of old files")

	return lastErr
}

// cleanupTemp removes the temporary directory
func (j *Job) cleanupTemp(tempDir string) {
	if err := os.RemoveAll(tempDir); err != nil {
		j.logger.Warn().Err(err).Str("dir", tempDir).Msg("Failed to cleanup temp directory")
	} else {
		j.logger.Debug().Str("dir", tempDir).Msg("Cleaned up temp directory")
	}
}

// complete marks the job as completed
func (j *Job) complete() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	now := time.Now()
	j.CompletedAt = &now
	j.Status = JobStatusCompleted
	j.DurationSeconds = now.Sub(*j.StartedAt).Seconds()

	compressionRatio := float64(0)
	if j.BytesBefore > 0 {
		compressionRatio = (1 - float64(j.BytesAfter)/float64(j.BytesBefore)) * 100
	}

	// Record metrics
	m := metrics.Get()
	m.IncCompactionJobs()
	m.IncCompactionSuccess()
	m.IncCompactionFilesCompacted(int64(j.FilesCompacted))
	m.IncCompactionBytesRead(j.BytesBefore)
	m.IncCompactionBytesWritten(j.BytesAfter)

	j.logger.Info().
		Int("files_compacted", j.FilesCompacted).
		Int64("bytes_before", j.BytesBefore).
		Int64("bytes_after", j.BytesAfter).
		Float64("compression_ratio", compressionRatio).
		Float64("duration_seconds", j.DurationSeconds).
		Msg("Compaction job completed")

	return nil
}

// fail marks the job as failed
func (j *Job) fail(err error) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	now := time.Now()
	j.CompletedAt = &now
	j.Status = JobStatusFailed
	j.Error = err

	if j.StartedAt != nil {
		j.DurationSeconds = now.Sub(*j.StartedAt).Seconds()
	}

	// Record metrics
	m := metrics.Get()
	m.IncCompactionJobs()
	m.IncCompactionFailed()

	j.logger.Error().Err(err).Msg("Compaction job failed")

	return err
}

// Stats returns job statistics
func (j *Job) Stats() map[string]interface{} {
	j.mu.Lock()
	defer j.mu.Unlock()

	stats := map[string]interface{}{
		"job_id":           j.JobID,
		"database":         j.Database,
		"measurement":      j.Measurement,
		"partition_path":   j.PartitionPath,
		"status":           string(j.Status),
		"files_compacted":  j.FilesCompacted,
		"bytes_before":     j.BytesBefore,
		"bytes_after":      j.BytesAfter,
		"duration_seconds": j.DurationSeconds,
		"tier":             j.Tier,
		"sort_keys":        j.SortKeys,
	}

	if j.BytesBefore > 0 {
		stats["compression_ratio"] = 1 - float64(j.BytesAfter)/float64(j.BytesBefore)
	}

	if j.StartedAt != nil {
		stats["started_at"] = j.StartedAt.Format(time.RFC3339)
	}
	if j.CompletedAt != nil {
		stats["completed_at"] = j.CompletedAt.Format(time.RFC3339)
	}
	if j.Error != nil {
		stats["error"] = j.Error.Error()
	}

	return stats
}

// cleanupEmptyDirectories attempts to remove empty directories after file deletion.
// Only works with storage backends that implement DirectoryRemover (e.g., LocalBackend).
// This is best-effort: errors are logged but don't fail the job.
func (j *Job) cleanupEmptyDirectories(ctx context.Context) {
	// Check if backend supports directory removal
	remover, ok := j.StorageBackend.(storage.DirectoryRemover)
	if !ok {
		j.logger.Debug().Msg("Storage backend does not support directory removal, skipping cleanup")
		return
	}

	// Collect unique directories from compacted files
	dirs := make(map[string]struct{})
	for _, fileKey := range j.compactedFiles {
		dir := filepath.Dir(fileKey)
		dirs[dir] = struct{}{}
	}

	if len(dirs) == 0 {
		return
	}

	// Try to remove each directory and walk up the tree
	var removed int
	for dir := range dirs {
		removed += j.removeDirectoryTree(ctx, remover, dir)
	}

	if removed > 0 {
		j.logger.Info().Int("directories_removed", removed).Msg("Cleaned up empty directories")
	}
}

// removeDirectoryTree attempts to remove a directory and its empty parents.
// Returns the number of directories successfully removed.
// Stops at the measurement level (database/measurement) to preserve the structure.
func (j *Job) removeDirectoryTree(ctx context.Context, remover storage.DirectoryRemover, dir string) int {
	// Path structure: database/measurement/YYYY/MM/DD/HH
	// Stop at the measurement level (don't delete measurement/database dirs)
	parts := strings.Split(dir, "/")
	if len(parts) <= 2 {
		return 0 // Don't remove database or measurement directories
	}

	if err := remover.RemoveDirectory(ctx, dir); err != nil {
		j.logger.Debug().Err(err).Str("dir", dir).Msg("Could not remove directory (may not be empty)")
		return 0 // Stop walking up if we can't remove this level
	}

	j.logger.Debug().Str("dir", dir).Msg("Removed empty directory")

	// Try parent directory
	parent := filepath.Dir(dir)
	return 1 + j.removeDirectoryTree(ctx, remover, parent)
}
