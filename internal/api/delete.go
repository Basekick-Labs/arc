package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// DeleteHandler handles delete operations using file rewrite strategy
type DeleteHandler struct {
	db      *database.DuckDB
	storage storage.Backend
	config  *config.DeleteConfig
	logger  zerolog.Logger
}

// DeleteRequest represents a delete operation request
type DeleteRequest struct {
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
	Where       string `json:"where"`
	DryRun      bool   `json:"dry_run"`
	Confirm     bool   `json:"confirm"`
}

// DeleteResponse represents a delete operation response
type DeleteResponse struct {
	Success         bool     `json:"success"`
	DeletedCount    int64    `json:"deleted_count"`
	AffectedFiles   int      `json:"affected_files"`
	RewrittenFiles  int      `json:"rewritten_files"`
	ExecutionTimeMs float64  `json:"execution_time_ms"`
	DryRun          bool     `json:"dry_run"`
	FilesProcessed  []string `json:"files_processed"`
	Error           string   `json:"error,omitempty"`
}

// DeleteConfigResponse represents delete configuration info
type DeleteConfigResponse struct {
	Enabled               bool              `json:"enabled"`
	ConfirmationThreshold int               `json:"confirmation_threshold"`
	MaxRowsPerDelete      int               `json:"max_rows_per_delete"`
	Implementation        string            `json:"implementation"`
	PerformanceImpact     map[string]string `json:"performance_impact"`
}

// affectedFile holds info about a file that has matching rows
type affectedFile struct {
	path         string
	matchCount   int64
	relativePath string
}

// Dangerous patterns for WHERE clause validation
var dangerousWherePatterns = []string{
	";",      // Statement terminator
	"--",     // SQL comment
	"/*",     // Multi-line comment
	"DROP",   // DDL
	"DELETE", // DML (in WHERE context means injection attempt)
	"INSERT",
	"UPDATE",
	"EXEC",
	"EXECUTE",
	"xp_",
	"sp_",
}

// NewDeleteHandler creates a new delete handler
func NewDeleteHandler(db *database.DuckDB, storage storage.Backend, cfg *config.DeleteConfig, logger zerolog.Logger) *DeleteHandler {
	return &DeleteHandler{
		db:      db,
		storage: storage,
		config:  cfg,
		logger:  logger.With().Str("component", "delete-handler").Logger(),
	}
}

// RegisterRoutes registers delete endpoints
func (h *DeleteHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/delete", h.handleDelete)
	app.Get("/api/v1/delete/config", h.handleGetConfig)
}

// handleGetConfig returns the current delete configuration
func (h *DeleteHandler) handleGetConfig(c *fiber.Ctx) error {
	return c.JSON(DeleteConfigResponse{
		Enabled:               h.config.Enabled,
		ConfirmationThreshold: h.config.ConfirmationThreshold,
		MaxRowsPerDelete:      h.config.MaxRowsPerDelete,
		Implementation:        "rewrite-based",
		PerformanceImpact: map[string]string{
			"writes":  "zero overhead",
			"queries": "zero overhead",
			"deletes": "expensive (file rewrites)",
		},
	})
}

// handleDelete processes delete requests
func (h *DeleteHandler) handleDelete(c *fiber.Ctx) error {
	start := time.Now()

	// Check if delete operations are enabled
	if !h.config.Enabled {
		return c.Status(fiber.StatusForbidden).JSON(DeleteResponse{
			Success: false,
			Error:   "Delete operations are disabled. Set delete.enabled=true in arc.toml to enable.",
		})
	}

	// Parse request
	var req DeleteRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "Invalid request body: " + err.Error(),
		})
	}

	// Validate required fields
	if req.Database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "database is required",
		})
	}
	if req.Measurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "measurement is required",
		})
	}

	// Validate WHERE clause
	isFullTableDelete, err := h.validateWhereClause(req.Where)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   err.Error(),
		})
	}

	// Check if confirmation is required for full table delete
	if isFullTableDelete && !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "Full table delete detected (WHERE 1=1). Set confirm=true to proceed.",
		})
	}

	// Require confirmation for non-dry-run operations
	if !req.DryRun && !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "Confirmation required for delete operation. Set confirm=true or use dry_run=true to preview.",
		})
	}

	h.logger.Info().
		Str("database", req.Database).
		Str("measurement", req.Measurement).
		Str("where", req.Where).
		Bool("dry_run", req.DryRun).
		Msg("Processing delete request")

	// Find affected files
	ctx := context.Background()
	affected, err := h.findAffectedFiles(ctx, req.Database, req.Measurement, req.Where)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to find affected files")
		return c.Status(fiber.StatusInternalServerError).JSON(DeleteResponse{
			Success:         false,
			Error:           "Failed to find affected files: " + err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
		})
	}

	if len(affected) == 0 {
		return c.JSON(DeleteResponse{
			Success:         true,
			DeletedCount:    0,
			AffectedFiles:   0,
			RewrittenFiles:  0,
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			DryRun:          req.DryRun,
			FilesProcessed:  []string{},
		})
	}

	// Calculate total rows to delete
	var totalToDelete int64
	for _, f := range affected {
		totalToDelete += f.matchCount
	}

	// Check max rows limit
	if totalToDelete > int64(h.config.MaxRowsPerDelete) {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error: fmt.Sprintf("Delete would affect %d rows, exceeding maximum of %d. "+
				"Adjust delete.max_rows_per_delete in arc.toml or refine WHERE clause.",
				totalToDelete, h.config.MaxRowsPerDelete),
		})
	}

	// Check confirmation threshold
	if totalToDelete > int64(h.config.ConfirmationThreshold) && !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error: fmt.Sprintf("Delete would affect %d rows (threshold: %d). Set confirm=true to proceed.",
				totalToDelete, h.config.ConfirmationThreshold),
		})
	}

	h.logger.Info().
		Int("affected_files", len(affected)).
		Int64("total_rows", totalToDelete).
		Msg("Found affected files")

	// If dry run, just return stats
	if req.DryRun {
		fileNames := make([]string, len(affected))
		for i, f := range affected {
			fileNames[i] = filepath.Base(f.path)
		}

		return c.JSON(DeleteResponse{
			Success:         true,
			DeletedCount:    totalToDelete,
			AffectedFiles:   len(affected),
			RewrittenFiles:  0,
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			DryRun:          true,
			FilesProcessed:  fileNames,
		})
	}

	// Execute actual deletion (rewrite files)
	h.logger.Info().
		Int("file_count", len(affected)).
		Int64("rows", totalToDelete).
		Msg("Rewriting files to remove rows")

	var totalDeleted int64
	var rewrittenCount int
	var processedFiles []string

	for _, f := range affected {
		deleted, err := h.rewriteFileWithoutDeletedRows(ctx, f.path, f.relativePath, req.Where)
		if err != nil {
			h.logger.Error().Err(err).Str("file", f.path).Msg("Failed to rewrite file")
			continue
		}

		totalDeleted += deleted
		rewrittenCount++
		processedFiles = append(processedFiles, filepath.Base(f.path))

		h.logger.Debug().
			Str("file", filepath.Base(f.path)).
			Int64("deleted", deleted).
			Msg("Processed file")
	}

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int64("deleted_count", totalDeleted).
		Int("rewritten_files", rewrittenCount).
		Float64("execution_time_ms", executionTime).
		Msg("Delete operation completed")

	return c.JSON(DeleteResponse{
		Success:         true,
		DeletedCount:    totalDeleted,
		AffectedFiles:   len(affected),
		RewrittenFiles:  rewrittenCount,
		ExecutionTimeMs: executionTime,
		DryRun:          false,
		FilesProcessed:  processedFiles,
	})
}

// validateWhereClause validates the WHERE clause and returns true if it's a full table delete
func (h *DeleteHandler) validateWhereClause(where string) (bool, error) {
	if where == "" || strings.TrimSpace(where) == "" {
		return false, fmt.Errorf("WHERE clause is required. To delete all data, use WHERE clause '1=1' with confirm=true")
	}

	whereUpper := strings.ToUpper(strings.TrimSpace(where))

	// Remove "WHERE" prefix if present
	if strings.HasPrefix(whereUpper, "WHERE ") {
		whereUpper = strings.TrimSpace(whereUpper[6:])
	}

	// Check for dangerous patterns
	for _, pattern := range dangerousWherePatterns {
		if strings.Contains(whereUpper, pattern) {
			return false, fmt.Errorf("WHERE clause contains forbidden keyword: %s", pattern)
		}
	}

	// Check for unmatched quotes
	if strings.Count(where, "'")%2 != 0 {
		return false, fmt.Errorf("WHERE clause has unmatched quotes")
	}

	// Check for unmatched parentheses
	if strings.Count(where, "(") != strings.Count(where, ")") {
		return false, fmt.Errorf("WHERE clause has unmatched parentheses")
	}

	// Check for dangerous full table delete patterns
	dangerousPatterns := []string{"1=1", "TRUE", "1"}
	for _, pattern := range dangerousPatterns {
		if whereUpper == pattern {
			return true, nil // Full table delete
		}
	}

	return false, nil
}

// findAffectedFiles finds all Parquet files that contain rows matching the WHERE clause
func (h *DeleteHandler) findAffectedFiles(ctx context.Context, database, measurement, whereClause string) ([]affectedFile, error) {
	var affected []affectedFile

	// Use storage backend's List method to find parquet files
	prefix := fmt.Sprintf("%s/%s/", database, measurement)
	files, err := h.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// Filter for parquet files and convert to query paths
	var parquetFiles []fileInfo
	for _, f := range files {
		if strings.HasSuffix(strings.ToLower(f), ".parquet") {
			parquetFiles = append(parquetFiles, fileInfo{
				queryPath:    h.getQueryPath(f),
				relativePath: f,
			})
		}
	}

	if len(parquetFiles) == 0 {
		h.logger.Warn().Str("prefix", prefix).Msg("No parquet files found for measurement")
		return affected, nil
	}

	h.logger.Info().Int("file_count", len(parquetFiles)).Msg("Scanning parquet files for matching rows")

	// Optimized: Use batch query to count matching rows across all files in one query
	// This replaces N individual queries with a single query using read_parquet with file list
	affected, err = h.countMatchingRowsInFiles(ctx, parquetFiles, whereClause)
	if err != nil {
		// Fallback to individual queries if batch fails (e.g., schema mismatch)
		h.logger.Warn().Err(err).Msg("Batch count failed, falling back to individual queries")
		return h.countMatchingRowsIndividually(ctx, parquetFiles, whereClause)
	}

	return affected, nil
}

// fileInfo holds query and relative paths for a parquet file
type fileInfo struct {
	queryPath    string
	relativePath string
}

// countMatchingRowsInFiles counts matching rows across multiple files in a single query
func (h *DeleteHandler) countMatchingRowsInFiles(ctx context.Context, files []fileInfo, whereClause string) ([]affectedFile, error) {
	if len(files) == 0 {
		return nil, nil
	}

	db := h.db.DB()

	// Build array of file paths for DuckDB
	var pathList strings.Builder
	pathList.WriteString("[")
	for i, f := range files {
		if i > 0 {
			pathList.WriteString(", ")
		}
		pathList.WriteString("'")
		pathList.WriteString(f.queryPath)
		pathList.WriteString("'")
	}
	pathList.WriteString("]")

	// Single query to get counts per file using filename column
	query := fmt.Sprintf(`
		SELECT filename, COUNT(*) as match_count
		FROM read_parquet(%s, filename=true, union_by_name=true)
		WHERE %s
		GROUP BY filename
		HAVING COUNT(*) > 0`,
		pathList.String(), whereClause)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("batch count query failed: %w", err)
	}
	defer rows.Close()

	// Build map of query path -> relative path for lookup
	pathMap := make(map[string]string)
	for _, f := range files {
		pathMap[f.queryPath] = f.relativePath
	}

	var affected []affectedFile
	for rows.Next() {
		var filename string
		var count int64
		if err := rows.Scan(&filename, &count); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		relativePath, ok := pathMap[filename]
		if !ok {
			// Try to find by suffix match (DuckDB may return absolute paths)
			for qp, rp := range pathMap {
				if strings.HasSuffix(filename, filepath.Base(qp)) {
					relativePath = rp
					ok = true
					break
				}
			}
		}
		if !ok {
			h.logger.Warn().Str("filename", filename).Msg("Could not map filename to relative path")
			continue
		}

		affected = append(affected, affectedFile{
			path:         filename,
			matchCount:   count,
			relativePath: relativePath,
		})
		h.logger.Debug().Str("file", filepath.Base(relativePath)).Int64("matches", count).Msg("Found matching rows")
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return affected, nil
}

// countMatchingRowsIndividually is the fallback when batch query fails
func (h *DeleteHandler) countMatchingRowsIndividually(ctx context.Context, files []fileInfo, whereClause string) ([]affectedFile, error) {
	var affected []affectedFile
	db := h.db.DB()

	for _, f := range files {
		query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s') WHERE %s", f.queryPath, whereClause)
		var count int64
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			h.logger.Warn().Err(err).Str("file", f.relativePath).Msg("Failed to count matching rows, skipping file")
			continue
		}

		if count > 0 {
			affected = append(affected, affectedFile{
				path:         f.queryPath,
				matchCount:   count,
				relativePath: f.relativePath,
			})
			h.logger.Debug().Str("file", filepath.Base(f.relativePath)).Int64("matches", count).Msg("Found matching rows")
		}
	}

	return affected, nil
}

// rewriteFileWithoutDeletedRows rewrites a Parquet file excluding rows that match the WHERE clause
// For local storage: uses atomic rename
// For S3: downloads, processes locally, then uploads
func (h *DeleteHandler) rewriteFileWithoutDeletedRows(ctx context.Context, queryPath, relativePath, whereClause string) (int64, error) {
	// Use the shared DuckDB connection to avoid memory retention from temporary connections
	db := h.db.DB()

	// Optimized: Single query to count both total rows and rows to keep
	// Uses COUNT(*) FILTER to get conditional count in one scan
	var rowsBefore, rowsAfter int64
	countQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE NOT (%s)) as remaining
		FROM read_parquet('%s')`,
		whereClause, queryPath)

	if err := db.QueryRowContext(ctx, countQuery).Scan(&rowsBefore, &rowsAfter); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	deleted := rowsBefore - rowsAfter

	// If all rows would be deleted, just delete the file
	if rowsAfter == 0 {
		h.logger.Info().Str("file", filepath.Base(relativePath)).Int64("deleted", deleted).Msg("All rows deleted, removing file")
		if err := h.storage.Delete(ctx, relativePath); err != nil {
			return 0, fmt.Errorf("failed to delete file: %w", err)
		}
		return deleted, nil
	}

	// For S3, we need to write to a temp file locally, then upload
	if h.isS3Backend() {
		return h.rewriteS3File(ctx, queryPath, relativePath, whereClause, rowsBefore, rowsAfter)
	}

	// Local storage: use atomic rename strategy
	return h.rewriteLocalFile(ctx, queryPath, relativePath, whereClause, rowsBefore, rowsAfter)
}

// rewriteLocalFile handles file rewrite for local storage using atomic rename
func (h *DeleteHandler) rewriteLocalFile(ctx context.Context, filePath, _, whereClause string, rowsBefore, rowsAfter int64) (int64, error) {
	db := h.db.DB()
	deleted := rowsBefore - rowsAfter

	// Create temp file for the rewritten data
	dir := filepath.Dir(filePath)
	tempDir := filepath.Join(dir, ".tmp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create temp directory: %w", err)
	}

	tempFile := filepath.Join(tempDir, filepath.Base(filePath)+".new")

	// Write filtered data to temp file using DuckDB COPY
	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * FROM read_parquet('%s') WHERE NOT (%s)
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3
		)
	`, filePath, whereClause, tempFile)

	if _, err := db.ExecContext(ctx, copyQuery); err != nil {
		os.Remove(tempFile)
		return 0, fmt.Errorf("failed to write filtered data: %w", err)
	}

	// Atomic replace: rename temp file to original
	if err := os.Rename(tempFile, filePath); err != nil {
		os.Remove(tempFile)
		return 0, fmt.Errorf("failed to replace original file: %w", err)
	}

	// Try to clean up temp directory if empty
	os.Remove(tempDir) // Ignore error, directory might not be empty

	h.logger.Info().
		Str("file", filepath.Base(filePath)).
		Int64("rows_before", rowsBefore).
		Int64("rows_after", rowsAfter).
		Int64("deleted", deleted).
		Msg("Rewrote file")

	return deleted, nil
}

// rewriteS3File handles file rewrite for S3 storage
// DuckDB can read from S3 directly, then we write to a temp file and upload
func (h *DeleteHandler) rewriteS3File(ctx context.Context, s3Path, relativePath, whereClause string, rowsBefore, rowsAfter int64) (int64, error) {
	db := h.db.DB()
	deleted := rowsBefore - rowsAfter

	// Create temp file locally for the rewritten data
	tempFile, err := os.CreateTemp("", "arc-delete-*.parquet")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()
	defer os.Remove(tempPath)

	// DuckDB reads from S3 and writes to local temp file
	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * FROM read_parquet('%s') WHERE NOT (%s)
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3
		)
	`, s3Path, whereClause, tempPath)

	if _, err := db.ExecContext(ctx, copyQuery); err != nil {
		return 0, fmt.Errorf("failed to write filtered data: %w", err)
	}

	// Read the temp file and upload to S3
	data, err := os.ReadFile(tempPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read temp file: %w", err)
	}

	// Upload back to S3 (overwrites the original)
	if err := h.storage.Write(ctx, relativePath, data); err != nil {
		return 0, fmt.Errorf("failed to upload rewritten file to S3: %w", err)
	}

	h.logger.Info().
		Str("file", filepath.Base(relativePath)).
		Int64("rows_before", rowsBefore).
		Int64("rows_after", rowsAfter).
		Int64("deleted", deleted).
		Msg("Rewrote S3 file")

	return deleted, nil
}

// getQueryPath converts a storage-relative path to a DuckDB-compatible path
func (h *DeleteHandler) getQueryPath(relativePath string) string {
	switch backend := h.storage.(type) {
	case *storage.LocalBackend:
		return filepath.Join(backend.GetBasePath(), relativePath)
	case *storage.S3Backend:
		return fmt.Sprintf("s3://%s/%s", backend.GetBucket(), relativePath)
	default:
		return relativePath
	}
}

// isS3Backend returns true if the storage backend is S3
func (h *DeleteHandler) isS3Backend() bool {
	_, ok := h.storage.(*storage.S3Backend)
	return ok
}
