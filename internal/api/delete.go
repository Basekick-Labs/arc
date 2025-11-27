package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

	// Get base path from storage backend
	basePath := h.getStorageBasePath()
	if basePath == "" {
		return nil, fmt.Errorf("unable to determine storage base path")
	}

	measurementPath := filepath.Join(basePath, database, measurement)

	// Check if measurement exists
	if _, err := os.Stat(measurementPath); os.IsNotExist(err) {
		h.logger.Warn().Str("path", measurementPath).Msg("No data directory found for measurement")
		return affected, nil
	}

	// Walk the directory to find all parquet files
	var parquetFiles []string
	err := filepath.Walk(measurementPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Continue on error
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".parquet") {
			parquetFiles = append(parquetFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk directory: %w", err)
	}

	h.logger.Info().Int("file_count", len(parquetFiles)).Msg("Scanning parquet files for matching rows")

	// Check each file for matching rows
	for _, filePath := range parquetFiles {
		matchCount, err := h.countMatchingRowsInFile(ctx, filePath, whereClause)
		if err != nil {
			h.logger.Warn().Err(err).Str("file", filePath).Msg("Failed to count matching rows, skipping file")
			continue
		}

		if matchCount > 0 {
			relPath, _ := filepath.Rel(basePath, filePath)
			affected = append(affected, affectedFile{
				path:         filePath,
				matchCount:   matchCount,
				relativePath: relPath,
			})
			h.logger.Debug().Str("file", filepath.Base(filePath)).Int64("matches", matchCount).Msg("Found matching rows")
		}
	}

	return affected, nil
}

// countMatchingRowsInFile counts rows that match the WHERE clause in a Parquet file
func (h *DeleteHandler) countMatchingRowsInFile(ctx context.Context, filePath, whereClause string) (int64, error) {
	// Use the shared DuckDB connection to avoid memory retention from temporary connections
	db := h.db.DB()

	// Query the parquet file directly
	query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s') WHERE %s", filePath, whereClause)

	var count int64
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	return count, nil
}

// rewriteFileWithoutDeletedRows rewrites a Parquet file excluding rows that match the WHERE clause
func (h *DeleteHandler) rewriteFileWithoutDeletedRows(ctx context.Context, filePath, relativePath, whereClause string) (int64, error) {
	// Use the shared DuckDB connection to avoid memory retention from temporary connections
	db := h.db.DB()

	// First, count rows before and after
	var rowsBefore, rowsAfter int64

	// Count total rows
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filePath)
	if err := db.QueryRowContext(ctx, countQuery).Scan(&rowsBefore); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	// Count rows after filtering out deleted rows
	afterQuery := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s') WHERE NOT (%s)", filePath, whereClause)
	if err := db.QueryRowContext(ctx, afterQuery).Scan(&rowsAfter); err != nil {
		return 0, fmt.Errorf("failed to count remaining rows: %w", err)
	}

	deleted := rowsBefore - rowsAfter

	// If all rows would be deleted, just delete the file
	if rowsAfter == 0 {
		h.logger.Info().Str("file", filepath.Base(filePath)).Int64("deleted", deleted).Msg("All rows deleted, removing file")
		if err := os.Remove(filePath); err != nil {
			return 0, fmt.Errorf("failed to delete file: %w", err)
		}
		return deleted, nil
	}

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

// getStorageBasePath returns the base path for storage
func (h *DeleteHandler) getStorageBasePath() string {
	switch backend := h.storage.(type) {
	case *storage.LocalBackend:
		return backend.GetBasePath()
	case *storage.S3Backend:
		// For S3, we need different handling
		// TODO: Implement S3 delete support
		h.logger.Warn().Msg("Delete not fully supported for S3 backend yet")
		return ""
	default:
		return "./data/arc"
	}
}

// extractTimeRange extracts time range from WHERE clause for partition pruning (future optimization)
var timeRangePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)time\s*>=\s*'([^']+)'`),
	regexp.MustCompile(`(?i)time\s*>\s*'([^']+)'`),
	regexp.MustCompile(`(?i)time\s*<\s*'([^']+)'`),
	regexp.MustCompile(`(?i)time\s*<=\s*'([^']+)'`),
	regexp.MustCompile(`(?i)time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'`),
}
