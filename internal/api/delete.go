package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// DeleteCoordinator is the minimal cluster interface the delete handler needs
// to gate execution to the primary writer and propagate file manifest changes.
// nil = standalone mode (no gate, manifest not updated).
type DeleteCoordinator interface {
	BatchFileOpsInManifest(ops []raft.BatchFileOp) error
	UpdateFileInManifest(file raft.FileEntry) error
	GetFileEntry(path string) (*raft.FileEntry, bool)
	IsPrimaryWriter() bool
	Role() string
}

// errManifestFailure is returned when a Raft manifest update fails.
// This is non-transient (e.g. Raft quorum loss) and aborts the delete operation.
var errManifestFailure = errors.New("cluster manifest update failed")

// parquetRowGroupSize is the row group size used for Parquet rewrites during delete operations.
// Matches compaction's row group size to limit DuckDB's internal write buffer per group.
const parquetRowGroupSize = 122880

// escapeDuckDBPath escapes single quotes in a path for safe interpolation into
// DuckDB read_parquet() calls, which do not support parameterized queries.
func escapeDuckDBPath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

// fileMetadata returns the byte size and hex-encoded SHA-256 of the file at path.
func fileMetadata(path string) (sizeBytes int64, sha256hex string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return n, fmt.Sprintf("%x", h.Sum(nil)), nil
}

// lastFreeOSMemoryNano is the last time freeOSMemoryThrottled fired, used to debounce GC calls.
var lastFreeOSMemoryNano atomic.Int64

// freeOSMemoryThrottled fires debug.FreeOSMemory in a goroutine at most once every 30 seconds.
// This prevents GC storms when multiple concurrent delete/retention requests complete together.
func freeOSMemoryThrottled() {
	now := time.Now().UnixNano()
	last := lastFreeOSMemoryNano.Load()
	if now-last < int64(30*time.Second) {
		return
	}
	if lastFreeOSMemoryNano.CompareAndSwap(last, now) {
		go debug.FreeOSMemory()
	}
}

// DeleteHandler handles delete operations using file rewrite strategy
type DeleteHandler struct {
	db          *database.DuckDB
	storage     storage.Backend
	config      *config.DeleteConfig
	authManager *auth.AuthManager
	coordinator DeleteCoordinator // nil in standalone mode
	logger      zerolog.Logger
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
	FailedFiles     []string `json:"failed_files,omitempty"`
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

// Dangerous punctuation patterns for WHERE clause validation (exact substring match)
var dangerousPunctuationPatterns = []string{
	";",  // Statement terminator
	"--", // SQL comment
	"/*", // Multi-line comment
}

// Dangerous keyword patterns for WHERE clause validation (word-boundary match)
// Uses \b word boundaries to avoid false positives on column names like "offset", "payload", "dataset"
var dangerousKeywordPattern = regexp.MustCompile(`(?i)\b(DROP|DELETE|INSERT|UPDATE|EXEC|EXECUTE|UNION|SELECT|CREATE|ALTER|COPY|ATTACH|DETACH|LOAD|INSTALL|PRAGMA|CALL|SET)\b`)

// Dangerous prefix patterns (match at word start)
var dangerousPrefixPatterns = []string{
	"xp_",
	"sp_",
}

// NewDeleteHandler creates a new delete handler
func NewDeleteHandler(db *database.DuckDB, storage storage.Backend, cfg *config.DeleteConfig, authManager *auth.AuthManager, logger zerolog.Logger) *DeleteHandler {
	return &DeleteHandler{
		db:          db,
		storage:     storage,
		config:      cfg,
		authManager: authManager,
		logger:      logger.With().Str("component", "delete-handler").Logger(),
	}
}

// SetCoordinator wires the cluster coordinator for manifest updates and role
// gating. Called after construction when cluster mode is enabled.
func (h *DeleteHandler) SetCoordinator(c DeleteCoordinator) {
	h.coordinator = c
}

// RegisterRoutes registers delete endpoints
func (h *DeleteHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/delete")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}
	group.Post("/", h.handleDelete)
	group.Get("/config", h.handleGetConfig)
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

	// Reject path traversal in database/measurement names — these values are
	// concatenated directly into storage prefixes and DuckDB paths.
	if strings.ContainsAny(req.Database, "/\\") || strings.Contains(req.Database, "..") {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "database name contains invalid characters",
		})
	}
	if strings.ContainsAny(req.Measurement, "/\\") || strings.Contains(req.Measurement, "..") {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "measurement name contains invalid characters",
		})
	}

	// In cluster mode, only the primary writer may execute deletes. Check before
	// findAffectedFiles to avoid running expensive DuckDB scans on reader nodes.
	if !req.DryRun && h.coordinator != nil && !h.coordinator.IsPrimaryWriter() {
		return c.Status(fiber.StatusServiceUnavailable).JSON(DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("delete rejected: node role %q is not primary writer", h.coordinator.Role()),
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
	ctx := c.Context()
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
		// findAffectedFiles scanned parquet files via read_parquet — clear the cache even though
		// no rows matched, to avoid accumulating metadata for files that may later be deleted.
		h.db.ClearHTTPCache()
		freeOSMemoryThrottled()
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
	processedFiles := make([]string, 0, len(affected))
	failedFiles := make([]string, 0, len(affected))

	for _, f := range affected {
		deleted, err := h.rewriteFileWithoutDeletedRows(ctx, f.path, f.relativePath, req.Where)
		totalDeleted += deleted
		if err != nil {
			h.logger.Error().Err(err).Str("file", f.path).Msg("Failed to rewrite file")
			// Manifest failures are non-transient (Raft quorum loss) — abort the
			// entire operation to avoid deleting files from storage without a
			// corresponding manifest record.
			if errors.Is(err, errManifestFailure) {
				h.db.ClearHTTPCache()
				freeOSMemoryThrottled()
				return c.Status(fiber.StatusInternalServerError).JSON(DeleteResponse{
					Success:         false,
					Error:           "Delete aborted: " + err.Error(),
					DeletedCount:    totalDeleted,
					AffectedFiles:   len(affected),
					RewrittenFiles:  rewrittenCount,
					FilesProcessed:  processedFiles,
					ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				})
			}
			failedFiles = append(failedFiles, filepath.Base(f.path))
			continue
		}

		rewrittenCount++
		processedFiles = append(processedFiles, filepath.Base(f.path))

		h.logger.Debug().
			Str("file", filepath.Base(f.path)).
			Int64("deleted", deleted).
			Msg("Processed file")
	}

	// Clear DuckDB parquet metadata/data cache and release memory back to OS.
	h.db.ClearHTTPCache()
	freeOSMemoryThrottled()

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int64("deleted_count", totalDeleted).
		Int("rewritten_files", rewrittenCount).
		Int("failed_files", len(failedFiles)).
		Float64("execution_time_ms", executionTime).
		Msg("Delete operation completed")

	resp := DeleteResponse{
		Success:         len(failedFiles) == 0,
		DeletedCount:    totalDeleted,
		AffectedFiles:   len(affected),
		RewrittenFiles:  rewrittenCount,
		ExecutionTimeMs: executionTime,
		DryRun:          false,
		FilesProcessed:  processedFiles,
		FailedFiles:     failedFiles,
	}

	if len(failedFiles) > 0 {
		resp.Error = fmt.Sprintf("%d of %d files failed to process", len(failedFiles), len(affected))
		return c.Status(fiber.StatusMultiStatus).JSON(resp)
	}

	return c.JSON(resp)
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

	// Check for dangerous punctuation patterns (exact substring match)
	for _, pattern := range dangerousPunctuationPatterns {
		if strings.Contains(whereUpper, pattern) {
			return false, fmt.Errorf("WHERE clause contains forbidden pattern: %s", pattern)
		}
	}

	// Check for dangerous SQL keywords using word boundaries to avoid false positives
	// on column names like "offset" (contains SET), "payload" (contains LOAD), "dataset" (contains SET)
	if match := dangerousKeywordPattern.FindString(where); match != "" {
		return false, fmt.Errorf("WHERE clause contains forbidden keyword: %s", strings.ToUpper(match))
	}


	// Check for dangerous prefixes
	for _, pattern := range dangerousPrefixPatterns {
		if strings.Contains(whereUpper, pattern) {
			return false, fmt.Errorf("WHERE clause contains forbidden pattern: %s", pattern)
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

	// Build array of file paths for DuckDB, escaping single quotes in paths.
	var pathList strings.Builder
	pathList.WriteString("[")
	for i, f := range files {
		if i > 0 {
			pathList.WriteString(", ")
		}
		pathList.WriteString("'")
		pathList.WriteString(escapeDuckDBPath(f.queryPath))
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
		query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s') WHERE %s", escapeDuckDBPath(f.queryPath), whereClause)
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
		whereClause, escapeDuckDBPath(queryPath))

	if err := db.QueryRowContext(ctx, countQuery).Scan(&rowsBefore, &rowsAfter); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	deleted := rowsBefore - rowsAfter

	// If all rows would be deleted, remove the file entirely.
	if rowsAfter == 0 {
		h.logger.Info().Str("file", filepath.Base(relativePath)).Int64("deleted", deleted).Msg("All rows deleted, removing file")

		// Manifest-before-storage: record the delete in the Raft manifest before
		// removing from storage. If the manifest update fails (Raft quorum loss),
		// abort — the file still exists in storage and the next delete will retry.
		if h.coordinator != nil {
			payload, err := json.Marshal(raft.DeleteFilePayload{Path: relativePath, Reason: "delete"})
			if err != nil {
				return 0, fmt.Errorf("%w: marshal failed for %s: %v", errManifestFailure, relativePath, err)
			}
			if err := h.coordinator.BatchFileOpsInManifest([]raft.BatchFileOp{{Type: raft.CommandDeleteFile, Payload: payload}}); err != nil {
				return 0, fmt.Errorf("%w: %v", errManifestFailure, err)
			}
		}

		if err := h.storage.Delete(ctx, relativePath); err != nil {
			// Manifest is already committed — storage failure is transient (network
			// blip, file already gone). Log as Warn and return the deleted count;
			// Phase 5 reconciliation will clean up the orphaned storage entry.
			h.logger.Warn().Err(err).Str("file", relativePath).Msg("Failed to delete file from storage after manifest update")
			return deleted, nil
		}
		return deleted, nil
	}

	// For S3, we need to write to a temp file locally, then upload
	var rewroteDeleted int64
	var rewriteErr error
	if h.isS3Backend() {
		rewroteDeleted, rewriteErr = h.rewriteS3File(ctx, queryPath, relativePath, whereClause, rowsBefore, rowsAfter)
	} else {
		rewroteDeleted, rewriteErr = h.rewriteLocalFile(ctx, queryPath, relativePath, whereClause, rowsBefore, rowsAfter)
	}
	if rewriteErr != nil {
		return 0, rewriteErr
	}

	// Notify the cluster manifest that this file's content changed. Non-fatal:
	// the rewrite succeeded in storage; a stale manifest entry is eventually
	// consistent and does not affect query correctness.
	if h.coordinator != nil {
		if err := h.updateManifestAfterRewrite(relativePath); err != nil {
			h.logger.Warn().Err(err).Str("file", relativePath).Msg("Failed to update manifest after partial rewrite")
		}
	}

	return rewroteDeleted, nil
}

// rewriteLocalFile handles file rewrite for local storage using atomic rename
func (h *DeleteHandler) rewriteLocalFile(ctx context.Context, filePath, _, whereClause string, rowsBefore, rowsAfter int64) (int64, error) {
	db := h.db.DB()
	deleted := rowsBefore - rowsAfter

	// Create temp file for the rewritten data
	dir := filepath.Dir(filePath)
	tempDir := filepath.Join(dir, ".tmp")
	if err := os.MkdirAll(tempDir, 0700); err != nil {
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
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE %d
		)`, escapeDuckDBPath(filePath), whereClause, escapeDuckDBPath(tempFile), parquetRowGroupSize)

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
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE %d
		)`, escapeDuckDBPath(s3Path), whereClause, escapeDuckDBPath(tempPath), parquetRowGroupSize)

	if _, err := db.ExecContext(ctx, copyQuery); err != nil {
		return 0, fmt.Errorf("failed to write filtered data: %w", err)
	}

	// Stream temp file to S3 (avoids loading entire file into memory)
	uploadFile, err := os.Open(tempPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open temp file for upload: %w", err)
	}
	defer uploadFile.Close()

	info, err := uploadFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat temp file: %w", err)
	}

	if err := h.storage.WriteReader(ctx, relativePath, uploadFile, info.Size()); err != nil {
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
		return fmt.Sprintf("s3://%s/%s%s", backend.GetBucket(), backend.GetPrefix(), relativePath)
	default:
		return relativePath
	}
}

// isS3Backend returns true if the storage backend is S3
func (h *DeleteHandler) isS3Backend() bool {
	_, ok := h.storage.(*storage.S3Backend)
	return ok
}

// updateManifestAfterRewrite updates the cluster manifest entry for a partially
// rewritten file. It reads the existing entry to preserve all immutable fields
// (database, measurement, origin node, tier, etc.) and updates only the mutable
// metadata: SizeBytes and SHA256.
//
// For local storage, the new size and checksum are computed from the rewritten
// file on disk. For S3, size/checksum are not re-read (would require an extra
// round-trip) — the manifest entry is still re-committed so peers know the file
// changed, but SizeBytes and SHA256 will be stale until the next register.
//
// Failure is non-fatal: the rewrite already succeeded in storage; a stale
// manifest entry is eventually consistent and does not affect query correctness.
func (h *DeleteHandler) updateManifestAfterRewrite(relativePath string) error {
	existing, ok := h.coordinator.GetFileEntry(relativePath)
	if !ok {
		// File not in manifest (standalone-registered or pre-cluster file) — skip.
		return nil
	}

	entry := *existing // copy all fields to preserve immutable metadata

	if lb, ok := h.storage.(*storage.LocalBackend); ok {
		basePath := lb.GetBasePath()
		fullPath := filepath.Join(basePath, relativePath)
		// Containment check: relativePath is already validated upstream, but
		// guard here too since fileMetadata opens the file for reading.
		if !strings.HasPrefix(fullPath, basePath+string(filepath.Separator)) {
			return fmt.Errorf("path escapes storage root: %s", relativePath)
		}
		size, sha, err := fileMetadata(fullPath)
		if err != nil {
			return fmt.Errorf("failed to read rewritten file metadata: %w", err)
		}
		entry.SizeBytes = size
		entry.SHA256 = sha
	}
	// For S3: SizeBytes/SHA256 remain from the original entry until the next
	// explicit register. The manifest update still signals to peers that the
	// file's content changed (LSN advances on apply).

	return h.coordinator.UpdateFileInManifest(entry)
}
