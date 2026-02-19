package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// ImportResult holds the result of a bulk import operation
type ImportResult struct {
	Database          string   `json:"database"`
	Measurement       string   `json:"measurement"`
	RowsImported      int64    `json:"rows_imported"`
	PartitionsCreated int      `json:"partitions_created"`
	TimeRangeMin      string   `json:"time_range_min,omitempty"`
	TimeRangeMax      string   `json:"time_range_max,omitempty"`
	Columns           []string `json:"columns"`
	DurationMs        int64    `json:"duration_ms"`
}

// LPImportResult holds the result of a Line Protocol bulk import operation
type LPImportResult struct {
	Database     string   `json:"database"`
	Measurements []string `json:"measurements"`
	RowsImported int64    `json:"rows_imported"`
	Precision    string   `json:"precision"`
	DurationMs   int64    `json:"duration_ms"`
}

// TLEImportResult holds the result of a TLE bulk import operation
type TLEImportResult struct {
	Database       string   `json:"database"`
	Measurement    string   `json:"measurement"`
	SatelliteCount int      `json:"satellite_count"`
	RowsImported   int64    `json:"rows_imported"`
	ParseWarnings  []string `json:"parse_warnings,omitempty"`
	DurationMs     int64    `json:"duration_ms"`
}

// ImportHandler handles bulk CSV, Parquet, and Line Protocol file imports
type ImportHandler struct {
	db      *database.DuckDB
	storage storage.Backend
	logger  zerolog.Logger

	// ArrowBuffer for LP import (uses the streaming ingest pipeline)
	arrowBuffer *ingest.ArrowBuffer

	// RBAC support
	authManager AuthManager
	rbacManager RBACChecker

	// Stats
	totalRequests atomic.Int64
	totalRecords  atomic.Int64
	totalErrors   atomic.Int64
}

// NewImportHandler creates a new ImportHandler
func NewImportHandler(db *database.DuckDB, storage storage.Backend, logger zerolog.Logger) *ImportHandler {
	return &ImportHandler{
		db:      db,
		storage: storage,
		logger:  logger.With().Str("component", "import-handler").Logger(),
	}
}

// SetArrowBuffer sets the ArrowBuffer for Line Protocol import
func (h *ImportHandler) SetArrowBuffer(buf *ingest.ArrowBuffer) {
	h.arrowBuffer = buf
}

// SetAuthAndRBAC sets the auth and RBAC managers for permission checking
func (h *ImportHandler) SetAuthAndRBAC(authManager AuthManager, rbacManager RBACChecker) {
	h.authManager = authManager
	h.rbacManager = rbacManager
}

// RegisterRoutes registers import API routes
func (h *ImportHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/import/csv", h.handleCSVImport)
	app.Post("/api/v1/import/parquet", h.handleParquetImport)
	app.Post("/api/v1/import/lp", h.handleLineProtocolImport)
	app.Post("/api/v1/import/tle", h.handleTLEImport)
	app.Get("/api/v1/import/stats", h.Stats)

	h.logger.Info().Msg("Import routes registered")
}

// handleCSVImport handles CSV file upload and import
func (h *ImportHandler) handleCSVImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	start := time.Now()

	database := c.Get("x-arc-database")
	if database == "" {
		database = c.Query("db")
	}
	if database == "" {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "database is required (set x-arc-database header or db query param)",
		})
	}

	measurement := c.Query("measurement")
	if measurement == "" {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "measurement query parameter is required",
		})
	}

	if !isValidMeasurementName(measurement) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement),
		})
	}

	// Check RBAC permissions
	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		if err := CheckWritePermissions(c, h.rbacManager, h.logger, database, []string{measurement}); err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Get uploaded file
	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no file uploaded: use multipart/form-data with field name 'file'",
		})
	}

	// Save to temp file
	tempDir, err := os.MkdirTemp("", "arc-import-*")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to create temp directory: " + err.Error(),
		})
	}
	defer os.RemoveAll(tempDir)

	tempFile := filepath.Join(tempDir, "import.csv")
	if err := c.SaveFile(fileHeader, tempFile); err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to save uploaded file: " + err.Error(),
		})
	}

	// Parse import options
	opts := importOptions{
		format:     "csv",
		timeColumn: c.Query("time_column", "time"),
		timeFormat: c.Query("time_format", ""),
		delimiter:  c.Query("delimiter", ","),
		skipRows:   c.QueryInt("skip_rows", 0),
	}

	result, err := h.importFile(c.Context(), database, measurement, tempFile, tempDir, opts)
	if err != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Str("format", "csv").
			Msg("Import failed")
		return h.importErrorResponse(c, err)
	}

	result.DurationMs = time.Since(start).Milliseconds()
	h.totalRecords.Add(result.RowsImported)

	h.logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Int64("rows", result.RowsImported).
		Int("partitions", result.PartitionsCreated).
		Int64("duration_ms", result.DurationMs).
		Msg("CSV import completed")

	return c.JSON(fiber.Map{
		"status": "ok",
		"result": result,
	})
}

// handleParquetImport handles Parquet file upload and import
func (h *ImportHandler) handleParquetImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	start := time.Now()

	database := c.Get("x-arc-database")
	if database == "" {
		database = c.Query("db")
	}
	if database == "" {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "database is required (set x-arc-database header or db query param)",
		})
	}

	measurement := c.Query("measurement")
	if measurement == "" {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "measurement query parameter is required",
		})
	}

	if !isValidMeasurementName(measurement) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement),
		})
	}

	// Check RBAC permissions
	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		if err := CheckWritePermissions(c, h.rbacManager, h.logger, database, []string{measurement}); err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Get uploaded file
	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no file uploaded: use multipart/form-data with field name 'file'",
		})
	}

	// Save to temp file
	tempDir, err := os.MkdirTemp("", "arc-import-*")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to create temp directory: " + err.Error(),
		})
	}
	defer os.RemoveAll(tempDir)

	tempFile := filepath.Join(tempDir, "import.parquet")
	if err := c.SaveFile(fileHeader, tempFile); err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to save uploaded file: " + err.Error(),
		})
	}

	opts := importOptions{
		format:     "parquet",
		timeColumn: c.Query("time_column", "time"),
	}

	result, err := h.importFile(c.Context(), database, measurement, tempFile, tempDir, opts)
	if err != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Str("format", "parquet").
			Msg("Import failed")
		return h.importErrorResponse(c, err)
	}

	result.DurationMs = time.Since(start).Milliseconds()
	h.totalRecords.Add(result.RowsImported)

	h.logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Int64("rows", result.RowsImported).
		Int("partitions", result.PartitionsCreated).
		Int64("duration_ms", result.DurationMs).
		Msg("Parquet import completed")

	return c.JSON(fiber.Map{
		"status": "ok",
		"result": result,
	})
}

// handleLineProtocolImport handles Line Protocol file upload and import.
// Uses the same ArrowBuffer ingest pipeline as streaming LP ingestion.
func (h *ImportHandler) handleLineProtocolImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	start := time.Now()

	database := c.Get("x-arc-database")
	if database == "" {
		database = c.Query("db")
	}
	if database == "" {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "database is required (set x-arc-database header or db query param)",
		})
	}

	if !isValidDatabaseName(database) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	if h.arrowBuffer == nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "line protocol import not available (ingest pipeline not configured)",
		})
	}

	// measurement is optional for LP — lines contain measurement names
	measurementFilter := c.Query("measurement")
	if measurementFilter != "" && !isValidMeasurementName(measurementFilter) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurementFilter),
		})
	}

	precision := c.Query("precision", "ns")
	if precision != "ns" && precision != "us" && precision != "ms" && precision != "s" {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid precision %q: must be ns, us, ms, or s", precision),
		})
	}

	// Get uploaded file
	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no file uploaded: use multipart/form-data with field name 'file'",
		})
	}

	// Read file contents
	file, err := fileHeader.Open()
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to open uploaded file: " + err.Error(),
		})
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to read uploaded file: " + err.Error(),
		})
	}

	// Size limit for bulk LP import (applies to both compressed and uncompressed)
	const maxImportSize = 500 * 1024 * 1024 // 500MB

	// Detect and decompress gzip (magic bytes: 0x1f 0x8b)
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to decompress gzip file: " + err.Error(),
			})
		}
		decompressed, err := io.ReadAll(io.LimitReader(reader, maxImportSize+1))
		reader.Close()
		if err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to decompress gzip file: " + err.Error(),
			})
		}
		if len(decompressed) > maxImportSize {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
				"error": "decompressed file exceeds 500MB limit",
			})
		}
		data = decompressed
	}

	// Enforce size limit on uncompressed data
	if len(data) > maxImportSize {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": "file exceeds 500MB limit",
		})
	}

	if len(data) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "file is empty",
		})
	}

	// Parse Line Protocol with precision-aware timestamp conversion
	parser := ingest.NewLineProtocolParser()
	records := parser.ParseBatchWithPrecision(data, precision)
	if len(records) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no valid line protocol records found in file",
		})
	}

	// Filter by measurement if specified
	if measurementFilter != "" {
		filtered := make([]*models.Record, 0, len(records))
		for _, r := range records {
			if r.Measurement == measurementFilter {
				filtered = append(filtered, r)
			}
		}
		if len(filtered) == 0 {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("no records found for measurement %q", measurementFilter),
			})
		}
		records = filtered
	}

	// Check RBAC permissions for all measurements
	measurements := make(map[string]bool)
	for _, r := range records {
		measurements[r.Measurement] = true
	}
	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		measList := make([]string, 0, len(measurements))
		for m := range measurements {
			measList = append(measList, m)
		}
		if err := CheckWritePermissions(c, h.rbacManager, h.logger, database, measList); err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Group by measurement and convert to columnar format
	columnarByMeasurement := ingest.BatchToColumnar(records)

	// Validate all measurement names from the LP body (prevent path traversal)
	for measurement := range columnarByMeasurement {
		if !isValidMeasurementName(measurement) {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("invalid measurement name %q in LP data: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement),
			})
		}
	}

	// Feed each measurement into the ArrowBuffer ingest pipeline
	var totalRows int64
	importedMeasurements := make([]string, 0, len(columnarByMeasurement))
	for measurement, columns := range columnarByMeasurement {
		if err := h.arrowBuffer.WriteColumnarDirect(c.Context(), database, measurement, columns); err != nil {
			h.totalErrors.Add(1)
			h.logger.Error().Err(err).
				Str("database", database).
				Str("measurement", measurement).
				Msg("LP import: failed to write to buffer")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("failed to ingest measurement %q: %v", measurement, err),
			})
		}
		// Count rows from the time column
		if timeCol, ok := columns["time"]; ok {
			totalRows += int64(len(timeCol))
		}
		importedMeasurements = append(importedMeasurements, measurement)
	}

	// Force flush to ensure data is persisted before returning
	if err := h.arrowBuffer.FlushAll(c.Context()); err != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(err).Str("database", database).Msg("LP import: flush failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to flush imported data: " + err.Error(),
		})
	}

	durationMs := time.Since(start).Milliseconds()
	h.totalRecords.Add(totalRows)

	h.logger.Info().
		Str("database", database).
		Strs("measurements", importedMeasurements).
		Int64("rows", totalRows).
		Str("precision", precision).
		Int64("duration_ms", durationMs).
		Msg("Line protocol import completed")

	return c.JSON(fiber.Map{
		"status": "ok",
		"result": LPImportResult{
			Database:     database,
			Measurements: importedMeasurements,
			RowsImported: totalRows,
			Precision:    precision,
			DurationMs:   durationMs,
		},
	})
}

// importOptions holds configuration for an import operation
type importOptions struct {
	format     string // "csv" or "parquet"
	timeColumn string
	timeFormat string // "", "epoch_s", "epoch_ms", "epoch_us", "epoch_ns"
	delimiter  string
	skipRows   int
}

// importError wraps import errors with HTTP status context
type importError struct {
	StatusCode int
	Message    string
	Err        error
}

func (e *importError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// importFile uses DuckDB to read the uploaded file, partition by hour, and write to storage
func (h *ImportHandler) importFile(ctx context.Context, dbName, measurement, filePath, tempDir string, opts importOptions) (*ImportResult, error) {
	// Build the DuckDB read expression for the source file
	readExpr := h.buildReadExpression(filePath, opts)

	// 1. Validate schema — check that time column exists and get column list
	columns, err := h.getColumns(readExpr)
	if err != nil {
		return nil, &importError{
			StatusCode: fiber.StatusUnprocessableEntity,
			Message:    "failed to read file",
			Err:        err,
		}
	}

	hasTimeCol := false
	for _, col := range columns {
		if col == opts.timeColumn {
			hasTimeCol = true
			break
		}
	}
	if !hasTimeCol {
		return nil, &importError{
			StatusCode: fiber.StatusBadRequest,
			Message:    fmt.Sprintf("time column %q not found in file; available columns: %s", opts.timeColumn, strings.Join(columns, ", ")),
		}
	}

	// 2. Build time cast expression for normalization to TIMESTAMP
	timeCast := h.buildTimeCast(opts.timeColumn, opts.timeFormat)

	// 3. Get row count and time range
	statsQuery := fmt.Sprintf(
		"SELECT COUNT(*), MIN(%s)::VARCHAR, MAX(%s)::VARCHAR FROM %s",
		timeCast, timeCast, readExpr,
	)
	rows, err := h.db.Query(statsQuery)
	if err != nil {
		return nil, &importError{
			StatusCode: fiber.StatusUnprocessableEntity,
			Message:    "failed to analyze file",
			Err:        err,
		}
	}

	var totalRows int64
	var minTime, maxTime string
	if rows.Next() {
		if err := rows.Scan(&totalRows, &minTime, &maxTime); err != nil {
			rows.Close()
			return nil, &importError{
				StatusCode: fiber.StatusUnprocessableEntity,
				Message:    "failed to read file statistics",
				Err:        err,
			}
		}
	}
	rows.Close()

	if totalRows == 0 {
		return nil, &importError{
			StatusCode: fiber.StatusBadRequest,
			Message:    "file contains no rows",
		}
	}

	// 4. Get distinct hour partitions
	partitionQuery := fmt.Sprintf(
		"SELECT DISTINCT date_trunc('hour', %s)::VARCHAR AS partition_hour FROM %s ORDER BY partition_hour",
		timeCast, readExpr,
	)
	partRows, err := h.db.Query(partitionQuery)
	if err != nil {
		return nil, &importError{
			StatusCode: fiber.StatusInternalServerError,
			Message:    "failed to determine partitions",
			Err:        err,
		}
	}

	var partitionHours []string
	for partRows.Next() {
		var hour string
		if err := partRows.Scan(&hour); err != nil {
			partRows.Close()
			return nil, &importError{
				StatusCode: fiber.StatusInternalServerError,
				Message:    "failed to read partition hours",
				Err:        err,
			}
		}
		partitionHours = append(partitionHours, hour)
	}
	partRows.Close()

	// 5. For each partition hour, COPY to a temp Parquet file, then upload to storage
	outputDir := filepath.Join(tempDir, "output")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, &importError{
			StatusCode: fiber.StatusInternalServerError,
			Message:    "failed to create output directory",
			Err:        err,
		}
	}

	// Rename the time column to "time" if it has a different name, so Arc's
	// standard schema always has a "time" column
	selectExpr := "*"
	if opts.timeColumn != "time" {
		// Build column list, renaming the time column
		var selectCols []string
		for _, col := range columns {
			if col == opts.timeColumn {
				selectCols = append(selectCols, fmt.Sprintf("%s AS time", timeCast))
			} else {
				selectCols = append(selectCols, fmt.Sprintf("\"%s\"", col))
			}
		}
		selectExpr = strings.Join(selectCols, ", ")
	} else {
		// Still need to cast time column for normalization
		var selectCols []string
		for _, col := range columns {
			if col == "time" {
				selectCols = append(selectCols, fmt.Sprintf("%s AS time", timeCast))
			} else {
				selectCols = append(selectCols, fmt.Sprintf("\"%s\"", col))
			}
		}
		selectExpr = strings.Join(selectCols, ", ")
	}

	partitionsCreated := 0
	for _, hourStr := range partitionHours {
		// Parse the partition hour to construct Arc's storage path
		partTime, err := time.Parse("2006-01-02 15:04:05", hourStr)
		if err != nil {
			// Try with timezone suffix
			partTime, err = time.Parse("2006-01-02 15:04:05-07", hourStr)
			if err != nil {
				partTime, err = time.Parse("2006-01-02T15:04:05Z", hourStr)
				if err != nil {
					h.logger.Warn().Str("hour", hourStr).Msg("Failed to parse partition hour, skipping")
					continue
				}
			}
		}
		partTime = partTime.UTC()

		// Generate output Parquet file
		outFile := filepath.Join(outputDir, fmt.Sprintf("part_%s.parquet", partTime.Format("20060102_150405")))

		copyQuery := fmt.Sprintf(
			"COPY (SELECT %s FROM %s WHERE date_trunc('hour', %s) = '%s' ORDER BY %s) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
			selectExpr, readExpr, timeCast, hourStr, timeCast, escapeSQLString(outFile),
		)

		if _, err := h.db.Exec(copyQuery); err != nil {
			return nil, &importError{
				StatusCode: fiber.StatusInternalServerError,
				Message:    fmt.Sprintf("failed to write partition %s", hourStr),
				Err:        err,
			}
		}

		// Read the generated Parquet file
		parquetData, err := os.ReadFile(outFile)
		if err != nil {
			return nil, &importError{
				StatusCode: fiber.StatusInternalServerError,
				Message:    "failed to read generated parquet file",
				Err:        err,
			}
		}

		// Generate Arc-standard storage path
		storagePath := generateStoragePath(dbName, measurement, partTime)

		// Upload to storage backend
		if err := h.storage.Write(ctx, storagePath, parquetData); err != nil {
			return nil, &importError{
				StatusCode: fiber.StatusInternalServerError,
				Message:    fmt.Sprintf("failed to upload partition %s to storage", hourStr),
				Err:        err,
			}
		}

		h.logger.Debug().
			Str("partition", hourStr).
			Str("path", storagePath).
			Int("size_bytes", len(parquetData)).
			Msg("Partition uploaded")

		partitionsCreated++
	}

	return &ImportResult{
		Database:          dbName,
		Measurement:       measurement,
		RowsImported:      totalRows,
		PartitionsCreated: partitionsCreated,
		TimeRangeMin:      minTime,
		TimeRangeMax:      maxTime,
		Columns:           columns,
	}, nil
}

// buildReadExpression builds the DuckDB read expression for the source file
func (h *ImportHandler) buildReadExpression(filePath string, opts importOptions) string {
	escaped := escapeSQLString(filePath)
	switch opts.format {
	case "csv":
		parts := []string{fmt.Sprintf("'%s'", escaped)}
		parts = append(parts, "auto_detect=true")
		parts = append(parts, "header=true")
		if opts.delimiter != "," {
			parts = append(parts, fmt.Sprintf("delim='%s'", escapeSQLString(opts.delimiter)))
		}
		if opts.skipRows > 0 {
			parts = append(parts, fmt.Sprintf("skip=%d", opts.skipRows))
		}
		return fmt.Sprintf("read_csv(%s)", strings.Join(parts, ", "))
	case "parquet":
		return fmt.Sprintf("read_parquet('%s')", escaped)
	default:
		return fmt.Sprintf("read_csv('%s', auto_detect=true)", escaped)
	}
}

// buildTimeCast builds the SQL expression to cast the time column to TIMESTAMP
func (h *ImportHandler) buildTimeCast(timeColumn, timeFormat string) string {
	col := fmt.Sprintf("\"%s\"", timeColumn)
	switch timeFormat {
	case "epoch_s":
		return fmt.Sprintf("to_timestamp(%s::BIGINT)", col)
	case "epoch_ms":
		return fmt.Sprintf("to_timestamp(%s::BIGINT / 1000.0)", col)
	case "epoch_us":
		return fmt.Sprintf("to_timestamp(%s::BIGINT / 1000000.0)", col)
	case "epoch_ns":
		return fmt.Sprintf("to_timestamp(%s::BIGINT / 1000000000.0)", col)
	default:
		// Auto-detect: DuckDB handles most timestamp formats natively
		return fmt.Sprintf("%s::TIMESTAMP", col)
	}
}

// getColumns returns the column names from the source file
func (h *ImportHandler) getColumns(readExpr string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM (DESCRIBE %s)", readExpr)
	rows, err := h.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, nil
}

// generateStoragePath creates an Arc-standard storage path for a partition
// Format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{measurement}_{YYYYMMDD}_{HHMMSS}_{nanos}.parquet
func generateStoragePath(dbName, measurement string, partitionTime time.Time) string {
	year := partitionTime.Format("2006")
	month := partitionTime.Format("01")
	day := partitionTime.Format("02")
	hour := partitionTime.Format("15")

	now := time.Now().UTC()
	timestamp := now.Format("20060102_150405")
	nanos := now.UnixNano() % 1_000_000_000

	return fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s_%s_%09d.parquet",
		dbName, measurement, year, month, day, hour, measurement, timestamp, nanos)
}

// importErrorResponse returns the appropriate HTTP error response for an import error
func (h *ImportHandler) importErrorResponse(c *fiber.Ctx, err error) error {
	if ie, ok := err.(*importError); ok {
		return c.Status(ie.StatusCode).JSON(fiber.Map{
			"error": ie.Error(),
		})
	}
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
		"error": err.Error(),
	})
}

// escapeSQLString escapes single quotes for safe use in DuckDB SQL strings
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// handleTLEImport handles TLE file upload and import.
// Uses the same ArrowBuffer ingest pipeline as streaming TLE ingestion.
func (h *ImportHandler) handleTLEImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	start := time.Now()

	database := c.Get("x-arc-database")
	if database == "" {
		database = c.Query("db")
	}
	if database == "" {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "database is required (set x-arc-database header or db query param)",
		})
	}

	if !isValidDatabaseName(database) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	if h.arrowBuffer == nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "TLE import not available (ingest pipeline not configured)",
		})
	}

	// Get uploaded file
	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no file uploaded: use multipart/form-data with field name 'file'",
		})
	}

	// Read file contents
	file, err := fileHeader.Open()
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to open uploaded file: " + err.Error(),
		})
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to read uploaded file: " + err.Error(),
		})
	}

	const maxImportSize = 500 * 1024 * 1024 // 500MB

	// Detect and decompress gzip (magic bytes: 0x1f 0x8b)
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, gzErr := gzip.NewReader(bytes.NewReader(data))
		if gzErr != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to decompress gzip file: " + gzErr.Error(),
			})
		}
		decompressed, gzErr := io.ReadAll(io.LimitReader(reader, maxImportSize+1))
		reader.Close()
		if gzErr != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to decompress gzip file: " + gzErr.Error(),
			})
		}
		if len(decompressed) > maxImportSize {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
				"error": "decompressed file exceeds 500MB limit",
			})
		}
		data = decompressed
	}

	if len(data) > maxImportSize {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": "file exceeds 500MB limit",
		})
	}

	if len(data) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "file is empty",
		})
	}

	// Measurement name from header (default: satellite_tle)
	measurement := c.Get("x-arc-measurement", "satellite_tle")
	if !isValidMeasurementName(measurement) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement),
		})
	}

	// Parse TLE data
	parser := ingest.NewTLEParser()
	tleRecords, warnings := parser.ParseTLEFile(data)
	if len(tleRecords) == 0 {
		h.totalErrors.Add(1)
		errMsg := "no valid TLE records found in file"
		if len(warnings) > 0 {
			errMsg = fmt.Sprintf("no valid TLE records found in file (warnings: %v)", warnings)
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": errMsg,
		})
	}

	satelliteCount := len(tleRecords)

	// Convert directly to typed columnar format (single pass, no []interface{})
	batch, numRecords := ingest.TLERecordsToTypedColumnar(tleRecords)

	// Check RBAC permissions
	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		if err := CheckWritePermissions(c, h.rbacManager, h.logger, database, []string{measurement}); err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Write directly to the ArrowBuffer ingest pipeline (typed path)
	totalRows := int64(numRecords)
	if err := h.arrowBuffer.WriteTypedColumnarDirect(c.Context(), database, measurement, batch, numRecords); err != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Msg("TLE import: failed to write to buffer")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("failed to ingest measurement %q: %v", measurement, err),
		})
	}

	// Force flush to ensure data is persisted before returning
	if err := h.arrowBuffer.FlushAll(c.Context()); err != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(err).Str("database", database).Msg("TLE import: flush failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to flush imported data: " + err.Error(),
		})
	}

	durationMs := time.Since(start).Milliseconds()
	h.totalRecords.Add(totalRows)

	h.logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Int("satellites", satelliteCount).
		Int64("rows", totalRows).
		Int("warnings", len(warnings)).
		Int64("duration_ms", durationMs).
		Msg("TLE import completed")

	return c.JSON(fiber.Map{
		"status": "ok",
		"result": TLEImportResult{
			Database:       database,
			Measurement:    measurement,
			SatelliteCount: satelliteCount,
			RowsImported:   totalRows,
			ParseWarnings:  warnings,
			DurationMs:     durationMs,
		},
	})
}

// Stats returns import handler statistics
func (h *ImportHandler) Stats(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status": "success",
		"stats": fiber.Map{
			"total_requests": h.totalRequests.Load(),
			"total_records":  h.totalRecords.Load(),
			"total_errors":   h.totalErrors.Load(),
		},
	})
}
