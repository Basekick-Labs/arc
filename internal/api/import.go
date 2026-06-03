package api

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/ingest"
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

// ImportHandler handles bulk CSV, Parquet, Line Protocol, and TLE file imports.
//
// All formats parse rows in-process and ingest through the ArrowBuffer pipeline
// (the same path as streaming writes). No import path issues DuckDB queries
// against the uploaded file, so imports do not require the upload directory to
// be in the DuckDB sandbox allowlist. (The allowlist entry that still exists is
// retained solely for delete.go's S3 COPY ... TO staging — see cmd/arc/main.go.)
type ImportHandler struct {
	logger zerolog.Logger

	// arrowBuffer is the streaming ingest pipeline used by every import format.
	arrowBuffer *ingest.ArrowBuffer

	// authManager holds the concrete *auth.AuthManager. See
	// MsgPackHandler.authManager for the full rationale. Imports use
	// admin-tier auth because they rewrite historical data.
	authManager *auth.AuthManager
	rbacManager RBACChecker

	// Stats
	totalRequests atomic.Int64
	totalRecords  atomic.Int64
	totalErrors   atomic.Int64
}

// NewImportHandler creates a new ImportHandler. The ArrowBuffer must be set via
// SetArrowBuffer before any import is served.
func NewImportHandler(logger zerolog.Logger) *ImportHandler {
	return &ImportHandler{
		logger: logger.With().Str("component", "import-handler").Logger(),
	}
}

// SetArrowBuffer sets the ArrowBuffer for Line Protocol import
func (h *ImportHandler) SetArrowBuffer(buf *ingest.ArrowBuffer) {
	h.arrowBuffer = buf
}

// SetAuthAndRBAC sets the auth and RBAC managers. See
// MsgPackHandler.SetAuthAndRBAC for the full rationale.
func (h *ImportHandler) SetAuthAndRBAC(authManager *auth.AuthManager, rbacManager RBACChecker) {
	h.authManager = authManager
	h.rbacManager = rbacManager
}

// RegisterRoutes registers import API routes. Import endpoints write
// historical data and use admin-tier auth (not write-tier) — bulk
// imports can rewrite or supplant existing partitions, so the
// stricter gate is appropriate.
func (h *ImportHandler) RegisterRoutes(app *fiber.App) {
	adminAuth := withAdminAuth(h.authManager)

	app.Post("/api/v1/import/csv", adminAuth, h.handleCSVImport)
	app.Post("/api/v1/import/parquet", adminAuth, h.handleParquetImport)
	app.Post("/api/v1/import/lp", adminAuth, h.handleLineProtocolImport)
	app.Post("/api/v1/import/tle", adminAuth, h.handleTLEImport)
	app.Get("/api/v1/import/stats", h.Stats)

	h.logger.Info().Msg("Import routes registered")
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

	// Size limit (maxImportSize, package-level) applies to both compressed and
	// uncompressed data. Bound the read at the limit (+1 to detect overflow) so an
	// oversized upload is rejected without buffering the whole body — the global
	// BodyLimit (1GB) is a coarser backstop; this matches the CSV/Parquet handlers.
	data, err := io.ReadAll(io.LimitReader(file, maxImportSize+1))
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to read uploaded file: " + err.Error(),
		})
	}
	if int64(len(data)) > maxImportSize {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": "file exceeds 500MB limit",
		})
	}

	// Detect and decompress gzip (magic bytes: 0x1f 0x8b)
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		decompressed, err := decompressGzipPooled(data, maxImportSize)
		if err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to decompress gzip file: " + err.Error(),
			})
		}
		data = decompressed
	}

	// Enforce size limit on uncompressed data (decompressed gzip may exceed it)
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
	for measurement, record := range columnarByMeasurement {
		if err := h.arrowBuffer.WriteColumnarRecord(c.Context(), database, record); err != nil {
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
		if timeCol, ok := record.Columns["time"]; ok {
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

	// Bound the read at maxImportSize (+1 to detect overflow) so an oversized
	// upload is rejected without buffering the whole body — matches CSV/Parquet/LP.
	data, err := io.ReadAll(io.LimitReader(file, maxImportSize+1))
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to read uploaded file: " + err.Error(),
		})
	}
	if int64(len(data)) > maxImportSize {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": "file exceeds 500MB limit",
		})
	}

	// Detect and decompress gzip (magic bytes: 0x1f 0x8b)
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		decompressed, gzErr := decompressGzipPooled(data, maxImportSize)
		if gzErr != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to decompress gzip file: " + gzErr.Error(),
			})
		}
		data = decompressed
	}

	// Enforce size limit on uncompressed data (decompressed gzip may exceed it)
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
