package api

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
)

// maxImportSize bounds in-memory buffering of an uploaded import file.
// Mirrors the limit already enforced by the LP/TLE import handlers.
const maxImportSize = 500 * 1024 * 1024 // 500MB

// microPerHour matches ingest.groupByHour's bucketing (t / 3_600_000_000).
// Used to compute partitions_created in-process, identical to what the
// ArrowBuffer will write for a single-flush import.
const microPerHour = int64(3_600_000_000)

// =============================================================================
// CSV import — in-process parsing, no DuckDB on the temp file.
// =============================================================================

// handleCSVImport parses an uploaded CSV in-process and ingests it through the
// ArrowBuffer pipeline (same path as LP/TLE imports). It never issues DuckDB
// queries against the uploaded file, so the upload does not need to be in the
// DuckDB sandbox allowlist.
func (h *ImportHandler) handleCSVImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	start := time.Now()

	database, measurement, errResp := h.importPreamble(c)
	if errResp != nil {
		return errResp
	}

	if h.arrowBuffer == nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "import handler is misconfigured: ArrowBuffer is not set",
		})
	}

	opts := importOptions{
		format:     "csv",
		timeColumn: c.Query("time_column", "time"),
		timeFormat: c.Query("time_format", ""),
		delimiter:  c.Query("delimiter", ","),
		skipRows:   c.QueryInt("skip_rows", 0),
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no file uploaded: use multipart/form-data with field name 'file'",
		})
	}
	if fileHeader.Size > maxImportSize {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": fmt.Sprintf("file exceeds maximum import size of %d bytes", maxImportSize),
		})
	}

	f, err := fileHeader.Open()
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to open uploaded file: " + err.Error(),
		})
	}
	defer f.Close()

	// Defense in depth: bound how much we read even if Size under-reports
	// (e.g. chunked uploads). Use an erroring limit reader (NOT io.LimitReader,
	// which returns EOF at the cap — the CSV parser would treat that as a clean
	// end-of-file and silently import a truncated file with a 200 response).
	result, ierr := h.importCSV(c.Context(), database, measurement, &limitedReader{r: f, limit: maxImportSize}, opts)
	if ierr != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(ierr).Str("database", database).Str("measurement", measurement).Msg("CSV import failed")
		return h.importErrorResponse(c, ierr)
	}

	result.DurationMs = time.Since(start).Milliseconds()
	h.totalRecords.Add(result.RowsImported)
	h.logger.Info().
		Str("database", database).Str("measurement", measurement).
		Int64("rows", result.RowsImported).Int("partitions", result.PartitionsCreated).
		Int64("duration_ms", result.DurationMs).Msg("CSV import completed")

	return c.JSON(fiber.Map{"status": "ok", "result": result})
}

// importCSV reads the CSV stream, infers per-column types, normalizes the time
// column to int64 microseconds, and ingests via WriteColumnarRecord.
func (h *ImportHandler) importCSV(ctx fiberContext, database, measurement string, r io.Reader, opts importOptions) (*ImportResult, *importError) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // tolerate ragged rows; we validate against the header
	reader.LazyQuotes = true    // tolerate unescaped quotes in user-uploaded CSVs
	// NOTE: deliberately NOT setting ReuseRecord. We retain the `header` slice
	// across all subsequent Read() calls (for validation, column naming, and the
	// result), and ReuseRecord would overwrite its backing array with later rows.
	if opts.delimiter != "" {
		runes := []rune(opts.delimiter)
		if len(runes) != 1 {
			return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("delimiter must be a single character, got %q", opts.delimiter)}
		}
		reader.Comma = runes[0]
	}

	// skip_rows: discard N leading rows before the header.
	for i := 0; i < opts.skipRows; i++ {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file is empty"}
			}
			return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "failed to skip rows", Err: err}
		}
	}

	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file is empty"}
		}
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "failed to read CSV header", Err: err}
	}
	if len(header) == 0 {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file is empty"}
	}
	// Strip a UTF-8 BOM from the first header field (Excel-on-Windows exports
	// prepend one); otherwise the first column name carries the BOM and a
	// time_column like "time" won't match.
	header[0] = strings.TrimPrefix(header[0], "\xef\xbb\xbf")

	timeIdx, herr := validateImportHeader(header, opts.timeColumn)
	if herr != nil {
		return nil, herr
	}

	// Read all rows as raw strings, one slice per column.
	rawCols := make([][]string, len(header))
	for i := range rawCols {
		rawCols[i] = make([]string, 0, 1024)
	}
	rowCount := 0
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// csv.Reader wraps the underlying reader's error; surface the size
			// cap as 413 rather than a generic 422 parse error.
			if errors.Is(err, errImportTooLarge) {
				return nil, &importError{StatusCode: fiber.StatusRequestEntityTooLarge, Message: fmt.Sprintf("file exceeds maximum import size of %d bytes", maxImportSize)}
			}
			return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: fmt.Sprintf("failed to parse CSV at row %d", rowCount+1), Err: err}
		}
		for i := range header {
			if i < len(rec) {
				rawCols[i] = append(rawCols[i], rec[i])
			} else {
				rawCols[i] = append(rawCols[i], "") // ragged row: missing trailing fields -> empty
			}
		}
		rowCount++
	}

	if rowCount == 0 {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file contains no rows"}
	}

	// Normalize the time column to int64 micros.
	timeMicros, terr := stringsToTimeMicros(rawCols[timeIdx], opts.timeFormat)
	if terr != nil {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("failed to parse time column %q", opts.timeColumn), Err: terr}
	}

	// Build typed columnar data (rename time column to "time") and ingest via
	// WriteTypedColumnarDirect, avoiding per-value []interface{} boxing.
	cols := make(map[string]interface{}, len(header))
	validity := make(map[string][]bool, len(header))
	for i, name := range header {
		if i == timeIdx {
			continue // handled separately
		}
		vals, valid := inferAndConvertColumn(rawCols[i])
		cols[name] = vals
		if valid != nil {
			validity[name] = valid
		}
	}
	cols["time"] = timeMicros

	batch := &ingest.TypedColumnBatch{
		Data:     cols,
		Validity: validity,
	}
	if err := h.arrowBuffer.WriteTypedColumnarDirect(ctx, database, measurement, batch, rowCount); err != nil {
		return nil, &importError{StatusCode: fiber.StatusInternalServerError, Message: "failed to ingest CSV data", Err: err}
	}
	if err := h.arrowBuffer.FlushAll(ctx); err != nil {
		return nil, &importError{StatusCode: fiber.StatusInternalServerError, Message: "failed to flush imported data", Err: err}
	}

	return buildImportResult(database, measurement, header, opts.timeColumn, timeMicros), nil
}

// =============================================================================
// Parquet import — in-process arrow-go read, no DuckDB on the temp file.
// =============================================================================

// handleParquetImport reads an uploaded Parquet file in-process via arrow-go and
// ingests it through the ArrowBuffer pipeline. No DuckDB queries against the file.
func (h *ImportHandler) handleParquetImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	start := time.Now()

	database, measurement, errResp := h.importPreamble(c)
	if errResp != nil {
		return errResp
	}

	if h.arrowBuffer == nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "import handler is misconfigured: ArrowBuffer is not set",
		})
	}

	opts := importOptions{
		format:     "parquet",
		timeColumn: c.Query("time_column", "time"),
		timeFormat: c.Query("time_format", ""),
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no file uploaded: use multipart/form-data with field name 'file'",
		})
	}

	f, err := fileHeader.Open()
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to open uploaded file: " + err.Error(),
		})
	}
	defer f.Close()

	// Parquet readers need random access; buffer the upload in memory (bounded).
	data, err := io.ReadAll(io.LimitReader(f, maxImportSize+1))
	if err != nil {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to read uploaded file: " + err.Error()})
	}
	if int64(len(data)) > maxImportSize {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{"error": fmt.Sprintf("file exceeds maximum import size of %d bytes", maxImportSize)})
	}
	if len(data) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "file is empty"})
	}

	result, ierr := h.importParquet(c.Context(), database, measurement, data, opts)
	if ierr != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(ierr).Str("database", database).Str("measurement", measurement).Msg("Parquet import failed")
		return h.importErrorResponse(c, ierr)
	}

	result.DurationMs = time.Since(start).Milliseconds()
	h.totalRecords.Add(result.RowsImported)
	h.logger.Info().
		Str("database", database).Str("measurement", measurement).
		Int64("rows", result.RowsImported).Int("partitions", result.PartitionsCreated).
		Int64("duration_ms", result.DurationMs).Msg("Parquet import completed")

	return c.JSON(fiber.Map{"status": "ok", "result": result})
}

// importParquet reads the in-memory Parquet bytes into Arrow arrays, normalizes
// the time column to int64 micros, and ingests via WriteColumnarRecord.
func (h *ImportHandler) importParquet(ctx fiberContext, database, measurement string, data []byte, opts importOptions) (*ImportResult, *importError) {
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to read parquet file", Err: err}
	}
	defer pf.Close()

	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to open parquet reader", Err: err}
	}

	tbl, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to read parquet table", Err: err}
	}
	defer tbl.Release()

	schema := tbl.Schema()
	header := make([]string, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		header[i] = schema.Field(i).Name
	}

	timeFieldIdx, herr := validateImportHeader(header, opts.timeColumn)
	if herr != nil {
		return nil, herr
	}

	numRows := int(tbl.NumRows())
	if numRows == 0 {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file contains no rows"}
	}

	// Extract typed slices directly from the Arrow chunks (no []interface{}
	// boxing) and ingest via WriteTypedColumnarDirect.
	cols := make(map[string]interface{}, len(header))
	validity := make(map[string][]bool, len(header))
	var timeMicros []int64
	for i, name := range header {
		// Time column: normalize to micros via the dedicated path.
		if i == timeFieldIdx {
			tm, terr := parquetColumnToTimeMicros(tbl.Column(i), opts.timeFormat)
			if terr != nil {
				return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("failed to parse time column %q", opts.timeColumn), Err: terr}
			}
			timeMicros = tm
			continue
		}
		vals, valid, conv := arrowColumnToTyped(tbl.Column(i))
		if conv != nil {
			return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: fmt.Sprintf("unsupported parquet column %q", name), Err: conv}
		}
		cols[name] = vals
		if valid != nil {
			validity[name] = valid
		}
	}
	cols["time"] = timeMicros

	batch := &ingest.TypedColumnBatch{
		Data:     cols,
		Validity: validity,
	}
	if err := h.arrowBuffer.WriteTypedColumnarDirect(ctx, database, measurement, batch, numRows); err != nil {
		return nil, &importError{StatusCode: fiber.StatusInternalServerError, Message: "failed to ingest parquet data", Err: err}
	}
	if err := h.arrowBuffer.FlushAll(ctx); err != nil {
		return nil, &importError{StatusCode: fiber.StatusInternalServerError, Message: "failed to flush imported data", Err: err}
	}

	return buildImportResult(database, measurement, header, opts.timeColumn, timeMicros), nil
}

// =============================================================================
// Shared helpers
// =============================================================================

// fiberContext is the context type accepted by ArrowBuffer write methods.
// c.Context() returns context.Context; aliased for readability.
type fiberContext = context.Context

// importPreamble validates the database/measurement and RBAC, returning a ready
// error response if validation fails. Shared by CSV and Parquet handlers.
func (h *ImportHandler) importPreamble(c *fiber.Ctx) (string, string, error) {
	database := c.Get("x-arc-database")
	if database == "" {
		database = c.Query("db")
	}
	if database == "" {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "database is required (set x-arc-database header or db query param)"})
	}
	if !isValidDatabaseName(database) {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)"})
	}

	measurement := c.Query("measurement")
	if measurement == "" {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "measurement query parameter is required"})
	}
	if !isValidMeasurementName(measurement) {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement)})
	}

	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		if err := CheckWritePermissions(c, h.rbacManager, h.logger, database, []string{measurement}); err != nil {
			h.totalErrors.Add(1)
			return "", "", c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": err.Error()})
		}
	}

	return database, measurement, nil
}

// validateImportHeader rejects header shapes that would cause silent data loss
// when columns are keyed by name into a map[string][]interface{}:
//   - empty column names (e.g. a trailing comma in a CSV header) — these would
//     pass through to the Arrow schema builder, which indexes name[0] without a
//     length check and panics on the empty string (a DoS vector),
//   - duplicate column names (one would overwrite the other), and
//   - a literal "time" column colliding with a renamed non-"time" time_column.
//
// Returns the index of timeColumn in the header on success.
func validateImportHeader(header []string, timeColumn string) (int, *importError) {
	timeIdx := -1
	seen := make(map[string]struct{}, len(header))
	for i, name := range header {
		if name == "" {
			return -1, &importError{
				StatusCode: fiber.StatusBadRequest,
				Message:    "column name cannot be empty (check for trailing/empty delimiters in the header)",
			}
		}
		if _, dup := seen[name]; dup {
			return -1, &importError{
				StatusCode: fiber.StatusBadRequest,
				Message:    fmt.Sprintf("duplicate column name %q in file; column names must be unique", name),
			}
		}
		seen[name] = struct{}{}
		if name == timeColumn {
			timeIdx = i
		}
	}
	if timeIdx == -1 {
		return -1, &importError{
			StatusCode: fiber.StatusBadRequest,
			Message:    fmt.Sprintf("time column %q not found in file; available columns: %s", timeColumn, strings.Join(header, ", ")),
		}
	}
	// The time column is renamed to "time" before ingest. If the file already has
	// a different column literally named "time", renaming would overwrite it.
	if timeColumn != "time" {
		if _, hasTime := seen["time"]; hasTime {
			return -1, &importError{
				StatusCode: fiber.StatusBadRequest,
				Message:    fmt.Sprintf("cannot rename time column %q to \"time\": a column named \"time\" already exists in the file", timeColumn),
			}
		}
	}
	return timeIdx, nil
}

// buildImportResult computes the ImportResult fields in-process from the parsed
// time column, with no follow-up DuckDB query. partitions_created counts distinct
// hour buckets exactly as ingest.groupByHour will (t / microPerHour).
func buildImportResult(database, measurement string, header []string, timeColumn string, timeMicros []int64) *ImportResult {
	// Output columns: header with the time column renamed to "time".
	cols := make([]string, len(header))
	for i, c := range header {
		if c == timeColumn {
			cols[i] = "time"
		} else {
			cols[i] = c
		}
	}

	var minT, maxT int64
	hourBuckets := make(map[int64]struct{})
	for i, t := range timeMicros {
		if i == 0 || t < minT {
			minT = t
		}
		if i == 0 || t > maxT {
			maxT = t
		}
		hourBuckets[t/microPerHour] = struct{}{}
	}

	return &ImportResult{
		Database:          database,
		Measurement:       measurement,
		RowsImported:      int64(len(timeMicros)),
		PartitionsCreated: len(hourBuckets),
		TimeRangeMin:      time.UnixMicro(minT).UTC().Format(time.RFC3339Nano),
		TimeRangeMax:      time.UnixMicro(maxT).UTC().Format(time.RFC3339Nano),
		Columns:           cols,
	}
}

// inferAndConvertColumn scans all cells of a string column and converts to the
// narrowest native type that fits the whole column: int64, then float64, then
// bool, else string. It returns a flat typed slice ([]int64/[]float64/[]bool/
// []string) and a null-validity bitmap (nil when the column has no empty cells;
// validity[i]=false marks an empty numeric/bool cell as null). String columns
// keep empty cells as "" and have no nulls.
//
// Returning typed slices directly (rather than a boxed []interface{}) lets the
// caller use WriteTypedColumnarDirect, avoiding per-value boxing — same reason
// the Parquet path uses arrowColumnToTyped.
func inferAndConvertColumn(raw []string) (interface{}, []bool) {
	isInt, isFloat, isBool := true, true, true
	hasValue, hasEmpty := false, false
	for _, s := range raw {
		if s == "" {
			hasEmpty = true
			continue // empty cells don't constrain the type
		}
		hasValue = true
		if isInt {
			if _, err := strconv.ParseInt(s, 10, 64); err != nil {
				isInt = false
			}
		}
		// Any valid base-10 integer is also a valid float, so only parse as
		// float once a value has disqualified the integer type.
		if isFloat && !isInt {
			if _, err := strconv.ParseFloat(s, 64); err != nil {
				isFloat = false
			}
		}
		if isBool {
			if !isBoolLiteral(s) {
				isBool = false
			}
		}
		// Once every candidate type is ruled out the column is a string; stop
		// scanning. (hasValue is already true here, and string columns ignore
		// hasEmpty — empty cells are valid "" values, not nulls — so an early
		// break can't change the result.)
		if !isInt && !isFloat && !isBool {
			break
		}
	}

	// An all-empty column has no values to constrain the type; default to string
	// (keep the empty strings) rather than letting it fall through to int64.
	// String columns carry no nulls (empty string is a valid value).
	if !hasValue || (!isInt && !isFloat && !isBool) {
		out := make([]string, len(raw))
		copy(out, raw)
		return out, nil
	}

	// Numeric/bool columns: build a validity bitmap only if empty cells exist.
	var validity []bool
	if hasEmpty {
		validity = make([]bool, len(raw))
		for i, s := range raw {
			validity[i] = s != ""
		}
	}

	switch {
	case isInt:
		out := make([]int64, len(raw))
		for i, s := range raw {
			if s != "" {
				out[i], _ = strconv.ParseInt(s, 10, 64)
			}
		}
		return out, validity
	case isFloat:
		out := make([]float64, len(raw))
		for i, s := range raw {
			if s != "" {
				out[i], _ = strconv.ParseFloat(s, 64)
			}
		}
		return out, validity
	default: // isBool
		out := make([]bool, len(raw))
		for i, s := range raw {
			if s != "" {
				out[i] = strings.EqualFold(s, "true") || s == "1"
			}
		}
		return out, validity
	}
}

// isBoolLiteral reports whether s is a recognized boolean literal. Uses
// EqualFold / direct comparison to avoid the per-call allocation of ToLower.
func isBoolLiteral(s string) bool {
	return s == "1" || s == "0" || strings.EqualFold(s, "true") || strings.EqualFold(s, "false")
}

// stringsToTimeMicros converts a string time column to []int64 microseconds.
// Explicit epoch_* formats are deterministic; "" (auto) magnitude-detects numeric
// values and falls back to parsing common timestamp string layouts. In the auto
// string case the matched layout is cached and tried first on subsequent rows —
// CSV time columns are homogeneous, so this avoids re-scanning all layouts per row.
func stringsToTimeMicros(raw []string, timeFormat string) ([]int64, error) {
	out := make([]int64, len(raw))
	cachedLayout := ""
	for i, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("empty value in time column at row %d", i+1)
		}

		// Explicit epoch format.
		if timeFormat != "" {
			micros, err := oneTimeValueToMicros(s, timeFormat)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", i+1, err)
			}
			out[i] = micros
			continue
		}

		// Auto: integer epoch -> int64 math (full precision for large ns epochs).
		if !strings.Contains(s, ".") {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				out[i] = autoIntEpochToMicros(n)
				continue
			}
		}
		// Auto: fractional epoch -> float magnitude detection.
		if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			out[i] = autoEpochToMicros(f)
			continue
		}

		// Auto: timestamp string. Try the cached layout first.
		if cachedLayout != "" {
			if t, err := time.Parse(cachedLayout, s); err == nil {
				out[i] = t.UTC().UnixMicro()
				continue
			}
		}
		micros, layout, err := parseTimestampString(s)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}
		cachedLayout = layout
		out[i] = micros
	}
	return out, nil
}

// oneTimeValueToMicros converts a single time value with an explicit time_format
// or auto-detection. Used for the (rare) Parquet string time column; the CSV path
// uses stringsToTimeMicros directly with layout caching.
func oneTimeValueToMicros(s, timeFormat string) (int64, error) {
	// Trim so Parquet string/binary time columns behave like the CSV path
	// (stringsToTimeMicros trims before parsing); a value like " 1609459200 "
	// should parse identically regardless of source.
	s = strings.TrimSpace(s)
	switch timeFormat {
	case "epoch_s", "epoch_ms", "epoch_us", "epoch_ns":
		// Integer epoch -> int64 math (no float precision loss for large ns).
		if !strings.Contains(s, ".") {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				return intTimeToMicros(n, timeFormat), nil
			}
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid epoch value %q: %w", s, err)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("invalid epoch value %q: NaN or Inf", s)
		}
		return epochToMicros(f, timeFormat), nil
	case "":
		// Auto: integer epoch -> int64 math; fractional -> float; else timestamp string.
		if !strings.Contains(s, ".") {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				return autoIntEpochToMicros(n), nil
			}
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return autoEpochToMicros(f), nil
		}
		micros, _, err := parseTimestampString(s)
		return micros, err
	default:
		return 0, fmt.Errorf("unsupported time_format %q (want epoch_s|epoch_ms|epoch_us|epoch_ns or empty for auto)", timeFormat)
	}
}

// epochToMicros converts an epoch value (float, to allow fractional seconds like
// 1609459200.123) in the given unit to int64 microseconds.
func epochToMicros(f float64, format string) int64 {
	switch format {
	case "epoch_s":
		return int64(f * 1_000_000)
	case "epoch_ms":
		return int64(f * 1_000)
	case "epoch_us":
		return int64(f)
	case "epoch_ns":
		return int64(f / 1_000)
	default:
		return int64(f)
	}
}

// autoEpochToMicros detects the unit by magnitude, matching the heuristic in
// ingest.MessagePackDecoder.normalizeTimestamps. Magnitude is taken on the
// absolute value so negative epochs (historical data before 1970) classify by
// the same thresholds rather than always falling into the "seconds" branch.
// Takes float64 to support fractional epochs (sub-second precision preserved).
func autoEpochToMicros(f float64) int64 {
	absF := math.Abs(f)
	switch {
	case absF < 1e10: // seconds
		return int64(f * 1_000_000)
	case absF < 1e13: // milliseconds
		return int64(f * 1_000)
	case absF < 1e16: // microseconds
		return int64(f)
	default: // nanoseconds
		return int64(f / 1_000)
	}
}

// parseTimestampString parses common timestamp string layouts (UTC if no zone),
// returning the micros and the layout that matched (so callers can cache it).
func parseTimestampString(s string) (int64, string, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().UnixMicro(), layout, nil
		}
	}
	return 0, "", fmt.Errorf("unrecognized timestamp %q (supported: RFC3339, 'YYYY-MM-DD[ T]HH:MM:SS[.fff]', 'YYYY-MM-DD', or set time_format)", s)
}

// =============================================================================
// Arrow column conversion
// =============================================================================

// arrowColumnToTyped converts a (possibly chunked) Arrow column to a flat typed
// Go slice ([]int64 / []float64 / []string / []bool) plus a null-validity bitmap
// (nil when the column has no nulls; validity[i]=false marks a null).
//
// This feeds ArrowBuffer.WriteTypedColumnarDirect, avoiding the per-value boxing
// of a []interface{} intermediate. Measured on 1M rows × 4 cols, the typed path
// is ~13x faster and allocates ~8 slices vs ~4M boxed values.
func arrowColumnToTyped(col *arrow.Column) (interface{}, []bool, error) {
	if col == nil || col.Data() == nil {
		return nil, nil, fmt.Errorf("column or column data is nil")
	}
	length := col.Len()
	chunks := col.Data().Chunks()
	if len(chunks) == 0 {
		return nil, nil, fmt.Errorf("column has no data chunks")
	}

	// Validity bitmap only when the column actually contains nulls.
	var validity []bool
	if col.Data().NullN() > 0 {
		validity = make([]bool, length)
		idx := 0
		for _, chunk := range chunks {
			for i := 0; i < chunk.Len(); i++ {
				validity[idx] = !chunk.IsNull(i)
				idx++
			}
		}
	}

	switch chunks[0].(type) {
	case *array.Int64, *array.Int32, *array.Int16, *array.Int8,
		*array.Uint64, *array.Uint32, *array.Uint16, *array.Uint8:
		res := make([]int64, length)
		idx := 0
		for _, chunk := range chunks {
			switch a := chunk.(type) {
			case *array.Int64:
				for i := 0; i < a.Len(); i++ {
					res[idx] = a.Value(i)
					idx++
				}
			case *array.Int32:
				for i := 0; i < a.Len(); i++ {
					res[idx] = int64(a.Value(i))
					idx++
				}
			case *array.Int16:
				for i := 0; i < a.Len(); i++ {
					res[idx] = int64(a.Value(i))
					idx++
				}
			case *array.Int8:
				for i := 0; i < a.Len(); i++ {
					res[idx] = int64(a.Value(i))
					idx++
				}
			case *array.Uint64:
				for i := 0; i < a.Len(); i++ {
					res[idx] = int64(a.Value(i))
					idx++
				}
			case *array.Uint32:
				for i := 0; i < a.Len(); i++ {
					res[idx] = int64(a.Value(i))
					idx++
				}
			case *array.Uint16:
				for i := 0; i < a.Len(); i++ {
					res[idx] = int64(a.Value(i))
					idx++
				}
			case *array.Uint8:
				for i := 0; i < a.Len(); i++ {
					res[idx] = int64(a.Value(i))
					idx++
				}
			default:
				return nil, nil, fmt.Errorf("mixed chunk types in integer column: %s", chunk.DataType())
			}
		}
		return res, validity, nil
	case *array.Float64, *array.Float32:
		res := make([]float64, length)
		idx := 0
		for _, chunk := range chunks {
			switch a := chunk.(type) {
			case *array.Float64:
				for i := 0; i < a.Len(); i++ {
					res[idx] = a.Value(i)
					idx++
				}
			case *array.Float32:
				for i := 0; i < a.Len(); i++ {
					res[idx] = float64(a.Value(i))
					idx++
				}
			default:
				return nil, nil, fmt.Errorf("mixed chunk types in float column: %s", chunk.DataType())
			}
		}
		return res, validity, nil
	case *array.String:
		res := make([]string, length)
		idx := 0
		for _, chunk := range chunks {
			a := chunk.(*array.String)
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					res[idx] = a.Value(i)
				}
				idx++
			}
		}
		return res, validity, nil
	case *array.Binary:
		// BYTE_ARRAY without a UTF8 logical annotation reads as Binary, not
		// String (common from Spark and some writers). Treat as string.
		res := make([]string, length)
		idx := 0
		for _, chunk := range chunks {
			a := chunk.(*array.Binary)
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					res[idx] = string(a.Value(i))
				}
				idx++
			}
		}
		return res, validity, nil
	case *array.FixedSizeBinary:
		res := make([]string, length)
		idx := 0
		for _, chunk := range chunks {
			a := chunk.(*array.FixedSizeBinary)
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					res[idx] = string(a.Value(i))
				}
				idx++
			}
		}
		return res, validity, nil
	case *array.Boolean:
		res := make([]bool, length)
		idx := 0
		for _, chunk := range chunks {
			a := chunk.(*array.Boolean)
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					res[idx] = a.Value(i)
				}
				idx++
			}
		}
		return res, validity, nil
	case *array.Decimal128:
		// Arc's ingest pipeline only carries decimals when a measurement has an
		// explicit DecimalSpec configured; imports don't. Convert to float64
		// (DECIMAL -> DOUBLE), the same type CSV infers for decimal values. Lossy
		// for very high-precision decimals but matches how imports are queried.
		res := make([]float64, length)
		idx := 0
		for _, chunk := range chunks {
			a := chunk.(*array.Decimal128)
			scale := a.DataType().(*arrow.Decimal128Type).Scale
			for i := 0; i < a.Len(); i++ {
				if !a.IsNull(i) {
					res[idx] = a.Value(i).ToFloat64(scale)
				}
				idx++
			}
		}
		return res, validity, nil
	case *array.Timestamp:
		res := make([]int64, length)
		idx := 0
		for _, chunk := range chunks {
			a := chunk.(*array.Timestamp)
			unit := a.DataType().(*arrow.TimestampType).Unit
			for i := 0; i < a.Len(); i++ {
				res[idx] = arrowTimestampToMicros(int64(a.Value(i)), unit)
				idx++
			}
		}
		return res, validity, nil
	default:
		return nil, nil, fmt.Errorf("unsupported arrow type %s", chunks[0].DataType())
	}
}

// parquetColumnToTimeMicros converts the time column to []int64 micros. Arrow
// TIMESTAMP columns convert by unit; integer columns use the epoch/auto logic.
func parquetColumnToTimeMicros(col *arrow.Column, timeFormat string) ([]int64, error) {
	if col == nil || col.Data() == nil {
		return nil, fmt.Errorf("column or column data is nil")
	}
	out := make([]int64, 0, col.Len())
	for _, chunk := range col.Data().Chunks() {
		switch a := chunk.(type) {
		case *array.Timestamp:
			unit := a.DataType().(*arrow.TimestampType).Unit
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				out = append(out, arrowTimestampToMicros(int64(a.Value(i)), unit))
			}
		case *array.Int64:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				out = append(out, intTimeToMicros(a.Value(i), timeFormat))
			}
		case *array.Int32:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				out = append(out, intTimeToMicros(int64(a.Value(i)), timeFormat))
			}
		case *array.Int16:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				out = append(out, intTimeToMicros(int64(a.Value(i)), timeFormat))
			}
		case *array.Uint64:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				out = append(out, intTimeToMicros(int64(a.Value(i)), timeFormat))
			}
		case *array.Uint32:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				out = append(out, intTimeToMicros(int64(a.Value(i)), timeFormat))
			}
		case *array.Float64:
			// Fractional epoch stored as a floating-point column.
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				v := a.Value(i)
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return nil, fmt.Errorf("invalid time value at row %d: NaN or Inf", len(out)+1)
				}
				out = append(out, floatTimeToMicros(v, timeFormat))
			}
		case *array.Float32:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				v := float64(a.Value(i))
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return nil, fmt.Errorf("invalid time value at row %d: NaN or Inf", len(out)+1)
				}
				out = append(out, floatTimeToMicros(v, timeFormat))
			}
		case *array.String:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				micros, err := oneTimeValueToMicros(a.Value(i), timeFormat)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", len(out)+1, err)
				}
				out = append(out, micros)
			}
		case *array.Binary:
			// BYTE_ARRAY without a UTF8 logical annotation reads as Binary.
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				micros, err := oneTimeValueToMicros(string(a.Value(i)), timeFormat)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", len(out)+1, err)
				}
				out = append(out, micros)
			}
		case *array.FixedSizeBinary:
			for i := 0; i < a.Len(); i++ {
				if a.IsNull(i) {
					return nil, fmt.Errorf("null value in time column at row %d", len(out)+1)
				}
				micros, err := oneTimeValueToMicros(string(a.Value(i)), timeFormat)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", len(out)+1, err)
				}
				out = append(out, micros)
			}
		default:
			return nil, fmt.Errorf("unsupported time column arrow type %s", chunk.DataType())
		}
	}
	return out, nil
}

// intTimeToMicros converts an integer epoch to micros using int64 math (no
// float64 round-trip), so large nanosecond epochs (>2^53) keep full precision.
func intTimeToMicros(n int64, timeFormat string) int64 {
	switch timeFormat {
	case "epoch_s":
		return n * 1_000_000
	case "epoch_ms":
		return n * 1_000
	case "epoch_us":
		return n
	case "epoch_ns":
		return n / 1_000
	default:
		return autoIntEpochToMicros(n)
	}
}

// autoIntEpochToMicros is the int64 counterpart of autoEpochToMicros: magnitude
// detection on an integer epoch without a lossy float64 conversion.
func autoIntEpochToMicros(n int64) int64 {
	absN := n
	if absN < 0 {
		if absN == math.MinInt64 {
			absN = math.MaxInt64
		} else {
			absN = -absN
		}
	}
	switch {
	case absN < 1e10: // seconds
		return n * 1_000_000
	case absN < 1e13: // milliseconds
		return n * 1_000
	case absN < 1e16: // microseconds
		return n
	default: // nanoseconds
		return n / 1_000
	}
}

// floatTimeToMicros converts a fractional epoch (Parquet Float/Double time
// column) to micros honoring the unit (or auto-detecting it).
func floatTimeToMicros(f float64, timeFormat string) int64 {
	if timeFormat == "" {
		return autoEpochToMicros(f)
	}
	return epochToMicros(f, timeFormat)
}

func arrowTimestampToMicros(v int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return v * 1_000_000
	case arrow.Millisecond:
		return v * 1_000
	case arrow.Microsecond:
		return v
	case arrow.Nanosecond:
		return v / 1_000
	default:
		return v
	}
}

// errImportTooLarge is returned by limitedReader when the byte cap is exceeded.
// Distinct from a parse error so callers map it to 413 rather than 422.
var errImportTooLarge = errors.New("file exceeds maximum import size")

// limitedReader caps total bytes read and returns errImportTooLarge once the
// limit is passed. Unlike io.LimitReader (which returns io.EOF at the cap and
// would make csv.Reader silently truncate the file), this surfaces an explicit
// error so an oversized upload fails instead of importing partial data.
type limitedReader struct {
	r     io.Reader
	limit int64
	read  int64
}

func (lr *limitedReader) Read(p []byte) (int, error) {
	n, err := lr.r.Read(p)
	lr.read += int64(n)
	if lr.read > lr.limit {
		return n, errImportTooLarge
	}
	return n, err
}
