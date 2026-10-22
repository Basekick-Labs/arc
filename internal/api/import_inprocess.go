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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
)

// maxImportSize bounds in-memory buffering of an uploaded import file.
// Mirrors the limit already enforced by the LP/TLE import handlers.
const maxImportSize = 500 * 1024 * 1024 // 500MB

// =============================================================================
// CSV import — in-process parsing, no DuckDB on the temp file.
// =============================================================================

// handleCSVImport parses an uploaded CSV in-process and ingests it through the
// ArrowBuffer pipeline (same path as LP/TLE imports). It never issues DuckDB
// queries against the uploaded file, so the upload does not need to be in the
// DuckDB sandbox allowlist.
func (h *ImportHandler) handleCSVImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	if handled, err := h.forwardWriteIfNeeded(c); handled {
		return err
	}
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

	upload, uerr := importUploadFrom(c)
	if uerr != nil {
		h.totalErrors.Add(1)
		return h.importErrorResponse(c, uerr)
	}
	result, ierr := h.importCSV(c, database, measurement, upload, opts)
	if ierr != nil {
		h.totalErrors.Add(1)
		if pe, ok := asImportPartial(ierr); ok {
			h.logger.Error().Err(pe.Err).Str("database", database).Str("measurement", measurement).
				Int64("rows_written", pe.RowsWritten).Msg("CSV import: failed part-way")
			return respondImportPartial(c, pe)
		}
		if !ingest.IsWriteRefusal(ierr) { // a refused write is logged by reason (#1268)
			h.logger.Error().Err(ierr).Str("database", database).Str("measurement", measurement).Msg("CSV import failed")
		}
		if ie, ok := ierr.(*importError); ok {
			return h.importErrorResponse(c, ie)
		}
		return respondWriteRefused(c, ierr)
	}

	result.DurationMs = time.Since(start).Milliseconds()
	h.totalRecords.Add(result.RowsImported)
	h.logger.Info().
		Str("database", database).Str("measurement", measurement).
		Int64("rows", result.RowsImported).Int("partitions", result.PartitionsCreated).
		Int64("duration_ms", result.DurationMs).Msg("CSV import completed")

	return c.JSON(fiber.Map{"status": "ok", "result": result})
}

// csvChunkRows is how many CSV rows an import converts and writes at once,
// at most; csvChunkCells bounds a chunk of a wide file (a 9,000-column file
// takes 111 rows at a time). Vars for tests.
var (
	csvChunkRows  = 100_000
	csvChunkCells = 1_000_000
)

// chunkRows is how many rows of columns cells fit a chunk.
func chunkRows(maxRows, maxCells, columns int) int {
	return max(1, min(maxRows, maxCells/max(columns, 1)))
}

// newImportCSVReader opens the upload as CSV positioned after its header:
// delimiter, skip_rows, the header itself and a UTF-8 BOM on it.
func newImportCSVReader(upload *importUpload, opts importOptions) (*csv.Reader, []string, *importError) {
	r, err := upload.open()
	if err != nil {
		return nil, nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "no file uploaded: use multipart/form-data with field name 'file'"}
	}
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // tolerate ragged rows; we validate against the header
	reader.LazyQuotes = true    // tolerate unescaped quotes in user-uploaded CSVs
	// NOTE: deliberately NOT setting ReuseRecord. We retain the `header` slice
	// across all subsequent Read() calls (for validation, column naming, and the
	// result), and ReuseRecord would overwrite its backing array with later rows.
	if opts.delimiter != "" {
		runes := []rune(opts.delimiter)
		if len(runes) != 1 {
			return nil, nil, &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("delimiter must be a single character, got %q", opts.delimiter)}
		}
		reader.Comma = runes[0]
	}

	// skip_rows: discard N leading rows before the header.
	for i := 0; i < opts.skipRows; i++ {
		if _, err := reader.Read(); err != nil {
			if err == io.EOF {
				return nil, nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file is empty"}
			}
			return nil, nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "failed to skip rows", Err: err}
		}
	}

	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file is empty"}
		}
		if errors.Is(err, errImportTooLarge) {
			return nil, nil, &importError{StatusCode: fiber.StatusRequestEntityTooLarge, Message: fmt.Sprintf("file exceeds maximum import size of %d bytes", maxImportSize)}
		}
		return nil, nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "failed to read CSV header", Err: err}
	}
	if len(header) == 0 {
		return nil, nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file is empty"}
	}
	// Strip a UTF-8 BOM from the first header field (Excel-on-Windows exports
	// prepend one); otherwise the first column name carries the BOM and a
	// time_column like "time" won't match.
	header[0] = strings.TrimPrefix(header[0], "\xef\xbb\xbf")
	return reader, header, nil
}

// forEachCSVChunk reads the rows after the header in chunks of csvChunkRows,
// one string slice per column (a ragged row's missing trailing cells are
// empty). firstRow is the 0-based index of the chunk's first data row.
func forEachCSVChunk(reader *csv.Reader, columns int, fn func(rawCols [][]string, firstRow int) error) *importError {
	rawCols := make([][]string, columns)
	perChunk := chunkRows(csvChunkRows, csvChunkCells, columns)
	rows, firstRow := 0, 0
	flush := func() error {
		if rows == 0 {
			return nil
		}
		err := fn(rawCols, firstRow)
		firstRow += rows
		rows = 0
		for i := range rawCols {
			rawCols[i] = rawCols[i][:0]
		}
		return err
	}
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// csv.Reader wraps the underlying reader's error; surface the size
			// cap as 413 rather than a generic 422 parse error.
			if errors.Is(err, errImportTooLarge) {
				return &importError{StatusCode: fiber.StatusRequestEntityTooLarge, Message: fmt.Sprintf("file exceeds maximum import size of %d bytes", maxImportSize)}
			}
			var perr *csv.ParseError
			if !errors.As(err, &perr) {
				// Not the CSV: the body was cut short.
				return &importError{StatusCode: fiber.StatusBadRequest, Message: "failed to read uploaded file", Err: err}
			}
			return &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: fmt.Sprintf("failed to parse CSV at row %d", firstRow+rows+1), Err: err}
		}
		for i := range rawCols {
			if i < len(rec) {
				rawCols[i] = append(rawCols[i], rec[i])
			} else {
				rawCols[i] = append(rawCols[i], "") // ragged row: missing trailing fields -> empty
			}
		}
		rows++
		if rows == perChunk {
			if err := flush(); err != nil {
				return chunkError(err)
			}
		}
	}
	if err := flush(); err != nil {
		return chunkError(err)
	}
	return nil
}

// chunkError carries an error a chunk callback returned out of a chunk loop
// that answers *importError.
func chunkError(err error) *importError {
	if ie, ok := err.(*importError); ok {
		return ie
	}
	return &importError{StatusCode: fiber.StatusInternalServerError, Message: "import failed", Err: err}
}

// importStats accumulates an import's answer fields chunk by chunk.
type importStats struct {
	rows       int64
	minT, maxT int64
	hours      map[int64]struct{}
}

func (st *importStats) add(timeMicros []int64) {
	if st.hours == nil {
		st.hours = map[int64]struct{}{}
	}
	for _, t := range timeMicros {
		if st.rows == 0 || t < st.minT {
			st.minT = t
		}
		if st.rows == 0 || t > st.maxT {
			st.maxT = t
		}
		st.rows++
		// Count distinct hour buckets exactly as ingest.groupByHour will
		// (floor division so pre-epoch times match, #312).
		st.hours[ingest.HourBucketID(t)] = struct{}{}
	}
}

func (st *importStats) result(database, measurement string, header []string, timeColumn string) *ImportResult {
	cols := make([]string, len(header))
	for i, c := range header {
		if c == timeColumn {
			cols[i] = "time"
		} else {
			cols[i] = c
		}
	}
	return &ImportResult{
		Database:          database,
		Measurement:       measurement,
		RowsImported:      st.rows,
		PartitionsCreated: len(st.hours),
		TimeRangeMin:      time.UnixMicro(st.minT).UTC().Format(time.RFC3339Nano),
		TimeRangeMax:      time.UnixMicro(st.maxT).UTC().Format(time.RFC3339Nano),
		Columns:           cols,
	}
}

// importCSV imports a CSV upload in two passes over its chunks. The
// validation pass parses every row, checks the time column and observes each
// column's type, keeping only those; nothing is written if it fails. The
// write pass converts each chunk to the types the whole file allows and
// writes it, paced to the flushes, so memory does not grow with the file
// (#1070 follow-up). An error is an *importError, an *importPartialError
// once rows were written, or a write refusal.
func (h *ImportHandler) importCSV(c *fiber.Ctx, database, measurement string, upload *importUpload, opts importOptions) (*ImportResult, error) {
	// A body this large holds a file over the limit: 413, not a refusal to
	// reserve its size.
	if len(upload.body) > maxForwardedImportBody {
		return nil, &importError{StatusCode: fiber.StatusRequestEntityTooLarge, Message: fmt.Sprintf("file exceeds maximum import size of %d bytes", maxImportSize)}
	}
	// Admit before parsing the file: refused when fenced (#1207), or when its
	// size does not fit the WAL volume's free space (#676), which stays
	// reserved for it until the import ends.
	writeCtx, release, err := h.arrowBuffer.AdmitRequest(c.Context(), int64(len(upload.body)))
	if err != nil {
		return nil, err
	}
	defer release()

	// Validation pass.
	reader, header, ierr := newImportCSVReader(upload, opts)
	if ierr != nil {
		return nil, ierr
	}
	timeIdx, ierr := validateImportHeader(header, opts.timeColumn, h.knownFolds(database, measurement))
	if ierr != nil {
		return nil, ierr
	}
	kinds := make([]csvColumnKind, len(header))
	for i := range kinds {
		kinds[i] = newCSVColumnKind()
	}
	var stats importStats
	if ierr := forEachCSVChunk(reader, len(header), func(rawCols [][]string, firstRow int) error {
		tm, err := stringsToTimeMicrosFrom(rawCols[timeIdx], opts.timeFormat, firstRow)
		if err != nil {
			return &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("failed to parse time column %q", opts.timeColumn), Err: err}
		}
		stats.add(tm)
		for i, col := range rawCols {
			if i == timeIdx {
				continue
			}
			for _, s := range col {
				kinds[i].observe(s)
			}
		}
		return nil
	}); ierr != nil {
		return nil, ierr
	}
	if stats.rows == 0 {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file contains no rows"}
	}

	// Write pass.
	reader, _, ierr = newImportCSVReader(upload, opts)
	if ierr != nil {
		return nil, ierr
	}
	w, err := newImportWriter(writeCtx, h.arrowBuffer, database)
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusServiceUnavailable, Message: "import rejected (retry)", Err: err}
	}
	defer w.close()
	if ierr := forEachCSVChunk(reader, len(header), func(rawCols [][]string, firstRow int) error {
		tm, err := stringsToTimeMicrosFrom(rawCols[timeIdx], opts.timeFormat, firstRow)
		if err != nil {
			return err // checked by the validation pass
		}
		cols := make(map[string]interface{}, len(header))
		validity := make(map[string][]bool, len(header))
		for i, name := range header {
			if i == timeIdx {
				continue
			}
			vals, valid := convertCSVColumn(rawCols[i], kinds[i])
			cols[name] = vals
			if valid != nil {
				validity[name] = valid
			}
		}
		cols["time"] = tm
		batch := &ingest.TypedColumnBatch{Data: cols, Validity: validity}
		return importWriteError(w.add(writeCtx, measurement, batch, len(tm)), "failed to ingest CSV data")
	}); ierr != nil {
		if pe, ok := asImportPartial(ierr.Err); ok {
			return nil, pe
		}
		if w.started() {
			return nil, w.partial(ierr, "")
		}
		return nil, ierr
	}
	if err := w.finish(importWriteError(w.end(writeCtx), "failed to ingest CSV data")); err != nil {
		return nil, err
	}
	// Hand what is still buffered to the flush: the rows are in the buffer and
	// the WAL (when enabled); files follow as their flushes finish.
	if err := h.arrowBuffer.FlushAll(writeCtx); err != nil {
		return nil, &importError{StatusCode: fiber.StatusInternalServerError, Message: importFlushFailed, Err: err}
	}
	return stats.result(database, measurement, header, opts.timeColumn), nil
}

// =============================================================================
// Parquet import — in-process arrow-go read, no DuckDB on the temp file.
// =============================================================================

// handleParquetImport reads an uploaded Parquet file in-process via arrow-go and
// ingests it through the ArrowBuffer pipeline. No DuckDB queries against the file.
func (h *ImportHandler) handleParquetImport(c *fiber.Ctx) error {
	h.totalRequests.Add(1)
	if handled, err := h.forwardWriteIfNeeded(c); handled {
		return err
	}
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

	upload, uerr := importUploadFrom(c)
	if uerr != nil {
		h.totalErrors.Add(1)
		return h.importErrorResponse(c, uerr)
	}
	// Parquet readers need random access: the file part read in place in the
	// request body (FormFile plus ReadAll made two copies). Row groups are
	// then read in batches.
	data, err := upload.fileBytes()
	if err != nil {
		h.totalErrors.Add(1)
		if errors.Is(err, errImportTooLarge) {
			return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{"error": fmt.Sprintf("file exceeds maximum import size of %d bytes", maxImportSize)})
		}
		// A body cut short: the file part ends before its closing delimiter.
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "failed to read uploaded file: " + err.Error()})
	}
	if len(data) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "file is empty"})
	}

	// Admit before parsing the file: refused when fenced (#1207), or when
	// its size does not fit the WAL volume's free space (#676), which stays
	// reserved for it until the import ends.
	writeCtx, release, aerr := h.arrowBuffer.AdmitRequest(c.Context(), int64(len(data)))
	if aerr != nil {
		h.totalErrors.Add(1)
		return respondWriteRefused(c, aerr)
	}
	defer release()
	result, ierr := h.importParquet(writeCtx, database, measurement, data, opts)
	if ierr != nil {
		h.totalErrors.Add(1)
		if pe, ok := asImportPartial(ierr); ok {
			h.logger.Error().Err(pe.Err).Str("database", database).Str("measurement", measurement).
				Int64("rows_written", pe.RowsWritten).Msg("Parquet import: failed part-way")
			return respondImportPartial(c, pe)
		}
		if !ingest.IsWriteRefusal(ierr) { // a refused write is logged by reason (#1268)
			h.logger.Error().Err(ierr).Str("database", database).Str("measurement", measurement).Msg("Parquet import failed")
		}
		if ie, ok := ierr.(*importError); ok {
			return h.importErrorResponse(c, ie)
		}
		return respondWriteRefused(c, ierr)
	}

	result.DurationMs = time.Since(start).Milliseconds()
	h.totalRecords.Add(result.RowsImported)
	h.logger.Info().
		Str("database", database).Str("measurement", measurement).
		Int64("rows", result.RowsImported).Int("partitions", result.PartitionsCreated).
		Int64("duration_ms", result.DurationMs).Msg("Parquet import completed")

	return c.JSON(fiber.Map{"status": "ok", "result": result})
}

// parquetBatchRows is how many Parquet rows an import converts and writes at
// once, at most; parquetBatchCells bounds a batch of a wide file. Vars for
// tests.
var (
	parquetBatchRows  = 100_000
	parquetBatchCells = 2_000_000
)

// openImportParquet opens Parquet bytes for batched reads. Buffered streams
// read a column chunk a buffer at a time; without them each row group's
// column chunks are read into memory whole, whatever the batch size, and a
// file written as one row group is read at once. Page streaming stays off:
// it reuses buffers that string values would alias.
func openImportParquet(data []byte) (*pqarrow.FileReader, *importError) {
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	props.BufferedStreamEnabled = true
	pf, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(props))
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to read parquet file", Err: err}
	}
	batch := chunkRows(parquetBatchRows, parquetBatchCells, pf.MetaData().Schema.NumColumns())
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: int64(batch)}, memory.DefaultAllocator)
	if err != nil {
		_ = pf.Close()
		return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to open parquet reader", Err: err}
	}
	return fr, nil
}

// forEachParquetBatch reads the file in batches of parquetBatchRows; columns
// nil reads every column. firstRow is the 0-based index of the batch's first
// row.
func forEachParquetBatch(ctx context.Context, fr *pqarrow.FileReader, columns []int, fn func(rec arrow.RecordBatch, firstRow int) error) error {
	rr, err := fr.GetRecordReader(ctx, columns, nil)
	if err != nil {
		return &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to read parquet table", Err: err}
	}
	defer rr.Release()
	return readParquetBatches(rr, fn)
}

// parquetBatchReader is the part of pqarrow.RecordReader the import reads.
type parquetBatchReader interface {
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
}

// readParquetBatches calls fn for each batch rr decodes. A panic while
// decoding (rr.Next) is a corrupt file, answered like any other: the
// uploaded bytes reach arrow-go's page decompressors on this goroutine, and
// klauspost/compress zstd <= 1.20.1 panics on a crafted frame
// (GHSA-fpmh-frq8-7rj8). A panic in fn is Arc's own and is not caught here.
func readParquetBatches(rr parquetBatchReader, fn func(rec arrow.RecordBatch, firstRow int) error) error {
	next := func() (ok bool, err error) {
		defer func() {
			if r := recover(); r != nil {
				ok, err = false, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to read parquet table", Err: fmt.Errorf("corrupt file: %v", r)}
			}
		}()
		return rr.Next(), nil
	}
	firstRow := 0
	for {
		ok, err := next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		rec := rr.RecordBatch()
		if err := fn(rec, firstRow); err != nil {
			return err
		}
		firstRow += int(rec.NumRows())
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to read parquet table", Err: err}
	}
	return nil
}

// importParquet imports Parquet bytes in two passes over row batches. The
// validation pass reads only the time column and checks it; nothing is
// written if it fails. The write pass converts each batch and writes it,
// paced to the flushes, so memory does not grow with the file (#1070
// follow-up). Column types come from the schema, so the first batch fails on
// an unsupported one before anything is written. An error is an
// *importError, an *importPartialError once rows were written, or a write
// refusal.
func (h *ImportHandler) importParquet(ctx fiberContext, database, measurement string, data []byte, opts importOptions) (*ImportResult, error) {
	fr, ierr := openImportParquet(data)
	if ierr != nil {
		return nil, ierr
	}
	defer fr.ParquetReader().Close()

	schema, err := fr.Schema()
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "failed to read parquet schema", Err: err}
	}
	header := make([]string, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		header[i] = schema.Field(i).Name
	}
	timeFieldIdx, herr := validateImportHeader(header, opts.timeColumn, h.knownFolds(database, measurement))
	if herr != nil {
		return nil, herr
	}

	timeColumn := func(rec arrow.RecordBatch, idx, firstRow int) ([]int64, error) {
		col := arrow.NewColumnFromArr(rec.Schema().Field(idx), rec.Column(idx))
		defer col.Release()
		tm, err := parquetColumnToTimeMicros(&col, opts.timeFormat)
		if err != nil {
			return nil, fmt.Errorf("in the %d rows from row %d (row numbers below count from there): %w", rec.NumRows(), firstRow+1, err)
		}
		return tm, nil
	}

	// Validation pass: the time column alone (the leaf column of a flat
	// schema; every column when it cannot be told apart).
	var timeOnly []int
	timeIdxInBatch := timeFieldIdx
	if leaf := fr.ParquetReader().MetaData().Schema.ColumnIndexByName(opts.timeColumn); leaf >= 0 {
		timeOnly, timeIdxInBatch = []int{leaf}, 0
	}
	var stats importStats
	if err := forEachParquetBatch(ctx, fr, timeOnly, func(rec arrow.RecordBatch, firstRow int) error {
		tm, err := timeColumn(rec, timeIdxInBatch, firstRow)
		if err != nil {
			return &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("failed to parse time column %q", opts.timeColumn), Err: err}
		}
		stats.add(tm)
		return nil
	}); err != nil {
		return nil, err
	}
	if stats.rows == 0 {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file contains no rows"}
	}

	// Write pass.
	w, err := newImportWriter(ctx, h.arrowBuffer, database)
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusServiceUnavailable, Message: "import rejected (retry)", Err: err}
	}
	defer w.close()
	if err := w.finish(forEachParquetBatch(ctx, fr, nil, func(rec arrow.RecordBatch, firstRow int) error {
		// Extract typed slices directly from the Arrow arrays (no
		// []interface{} boxing) and ingest via WriteTypedColumnarDirect.
		cols := make(map[string]interface{}, len(header))
		validity := make(map[string][]bool, len(header))
		var timeMicros []int64
		for i, name := range header {
			if i == timeFieldIdx {
				tm, err := timeColumn(rec, i, firstRow)
				if err != nil {
					return err // checked by the validation pass
				}
				timeMicros = tm
				continue
			}
			col := arrow.NewColumnFromArr(rec.Schema().Field(i), rec.Column(i))
			vals, valid, conv := arrowColumnToTyped(&col)
			col.Release()
			if conv != nil {
				if w.started() {
					return w.partial(conv, measurement)
				}
				return &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: fmt.Sprintf("unsupported parquet column %q", name), Err: conv}
			}
			cols[name] = vals
			if valid != nil {
				validity[name] = valid
			}
		}
		cols["time"] = timeMicros
		batch := &ingest.TypedColumnBatch{Data: cols, Validity: validity}
		return importWriteError(w.add(ctx, measurement, batch, len(timeMicros)), "failed to ingest parquet data")
	})); err != nil {
		return nil, err
	}
	if err := w.finish(importWriteError(w.end(ctx), "failed to ingest parquet data")); err != nil {
		return nil, err
	}
	// Hand what is still buffered to the flush: the rows are in the buffer and
	// the WAL (when enabled); files follow as their flushes finish.
	if err := h.arrowBuffer.FlushAll(ctx); err != nil {
		return nil, &importError{StatusCode: fiber.StatusInternalServerError, Message: importFlushFailed, Err: err}
	}
	return stats.result(database, measurement, header, opts.timeColumn), nil
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
	// c.Get / c.Query are zero-copy aliases into the fasthttp request buffer.
	// The database and measurement returned here are retained past the handler by
	// the async Arrow buffer (WriteTypedColumnarDirect) as storage-path segments,
	// so both are strings.Clone'd before they can escape — otherwise a concurrent
	// CSV/Parquet import reusing the buffer could redirect this write. See the
	// full explanation in msgpack.go.
	database := c.Get("x-arc-database")
	if database == "" {
		database = c.Query("db")
	}
	database = strings.Clone(database)
	if database == "" {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "database is required (set x-arc-database header or db query param)"})
	}
	if !isValidDatabaseName(database) {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)"})
	}

	measurement := strings.Clone(c.Query("measurement"))
	if measurement == "" {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "measurement query parameter is required"})
	}
	if !isValidMeasurementName(measurement) {
		h.totalErrors.Add(1)
		return "", "", c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement)})
	}

	if h.rbacManager != nil { // not license-gated; see CheckWritePermissions
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
func validateImportHeader(header []string, timeColumn string, known ...map[string]string) (int, *importError) {
	timeIdx := -1
	seen := make(map[string]struct{}, len(header))
	folds := make(map[string]string, len(header))
	var stored map[string]string
	if len(known) > 0 {
		stored = known[0]
	}
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
		if ingest.IsWALTargetColumnName(name) && name != timeColumn { // the time column is renamed "time"
			return -1, &importError{
				StatusCode: fiber.StatusBadRequest,
				Message:    fmt.Sprintf("column name %q is kept for internal use; rename the column", name),
			}
		}
		seen[name] = struct{}{}
		if name == timeColumn {
			timeIdx = i
		} else if ierr := importColumnNameError(name, folds, stored); ierr != nil {
			return -1, ierr
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

// importColumnNameError refuses a column Arc could not read back (#1342):
// not valid UTF-8, "time" in another letter case, or one differing only in
// letter case from another column of the file (folds) or of the measurement
// (stored). The file's time column is exempt: it is renamed "time".
func importColumnNameError(name string, folds, stored map[string]string) *importError {
	if p := ingest.ColumnNameProblem(name); p != "" && name != "time" {
		return &importError{
			StatusCode: fiber.StatusBadRequest,
			Message:    fmt.Sprintf("column name %q cannot be stored: %s; rename the column", ingest.TruncateColumnName(name), p),
		}
	}
	if other, twin := ingest.ColumnCaseTwin(name, folds, stored); twin {
		return &importError{
			StatusCode: fiber.StatusBadRequest,
			Message:    fmt.Sprintf("column name %q differs only in letter case from %q of this measurement; rename one of them", ingest.TruncateColumnName(name), ingest.TruncateColumnName(other)),
		}
	}
	folds[ingest.FoldColumnName(name)] = name
	return nil
}

// csvColumnKind is what a CSV column's cells allow: the narrowest native type
// that fits every cell is int64, then float64, then bool, else string. An
// import observes every cell in its validation pass and converts each chunk
// to the type the whole column allows, so a chunk types its cells as the
// whole file would.
type csvColumnKind struct {
	isInt, isFloat, isBool bool
	hasValue               bool
}

func newCSVColumnKind() csvColumnKind { return csvColumnKind{isInt: true, isFloat: true, isBool: true} }

// observe narrows the kind by one cell. Empty cells do not constrain it.
func (k *csvColumnKind) observe(s string) {
	if s == "" {
		return
	}
	k.hasValue = true
	if !k.isInt && !k.isFloat && !k.isBool {
		return // a string column: nothing left to rule out
	}
	if k.isInt {
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			k.isInt = false
		}
	}
	// Any valid base-10 integer is also a valid float, so only parse as
	// float once a value has disqualified the integer type.
	if !k.isInt && k.isFloat {
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			k.isFloat = false
		}
	}
	if k.isBool && !isBoolLiteral(s) {
		k.isBool = false
	}
}

func (k csvColumnKind) isString() bool { return !k.hasValue || (!k.isInt && !k.isFloat && !k.isBool) }

// convertCSVColumn converts cells to kind k: a flat typed slice
// ([]int64/[]float64/[]bool/[]string) and a null-validity bitmap (nil when no
// cell is empty; validity[i]=false marks an empty numeric/bool cell as null).
// String columns keep empty cells as "" and have no nulls. Every non-empty
// cell must fit k (the kind was observed over them).
//
// Returning typed slices directly (rather than a boxed []interface{}) lets the
// caller use WriteTypedColumnarDirect, avoiding per-value boxing — same reason
// the Parquet path uses arrowColumnToTyped.
func convertCSVColumn(raw []string, k csvColumnKind) (interface{}, []bool) {
	if k.isString() {
		out := make([]string, len(raw))
		copy(out, raw)
		return out, nil
	}
	var validity []bool
	for i, s := range raw {
		if s == "" {
			if validity == nil {
				validity = make([]bool, len(raw))
				for j := 0; j < i; j++ {
					validity[j] = true
				}
			}
			continue
		}
		if validity != nil {
			validity[i] = true
		}
	}
	// Order matters: the flags are NOT mutually exclusive (a pure-int column has
	// both isInt and isFloat true, and a 0/1 column has isInt and isBool true).
	// Narrowest-type precedence is encoded by this case ordering — keep int first,
	// then float, then bool. Do not reorder.
	switch {
	case k.isInt:
		out := make([]int64, len(raw))
		for i, s := range raw {
			if s != "" {
				out[i], _ = strconv.ParseInt(s, 10, 64)
			}
		}
		return out, validity
	case k.isFloat:
		out := make([]float64, len(raw))
		for i, s := range raw {
			if s != "" {
				out[i], _ = strconv.ParseFloat(s, 64)
			}
		}
		return out, validity
	default: // isBool — no numeric parsing needed
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

// stringsToTimeMicrosFrom converts a string time column to []int64
// microseconds. Explicit epoch_* formats are deterministic; "" (auto)
// magnitude-detects numeric values and falls back to parsing common timestamp
// string layouts. In the auto string case the matched layout is cached and
// tried first on subsequent rows — CSV time columns are homogeneous, so this
// avoids re-scanning all layouts per row. The rows start at row firstRow+1 of
// the file: an import converts its time column chunk by chunk, and an error
// names the row in the file.
func stringsToTimeMicrosFrom(raw []string, timeFormat string, firstRow int) ([]int64, error) {
	out := make([]int64, len(raw))
	cachedLayout := ""
	for i, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("empty value in time column at row %d", firstRow+i+1)
		}

		// Explicit epoch format.
		if timeFormat != "" {
			micros, err := oneTimeValueToMicros(s, timeFormat)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", firstRow+i+1, err)
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
			return nil, fmt.Errorf("row %d: %w", firstRow+i+1, err)
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

// importBufferStatus maps the ArrowBuffer's two retryable states to 503 so an
// import that raced a shutdown, or hit sustained schema churn, is not reported
// as a permanent server error.
func importBufferStatus(err error) int {
	if errors.Is(err, ingest.ErrBufferClosing) || errors.Is(err, ingest.ErrSchemaChurnExceeded) || ingest.IsWriteRefusal(err) {
		return fiber.StatusServiceUnavailable
	}
	return fiber.StatusInternalServerError
}

// knownFolds is the measurement's stored column spellings (#1342), or nil.
func (h *ImportHandler) knownFolds(database, measurement string) map[string]string {
	if known := h.arrowBuffer.KnownColumns(database); known != nil {
		return known(measurement)
	}
	return nil
}
