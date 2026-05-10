//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
)

func init() {
	arrowJSONQueryFunc = executeArrowJSONQuery
}

// executeArrowJSONQuery executes a query via DuckDB's native Arrow API and streams
// the JSON response. Returns (rowCount, handled). If handled is true, the response
// has been written (either success or error). If false, the caller should fall back
// to the database/sql path.
//
// Note: When successful, metrics are recorded inside the async stream writer callback
// (not by the caller) because SetBodyStreamWriter runs after the handler returns.
func executeArrowJSONQuery(
	h *QueryHandler,
	c *fiber.Ctx,
	ctx context.Context,
	cancel context.CancelFunc,
	convertedSQL string,
	profileMode bool,
	governanceMaxRows int,
	start time.Time,
	timestamp string,
	onComplete func(int),
	onFail func(string),
	onTimeout func(),
) (int, bool) {
	m := metrics.Get()

	var reader array.RecordReader
	var conn interface{ Close() error }
	var profile *database.QueryProfile
	var err error

	if profileMode {
		var sqlConn interface{ Close() error }
		reader, sqlConn, profile, err = h.db.ArrowQueryWithProfileContext(ctx, convertedSQL)
		conn = sqlConn
	} else {
		var sqlConn interface{ Close() error }
		reader, sqlConn, err = h.db.ArrowQueryContext(ctx, convertedSQL)
		conn = sqlConn
	}

	if err != nil {
		// Check for "no files found" — return empty result, not error
		if isNoFilesFoundError(err) {
			if cancel != nil {
				cancel()
			}
			if onComplete != nil {
				onComplete(0)
			}
			c.JSON(QueryResponse{
				Success:         true,
				Columns:         []string{},
				Data:            [][]interface{}{},
				RowCount:        0,
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
			return 0, true
		}

		// Check for timeout
		if ctx.Err() == context.DeadlineExceeded {
			if cancel != nil {
				cancel()
			}
			m.IncQueryErrors()
			m.IncQueryTimeouts()
			if onTimeout != nil {
				onTimeout()
			}
			c.Status(fiber.StatusGatewayTimeout).JSON(QueryResponse{
				Success:         false,
				Error:           "Query timed out",
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
				Timestamp:       timestamp,
			})
			return -1, true
		}

		// For connection-level issues (driver doesn't support Arrow),
		// fall back to database/sql path.
		errStr := err.Error()
		if strings.Contains(errStr, "does not implement driver.Conn") ||
			strings.Contains(errStr, "failed to create Arrow interface") {
			return 0, false
		}

		// Query-level error — report directly
		if cancel != nil {
			cancel()
		}
		m.IncQueryErrors()
		if onFail != nil {
			onFail(err.Error())
		}
		h.logger.Error().Err(err).Str("sql", convertedSQL).Msg("Arrow JSON query failed")
		c.Status(fiber.StatusInternalServerError).JSON(QueryResponse{
			Success:         false,
			Error:           err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       timestamp,
		})
		return -1, true
	}

	// Capture token name before async callback (Fiber context not safe in callbacks)
	tokenName := getTokenName(c)

	// Stream Arrow JSON response.
	// SetBodyStreamWriter runs asynchronously — metrics are recorded in the callback.
	streamCtx := ctx
	c.Set("Content-Type", "application/json")
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		rc, streamErr := streamArrowJSON(streamCtx, w, reader, governanceMaxRows, profile, start, timestamp)
		w.Flush()

		reader.Release()
		conn.Close()
		if cancel != nil {
			cancel()
		}

		if streamErr != nil {
			m.IncQueryErrors()
			if errors.Is(streamErr, context.DeadlineExceeded) {
				m.IncQueryTimeouts()
				if onTimeout != nil {
					onTimeout()
				}
			} else if onFail != nil {
				onFail(streamErr.Error())
			}
			// Warn (not Error): the response headers were already committed
			// when this fired, so by definition the client got a partial
			// result. The common cause is the client disconnecting mid-
			// stream (Grafana panel close, browser tab kill). That's
			// expected ops noise, not an Arc fault.
			h.logger.Warn().Err(streamErr).
				Int("rows_sent", rc).
				Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
				Msg("Arrow JSON stream truncated after headers committed; client received partial result")
			return
		}

		m.IncQuerySuccess()
		m.IncQueryRows(int64(rc))
		m.RecordQueryLatency(time.Since(start).Microseconds())

		if onComplete != nil {
			onComplete(rc)
		}

		h.logger.Info().
			Int("row_count", rc).
			Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
			Msg("Arrow JSON query completed")
		h.logSlowQuery(convertedSQL, start, rc, tokenName)
	})

	// Return 0 — actual row count is only known after async streaming completes.
	// Metrics are recorded in the callback above.
	return 0, true
}

// streamArrowJSON writes a complete JSON query response from Arrow record
// batches directly to a bufio.Writer. Values are read from typed Arrow column
// arrays — no interface{} boxing, no Scan overhead.
//
// Returns (rowsWritten, err). Errors that may occur after the response
// envelope has been opened cannot retroactively change the HTTP status —
// but the caller MUST surface them so operators see the partial-result
// signal instead of recording Complete(rowCount). See review/query-path-
// criticals C5.
//
// Returned error sources:
//   - ctx cancellation observed at a batch boundary (timeout / client-
//     disconnect mid-stream).
//   - reader.Err() after the batch loop exits — Arrow IPC reader
//     surfaces deferred read failures here.
func streamArrowJSON(
	ctx context.Context,
	w *bufio.Writer,
	reader array.RecordReader,
	governanceMaxRows int,
	profile *database.QueryProfile,
	start time.Time,
	timestamp string,
) (int, error) {
	schema := reader.Schema()
	fields := schema.Fields()
	numCols := len(fields)

	columns := make([]string, numCols)
	for i, f := range fields {
		columns[i] = f.Name
	}

	scratch := make([]byte, 0, 128)

	// --- Write envelope open ---
	w.WriteString(`{"success":true,"columns":`)
	writeJSONStringArray(w, columns)
	w.WriteString(`,"data":[`)

	rowCount := 0
	var streamErr error

batchLoop:
	for reader.Next() {
		// Per-batch ctx check: a timeout firing or client disconnect
		// must short-circuit instead of draining DuckDB into a buffer
		// nobody reads. Non-blocking select with labeled break is the
		// idiomatic Go cancellation pattern (gemini r1).
		select {
		case <-ctx.Done():
			streamErr = fmt.Errorf("stream cancelled at row %d: %w", rowCount, ctx.Err())
			break batchLoop
		default:
		}

		batch := reader.Record()
		if batch == nil {
			break
		}

		nRows := int(batch.NumRows())
		cols := make([]arrow.Array, numCols)
		for c := 0; c < numCols; c++ {
			cols[c] = batch.Column(c)
		}

		for row := 0; row < nRows; row++ {
			if governanceMaxRows > 0 && rowCount >= governanceMaxRows {
				goto done
			}

			if rowCount > 0 {
				w.WriteByte(',')
			}

			w.WriteByte('[')
			for col := 0; col < numCols; col++ {
				if col > 0 {
					w.WriteByte(',')
				}
				scratch = writeArrowValue(w, scratch, cols[col], row)
			}
			w.WriteByte(']')

			rowCount++

			if rowCount%jsonFlushInterval == 0 {
				// Capture Flush error: fasthttp's RequestCtx.Done() only fires
				// on server shutdown (not per-request client disconnect), so
				// the bufio.Writer's error on the closed connection is our
				// signal that the client has gone away. Breaking out of both
				// nested loops stops DuckDB from draining the rest of the
				// result set into a buffer nobody reads.
				if err := w.Flush(); err != nil {
					streamErr = fmt.Errorf("stream flush failed at row %d (client likely disconnected): %w", rowCount, err)
					break batchLoop
				}
			}
		}
	}

	if streamErr == nil {
		if err := reader.Err(); err != nil {
			streamErr = fmt.Errorf("arrow reader error after %d rows: %w", rowCount, err)
		}
	}

done:
	// --- Write envelope close ---
	// Emit a valid JSON document regardless of whether streamErr is set
	// (headers are already committed). Caller logs/metrics the error.
	w.WriteString(`],"row_count":`)
	scratch = strconv.AppendInt(scratch[:0], int64(rowCount), 10)
	w.Write(scratch)

	executionTime := float64(time.Since(start).Milliseconds())
	w.WriteString(`,"execution_time_ms":`)
	scratch = strconv.AppendFloat(scratch[:0], executionTime, 'f', -1, 64)
	w.Write(scratch)

	w.WriteString(`,"timestamp":`)
	writeJSONString(w, scratch, timestamp)

	if profile != nil {
		w.WriteString(`,"profile":`)
		if pb, err := json.Marshal(profile); err == nil {
			w.Write(pb)
		} else {
			w.WriteString("null")
		}
	}

	w.WriteByte('}')

	return rowCount, streamErr
}

// writeArrowValue writes a single value from a typed Arrow column array.
// No interface{} boxing — values are read directly from the array's typed accessor.
func writeArrowValue(w *bufio.Writer, scratch []byte, col arrow.Array, row int) []byte {
	if col.IsNull(row) {
		w.WriteString("null")
		return scratch
	}

	switch c := col.(type) {
	case *array.Int64:
		scratch = strconv.AppendInt(scratch[:0], c.Value(row), 10)
		w.Write(scratch)
	case *array.Int32:
		scratch = strconv.AppendInt(scratch[:0], int64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Int16:
		scratch = strconv.AppendInt(scratch[:0], int64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Int8:
		scratch = strconv.AppendInt(scratch[:0], int64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Uint64:
		scratch = strconv.AppendUint(scratch[:0], c.Value(row), 10)
		w.Write(scratch)
	case *array.Uint32:
		scratch = strconv.AppendUint(scratch[:0], uint64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Uint16:
		scratch = strconv.AppendUint(scratch[:0], uint64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Uint8:
		scratch = strconv.AppendUint(scratch[:0], uint64(c.Value(row)), 10)
		w.Write(scratch)
	case *array.Float64:
		v := c.Value(row)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			w.WriteString("null")
		} else {
			scratch = strconv.AppendFloat(scratch[:0], v, 'f', -1, 64)
			w.Write(scratch)
		}
	case *array.Float32:
		v := float64(c.Value(row))
		if math.IsNaN(v) || math.IsInf(v, 0) {
			w.WriteString("null")
		} else {
			scratch = strconv.AppendFloat(scratch[:0], v, 'f', -1, 64)
			w.Write(scratch)
		}
	case *array.Boolean:
		if c.Value(row) {
			w.WriteString("true")
		} else {
			w.WriteString("false")
		}
	case *array.Timestamp:
		ts := c.Value(row)
		unit := c.DataType().(*arrow.TimestampType).Unit
		t := ts.ToTime(unit)
		w.WriteByte('"')
		scratch = t.UTC().AppendFormat(scratch[:0], time.RFC3339Nano)
		w.Write(scratch)
		w.WriteByte('"')
	case *array.Date32:
		d := c.Value(row)
		t := d.ToTime()
		w.WriteByte('"')
		scratch = t.UTC().AppendFormat(scratch[:0], time.RFC3339Nano)
		w.Write(scratch)
		w.WriteByte('"')
	case *array.String:
		writeJSONString(w, scratch, c.Value(row))
	case *array.LargeString:
		writeJSONString(w, scratch, c.Value(row))
	case *array.Binary:
		writeJSONString(w, scratch, string(c.Value(row)))
	default:
		// Fallback: use ValueStr for any unhandled Arrow types
		writeJSONString(w, scratch, col.ValueStr(row))
	}

	return scratch
}
