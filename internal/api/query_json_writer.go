package api

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/database"
)

// colType represents a simplified column type for fast JSON serialization.
type colType int

const (
	colString colType = iota
	colInt64
	colFloat64
	colBool
	colTimestamp
)

// jsonFlushInterval controls how often we flush the bufio.Writer to the
// HTTP response during streaming. Flushing every N rows keeps memory bounded
// while avoiding per-row flush overhead. The Flush also doubles as the
// client-disconnect detection point (see errClientDisconnected) — at 1000
// rows the worst-case wasted formatting work after a disconnect is ~1000
// rows of CPU, which is acceptable for typical query rates (~10-100µs).
// Per-row Flush would be cleaner for fast disconnect detection but doubles
// as forced syscalls on a healthy connection; 1000 is the compromise.
const jsonFlushInterval = 1000

// rowScanner is the interface satisfied by both *sql.Rows and *MergedRowIterator.
type rowScanner interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

// mapColumnTypes converts sql.ColumnType slices to our fast-path enum.
func mapColumnTypes(cts []*sql.ColumnType) []colType {
	out := make([]colType, len(cts))
	for i, ct := range cts {
		out[i] = mapColumnType(ct.DatabaseTypeName())
	}
	return out
}

// mapColumnType converts a DuckDB type name to our serialization enum.
// Mirrors sqlTypeToArrowType in query_arrow.go.
func mapColumnType(dbType string) colType {
	dbType = strings.ToUpper(dbType)
	switch {
	case strings.Contains(dbType, "BOOL"):
		return colBool
	case strings.Contains(dbType, "TIMESTAMP"), strings.Contains(dbType, "DATETIME"):
		return colTimestamp
	case strings.Contains(dbType, "HUGEINT"), strings.Contains(dbType, "UBIGINT"):
		// 128-bit integers don't fit in int64 — serialize as strings
		return colString
	case strings.Contains(dbType, "INT64"), strings.Contains(dbType, "BIGINT"):
		return colInt64
	case strings.Contains(dbType, "INT"):
		// INT, INTEGER, INT32, SMALLINT, TINYINT, etc.
		return colInt64
	case strings.Contains(dbType, "FLOAT"), strings.Contains(dbType, "DOUBLE"),
		strings.Contains(dbType, "REAL"), strings.Contains(dbType, "DECIMAL"),
		strings.Contains(dbType, "NUMERIC"):
		return colFloat64
	case strings.Contains(dbType, "DATE"):
		// DATE (without TIMESTAMP prefix) — format as string
		return colTimestamp
	default:
		return colString
	}
}

// streamTypedJSON writes a complete JSON query response directly to a
// bufio.Writer (typically the HTTP response), avoiding any full-response
// buffering. Rows are flushed incrementally every jsonFlushInterval rows.
//
// Returns (rowsWritten, err). Errors that may occur after the response
// envelope has been opened (and possibly partially flushed) cannot
// retroactively change the HTTP status — but the caller MUST surface
// them to logs / metrics / query-registry so operators see that the
// stream truncated, instead of the previous behaviour of silently
// `continue`'ing past Scan errors and reporting Complete(rowCount).
// See review/query-path-criticals C5.
//
// Returned error sources:
//   - ctx cancellation observed at a row boundary (timeout / client-
//     disconnect). Surfaces ctx.Err().
//   - scanner.Scan failure on any row. The row is NOT written; the
//     stream stops. Operator must treat this as a partial-result failure.
//   - scanner.Err() after the row loop exits cleanly (e.g. underlying
//     *sql.Rows ran into a deferred error). Same partial-result class.
func streamTypedJSON(
	ctx context.Context,
	w *bufio.Writer,
	columns []string,
	colTypes []colType,
	scanner rowScanner,
	governanceMaxRows int,
	profile *database.QueryProfile,
	start time.Time,
	timestamp string,
) (int, error) {
	numCols := len(columns)

	// Scan buffer — reused for every row
	values := make([]interface{}, numCols)
	valuePtrs := make([]interface{}, numCols)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Scratch buffer for strconv.Append* and time formatting
	scratch := make([]byte, 0, 128)

	// --- Write envelope open ---
	w.WriteString(`{"success":true,"columns":`)
	writeJSONStringArray(w, columns)
	w.WriteString(`,"data":[`)

	rowCount := 0
	var streamErr error

scanLoop:
	for scanner.Next() {
		// Per-row ctx check: a timeout firing mid-stream must short-
		// circuit instead of being silently absorbed by the next
		// Scan/Err round (which the prior code did via `continue`).
		// Non-blocking select with labeled break is the idiomatic Go
		// cancellation pattern (gemini r1).
		select {
		case <-ctx.Done():
			streamErr = fmt.Errorf("stream cancelled at row %d: %w", rowCount, ctx.Err())
			break scanLoop
		default:
		}

		if err := scanner.Scan(valuePtrs...); err != nil {
			// SCAN error on this row. We refuse to silently drop rows
			// (companion to #330) — surface to caller so the request
			// is recorded as a failure.
			streamErr = fmt.Errorf("scan failed at row %d: %w", rowCount, err)
			break
		}

		// Row separator
		if rowCount > 0 {
			w.WriteByte(',')
		}

		// Write row array
		w.WriteByte('[')
		for col := 0; col < numCols; col++ {
			if col > 0 {
				w.WriteByte(',')
			}
			scratch = writeJSONValue(w, scratch, values[col], colTypes[col])
		}
		w.WriteByte(']')

		rowCount++

		// Flush periodically to keep memory bounded. fasthttp's RequestCtx.Done
		// only fires on server shutdown, so the bufio.Writer error on the closed
		// connection is our only signal that the client has disconnected
		// mid-stream. Break out so the underlying *sql.Rows isn't drained into
		// a buffer nobody reads. Wrap with errClientDisconnected so the caller
		// can log at Warn (not Error) for this expected ops noise.
		if rowCount%jsonFlushInterval == 0 {
			if err := w.Flush(); err != nil {
				streamErr = fmt.Errorf("stream flush failed at row %d: %w: %w", rowCount, errClientDisconnected, err)
				break scanLoop
			}
		}

		if governanceMaxRows > 0 && rowCount >= governanceMaxRows {
			break
		}
	}

	// Check for deferred iterator errors (e.g. row-fetch failure
	// from the underlying *sql.Rows after the loop terminates).
	if streamErr == nil {
		if err := scanner.Err(); err != nil {
			streamErr = fmt.Errorf("iterator error after %d rows: %w", rowCount, err)
		}
	}

	// --- Write envelope close ---
	// We close the JSON document regardless of whether streamErr is set
	// — the response headers are already committed so we cannot change
	// the status code, and emitting valid JSON is friendlier to clients
	// that may have already started parsing. The caller logs/metrics
	// the error.
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
		// Profile is a small struct — json.Marshal is fine here
		if pb, err := json.Marshal(profile); err == nil {
			w.Write(pb)
		} else {
			w.WriteString("null")
		}
	}

	w.WriteByte('}')

	return rowCount, streamErr
}

// writeJSONValue writes a single value using type-specific serialization.
// Returns the scratch buffer (may have been resliced).
func writeJSONValue(w *bufio.Writer, scratch []byte, val interface{}, ct colType) []byte {
	if val == nil {
		w.WriteString("null")
		return scratch
	}

	switch ct {
	case colInt64:
		switch v := val.(type) {
		case int64:
			scratch = strconv.AppendInt(scratch[:0], v, 10)
			w.Write(scratch)
		case int32:
			scratch = strconv.AppendInt(scratch[:0], int64(v), 10)
			w.Write(scratch)
		case int:
			scratch = strconv.AppendInt(scratch[:0], int64(v), 10)
			w.Write(scratch)
		case uint64:
			scratch = strconv.AppendUint(scratch[:0], v, 10)
			w.Write(scratch)
		case uint32:
			scratch = strconv.AppendUint(scratch[:0], uint64(v), 10)
			w.Write(scratch)
		case float64:
			// DuckDB sometimes returns float64 for integer columns
			if v == math.Trunc(v) && !math.IsInf(v, 0) && !math.IsNaN(v) {
				scratch = strconv.AppendInt(scratch[:0], int64(v), 10)
				w.Write(scratch)
			} else {
				scratch = writeFloat64(w, scratch, v)
			}
		case sql.NullInt64:
			if v.Valid {
				scratch = strconv.AppendInt(scratch[:0], v.Int64, 10)
				w.Write(scratch)
			} else {
				w.WriteString("null")
			}
		default:
			scratch = writeFallbackJSON(w, scratch, v)
		}

	case colFloat64:
		switch v := val.(type) {
		case float64:
			scratch = writeFloat64(w, scratch, v)
		case float32:
			scratch = writeFloat64(w, scratch, float64(v))
		case int64:
			scratch = strconv.AppendInt(scratch[:0], v, 10)
			w.Write(scratch)
		case sql.NullFloat64:
			if v.Valid {
				scratch = writeFloat64(w, scratch, v.Float64)
			} else {
				w.WriteString("null")
			}
		default:
			scratch = writeFallbackJSON(w, scratch, v)
		}

	case colBool:
		switch v := val.(type) {
		case bool:
			if v {
				w.WriteString("true")
			} else {
				w.WriteString("false")
			}
		case sql.NullBool:
			if v.Valid {
				if v.Bool {
					w.WriteString("true")
				} else {
					w.WriteString("false")
				}
			} else {
				w.WriteString("null")
			}
		default:
			scratch = writeFallbackJSON(w, scratch, v)
		}

	case colTimestamp:
		switch v := val.(type) {
		case time.Time:
			w.WriteByte('"')
			scratch = v.UTC().AppendFormat(scratch[:0], time.RFC3339Nano)
			w.Write(scratch)
			w.WriteByte('"')
		case string:
			writeJSONString(w, scratch, v)
		case sql.NullString:
			if v.Valid {
				writeJSONString(w, scratch, v.String)
			} else {
				w.WriteString("null")
			}
		default:
			scratch = writeFallbackJSON(w, scratch, v)
		}

	default: // colString
		switch v := val.(type) {
		case string:
			writeJSONString(w, scratch, v)
		case []byte:
			writeJSONString(w, scratch, string(v))
		case sql.NullString:
			if v.Valid {
				writeJSONString(w, scratch, v.String)
			} else {
				w.WriteString("null")
			}
		case time.Time:
			w.WriteByte('"')
			scratch = v.UTC().AppendFormat(scratch[:0], time.RFC3339Nano)
			w.Write(scratch)
			w.WriteByte('"')
		case int64:
			scratch = strconv.AppendInt(scratch[:0], v, 10)
			w.Write(scratch)
		case float64:
			scratch = writeFloat64(w, scratch, v)
		case bool:
			if v {
				w.WriteString("true")
			} else {
				w.WriteString("false")
			}
		default:
			scratch = writeFallbackJSON(w, scratch, v)
		}
	}

	return scratch
}

// writeFloat64 writes a float64, handling NaN/Inf as null.
func writeFloat64(w *bufio.Writer, scratch []byte, v float64) []byte {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		w.WriteString("null")
		return scratch
	}
	scratch = strconv.AppendFloat(scratch[:0], v, 'f', -1, 64)
	w.Write(scratch)
	return scratch
}

// writeFallbackJSON marshals an unknown value using json.Marshal.
func writeFallbackJSON(w *bufio.Writer, scratch []byte, v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		w.WriteString("null")
	} else {
		w.Write(b)
	}
	return scratch
}

// writeJSONStringArray writes a JSON array of strings: ["a","b","c"]
func writeJSONStringArray(w *bufio.Writer, ss []string) {
	scratch := make([]byte, 0, 64)
	w.WriteByte('[')
	for i, s := range ss {
		if i > 0 {
			w.WriteByte(',')
		}
		writeJSONString(w, scratch, s)
	}
	w.WriteByte(']')
}

// writeJSONString writes a JSON-escaped string.
// Zero-allocation for strings that don't need escaping (the common case).
func writeJSONString(w *bufio.Writer, scratch []byte, s string) {
	w.WriteByte('"')

	// Fast path: scan for characters that need escaping
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' || c < 0x20 {
			// Write clean segment before this character
			if start < i {
				w.WriteString(s[start:i])
			}
			// Write escape sequence
			switch c {
			case '"':
				w.WriteString(`\"`)
			case '\\':
				w.WriteString(`\\`)
			case '\n':
				w.WriteString(`\n`)
			case '\r':
				w.WriteString(`\r`)
			case '\t':
				w.WriteString(`\t`)
			case '\b':
				w.WriteString(`\b`)
			case '\f':
				w.WriteString(`\f`)
			default:
				// Control character: \u00XX
				w.WriteString(`\u00`)
				w.WriteByte(hexDigit(c >> 4))
				w.WriteByte(hexDigit(c & 0x0f))
			}
			start = i + 1
		}
	}
	// Write remaining clean segment
	if start < len(s) {
		w.WriteString(s[start:])
	}

	w.WriteByte('"')
	_ = scratch // suppress unused warning — caller manages scratch lifetime
}

func hexDigit(b byte) byte {
	if b < 10 {
		return '0' + b
	}
	return 'a' + b - 10
}
