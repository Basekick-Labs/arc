package api

import (
	"bufio"
	"database/sql"
	"encoding/json"
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
// while avoiding per-row flush overhead.
const jsonFlushInterval = 5000

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
	case strings.Contains(dbType, "INT64"), strings.Contains(dbType, "BIGINT"),
		strings.Contains(dbType, "HUGEINT"), strings.Contains(dbType, "UBIGINT"):
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
// Returns the number of rows written.
func streamTypedJSON(
	w *bufio.Writer,
	columns []string,
	colTypes []colType,
	scanner rowScanner,
	governanceMaxRows int,
	profile *database.QueryProfile,
	start time.Time,
	timestamp string,
) int {
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

	for scanner.Next() {
		if err := scanner.Scan(valuePtrs...); err != nil {
			continue
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

		// Flush periodically to keep memory bounded
		if rowCount%jsonFlushInterval == 0 {
			w.Flush()
		}

		if governanceMaxRows > 0 && rowCount >= governanceMaxRows {
			break
		}
	}

	// --- Write envelope close ---
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

	return rowCount
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
