package api

import (
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/gofiber/fiber/v2"
)

// wireFormatLocalsKey is the c.Locals key the msgpack endpoint sets to
// route response serialization through the msgpack encoder. Read at every
// response site that previously emitted c.JSON unconditionally; the
// default (key absent) keeps JSON behavior unchanged.
const wireFormatLocalsKey = "wire_format"

// wireFormatMsgPack is the value stored on c.Locals(wireFormatLocalsKey)
// by the /api/v1/query/msgpack wrapper handler.
const wireFormatMsgPack = "msgpack"

// isMsgPackWire reports whether the current request asked for a
// MessagePack response. Safe to call from anywhere a *fiber.Ctx is in
// scope; returns false if the Locals key was never set (the default
// JSON path).
func isMsgPackWire(c *fiber.Ctx) bool {
	v, ok := c.Locals(wireFormatLocalsKey).(string)
	return ok && v == wireFormatMsgPack
}

// elapsedMsUint returns the milliseconds elapsed since start as a
// uint64, clamped to 0 below. time.Since uses the monotonic clock so
// the value cannot actually go negative on Go ≥ 1.9, but the cast to
// uint64 of any negative int64 produces a very large wire value — the
// max(0, ...) is a belt-and-suspenders guard that costs nothing.
func elapsedMsUint(start time.Time) uint64 {
	ms := time.Since(start).Milliseconds()
	if ms < 0 {
		return 0
	}
	return uint64(ms)
}

// respondError emits a structured error response in the wire format the
// caller requested. Used in place of c.Status(status).JSON(QueryResponse{
// Success:false, Error:msg, Timestamp:ts}) at every error site in the
// JSON-and-msgpack-shared executeQuery pipeline. Keeps the contract
// uniform: a msgpack client gets msgpack on errors, not JSON.
func respondError(c *fiber.Ctx, status int, errMsg, timestamp string, start time.Time) error {
	if !isMsgPackWire(c) {
		return c.Status(status).JSON(QueryResponse{
			Success:         false,
			Error:           errMsg,
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       timestamp,
		})
	}
	writeMsgPackError(c, errMsg, start, timestamp)
	return c.SendStatus(status)
}

// respondSuccessRows emits a SHOW-style success response in the wire
// format the caller requested. Used by handleShowDatabases /
// handleShowTables where the caller already has the column names and
// the row-major data slice in hand. For msgpack the data slice is
// transposed to the columnar wire format on the fly; for JSON it
// passes through unchanged.
//
// types is parallel to columns and carries the wire-level type name
// per column (matching the streaming hot path's arrow.DataType.String()
// output for the comparable Arrow type). Passed explicitly by the
// caller rather than inferred from cell content, because:
//
//  1. SHOW handlers know the schema by construction — the row-major
//     []interface{} representation is for JSON convenience, not type
//     ground-truth.
//  2. First-non-nil-cell inference is fragile when leading rows are
//     nil for a column that later carries data.
//
// types may be nil, in which case the JSON path doesn't emit a types
// field (the JSON envelope shape never had one) and the msgpack path
// falls back to empty strings — a deliberate signal to the client that
// the schema is unknown.
func respondSuccessRows(c *fiber.Ctx, columns []string, types []string, data [][]interface{}, timestamp string, start time.Time) error {
	if !isMsgPackWire(c) {
		return c.JSON(QueryResponse{
			Success:         true,
			Columns:         columns,
			Data:            data,
			RowCount:        len(data),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       timestamp,
		})
	}
	writeMsgPackRowsColumnar(c, columns, types, data, start, timestamp)
	return nil
}

// respondEmptySuccess emits a no-rows success envelope in the wire format
// the caller requested. Used for "no files found yet" results that the
// JSON path historically returned via c.JSON(QueryResponse{Success:true,
// Columns:[], Data:[], ...}).
func respondEmptySuccess(c *fiber.Ctx, timestamp string, start time.Time) error {
	if !isMsgPackWire(c) {
		return c.JSON(QueryResponse{
			Success:         true,
			Columns:         []string{},
			Data:            [][]interface{}{},
			RowCount:        0,
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			Timestamp:       timestamp,
		})
	}
	writeMsgPackEmpty(c, start, timestamp)
	return nil
}

// writeMsgPackRowsColumnar writes a SHOW-style success envelope as a
// columnar msgpack response. Transposes the row-major data slice into
// per-column arrays so the wire shape matches the streaming hot path:
// `data` is array-of-columns, with a parallel `types` array (inferred
// from the first non-nil cell in each column). Bounded payload — SHOW
// results are small (databases/measurements) — so direct in-memory
// encode without streaming.
func writeMsgPackRowsColumnar(c *fiber.Ctx, columns []string, types []string, data [][]interface{}, start time.Time, timestamp string) {
	c.Set(fiber.HeaderContentType, msgpackContentType)
	enc := msgpack.GetEncoder()
	enc.Reset(c.Response().BodyWriter())
	enc.SetCustomStructTag("json")
	defer func() {
		enc.Reset(nil)
		msgpack.PutEncoder(enc)
	}()

	numCols := len(columns)
	numRows := len(data)

	_ = enc.EncodeMapLen(7)

	_ = enc.EncodeString("success")
	_ = enc.EncodeBool(true)

	_ = enc.EncodeString("columns")
	_ = enc.EncodeArrayLen(numCols)
	for _, col := range columns {
		_ = enc.EncodeString(col)
	}

	// "types": parallel to columns. Caller passes them explicitly
	// (SHOW handlers know the schema by construction). When nil or
	// short, missing entries fall back to "" so the array length
	// invariant (len(types) == len(columns)) holds on the wire.
	_ = enc.EncodeString("types")
	_ = enc.EncodeArrayLen(numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		var typeName string
		if colIdx < len(types) {
			typeName = types[colIdx]
		}
		_ = enc.EncodeString(typeName)
	}

	// "data": [[col0_values...], [col1_values...], ...]
	_ = enc.EncodeString("data")
	_ = enc.EncodeArrayLen(numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		_ = enc.EncodeArrayLen(numRows)
		for _, row := range data {
			var v interface{}
			if colIdx < len(row) {
				v = row[colIdx]
			}
			if v == nil {
				_ = enc.EncodeNil()
				continue
			}
			// int64 / uint64 columns use EncodeInt64 / EncodeUint64 (always
			// 9 bytes on the wire) for consistency with the streaming hot
			// path in query_msgpack.go. The narrower int/int32/uint/uint32
			// types keep EncodeInt / EncodeUint so small values benefit
			// from fixint compaction (1 byte for 0-127) — they're outside
			// the "predictable 9-byte int64" guarantee clients rely on for
			// the typed-by-column wire shape.
			switch x := v.(type) {
			case bool:
				_ = enc.EncodeBool(x)
			case int:
				_ = enc.EncodeInt(int64(x))
			case int32:
				_ = enc.EncodeInt(int64(x))
			case int64:
				_ = enc.EncodeInt64(x)
			case uint:
				_ = enc.EncodeUint(uint64(x))
			case uint32:
				_ = enc.EncodeUint(uint64(x))
			case uint64:
				_ = enc.EncodeUint64(x)
			case float32:
				_ = enc.EncodeFloat32(x)
			case float64:
				_ = enc.EncodeFloat64(x)
			case string:
				_ = enc.EncodeString(x)
			case []byte:
				_ = enc.EncodeBytes(x)
			default:
				_ = enc.Encode(v)
			}
		}
	}

	_ = enc.EncodeString("row_count")
	_ = enc.EncodeUint(uint64(numRows))

	_ = enc.EncodeString("execution_time_ms")
	_ = enc.EncodeUint(elapsedMsUint(start))

	_ = enc.EncodeString("timestamp")
	_ = enc.EncodeString(timestamp)
}

// writeMsgPackEmpty writes an empty-success envelope in msgpack format
// directly to the response body. Bounded, fixed-shape payload; errors on
// the encoder are elided because the writer is an in-memory bodywriter
// — write failure is essentially impossible.
func writeMsgPackEmpty(c *fiber.Ctx, start time.Time, timestamp string) {
	c.Set(fiber.HeaderContentType, msgpackContentType)
	enc := msgpack.GetEncoder()
	enc.Reset(c.Response().BodyWriter())
	defer func() {
		enc.Reset(nil)
		msgpack.PutEncoder(enc)
	}()
	// 7-field envelope matches the columnar streaming shape, including
	// the empty types[] array so client decoders that branch on the
	// presence of "types" don't need a special case for empty results.
	_ = enc.EncodeMapLen(7)
	_ = enc.EncodeString("success")
	_ = enc.EncodeBool(true)
	_ = enc.EncodeString("columns")
	_ = enc.EncodeArrayLen(0)
	_ = enc.EncodeString("types")
	_ = enc.EncodeArrayLen(0)
	_ = enc.EncodeString("data")
	_ = enc.EncodeArrayLen(0)
	_ = enc.EncodeString("row_count")
	_ = enc.EncodeUint(0)
	_ = enc.EncodeString("execution_time_ms")
	_ = enc.EncodeUint(elapsedMsUint(start))
	_ = enc.EncodeString("timestamp")
	_ = enc.EncodeString(timestamp)
}

// writeMsgPackError writes a failure envelope as msgpack. Caller sets
// the HTTP status code; see respondError. Errors elided — see
// writeMsgPackEmpty.
func writeMsgPackError(c *fiber.Ctx, errMsg string, start time.Time, timestamp string) {
	c.Set(fiber.HeaderContentType, msgpackContentType)
	enc := msgpack.GetEncoder()
	enc.Reset(c.Response().BodyWriter())
	defer func() {
		enc.Reset(nil)
		msgpack.PutEncoder(enc)
	}()
	_ = enc.EncodeMapLen(4)
	_ = enc.EncodeString("success")
	_ = enc.EncodeBool(false)
	_ = enc.EncodeString("error")
	_ = enc.EncodeString(errMsg)
	_ = enc.EncodeString("execution_time_ms")
	_ = enc.EncodeUint(elapsedMsUint(start))
	_ = enc.EncodeString("timestamp")
	_ = enc.EncodeString(timestamp)
}

// msgpackContentType is the response Content-Type for the experimental
// MessagePack query endpoint. Matches the content type advertised by
// the ingest msgpack spec at msgpack.go.
const msgpackContentType = "application/msgpack"
