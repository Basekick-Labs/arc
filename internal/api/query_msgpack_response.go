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
func respondSuccessRows(c *fiber.Ctx, columns []string, data [][]interface{}, timestamp string, start time.Time) error {
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
	writeMsgPackRowsColumnar(c, columns, data, start, timestamp)
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
func writeMsgPackRowsColumnar(c *fiber.Ctx, columns []string, data [][]interface{}, start time.Time, timestamp string) {
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

	// "types": infer per-column from the first non-nil cell. SHOW
	// responses use Go-native types (string for names, int64 for
	// counts, float64 for sizes) so the rendered names mirror what
	// the streaming path emits via arrow.DataType.String(). Nil-only
	// columns fall back to "null".
	_ = enc.EncodeString("types")
	_ = enc.EncodeArrayLen(numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		typeName := "null"
		for _, row := range data {
			if colIdx >= len(row) || row[colIdx] == nil {
				continue
			}
			switch row[colIdx].(type) {
			case bool:
				typeName = "bool"
			case int, int32, int64:
				typeName = "int64"
			case uint, uint32, uint64:
				typeName = "uint64"
			case float32, float64:
				typeName = "float64"
			case string:
				typeName = "string"
			case []byte:
				typeName = "binary"
			default:
				typeName = "string"
			}
			break
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
			switch x := v.(type) {
			case bool:
				_ = enc.EncodeBool(x)
			case int:
				_ = enc.EncodeInt(int64(x))
			case int32:
				_ = enc.EncodeInt(int64(x))
			case int64:
				_ = enc.EncodeInt(x)
			case uint:
				_ = enc.EncodeUint(uint64(x))
			case uint32:
				_ = enc.EncodeUint(uint64(x))
			case uint64:
				_ = enc.EncodeUint(x)
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
	_ = enc.EncodeUint(uint64(time.Since(start).Milliseconds()))

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
	_ = enc.EncodeUint(uint64(time.Since(start).Milliseconds()))
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
	_ = enc.EncodeUint(uint64(time.Since(start).Milliseconds()))
	_ = enc.EncodeString("timestamp")
	_ = enc.EncodeString(timestamp)
}

// msgpackContentType is the response Content-Type for the experimental
// MessagePack query endpoint. Matches the content type advertised by
// the ingest msgpack spec at msgpack.go.
const msgpackContentType = "application/msgpack"
