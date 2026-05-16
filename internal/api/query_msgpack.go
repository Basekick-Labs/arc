//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
)

// The columnar streamer relies on the bufio.Writer's natural flush
// behavior (its internal 4KiB buffer flushes when full as the encoder
// writes primitives). Explicit per-column flushes added 7+ syscalls per
// LIMIT 1M without measurable disconnect-detection benefit; the column
// encoders below check ctx.Done() once per Arrow batch instead, which
// bounds disconnect detection at ~tens of milliseconds while keeping
// the per-row hot path branch-free.

func init() {
	arrowMsgPackQueryFunc = executeArrowMsgPackQuery
}

// executeArrowMsgPackQuery is the MessagePack twin of executeArrowJSONQuery.
// It is wired through the same dispatch surface as the JSON path
// (validation, RBAC, transform, ctx-timeout, profile dispatch happen
// upstream in executeQuery / executeQueryMsgPack), so this function is
// only responsible for two things:
//
//  1. Acquire the Arrow record reader from DuckDB.
//  2. Stream the response document as a single msgpack value over the HTTP
//     body. **The data array is columnar**, not row-oriented: each entry
//     in `data` is a per-column array of N values. This matches Arc's
//     columnar storage and ingest path, and avoids the per-cell type
//     dispatch that dominates row-oriented encode cost.
//
// Returns (rowCount, handled). Contract differs from the JSON path in one
// important way: when handled=false the caller MUST return 501 — there
// is no database/sql fallback because the entire point of the msgpack
// path is the typed Arrow → msgpack encode. A database/sql fallback
// would defeat the purpose and surprise clients who chose this endpoint
// specifically for the perf profile.
func executeArrowMsgPackQuery(
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
		// "no files found" → empty result, not error. Mirrors the JSON
		// path so a client polling a measurement before its first write
		// sees the same shape regardless of wire format.
		if isNoFilesFoundError(err) {
			if cancel != nil {
				cancel()
			}
			if onComplete != nil {
				onComplete(0)
			}
			_ = respondEmptySuccess(c, timestamp, start)
			return 0, true
		}

		// Timeout: same 504 + error envelope as the JSON path.
		if ctx.Err() == context.DeadlineExceeded {
			if cancel != nil {
				cancel()
			}
			m.IncQueryErrors()
			m.IncQueryTimeouts()
			if onTimeout != nil {
				onTimeout()
			}
			_ = respondError(c, fiber.StatusGatewayTimeout, "Query timed out", timestamp, start)
			return -1, true
		}

		// Driver doesn't implement Arrow — caller returns 501. The
		// JSON path falls back to database/sql here; we deliberately
		// do not (see function comment). String-matching here is
		// fragile (CLAUDE.md flags this anti-pattern); a sentinel
		// error from internal/database would be the durable fix but
		// the JSON path uses the same shape, so we keep parity until
		// a follow-up PR introduces ErrArrowUnsupported across both.
		errStr := err.Error()
		if strings.Contains(errStr, "does not implement driver.Conn") ||
			strings.Contains(errStr, "failed to create Arrow interface") {
			return 0, false
		}

		if cancel != nil {
			cancel()
		}
		m.IncQueryErrors()
		if onFail != nil {
			onFail(err.Error())
		}
		h.logger.Error().Err(err).Str("format", "msgpack").Str("sql", convertedSQL).Msg("Arrow MsgPack query failed")
		_ = respondError(c, fiber.StatusInternalServerError, err.Error(), timestamp, start)
		return -1, true
	}

	tokenName := getTokenName(c)

	// Materialize all Arrow batches synchronously BEFORE committing
	// HTTP headers. The columnar msgpack wire format buffers every
	// batch in memory anyway (array length prefix); doing the drain
	// here lets us return a proper 5xx via respondError when DuckDB
	// errors mid-materialization, instead of streaming a truncated
	// body under a 200 status that's already been sent. Gemini r3
	// flagged this; the JSON path has the same shape but its row-by-
	// row stream can produce partial JSON-valid output, so the
	// failure mode is less harmful there.
	schema := reader.Schema()
	batches, rowCount, drainErr := drainArrowBatches(ctx, reader, governanceMaxRows)
	// reader and conn are no longer needed after the drain — release
	// them before either the error or the streaming branch.
	reader.Release()
	if conn != nil {
		conn.Close()
	}

	if drainErr != nil {
		// Release any batches we did retain before bailing.
		for _, b := range batches {
			b.Release()
		}
		if cancel != nil {
			cancel()
		}
		// Timeout vs generic error: same shape as the up-front error
		// branch above, just driven by ctx state after the drain
		// attempt.
		if errors.Is(drainErr, context.DeadlineExceeded) {
			m.IncQueryErrors()
			m.IncQueryTimeouts()
			if onTimeout != nil {
				onTimeout()
			}
			_ = respondError(c, fiber.StatusGatewayTimeout, "Query timed out", timestamp, start)
			return -1, true
		}
		m.IncQueryErrors()
		if onFail != nil {
			onFail(drainErr.Error())
		}
		h.logger.Error().Err(drainErr).Str("format", "msgpack").
			Str("sql", convertedSQL).Msg("Arrow MsgPack drain failed")
		_ = respondError(c, fiber.StatusInternalServerError, drainErr.Error(), timestamp, start)
		return -1, true
	}

	// From here the response WILL be 200 — all batches are materialized
	// in memory, encode is the only remaining work and can only fail
	// via client disconnect (no DuckDB or context-deadline path).
	c.Set(fiber.HeaderContentType, msgpackContentType)
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		// Release retained batches when the stream finishes (or fails).
		defer func() {
			for _, b := range batches {
				b.Release()
			}
		}()

		// Wrap the 4KiB-default bufio.Writer Fiber hands us with a
		// 256 KiB layer. The msgpack encoder writes one primitive at a
		// time (~9 bytes per int64), so on a 1M-row response the
		// default bufio fills and flushes ~15k times — each flush is a
		// channel-send to fasthttp's chunked-transfer goroutine. The
		// larger buffer cuts that to ~250 sends.
		bw := bufio.NewWriterSize(w, 256*1024)
		rc, streamErr := streamMsgPackFromBatches(ctx, bw, schema, batches, rowCount, profile, start, timestamp)
		bw.Flush()
		w.Flush()

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
			h.streamErrEvent(streamErr).Err(streamErr).
				Str("format", "msgpack").
				Int("rows_sent", rc).
				Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
				Msg("Arrow MsgPack stream truncated after headers committed; client received partial result")
			return
		}

		m.IncQuerySuccess()
		m.IncQueryRows(int64(rc))
		m.RecordQueryLatency(time.Since(start).Microseconds())

		if onComplete != nil {
			onComplete(rc)
		}

		h.logger.Info().
			Str("format", "msgpack").
			Int("row_count", rc).
			Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
			Msg("Arrow MsgPack query completed")
		h.logSlowQuery(convertedSQL, start, rc, tokenName)
	})

	return 0, true
}

// drainArrowBatches reads all batches from the Arrow reader into a
// retained slice. governanceMaxRows (when > 0) trims the trailing batch
// to fit. Returns the batches, total row count, and an error if ctx
// fires or the reader surfaces a deferred error. Callers MUST Release()
// the returned batches when done.
func drainArrowBatches(ctx context.Context, reader array.RecordReader, governanceMaxRows int) ([]arrow.Record, int, error) {
	rowCap := 0
	if governanceMaxRows > 0 {
		rowCap = governanceMaxRows
	}

	var batches []arrow.Record
	rowCount := 0

	for reader.Next() {
		select {
		case <-ctx.Done():
			return batches, rowCount, fmt.Errorf("drain cancelled at row %d: %w", rowCount, ctx.Err())
		default:
		}
		batch := reader.Record()
		if batch == nil {
			break
		}
		n := int(batch.NumRows())
		if rowCap > 0 {
			if rowCount >= rowCap {
				break
			}
			if rowCount+n > rowCap {
				keep := rowCap - rowCount
				trimmed := batch.NewSlice(0, int64(keep))
				batches = append(batches, trimmed)
				rowCount = rowCap
				break
			}
		}
		batch.Retain()
		batches = append(batches, batch)
		rowCount += n
	}
	if err := reader.Err(); err != nil {
		return batches, rowCount, fmt.Errorf("arrow reader error after %d rows: %w", rowCount, err)
	}
	return batches, rowCount, nil
}

// streamMsgPackFromBatches writes the query response as a single
// msgpack map over a pre-drained slice of Arrow record batches. Shape:
//
//	map(7 or 8) {
//	  "success":           bool
//	  "columns":           [string...]            // column names
//	  "types":             [string...]            // arrow type names, parallel to columns
//	  "data":              [[v...], [v...], ...]  // numCols arrays, each with N values
//	  "row_count":         uint
//	  "execution_time_ms": uint                   (milliseconds)
//	  "timestamp":         string                 (RFC3339)
//	  "profile":           map (optional)
//	}
//
// Per column, the encoder branches once on the Arrow concrete type and
// runs a tight typed loop over the column buffer. This eliminates the
// per-cell type-switch that dominates row-oriented encode cost.
//
// The drain phase (reading batches from the Arrow reader, governanceMaxRows
// trimming) is the caller's responsibility via drainArrowBatches — done
// BEFORE the HTTP body stream opens, so DuckDB errors produce clean 5xx
// responses instead of truncated 200s. This function only encodes; the
// errors it returns are wire-write failures (client disconnect during
// encode) or ctx cancellation observed mid-column.
//
// Returns (rowsWritten, err). An error after the envelope has opened
// cannot change HTTP status, but the caller records it so operators
// see the partial-result signal.
func streamMsgPackFromBatches(
	ctx context.Context,
	w *bufio.Writer,
	schema *arrow.Schema,
	batches []arrow.Record,
	rowCount int,
	profile *database.QueryProfile,
	start time.Time,
	timestamp string,
) (int, error) {
	enc := msgpack.GetEncoder()
	enc.Reset(w)
	// The Basekick-Labs library reads `msgpack:"..."` struct tags by
	// default and falls back to the Go field name — NOT the json tag.
	// QueryProfile only has `json:"..."` tags, so without this the
	// profile field ships CamelCase (TotalMs / PlannerMs / ...) and
	// breaks the documented snake_case contract. Set the custom tag
	// before any Encode call.
	enc.SetCustomStructTag("json")
	defer func() {
		// Reset the struct tag back to the library default before
		// returning the encoder to the shared pool — otherwise the
		// next consumer (potentially the ingest path or cluster
		// replication, which expect `msgpack:` tags) silently
		// inherits our `json` setting. Gemini r3 flagged this as
		// pool poisoning.
		enc.SetCustomStructTag("msgpack")
		enc.Reset(nil)
		msgpack.PutEncoder(enc)
	}()

	fields := schema.Fields()
	numCols := len(fields)

	// Decide map size up front: 7 base keys + 1 if profile attached.
	mapLen := 7
	if profile != nil {
		mapLen = 8
	}
	if err := enc.EncodeMapLen(mapLen); err != nil {
		return 0, fmt.Errorf("encode map header: %w", err)
	}

	// "success": true
	if err := enc.EncodeString("success"); err != nil {
		return 0, err
	}
	if err := enc.EncodeBool(true); err != nil {
		return 0, err
	}

	// "columns": [name...]
	if err := enc.EncodeString("columns"); err != nil {
		return 0, err
	}
	if err := enc.EncodeArrayLen(numCols); err != nil {
		return 0, err
	}
	for _, f := range fields {
		if err := enc.EncodeString(f.Name); err != nil {
			return 0, err
		}
	}

	// "types": [arrow-type-name...] — clients use this to interpret
	// each column's elements (int64 vs uint64 vs string vs timestamp
	// ext type vs binary). Names come from arrow.DataType.String()
	// which is stable across the arrow-go library.
	if err := enc.EncodeString("types"); err != nil {
		return 0, err
	}
	if err := enc.EncodeArrayLen(numCols); err != nil {
		return 0, err
	}
	for _, f := range fields {
		if err := enc.EncodeString(f.Type.String()); err != nil {
			return 0, err
		}
	}

	// "data": [[col0_values...], [col1_values...], ...]
	if err := enc.EncodeString("data"); err != nil {
		return 0, err
	}

	// Outer data array: one entry per column.
	if err := enc.EncodeArrayLen(numCols); err != nil {
		return rowCount, err
	}

	// Per-column emit: one type-switch on the first batch's column,
	// then a typed loop over every batch's slice of that column. The
	// switch fires numCols times total — not numCols×numRows like the
	// row-oriented path. For LIMIT 1M × 7 columns this is 7 type
	// assertions instead of 7M.
	for colIdx := 0; colIdx < numCols; colIdx++ {
		// Ctx check at column boundary — a long column emit (1M ints)
		// is the longest uninterruptible work this function does, so
		// also check inside the typed loops below at row granularity.
		select {
		case <-ctx.Done():
			return rowCount, fmt.Errorf("stream cancelled at column %d: %w", colIdx, ctx.Err())
		default:
		}
		if err := enc.EncodeArrayLen(rowCount); err != nil {
			return rowCount, fmt.Errorf("encode column %d header: %w", colIdx, err)
		}
		if err := encodeColumn(ctx, enc, batches, colIdx); err != nil {
			return rowCount, err
		}
		// No explicit Flush per column. bufio.Writer flushes its 4KiB
		// internal buffer automatically when full; for a multi-MB
		// response that's already dozens of flushes triggered by the
		// encode itself. The per-batch ctx check inside the column
		// encoders handles disconnect detection. Saves 7+ syscalls
		// per typical LIMIT 1M query.
	}
	// Final flush of any trailing buffered bytes before the trailer
	// fields. Single syscall regardless of result size — bounds the
	// "last few KiB stuck in bufio" hazard at end-of-stream.
	if err := w.Flush(); err != nil {
		return rowCount, fmt.Errorf("stream flush failed before trailer: %w: %w", errClientDisconnected, err)
	}

	// "row_count": uint
	if err := enc.EncodeString("row_count"); err != nil {
		return rowCount, err
	}
	if err := enc.EncodeUint(uint64(rowCount)); err != nil {
		return rowCount, err
	}

	// "execution_time_ms": uint (milliseconds, integer — JSON sends
	// float but msgpack integers are cheaper and millisecond
	// precision matches the Arrow IPC HTTP trailer)
	if err := enc.EncodeString("execution_time_ms"); err != nil {
		return rowCount, err
	}
	if err := enc.EncodeUint(elapsedMsUint(start)); err != nil {
		return rowCount, err
	}

	// "timestamp": string (RFC3339)
	if err := enc.EncodeString("timestamp"); err != nil {
		return rowCount, err
	}
	if err := enc.EncodeString(timestamp); err != nil {
		return rowCount, err
	}

	// "profile": directly reflect-encoded; honors `json:"..."` tags
	// because we set SetCustomStructTag("json") at the top.
	if profile != nil {
		if err := enc.EncodeString("profile"); err != nil {
			return rowCount, err
		}
		if err := enc.Encode(profile); err != nil {
			return rowCount, err
		}
	}

	return rowCount, nil
}

// encodeColumn dispatches once on the Arrow concrete type and runs a
// tight typed loop over the column's slice in every retained batch.
// The first batch determines the column's runtime type; all subsequent
// batches in a single result must share that type (Arrow contract).
//
// Per-row work is minimal: a null-bitmap check + a typed encoder call.
// No interface dispatch, no type assertion, no heap allocation on the
// hot path (msgpack timestamp ext is 10 stack bytes; the float/int/
// string paths write directly to the encoder's pooled buffer).
func encodeColumn(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	if len(batches) == 0 {
		return nil
	}
	// Determine the column's runtime type from the first batch. All
	// batches in the same RecordReader share schema, so a single
	// type-switch is sufficient.
	first := batches[0].Column(colIdx)
	switch first.(type) {
	case *array.Int64:
		return encodeInt64Column(ctx, enc, batches, colIdx)
	case *array.Int32:
		return encodeInt32Column(ctx, enc, batches, colIdx)
	case *array.Int16:
		return encodeInt16Column(ctx, enc, batches, colIdx)
	case *array.Int8:
		return encodeInt8Column(ctx, enc, batches, colIdx)
	case *array.Uint64:
		return encodeUint64Column(ctx, enc, batches, colIdx)
	case *array.Uint32:
		return encodeUint32Column(ctx, enc, batches, colIdx)
	case *array.Uint16:
		return encodeUint16Column(ctx, enc, batches, colIdx)
	case *array.Uint8:
		return encodeUint8Column(ctx, enc, batches, colIdx)
	case *array.Float64:
		return encodeFloat64Column(ctx, enc, batches, colIdx)
	case *array.Float32:
		return encodeFloat32Column(ctx, enc, batches, colIdx)
	case *array.Boolean:
		return encodeBoolColumn(ctx, enc, batches, colIdx)
	case *array.Timestamp:
		return encodeTimestampColumn(ctx, enc, batches, colIdx)
	case *array.Date32:
		return encodeDate32Column(ctx, enc, batches, colIdx)
	case *array.String:
		return encodeStringColumn(ctx, enc, batches, colIdx)
	case *array.LargeString:
		return encodeLargeStringColumn(ctx, enc, batches, colIdx)
	case *array.Binary:
		return encodeBinaryColumn(ctx, enc, batches, colIdx)
	default:
		return encodeFallbackColumn(ctx, enc, batches, colIdx)
	}
}

// ctxCanceled returns ctx.Err() if the context has been cancelled,
// nil otherwise. Called once per Arrow batch (not per row) inside each
// typed column encoder so the inner row loop is free of branches. The
// function is small enough that the Go compiler inlines it without a
// pragma; the now-removed //go:inline annotation was a mistake (the
// only real Go compiler pragma in this family is //go:noinline, the
// negative form).
func ctxCanceled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// encodeInt64Column uses EncodeInt64 directly (always emits the 9-byte
// int64 form) instead of EncodeInt (which walks 5 size-class branches
// per cell). Wire is +0-7 bytes per non-fixint cell, CPU saves branching
// across the typical large-int64 column (e.g. UNIX nano timestamps,
// IDs). Same shape applies to encodeUint64Column.
func encodeInt64Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Int64)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeInt64(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Narrower int columns keep EncodeInt — fixint compaction (1-byte
// encoding for values 0-127) saves wire bytes on the common low-int
// case (booleans-as-int, small enums, status codes) without much CPU
// cost since the size-class branch is well-predicted for narrow types.
func encodeInt32Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Int32)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeInt(int64(c.Value(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeInt16Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Int16)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeInt(int64(c.Value(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeInt8Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Int8)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeInt(int64(c.Value(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeUint64Column: direct EncodeUint64 for the same reason as
// encodeInt64Column — typical uint64 columns (hashes, large counts)
// don't benefit from fixint compaction.
func encodeUint64Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Uint64)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeUint64(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeUint32Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Uint32)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeUint(uint64(c.Value(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeUint16Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Uint16)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeUint(uint64(c.Value(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeUint8Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Uint8)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeUint(uint64(c.Value(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeFloat64Column emits raw IEEE 754 float64 bits. NaN/Inf are
// preserved natively (msgpack float64 carries the bit pattern verbatim);
// no need for the JSON-style "is this representable" check.
func encodeFloat64Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Float64)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeFloat64(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeFloat32Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Float32)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeFloat32(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeBoolColumn(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Boolean)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeBool(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeTimestampColumn emits msgpack's native timestamp Ext type
// (fixext8 or ext8 depending on range). Saves ~25 bytes per cell vs
// the previous RFC3339Nano string encoding, eliminates the time.Time
// allocation chain (UTC() + AppendFormat + string(buf) per cell), and
// is the format language-level msgpack libraries decode to native time
// types.
func encodeTimestampColumn(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	// Resolve the unit once outside the batch loop. Arrow guarantees
	// schema (and therefore unit) is constant across batches in a
	// single RecordReader, so the assertion can hoist.
	var unit arrow.TimeUnit
	if len(batches) > 0 {
		unit = batches[0].Column(colIdx).(*array.Timestamp).DataType().(*arrow.TimestampType).Unit
	}
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Timestamp)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else {
				t := c.Value(i).ToTime(unit)
				if err := enc.EncodeTime(t); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func encodeDate32Column(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Date32)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeTime(c.Value(i).ToTime()); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeStringColumn(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.String)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeString(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeLargeStringColumn(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.LargeString)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeString(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func encodeBinaryColumn(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx).(*array.Binary)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeBytes(c.Value(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeFallbackColumn handles Arrow types that don't have a typed
// encoder above. Uses Arrow's ValueStr to render each cell as a string;
// loses the type-fidelity wins of the typed path but keeps the wire
// format valid for unanticipated types.
func encodeFallbackColumn(ctx context.Context, enc *msgpack.Encoder, batches []arrow.Record, colIdx int) error {
	for _, batch := range batches {
		if err := ctxCanceled(ctx); err != nil {
			return err
		}
		c := batch.Column(colIdx)
		n := c.Len()
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				if err := enc.EncodeNil(); err != nil {
					return err
				}
			} else if err := enc.EncodeString(c.ValueStr(i)); err != nil {
				return err
			}
		}
	}
	return nil
}
