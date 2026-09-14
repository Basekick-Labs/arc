//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// streamArrowIPCFunc indirects streamArrowIPC so tests can drive the
// response writer's panic path (#716). Production always uses the real one.
var streamArrowIPCFunc = streamArrowIPC

// releaseArrowStreamResourcesFunc indirects the cleanup so tests can count how
// many times it runs (#733). Production always uses the real one.
var releaseArrowStreamResourcesFunc = releaseArrowStreamResources

// arrowTrailerWarnOnce gates the AddTrailer failure log so a fasthttp
// upgrade that ever rejects the trailer name does not produce a Warn per
// request.
var arrowTrailerWarnOnce sync.Once

// arrowExecutionTimeTrailer is the HTTP response trailer carrying server-
// side query execution time (milliseconds) on the Arrow IPC endpoint.
// Clients consume the full Arrow stream then read this trailer.
const arrowExecutionTimeTrailer = "Arc-Execution-Time-Ms"

// arrowBatchSize is the number of rows per Arrow record batch.
// Smaller batches reduce peak memory usage and enable streaming.
// 10K rows is a good balance between overhead and memory efficiency.
const arrowBatchSize = 10000

// arrowStreamTruncatedTrailer tells the client the Arrow IPC body it received
// is short. It is a trailer because the status line and headers are long gone
// by the time a stream fails.
const arrowStreamTruncatedTrailer = "Arc-Stream-Truncated"

// arrowRowsCappedTrailer tells the client the Arrow IPC body it received
// stopped at an Enterprise governance row cap and may therefore be missing
// rows the query matched (#724). The value is the cap.
//
// Read it alongside arrowStreamTruncatedTrailer, which means nearly the
// opposite: that trailer says the body is short because the stream FAILED and
// must not be trusted, while this one says the body is short because policy
// said so, and is a complete, valid result up to the cap.
//
// A trailer for the same reason the other two are: the cap is only known to
// have been reached once the last batch has been written, long after the
// status line and headers went out. Like every fasthttp trailer it is emitted
// on every response once registered, carrying an empty value when never Set,
// so the client contract is "non-empty means capped".
const arrowRowsCappedTrailer = "Arc-Rows-Capped"

// transportWriter distinguishes "the socket went away" from "the encoder
// failed". Both surface as an error out of ipc.Writer.Write, and only the
// former means nobody is listening. Wrapping the sink is the only way to tell:
// a real Arrow batch is far larger than the 4KB bufio fasthttp hands the
// stream callback, so writes go straight through and a hangup lands on Write
// rather than on the later Flush that used to be the only place it was tagged.
type transportWriter struct {
	w   *bufio.Writer
	err error
}

func (t *transportWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if err != nil {
		t.err = err
	}
	return n, err
}

// poisonArrowStream writes an Arrow IPC message header that can never be
// satisfied, so a client decoding the stream fails instead of accepting a
// short read as a complete result (#721).
//
// A truncated Arrow stream is otherwise indistinguishable from a complete one:
// cut on a record-batch boundary it decodes with no error, and the paths that
// still reach ipcWriter.Close() emit a valid end-of-stream marker on top. The
// eight bytes here are the encapsulated-message format from the Arrow spec, a
// 0xFFFFFFFF continuation token followed by a metadata length, so a reader hits
// EOF partway through a message it was promised.
//
// The length must be non-zero, since zero after the continuation token is
// itself the end-of-stream marker, and small, because readers allocate it
// before reading.
func poisonArrowStream(w *bufio.Writer, logger zerolog.Logger) {
	if w == nil {
		return
	}
	// The trailing byte matters: io.ReadFull returns a plain io.EOF when it
	// reads nothing at all, and arrow-go treats that as a clean end of stream.
	// One byte of the promised metadata makes the read genuinely short, which
	// surfaces as an unexpected EOF the reader reports.
	if _, err := w.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x40, 0x00, 0x00, 0x00, 0x00}); err != nil {
		logger.Debug().Err(err).Msg("Could not mark the Arrow IPC stream truncated; the client is already gone")
		return
	}
	if err := w.Flush(); err != nil {
		logger.Debug().Err(err).Msg("Could not flush the Arrow IPC truncation marker")
	}
}

// releaseArrowStreamResources frees everything the Arrow IPC response owns:
// the DuckDB-backed reader, the pooled connection behind it, and the query
// timeout context. Order matters — the reader's Release closes the result set
// and statement that live on this connection, so returning the connection to
// the pool first would hand another query a connection with an open result.
//
// It recovers on its own behalf (#716). Its single caller is a defer inside the
// Arrow IPC stream writer, so it runs on a bare fasthttp goroutine and, on the
// panic path, during the unwind with a panic already in flight: a second panic
// raised here would either kill the process or, if caught by the caller's
// recover, replace the root-cause panic value, since recover() only ever yields
// the most recent one. Do not remove the recover on the grounds that the happy
// path cannot panic; the unwind is the case it exists for.
func releaseArrowStreamResources(
	reader array.RecordReader,
	conn interface{ Close() error },
	cancel context.CancelFunc,
	logger zerolog.Logger,
) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error().Interface("panic", r).
				Msg("Arrow IPC resource cleanup panicked; connection or reader may be leaked")
		}
	}()
	// Nil checks are not academic: conn is held as an interface, so a typed
	// nil pointer is itself non-nil here and would nil-panic inside cleanup.
	if reader != nil {
		reader.Release()
	}
	if conn != nil {
		conn.Close()
	}
	if cancel != nil {
		cancel()
	}
}

// streamArrowIPC writes Arrow record batches as IPC frames to w. It returns
// the number of rows consumed and the error that stopped the stream, if any.
// The caller retains ownership of reader and performs request-level cleanup.
//
// maxRows, when > 0, is the governance row cap (#702): the batch that would
// cross it is sliced at the boundary and the stream stops there, matching the
// truncation semantics drainArrowBatches applies on the msgpack path.
func streamArrowIPC(
	ctx context.Context,
	w *bufio.Writer,
	reader array.RecordReader,
	schema *arrow.Schema,
	castInfo *decimalCastInfo,
	dictEnabled bool,
	ipcCompression string,
	maxRows int,
	logger zerolog.Logger,
) (int64, error) {
	// The IPC writer is created lazily on the first batch: when
	// dictionary encoding is requested, the output schema depends on a
	// cardinality analysis of that batch (see newArrowDictTransformer).
	var ipcWriter *ipc.Writer
	var dictXform *arrowDictTransformer
	// Dictionary columns use REPLACEMENT dictionaries (arrow-go's
	// default): the writer skips re-sending an unchanged dictionary and
	// re-sends the full dictionary when it grows. Deliberately NOT
	// WithDictionaryDeltas — polars cannot read delta batches (verified
	// against polars 1.43; pyarrow reads both).
	// Everything the encoder writes goes through tw, so a socket failure can
	// be told apart from an encoder failure when classifying the error below.
	tw := &transportWriter{w: w}
	newIPCWriter := func(outSchema *arrow.Schema) *ipc.Writer {
		opts := []ipc.Option{ipc.WithSchema(outSchema)}
		switch ipcCompression {
		case "zstd":
			opts = append(opts, ipc.WithZstd())
		case "lz4":
			opts = append(opts, ipc.WithLZ4())
		}
		return ipc.NewWriter(tw, opts...)
	}

	var totalRows int64
	var streamErr error
streamLoop:
	for reader.Next() {
		// Per-batch ctx check: a timeout firing or client disconnect
		// must short-circuit instead of draining DuckDB into a
		// buffer the client is no longer reading. See review/
		// query-path-criticals C5. Non-blocking select with labeled
		// break is the idiomatic Go cancellation pattern (gemini r1).
		select {
		case <-ctx.Done():
			streamErr = fmt.Errorf("stream cancelled at row %d: %w", totalRows, ctx.Err())
			break streamLoop
		default:
		}

		batch := reader.Record()
		if batch == nil {
			break
		}

		// Each iteration runs as its own function so the records it owns
		// are freed by one defer (#716). Straight-line releases are
		// skipped when anything inside panics, and DuckDB-backed records
		// hold C-allocated buffers the GC will not reclaim. The cgo
		// reader path, castDecimalBatch, the dictionary transformer and
		// NewSlice can all raise, so this is the panic surface that a
		// caller-side defer cannot cover.
		//
		// Returns true when the caller should stop iterating; streamErr
		// carries the reason when stopping is a failure.
		stop := func() bool {
			var slicedBatch, castedBatch, encodedBatch arrow.Record
			defer func() {
				if encodedBatch != nil {
					encodedBatch.Release()
				}
				if castedBatch != nil {
					castedBatch.Release()
				}
				if slicedBatch != nil {
					slicedBatch.Release()
				}
			}()

			// Governance row cap (#702): slice the batch that would cross
			// the cap and stop draining.
			if maxRows > 0 {
				remaining := int64(maxRows) - totalRows
				// Defensive: the post-flush stop below means the loop
				// should never re-enter with the cap already reached.
				// This keeps the row count correct if that is removed.
				if remaining <= 0 {
					return true
				}
				if batch.NumRows() > remaining {
					slicedBatch = batch.NewSlice(0, remaining)
					batch = slicedBatch
				}
			}
			totalRows += batch.NumRows()

			// castInfo != nil replaces batch with a record this iteration
			// owns. The original reader.Record() is NOT released here:
			// reader.Next() releases the prior record automatically.
			if castInfo != nil {
				var castErr error
				castedBatch, castErr = castDecimalBatch(batch, castInfo)
				if castErr != nil {
					streamErr = fmt.Errorf("failed to cast decimal columns at row %d: %w", totalRows, castErr)
					return true
				}
				batch = castedBatch
			}

			// First batch: decide dictionary columns (if requested) and
			// create the writer with the final output schema.
			if ipcWriter == nil {
				outSchema := schema
				if dictEnabled {
					// Analysis runs on the (possibly decimal-casted) batch
					// so the transformer's schema matches what it receives.
					dictXform = newArrowDictTransformer(batch, &logger)
					if dictXform != nil {
						outSchema = dictXform.schema
					}
				}
				ipcWriter = newIPCWriter(outSchema)
			}

			writeBatch := batch
			if dictXform != nil {
				var encErr error
				encodedBatch, encErr = dictXform.transform(batch)
				if encErr != nil {
					streamErr = fmt.Errorf("failed to dictionary-encode batch at row %d: %w", totalRows, encErr)
					return true
				}
				writeBatch = encodedBatch
			}

			if err := ipcWriter.Write(writeBatch); err != nil {
				// A hangup surfaces here, not at the Flush below, because a
				// batch overflows the 4KB bufio and writes straight through.
				// Tag it so the caller treats it as a client disconnect
				// rather than a server-side failure worth alerting on.
				if tw.err != nil {
					streamErr = fmt.Errorf("stream write failed at row %d: %w: %w", totalRows, errClientDisconnected, err)
				} else {
					streamErr = fmt.Errorf("failed to write arrow batch at row %d: %w", totalRows, err)
				}
				return true
			}
			// Capture Flush error: fasthttp's RequestCtx.Done() only fires
			// on server shutdown (not per-request client disconnect), so
			// the underlying bufio.Writer's error on the closed connection
			// is our signal that the client has gone away. Wrap with the
			// sentinel so the caller logs at Warn (not Error) for this
			// expected ops noise.
			if err := w.Flush(); err != nil {
				streamErr = fmt.Errorf("stream flush failed at row %d: %w: %w", totalRows, errClientDisconnected, err)
				return true
			}
			// Cap reached: stop before reader.Next() materializes another
			// DuckDB batch that would only be discarded (same early break
			// as drainArrowBatches).
			return maxRows > 0 && totalRows >= int64(maxRows)
		}()
		if stop {
			break
		}
	}

	if streamErr == nil {
		if err := reader.Err(); err != nil {
			streamErr = fmt.Errorf("arrow reader error after %d rows: %w", totalRows, err)
		}
	}

	// Zero-batch result: no writer was created in the loop. Emit an
	// empty stream with the base schema so clients still get a valid
	// Arrow IPC response.
	if ipcWriter == nil {
		ipcWriter = newIPCWriter(schema)
	}
	if dictXform != nil {
		defer dictXform.release()
	}

	// A failed stream must not be closed cleanly (#721). ipc.Writer.Close
	// emits the end-of-stream marker, which would make a short result read as
	// a complete one: an Arrow stream cut on a batch boundary decodes without
	// error, and this path is always on a boundary because every batch is
	// flushed. Mark the body unsatisfiable instead, so the client's decode
	// fails rather than quietly returning fewer rows than the query matched.
	// This covers the zero-batch case too, where the schema-only stream would
	// otherwise read as a legitimate empty result.
	//
	// A client that has already gone is skipped: there is nobody to tell, and
	// writing to a dead socket only produces noise.
	if streamErr != nil && !errors.Is(streamErr, errClientDisconnected) {
		poisonArrowStream(w, logger)
		return totalRows, streamErr
	}

	if err := ipcWriter.Close(); err != nil {
		// Warn (not Error): when the loop broke because the client
		// disconnected, ipcWriter.Close is guaranteed to fail flushing
		// trailing IPC metadata over the already-closed connection.
		// That's the same client-disconnect event already captured in
		// streamErr — emitting Error here would defeat the ops-noise
		// reduction.
		logger.Warn().Err(err).Msg("Failed to close Arrow IPC writer")
	}
	return totalRows, streamErr
}

// executeQueryArrow handles POST /api/v1/query/arrow - returns Arrow IPC stream
// Optimized to stream rows directly into Arrow batches without intermediate buffering.
func (h *QueryHandler) executeQueryArrow(c *fiber.Ctx) error {
	start := time.Now()
	m := metrics.Get()

	// Parse request body
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Validate SQL (empty, max length, dangerous patterns)
	if err := ValidateSQLRequest(req.SQL); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Opt-in stream encodings. Both change the bytes the client must be able
	// to parse, so they are strictly request-driven — no config default:
	//   x-arc-arrow-dictionary: true  → dictionary-encode low-cardinality
	//     string columns (schema becomes dictionary<int32, utf8> for them)
	//   x-arc-arrow-compression: zstd|lz4 → Arrow IPC buffer compression
	dictEnabled := isTruthyHeader(c.Get("x-arc-arrow-dictionary"))
	// strings.Clone is REQUIRED: c.Get aliases the fasthttp header buffer,
	// and ToLower/TrimSpace return the ORIGINAL string when unchanged (e.g.
	// "zstd"). ipcCompression is captured by the SetBodyStreamWriter closure
	// and read after the handler returns — same aliasing class as the
	// x-arc-database wrong-DB corruption bug.
	ipcCompression := strings.Clone(strings.ToLower(strings.TrimSpace(c.Get("x-arc-arrow-compression"))))
	switch ipcCompression {
	case "", "zstd", "lz4":
	default:
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "invalid x-arc-arrow-compression: must be zstd or lz4",
		})
	}

	// Extract x-arc-database header for optimized query path
	headerDB := c.Get("x-arc-database")
	if err := validateHeaderDatabase(headerDB); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "invalid x-arc-database header: " + err.Error(),
		})
	}

	// If header is set, reject cross-database syntax (db.table not allowed)
	if headerDB != "" && hasCrossDatabaseSyntax(req.SQL) {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Cross-database queries (db.table syntax) not allowed when x-arc-database header is set",
		})
	}

	// RBAC-gate SHOW commands, mirroring executeQuery / estimateQuery. SHOW
	// commands carry no FROM/JOIN table references, so checkQueryPermissions
	// would pass them through unchecked. The Arrow IPC endpoint has no SHOW
	// result handler (handleShowDatabases/handleShowTables emit JSON), so we
	// gate on the same permission and return a 400 directing callers to
	// /api/v1/query. normalizeSQLForShow masks literals before stripping
	// comments so a comment marker inside a quoted db name can't truncate it.
	showNormalised := normalizeSQLForShow(req.SQL)
	if showDatabasesPattern.MatchString(showNormalised) {
		if err := h.checkMeasurementPermission(c, "*", "*", "read"); err != nil {
			m.IncQueryErrors()
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"success": false,
				"error":   "access denied: no read permission to list databases",
			})
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "SHOW DATABASES is not supported on the Arrow endpoint; use /api/v1/query instead",
		})
	}
	if matches := showTablesPattern.FindStringSubmatch(showNormalised); matches != nil {
		database := "default"
		if len(matches) > 1 && matches[1] != "" {
			database = matches[1]
		} else if headerDB != "" {
			database = headerDB
		}
		if err := validateIdentifier(database); err != nil {
			m.IncQueryErrors()
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "invalid database name: " + err.Error(),
			})
		}
		if err := h.checkMeasurementPermission(c, database, "*", "read"); err != nil {
			m.IncQueryErrors()
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"success": false,
				"error":   fmt.Sprintf("access denied: no read permission for database '%s'", database),
			})
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "SHOW TABLES/MEASUREMENTS is not supported on the Arrow endpoint; use /api/v1/query instead",
		})
	}

	// Check RBAC permissions for all tables referenced in the query
	if err := h.checkQueryPermissions(c, req.SQL, "read"); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Enterprise query governance (#702): rate limits, quotas, the MaxRows
	// row cap, and the policy timeout apply here the same as on
	// POST /api/v1/query. The 429 uses this endpoint's fiber.Map error
	// shape, matching every other error it returns.
	governanceMaxRows, governanceTimeout, governanceRejection := h.checkQueryGovernance(c)
	if governanceRejection != nil {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"success": false,
			"error":   governanceRejection.reason,
		})
	}

	// Convert SQL to storage paths (with caching)
	// If headerDB is set, uses optimized path that skips db.table regex patterns
	//
	// The rejection must be answered HERE, before the arcx hook and before
	// ArrowQueryContext: once either commits to SetBodyStreamWriter the status
	// code is already on the wire, and the only way left to report a failure is
	// a trailer the client may not read.
	convertedSQL, _, err := h.getTransformedSQL(c.Context(), req.SQL, headerDB)
	if err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	h.logger.Debug().
		Str("original_sql", sqlutil.ForLog(req.SQL)).
		Str("converted_sql", sqlutil.ForLog(convertedSQL)).
		Str("header_db", headerDB).
		Msg("Executing Arrow query")

	// Create context with timeout if configured
	// Use context.Background() instead of c.UserContext() because SetBodyStreamWriter
	// runs asynchronously after the handler returns, and c.UserContext() would be cancelled
	// Note: We don't use defer cancel() here because the streaming callback runs after
	// this handler returns - cancel is called inside the callback after rows are consumed
	// A governance policy's MaxDuration overrides the global timeout (#702).
	effectiveTimeout := h.queryTimeout
	if governanceTimeout > 0 {
		effectiveTimeout = governanceTimeout
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if effectiveTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, effectiveTimeout)
	}

	// arcx router hook (Arrow-IPC variant). In serve mode a green shape is
	// streamed as Arrow IPC by arcx (its most natural output); shadow mode
	// compares off to the side and returns false. No-op stub without the
	// arcx_engine tag — stock Arc unaffected. Uses ctx (background-derived, safe
	// in the async writer), not c.UserContext().
	// Pass `cancel` INTO the hook — the arcx serve path streams asynchronously in
	// SetBodyStreamWriter (after this returns), so it owns calling cancel when the stream
	// finishes. Calling cancel() here would cancel execCtx mid-stream → truncate to schema-only.
	// Governance note (#702): rate limit, quota, and the policy timeout
	// (via ctx) apply to an arcx-served response, but the MaxRows cap is
	// enforced only by the DuckDB IPC loop below — the experimental arcx
	// serve path streams uncapped until it learns to take a row cap.
	if h.tryArcxRouterArrow(c, ctx, cancel, req.SQL, headerDB, convertedSQL) {
		return nil
	}

	// Execute query using DuckDB's native Arrow API — returns record batches
	// directly from DuckDB's internal columnar chunks, no row-by-row scanning.
	reader, conn, err := h.db.ArrowQueryContext(ctx, convertedSQL)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if effectiveTimeout > 0 && ctx.Err() == context.DeadlineExceeded {
			m.IncQueryTimeouts()
			h.logger.Error().Err(err).Str("sql", sqlutil.ForLog(req.SQL)).Dur("timeout", effectiveTimeout).Msg("Arrow query timed out")
			return c.Status(fiber.StatusGatewayTimeout).JSON(fiber.Map{
				"success": false,
				"error":   "Query timed out",
			})
		}
		m.IncQueryErrors()
		h.logger.Error().Err(err).Str("sql", sqlutil.ForLog(req.SQL)).Msg("Arrow query execution failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	schema := reader.Schema()

	// Normalize decimal columns in the schema — DuckDB returns SUM(integer) as
	// decimal(38,0) which many Arrow clients (e.g. Grafana) cannot handle.
	// castInfo is nil when there are no decimal columns (zero overhead on hot path).
	castInfo := normalizeDecimalSchema(schema)
	if castInfo != nil {
		schema = castInfo.schema
	}

	c.Set("Content-Type", "application/vnd.apache.arrow.stream")

	// Capture the fasthttp RequestCtx once. c *fiber.Ctx is pooled and
	// reset after this handler returns; the SetBodyStreamWriter callback
	// runs asynchronously after the handler exits, so any call to
	// c.Context() inside the closure observes a recycled context (nil
	// pointer panic in practice — verified by integration test against
	// clickbench). The fasthttp RequestCtx itself stays valid until the
	// stream writer returns.
	fctx := c.Context()
	respHeader := &fctx.Response.Header

	// Captured before the stream writer commits: the pooled fiber.Ctx is
	// recycled by the time the closure runs, so reading the token out of it
	// there is the same use-after-free the comment above describes.
	tokenName := getTokenName(c)
	tokenID := getTokenID(c)

	// Declare the execution-time trailer in the response head before
	// SetBodyStreamWriter runs so the `Trailer:` response header is
	// emitted before the chunked body starts. Clients that don't read
	// trailers degrade gracefully to wall-clock timing. The Warn is
	// sync.Once-gated against per-request log spam if a future fasthttp
	// release ever rejects the trailer name.
	if err := respHeader.AddTrailer(arrowStreamTruncatedTrailer); err != nil {
		arrowTrailerWarnOnce.Do(func() {
			h.logger.Warn().Err(err).Str("trailer", arrowStreamTruncatedTrailer).
				Msg("Failed to register Arrow truncation trailer; clients will not see the reason a stream was cut")
		})
	}
	if err := respHeader.AddTrailer(arrowExecutionTimeTrailer); err != nil {
		arrowTrailerWarnOnce.Do(func() {
			h.logger.Warn().Err(err).Str("trailer", arrowExecutionTimeTrailer).
				Msg("Failed to register Arrow execution-time trailer; clients will not see server-side timing")
		})
	}
	// Registered unconditionally rather than only when a cap applies: the
	// registration has to happen here, before the body streams, and whether
	// the cap is reached is not known until the stream ends.
	if err := respHeader.AddTrailer(arrowRowsCappedTrailer); err != nil {
		arrowTrailerWarnOnce.Do(func() {
			h.logger.Warn().Err(err).Str("trailer", arrowRowsCappedTrailer).
				Msg("Failed to register Arrow row-cap trailer; clients will not see that a governance cap truncated the result")
		})
	}

	// Trailer values are collected here and published from the connection
	// goroutine when the body ends (#729). Setting them directly on respHeader
	// from the stream writer is a data race against fasthttp serialising the
	// response head.
	trailers := newResponseTrailers()

	streamCtx := ctx
	// streamW lets the panic path reach the same writer the body used, so it
	// can mark the stream truncated. safeStream's onPanic takes no arguments,
	// and widening it would churn five other call sites that have no writer to
	// mark.
	//
	// Cleanup is an ordinary defer inside the writer, which is what every other
	// stream writer in this package already does (#733). It used to run from
	// onPanic as well as straight-line at the end of the writer, so a panic
	// after the straight-line call ran it twice. That was harmless only because
	// all three resources tolerate a second call, which no test pinned and the
	// array.RecordReader interface does not promise.
	//
	// It also covers a panic inside onPanic ahead of where the release used to
	// sit, which skipped it and stranded the pooled connection. That one is
	// reasoned, not tested: nothing in onPanic can be made to panic from a test
	// without adding a seam for it.
	//
	// One thing the old shape did that this does not: releaseArrowStreamResources
	// installs its recover first, so a panic in reader.Release skipped the
	// conn.Close and cancel below it, and a later panic then retried them
	// through onPanic. That retry is gone. It is not worth restoring, but the
	// change is not strictly better on every panic path.
	var streamW *bufio.Writer
	h.setBodyStreamWithTrailers(fctx, "query_arrow_ipc", trailers, func() {
		// A recovered panic leaves a stream the client would otherwise read as
		// complete: fasthttp still writes the terminating chunk, and a cut on
		// a batch boundary decodes without error (#721).
		//
		// The body marker is the signal a client acts on, and it is always
		// written: streamW is assigned before anything in the writer that can
		// panic, so it is never nil by the time a recovered panic reaches here.
		// The trailer adds the reason, which this path carried not at all until
		// #729 made setting one from this goroutine safe, and which the error
		// path has always carried.
		//
		// setIfAbsent, so a stream that failed with a real error and then
		// panicked on its way out still reports the error.
		poisonArrowStream(streamW, h.logger)
		trailers.setIfAbsent(arrowStreamTruncatedTrailer, "stream writer panicked")
	}, func(w *bufio.Writer) {
		// Registered before anything that can panic, so it runs exactly once
		// whether the writer returns normally or unwinds (#733).
		defer releaseArrowStreamResourcesFunc(reader, conn, cancel, h.logger)
		streamW = w
		totalRows, streamErr := streamArrowIPCFunc(
			streamCtx, w, reader, schema, castInfo, dictEnabled, ipcCompression, governanceMaxRows, h.logger,
		)

		// Publish authoritative server-side timing as a chunked-transfer
		// trailer. Set even on the error path so partial results carry
		// time-until-failure. On a hard client disconnect the trailer is
		// silently dropped, same as any other post-body byte.
		//
		// Recorded on `trailers`, not on the response header: this closure
		// runs on the stream-writer goroutine, which must not touch the header
		// at all (#729). The connection goroutine publishes them when the body
		// ends. fiber.Ctx is pooled and reset before this closure runs, so it
		// is equally off limits.
		execMs := time.Since(start).Milliseconds()
		trailers.set(arrowExecutionTimeTrailer, strconv.FormatInt(execMs, 10))

		// Reported before the error branch below: a stream can reach the cap
		// and then fail on the way out, so the operator-side record has to
		// fire on both paths or it would go missing in the one case where the
		// result is both capped and truncated (#724). The trailer below stays
		// on the success path only, because a failed body is poisoned and
		// must not be advertised as valid up to the cap.
		h.logGovernanceRowCap("arrow_ipc", convertedSQL, tokenID, tokenName, governanceMaxRows, totalRows)

		if streamErr != nil {
			m.IncQueryErrors()
			// Per-handler client-disconnect counter (#426). Lets operators
			// dashboard the rate without log-scraping. Only fires on
			// client-side events (disconnect / deadline / context-cancel)
			// — server-side stream failures stay in IncQueryErrors only.
			if isClientError(streamErr) {
				m.IncQueryClientDisconnect(metrics.DisconnectPathArrowIPC)
			}
			// Tell the client its Arrow stream is short (#721). Skipped only
			// when the socket is already gone: a server-side timeout is a
			// client-side error class here but the caller is still waiting,
			// so it must be told, which is why this keys off the disconnect
			// sentinel rather than isClientError.
			// streamArrowIPC has already marked the body itself; the
			// marker has to precede the end-of-stream bytes, so it cannot
			// be written from out here.
			if !errors.Is(streamErr, errClientDisconnected) {
				trailers.set(arrowStreamTruncatedTrailer, sqlutil.SanitizeErrText(streamErr.Error()))
			}
			// Warn for client-disconnect / timeout (expected ops noise);
			// Error for everything else (real server-side problem worth
			// alerting on).
			h.streamErrEvent(streamErr).Err(streamErr).
				Int64("rows_sent", totalRows).
				Int64("execution_time_ms", execMs).
				Msg("Arrow IPC stream truncated after headers committed; client received partial result")
			return
		}

		// Governance row cap (#724): an Arrow IPC stream that stopped at the
		// cap is a valid, cleanly terminated stream, so nothing in the body
		// distinguishes it from a complete result. Set on the success path
		// only: a failed stream is already poisoned and carries
		// arrowStreamTruncatedTrailer, which tells the client the opposite
		// thing (do not trust this body at all).
		if rowCapReached(governanceMaxRows, totalRows) {
			trailers.set(arrowRowsCappedTrailer, strconv.Itoa(governanceMaxRows))
		}

		h.logger.Info().
			Int64("row_count", totalRows).
			Int64("execution_time_ms", execMs).
			Msg("Arrow streaming query completed")
	})

	return nil
}

// decimalCastInfo holds the modified schema and per-column cast targets for
// queries that return decimal columns (e.g. SUM/AVG on integer columns).
type decimalCastInfo struct {
	schema  *arrow.Schema
	targets []arrow.DataType // nil entry = no cast needed for that column index
}

// normalizeDecimalSchema inspects the schema for decimal columns and returns
// a decimalCastInfo with a substituted schema if any are found, or nil if
// there are no decimal columns (zero overhead on the common path).
//
//   - decimal(x, 0) → int64  (SUM/COUNT of integers)
//   - decimal(x, y) → float64 (AVG or user-configured decimals)
func normalizeDecimalSchema(schema *arrow.Schema) *decimalCastInfo {
	hasDecimal := false
	for i := 0; i < schema.NumFields(); i++ {
		// Match the arrow.DecimalType interface, not *arrow.Decimal128Type:
		// arrow-go also has Decimal32/64/256, and a decimal that slips
		// through here is neither normalized nor encodable (the msgpack
		// encoder has no decimal case), so it would go out as a string
		// under a numeric-looking type name.
		if _, ok := schema.Field(i).Type.(arrow.DecimalType); ok {
			hasDecimal = true
			break
		}
	}
	if !hasDecimal {
		return nil
	}

	targets := make([]arrow.DataType, schema.NumFields())
	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		if dt, ok := f.Type.(arrow.DecimalType); ok {
			if dt.GetScale() == 0 {
				targets[i] = arrow.PrimitiveTypes.Int64
			} else {
				targets[i] = arrow.PrimitiveTypes.Float64
			}
			fields[i] = arrow.Field{Name: f.Name, Type: targets[i], Nullable: f.Nullable, Metadata: f.Metadata}
		} else {
			fields[i] = f
		}
	}

	md := schema.Metadata()
	return &decimalCastInfo{
		schema:  arrow.NewSchema(fields, &md),
		targets: targets,
	}
}

// castDecimalBatch replaces decimal columns in the batch with int64 or float64
// using arrow-go's compute.CastArray (SIMD-optimized, handles nulls via bitmap).
// The returned record must be Released by the caller.
func castDecimalBatch(batch arrow.Record, info *decimalCastInfo) (arrow.Record, error) {
	cols := make([]arrow.Array, batch.NumCols())
	toRelease := make([]arrow.Array, 0, batch.NumCols())

	ctx := compute.WithAllocator(context.Background(), memory.DefaultAllocator)

	for i, target := range info.targets {
		if target == nil {
			cols[i] = batch.Column(i)
			continue
		}
		casted, err := compute.CastArray(ctx, batch.Column(i), compute.SafeCastOptions(target))
		if err != nil {
			// Release any arrays we already allocated
			for _, a := range toRelease {
				a.Release()
			}
			return nil, err
		}
		cols[i] = casted
		toRelease = append(toRelease, casted)
	}

	rec := array.NewRecord(info.schema, cols, batch.NumRows())
	for _, a := range toRelease {
		a.Release()
	}
	return rec, nil
}

// sqlTypeToArrowType converts SQL type names to Arrow types
func sqlTypeToArrowType(sqlType string) arrow.DataType {
	sqlType = strings.ToUpper(sqlType)
	switch {
	case strings.Contains(sqlType, "INT64"), strings.Contains(sqlType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(sqlType, "INT32"), strings.Contains(sqlType, "INTEGER"), strings.Contains(sqlType, "INT"):
		return arrow.PrimitiveTypes.Int64 // Use Int64 for safety
	case strings.Contains(sqlType, "FLOAT"), strings.Contains(sqlType, "DOUBLE"), strings.Contains(sqlType, "REAL"):
		return arrow.PrimitiveTypes.Float64
	case strings.Contains(sqlType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean
	case strings.Contains(sqlType, "TIMESTAMP"), strings.Contains(sqlType, "DATETIME"):
		return arrow.FixedWidthTypes.Timestamp_us
	case strings.Contains(sqlType, "DATE"):
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}

// appendValueToBuilder appends a value to the appropriate Arrow builder
func appendValueToBuilder(builder array.Builder, val interface{}, _ arrow.DataType) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		case float64:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		case int:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		case time.Time:
			b.Append(v.Format(time.RFC3339Nano))
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BooleanBuilder:
		switch v := val.(type) {
		case bool:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixMicro()))
		case string:
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				b.Append(arrow.Timestamp(t.UTC().UnixMicro()))
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}
	case *array.Date32Builder:
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Date32FromTime(v))
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// registerArrowRoutes registers Arrow-specific query endpoints. The Arrow path
// is a user-facing read endpoint and gets the catch-up gate (#392) like the
// JSON paths.
func (h *QueryHandler) registerArrowRoutes(app *fiber.App, readAuth fiber.Handler) {
	app.Post("/api/v1/query/arrow", readAuth, h.checkReplicationReady, h.executeQueryArrow)
}
