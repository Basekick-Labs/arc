// arcx router hook — tagged wiring that connects the query handler to the
// standalone arcx engine via internal/arcxrouter. Built only with `arcx_engine`;
// without the tag, arcx_hook_stub.go's no-op takes over and stock Arc is
// byte-identical (the single call site in handleQuery compiles to a no-op).
//
// This file — NOT query.go — owns all the arcx wiring (Deps construction, the
// Metrics adapter, mode parsing), so the hot-path query.go gains only one
// tag-neutral method call.

//go:build cgo && arcx_engine

package api

import (
	"bufio"
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/basekick-labs/arc/internal/arcxrouter"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// arcxMode is parsed once from ARC_ROUTER. Default (empty/unknown) is shadow when
// the engine is built in — observe, never serve, until a human sets serve.
var (
	arcxModeOnce sync.Once
	arcxModeVal  arcxrouter.Mode
)

func arcxMode() arcxrouter.Mode {
	arcxModeOnce.Do(func() {
		raw := os.Getenv("ARC_ROUTER")
		arcxModeVal = arcxrouter.ParseMode(raw)
	})
	return arcxModeVal
}

// tryArcxRouter is the handleQuery hook. It decides eligibility on the RAW user
// SQL (rawSQL, before rewriteDateTrunc mangles date_trunc into epoch math) and,
// when eligible, runs the router. Returns handled=true only when the router
// served the response itself (serve mode, green shape); shadow mode and all
// declines return false so the caller's existing DuckDB dispatch runs untouched.
func (h *QueryHandler) tryArcxRouter(
	c *fiber.Ctx,
	ctx context.Context,
	cancel context.CancelFunc,
	start time.Time,
	rawSQL, headerDB, convertedSQL string,
	governanceMaxRows int,
	onComplete func(int),
	onFail func(string),
) (handled bool) {
	mode := arcxMode()
	if mode == arcxrouter.ModeOff {
		return false
	}
	deps := arcxrouter.Deps{
		Storage:      h.storage,
		DB:           h.db,
		Logger:       h.logger,
		Metrics:      arcxMetrics{logger: h.logger},
		Mode:         mode,
		ConvertedSQL: convertedSQL,
		// Capture the per-request governance cap, timeout ctx/cancel, and registry
		// callbacks in the streamer closure so the arcx serve path enforces the SAME
		// cap/timeout and records the SAME metrics as the DuckDB path (a hardcoded 0
		// cap + missing metrics were a governance escape + a registry leak).
		ServeStream: func(fc *fiber.Ctx, reader array.RecordReader, s time.Time) bool {
			return h.serveArcxResult(fc, ctx, cancel, reader, s, governanceMaxRows, onComplete, onFail)
		},
		AllowedDirs: h.db.AllowedDirectories(),
	}
	d := arcxrouter.Decide(rawSQL, headerDB, deps)
	if !d.Eligible {
		return false
	}
	return arcxrouter.Run(c, d, deps, mode, start)
}

// tryArcxRouterArrow is the hook for the raw Arrow-IPC endpoint
// (/api/v1/query/arrow), which has its own handler outside handleQuery. In serve
// mode on a green shape it streams arcx's record as Arrow IPC — arcx's most
// natural output (zero transcode). Returns handled=true only when it served; in
// shadow mode it runs the compare and returns false; declines/errors return false
// so the caller's DuckDB IPC path serves. execCtx must be the caller's
// background-derived context (NOT the pooled Fiber ctx — used inside the async
// stream writer).
func (h *QueryHandler) tryArcxRouterArrow(c *fiber.Ctx, execCtx context.Context, rawSQL, headerDB, convertedSQL string) (handled bool) {
	mode := arcxMode()
	if mode == arcxrouter.ModeOff {
		return false
	}
	deps := arcxrouter.Deps{
		Storage:      h.storage,
		DB:           h.db,
		Logger:       h.logger,
		Metrics:      arcxMetrics{logger: h.logger},
		Mode:         mode,
		ConvertedSQL: convertedSQL,
		AllowedDirs:  h.db.AllowedDirectories(),
	}
	d := arcxrouter.Decide(rawSQL, headerDB, deps)
	if !d.Eligible {
		return false
	}
	reader, served := arcxrouter.RunArrow(execCtx, d, deps, mode)
	if !served {
		return false
	}
	// arcx served a STREAMING reader — write each batch as Arrow IPC (true streaming, no
	// concat). FFI CLIFF: the reader's Release is a no-op and the arcx buffers free on a GC
	// finalizer, so runtime.KeepAlive(reader) as the callback's LAST statement holds them
	// alive across the async write. reader.Err() is checked after the loop so a mid-stream
	// engine error alarms instead of silently truncating.
	schema := reader.Schema()
	c.Set("Content-Type", "application/vnd.apache.arrow.stream")
	// Capture the fasthttp RequestCtx before the async callback; the pooled Fiber
	// *Ctx is recycled after this handler returns (the UAF trap).
	fctx := c.Context()
	fctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		defer runtime.KeepAlive(reader)
		ipcWriter := ipc.NewWriter(w, ipc.WithSchema(schema))
	batchLoop:
		for reader.Next() {
			select {
			case <-execCtx.Done():
				// Labeled break: a bare `break` here would break the SELECT, not the
				// loop, and keep writing batches after a client/timeout cancel.
				h.logger.Warn().Msg("arcx serve (arrow): client/timeout cancel mid-stream")
				break batchLoop
			default:
			}
			batch := reader.Record()
			if batch == nil {
				break
			}
			if err := ipcWriter.Write(batch); err != nil {
				h.logger.Warn().Err(err).Msg("arcx serve (arrow): IPC write failed after headers committed")
				break
			}
		}
		if rerr := reader.Err(); rerr != nil {
			h.logger.Error().Err(rerr).Msg("arcx serve (arrow): stream engine error mid-drain (partial IPC served)")
		}
		if err := ipcWriter.Close(); err != nil {
			h.logger.Warn().Err(err).Msg("arcx serve (arrow): IPC close failed")
		}
		w.Flush()
	})
	return true
}

// serveArcxResult streams a single arcx arrow.Record to the response in the
// request's wire format, reusing Arc's existing Arrow→wire streamers
// (streamArrowJSON / streamMsgPackFromBatches) — no re-implementation of encoding.
// arcx's native output is Arrow, so this is a thin adapter: wrap the one record as
// an array.RecordReader and hand it to the same code the DuckDB Arrow path uses.
//
// MEMORY SAFETY: the record is arcx-owned and the router releases its reference as
// soon as Run returns, but SetBodyStreamWriter runs the encode ASYNCHRONOUSLY
// after return. So we Retain the record here and Release it inside the async
// callback — owning the buffer lifetime across the async boundary. Without the
// Retain, the arcx buffers would be freed while the encoder still reads them (the
// use-after-free the design doc flags as the FFI cliff).
// serveArcxResult streams a STREAMING arcx result (array.RecordReader, un-concatenated
// batches) to the response, reusing Arc's Arrow→wire transcoders. It enforces the same
// governance cap and records the same metrics/registry completion as the DuckDB path.
//
// FFI MEMORY CLIFF: the reader is backed by arcx-owned Arrow buffers imported over the C
// Data Interface. Its Release() is a NO-OP (arrow-go v18); the buffers free on a GC
// FINALIZER. So a reader that goes unreachable while the async encoder still reads is a
// use-after-free (process-fatal, in-process cgo). The load-bearing guarantee is
// runtime.KeepAlive(reader) as the LAST statement of each async callback. `cancel` is the
// request's timeout cancel — owned here (called after the async writer finishes), not by
// the caller, because SetBodyStreamWriter runs after the handler returns.
func (h *QueryHandler) serveArcxResult(
	c *fiber.Ctx,
	streamCtx context.Context,
	cancel context.CancelFunc,
	reader array.RecordReader,
	start time.Time,
	governanceMaxRows int,
	onComplete func(int),
	onFail func(string),
) (handled bool) {
	schema := reader.Schema()
	// `start` is the query's true start (captured BEFORE the engine ran) so
	// execution_time_ms covers engine work, not just serialization.
	timestamp := time.Now().UTC().Format(time.RFC3339)

	if isMsgPackWire(c) {
		// MAJOR-4: msgpack must drain SYNCHRONOUSLY here — BEFORE committing headers —
		// so a mid-drain error is a clean 500, not a truncated 200 (the DuckDB msgpack
		// path's contract). drainArrowBatches enforces the governance cap and Retains
		// each batch (defending the reader's auto-release-on-Next). KeepAlive holds the
		// FFI buffers alive across the drain.
		batches, rowCount, derr := drainArrowBatches(streamCtx, reader, governanceMaxRows)
		runtime.KeepAlive(reader)
		if derr != nil {
			for _, b := range batches {
				b.Release()
			}
			if cancel != nil {
				cancel()
			}
			if onFail != nil {
				onFail(derr.Error())
			}
			h.logger.Error().Err(derr).Msg("arcx serve: msgpack drain error before headers; 500")
			_ = respondError(c, fiber.StatusInternalServerError, derr.Error(), timestamp, start)
			return true // we produced the (error) response
		}
		c.Set(fiber.HeaderContentType, msgpackContentType)
		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			defer func() {
				for _, b := range batches {
					b.Release()
				}
				if cancel != nil {
					cancel()
				}
			}()
			bw := bufio.NewWriterSize(w, 256*1024)
			if _, serr := streamMsgPackFromBatches(streamCtx, bw, schema, batches, rowCount, nil, start, timestamp); serr != nil {
				h.logger.Warn().Err(serr).Msg("arcx serve: msgpack stream error after headers committed")
			}
			bw.Flush()
			w.Flush()
			if onComplete != nil {
				onComplete(rowCount)
			}
		})
		return true
	}

	// JSON: streamArrowJSON consumes the reader directly (true streaming — pulls batches
	// as it encodes, so the concat is skipped). It enforces the governance cap and returns
	// the row count; we check reader.Err() after it to catch a mid-stream engine error
	// (else a truncated result serves as success — gotcha #4).
	c.Set("Content-Type", "application/json")
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer func() {
			if cancel != nil {
				cancel()
			}
			// LAST: keep the FFI-backed reader reachable until after the encoder's final
			// read. Release is a no-op; the finalizer frees the arcx buffers.
			runtime.KeepAlive(reader)
		}()
		rc, serr := streamArrowJSON(streamCtx, w, reader, governanceMaxRows, nil, start, timestamp)
		if serr != nil {
			h.logger.Warn().Err(serr).Msg("arcx serve: json stream error after headers committed")
			if onFail != nil {
				onFail(serr.Error())
			}
		} else if rerr := reader.Err(); rerr != nil {
			// A mid-stream engine error surfaced via the stream errno: Next() ended early,
			// indistinguishable from clean EOF at the transcoder. Alarm — never serve
			// partial-as-success silently.
			h.logger.Error().Err(rerr).Msg("arcx serve: json stream engine error mid-drain (partial result served)")
			if onFail != nil {
				onFail(rerr.Error())
			}
		} else if onComplete != nil {
			onComplete(rc)
		}
		w.Flush()
	})
	return true
}

// arcxMetrics is a logger-backed implementation of the router's Metrics interface.
// Phase 1 shadow observability rides on structured logs (the ERROR-level mismatch
// alarm is the load-bearing signal); wiring dedicated Prometheus counters into
// internal/metrics is a follow-up that would touch the shared metrics struct, so
// it's deliberately deferred to keep this change isolated.
type arcxMetrics struct {
	logger zerolog.Logger
}

func (m arcxMetrics) ArcxShadowMatch(shape string) {
	m.logger.Debug().Str("shape", shape).Msg("arcx shadow: match")
}
func (m arcxMetrics) ArcxShadowMismatch(shape string) {
	m.logger.Warn().Str("shape", shape).Msg("arcx shadow: mismatch metric")
}
func (m arcxMetrics) ArcxShadowError(shape string) {
	m.logger.Warn().Str("shape", shape).Msg("arcx shadow: error metric")
}
func (m arcxMetrics) ArcxShadowDeclined(shape string) {
	m.logger.Debug().Str("shape", shape).Msg("arcx shadow: declined")
}
func (m arcxMetrics) ArcxLatency(engine, shape string, micros int64) {
	m.logger.Debug().Str("engine", engine).Str("shape", shape).Int64("micros", micros).Msg("arcx latency")
}
