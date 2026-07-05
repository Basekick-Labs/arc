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
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
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
func (h *QueryHandler) tryArcxRouter(c *fiber.Ctx, rawSQL, headerDB, convertedSQL string) (handled bool) {
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
		ServeStream:  h.serveArcxResult,
	}
	d := arcxrouter.Decide(rawSQL, headerDB, deps)
	if !d.Eligible {
		return false
	}
	return arcxrouter.Run(c, d, deps, mode)
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
	}
	d := arcxrouter.Decide(rawSQL, headerDB, deps)
	if !d.Eligible {
		return false
	}
	rec, served := arcxrouter.RunArrow(execCtx, d, deps, mode)
	if !served {
		return false
	}
	// arcx served a record — stream it as Arrow IPC. We own rec and must Release
	// it after the async stream writer finishes.
	schema := rec.Schema()
	c.Set("Content-Type", "application/vnd.apache.arrow.stream")
	// Capture the fasthttp RequestCtx before the async callback; the pooled Fiber
	// *Ctx is recycled after this handler returns (the UAF trap).
	fctx := c.Context()
	fctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		defer rec.Release()
		ipcWriter := ipc.NewWriter(w, ipc.WithSchema(schema))
		if err := ipcWriter.Write(rec); err != nil {
			h.logger.Warn().Err(err).Msg("arcx serve (arrow): IPC write failed after headers committed")
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
func (h *QueryHandler) serveArcxResult(c *fiber.Ctx, rec arrow.Record) (handled bool) {
	schema := rec.Schema()
	start := time.Now()
	timestamp := time.Now().UTC().Format(time.RFC3339)

	// Capture the context BEFORE SetBodyStreamWriter. The Fiber *Ctx is recycled
	// once the handler returns; touching c.UserContext() inside the async stream
	// callback is a use-after-free (the "Fiber context not safe in callbacks"
	// trap the DuckDB path avoids the same way). The captured context.Context is
	// safe to close over.
	streamCtx := c.UserContext()

	// Retain for the async writer; the router's defer releases its own reference.
	// Released inside each stream callback below.
	rec.Retain()

	if isMsgPackWire(c) {
		batches := []arrow.Record{rec}
		rowCount := int(rec.NumRows())
		c.Set(fiber.HeaderContentType, msgpackContentType)
		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			defer rec.Release()
			bw := bufio.NewWriterSize(w, 256*1024)
			if _, serr := streamMsgPackFromBatches(streamCtx, bw, schema, batches, rowCount, nil, start, timestamp); serr != nil {
				h.logger.Warn().Err(serr).Msg("arcx serve: msgpack stream error after headers committed")
			}
			bw.Flush()
			w.Flush()
		})
		return true
	}

	// JSON: the streamer consumes an array.RecordReader. NewRecordReader retains
	// the record, so we release our extra Retain immediately and let the reader
	// own the record; releasing the reader in the callback frees it.
	reader, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		rec.Release()
		h.logger.Error().Err(err).Msg("arcx serve: failed to wrap record as reader; falling back")
		return false
	}
	rec.Release() // reader holds its own ref now

	c.Set("Content-Type", "application/json")
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		defer reader.Release()
		if _, serr := streamArrowJSON(streamCtx, w, reader, 0, nil, start, timestamp); serr != nil {
			h.logger.Warn().Err(serr).Msg("arcx serve: json stream error after headers committed")
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
