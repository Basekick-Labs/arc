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
	"os"
	"sync"

	"github.com/basekick-labs/arc/internal/arcxrouter"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// arcxMode is parsed once from ARCX_ROUTER. Default (empty/unknown) is shadow when
// the engine is built in — observe, never serve, until a human sets serve.
var (
	arcxModeOnce sync.Once
	arcxModeVal  arcxrouter.Mode
)

func arcxMode() arcxrouter.Mode {
	arcxModeOnce.Do(func() {
		arcxModeVal = arcxrouter.ParseMode(os.Getenv("ARCX_ROUTER"))
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
	}
	d := arcxrouter.Decide(rawSQL, headerDB, deps)
	if !d.Eligible {
		return false
	}
	return arcxrouter.Run(c, d, deps, mode)
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
