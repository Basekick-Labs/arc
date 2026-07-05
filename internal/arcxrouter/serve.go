// Serve-mode result streaming. Tagged.
//
// Phase 1's deliverable is SHADOW mode (run arcx, compare, serve DuckDB). Serve
// mode — actually serving arcx's result to the client — is deliberately deferred
// to a focused follow-up once shadow data proves a shape green over real traffic.
// Serving requires reproducing Arc's exact typed-JSON/msgpack wire shape
// (QueryResponse: columns, typed data rows, row_count, timing) from the
// arrow.Record; doing that half-way would risk a wire-format regression for the
// very shapes we're trying to accelerate. So for now streamArcxResult declines,
// which makes ModeServe fall back to DuckDB exactly like a shape arcx can't do —
// safe, and honest about what's built. The router's shape/eligibility/compare
// spine is complete and testable without it.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gofiber/fiber/v2"
)

// streamArcxResult writes rec to the response as Arc's typed JSON. Returns
// handled=false for now (serve streaming is a deferred follow-up) so ModeServe
// falls back to DuckDB. The signature is final; only the body is a stub.
func streamArcxResult(_ *fiber.Ctx, _ arrow.Record) (handled bool) {
	return false
}
