// No-op router for builds WITHOUT the arcx engine linked in (no `arcx_engine`
// tag, or no cgo). Keeps stock Arc byte-identical: Decide always declines and Run
// never handles, so the two-line hook in handleQuery is completely inert and no
// arcx/FFI dependency is pulled into the build.

//go:build !cgo || !arcx_engine

package arcxrouter

import "github.com/gofiber/fiber/v2"

// Handler is the subset of the query handler the router needs. In the stub build
// it is unconstrained (any type) since Decide/Run ignore it — declared as `any`
// so the call site compiles identically to the tagged build.
type Handler = any

// Decision mirrors the tagged build's type so the call site compiles identically.
type Decision struct {
	Eligible bool
}

// Decide always declines in the stub build — the caller runs DuckDB as today.
func Decide(sql, headerDB string, h Handler) Decision { return Decision{} }

// Run never handles in the stub build; the caller falls through to DuckDB.
func Run(c *fiber.Ctx, d Decision, h Handler, mode Mode) (handled bool) { return false }
