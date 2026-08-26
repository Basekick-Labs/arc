// No-op router for builds WITHOUT the arcx engine linked in (no `arcx_engine`
// tag, or no cgo). Keeps stock Arc byte-identical: Decide always declines and Run
// never handles, so the two-line hook in handleQuery is completely inert and no
// arcx/FFI dependency is pulled into the build.

//go:build !cgo || !arcx_engine

package arcxrouter

import (
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gofiber/fiber/v2"
)

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
//
// The signature MUST track the tagged build's exactly. It had drifted (missing
// `start`) and compiled only because nothing in the untagged build calls it — so the
// break would have surfaced whenever someone first wired the stub, far from the cause.
func Run(c *fiber.Ctx, d Decision, h Handler, mode Mode, start time.Time) (handled bool) {
	return false
}

// RunArrow never serves in the stub build. Was missing entirely; same drift risk as Run.
func RunArrow(ctx context.Context, d Decision, h Handler, mode Mode) (reader array.RecordReader, served bool) {
	return nil, false
}
