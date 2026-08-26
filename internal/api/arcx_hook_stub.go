// No-op arcx hook for builds WITHOUT the arcx engine (no `arcx_engine` tag, or no
// cgo). tryArcxRouter always returns false, so the single call site in handleQuery
// is inert and stock Arc behaves exactly as before — no arcx/arcxrouter/cgo
// dependency is pulled into the build.

//go:build !cgo || !arcx_engine

package api

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
)

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
	return false
}

func (h *QueryHandler) tryArcxRouterArrow(c *fiber.Ctx, execCtx context.Context, cancel context.CancelFunc, rawSQL, headerDB, convertedSQL string) (handled bool) {
	return false
}

// recordArcxShapeCensus is a no-op in the stub build. Deliberately EMPTY — a
// stub that counted would newly link the recognizer into stock Arc, undoing
// the zero-arcx-symbols property the stubs exist to preserve.
func (h *QueryHandler) recordArcxShapeCensus(rawSQL, headerDB string) {}
