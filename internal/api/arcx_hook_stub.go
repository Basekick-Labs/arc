// No-op arcx hook for builds WITHOUT the arcx engine (no `arcx_engine` tag, or no
// cgo). tryArcxRouter always returns false, so the single call site in handleQuery
// is inert and stock Arc behaves exactly as before — no arcx/arcxrouter/cgo
// dependency is pulled into the build.

//go:build !cgo || !arcx_engine

package api

import (
	"context"

	"github.com/gofiber/fiber/v2"
)

func (h *QueryHandler) tryArcxRouter(c *fiber.Ctx, rawSQL, headerDB, convertedSQL string) (handled bool) {
	return false
}

func (h *QueryHandler) tryArcxRouterArrow(c *fiber.Ctx, execCtx context.Context, rawSQL, headerDB, convertedSQL string) (handled bool) {
	return false
}
