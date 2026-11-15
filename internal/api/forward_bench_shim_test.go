package api

import (
	"net/http"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/gofiber/fiber/v2"
)

// buildForwardForBench is the forward setup as a forwarded query does it
// (forwardRead): the forward's context, then the request on it.
func buildForwardForBench(c *fiber.Ctx, router *cluster.Router) (*http.Request, func(), error) {
	ctx, cancel := forwardContext(router)
	req, release, err := BuildHTTPRequest(c, ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return req, func() { release(); cancel() }, nil
}
