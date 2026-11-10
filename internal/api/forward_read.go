package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Forwarding the reads of a node that cannot serve queries (a compactor).
//
// /api/v1/query and /query/msgpack have always been forwarded; #1567 added
// /query/arrow. Every other read a compactor received it answered itself from
// its own storage and field schema, which breaks the role isolation the query
// routes respect and, with peer replication, can lag the readers. These are
// now forwarded too, buffered (their answers are small JSON documents), only
// to peers that advertise cluster.CapabilityForwardReadQueries:
//
//	POST /api/v1/query/estimate
//	GET  /api/v1/query/:measurement
//	GET  /api/v1/measurements
//	GET  /api/v1/databases/:database/measurements/:measurement/schema
//	GET  /api/v1/databases, /api/v1/databases/:name, /api/v1/databases/:name/measurements
//
// With peers but none capable (a rolling upgrade) the read is answered here,
// as before; with no peer at all it is 503, as /api/v1/query answers.

// forwardReadOpts configures forwardRead.
type forwardReadOpts struct {
	// route sends the request: RouteQuery for the routes every build accepts
	// (/query, /query/msgpack), RouteReadQuery for the rest.
	route func(context.Context, *http.Request) (*http.Response, error)
	// timeout bounds the whole forward (listings); 0 = no bound here.
	timeout time.Duration
	// headerBound bounds only the wait for the answer to start (query
	// routes, whose answer the peer bounds itself); 0 = none.
	headerBound time.Duration
	// registry and description register the forward in this node's query
	// registry, so /api/v1/queries lists and cancels what it forwarded.
	// nil registry or empty description: not registered.
	registry    *queryregistry.Registry
	description string
	// watch, when set, cancels the forward if the client disconnects
	// (QueryHandler.watchQueryClientDisconnect).
	watch func(ctx context.Context, c *fiber.Ctx, queryID string, cancel context.CancelFunc)
	// onError counts a failed forward (metrics); may be nil.
	onError func()
	logger  zerolog.Logger
}

// forwardRead forwards c's request when this node cannot serve it locally.
// handled is false when it should be answered here: this node can serve it,
// or no peer accepts it (cluster.ErrNoCapablePeer).
func forwardRead(c *fiber.Ctx, router *cluster.Router, o forwardReadOpts) (handled bool, err error) {
	switch QueryForwardDecision(router, c) {
	case ForwardLocal:
		return false, nil
	case ForwardAlreadyForwarded:
		if o.onError != nil {
			o.onError()
		}
		return true, RespondAlreadyForwarded(c)
	}

	httpReq, err := BuildHTTPRequest(c)
	if err != nil {
		o.logger.Error().Err(err).Msg("Failed to build HTTP request for forwarding")
		if o.onError != nil {
			o.onError()
		}
		return true, c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to prepare request for forwarding",
		})
	}

	// The request's context carries the forwarded identity; every bound and
	// cancellation is layered on it.
	ctx, cancel := context.WithCancel(httpReq.Context())
	defer cancel()
	if o.timeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeout(ctx, o.timeout)
		defer cancelTimeout()
	}
	queryID := ""
	if o.registry != nil && o.description != "" {
		var tokenID int64
		var tokenName string
		if info := auth.GetTokenInfo(c); info != nil {
			tokenID, tokenName = info.ID, info.Name
		}
		// Register returns a context the registry can cancel
		// (DELETE /api/v1/queries/:id): the forward runs on it.
		queryID, ctx = o.registry.Register(ctx, o.description, tokenID, tokenName, c.IP())
		id := queryID
		ctx = cluster.WithForwardTargetHook(ctx, func(node string) { o.registry.SetForwardedTo(id, node) })
		if o.watch != nil {
			o.watch(ctx, c, queryID, cancel)
		}
	}
	var headerTimer *time.Timer
	if o.headerBound > 0 {
		headerTimer = time.AfterFunc(o.headerBound, cancel)
	}

	resp, err := o.route(ctx, httpReq.WithContext(ctx))
	if headerTimer != nil && !headerTimer.Stop() && err == nil {
		_ = resp.Body.Close()
		err = context.DeadlineExceeded
	}
	switch {
	case errors.Is(err, cluster.ErrLocalNodeCanHandle):
		if queryID != "" {
			o.registry.Discard(queryID)
		}
		return false, nil
	case errors.Is(err, cluster.ErrNoCapablePeer):
		if queryID != "" {
			o.registry.Discard(queryID)
		}
		o.logger.Warn().Str("path", c.Path()).
			Msg("No peer accepts this forwarded read (rolling upgrade); answering it on this node")
		return false, nil
	case err != nil:
		if queryID != "" {
			o.registry.Fail(queryID, "forward failed")
		}
		o.logger.Error().Err(err).Str("path", c.Path()).Msg("Failed to forward a read request")
		if o.onError != nil {
			o.onError()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return true, c.Status(fiber.StatusGatewayTimeout).JSON(fiber.Map{
				"success": false,
				"error":   "Timed out waiting for the forwarded request",
			})
		}
		return true, HandleRoutingError(c, err)
	}

	status := resp.StatusCode
	err = CopyResponse(c, resp)
	if queryID != "" {
		switch {
		case err != nil:
			o.registry.Fail(queryID, "forwarded response failed: "+err.Error())
		case status >= 400:
			o.registry.Fail(queryID, http.StatusText(status))
		default:
			// No row count: the rows were counted on the peer.
			o.registry.Complete(queryID, 0)
		}
	}
	return true, err
}
