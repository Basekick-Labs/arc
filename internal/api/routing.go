package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/sharding"
	"github.com/gofiber/fiber/v2"
)

// ForwardedByHeader is the header used to detect forwarded requests and prevent routing loops.
const ForwardedByHeader = "X-Arc-Forwarded-By"

// hopByHopHeaders are headers that should not be copied when forwarding responses.
// These are connection-specific and shouldn't be passed through proxies.
var hopByHopHeaders = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailers":            true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// isHopByHop returns true if the header is a hop-by-hop header that shouldn't be forwarded.
func isHopByHop(header string) bool {
	return hopByHopHeaders[header]
}

// clientForwardingHeaders are client-controlled forwarding / identity
// headers that MUST NOT be propagated verbatim from an inbound client
// request onto an inter-node forward. The forwarding node re-establishes
// the trustworthy values itself (internal/cluster/router.go Sets
// X-Forwarded-For, X-Arc-Forwarded-By, X-Arc-Original-Host from the socket
// peer and local node identity), so passing the client's copies through
// would let a caller inject spoofed values a peer might log or, in future
// code, trust — the same failure class as CVE-2026-45045 (X-Real-IP
// spoofing). Arc trusts none of these on the receive side today (identity
// is derived only from the socket via fiber c.IP()); stripping them here
// is defense-in-depth that keeps that property true regardless of what a
// downstream node later chooses to read.
//
// Keys are in http.CanonicalHeaderKey form (the VisitAll callback
// canonicalises before lookup).
// If future receive-side code ever derives client identity from a header
// (a custom Fiber ProxyHeader, or a CDN client-IP header), that header MUST
// be added here so a client cannot pre-seed it across a forward hop.
var clientForwardingHeaders = map[string]bool{
	"X-Real-Ip":           true, // canonical form of X-Real-IP
	"X-Forwarded-For":     true,
	"X-Forwarded-Host":    true,
	"X-Forwarded-Proto":   true,
	"X-Forwarded-Port":    true,
	"Forwarded":           true, // RFC 7239
	"X-Arc-Forwarded-By":  true, // internal loop marker; only the peer may set it
	"X-Arc-Original-Host": true,
	"X-Arc-Shard-Routed":  true, // internal shard-loop marker
	// CDN / proxy client-IP headers. Not read by Arc today (identity is
	// socket-only), stripped defensively so they cannot be trusted later.
	"True-Client-Ip":   true, // canonical form of True-Client-IP (Akamai/Cloudflare)
	"Cf-Connecting-Ip": true, // canonical form of CF-Connecting-IP (Cloudflare)
	"X-Client-Ip":      true, // canonical form of X-Client-IP
}

// isClientForwardingHeader reports whether a canonicalised header key is a
// client-controlled forwarding/identity header stripped at the forward
// boundary. See clientForwardingHeaders.
func isClientForwardingHeader(canonicalKey string) bool {
	return clientForwardingHeaders[canonicalKey]
}

// BuildHTTPRequest converts a Fiber context to a net/http Request for
// forwarding via the router.
//
// Lifecycle: every caller in this codebase (lineprotocol.go, msgpack.go,
// tle.go, query.go, routing.go's RouteShardedWrite/Query) invokes the
// returned *http.Request synchronously within the Fiber handler — they
// pass it to router.RouteWrite/RouteQuery (which blocks on
// http.Client.Do, and internally already buffers the body via io.ReadAll
// for retry support, see internal/cluster/router.go forwardRequest) or
// to shardRouter.RouteWrite/RouteQuery, then read+close the response
// body inside CopyResponse, then return. fasthttp does not recycle the
// RequestCtx until after the handler returns (fasthttp server.go
// releaseCtx is post-handler), so wrapping c.Body() in bytes.NewReader
// directly is safe and avoids a copy that can be large at max payload
// (up to 1GB). Using c.Context() (returns *fasthttp.RequestCtx which
// implements context.Context) is also correct here — it propagates
// client-disconnect cancellation to the forwarded request, which
// c.UserContext() (returns context.Background()) does NOT.
//
// If a future refactor moves any of this work to a background goroutine
// or stream writer, those properties no longer hold and the caller
// must take a defensive copy + use a non-pooled context. Today's
// callers are all synchronous; revisit this if that changes.
func BuildHTTPRequest(c *fiber.Ctx) (*http.Request, error) {
	// Build full URL from Fiber context
	// c.BaseURL() returns scheme://host, c.OriginalURL() returns path + query
	url := c.BaseURL() + c.OriginalURL()

	// Wrap fasthttp's body slice directly — synchronous-handler lifecycle
	// argument above. Router.forwardRequest then io.ReadAll's it into its
	// own buffer for retry support before issuing http.Client.Do.
	body := bytes.NewReader(c.Body())

	// c.Context() propagates client-disconnect cancellation to the
	// forwarded request (so the backend stops processing if the original
	// client gave up). c.UserContext() would default to context.Background
	// and lose that signal.
	req, err := http.NewRequestWithContext(c.Context(), c.Method(), url, body)
	if err != nil {
		return nil, err
	}

	// Copy headers from Fiber request, filtering connection-specific
	// (hop-by-hop) headers + Content-Length per RFC 7230 §6.1.
	//
	// Filtered headers:
	//   - hop-by-hop set (Connection, Keep-Alive, Proxy-Authenticate,
	//     Proxy-Authorization, Te, Trailers, Transfer-Encoding,
	//     Upgrade): these are connection-specific and MUST NOT be
	//     forwarded by intermediaries. CopyResponse already filters
	//     them on the response path via the same isHopByHop helper;
	//     the request path was missing this filter.
	//   - Content-Length: net/http sets req.ContentLength from the
	//     body (it knows the body is *bytes.Reader and gets its
	//     length), then writes Content-Length on the wire from
	//     ContentLength. Forwarding a Content-Length from the
	//     upstream header would duplicate the header or send a stale
	//     value mismatched with the actual body size.
	//
	// Add (not Set) preserves multi-value headers: fasthttp emits a
	// separate VisitAll callback per value, so Set would overwrite
	// earlier values and only the last would forward. Affects Via,
	// Accept (multiple content types), and similar. (Client forwarding
	// headers like X-Forwarded-For are stripped below, so they are not
	// among the multi-value headers preserved here.)
	//
	// http.CanonicalHeaderKey before isHopByHop lookup: fasthttp
	// normalises header keys to canonical form by default, but a
	// future config change or fasthttp behavior shift could leave
	// non-canonical keys leaking through. Canonicalising defensively
	// also matches CopyResponse's behavior (Go's http.Header map keys
	// are always canonical-cased).
	// Direct map-write (rather than req.Header.Add) bypasses Add's
	// internal CanonicalHeaderKey re-canonicalisation — k is already
	// canonical from the line above, so re-canonicalising would be a
	// wasted string scan + allocation per header.
	//
	// Host filter: net/http's Request.Write skips the Host header in the
	// header map and writes Host from req.Host instead (req.go's
	// reqWriteExcludeHeader), so leaving it in req.Header doesn't leak
	// it onto the wire — but other code (middleware, logging, custom
	// transports) that reads req.Header.Get("Host") would see the
	// upstream client's Host instead of the target peer's. Filtering
	// keeps the request struct's two notions of "host" consistent.
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := http.CanonicalHeaderKey(string(key))
		if isHopByHop(k) || k == "Content-Length" || k == "Host" {
			return
		}
		// Strip client-controlled forwarding/identity headers. The
		// forwarding node re-establishes the trustworthy values itself
		// (router.go), so a client cannot inject spoofed X-Real-IP /
		// X-Forwarded-* / X-Arc-* onto the inter-node hop. Same class as
		// CVE-2026-45045. See clientForwardingHeaders.
		if isClientForwardingHeader(k) {
			return
		}
		req.Header[k] = append(req.Header[k], string(value))
	})

	// Set remote address for X-Forwarded-For handling in router
	req.RemoteAddr = c.IP()

	return req, nil
}

// CopyResponse writes an HTTP response from the router to a Fiber context.
// It copies the status code, headers (excluding hop-by-hop), and body.
func CopyResponse(c *fiber.Ctx, resp *http.Response) error {
	defer resp.Body.Close()

	// Set status code
	c.Status(resp.StatusCode)

	// Copy headers (excluding hop-by-hop headers)
	for key, values := range resp.Header {
		if !isHopByHop(key) {
			for _, v := range values {
				c.Set(key, v)
			}
		}
	}

	// Copy body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return c.Send(body)
}

// ForwardDecision is the outcome of the pre-handler routing check. It
// separates the two reasons a request is not forwarded — "handle it here"
// versus "it was already forwarded and this node still cannot serve it" —
// so the second case produces a deterministic error instead of silently
// falling through to a local path that is structurally guaranteed to fail.
//
// Splitting these two cases is also what makes the X-Arc-Forwarded-By loop
// guard non-abusable. The header is client-settable (it survives at the
// HTTP boundary and is only overwritten on the *outbound* forward in
// internal/cluster/router.go), so an authenticated caller can set it on a
// direct request. Previously that collapsed into "do not forward" and the
// node then attempted local processing it could not complete — a
// client-triggered routing bypass / self-inflicted failure. Now a present
// header on a node that cannot route locally yields ForwardAlreadyForwarded,
// which the handler answers with a clear 508-style error. A genuine
// peer->peer loop terminates the same way. On a node that *can* serve
// locally the header is irrelevant and ignored entirely (the common case).
type ForwardDecision int

const (
	// ForwardLocal: handle the request on this node.
	ForwardLocal ForwardDecision = iota
	// ForwardToPeer: forward the request to another node.
	ForwardToPeer
	// ForwardAlreadyForwarded: the request carries the forwarded-by marker
	// but this node cannot serve it locally — a loop or a spoofed header.
	// The handler must return an error, never fall through to local.
	ForwardAlreadyForwarded
)

// decideForward centralizes the routing decision for both writes and
// queries. isWrite selects the capability checked on the local node.
func decideForward(router *cluster.Router, c *fiber.Ctx, isWrite bool) ForwardDecision {
	// No router means no clustering - process locally.
	if router == nil {
		return ForwardLocal
	}

	// A node that can serve this request type locally always does so.
	// The X-Arc-Forwarded-By header is irrelevant here and is NOT
	// consulted — this keeps a client-supplied value from having any
	// effect on the common (capable-node) path.
	if router.CanRouteLocally(isWrite) {
		return ForwardLocal
	}

	// This node cannot serve locally. If the request is already marked as
	// forwarded, forwarding it again would loop; and because the marker is
	// client-settable, honoring it as "handle locally" would let a caller
	// force a doomed local path. Surface a deterministic error instead.
	if c.Get(ForwardedByHeader) != "" {
		return ForwardAlreadyForwarded
	}

	return ForwardToPeer
}

// ShouldForwardWrite reports whether a write request should be forwarded to
// another node. It returns true only for ForwardToPeer; ForwardLocal and
// ForwardAlreadyForwarded both return false. Callers that need to
// distinguish the already-forwarded case (to emit an error rather than fall
// through to local processing) should use WriteForwardDecision instead.
func ShouldForwardWrite(router *cluster.Router, c *fiber.Ctx) bool {
	return decideForward(router, c, true) == ForwardToPeer
}

// ShouldForwardQuery reports whether a query request should be forwarded to
// another node. See ShouldForwardWrite for the return-value semantics.
func ShouldForwardQuery(router *cluster.Router, c *fiber.Ctx) bool {
	return decideForward(router, c, false) == ForwardToPeer
}

// WriteForwardDecision returns the full routing decision for a write.
func WriteForwardDecision(router *cluster.Router, c *fiber.Ctx) ForwardDecision {
	return decideForward(router, c, true)
}

// QueryForwardDecision returns the full routing decision for a query.
func QueryForwardDecision(router *cluster.Router, c *fiber.Ctx) ForwardDecision {
	return decideForward(router, c, false)
}

// RespondAlreadyForwarded writes the error returned when a node receives a
// request marked as already-forwarded that it cannot serve locally (a
// routing loop or a spoofed X-Arc-Forwarded-By header). Uses 508 Loop
// Detected so the condition is distinguishable from a transient 503.
func RespondAlreadyForwarded(c *fiber.Ctx) error {
	return c.Status(fiber.StatusLoopDetected).JSON(fiber.Map{
		"error": "request already forwarded and cannot be served by this node",
	})
}

// HandleRoutingError returns an appropriate error response for routing failures.
func HandleRoutingError(c *fiber.Ctx, err error) error {
	switch err {
	case cluster.ErrNoWriterAvailable:
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "No writer node available in the cluster",
		})
	case cluster.ErrNoReaderAvailable:
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "No reader node available in the cluster",
		})
	case cluster.ErrRoutingFailed:
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": "Failed to route request to cluster node",
		})
	default:
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": "Routing error: " + err.Error(),
		})
	}
}

// ShardRoutedHeader indicates a request has been routed by the shard router.
const ShardRoutedHeader = "X-Arc-Shard-Routed"

// RouteShardedWrite routes a write request using the shard router.
// Returns:
//   - (nil, nil) if request should be handled locally
//   - (*http.Response, nil) if request was forwarded successfully
//   - (nil, error) if routing failed
func RouteShardedWrite(shardRouter *sharding.ShardRouter, c *fiber.Ctx) (*http.Response, error) {
	if shardRouter == nil {
		return nil, nil // No shard router - handle locally
	}

	// Check if already shard-routed (prevent loops)
	if c.Get(ShardRoutedHeader) != "" {
		return nil, nil // Already routed - handle locally
	}

	// Build HTTP request for forwarding
	req, err := BuildHTTPRequest(c)
	if err != nil {
		return nil, err
	}

	// Route via shard router
	resp, err := shardRouter.RouteWrite(c.Context(), req)
	if err != nil {
		if errors.Is(err, sharding.ErrLocalNodeCanHandle) {
			return nil, nil // Handle locally
		}
		return nil, err
	}

	return resp, nil
}

// RouteShardedQuery routes a query request using the shard router.
// Returns:
//   - (nil, nil) if request should be handled locally
//   - (*http.Response, nil) if request was forwarded successfully
//   - (nil, error) if routing failed
func RouteShardedQuery(shardRouter *sharding.ShardRouter, database string, c *fiber.Ctx) (*http.Response, error) {
	if shardRouter == nil {
		return nil, nil // No shard router - handle locally
	}

	// Check if already shard-routed (prevent loops)
	if c.Get(ShardRoutedHeader) != "" {
		return nil, nil // Already routed - handle locally
	}

	// Build HTTP request for forwarding
	req, err := BuildHTTPRequest(c)
	if err != nil {
		return nil, err
	}

	// Route via shard router
	resp, err := shardRouter.RouteQuery(c.Context(), database, req)
	if err != nil {
		if errors.Is(err, sharding.ErrLocalNodeCanHandle) {
			return nil, nil // Handle locally
		}
		return nil, err
	}

	return resp, nil
}

// HandleShardRoutingError returns an appropriate error response for shard routing failures.
func HandleShardRoutingError(c *fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, sharding.ErrNoDatabaseHeader):
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Missing X-Arc-Database header required for sharded routing",
		})
	case errors.Is(err, sharding.ErrNoShardPrimary):
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "No primary node available for this shard",
		})
	case errors.Is(err, sharding.ErrShardingDisabled):
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Sharding is not enabled on this cluster",
		})
	default:
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": "Shard routing error: " + err.Error(),
		})
	}
}
