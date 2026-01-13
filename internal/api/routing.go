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

// BuildHTTPRequest converts a Fiber context to a net/http Request for forwarding via the router.
// It copies the method, URL, body, and headers from the Fiber request.
func BuildHTTPRequest(c *fiber.Ctx) (*http.Request, error) {
	// Build full URL from Fiber context
	// c.BaseURL() returns scheme://host, c.OriginalURL() returns path + query
	url := c.BaseURL() + c.OriginalURL()

	// Read body - use a bytes.Reader for the body to allow retries in the router
	body := bytes.NewReader(c.Body())

	// Create HTTP request with context
	req, err := http.NewRequestWithContext(c.Context(), c.Method(), url, body)
	if err != nil {
		return nil, err
	}

	// Copy all headers from Fiber request
	c.Request().Header.VisitAll(func(key, value []byte) {
		req.Header.Set(string(key), string(value))
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

// ShouldForwardWrite checks if a write request should be forwarded to another node.
// Returns true if the request should be forwarded (local node cannot handle writes).
// Returns false if:
//   - Router is nil (no clustering)
//   - Request is already forwarded (loop prevention)
//   - Local node can handle writes
func ShouldForwardWrite(router *cluster.Router, c *fiber.Ctx) bool {
	// No router means no clustering - process locally
	if router == nil {
		return false
	}

	// Check for forwarding loop - if X-Arc-Forwarded-By is set, this request was already forwarded
	if c.Get(ForwardedByHeader) != "" {
		return false
	}

	// Check if local node can handle writes
	return !router.CanRouteLocally(true) // isWrite=true
}

// ShouldForwardQuery checks if a query request should be forwarded to another node.
// Returns true if the request should be forwarded (local node cannot handle queries).
// Returns false if:
//   - Router is nil (no clustering)
//   - Request is already forwarded (loop prevention)
//   - Local node can handle queries
func ShouldForwardQuery(router *cluster.Router, c *fiber.Ctx) bool {
	// No router means no clustering - process locally
	if router == nil {
		return false
	}

	// Check for forwarding loop - if X-Arc-Forwarded-By is set, this request was already forwarded
	if c.Get(ForwardedByHeader) != "" {
		return false
	}

	// Check if local node can handle queries
	return !router.CanRouteLocally(false) // isWrite=false
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
