package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/sharding"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsHopByHop(t *testing.T) {
	tests := []struct {
		header   string
		expected bool
	}{
		{"Connection", true},
		{"Keep-Alive", true},
		{"Transfer-Encoding", true},
		{"Content-Type", false},
		{"Content-Length", false},
		{"X-Custom-Header", false},
		{"X-Arc-Forwarded-By", false},
	}

	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			result := isHopByHop(tt.header)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildHTTPRequest(t *testing.T) {
	app := fiber.New()

	app.Post("/api/v1/write/msgpack", func(c *fiber.Ctx) error {
		// Build HTTP request from Fiber context
		req, err := BuildHTTPRequest(c)
		require.NoError(t, err)

		// Verify method
		assert.Equal(t, "POST", req.Method)

		// Verify URL path is preserved
		assert.Contains(t, req.URL.String(), "/api/v1/write/msgpack")

		// Verify headers are copied
		assert.Equal(t, "application/msgpack", req.Header.Get("Content-Type"))
		assert.Equal(t, "test-database", req.Header.Get("X-Arc-Database"))

		return c.SendStatus(fiber.StatusOK)
	})

	// Create test request
	req := httptest.NewRequest("POST", "/api/v1/write/msgpack?foo=bar", nil)
	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set("X-Arc-Database", "test-database")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
}

// TestBuildHTTPRequest_PreservesMultiValueHeaders pins the
// req.Header.Add (not Set) behaviour: when a Fiber/fasthttp request
// has multiple values for the same header key, fasthttp's VisitAll
// calls the callback once per value. Using Set inside that callback
// overwrites earlier values and only the last forwards. Add preserves
// all of them.
//
// Cookie is special-cased in HTTP/1.1 (joined into one comma- or
// semicolon-separated value per RFC 6265), so we use X-Forwarded-For
// — a legitimately multi-valued header where each proxy hop appends
// its own value, and dropping intermediate hops corrupts the chain
// that downstream services depend on (rate limiting, geo, audit).
// Also tests Via, another standard multi-value request header.
//
// Verified to fail when req.Header.Add is reverted to Set:
// req.Header.Values(...) returns only the last value.
func TestBuildHTTPRequest_PreservesMultiValueHeaders(t *testing.T) {
	app := fiber.New()

	var capturedXFF, capturedVia []string

	app.Get("/test", func(c *fiber.Ctx) error {
		req, err := BuildHTTPRequest(c)
		require.NoError(t, err)
		capturedXFF = req.Header.Values("X-Forwarded-For")
		capturedVia = req.Header.Values("Via")
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	// X-Forwarded-For: three proxy hops. As a client-controlled
	// forwarding header it is now STRIPPED at the forward boundary
	// (CVE-2026-45045 class hardening) — the forwarding node re-sets a
	// single trusted value from the socket peer in router.go. See
	// clientForwardingHeaders.
	// Via: two proxies in the chain — NOT a forwarding/identity header,
	// so it must still be preserved multi-value and in order.
	req.Header.Add("X-Forwarded-For", "203.0.113.1")
	req.Header.Add("X-Forwarded-For", "198.51.100.7")
	req.Header.Add("X-Forwarded-For", "192.0.2.42")
	req.Header.Add("Via", "1.1 proxy1.example.com")
	req.Header.Add("Via", "1.1 proxy2.example.com")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	// Client-supplied X-Forwarded-For must not survive onto the
	// inter-node forward — the forwarding node establishes the trusted
	// value itself.
	assert.Empty(t, capturedXFF,
		"client X-Forwarded-For must be stripped at the forward boundary")

	// Equal (not ElementsMatch): order matters for Via — it represents
	// the sequential path of proxy hops. Multi-value preservation for
	// non-forwarding headers is unchanged (Gemini PR #463 round 7).
	assert.Equal(t,
		[]string{"1.1 proxy1.example.com", "1.1 proxy2.example.com"}, capturedVia,
		"Via values not preserved in chain order")
}

// TestBuildHTTPRequest_FiltersHopByHopAndContentLength pins the
// RFC 7230 §6.1 hop-by-hop filter on the request-forwarding path.
// Connection-specific headers (Connection, Keep-Alive, Proxy-Auth*,
// Te, Trailers, Transfer-Encoding, Upgrade) MUST NOT be forwarded
// by intermediaries — they describe the upstream connection, not
// end-to-end semantics. Forwarding them produces protocol violations
// (e.g. a Transfer-Encoding: chunked header copied onto a request
// the Go HTTP client is sending with a known Content-Length).
//
// Content-Length is filtered separately because net/http sets
// req.ContentLength from the body (*bytes.Reader knows its length)
// and writes the header from that field. Forwarding the upstream
// Content-Length would either duplicate the header on the wire or
// send a stale value if the body was somehow transformed.
//
// CopyResponse already filters these on the response path via the
// same isHopByHop helper; this test pins that the request path now
// does the same. Headers that should pass through (Authorization,
// Content-Type, X-Custom-*, X-Arc-Database) are also asserted.
func TestBuildHTTPRequest_FiltersHopByHopAndContentLength(t *testing.T) {
	app := fiber.New()

	var captured http.Header

	app.Post("/test", func(c *fiber.Ctx) error {
		req, err := BuildHTTPRequest(c)
		require.NoError(t, err)
		captured = req.Header
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/test", nil)
	// Hop-by-hop headers that MUST be filtered.
	// Note: Transfer-Encoding and Content-Length are not set via
	// req.Header.Set because Go's httptest validates protocol
	// consistency on the test request — setting them on a nil body
	// produces "unexpected EOF". The filter is exercised via the
	// other hop-by-hop headers; Transfer-Encoding and Content-Length
	// filtering is verified by code inspection (same isHopByHop map +
	// explicit Content-Length string comparison in the filter).
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Keep-Alive", "timeout=5, max=1000")
	req.Header.Set("Proxy-Authenticate", "Basic realm=\"proxy\"")
	req.Header.Set("Proxy-Authorization", "Basic Zm9vOmJhcg==")
	req.Header.Set("Te", "trailers")
	req.Header.Set("Trailers", "X-End-Of-Stream")
	req.Header.Set("Upgrade", "h2c")
	// Host filter (round 5): the upstream client's Host header would
	// shadow the target peer's req.Host for any middleware that reads
	// req.Header.Get("Host") instead of req.Host. Note: httptest sets
	// req.Host directly from the URL; we set the header via req.Header
	// to simulate what fasthttp's VisitAll would emit for an incoming
	// HTTP request with an explicit Host header.
	req.Header.Set("Host", "original-client.example.com")

	// End-to-end headers that MUST pass through.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("X-Arc-Database", "production")
	req.Header.Set("X-Custom-Trace-Id", "abc-123")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	// Hop-by-hop headers must NOT appear in the forwarded request.
	// (Transfer-Encoding + Content-Length filtering verified by code
	// inspection — see note above on httptest validation.)
	hopByHopFiltered := []string{
		"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailers", "Upgrade",
		"Host", // round 5: filtered to keep req.Header consistent with req.Host
	}
	for _, h := range hopByHopFiltered {
		assert.Empty(t, captured.Get(h),
			"%s should be filtered from forwarded request (hop-by-hop per RFC 7230 §6.1 / managed by net/http)", h)
	}

	// End-to-end headers must pass through.
	assert.Equal(t, "application/json", captured.Get("Content-Type"),
		"Content-Type (end-to-end) should pass through")
	assert.Equal(t, "Bearer token123", captured.Get("Authorization"),
		"Authorization (end-to-end) should pass through")
	assert.Equal(t, "production", captured.Get("X-Arc-Database"),
		"X-Arc-Database (Arc-internal routing header) should pass through")
	assert.Equal(t, "abc-123", captured.Get("X-Custom-Trace-Id"),
		"X-Custom-Trace-Id (custom end-to-end header) should pass through")
}

func TestCopyResponse(t *testing.T) {
	app := fiber.New()

	app.Get("/test", func(c *fiber.Ctx) error {
		// Create a mock HTTP response
		mockResp := &http.Response{
			StatusCode: http.StatusCreated,
			Header: http.Header{
				"Content-Type":      []string{"application/json"},
				"X-Custom-Header":   []string{"custom-value"},
				"Connection":        []string{"keep-alive"}, // Should be filtered out (hop-by-hop)
				"Transfer-Encoding": []string{"chunked"},    // Should be filtered out (hop-by-hop)
			},
			Body: http.NoBody,
		}

		err := CopyResponse(c, mockResp)
		require.NoError(t, err)
		return nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)

	// Verify status code is copied
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Verify regular headers are copied
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	assert.Equal(t, "custom-value", resp.Header.Get("X-Custom-Header"))

	// Verify hop-by-hop headers are NOT copied
	assert.Empty(t, resp.Header.Get("Connection"))
	assert.Empty(t, resp.Header.Get("Transfer-Encoding"))
}

func TestShouldForwardWrite(t *testing.T) {
	app := fiber.New()

	tests := []struct {
		name           string
		forwardedBy    string
		expectedResult bool
	}{
		{
			name:           "no router - should not forward",
			forwardedBy:    "",
			expectedResult: false, // Router is nil
		},
		{
			name:           "already forwarded - should not forward",
			forwardedBy:    "node-123",
			expectedResult: false, // Loop prevention
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool

			app.Post("/test", func(c *fiber.Ctx) error {
				// Test with nil router
				result = ShouldForwardWrite(nil, c)
				return c.SendStatus(fiber.StatusOK)
			})

			req := httptest.NewRequest("POST", "/test", nil)
			if tt.forwardedBy != "" {
				req.Header.Set(ForwardedByHeader, tt.forwardedBy)
			}

			_, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestShouldForwardQuery(t *testing.T) {
	app := fiber.New()

	tests := []struct {
		name           string
		forwardedBy    string
		expectedResult bool
	}{
		{
			name:           "no router - should not forward",
			forwardedBy:    "",
			expectedResult: false, // Router is nil
		},
		{
			name:           "already forwarded - should not forward",
			forwardedBy:    "node-456",
			expectedResult: false, // Loop prevention
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool

			app.Post("/test", func(c *fiber.Ctx) error {
				// Test with nil router
				result = ShouldForwardQuery(nil, c)
				return c.SendStatus(fiber.StatusOK)
			})

			req := httptest.NewRequest("POST", "/test", nil)
			if tt.forwardedBy != "" {
				req.Header.Set(ForwardedByHeader, tt.forwardedBy)
			}

			_, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// routerForRole builds a minimal Router whose local node has the given
// role, so CanRouteLocally reflects that role's capabilities. No registry
// or transport is needed because these tests only exercise the pre-forward
// decision, never an actual forward.
func routerForRole(role cluster.NodeRole) *cluster.Router {
	return cluster.NewRouter(&cluster.RouterConfig{
		LocalNode: cluster.NewNode("local", "Local", role, "test-cluster"),
		Logger:    zerolog.Nop(),
	})
}

// TestForwardDecision_ClientCannotForceLocal pins the CVE-2026-45045-class
// hardening on the loop guard: a client-supplied X-Arc-Forwarded-By must
// NOT let a caller force a node that cannot serve the request locally onto
// a doomed local path. On a non-capable node a present marker yields
// ForwardAlreadyForwarded (deterministic error), and on a capable node the
// marker is ignored entirely.
func TestForwardDecision_ClientCannotForceLocal(t *testing.T) {
	tests := []struct {
		name        string
		role        cluster.NodeRole
		isWrite     bool
		forwardedBy string
		want        ForwardDecision
	}{
		// Reader cannot ingest.
		{"reader write, no marker -> forward", cluster.RoleReader, true, "", ForwardToPeer},
		{"reader write, spoofed marker -> already-forwarded", cluster.RoleReader, true, "spoofed", ForwardAlreadyForwarded},
		// Writer can ingest: marker is irrelevant, always local.
		{"writer write, no marker -> local", cluster.RoleWriter, true, "", ForwardLocal},
		{"writer write, spoofed marker -> local (marker ignored)", cluster.RoleWriter, true, "spoofed", ForwardLocal},
		// Compactor cannot query.
		{"compactor query, no marker -> forward", cluster.RoleCompactor, false, "", ForwardToPeer},
		{"compactor query, spoofed marker -> already-forwarded", cluster.RoleCompactor, false, "spoofed", ForwardAlreadyForwarded},
		// Reader can query: marker irrelevant.
		{"reader query, spoofed marker -> local (marker ignored)", cluster.RoleReader, false, "spoofed", ForwardLocal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New()
			router := routerForRole(tt.role)
			var got ForwardDecision
			method := "GET"
			if tt.isWrite {
				method = "POST"
			}
			app.Add(method, "/decide", func(c *fiber.Ctx) error {
				got = decideForward(router, c, tt.isWrite)
				return c.SendStatus(fiber.StatusOK)
			})

			req := httptest.NewRequest(method, "/decide", nil)
			if tt.forwardedBy != "" {
				req.Header.Set(ForwardedByHeader, tt.forwardedBy)
			}
			_, err := app.Test(req)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestBuildHTTPRequest_StripsClientForwardingHeaders pins that every
// client-controlled forwarding/identity header is removed at the forward
// boundary (CVE-2026-45045 class). The forwarding node re-establishes the
// trusted values itself in internal/cluster/router.go.
func TestBuildHTTPRequest_StripsClientForwardingHeaders(t *testing.T) {
	app := fiber.New()

	stripped := []string{
		"X-Real-IP",
		"X-Forwarded-For",
		"X-Forwarded-Host",
		"X-Forwarded-Proto",
		"X-Forwarded-Port",
		"Forwarded",
		"X-Arc-Forwarded-By",
		"X-Arc-Original-Host",
		"X-Arc-Shard-Routed",
		"True-Client-IP",
		"CF-Connecting-IP",
		"X-Client-IP",
	}

	var built *http.Request
	app.Get("/test", func(c *fiber.Ctx) error {
		var err error
		built, err = BuildHTTPRequest(c)
		require.NoError(t, err)
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	for _, h := range stripped {
		req.Header.Set(h, "attacker-value")
	}
	// A legitimate end-to-end header must still pass through.
	req.Header.Set("X-Arc-Database", "mydb")

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, fiber.StatusOK, resp.StatusCode)

	for _, h := range stripped {
		assert.Empty(t, built.Header.Values(h),
			"client-supplied %s must be stripped at the forward boundary", h)
	}
	assert.Equal(t, "mydb", built.Header.Get("X-Arc-Database"),
		"legitimate application header must survive forwarding")
}

func TestHandleRoutingError(t *testing.T) {
	app := fiber.New()

	app.Get("/test-routing-error", func(c *fiber.Ctx) error {
		// Test with a generic error
		return HandleRoutingError(c, fiber.NewError(fiber.StatusInternalServerError, "connection refused"))
	})

	req := httptest.NewRequest("GET", "/test-routing-error", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, fiber.StatusBadGateway, resp.StatusCode)
}

// Tests for shard routing functions

func TestRouteShardedWrite_NoRouter(t *testing.T) {
	app := fiber.New()
	app.Post("/write", func(c *fiber.Ctx) error {
		resp, err := RouteShardedWrite(nil, c)
		assert.Nil(t, resp)
		assert.Nil(t, err)
		return c.SendString("handled locally")
	})

	req := httptest.NewRequest("POST", "/write", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestRouteShardedWrite_AlreadyRouted(t *testing.T) {
	sm := sharding.NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}
	router := sharding.NewShardRouter(&sharding.ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	app := fiber.New()
	app.Post("/write", func(c *fiber.Ctx) error {
		resp, err := RouteShardedWrite(router, c)
		assert.Nil(t, resp)
		assert.Nil(t, err)
		return c.SendString("handled locally")
	})

	req := httptest.NewRequest("POST", "/write", nil)
	req.Header.Set(ShardRoutedHeader, "true") // Already routed
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestRouteShardedWrite_LocalPrimary(t *testing.T) {
	sm := sharding.NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	// Make local node primary for all shards
	for i := 0; i < 3; i++ {
		sm.SetPrimary(i, localNode)
	}

	router := sharding.NewShardRouter(&sharding.ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	app := fiber.New()
	app.Post("/write", func(c *fiber.Ctx) error {
		resp, err := RouteShardedWrite(router, c)
		assert.Nil(t, resp) // Should be nil (handle locally)
		assert.Nil(t, err)
		return c.SendString("handled locally")
	})

	req := httptest.NewRequest("POST", "/write", nil)
	req.Header.Set("X-Arc-Database", "mydb")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestRouteShardedWrite_ForwardToRemote(t *testing.T) {
	// Create a target server to receive forwarded requests
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "local-node", r.Header.Get("X-Arc-Forwarded-By"))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	}))
	defer targetServer.Close()

	sm := sharding.NewShardMap(3)
	localNode := &cluster.Node{ID: "local-node"}
	localNode.UpdateState(cluster.StateHealthy)

	remoteNode := &cluster.Node{ID: "remote-node"}
	remoteNode.APIAddress = targetServer.Listener.Addr().String()
	remoteNode.UpdateState(cluster.StateHealthy)

	// Make remote node primary for all shards
	for i := 0; i < 3; i++ {
		sm.SetPrimary(i, remoteNode)
	}

	router := sharding.NewShardRouter(&sharding.ShardRouterConfig{
		ShardMap:  sm,
		LocalNode: localNode,
		Timeout:   5 * time.Second,
		Logger:    zerolog.Nop(),
	})

	app := fiber.New()
	app.Post("/api/v1/write", func(c *fiber.Ctx) error {
		resp, err := RouteShardedWrite(router, c)
		if err != nil {
			return HandleShardRoutingError(c, err)
		}
		if resp != nil {
			return CopyResponse(c, resp)
		}
		return c.SendString("handled locally")
	})

	req := httptest.NewRequest("POST", "/api/v1/write", nil)
	req.Header.Set("X-Arc-Database", "mydb")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestRouteShardedQuery_NoRouter(t *testing.T) {
	app := fiber.New()
	app.Post("/query", func(c *fiber.Ctx) error {
		resp, err := RouteShardedQuery(nil, "mydb", c)
		assert.Nil(t, resp)
		assert.Nil(t, err)
		return c.SendString("handled locally")
	})

	req := httptest.NewRequest("POST", "/query", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestHandleShardRoutingError_NoDatabaseHeader(t *testing.T) {
	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return HandleShardRoutingError(c, sharding.ErrNoDatabaseHeader)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestHandleShardRoutingError_NoShardPrimary(t *testing.T) {
	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return HandleShardRoutingError(c, sharding.ErrNoShardPrimary)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 503, resp.StatusCode)
}
