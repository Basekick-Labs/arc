package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
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
	assert.Equal(t, fiber.StatusOK, resp.StatusCode)
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
