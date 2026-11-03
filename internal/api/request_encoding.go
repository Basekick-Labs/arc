package api

import (
	"strings"

	"github.com/gofiber/fiber/v2"
)

// Fiber decompresses a request body sent with Content-Encoding (gzip, br,
// deflate) the first time a handler reads it (c.Body, c.BodyParser), into a
// new buffer with no size limit: a 1 MB gzip body to /api/v1/query took the
// server from 75 MB to 3.4 GB resident before it answered 400.
//
// The write endpoints take compressed bodies on purpose and decode them
// themselves from the raw body, bounded (decompressRequest, the msgpack
// handler), whether or not the client sends the header. Every other route
// refuses an encoded body: none of Arc's own clients sends one there.
var encodedBodyRoutes = map[string]bool{
	"/write":                      true, // InfluxDB 1.x
	"/api/v2/write":               true, // InfluxDB 2.x
	"/api/v1/write/line-protocol": true,
	"/api/v1/write/msgpack":       true,
	"/api/v1/write/tle":           true,
}

// rejectEncodedBodies answers 415 to a request with a Content-Encoding other
// than identity on a route that does not decode the body itself.
func rejectEncodedBodies(c *fiber.Ctx) error {
	// Routing ignores case and a trailing slash (Fiber's defaults), so the
	// match does too.
	path := strings.TrimSuffix(strings.ToLower(c.Path()), "/")
	if encodedBodyRoutes[path] || !hasBodyEncoding(c) {
		return c.Next()
	}
	return c.Status(fiber.StatusUnsupportedMediaType).JSON(fiber.Map{
		"error": "Content-Encoding is only accepted on write endpoints; send this request uncompressed",
	})
}

// hasBodyEncoding reports whether any Content-Encoding line of the request
// carries something other than identity. Every line counts: Fiber decodes
// with the last one, so checking only the first let "identity" followed by
// "gzip" through.
func hasBodyEncoding(c *fiber.Ctx) bool {
	encoded := false
	c.Request().Header.VisitAll(func(key, value []byte) {
		if !strings.EqualFold(string(key), fiber.HeaderContentEncoding) {
			return
		}
		if v := strings.TrimSpace(string(value)); v != "" && !strings.EqualFold(v, "identity") {
			encoded = true
		}
	})
	return encoded
}
