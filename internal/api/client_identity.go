package api

import (
	"path"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/auth"
)

// HeaderArcliInstallationID is sent by arcli on every request with a
// random per-installation UUID (see arcli's README, "Privacy").
const HeaderArcliInstallationID = "Arcli-Installation-Id"

// ClientRecorder receives the identity of CLI installations that talk
// to this node. The telemetry collector implements it; nil disables
// the middleware entirely.
type ClientRecorder interface {
	RecordClient(id, version string)
}

// clientIdentity hands the arcli installation id and version to rec
// after the request was served. Two gates keep anonymous callers out
// of the bounded registry: the response must be < 400, and when the
// server runs with authentication the auth middleware must have
// validated a token on this request (auth.GetTokenInfo). Public routes
// such as /health answer 200 without a token and are therefore never
// counted (the path list below is only belt-and-braces: Fiber routes
// case-insensitively and tolerates a trailing slash, so the path alone
// is not a safe gate). Without authentication configured every caller
// is trusted anyway, and the docs say so. Requests without the header
// cost one header lookup. Header values are copied out of fasthttp's
// request buffer before they outlive the request (Fiber runs without
// Immutable), and are validated by the recorder; nothing is logged.
func clientIdentity(rec ClientRecorder, authRequired bool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		raw := c.Get(HeaderArcliInstallationID)
		if raw == "" || len(raw) != 36 || isUnauthenticatedRoute(c.Path()) {
			return c.Next()
		}
		id := strings.Clone(raw)
		version := strings.Clone(arcliVersion(c.Get(fiber.HeaderUserAgent)))
		err := c.Next()
		// Fiber runs the error handler after the stack unwinds, so a
		// handler that returns an error (401 via fiber.NewError, a 500,
		// an unknown route's 404) still shows status 200 here; err must
		// be nil as well.
		if err == nil && c.Response().StatusCode() < 400 && (!authRequired || auth.GetTokenInfo(c) != nil) {
			rec.RecordClient(id, version)
		}
		return err
	}
}

// isUnauthenticatedRoute mirrors the auth middleware's default public
// routes (internal/auth/middleware.go DefaultMiddlewareConfig) plus the
// metrics route main.go adds, normalised the way Fiber matches them.
func isUnauthenticatedRoute(p string) bool {
	p = strings.ToLower(path.Clean("/" + p))
	switch p {
	case "/health", "/ready", "/api/v1/auth/verify":
		return true
	}
	return strings.HasPrefix(p, "/metrics") || strings.HasPrefix(p, "/api/v1/metrics")
}

// arcliVersion extracts "26.09.1" from "arcli/26.09.1 (darwin/arm64)";
// anything else yields "" and the recorder reports it as unknown.
func arcliVersion(ua string) string {
	const prefix = "arcli/"
	if !strings.HasPrefix(ua, prefix) {
		return ""
	}
	v := ua[len(prefix):]
	if i := strings.IndexByte(v, ' '); i >= 0 {
		v = v[:i]
	}
	return v
}
