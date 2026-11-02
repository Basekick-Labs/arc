package api

import (
	"path"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/auth"
)

// HeaderClientInstallationID is sent by Arc clients (the Python SDK
// today) with a random per-installation UUID. Generic so later clients
// reuse it; the client kind comes from the User-Agent product token.
const HeaderClientInstallationID = "Arc-Client-Installation-Id"

// HeaderArcliInstallationID is the legacy header arcli sends with its
// per-installation UUID (see arcli's README, "Privacy"). Still accepted;
// when both are present HeaderClientInstallationID wins.
const HeaderArcliInstallationID = "Arcli-Installation-Id"

// Client kinds, derived from the User-Agent product token. The telemetry
// registry accepts exactly these strings; client_identity_test.go pins
// them to telemetry.ClientKindArcli / telemetry.ClientKindPythonSDK.
const (
	ClientKindArcli     = "arcli"      // "arcli/<version> ..."
	ClientKindPythonSDK = "python_sdk" // "arc-client-python/<version> ..."
)

// ClientRecorder receives the identity of client installations (arcli,
// the Python SDK) that talk to this node. The telemetry collector
// implements it; nil disables the middleware entirely. runtime and
// platform are empty for kinds that do not report them.
type ClientRecorder interface {
	RecordClient(kind, id, version, runtime, platform string)
}

// clientIdentity hands the installation id, client kind and version to
// rec after the request was served. The id comes from
// Arc-Client-Installation-Id, or the legacy Arcli-Installation-Id; the
// kind from the User-Agent product token, and a request whose
// User-Agent names no known client is never recorded, so foreign
// traffic cannot pollute the counts. Two gates keep anonymous callers
// out of the bounded registry: the response must be < 400, and when the
// server runs with authentication the auth middleware must have
// validated a token on this request (auth.GetTokenInfo). Public routes
// such as /health answer 200 without a token and are therefore never
// counted (the path list below is only belt-and-braces: Fiber routes
// case-insensitively and tolerates a trailing slash, so the path alone
// is not a safe gate). Without authentication configured every caller
// is trusted anyway, and the docs say so. Requests without either
// header cost two header lookups. Header values are copied out of
// fasthttp's request buffer before they outlive the request (Fiber runs
// without Immutable), and are validated by the recorder; nothing is
// logged.
func clientIdentity(rec ClientRecorder, authRequired bool) fiber.Handler {
	return func(c *fiber.Ctx) error {
		raw := c.Get(HeaderClientInstallationID)
		if raw == "" {
			raw = c.Get(HeaderArcliInstallationID)
		}
		if raw == "" || len(raw) != 36 {
			return c.Next()
		}
		// The User-Agent check is cheaper than the route check (which
		// allocates), so foreign clients carrying an id leave first.
		ua := parseClientUserAgent(c.Get(fiber.HeaderUserAgent))
		if ua.kind == "" || isUnauthenticatedRoute(c.Path()) {
			return c.Next()
		}
		id := strings.Clone(raw)
		version := strings.Clone(ua.version)
		runtime := strings.Clone(ua.runtime)
		platform := strings.Clone(ua.platform)
		err := c.Next()
		// Fiber runs the error handler after the stack unwinds, so a
		// handler that returns an error (401 via fiber.NewError, a 500,
		// an unknown route's 404) still shows status 200 here; err must
		// be nil as well.
		if err == nil && c.Response().StatusCode() < 400 && (!authRequired || auth.GetTokenInfo(c) != nil) {
			rec.RecordClient(ua.kind, id, version, runtime, platform)
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
	case "/health", "/ready", "/ready/write", "/api/v1/auth/verify":
		return true
	}
	return strings.HasPrefix(p, "/metrics") || strings.HasPrefix(p, "/api/v1/metrics")
}

// maxClientFieldLen mirrors the recorder's limit (telemetry.cleanVersion).
const maxClientFieldLen = 32

// clientUA is what the User-Agent says about the client. The fields
// are views into the header (not owned); empty means not reported, and
// the recorder turns it into "unknown".
type clientUA struct {
	kind, version, runtime, platform string
}

// parseClientUserAgent recognises
//
//	arcli/26.09.1 (darwin/arm64)                                  → arcli, "26.09.1"
//	arc-client-python/0.2.0 (python/3.12.4; linux/amd64)          → python_sdk, "0.2.0", "3.12.4", "linux/amd64"
//	arc-client-python/0.2.0                                       → python_sdk, "0.2.0" (comment absent or malformed: runtime/platform unknown)
//
// Anything else yields kind "". For the Python SDK, runtime is the bare
// interpreter version ("python/" stripped) and platform the os/arch
// element verbatim; a comment that does not have that shape leaves
// both empty. arcli's comment is not reported (its payload is unchanged).
func parseClientUserAgent(ua string) clientUA {
	const (
		arcliPrefix  = "arcli/"
		pythonPrefix = "arc-client-python/"
	)
	var r clientUA
	var rest string
	switch {
	case strings.HasPrefix(ua, arcliPrefix):
		r.kind, rest = ClientKindArcli, ua[len(arcliPrefix):]
	case strings.HasPrefix(ua, pythonPrefix):
		r.kind, rest = ClientKindPythonSDK, ua[len(pythonPrefix):]
	default:
		return r
	}
	r.version = rest
	comment := ""
	if i := strings.IndexByte(rest, ' '); i >= 0 {
		r.version, comment = rest[:i], strings.TrimSpace(rest[i+1:])
	}
	// The recorder reports anything longer as "unknown"; dropping it here
	// keeps the middleware from cloning a header-sized string per request.
	if len(r.version) > maxClientFieldLen {
		r.version = ""
	}
	if r.kind != ClientKindPythonSDK || len(comment) < 2 || comment[0] != '(' {
		return r
	}
	end := strings.IndexByte(comment, ')')
	if end < 0 {
		return r
	}
	// Exactly two elements; Cut avoids allocating on every SDK request.
	first, second, ok := strings.Cut(comment[1:end], ";")
	if !ok || strings.Contains(second, ";") {
		return r
	}
	const pyPrefix = "python/"
	if rt := strings.TrimSpace(first); strings.HasPrefix(rt, pyPrefix) {
		r.runtime = rt[len(pyPrefix):]
	} else {
		return r
	}
	r.platform = strings.TrimSpace(second)
	return r
}
