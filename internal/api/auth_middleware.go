package api

import (
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// withWriteAuth returns auth.RequireWrite(am) when am is non-nil, or
// a no-op middleware when am is nil (auth disabled). This collapses
// the previous if/else branching that every ingest handler's
// RegisterRoutes carried — same security posture, single source of
// truth.
//
// nil-am case is the OSS no-auth deployment. In that mode every
// route registers without middleware; the operator has explicitly
// chosen not to gate writes.
func withWriteAuth(am *auth.AuthManager) fiber.Handler {
	if am == nil {
		return passthroughMiddleware
	}
	return auth.RequireWrite(am)
}

// withAdminAuth is the admin-tier counterpart to withWriteAuth.
// Used for endpoints that perform globally-disruptive operations
// (force-flush, bulk imports that rewrite history).
func withAdminAuth(am *auth.AuthManager) fiber.Handler {
	if am == nil {
		return passthroughMiddleware
	}
	return auth.RequireAdmin(am)
}

// passthroughMiddleware is the no-op middleware used when auth is
// disabled. Defined as a package-level value so each call to
// withWriteAuth/withAdminAuth doesn't allocate a new closure.
var passthroughMiddleware fiber.Handler = func(c *fiber.Ctx) error {
	return c.Next()
}
