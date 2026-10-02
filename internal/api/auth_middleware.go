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

// withReadAuth is the read-tier counterpart to withWriteAuth. Used for
// query endpoints so a write-only token cannot execute SELECTs — the
// token must carry read permission. nil-am is the OSS no-auth deployment.
func withReadAuth(am *auth.AuthManager) fiber.Handler {
	if am == nil {
		return passthroughMiddleware
	}
	return auth.RequireRead(am)
}

// withResourceReadAuth is withReadAuth for routes that name a database or
// measurement: it consults RBAC as well as the token's coarse permissions, so
// a token whose read authority comes from a grant rather than from the coarse
// "read" bit is not refused before the handler can evaluate RBAC.
//
// rm may be nil (OSS, or auth without RBAC wired); the middleware then falls
// back to the coarse list exactly as withReadAuth does. nil-am is the OSS
// no-auth deployment.
//
// NOTE: the checker is captured when the route is registered, so a handler
// must have been given its RBAC checker BEFORE RegisterRoutes runs. That is a
// real requirement, not a convention — see DatabasesHandler.SetRBACManager.
func withResourceReadAuth(am *auth.AuthManager, rm auth.ResourcePermissionChecker) fiber.Handler {
	if am == nil {
		return passthroughMiddleware
	}
	return auth.RequireResourceRead(am, rm)
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
