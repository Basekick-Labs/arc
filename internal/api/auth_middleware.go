package api

import (
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// withResourceReadAuth is the read door for routes that name a database or
// measurement: it consults RBAC as well as the token's coarse permissions, so
// a token whose read authority comes from a grant rather than from the coarse
// "read" bit is not refused before the handler can evaluate RBAC.
//
// rm may be nil (OSS, or auth without RBAC wired); the middleware then falls
// back to the coarse list. nil-am is the OSS
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

// withAnyResourceReadAuth is the door for read routes whose resources are in
// the request body — the query routes, where the tables come from the SQL. It
// admits a token holding "read" on anything, through RBAC or the coarse list,
// and the handler checks every table (checkQueryPermissions). The coarse
// auth.RequireRead it replaced refused tokens whose read authority comes from
// RBAC alone (#1559).
//
// rm may be nil (auth without RBAC wired): the coarse list decides, as with
// auth.RequireRead. The checker is captured at registration, so SetAuthAndRBAC
// must run before RegisterRoutes. nil-am is the OSS no-auth deployment.
func withAnyResourceReadAuth(am *auth.AuthManager, rm auth.AnyResourcePermissionChecker) fiber.Handler {
	if am == nil {
		return passthroughMiddleware
	}
	return auth.RequireAnyResourcePermission(am, rm, "read")
}

// withAnyResourceWriteAuth is withAnyResourceReadAuth for the ingest routes,
// where the measurements come from the payload and CheckWritePermissions
// checks each one (#1559).
func withAnyResourceWriteAuth(am *auth.AuthManager, rm auth.AnyResourcePermissionChecker) fiber.Handler {
	if am == nil {
		return passthroughMiddleware
	}
	return auth.RequireAnyResourcePermission(am, rm, "write")
}

// withAdminAuth is the admin-tier route middleware.
// Used for endpoints that perform globally-disruptive operations
// (force-flush, bulk imports that rewrite history).
func withAdminAuth(am *auth.AuthManager) fiber.Handler {
	if am == nil {
		return passthroughMiddleware
	}
	return auth.RequireAdmin(am)
}

// passthroughMiddleware is the no-op middleware used when auth is
// disabled. Defined as a package-level value so each with*Auth call
// doesn't allocate a new closure.
var passthroughMiddleware fiber.Handler = func(c *fiber.Ctx) error {
	return c.Next()
}
