package api

import (
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// databaseListingFilter decides which databases a listing of every database
// may show the caller: GET /api/v1/databases, SHOW DATABASES and the
// unscoped GET /api/v1/measurements (#1562).
//
// A caller with a read grant covering every database (or RBAC not wired, or
// no token: auth off) sees them all, as before: visible is nil. Any other
// caller sees the databases it can read something in (CanAccessAnythingIn,
// the same question GET /api/v1/databases/:name asks), so a tenant token can
// discover its own databases and no others. Until #1562 these listings
// refused such a caller outright.
//
// unavailable reports that the caller's grants could not be read: the
// listing must answer 503 rather than an empty list that looks like "you can
// see nothing".
func databaseListingFilter(c *fiber.Ctx, rm RBACChecker) (visible func(database string) bool, unavailable bool) {
	if rm == nil {
		return nil, false
	}
	tokenInfo := auth.GetTokenInfo(c)
	if tokenInfo == nil {
		return nil, false
	}
	all := rm.CheckPermission(&auth.PermissionCheckRequest{
		TokenInfo: tokenInfo, Database: "*", Measurement: "*", Permission: "read",
	})
	if all.Allowed {
		return nil, false
	}
	if all.Source == auth.SourceUnavailable {
		return nil, true
	}
	return func(database string) bool {
		return rm.CanAccessAnythingIn(tokenInfo, database, "read")
	}, false
}

// keepVisible filters database names in place by visible; nil keeps all.
func keepVisible(databases []string, visible func(string) bool) []string {
	if visible == nil {
		return databases
	}
	out := databases[:0]
	for _, db := range databases {
		if visible(db) {
			out = append(out, db)
		}
	}
	return out
}
