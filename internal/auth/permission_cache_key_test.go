package auth

import (
	"testing"
	"unsafe"
)

// The HTTP framework hands out header and parameter values that point into a
// request buffer it reuses for the next request on the same connection. A
// cached allow stored under such a string changed with the buffer and then
// matched another name (GHSA-q3hx-w8mj-pf6m). Here the names sit in a buffer
// the test rewrites, as fasthttp does, between an allowed check and a check
// for a name the token is not granted.
func TestPermissionCacheKeyOwnsItsStrings(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := map[bool]string{false: "single", true: "batch"}[batch]
		t.Run(name, func(t *testing.T) {
			rm, info, cleanup := scopedRig(t, "read") // grants db1/cpu only
			defer cleanup()

			buf := []byte("db1")
			reused := unsafe.String(&buf[0], len(buf)) // aliases buf, like c.Get
			req := &PermissionCheckRequest{TokenInfo: info, Database: reused, Measurement: "cpu", Permission: "read"}
			var first *PermissionCheckResult
			if batch {
				first = rm.CheckPermissionsBatch([]*PermissionCheckRequest{req})[0]
			} else {
				first = rm.CheckPermission(req)
			}
			if !first.Allowed {
				t.Fatalf("granted db1/cpu denied: %+v", first)
			}

			copy(buf, "db2") // the next request on the connection reuses the buffer

			// The stored key must still name db1. (A lookup for db2 alone would
			// usually miss: the map hashed the key as db1 when it stored it, so
			// a stale entry is found only on a hash collision, which is what the
			// report needed many requests to hit. Reading the stored keys shows
			// the aliasing every time.)
			rm.permCacheMu.RLock()
			for k := range rm.permCache {
				if k.tokenID == info.ID && k.database != "db1" {
					rm.permCacheMu.RUnlock()
					t.Fatalf("a cached key changed with the request buffer: database %q", k.database)
				}
			}
			rm.permCacheMu.RUnlock()

			denied := &PermissionCheckRequest{TokenInfo: info, Database: "db2", Measurement: "cpu", Permission: "read"}
			var got *PermissionCheckResult
			if batch {
				got = rm.CheckPermissionsBatch([]*PermissionCheckRequest{denied})[0]
			} else {
				got = rm.CheckPermission(denied)
			}
			if got.Allowed {
				t.Fatalf("db2/cpu allowed through the cached db1 entry: %+v", got)
			}
		})
	}
}
