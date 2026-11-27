package tiering

import (
	"context"
	"testing"
	"unsafe"
)

// The policy cache stores the database name it is given; a name read from a
// request (c.Params) points into a buffer the HTTP framework reuses for the
// next request, so a stored key that was not copied changed with it.
func TestPolicyCacheKeysOwnTheirStrings(t *testing.T) {
	store, cleanup := setupTestPolicyStore(t)
	defer cleanup()
	ctx := context.Background()

	buf := []byte("db1db2")
	set := unsafe.String(&buf[0], 3) // aliases buf, like c.Params
	days := 3
	if err := store.Set(ctx, &DatabasePolicy{Database: set, HotMaxAgeDays: &days}); err != nil {
		t.Fatal(err)
	}
	get := unsafe.String(&buf[3], 3) // a miss: Get caches the (nil) result under this name
	if _, err := store.Get(ctx, get); err != nil {
		t.Fatal(err)
	}
	copy(buf, "dbXdbY") // the next requests reuse the buffer

	store.mu.RLock()
	defer store.mu.RUnlock()
	for k, p := range store.cache {
		if k != "db1" && k != "db2" {
			t.Fatalf("a cached key changed with the request buffer: %q", k)
		}
		if p != nil && p.Database != k {
			t.Fatalf("cached policy for %q names %q", k, p.Database)
		}
	}
}
