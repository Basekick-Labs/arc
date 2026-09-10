package auth

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Regression tests for the token cache honoring the token's own expiry.
//
// VerifyToken memoizes a successful lookup for up to cacheTTL. The cache-hit
// branch used to test only that cache deadline, so a token that expired while
// cached stayed authorized until the entry aged out — a cache miss rejected the
// same credential that a cache hit accepted. These tests pin the invariant that
// an expired token is rejected on every path.

// expireToken moves a token's expiry to the given instant in BOTH the database
// row and the live cache entry, simulating "time passed and the token expired"
// without sleeping.
//
// Both must move together. Rewriting only the cached copy does not model
// expiry: the cache-hit check would reject and fall through to the database,
// which still holds the original expiry, so the token would be re-validated and
// re-cached — the token is genuinely still valid, and the test would be
// asserting the wrong thing. Writing the row directly (rather than via
// UpdateToken) deliberately skips the automatic cache invalidation, which is
// what leaves a live cache entry for an expired credential — precisely the
// state this fix is about.
//
// The cache entry's own deadline is left untouched and in the future, so these
// tests exercise the cache-hit branch rather than a natural cache miss.
func expireToken(t *testing.T, am *AuthManager, token string, expiresAt time.Time) {
	t.Helper()

	if _, err := am.db.Exec("UPDATE api_tokens SET expires_at = ? WHERE token_prefix = ?", expiresAt, tokenPrefix(token)); err != nil {
		t.Fatalf("update expires_at: %v", err)
	}

	am.cacheMu.Lock()
	defer am.cacheMu.Unlock()

	entry, ok := am.cache[cacheKey(token)]
	if !ok {
		t.Fatal("token is not cached; the test needs a populated cache entry to be meaningful")
	}
	entry.info.ExpiresAt = &expiresAt
}

func cachedEntryCount(am *AuthManager) int {
	am.cacheMu.RLock()
	defer am.cacheMu.RUnlock()
	return len(am.cache)
}

// TestVerifyToken_CachedTokenRejectedAfterExpiry is the primary regression
// proof. It is deterministic: expiry is moved rather than waited for.
func TestVerifyToken_CachedTokenRejectedAfterExpiry(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	expiresAt := time.Now().Add(time.Hour)
	token, err := am.CreateToken(context.Background(), "cached-expiry", "Expires while cached", "admin", &expiresAt)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	// Populate the cache while the token is still valid.
	if info := am.VerifyToken(token); info == nil {
		t.Fatal("VerifyToken returned nil for a valid token")
	}
	if cachedEntryCount(am) == 0 {
		t.Fatal("expected the successful verification to populate the cache")
	}

	// The token expires; the cache entry's own TTL is still live.
	expireToken(t, am, token, time.Now().Add(-time.Second))

	if info := am.VerifyToken(token); info != nil {
		t.Error("VerifyToken accepted a token that expired while cached")
	}

	// The stale entry must not linger.
	if cachedEntryCount(am) != 0 {
		t.Error("expected the expired entry to be evicted from the cache")
	}

	// And it stays rejected once the database is consulted again.
	if info := am.VerifyToken(token); info != nil {
		t.Error("VerifyToken accepted an expired token on the database path")
	}
}

// TestVerifyToken_CachedTokenValidBeforeExpiry is the negative control: the
// fix must not reject tokens that are merely close to expiring.
func TestVerifyToken_CachedTokenValidBeforeExpiry(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	expiresAt := time.Now().Add(time.Hour)
	token, err := am.CreateToken(context.Background(), "not-yet-expired", "Still valid", "read", &expiresAt)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	if info := am.VerifyToken(token); info == nil {
		t.Fatal("VerifyToken returned nil for a valid token")
	}

	// Expiry moved close, but still in the future.
	expireToken(t, am, token, time.Now().Add(2*time.Second))

	if info := am.VerifyToken(token); info == nil {
		t.Error("VerifyToken rejected a token that has not expired yet")
	}
	if cachedEntryCount(am) == 0 {
		t.Error("a still-valid token should remain cached")
	}
}

// TestVerifyToken_NonExpiringTokenUnaffected guards the common case: tokens
// created without an expiry must keep the full cache TTL.
func TestVerifyToken_NonExpiringTokenUnaffected(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	token, err := am.CreateToken(context.Background(), "no-expiry", "Never expires", "read", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	info := am.VerifyToken(token)
	if info == nil {
		t.Fatal("VerifyToken returned nil for a valid token")
	}
	if info.ExpiresAt != nil {
		t.Fatalf("expected no expiry, got %v", info.ExpiresAt)
	}

	am.cacheMu.RLock()
	entry, ok := am.cache[cacheKey(token)]
	am.cacheMu.RUnlock()
	if !ok {
		t.Fatal("expected the token to be cached")
	}

	// Deadline should be the full cache TTL, not capped by anything.
	if remaining := time.Until(entry.expiresAt); remaining < am.cacheTTL-time.Minute {
		t.Errorf("non-expiring token got a shortened cache deadline: %v remaining, cacheTTL %v", remaining, am.cacheTTL)
	}

	// Repeated verification keeps succeeding from cache.
	if info := am.VerifyToken(token); info == nil {
		t.Error("VerifyToken rejected a non-expiring token on a cache hit")
	}
}

// TestVerifyToken_CacheDeadlineCappedAtTokenExpiry pins the defense-in-depth
// half of the fix: an entry is never cached past the credential's own life.
func TestVerifyToken_CacheDeadlineCappedAtTokenExpiry(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Well inside the 5-minute cache TTL the test manager uses.
	expiresAt := time.Now().Add(30 * time.Second)
	token, err := am.CreateToken(context.Background(), "short-lived", "Expires before the cache TTL", "read", &expiresAt)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	info := am.VerifyToken(token)
	if info == nil {
		t.Fatal("VerifyToken returned nil for a valid token")
	}
	if info.ExpiresAt == nil {
		t.Fatal("expected the token to carry an expiry")
	}

	am.cacheMu.RLock()
	entry, ok := am.cache[cacheKey(token)]
	am.cacheMu.RUnlock()
	if !ok {
		t.Fatal("expected the token to be cached")
	}

	// SQLite round-trips the timestamp, so compare instants, not structs.
	if !entry.expiresAt.Equal(*info.ExpiresAt) {
		t.Errorf("cache deadline should be capped at the token expiry:\n  cache deadline %v\n  token expiry   %v", entry.expiresAt, *info.ExpiresAt)
	}
	if entry.expiresAt.After(time.Now().Add(am.cacheTTL)) {
		t.Error("cache deadline exceeds the cache TTL")
	}
}

// TestVerifyToken_ExpiresWhileCached_EndToEnd is the real-path proof: no
// planted state, a token that genuinely expires between two verifications
// while its cache entry is still live.
//
// The window is seconds rather than milliseconds because CreateToken and the
// first VerifyToken each run PBKDF2 at pbkdf2Iterations, which is materially
// slower under the race detector used in CI.
func TestVerifyToken_ExpiresWhileCached_EndToEnd(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	expiresAt := time.Now().Add(4 * time.Second)
	token, err := am.CreateToken(context.Background(), "expiring", "Expires mid-test", "admin", &expiresAt)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	info := am.VerifyToken(token)
	if info == nil {
		t.Fatal("VerifyToken returned nil before expiry; the token window was too short for this machine")
	}

	missesBefore := am.cacheMisses.Load()

	// Wait for the credential to expire. The cache entry is capped at the
	// token expiry, so this also proves the entry is not simply gone: the
	// assertions below distinguish the two paths.
	time.Sleep(time.Until(expiresAt) + 250*time.Millisecond)

	if info := am.VerifyToken(token); info != nil {
		t.Error("VerifyToken accepted the token after it expired")
	}
	if am.cacheMisses.Load() <= missesBefore {
		t.Error("expected the expired token to fall through to the database path")
	}

	am.InvalidateCache()
	if info := am.VerifyToken(token); info != nil {
		t.Error("VerifyToken accepted the expired token after cache invalidation")
	}
}

// TestCreateToken_ExpiresAtStoredInUTC pins the storage timezone for
// expires_at.
//
// created_at is filled by SQLite's CURRENT_TIMESTAMP and is therefore always
// UTC, while expires_at is bound as a Go time.Time — and go-sqlite3 text-encodes
// those using the value's own location. A caller in a non-UTC zone used to
// store an offset-bearing string next to a UTC one in the same table. Both are
// the same instant and Go parses either correctly, so this is not an expiry
// bypass, but the mixed domain is exactly what the SQLite review checklist
// warns about and it makes the two columns render in different zones over the
// API. The cluster apply path already normalizes (#459/#460); this covers the
// standalone path.
func TestCreateToken_ExpiresAtStoredInUTC(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// A deliberately non-UTC expiry, as a caller in any other zone would pass.
	loc := time.FixedZone("UTC-6", -6*60*60)
	expiresAt := time.Now().In(loc).Add(time.Hour)

	if _, err := am.CreateToken(context.Background(), "utc-storage", "Zone check", "read", &expiresAt); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	assertStoredUTC := func(stage string) {
		t.Helper()
		var stored string
		if err := am.db.QueryRow("SELECT expires_at FROM api_tokens WHERE name = ?", "utc-storage").Scan(&stored); err != nil {
			t.Fatalf("%s: read expires_at: %v", stage, err)
		}
		// go-sqlite3 appends the zone offset for non-UTC values ("-06:00")
		// and writes a bare "...Z"-less UTC string otherwise. Either an
		// explicit offset or a trailing zone name means the value did not
		// land in UTC.
		if strings.Contains(stored, "+") || strings.Contains(stored, "-06:00") {
			t.Errorf("%s: expires_at stored with a zone offset, expected UTC: %q", stage, stored)
		}
	}

	assertStoredUTC("after create")

	// The same rule applies to updates.
	newExpiry := time.Now().In(loc).Add(2 * time.Hour)
	var id int64
	if err := am.db.QueryRow("SELECT id FROM api_tokens WHERE name = ?", "utc-storage").Scan(&id); err != nil {
		t.Fatalf("lookup id: %v", err)
	}
	if err := am.UpdateToken(context.Background(), id, nil, nil, nil, &newExpiry); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	assertStoredUTC("after update")
}
