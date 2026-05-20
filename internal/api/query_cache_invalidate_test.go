package api

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/rs/zerolog"
)

// cacheInvalidateTestSetup spins up a minimal *fiber.App routing
// POST /api/v1/internal/cache/invalidate to handleCacheInvalidate, with
// a QueryHandler whose cluster-auth state is configurable per-test. The
// db/storage fields are nil because every test in this file exercises
// the rejection paths (403 before any db call). The single positive case
// would 500 (nil deref on db.ClearHTTPCache) — we skip it because the
// validation logic is covered by the security-pkg unit tests, and the
// "handler reaches the success path" wiring is a code-review check
// against the diff, not a runtime invariant a test can easily exercise
// without a real DuckDB.
func cacheInvalidateTestSetup(t *testing.T, configure func(*QueryHandler)) *fiber.App {
	t.Helper()
	h := &QueryHandler{logger: zerolog.Nop()}
	if configure != nil {
		configure(h)
	}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	// Mirror the production middleware order: recover catches any nil
	// deref inside the handler so the test goroutine doesn't crash. In
	// these tests h.db is nil, so a successful auth path that proceeds
	// to db.ClearHTTPCache panics — recover turns that into a 500 instead.
	// The negative tests all return 403 before any db access, so recover
	// is a no-op for them.
	app.Use(recover.New())
	app.Post("/api/v1/internal/cache/invalidate", h.handleCacheInvalidate)
	return app
}

// validHeaders computes the five auth headers a legitimate cluster peer
// would send. Test cases mutate the map to exercise each rejection path.
func validHeaders(t *testing.T, secret, peerNodeID, clusterName string) map[string]string {
	t.Helper()
	nonce, err := security.GenerateNonce()
	if err != nil {
		// GenerateNonce only fails if crypto/rand fails; surface the
		// failure with t.Fatal so the test framework attributes it
		// cleanly instead of crashing the goroutine.
		t.Fatalf("GenerateNonce: %v", err)
	}
	ts := time.Now().Unix()
	mac := security.ComputeCacheInvalidateHMAC(secret, nonce, peerNodeID, clusterName, ts)
	return map[string]string{
		"X-Arc-Node-ID":   peerNodeID,
		"X-Arc-Cluster":   clusterName,
		"X-Arc-Nonce":     nonce,
		"X-Arc-Timestamp": strconv.FormatInt(ts, 10),
		"X-Arc-HMAC":      mac,
	}
}

func sendInvalidate(t *testing.T, app *fiber.App, headers map[string]string) int {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/internal/cache/invalidate", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp.StatusCode
}

// TestCacheInvalidate_NoSharedSecret_AlwaysRefuses pins the security
// invariant that the endpoint is OFF when SetClusterAuth was never
// called: OSS deployments, standalone, or any node that started without
// cluster.shared_secret configured. A valid-looking request with all the
// right headers must STILL be refused — the only legitimate caller
// requires a shared secret to produce a MAC, which by definition is
// impossible here.
func TestCacheInvalidate_NoSharedSecret_AlwaysRefuses(t *testing.T) {
	t.Parallel()
	app := cacheInvalidateTestSetup(t, nil) // no SetClusterAuth call

	// Even a "looks valid against a hypothetical secret" request must 403.
	headers := validHeaders(t, "any-secret", "peer-1", "cluster-A")
	if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 when no shared secret configured, got %d", got)
	}

	// An empty request must also 403 (not 400, not 500 — the rejection
	// path must be uniform so an attacker probing for state cannot
	// distinguish "no secret" from "invalid MAC").
	if got := sendInvalidate(t, app, map[string]string{}); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with empty headers and no secret, got %d", got)
	}
}

// TestCacheInvalidate_ValidAuth_ReachesDB is the positive-path proxy:
// when auth passes, the handler reaches into h.db.ClearHTTPCache() which
// panics on nil-db; Fiber's recover middleware catches the panic and
// returns 500. We assert exactly 500 so the test fails loudly if a
// future refactor makes ClearHTTPCache nil-safe or moves the call past
// an early-return — in which case this test would silently pass for the
// wrong reason ("no 403" without actually exercising the success path).
// Asserting the concrete 500 pins the post-auth code flow to the panic
// site, giving the test something specific to break against.
func TestCacheInvalidate_ValidAuth_ReachesDB(t *testing.T) {
	t.Parallel()
	const secret = "test-shared-secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
	})

	headers := validHeaders(t, secret, peerNodeID, clusterName)
	got := sendInvalidate(t, app, headers)
	if got != fiber.StatusInternalServerError {
		t.Errorf("expected 500 (recover-from-nil-db-panic on valid auth), got %d", got)
	}
}

// TestCacheInvalidate_MissingHeaders covers the early-return guards
// before any HMAC computation. Each individual missing header must
// produce 403 — leaking which header is missing would help an attacker
// debug their forgery attempt.
func TestCacheInvalidate_MissingHeaders(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	cases := []struct {
		name string
		drop string
	}{
		{"missing X-Arc-Node-ID", "X-Arc-Node-ID"},
		{"missing X-Arc-Cluster", "X-Arc-Cluster"},
		{"missing X-Arc-Nonce", "X-Arc-Nonce"},
		{"missing X-Arc-Timestamp", "X-Arc-Timestamp"},
		{"missing X-Arc-HMAC", "X-Arc-HMAC"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
				h.SetClusterAuth(secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
			})
			headers := validHeaders(t, secret, peerNodeID, clusterName)
			delete(headers, c.drop)
			if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
				t.Errorf("expected 403 with %s, got %d", c.name, got)
			}
		})
	}
}

// TestCacheInvalidate_SelfAddressed pins the self-loop guard. A request
// claiming X-Arc-Node-ID == this node's own ID is either misconfiguration
// (local invalidation runs in-process, never over HTTP) or a confused
// attacker — refuse it regardless of MAC validity.
func TestCacheInvalidate_SelfAddressed(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"

	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
	})

	// Use localNodeID as the sender — a valid MAC for the local node ID
	// must still be refused because the local invalidation path is
	// in-process, not HTTP.
	headers := validHeaders(t, secret, localNodeID, clusterName)
	if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 for self-addressed request, got %d", got)
	}
}

// TestCacheInvalidate_WrongCluster pins the cluster-name binding. A
// leaked MAC from cluster A must not be replayable against cluster B
// — receiver checks the X-Arc-Cluster header against its own
// configured cluster name before the HMAC computation.
func TestCacheInvalidate_WrongCluster(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const ourClusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(secret, ourClusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
	})

	headers := validHeaders(t, secret, peerNodeID, "different-cluster")
	if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with mismatched cluster name, got %d", got)
	}
}

// TestCacheInvalidate_WrongSecret pins HMAC-validation rejection. A
// well-formed request signed with a different secret must 403.
func TestCacheInvalidate_WrongSecret(t *testing.T) {
	t.Parallel()
	const ourSecret = "correct-secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(ourSecret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
	})

	headers := validHeaders(t, "WRONG-secret", peerNodeID, clusterName)
	if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with wrong secret MAC, got %d", got)
	}
}

// TestCacheInvalidate_StaleTimestamp pins the freshness window. A
// request with a timestamp outside the tolerance must 403, even with a
// MAC that's internally consistent for that timestamp.
func TestCacheInvalidate_StaleTimestamp(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
	})

	// Stale by 10 minutes — well outside the 5-minute tolerance.
	nonce, _ := security.GenerateNonce()
	staleTS := time.Now().Add(-10 * time.Minute).Unix()
	mac := security.ComputeCacheInvalidateHMAC(secret, nonce, peerNodeID, clusterName, staleTS)
	headers := map[string]string{
		"X-Arc-Node-ID":   peerNodeID,
		"X-Arc-Cluster":   clusterName,
		"X-Arc-Nonce":     nonce,
		"X-Arc-Timestamp": strconv.FormatInt(staleTS, 10),
		"X-Arc-HMAC":      mac,
	}
	if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with stale timestamp, got %d", got)
	}
}

// TestCacheInvalidate_NonNumericTimestamp pins the parse-error path:
// `X-Arc-Timestamp: garbage` must 403, not 500. Defense against an
// attacker probing for stack traces or panic-on-bad-input.
func TestCacheInvalidate_NonNumericTimestamp(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
	})

	headers := validHeaders(t, secret, peerNodeID, clusterName)
	headers["X-Arc-Timestamp"] = "not-a-number"
	if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with non-numeric timestamp, got %d", got)
	}
}

// TestCacheInvalidate_Replay pins the nonce-cache check: a valid request
// that succeeds once must fail when replayed within the TTL window.
//
// Trick: the FIRST send passes auth, reaches the nil-db panic site, and
// Fiber's recover middleware turns that into a 500 (the nonce is tracked
// regardless of the post-auth panic). The SECOND send with the same
// headers must hit the replay path (403) without ever reaching the db.
// We assert: first send is 500 (exact, pins the nonce-tracked code-flow
// past auth), second send is 403 (replay reject).
func TestCacheInvalidate_Replay(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	// Use a shared NonceCache so the second call sees the first call's
	// tracked nonce.
	cache := security.NewNonceCache(5 * time.Minute)
	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(secret, clusterName, localNodeID, cache, 5*time.Minute)
	})

	headers := validHeaders(t, secret, peerNodeID, clusterName)
	first := sendInvalidate(t, app, headers)
	if first != fiber.StatusInternalServerError {
		t.Fatalf("first request: expected 500 (auth passed, hit nil-db panic), got %d — auth setup may be broken", first)
	}
	second := sendInvalidate(t, app, headers)
	if second != fiber.StatusForbidden {
		t.Errorf("replay of valid request was accepted (got %d) — nonce cache is not preventing replay", second)
	}
}

// TestCacheInvalidate_NonceNotBurnedByBadMAC pins the ordering invariant
// in handleCacheInvalidate: the nonce-cache Track() runs AFTER HMAC
// validation, so a flood of bad-MAC requests cannot consume cache
// entries for a nonce that a legitimate peer might later use. A future
// refactor that swaps the order would let an off-path attacker (no
// secret) deny service by burning nonces a real peer is about to
// present.
//
// Shape: send N requests with the SAME nonce but a bad MAC — each must
// 403 at the HMAC stage without tracking the nonce. Then send ONE
// request with the same nonce but a valid MAC — must reach the success
// path (500 here, recovered from nil-db panic). If the order were
// inverted, the bad-MAC requests would Track the nonce first and the
// valid request would 403 as a replay.
func TestCacheInvalidate_NonceNotBurnedByBadMAC(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	cache := security.NewNonceCache(5 * time.Minute)
	app := cacheInvalidateTestSetup(t, func(h *QueryHandler) {
		h.SetClusterAuth(secret, clusterName, localNodeID, cache, 5*time.Minute)
	})

	// Build a valid header set, then keep the nonce/timestamp but
	// corrupt the MAC. Send several bad-MAC requests with the SAME nonce.
	headers := validHeaders(t, secret, peerNodeID, clusterName)
	originalMAC := headers["X-Arc-HMAC"]
	headers["X-Arc-HMAC"] = "deadbeef" + originalMAC[8:] // wrong bytes, same length shape

	const badAttempts = 5
	for i := 0; i < badAttempts; i++ {
		if got := sendInvalidate(t, app, headers); got != fiber.StatusForbidden {
			t.Fatalf("bad-MAC attempt %d returned %d, expected 403", i, got)
		}
	}

	// Restore the valid MAC and send the request — the nonce must NOT
	// have been tracked by the bad-MAC attempts above, so this is a
	// fresh-nonce success path (not a replay).
	headers["X-Arc-HMAC"] = originalMAC
	if got := sendInvalidate(t, app, headers); got != fiber.StatusInternalServerError {
		t.Errorf("valid-MAC request after bad-MAC flood returned %d, expected 500 (nonce was burned by bad-MAC attacker — ordering bug)", got)
	}
}
