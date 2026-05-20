package api

import (
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// cacheInvalidateTestHarness bundles a CacheInvalidateHandler with a
// running *fiber.App and an invalidation-count atomic the tests assert
// on. Every rejection branch returns 403 before onInvalidate runs, so
// the counter must stay at 0 for negative tests and increment by 1 on
// the single positive-path test.
type cacheInvalidateTestHarness struct {
	app           *fiber.App
	invalidations *atomic.Int32
}

func newCacheInvalidateTestHarness(t *testing.T, secret, clusterName, localNodeID string, nonceCache *security.NonceCache, tolerance time.Duration) *cacheInvalidateTestHarness {
	t.Helper()
	var invalidations atomic.Int32
	h := NewCacheInvalidateHandler(
		secret, clusterName, localNodeID, nonceCache, tolerance,
		func() { invalidations.Add(1) },
		zerolog.Nop(),
	)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.Register(app)
	return &cacheInvalidateTestHarness{app: app, invalidations: &invalidations}
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
	req := httptest.NewRequest("POST", CacheInvalidatePath, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp.StatusCode
}

// TestNewCacheInvalidateHandler_RejectsInvalidConfig pins the
// constructor's defensive guards: a misconfigured caller (empty
// strings, nil cache, zero tolerance, nil callback) panics at startup
// instead of producing a silently half-armed handler at runtime.
func TestNewCacheInvalidateHandler_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	good := func() (string, string, string, *security.NonceCache, time.Duration, func(), zerolog.Logger) {
		return "secret", "cluster-A", "node-1", security.NewNonceCache(5 * time.Minute), 5 * time.Minute, func() {}, zerolog.Nop()
	}

	mustPanic := func(name string, build func()) {
		t.Helper()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("%s: expected panic, got none", name)
			}
		}()
		build()
	}

	mustPanic("empty secret", func() {
		_, c, n, nc, tol, cb, lg := good()
		NewCacheInvalidateHandler("", c, n, nc, tol, cb, lg)
	})
	mustPanic("empty cluster name", func() {
		s, _, n, nc, tol, cb, lg := good()
		NewCacheInvalidateHandler(s, "", n, nc, tol, cb, lg)
	})
	mustPanic("empty local node ID", func() {
		s, c, _, nc, tol, cb, lg := good()
		NewCacheInvalidateHandler(s, c, "", nc, tol, cb, lg)
	})
	mustPanic("nil nonce cache", func() {
		s, c, n, _, tol, cb, lg := good()
		NewCacheInvalidateHandler(s, c, n, nil, tol, cb, lg)
	})
	mustPanic("zero tolerance", func() {
		s, c, n, nc, _, cb, lg := good()
		NewCacheInvalidateHandler(s, c, n, nc, 0, cb, lg)
	})
	mustPanic("negative tolerance", func() {
		s, c, n, nc, _, cb, lg := good()
		NewCacheInvalidateHandler(s, c, n, nc, -1*time.Second, cb, lg)
	})
	mustPanic("nil callback", func() {
		s, c, n, nc, tol, _, lg := good()
		NewCacheInvalidateHandler(s, c, n, nc, tol, nil, lg)
	})
}

// TestCacheInvalidate_ValidAuth_InvokesCallback is the positive path:
// a request with a fresh nonce + valid MAC must return 204 AND invoke
// the onInvalidate callback exactly once. Asserting both pins the
// post-auth wiring — a future refactor that makes the handler return
// early without invoking the callback (or invokes it twice) breaks
// this test.
func TestCacheInvalidate_ValidAuth_InvokesCallback(t *testing.T) {
	t.Parallel()
	const secret = "test-shared-secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	hx := newCacheInvalidateTestHarness(t, secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
	headers := validHeaders(t, secret, peerNodeID, clusterName)
	if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusNoContent {
		t.Errorf("expected 204 on valid auth, got %d", got)
	}
	if n := hx.invalidations.Load(); n != 1 {
		t.Errorf("expected onInvalidate to fire exactly once, got %d", n)
	}
}

// TestCacheInvalidate_MissingHeaders covers the early-return guards
// before any HMAC computation. Each individual missing header must
// produce 403 — leaking which header is missing would help an attacker
// debug their forgery attempt. The callback MUST NOT fire on any path.
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
			hx := newCacheInvalidateTestHarness(t, secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)
			headers := validHeaders(t, secret, peerNodeID, clusterName)
			delete(headers, c.drop)
			if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusForbidden {
				t.Errorf("expected 403 with %s, got %d", c.name, got)
			}
			if n := hx.invalidations.Load(); n != 0 {
				t.Errorf("%s: onInvalidate fired on a rejected request (count=%d)", c.name, n)
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

	hx := newCacheInvalidateTestHarness(t, secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)

	// Use localNodeID as the sender — a valid MAC for the local node ID
	// must still be refused because the local invalidation path is
	// in-process, not HTTP.
	headers := validHeaders(t, secret, localNodeID, clusterName)
	if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 for self-addressed request, got %d", got)
	}
	if n := hx.invalidations.Load(); n != 0 {
		t.Errorf("onInvalidate fired on a self-addressed request (count=%d)", n)
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

	hx := newCacheInvalidateTestHarness(t, secret, ourClusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)

	headers := validHeaders(t, secret, peerNodeID, "different-cluster")
	if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with mismatched cluster name, got %d", got)
	}
	if n := hx.invalidations.Load(); n != 0 {
		t.Errorf("onInvalidate fired on a cross-cluster request (count=%d)", n)
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

	hx := newCacheInvalidateTestHarness(t, ourSecret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)

	headers := validHeaders(t, "WRONG-secret", peerNodeID, clusterName)
	if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with wrong secret MAC, got %d", got)
	}
	if n := hx.invalidations.Load(); n != 0 {
		t.Errorf("onInvalidate fired on a wrong-secret request (count=%d)", n)
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

	hx := newCacheInvalidateTestHarness(t, secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)

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
	if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with stale timestamp, got %d", got)
	}
	if n := hx.invalidations.Load(); n != 0 {
		t.Errorf("onInvalidate fired on a stale-timestamp request (count=%d)", n)
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

	hx := newCacheInvalidateTestHarness(t, secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)

	headers := validHeaders(t, secret, peerNodeID, clusterName)
	headers["X-Arc-Timestamp"] = "not-a-number"
	if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusForbidden {
		t.Errorf("expected 403 with non-numeric timestamp, got %d", got)
	}
	if n := hx.invalidations.Load(); n != 0 {
		t.Errorf("onInvalidate fired on a non-numeric-timestamp request (count=%d)", n)
	}
}

// TestCacheInvalidate_Replay pins the nonce-cache check: a valid request
// that succeeds once must fail when replayed within the TTL window.
// First send: 204 + callback fires. Second send (same headers): 403
// (replay reject) + callback does NOT fire again.
func TestCacheInvalidate_Replay(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	hx := newCacheInvalidateTestHarness(t, secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)

	headers := validHeaders(t, secret, peerNodeID, clusterName)
	if first := sendInvalidate(t, hx.app, headers); first != fiber.StatusNoContent {
		t.Fatalf("first request: expected 204, got %d — auth setup may be broken", first)
	}
	if n := hx.invalidations.Load(); n != 1 {
		t.Fatalf("first request: expected onInvalidate count=1, got %d", n)
	}
	if second := sendInvalidate(t, hx.app, headers); second != fiber.StatusForbidden {
		t.Errorf("replay of valid request was accepted (got %d) — nonce cache is not preventing replay", second)
	}
	if n := hx.invalidations.Load(); n != 1 {
		t.Errorf("replay caused a second onInvalidate fire (count=%d) — replay reject is not gating the callback", n)
	}
}

// TestCacheInvalidate_NonceNotBurnedByBadMAC pins the ordering
// invariant: the nonce-cache Track() runs AFTER HMAC validation, so a
// flood of bad-MAC requests cannot consume cache entries for a nonce
// that a legitimate peer might later use. A future refactor that swaps
// the order would let an off-path attacker (no secret) deny service by
// burning nonces a real peer is about to present.
//
// Shape: send N requests with the SAME nonce but a bad MAC — each must
// 403 at the HMAC stage without tracking the nonce. Then send ONE
// request with the same nonce but a valid MAC — must reach 204 and
// fire onInvalidate. If the order were inverted, the bad-MAC requests
// would Track the nonce first and the valid request would 403 as a
// replay.
func TestCacheInvalidate_NonceNotBurnedByBadMAC(t *testing.T) {
	t.Parallel()
	const secret = "secret"
	const clusterName = "cluster-A"
	const localNodeID = "local-node"
	const peerNodeID = "peer-1"

	hx := newCacheInvalidateTestHarness(t, secret, clusterName, localNodeID, security.NewNonceCache(5*time.Minute), 5*time.Minute)

	// Build a valid header set, then keep the nonce/timestamp but
	// corrupt the MAC. Send several bad-MAC requests with the SAME nonce.
	headers := validHeaders(t, secret, peerNodeID, clusterName)
	originalMAC := headers["X-Arc-HMAC"]
	headers["X-Arc-HMAC"] = "deadbeef" + originalMAC[8:] // wrong bytes, same length shape

	const badAttempts = 5
	for i := 0; i < badAttempts; i++ {
		if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusForbidden {
			t.Fatalf("bad-MAC attempt %d returned %d, expected 403", i, got)
		}
	}
	if n := hx.invalidations.Load(); n != 0 {
		t.Fatalf("bad-MAC attempts fired onInvalidate (count=%d) — HMAC validation is broken", n)
	}

	// Restore the valid MAC and send the request — the nonce must NOT
	// have been tracked by the bad-MAC attempts above, so this is a
	// fresh-nonce success path (not a replay).
	headers["X-Arc-HMAC"] = originalMAC
	if got := sendInvalidate(t, hx.app, headers); got != fiber.StatusNoContent {
		t.Errorf("valid-MAC request after bad-MAC flood returned %d, expected 204 (nonce was burned by bad-MAC attacker — ordering bug)", got)
	}
	if n := hx.invalidations.Load(); n != 1 {
		t.Errorf("valid-MAC request after bad-MAC flood: expected onInvalidate count=1, got %d", n)
	}
}
