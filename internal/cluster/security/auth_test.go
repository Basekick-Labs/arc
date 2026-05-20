package security

import (
	"strings"
	"testing"
	"time"
)

// TestComputeCacheInvalidateHMAC_Determinism pins the contract that the
// same inputs always produce the same MAC. If the format string ever
// changes (e.g. a refactor reorders fields or drops the "cache-invalidate:"
// label), an in-progress rolling upgrade between Arc versions will stop
// being able to invalidate caches across peers — silent breakage in
// production. This test catches that.
func TestComputeCacheInvalidateHMAC_Determinism(t *testing.T) {
	t.Parallel()
	mac1 := ComputeCacheInvalidateHMAC("secret", "nonce-abc", "node-1", "cluster-A", 1700000000)
	mac2 := ComputeCacheInvalidateHMAC("secret", "nonce-abc", "node-1", "cluster-A", 1700000000)
	if mac1 != mac2 {
		t.Fatalf("same inputs produced different MACs:\n  %s\n  %s", mac1, mac2)
	}
	// Reference MAC pinned so a future refactor that subtly changes the
	// format string (e.g. swapping field order or dropping the label) is
	// caught immediately. If this hash changes, you are breaking the
	// on-wire format the receiver expects — bump the protocol version and
	// document the migration before changing the constant.
	const want = "6740a471e27f9657323ef9cb0013e50c42cd4a31e36ced84489e970697559878"
	if mac1 != want {
		t.Errorf("MAC drift detected:\n  got  %s\n  want %s\nIf this test fails, the message format changed — coordinate with the deployed-version matrix before merging.", mac1, want)
	}
}

// TestComputeHMAC_Determinism pins the on-wire format for the join HMAC
// (used by cluster.AuthenticatePeer). Same purpose as the cache-invalidate
// determinism test: a silent format change across an Arc upgrade silently
// breaks peer joins. Bump the protocol version and document the migration
// before changing the constant.
func TestComputeHMAC_Determinism(t *testing.T) {
	t.Parallel()
	got := ComputeHMAC("secret", "nonce-abc", "node-1", "cluster-A", 1700000000)
	const want = "49f7533612fce7403bc6496e7de19132b6f9812e5e6731bafeb6c3f5f18df2e4"
	if got != want {
		t.Errorf("join MAC drift detected:\n  got  %s\n  want %s\nIf this test fails, the join-endpoint message format changed — coordinate with the deployed-version matrix before merging.", got, want)
	}
}

// TestComputeFetchHMAC_Determinism pins the on-wire format for the
// peer-fetch HMAC (used by internal/cluster/filereplication.go).
func TestComputeFetchHMAC_Determinism(t *testing.T) {
	t.Parallel()
	got := ComputeFetchHMAC("secret", "nonce-abc", "node-1", "cluster-A", "/some/path", 1700000000)
	const want = "34181b01d618592db8955341970c40d1cefb7a8e624be25ab061d8b2e84f1c0a"
	if got != want {
		t.Errorf("fetch MAC drift detected:\n  got  %s\n  want %s\nIf this test fails, the fetch-endpoint message format changed — coordinate with the deployed-version matrix before merging.", got, want)
	}
}

// TestComputeForwardHMAC_Determinism pins the on-wire format for the
// leader-forwarding HMAC (used by internal/cluster/forward_apply.go).
// The payload "hello" hashes to a fixed SHA-256, which then feeds the
// outer HMAC, so this also indirectly pins that we still SHA-256 the
// payload before binding it (rather than e.g. switching to BLAKE3).
func TestComputeForwardHMAC_Determinism(t *testing.T) {
	t.Parallel()
	got := ComputeForwardHMAC("secret", "nonce-abc", "node-1", "cluster-A", []byte("hello"), 1700000000)
	const want = "f1bd7574435144fd4741832cc4985d31aa87d6d5e99f25a0434b6482a6c8b7cf"
	if got != want {
		t.Errorf("forward MAC drift detected:\n  got  %s\n  want %s\nIf this test fails, the forward-endpoint message format changed — coordinate with the deployed-version matrix before merging.", got, want)
	}
}

// TestValidate_RejectsMalformedHexMAC pins the new fail-closed behaviour
// of the hex-decode step in every Validate*HMAC. A non-hex receivedMAC
// must reject; previously the code compared hex strings directly, which
// also rejected, but now we explicitly decode hex first — the test pins
// that invalid hex doesn't slip through to hmac.Equal as raw bytes that
// might inadvertently match a recompute.
func TestValidate_RejectsMalformedHexMAC(t *testing.T) {
	t.Parallel()
	ts := time.Now().Unix()
	const malformed = "not-hex-at-all-zzzzzzzzzzzzzzzz"

	if err := ValidateHMAC("s", "n", "id", "c", ts, malformed, 5*time.Minute); err == nil {
		t.Error("ValidateHMAC accepted a non-hex MAC")
	}
	if err := ValidateFetchHMAC("s", "n", "id", "c", "/p", ts, malformed, 5*time.Minute); err == nil {
		t.Error("ValidateFetchHMAC accepted a non-hex MAC")
	}
	if err := ValidateForwardHMAC("s", "n", "id", "c", []byte("x"), ts, malformed, 5*time.Minute); err == nil {
		t.Error("ValidateForwardHMAC accepted a non-hex MAC")
	}
	if err := ValidateCacheInvalidateHMAC("s", "n", "id", "c", ts, malformed, 5*time.Minute); err == nil {
		t.Error("ValidateCacheInvalidateHMAC accepted a non-hex MAC")
	}
}

// TestValidateCacheInvalidateHMAC_Valid is the happy path.
func TestValidateCacheInvalidateHMAC_Valid(t *testing.T) {
	t.Parallel()
	ts := time.Now().Unix()
	mac := ComputeCacheInvalidateHMAC("secret", "nonce-abc", "node-1", "cluster-A", ts)
	if err := ValidateCacheInvalidateHMAC("secret", "nonce-abc", "node-1", "cluster-A", ts, mac, 5*time.Minute); err != nil {
		t.Fatalf("valid MAC rejected: %v", err)
	}
}

// TestValidateCacheInvalidateHMAC_WrongSecret pins HMAC integrity:
// an attacker who knows the nonce and metadata but not the shared secret
// cannot produce a valid MAC.
func TestValidateCacheInvalidateHMAC_WrongSecret(t *testing.T) {
	t.Parallel()
	ts := time.Now().Unix()
	mac := ComputeCacheInvalidateHMAC("correct-secret", "nonce", "node-1", "cluster-A", ts)
	err := ValidateCacheInvalidateHMAC("WRONG-secret", "nonce", "node-1", "cluster-A", ts, mac, 5*time.Minute)
	if err == nil {
		t.Fatal("wrong secret accepted — HMAC validation is broken")
	}
	if !strings.Contains(err.Error(), "shared secret mismatch") {
		t.Errorf("expected shared-secret-mismatch error, got: %v", err)
	}
}

// TestValidateCacheInvalidateHMAC_StaleTimestamp pins the freshness window.
// Replays older than the tolerance must be rejected even if the MAC is
// otherwise valid.
func TestValidateCacheInvalidateHMAC_StaleTimestamp(t *testing.T) {
	t.Parallel()
	staleTS := time.Now().Add(-10 * time.Minute).Unix()
	mac := ComputeCacheInvalidateHMAC("secret", "nonce", "node-1", "cluster-A", staleTS)
	err := ValidateCacheInvalidateHMAC("secret", "nonce", "node-1", "cluster-A", staleTS, mac, 5*time.Minute)
	if err == nil {
		t.Fatal("stale timestamp accepted — freshness check is broken")
	}
	if !strings.Contains(err.Error(), "timestamp expired") {
		t.Errorf("expected timestamp-expired error, got: %v", err)
	}
}

// TestValidateCacheInvalidateHMAC_FutureTimestamp pins symmetric drift
// rejection. A clock-skewed sender that's 10 minutes ahead must also be
// rejected, not silently accepted — symmetric tolerance prevents an
// attacker from holding a MAC and replaying it later by lying about the
// timestamp.
func TestValidateCacheInvalidateHMAC_FutureTimestamp(t *testing.T) {
	t.Parallel()
	futureTS := time.Now().Add(10 * time.Minute).Unix()
	mac := ComputeCacheInvalidateHMAC("secret", "nonce", "node-1", "cluster-A", futureTS)
	err := ValidateCacheInvalidateHMAC("secret", "nonce", "node-1", "cluster-A", futureTS, mac, 5*time.Minute)
	if err == nil {
		t.Fatal("future timestamp accepted — drift check is one-sided")
	}
}

// TestCacheInvalidateHMAC_LabelBinding_NoCrossEndpointReplay is the
// critical security property: a MAC computed for one endpoint must NOT
// validate against the verifier for a different endpoint, even with
// identical (nonce, nodeID, clusterName, timestamp) inputs.
//
// Without the "cache-invalidate:" / "forward" / "fetch" / "join" labels
// in the message format, a leaked Forward MAC could be replayed against
// the cache-invalidate endpoint within the 5-minute freshness window.
// This test ensures the labels make that impossible across every existing
// HMAC family (Forward, Fetch, Join) in both directions.
func TestCacheInvalidateHMAC_LabelBinding_NoCrossEndpointReplay(t *testing.T) {
	t.Parallel()
	const (
		secret      = "secret"
		nonce       = "nonce-abc"
		nodeID      = "node-1"
		clusterName = "cluster-A"
		fetchPath   = "/some/path"
	)
	ts := time.Now().Unix()

	// Compute MACs for every endpoint with identical inputs.
	cacheMAC := ComputeCacheInvalidateHMAC(secret, nonce, nodeID, clusterName, ts)
	forwardMAC := ComputeForwardHMAC(secret, nonce, nodeID, clusterName, []byte{}, ts)
	fetchMAC := ComputeFetchHMAC(secret, nonce, nodeID, clusterName, fetchPath, ts)
	joinMAC := ComputeHMAC(secret, nonce, nodeID, clusterName, ts)

	if cacheMAC == forwardMAC {
		t.Error("cache-invalidate MAC collided with forward MAC — cross-endpoint replay possible")
	}
	if cacheMAC == fetchMAC {
		t.Error("cache-invalidate MAC collided with fetch MAC — cross-endpoint replay possible")
	}
	if cacheMAC == joinMAC {
		t.Error("cache-invalidate MAC collided with join MAC — cross-endpoint replay possible")
	}

	// Verify the cache-invalidate validator rejects MACs computed for the
	// other endpoints, even when (nonce, nodeID, clusterName, timestamp)
	// match. This is the direct exploit-shape the labels prevent.
	if err := ValidateCacheInvalidateHMAC(secret, nonce, nodeID, clusterName, ts, forwardMAC, 5*time.Minute); err == nil {
		t.Error("forward MAC accepted by cache-invalidate validator — endpoint labels not binding")
	}
	if err := ValidateCacheInvalidateHMAC(secret, nonce, nodeID, clusterName, ts, fetchMAC, 5*time.Minute); err == nil {
		t.Error("fetch MAC accepted by cache-invalidate validator — endpoint labels not binding")
	}
	if err := ValidateCacheInvalidateHMAC(secret, nonce, nodeID, clusterName, ts, joinMAC, 5*time.Minute); err == nil {
		t.Error("join MAC accepted by cache-invalidate validator — endpoint labels not binding")
	}

	// And the reverse: a cache-invalidate MAC must not be accepted by
	// the other validators (empty payload / matching path to align inputs).
	if err := ValidateForwardHMAC(secret, nonce, nodeID, clusterName, []byte{}, ts, cacheMAC, 5*time.Minute); err == nil {
		t.Error("cache-invalidate MAC accepted by forward validator — endpoint labels not binding")
	}
	if err := ValidateFetchHMAC(secret, nonce, nodeID, clusterName, fetchPath, ts, cacheMAC, 5*time.Minute); err == nil {
		t.Error("cache-invalidate MAC accepted by fetch validator — endpoint labels not binding")
	}
}

// TestValidateCacheInvalidateHMAC_FieldBinding pins per-field tampering
// rejection: an attacker who flips any one field in the verification call
// (nonce / nodeID / clusterName) must be rejected. Catches a future bug
// where ValidateCacheInvalidateHMAC stops including one of the fields in
// its recomputation.
func TestValidateCacheInvalidateHMAC_FieldBinding(t *testing.T) {
	t.Parallel()
	ts := time.Now().Unix()
	mac := ComputeCacheInvalidateHMAC("secret", "nonce", "node-1", "cluster-A", ts)

	cases := []struct {
		name, nonce, nodeID, cluster string
	}{
		{"nonce tampered", "nonce-X", "node-1", "cluster-A"},
		{"nodeID tampered", "nonce", "node-2", "cluster-A"},
		{"cluster tampered", "nonce", "node-1", "cluster-B"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCacheInvalidateHMAC("secret", c.nonce, c.nodeID, c.cluster, ts, mac, 5*time.Minute)
			if err == nil {
				t.Errorf("%s: tampered input accepted", c.name)
			}
		})
	}
}
