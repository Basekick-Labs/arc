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
	const want = "0babfa3cf90d1a85e9f55f37a896f489833470f1a39af0ffa9215e834eed15eb"
	if mac1 != want {
		t.Errorf("MAC drift detected:\n  got  %s\n  want %s\nIf this test fails, the message format changed — coordinate with the deployed-version matrix before merging.", mac1, want)
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
			err := ValidateCacheInvalidateHMAC("secret", c.nonce, c.nodeID, c.cluster, ts, mac, 5*time.Minute)
			if err == nil {
				t.Errorf("%s: tampered input accepted", c.name)
			}
		})
	}
}
