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

// TestHandshakeHMAC_Determinism (above) pins the on-wire format for the
// handshake family. This section covers the other MAC families.
// (used by cluster.AuthenticatePeer). Same purpose as the cache-invalidate
// Determinism tests pin the on-wire format of the coordinator handshake
// family. A silent format change across an Arc upgrade breaks peer joins,
// heartbeats and leaves cluster-wide, so these constants are a deliberate
// tripwire: if one fails, the wire format changed and the upgrade is a
// lockstep cutover that must be documented in the release notes before merge.
//
// The format changed once already, in 26.09.2 (GHSA-p2rx): from a
// NUL-delimited {msgType, nonce, nodeID, clusterName, timestamp} to
// length-prefixed canonicalization over EVERY field of the message.
func TestHandshakeHMAC_Determinism(t *testing.T) {
	t.Parallel()
	const (
		secret = "secret"
		nonce  = "nonce-abc"
		ts     = int64(1700000000)
	)

	join := JoinAuthFields{
		NodeID: "node-1", NodeName: "node-one", Role: "writer", ClusterName: "cluster-A",
		RaftAddr: "10.0.0.1:9200", APIAddr: "10.0.0.1:8080", CoordAddr: "10.0.0.1:9100",
		Version: "26.09.2", CoreCount: 8,
	}
	hb := HeartbeatAuthFields{
		NodeID: "node-1", ClusterName: "cluster-A", State: "healthy",
		IsLeader: true, TimestampUnixNano: 1700000000000000000,
	}
	leave := LeaveAuthFields{NodeID: "node-1", ClusterName: "cluster-A", Reason: "graceful shutdown"}
	resp := JoinResponseAuthFields{
		Success: true, LeaderID: "node-0", LeaderAddr: "10.0.0.9:9100", RaftLeader: "10.0.0.9:9200",
		Nodes: []NodeAuthFields{{
			ID: "node-0", Name: "node-zero", Role: "writer", State: "healthy",
			RaftAddr: "", APIAddr: "10.0.0.9:8080", CoordAddr: "10.0.0.9:9100", CoreCount: 4,
		}},
	}
	info := LeaderInfoAuthFields{LeaderID: "node-0", LeaderCoordAddr: "10.0.0.9:9100", LeaderRaftAddr: "10.0.0.9:9200"}
	ack := ForwardAckAuthFields{Status: "ok"}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"join", ComputeJoinHMAC(secret, nonce, ts, join), "0922d84d08e2d183f88409626819c5c3300d1d2936502aa1bd3a91bdc41ab31c"},
		{"heartbeat", ComputeHeartbeatHMAC(secret, nonce, ts, hb), "1fde9f8fb75119f2adb21833d40fabe636b9fbba796bab39e075ca10808f3754"},
		{"leave", ComputeLeaveHMAC(secret, nonce, ts, leave), "2bc824a7ff74363b93f88d3545fca251b56a63d61c47556f88a2726caf39be48"},
		{"join-resp", ComputeJoinResponseHMAC(secret, nonce, ts, resp), "3f5aa4d563b6e0c2c0cee2eea5278379d05729c09ddbe0364c3dae56a2452332"},
		{"leader-info", ComputeLeaderInfoHMAC(secret, nonce, ts, info), "ddf8663bc24ba19d22c41a28e91fe7398f88f0bd640364f3dd8270b4f1c4e8cb"},
		{"forward-ack", ComputeForwardAckHMAC(secret, nonce, ts, ack), "2f52aac26df0e65fd07593bb2b5b9bb6cbbcd9a9b7204026e775b0117ea8c759"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("%s MAC drift:\n  got  %s\n  want %s\nThe handshake wire format changed — coordinate the lockstep upgrade and update the release notes before merging.", tc.name, tc.got, tc.want)
		}
	}
}

// TestHandshakeHMAC_LabelBinding_NoCrossMessageTypeReplay is the critical
// security property from #504, carried forward to the 26.09.2 format: a MAC
// computed for one message type must not validate as another, even with
// identical nonce/timestamp inputs.
//
// Without the per-type label a heartbeat MAC could be replayed as a
// node-evicting leave inside the freshness window. The response labels matter
// for the same reason in the other direction — a join response replayed as a
// leader redirect would send a joiner to an address of the attacker's choosing.
func TestHandshakeHMAC_LabelBinding_NoCrossMessageTypeReplay(t *testing.T) {
	t.Parallel()
	const (
		secret  = "secret"
		nonce   = "nonce-abc"
		cluster = "cluster-A"
		nodeID  = "node-1"
	)
	ts := time.Now().Unix()

	// Same NodeID/ClusterName/nonce/timestamp across every type, so only the
	// label can be what distinguishes them.
	join := JoinAuthFields{NodeID: nodeID, ClusterName: cluster}
	hb := HeartbeatAuthFields{NodeID: nodeID, ClusterName: cluster}
	leave := LeaveAuthFields{NodeID: nodeID, ClusterName: cluster}
	resp := JoinResponseAuthFields{LeaderID: nodeID}
	info := LeaderInfoAuthFields{LeaderID: nodeID}
	ack := ForwardAckAuthFields{Status: nodeID}

	macs := map[string]string{
		"join":        ComputeJoinHMAC(secret, nonce, ts, join),
		"heartbeat":   ComputeHeartbeatHMAC(secret, nonce, ts, hb),
		"leave":       ComputeLeaveHMAC(secret, nonce, ts, leave),
		"join-resp":   ComputeJoinResponseHMAC(secret, nonce, ts, resp),
		"leader-info": ComputeLeaderInfoHMAC(secret, nonce, ts, info),
		"forward-ack": ComputeForwardAckHMAC(secret, nonce, ts, ack),
	}

	// Every MAC must be distinct — a collision is trivially replayable.
	seen := make(map[string]string, len(macs))
	for label, mac := range macs {
		if other, dup := seen[mac]; dup {
			t.Errorf("%s and %s produced the same MAC — cross-message-type replay possible", label, other)
		}
		seen[mac] = label
	}

	// Each validator accepts only its own label's MAC.
	validators := map[string]func(mac string) error{
		"join":        func(mac string) error { return ValidateJoinHMAC(secret, nonce, ts, join, mac, 5*time.Minute) },
		"heartbeat":   func(mac string) error { return ValidateHeartbeatHMAC(secret, nonce, ts, hb, mac, 5*time.Minute) },
		"leave":       func(mac string) error { return ValidateLeaveHMAC(secret, nonce, ts, leave, mac, 5*time.Minute) },
		"join-resp":   func(mac string) error { return ValidateJoinResponseHMAC(secret, nonce, ts, resp, mac, 5*time.Minute) },
		"leader-info": func(mac string) error { return ValidateLeaderInfoHMAC(secret, nonce, ts, info, mac, 5*time.Minute) },
		"forward-ack": func(mac string) error { return ValidateForwardAckHMAC(secret, nonce, ts, ack, mac, 5*time.Minute) },
	}
	for vLabel, validate := range validators {
		for mLabel, mac := range macs {
			err := validate(mac)
			if vLabel == mLabel && err != nil {
				t.Errorf("%s validator rejected its own MAC: %v", vLabel, err)
			}
			if vLabel != mLabel && err == nil {
				t.Errorf("%s MAC accepted by %s validator — label binding broken", mLabel, vLabel)
			}
		}
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

	if err := ValidateJoinHMAC("s", "n", ts, JoinAuthFields{NodeID: "id", ClusterName: "c"}, malformed, 5*time.Minute); err == nil {
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
	joinMAC := ComputeJoinHMAC(secret, nonce, ts, JoinAuthFields{NodeID: nodeID, ClusterName: clusterName})

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
	// Also the join-family validator must reject MACs from other endpoints.
	if err := ValidateJoinHMAC(secret, nonce, ts, JoinAuthFields{NodeID: nodeID, ClusterName: clusterName}, cacheMAC, 5*time.Minute); err == nil {
		t.Error("cache-invalidate MAC accepted by join validator — endpoint labels not binding")
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

// ----------------------------------------------------------------------
// Replication protocol HMAC tests (GHSA-wfgr-8x84-22q7 / CVE-2026-48106)
// ----------------------------------------------------------------------

func TestHMAC_LabelBinding_NoCrossEndpointReplay(t *testing.T) {
	t.Parallel()
	const (
		secret      = "secret"
		nonce       = "nonce-abc"
		nodeID      = "node-1"
		clusterName = "cluster-A"
		fetchPath   = "/some/path"
	)
	ts := time.Now().Unix()

	cacheMAC := ComputeCacheInvalidateHMAC(secret, nonce, nodeID, clusterName, ts)
	forwardMAC := ComputeForwardHMAC(secret, nonce, nodeID, clusterName, []byte{}, ts)
	fetchMAC := ComputeFetchHMAC(secret, nonce, nodeID, clusterName, fetchPath, ts)
	joinMAC := ComputeJoinHMAC(secret, nonce, ts, JoinAuthFields{NodeID: nodeID, ClusterName: clusterName})

	// All MAC families must produce distinct values for identical-ish inputs.
	macs := map[string]string{"cache": cacheMAC, "forward": forwardMAC, "fetch": fetchMAC, "join": joinMAC}
	for nameA, macA := range macs {
		for nameB, macB := range macs {
			if nameA < nameB && macA == macB {
				t.Errorf("MAC collision: %s == %s — cross-endpoint replay possible", nameA, nameB)
			}
		}
	}
	if err := ValidateCacheInvalidateHMAC(secret, nonce, nodeID, clusterName, ts, forwardMAC, 5*time.Minute); err == nil {
		t.Error("forward MAC accepted by cache-invalidate validator")
	}
	if err := ValidateForwardHMAC(secret, nonce, nodeID, clusterName, []byte{}, ts, cacheMAC, 5*time.Minute); err == nil {
		t.Error("cache MAC accepted by forward validator")
	}
	if err := ValidateFetchHMAC(secret, nonce, nodeID, clusterName, fetchPath, ts, joinMAC, 5*time.Minute); err == nil {
		t.Error("join MAC accepted by fetch validator")
	}
	if err := ValidateJoinHMAC(secret, nonce, ts, JoinAuthFields{NodeID: nodeID, ClusterName: clusterName}, cacheMAC, 5*time.Minute); err == nil {
		t.Error("cache MAC accepted by join validator")
	}
}
