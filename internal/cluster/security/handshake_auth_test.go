package security

import (
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	hsSecret = "handshake-test-secret"
	hsNonce  = "handshake-test-nonce"
)

func hsTS() int64 { return time.Now().Unix() }

// baseJoin is a fully-populated join request. Every field is non-zero so that
// a mutation test can meaningfully change it.
func baseJoin() JoinAuthFields {
	return JoinAuthFields{
		NodeID: "node-1", NodeName: "node-one", Role: "reader", ClusterName: "cluster-A",
		RaftAddr: "10.0.0.2:9200", APIAddr: "10.0.0.2:8080", CoordAddr: "10.0.0.2:9100",
		Version: "26.09.2", CoreCount: 4,
	}
}

// TestJoinHMAC_EveryFieldIsBound is the regression test for GHSA-p2rx.
//
// The reported vulnerability was precisely that Role and the advertised
// addresses sat OUTSIDE the MAC: an on-path attacker could promote a joining
// node to writer and redirect its coordinator/API/Raft addresses while the tag
// still verified. Each case below mutates exactly one field after signing and
// requires rejection. The unmodified control proves the validator is not
// simply rejecting everything.
func TestJoinHMAC_EveryFieldIsBound(t *testing.T) {
	t.Parallel()
	ts := hsTS()
	mac := ComputeJoinHMAC(hsSecret, hsNonce, ts, baseJoin())

	if err := ValidateJoinHMAC(hsSecret, hsNonce, ts, baseJoin(), mac, time.Minute); err != nil {
		t.Fatalf("control: unmodified join rejected: %v", err)
	}

	mutations := map[string]func(*JoinAuthFields){
		"NodeID":      func(f *JoinAuthFields) { f.NodeID = "attacker-node" },
		"NodeName":    func(f *JoinAuthFields) { f.NodeName = "attacker-name" },
		"Role":        func(f *JoinAuthFields) { f.Role = "writer" },
		"ClusterName": func(f *JoinAuthFields) { f.ClusterName = "cluster-B" },
		"RaftAddr":    func(f *JoinAuthFields) { f.RaftAddr = "attacker.example:9200" },
		"APIAddr":     func(f *JoinAuthFields) { f.APIAddr = "attacker.example:8080" },
		"CoordAddr":   func(f *JoinAuthFields) { f.CoordAddr = "attacker.example:9100" },
		"Version":     func(f *JoinAuthFields) { f.Version = "0.0.1" },
		"CoreCount":   func(f *JoinAuthFields) { f.CoreCount = 1024 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f := baseJoin()
			mutate(&f)
			if err := ValidateJoinHMAC(hsSecret, hsNonce, ts, f, mac, time.Minute); err == nil {
				t.Errorf("mutating %s did not invalidate the join MAC — the field is outside the signed payload", name)
			}
		})
	}
}

// TestHeartbeatHMAC_EveryFieldIsBound covers the variant found alongside
// GHSA-p2rx: State is written straight into the registry by the receiver, so
// an unsigned State lets an attacker mark any node unhealthy.
func TestHeartbeatHMAC_EveryFieldIsBound(t *testing.T) {
	t.Parallel()
	base := func() HeartbeatAuthFields {
		return HeartbeatAuthFields{
			NodeID: "node-1", ClusterName: "cluster-A", State: "healthy",
			IsLeader: false, TimestampUnixNano: 1700000000000000000,
		}
	}
	ts := hsTS()
	mac := ComputeHeartbeatHMAC(hsSecret, hsNonce, ts, base())

	if err := ValidateHeartbeatHMAC(hsSecret, hsNonce, ts, base(), mac, time.Minute); err != nil {
		t.Fatalf("control: unmodified heartbeat rejected: %v", err)
	}

	mutations := map[string]func(*HeartbeatAuthFields){
		"NodeID":            func(f *HeartbeatAuthFields) { f.NodeID = "other-node" },
		"ClusterName":       func(f *HeartbeatAuthFields) { f.ClusterName = "cluster-B" },
		"State":             func(f *HeartbeatAuthFields) { f.State = "unhealthy" },
		"IsLeader":          func(f *HeartbeatAuthFields) { f.IsLeader = true },
		"TimestampUnixNano": func(f *HeartbeatAuthFields) { f.TimestampUnixNano = 1 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f := base()
			mutate(&f)
			if err := ValidateHeartbeatHMAC(hsSecret, hsNonce, ts, f, mac, time.Minute); err == nil {
				t.Errorf("mutating %s did not invalidate the heartbeat MAC", name)
			}
		})
	}
}

func TestLeaveHMAC_EveryFieldIsBound(t *testing.T) {
	t.Parallel()
	base := func() LeaveAuthFields {
		return LeaveAuthFields{NodeID: "node-1", ClusterName: "cluster-A", Reason: "graceful shutdown"}
	}
	ts := hsTS()
	mac := ComputeLeaveHMAC(hsSecret, hsNonce, ts, base())

	if err := ValidateLeaveHMAC(hsSecret, hsNonce, ts, base(), mac, time.Minute); err != nil {
		t.Fatalf("control: unmodified leave rejected: %v", err)
	}

	mutations := map[string]func(*LeaveAuthFields){
		"NodeID":      func(f *LeaveAuthFields) { f.NodeID = "victim-node" },
		"ClusterName": func(f *LeaveAuthFields) { f.ClusterName = "cluster-B" },
		"Reason":      func(f *LeaveAuthFields) { f.Reason = "evicted" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f := base()
			mutate(&f)
			if err := ValidateLeaveHMAC(hsSecret, hsNonce, ts, f, mac, time.Minute); err == nil {
				t.Errorf("mutating %s did not invalidate the leave MAC", name)
			}
		})
	}
}

func baseJoinResponse() JoinResponseAuthFields {
	return JoinResponseAuthFields{
		Success: true, LeaderID: "node-0", LeaderAddr: "10.0.0.9:9100", RaftLeader: "10.0.0.9:9200",
		Nodes: []NodeAuthFields{
			{ID: "node-0", Name: "zero", Role: "writer", State: "healthy", RaftAddr: "10.0.0.9:9200", APIAddr: "10.0.0.9:8080", CoordAddr: "10.0.0.9:9100", CoreCount: 4},
			{ID: "node-1", Name: "one", Role: "reader", State: "healthy", RaftAddr: "10.0.0.8:9200", APIAddr: "10.0.0.8:8080", CoordAddr: "10.0.0.8:9100", CoreCount: 2},
		},
	}
}

// TestJoinResponseHMAC_EveryFieldIsBound covers the response direction: the
// joiner writes every returned NodeInfo into its own registry, so an unsigned
// peer list lets an attacker point this node's replication and forwarding at
// hosts of its choosing.
//
// The Nodes slice needs more than per-field mutation — adding, removing or
// reordering entries must all invalidate too, which is what the length prefix
// and per-node field expansion are for.
func TestJoinResponseHMAC_EveryFieldIsBound(t *testing.T) {
	t.Parallel()
	ts := hsTS()
	mac := ComputeJoinResponseHMAC(hsSecret, hsNonce, ts, baseJoinResponse())

	if err := ValidateJoinResponseHMAC(hsSecret, hsNonce, ts, baseJoinResponse(), mac, time.Minute); err != nil {
		t.Fatalf("control: unmodified join response rejected: %v", err)
	}

	mutations := map[string]func(*JoinResponseAuthFields){
		"Success":          func(f *JoinResponseAuthFields) { f.Success = false },
		"LeaderID":         func(f *JoinResponseAuthFields) { f.LeaderID = "attacker" },
		"LeaderAddr":       func(f *JoinResponseAuthFields) { f.LeaderAddr = "attacker.example:9100" },
		"RaftLeader":       func(f *JoinResponseAuthFields) { f.RaftLeader = "attacker.example:9200" },
		"Error":            func(f *JoinResponseAuthFields) { f.Error = "injected" },
		"node field":       func(f *JoinResponseAuthFields) { f.Nodes[1].CoordAddr = "attacker.example:9100" },
		"node role":        func(f *JoinResponseAuthFields) { f.Nodes[1].Role = "writer" },
		"node added":       func(f *JoinResponseAuthFields) { f.Nodes = append(f.Nodes, NodeAuthFields{ID: "ghost"}) },
		"node removed":     func(f *JoinResponseAuthFields) { f.Nodes = f.Nodes[:1] },
		"nodes reordered":  func(f *JoinResponseAuthFields) { f.Nodes[0], f.Nodes[1] = f.Nodes[1], f.Nodes[0] },
		"nodes emptied":    func(f *JoinResponseAuthFields) { f.Nodes = nil },
		"node core count":  func(f *JoinResponseAuthFields) { f.Nodes[0].CoreCount = 9999 },
		"node state":       func(f *JoinResponseAuthFields) { f.Nodes[0].State = "unhealthy" },
		"node api address": func(f *JoinResponseAuthFields) { f.Nodes[0].APIAddr = "attacker.example:8080" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f := baseJoinResponse()
			mutate(&f)
			if err := ValidateJoinResponseHMAC(hsSecret, hsNonce, ts, f, mac, time.Minute); err == nil {
				t.Errorf("mutating %q did not invalidate the join-response MAC", name)
			}
		})
	}
}

func TestLeaderInfoHMAC_EveryFieldIsBound(t *testing.T) {
	t.Parallel()
	base := func() LeaderInfoAuthFields {
		return LeaderInfoAuthFields{LeaderID: "node-0", LeaderCoordAddr: "10.0.0.9:9100", LeaderRaftAddr: "10.0.0.9:9200"}
	}
	ts := hsTS()
	mac := ComputeLeaderInfoHMAC(hsSecret, hsNonce, ts, base())

	if err := ValidateLeaderInfoHMAC(hsSecret, hsNonce, ts, base(), mac, time.Minute); err != nil {
		t.Fatalf("control: unmodified leader info rejected: %v", err)
	}

	mutations := map[string]func(*LeaderInfoAuthFields){
		"LeaderID":        func(f *LeaderInfoAuthFields) { f.LeaderID = "attacker" },
		"LeaderCoordAddr": func(f *LeaderInfoAuthFields) { f.LeaderCoordAddr = "attacker.example:9100" },
		"LeaderRaftAddr":  func(f *LeaderInfoAuthFields) { f.LeaderRaftAddr = "attacker.example:9200" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f := base()
			mutate(&f)
			if err := ValidateLeaderInfoHMAC(hsSecret, hsNonce, ts, f, mac, time.Minute); err == nil {
				t.Errorf("mutating %s did not invalidate the leader-info MAC", name)
			}
		})
	}
}

// TestForwardAckHMAC_EveryFieldIsBound: Code drives caller retry behaviour, so
// it must be bound as tightly as Status.
func TestForwardAckHMAC_EveryFieldIsBound(t *testing.T) {
	t.Parallel()
	base := func() ForwardAckAuthFields {
		return ForwardAckAuthFields{Status: "error", Code: "apply_failed", Error: "boom"}
	}
	ts := hsTS()
	mac := ComputeForwardAckHMAC(hsSecret, hsNonce, ts, base())

	if err := ValidateForwardAckHMAC(hsSecret, hsNonce, ts, base(), mac, time.Minute); err != nil {
		t.Fatalf("control: unmodified ack rejected: %v", err)
	}

	mutations := map[string]func(*ForwardAckAuthFields){
		"Status": func(f *ForwardAckAuthFields) { f.Status = "ok" },
		"Code":   func(f *ForwardAckAuthFields) { f.Code = "not_leader" },
		"Error":  func(f *ForwardAckAuthFields) { f.Error = "token already exists" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f := base()
			mutate(&f)
			if err := ValidateForwardAckHMAC(hsSecret, hsNonce, ts, f, mac, time.Minute); err == nil {
				t.Errorf("mutating %s did not invalidate the forward-ack MAC", name)
			}
		})
	}
}

// TestHandshakeHMAC_NonceIsBound: a response MAC is bound to the request's
// nonce, which is what makes a captured response un-replayable against a
// later request without the requester keeping a nonce cache.
func TestHandshakeHMAC_NonceIsBound(t *testing.T) {
	t.Parallel()
	ts := hsTS()
	mac := ComputeJoinResponseHMAC(hsSecret, hsNonce, ts, baseJoinResponse())
	if err := ValidateJoinResponseHMAC(hsSecret, "a-different-nonce", ts, baseJoinResponse(), mac, time.Minute); err == nil {
		t.Error("a join response validated against a nonce other than the request's — captured responses would be replayable")
	}
}

// TestHandshakeHMAC_FieldSplittingIsNotPossible guards the canonicalization
// choice itself.
//
// With a naive delimiter-joined encoding, field contents containing the
// delimiter can be re-partitioned: here ("a", "b|c") and ("a|b", "c") would
// collide. Length-prefixing makes the partition unambiguous, which is why the
// handshake family uses canonicalSyncInput rather than the older NUL join —
// this transport is raw TCP carrying JSON and passes any byte through.
func TestHandshakeHMAC_FieldSplittingIsNotPossible(t *testing.T) {
	t.Parallel()
	ts := hsTS()

	a := baseJoin()
	a.NodeName = "one"
	a.Role = "two:three"

	b := baseJoin()
	b.NodeName = "one:two"
	b.Role = "three"

	if ComputeJoinHMAC(hsSecret, hsNonce, ts, a) == ComputeJoinHMAC(hsSecret, hsNonce, ts, b) {
		t.Error("two distinct field tuples produced the same MAC — the canonical encoding is ambiguous")
	}

	// The same property with NUL bytes, the delimiter the previous format used.
	c := baseJoin()
	c.NodeName = "one\x00two"
	d := baseJoin()
	d.NodeName = "one"
	d.Role = "two"
	if ComputeJoinHMAC(hsSecret, hsNonce, ts, c) == ComputeJoinHMAC(hsSecret, hsNonce, ts, d) {
		t.Error("a NUL-containing field collided with a different field tuple")
	}
}

// stubGuard records Track calls so tests can assert the nonce is consumed
// only after the MAC verifies.
type stubGuard struct {
	seen  map[string]bool
	calls int
}

func newStubGuard() *stubGuard { return &stubGuard{seen: map[string]bool{}} }

func (g *stubGuard) Track(id, nonce string) bool {
	g.calls++
	key := id + "\x00" + nonce
	if g.seen[key] {
		return false
	}
	g.seen[key] = true
	return true
}

// TestValidateWithReplay_RejectsReplayAndFailsClosed covers the three
// properties the wrapper exists for.
func TestValidateWithReplay_RejectsReplayAndFailsClosed(t *testing.T) {
	t.Parallel()
	ts := hsTS()
	f := baseJoin()
	mac := ComputeJoinHMAC(hsSecret, hsNonce, ts, f)

	t.Run("first use accepted, second rejected as replay", func(t *testing.T) {
		g := newStubGuard()
		if err := ValidateJoinHMACWithReplay(g, hsSecret, hsNonce, ts, f, mac, time.Minute); err != nil {
			t.Fatalf("first use rejected: %v", err)
		}
		err := ValidateJoinHMACWithReplay(g, hsSecret, hsNonce, ts, f, mac, time.Minute)
		if !errors.Is(err, ErrHandshakeReplay) {
			t.Errorf("replayed message: got %v, want ErrHandshakeReplay", err)
		}
	})

	t.Run("typed-nil guard fails closed without panicking", func(t *testing.T) {
		// A *NonceCache that is nil but stored in a ReplayGuard interface is
		// not == nil at the interface level. Before this was handled, the
		// wrapper's `guard == nil` check missed it and Track panicked on the
		// nil receiver — i.e. any handshake reaching a Coordinator whose
		// cache had not been installed would crash the node rather than
		// reject the message.
		var nilCache *NonceCache
		err := ValidateJoinHMACWithReplay(nilCache, hsSecret, hsNonce, ts, f, mac, time.Minute)
		if err == nil {
			t.Fatal("a typed-nil replay guard was accepted")
		}
		if errors.Is(err, ErrHandshakeReplay) {
			t.Error("a nil guard should not report a replay; it is a construction error")
		}
		// And the underlying cache must itself be nil-safe, so a guard
		// implementation this package does not know about still fails closed.
		if nilCache.Track("node", "nonce") {
			t.Error("Track on a nil *NonceCache reported the nonce as new")
		}
	})

	t.Run("nil guard fails closed", func(t *testing.T) {
		err := ValidateJoinHMACWithReplay(nil, hsSecret, hsNonce, ts, f, mac, time.Minute)
		if err == nil {
			t.Fatal("a nil replay guard was accepted — the message would be replayable for the whole freshness window")
		}
		if errors.Is(err, ErrHandshakeReplay) {
			t.Error("a nil guard should not report a replay; it is a construction error")
		}
	})

	t.Run("forged MAC does not consume a nonce", func(t *testing.T) {
		// Otherwise an attacker could burn the cache slot for a nonce a
		// legitimate peer is about to use, turning replay protection into a
		// denial-of-service primitive.
		g := newStubGuard()
		if err := ValidateJoinHMACWithReplay(g, hsSecret, hsNonce, ts, f, "00ff", time.Minute); err == nil {
			t.Fatal("forged MAC accepted")
		}
		if g.calls != 0 {
			t.Errorf("forged MAC consumed a nonce (%d Track calls) — attacker could lock out legitimate nonces", g.calls)
		}
	})

	t.Run("heartbeat and leave wrappers behave the same", func(t *testing.T) {
		hbFields := HeartbeatAuthFields{NodeID: "node-1", ClusterName: "cluster-A", State: "healthy"}
		hbMAC := ComputeHeartbeatHMAC(hsSecret, hsNonce, ts, hbFields)
		g := newStubGuard()
		if err := ValidateHeartbeatHMACWithReplay(g, hsSecret, hsNonce, ts, hbFields, hbMAC, time.Minute); err != nil {
			t.Fatalf("heartbeat first use: %v", err)
		}
		if err := ValidateHeartbeatHMACWithReplay(g, hsSecret, hsNonce, ts, hbFields, hbMAC, time.Minute); !errors.Is(err, ErrHandshakeReplay) {
			t.Errorf("heartbeat replay: got %v", err)
		}

		lvFields := LeaveAuthFields{NodeID: "node-1", ClusterName: "cluster-A", Reason: "bye"}
		lvMAC := ComputeLeaveHMAC(hsSecret, hsNonce, ts, lvFields)
		g2 := newStubGuard()
		if err := ValidateLeaveHMACWithReplay(g2, hsSecret, hsNonce, ts, lvFields, lvMAC, time.Minute); err != nil {
			t.Fatalf("leave first use: %v", err)
		}
		if err := ValidateLeaveHMACWithReplay(g2, hsSecret, hsNonce, ts, lvFields, lvMAC, time.Minute); !errors.Is(err, ErrHandshakeReplay) {
			t.Errorf("leave replay: got %v", err)
		}
	})
}

// TestNonceCacheOutlivesFreshnessWindow pins the lifetime arithmetic in
// NewNonceCache.
//
// A nonce must stay in the cache for as long as a MAC bearing it could still
// be accepted. Because a message may be stamped up to one tolerance in the
// FUTURE, and the validator truncates its own clock to whole seconds, the
// cache has to outlive the tolerance by more than the tolerance itself —
// hence 2T+1s rather than T. With T as the TTL, a peer whose clock runs ahead
// gets its nonce evicted while its MAC is still fresh, and the replay is
// accepted.
func TestNonceCacheOutlivesFreshnessWindow(t *testing.T) {
	t.Parallel()
	const tolerance = 30 * time.Second
	nc := NewNonceCache(tolerance)

	if got, want := nc.ttl, 2*tolerance+time.Second; got != want {
		t.Errorf("nonce cache lifetime = %v, want %v (2*tolerance + 1s)", got, want)
	}
	if nc.ttl <= tolerance {
		t.Error("nonce cache lifetime does not exceed the freshness window — a future-dated message outlives its cache slot")
	}
}

// TestHandshakeFreshness_RejectsStaleAndFuture: the window is symmetric, so a
// clock-skewed peer in either direction is rejected rather than silently
// trusted.
func TestHandshakeFreshness_RejectsStaleAndFuture(t *testing.T) {
	t.Parallel()
	f := baseJoin()
	tol := 30 * time.Second

	stale := time.Now().Add(-2 * time.Minute).Unix()
	if err := ValidateJoinHMAC(hsSecret, hsNonce, stale, f, ComputeJoinHMAC(hsSecret, hsNonce, stale, f), tol); err == nil {
		t.Error("a stale timestamp was accepted")
	}
	future := time.Now().Add(2 * time.Minute).Unix()
	if err := ValidateJoinHMAC(hsSecret, hsNonce, future, f, ComputeJoinHMAC(hsSecret, hsNonce, future, f), tol); err == nil {
		t.Error("a future timestamp was accepted")
	}
}

// TestHandshakeHMAC_WrongSecretRejected is the baseline: without the shared
// secret nothing validates, whatever the fields say.
func TestHandshakeHMAC_WrongSecretRejected(t *testing.T) {
	t.Parallel()
	ts := hsTS()
	f := baseJoin()
	mac := ComputeJoinHMAC(hsSecret, hsNonce, ts, f)
	if err := ValidateJoinHMAC("not-the-secret", hsNonce, ts, f, mac, time.Minute); err == nil {
		t.Error("a join MAC validated under the wrong shared secret")
	}
	// A well-formed but wrong MAC is rejected too (not just malformed hex).
	if err := ValidateJoinHMAC(hsSecret, hsNonce, ts, f, strings.Repeat("0", 64), time.Minute); err == nil {
		t.Error("an all-zero MAC was accepted")
	}
}
