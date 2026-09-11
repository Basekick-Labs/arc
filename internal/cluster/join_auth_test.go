package cluster

import (
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

const (
	joinTestCluster = "test-cluster"
	joinTestSecret  = "join-test-secret"
)

// newJoinTestCoordinator builds a minimal no-Raft Coordinator that can serve a
// join request: the registry path registers the node locally rather than going
// through AddVoter, which keeps the test free of a real Raft cluster while
// still exercising the whole authentication and registration path.
func newJoinTestCoordinator(t *testing.T, secret string) *Coordinator {
	t.Helper()
	local := NewNode("leader-node", "leader", RoleWriter, joinTestCluster)
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	return &Coordinator{
		cfg:        &config.ClusterConfig{ClusterName: joinTestCluster, SharedSecret: secret},
		registry:   reg,
		localNode:  local,
		logger:     zerolog.Nop(),
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
	}
}

// signedJoinRequest builds a fully-populated, correctly-signed join request.
// Callers mutate a field afterwards to simulate an on-path attacker.
func signedJoinRequest(secret string) *protocol.JoinRequest {
	req := &protocol.JoinRequest{
		NodeID:      "joining-node",
		NodeName:    "joining-node",
		Role:        string(RoleReader),
		ClusterName: joinTestCluster,
		RaftAddr:    "10.0.0.2:9200",
		APIAddr:     "10.0.0.2:8080",
		CoordAddr:   "10.0.0.2:9100",
		Version:     "26.09.2",
		CoreCount:   runtime.GOMAXPROCS(0),
	}
	if secret != "" {
		nonce, _ := security.GenerateNonce()
		req.AuthTimestamp = time.Now().Unix()
		req.AuthNonce = nonce
		req.AuthHMAC = security.ComputeJoinHMAC(secret, nonce, req.AuthTimestamp, joinAuthFields(req))
	}
	return req
}

// deliverJoin runs handleJoinRequest over a pipe and returns the decoded
// response, so a test can assert on both the registry effect and what the
// joiner would have seen on the wire.
func deliverJoin(t *testing.T, c *Coordinator, req *protocol.JoinRequest) *protocol.JoinResponse {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.handleJoinRequest(server, req)
		server.Close()
	}()

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	var resp *protocol.JoinResponse
	if msg, err := protocol.ReceiveMessage(client, 2*time.Second); err == nil {
		if jr, ok := msg.Payload.(*protocol.JoinResponse); ok {
			resp = jr
		}
	}
	client.Close()
	<-done
	return resp
}

// TestHandleJoinRequest_AcceptsValid is the control: without it, every
// rejection test below could pass for the wrong reason.
func TestHandleJoinRequest_AcceptsValid(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)
	req := signedJoinRequest(joinTestSecret)

	resp := deliverJoin(t, c, req)
	if resp == nil || !resp.Success {
		t.Fatalf("valid join rejected: %+v", resp)
	}

	node, ok := c.registry.Get(req.NodeID)
	if !ok {
		t.Fatal("valid join did not register the node")
	}
	// The registered values must be the ones that were signed.
	if node.Role != RoleReader {
		t.Errorf("registered role = %v, want %v", node.Role, RoleReader)
	}
	if node.Address != req.CoordAddr || node.APIAddress != req.APIAddr {
		t.Errorf("registered addresses = %q/%q, want %q/%q", node.Address, node.APIAddress, req.CoordAddr, req.APIAddr)
	}

	// The response itself must be signed over the request's nonce, otherwise
	// the joiner has no way to tell it from an injected one.
	if err := security.ValidateJoinResponseHMAC(
		joinTestSecret, req.AuthNonce, resp.AuthTimestamp,
		joinResponseAuthFields(resp), resp.AuthHMAC, security.HMACTimestampTolerance,
	); err != nil {
		t.Errorf("join success response failed its own validation: %v", err)
	}
}

// TestHandleJoinRequest_RejectsFieldMutation is the direct regression test for
// GHSA-p2rx as reported: each of these fields was consumed by the handler
// while sitting outside the MAC, so an on-path attacker could rewrite it and
// the tag still verified.
func TestHandleJoinRequest_RejectsFieldMutation(t *testing.T) {
	mutations := map[string]func(*protocol.JoinRequest){
		"Role":      func(r *protocol.JoinRequest) { r.Role = string(RoleWriter) },
		"NodeName":  func(r *protocol.JoinRequest) { r.NodeName = "attacker" },
		"APIAddr":   func(r *protocol.JoinRequest) { r.APIAddr = "attacker.example:8080" },
		"CoordAddr": func(r *protocol.JoinRequest) { r.CoordAddr = "attacker.example:9100" },
		"RaftAddr":  func(r *protocol.JoinRequest) { r.RaftAddr = "attacker.example:9200" },
		"Version":   func(r *protocol.JoinRequest) { r.Version = "0.0.1" },
		"CoreCount": func(r *protocol.JoinRequest) { r.CoreCount = 1024 },
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := newJoinTestCoordinator(t, joinTestSecret)
			req := signedJoinRequest(joinTestSecret)
			mutate(req)

			resp := deliverJoin(t, c, req)
			if resp != nil && resp.Success {
				t.Errorf("join with mutated %s was accepted", name)
			}
			if _, ok := c.registry.Get(req.NodeID); ok {
				t.Errorf("join with mutated %s registered the node", name)
			}
			if resp != nil && resp.Error != joinAuthFailedMsg {
				t.Errorf("pre-auth rejection leaked detail: %q", resp.Error)
			}
		})
	}
}

// TestHandleJoinRequest_RejectsMutatedNodeID is the MAC-bound control from the
// original report: NodeID was already covered, so mutating it was rejected
// even before this fix. Keeping it guards against a change that accidentally
// weakens the validator itself.
func TestHandleJoinRequest_RejectsMutatedNodeID(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)
	req := signedJoinRequest(joinTestSecret)
	req.NodeID = "tampered-node"

	deliverJoin(t, c, req)
	if _, ok := c.registry.Get("tampered-node"); ok {
		t.Error("a join with a mutated NodeID was accepted")
	}
}

// TestHandleJoinRequest_RejectsReplay: a captured join must not be replayable
// inside the freshness window.
func TestHandleJoinRequest_RejectsReplay(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)
	req := signedJoinRequest(joinTestSecret)

	if resp := deliverJoin(t, c, req); resp == nil || !resp.Success {
		t.Fatal("first join rejected")
	}
	c.registry.Unregister(req.NodeID)

	resp := deliverJoin(t, c, req)
	if resp != nil && resp.Success {
		t.Error("a replayed join was accepted — the nonce was not consumed")
	}
	if _, ok := c.registry.Get(req.NodeID); ok {
		t.Error("a replayed join re-registered the node")
	}
}

// TestHandleJoinRequest_FailsClosedWithoutReplayGuard: see the heartbeat
// equivalent. A nil *NonceCache in the interface is the typed-nil trap, so
// this also guards against the validator panicking instead of rejecting.
func TestHandleJoinRequest_FailsClosedWithoutReplayGuard(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)
	c.nonceCache = nil

	resp := deliverJoin(t, c, signedJoinRequest(joinTestSecret))
	if resp != nil && resp.Success {
		t.Error("a join was accepted with no replay guard installed")
	}
}

// TestHandleJoinRequest_UnsignedRejected covers the plain case: a peer that
// does not know the secret at all.
func TestHandleJoinRequest_UnsignedRejected(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)
	req := signedJoinRequest("")

	resp := deliverJoin(t, c, req)
	if resp != nil && resp.Success {
		t.Error("an unsigned join was accepted by a secret-configured node")
	}
}

// TestHandleJoinRequest_PreAuthErrorsAreUniform: every rejection that happens
// before the peer proves it holds the shared secret returns the same opaque
// string, and in particular never echoes the cluster name back.
func TestHandleJoinRequest_PreAuthErrorsAreUniform(t *testing.T) {
	cases := map[string]func() *protocol.JoinRequest{
		"cluster name mismatch": func() *protocol.JoinRequest {
			req := signedJoinRequest(joinTestSecret)
			req.ClusterName = "someone-elses-cluster"
			return req
		},
		"missing MAC": func() *protocol.JoinRequest {
			req := signedJoinRequest(joinTestSecret)
			req.AuthHMAC = ""
			return req
		},
		"bad MAC": func() *protocol.JoinRequest {
			req := signedJoinRequest(joinTestSecret)
			req.AuthHMAC = strings.Repeat("0", 64)
			return req
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			c := newJoinTestCoordinator(t, joinTestSecret)
			resp := deliverJoin(t, c, build())
			if resp == nil {
				t.Fatal("no response sent")
			}
			if resp.Success {
				t.Fatal("rejection expected")
			}
			if resp.Error != joinAuthFailedMsg {
				t.Errorf("error = %q, want the uniform %q", resp.Error, joinAuthFailedMsg)
			}
			if strings.Contains(resp.Error, joinTestCluster) {
				t.Errorf("rejection leaked the cluster name: %q", resp.Error)
			}
		})
	}
}

// TestHandleJoinRequest_NoSecretAcceptsUnsigned: clusters without a shared
// secret are unsupported since #505 but still reachable in tests; the handler
// must not start requiring auth it was never given.
func TestHandleJoinRequest_NoSecretAcceptsUnsigned(t *testing.T) {
	c := newJoinTestCoordinator(t, "")
	resp := deliverJoin(t, c, signedJoinRequest(""))
	if resp == nil || !resp.Success {
		t.Fatal("unsigned join rejected by a coordinator with no secret")
	}
	if _, ok := c.registry.Get("joining-node"); !ok {
		t.Error("node not registered")
	}
}

// ---- Joiner side: validating the response ----

// TestHandleJoinResponse_RejectsForgedResponse covers the reverse direction.
// The joiner writes every returned NodeInfo into its own registry and follows
// LeaderInfo redirects, so an unsigned response lets an on-path attacker
// choose this node's view of the cluster.
func TestHandleJoinResponse_RejectsForgedResponse(t *testing.T) {
	reqNonce := "the-request-nonce"

	build := func(mutate func(*protocol.JoinResponse)) *protocol.Message {
		resp := &protocol.JoinResponse{
			Success:    true,
			LeaderID:   "leader-node",
			LeaderAddr: "10.0.0.9:9100",
			Nodes: []protocol.NodeInfo{
				{ID: "peer-1", Name: "peer-1", Role: "writer", State: "healthy", APIAddr: "10.0.0.9:8080", CoordAddr: "10.0.0.9:9100"},
			},
		}
		resp.AuthTimestamp = time.Now().Unix()
		resp.AuthHMAC = security.ComputeJoinResponseHMAC(joinTestSecret, reqNonce, resp.AuthTimestamp, joinResponseAuthFields(resp))
		if mutate != nil {
			mutate(resp)
		}
		return protocol.NewJoinResponse(resp)
	}

	cases := map[string]func(*protocol.JoinResponse){
		"forged peer address": func(r *protocol.JoinResponse) { r.Nodes[0].CoordAddr = "attacker.example:9100" },
		"forged peer role":    func(r *protocol.JoinResponse) { r.Nodes[0].Role = "reader" },
		"injected peer": func(r *protocol.JoinResponse) {
			r.Nodes = append(r.Nodes, protocol.NodeInfo{ID: "ghost", CoordAddr: "attacker.example:9100"})
		},
		"forged leader addr": func(r *protocol.JoinResponse) { r.LeaderAddr = "attacker.example:9100" },
		"stripped signature": func(r *protocol.JoinResponse) { r.AuthHMAC = "" },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := newJoinTestCoordinator(t, joinTestSecret)
			err := c.handleJoinResponse(build(mutate), "10.0.0.9:9100", reqNonce)
			if err == nil {
				t.Fatalf("forged join response (%s) was accepted", name)
			}
			if _, ok := c.registry.Get("peer-1"); ok {
				t.Error("a peer from a forged response was registered")
			}
			if _, ok := c.registry.Get("ghost"); ok {
				t.Error("an injected peer was registered")
			}
		})
	}

	t.Run("valid response is accepted", func(t *testing.T) {
		c := newJoinTestCoordinator(t, joinTestSecret)
		if err := c.handleJoinResponse(build(nil), "10.0.0.9:9100", reqNonce); err != nil {
			t.Fatalf("valid join response rejected: %v", err)
		}
		if _, ok := c.registry.Get("peer-1"); !ok {
			t.Error("valid response did not register the peer")
		}
	})

	t.Run("response bound to a different nonce is rejected", func(t *testing.T) {
		c := newJoinTestCoordinator(t, joinTestSecret)
		if err := c.handleJoinResponse(build(nil), "10.0.0.9:9100", "a-different-nonce"); err == nil {
			t.Error("a response bound to another request's nonce was accepted — captured responses would be replayable")
		}
	})
}

// TestHandleJoinResponse_RejectsForgedRedirect: a redirect names the address
// the joiner dials next and hands its next signed join request to, so an
// unauthenticated one is a free relay for an on-path attacker.
func TestHandleJoinResponse_RejectsForgedRedirect(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)
	info := &protocol.LeaderInfo{
		LeaderID:        "leader-node",
		LeaderCoordAddr: "attacker.example:9100",
	}
	// Unsigned: exactly what an old-version peer or an injector sends.
	err := c.handleJoinResponse(protocol.NewLeaderInfo(info), "10.0.0.9:9100", "req-nonce")
	if err == nil {
		t.Fatal("an unsigned leader redirect was accepted")
	}
	if !strings.Contains(err.Error(), "authentication") {
		t.Errorf("error should name the authentication failure, got: %v", err)
	}
}

// TestHandleJoinResponse_ValidRedirectIsFollowed proves the redirect path is
// reached after validation — the dial fails because the target port is closed,
// which is a different error than the authentication rejection above.
func TestHandleJoinResponse_ValidRedirectIsFollowed(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)

	// A port nothing is listening on: reserve one, then release it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	closedAddr := ln.Addr().String()
	ln.Close()

	reqNonce := "req-nonce"
	info := &protocol.LeaderInfo{LeaderID: "leader-node", LeaderCoordAddr: closedAddr}
	info.AuthTimestamp = time.Now().Unix()
	info.AuthHMAC = security.ComputeLeaderInfoHMAC(joinTestSecret, reqNonce, info.AuthTimestamp, leaderInfoAuthFields(info))

	err = c.handleJoinResponse(protocol.NewLeaderInfo(info), "10.0.0.9:9100", reqNonce)
	if err == nil {
		t.Fatal("expected the redirect dial to fail against a closed port")
	}
	if strings.Contains(err.Error(), "authentication") {
		t.Errorf("a correctly signed redirect was rejected as unauthenticated: %v", err)
	}
	if !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected a connection error, got: %v", err)
	}
}

// TestHandleJoinResponse_NoSecretAcceptsUnsigned: a coordinator with no shared
// secret must still accept plain responses.
func TestHandleJoinResponse_NoSecretAcceptsUnsigned(t *testing.T) {
	c := newJoinTestCoordinator(t, "")
	resp := &protocol.JoinResponse{
		Success:  true,
		LeaderID: "leader-node",
		Nodes:    []protocol.NodeInfo{{ID: "peer-1", Name: "peer-1", Role: "writer", State: "healthy"}},
	}
	if err := c.handleJoinResponse(protocol.NewJoinResponse(resp), "10.0.0.9:9100", ""); err != nil {
		t.Fatalf("unsigned response rejected with no secret configured: %v", err)
	}
	if _, ok := c.registry.Get("peer-1"); !ok {
		t.Error("peer not registered")
	}
}
