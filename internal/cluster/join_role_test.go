package cluster

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// #848: the joiner's role was stored as sent, and ParseRole maps anything it
// does not recognise — the empty string included — to standalone, whose
// capabilities include CanIngest. A bogus role therefore produced a registry
// and FSM entry that passed the manifest-command role gate and appeared in
// listings with a role no operator configured.

func TestParseRoleStrict(t *testing.T) {
	tests := []struct {
		in    string
		want  NodeRole
		valid bool
	}{
		{"writer", RoleWriter, true},
		{"reader", RoleReader, true},
		{"compactor", RoleCompactor, true},
		{"standalone", RoleStandalone, true},
		// The whole point: these used to come back as standalone.
		{"", "", false},
		{"writter", "", false},
		{"Writer", "", false},
		{"WRITER", "", false},
		{"writer ", "", false},
		{"leader", "", false},
		{"../writer", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, ok := ParseRoleStrict(tt.in)
			if ok != tt.valid {
				t.Fatalf("ParseRoleStrict(%q) ok = %v, want %v", tt.in, ok, tt.valid)
			}
			if got != tt.want {
				t.Errorf("ParseRoleStrict(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// ParseRole keeps its fallback. Callers that want "what role should this node
// have" still get standalone for an unset value, which is the documented
// default; only callers asking "is this a role at all" use the strict form.
func TestParseRoleKeepsItsFallback(t *testing.T) {
	if got := ParseRole(""); got != RoleStandalone {
		t.Errorf("ParseRole(\"\") = %q, want standalone", got)
	}
	if got := ParseRole("writter"); got != RoleStandalone {
		t.Errorf("ParseRole of an unknown role should still fall back to standalone, got %q", got)
	}
}

// The local config path. This check existed but could never fire, because
// ParseRole had already turned the typo into a valid role.
func TestResolveRole(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		want       NodeRole
		wantErr    bool
	}{
		{"unset means standalone", "", RoleStandalone, false},
		{"explicit standalone", "standalone", RoleStandalone, false},
		{"writer", "writer", RoleWriter, false},
		{"reader", "reader", RoleReader, false},
		{"compactor", "compactor", RoleCompactor, false},
		// Each of these used to start a node silently reporting standalone.
		{"a typo", "writter", "", true},
		{"wrong case", "Writer", "", true},
		{"trailing space", "writer ", "", true},
		{"a plausible invention", "leader", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveRole(tt.configured)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ResolveRole(%q) err = %v, wantErr %v", tt.configured, err, tt.wantErr)
			}
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidRole) {
					t.Errorf("expected ErrInvalidRole, got %v", err)
				}
				if !strings.Contains(err.Error(), tt.configured) {
					t.Errorf("the error should name the offending value, got: %v", err)
				}
				if !strings.Contains(err.Error(), "valid roles") {
					t.Errorf("the error should list the valid roles, got: %v", err)
				}
				return
			}
			if got != tt.want {
				t.Errorf("ResolveRole(%q) = %q, want %q", tt.configured, got, tt.want)
			}
		})
	}
}

// The join path, driven end to end through handleJoinRequest with the same
// harness the GHSA-p2rx tests use. A correctly signed request is essential
// here: the role is inside the MAC, so this is a node that legitimately holds
// the shared secret and presents a role nobody configured — not an on-path
// attacker, which the HMAC already stops.
func TestHandleJoinRequest_RejectsAnUnrecognisedRole(t *testing.T) {
	for _, role := range []string{"", "writter", "Writer", "leader", "root"} {
		t.Run("role="+role, func(t *testing.T) {
			c := newJoinTestCoordinator(t, joinTestSecret)
			req := signedJoinRequest(joinTestSecret)
			req.Role = role
			// Re-sign: Role is covered by the MAC, so a bogus role has to be
			// presented by a node that holds the secret.
			req.AuthHMAC = security.ComputeJoinHMAC(joinTestSecret, req.AuthNonce, req.AuthTimestamp, joinAuthFields(req))

			resp := deliverJoin(t, c, req)
			if resp == nil {
				t.Fatalf("join with role %q produced no JoinResponse at all; the handler took some other path", role)
			}
			if resp.Success {
				t.Fatalf("join with role %q was accepted", role)
			}
			// Assert the reason. Without this the test would pass just as
			// happily on an auth failure or a core-limit rejection, and would
			// stop proving anything about the role guard.
			if !strings.Contains(resp.Error, "unrecognised node role") {
				t.Errorf("join with role %q was refused for the wrong reason: %q", role, resp.Error)
			}
			if _, ok := c.registry.Get(req.NodeID); ok {
				t.Errorf("join with role %q registered the node anyway", role)
			}
		})
	}
}

// The roles an operator can actually configure must still join. Guards against
// a validation that is too strict, which would be a far worse outage than the
// bug it fixes.
func TestHandleJoinRequest_AcceptsEveryConfigurableRole(t *testing.T) {
	for _, role := range AllRoles() {
		t.Run(string(role), func(t *testing.T) {
			c := newJoinTestCoordinator(t, joinTestSecret)
			req := signedJoinRequest(joinTestSecret)
			req.Role = string(role)
			req.AuthHMAC = security.ComputeJoinHMAC(joinTestSecret, req.AuthNonce, req.AuthTimestamp, joinAuthFields(req))

			resp := deliverJoin(t, c, req)
			if resp == nil || !resp.Success {
				t.Fatalf("join with the configurable role %q was rejected: %+v", role, resp)
			}
			node, ok := c.registry.Get(req.NodeID)
			if !ok {
				t.Fatalf("join with role %q did not register the node", role)
			}
			if node.Role != role {
				t.Errorf("registered role = %q, want %q", node.Role, role)
			}
		})
	}
}

// H2 from review: every other join test builds a coordinator with no Raft
// node, so they only ever drive the `else { registry.Register }` branch. The
// line #848 is actually about — AddNode writing req.Role into the FSM — lives
// in the `c.raftNode != nil` branch and was covered by nothing. A guard
// written as `!ValidRole(req.Role) && c.raftNode == nil` would have restored
// the bug on every real cluster with the suite still green.
//
// This drives the real branch against a single-node Raft leader and asserts
// the role never reaches cluster state. Single node deliberately: a two-voter
// rig cannot elect and flakes.
func TestHandleJoinRequest_ABogusRoleNeverReachesClusterState(t *testing.T) {
	c := newJoinTestCoordinator(t, joinTestSecret)
	rn := startRaftNode(t, "leader-node", allocFreePort(t), true)
	t.Cleanup(func() { _ = rn.Stop() })
	if err := rn.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("test Raft node never became leader: %v", err)
	}
	c.raftNode = rn
	c.raftFSM = rn.FSM()

	req := signedJoinRequest(joinTestSecret)
	req.Role = "writter"
	req.AuthHMAC = security.ComputeJoinHMAC(joinTestSecret, req.AuthNonce, req.AuthTimestamp, joinAuthFields(req))

	resp := deliverJoin(t, c, req)
	if resp == nil || resp.Success {
		t.Fatalf("join with a bogus role was accepted by a Raft-backed leader: %+v", resp)
	}
	if !strings.Contains(resp.Error, "unrecognised node role") {
		t.Errorf("refused for the wrong reason: %q", resp.Error)
	}

	// The point of the test: nothing was written to cluster state.
	if _, ok := rn.FSM().GetNode(req.NodeID); ok {
		t.Error("a bogus role reached the Raft FSM node table")
	}
	ids, err := rn.ConfigurationServerIDs()
	if err != nil {
		t.Fatalf("ConfigurationServerIDs: %v", err)
	}
	for _, id := range ids {
		if id == req.NodeID {
			t.Error("a node with a bogus role was added to the Raft configuration")
		}
	}
}

// The warning about a role recorded before this change has to be reachable,
// or it is another check that can never fire — the shape #848 exists to
// remove. A JoinResponse peer list cannot carry one, because sendJoinSuccess
// serialises from registry Nodes whose roles have already been through
// ParseRole. The FSM record can, and this is where it arrives.
func TestOnRaftNodeAdded_WarnsAboutARoleRecordedBeforeValidation(t *testing.T) {
	var buf bytes.Buffer
	local := NewNode("local-1", "local-1", RoleWriter, "test-cluster")
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: "test-cluster"},
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()}),
		localNode: local,
		logger:    zerolog.New(&buf),
	}

	c.onRaftNodeAdded(&raft.NodeInfo{
		ID:          "legacy-node",
		Name:        "legacy-node",
		Role:        "writter",
		ClusterName: "test-cluster",
		State:       string(StateHealthy),
	})

	if !strings.Contains(buf.String(), "unrecognised node role") {
		t.Errorf("expected a warning about the recorded role, got:\n%s", buf.String())
	}

	// Carried, not dropped: losing an existing member during a restore is
	// worse than carrying it as standalone.
	node, ok := c.registry.Get("legacy-node")
	if !ok {
		t.Fatal("the node should still be registered")
	}
	if node.Role != RoleStandalone {
		t.Errorf("registered role = %q, want standalone", node.Role)
	}
}

// And a role the cluster recorded normally must not warn, or the log fills
// with noise on every restore.
func TestOnRaftNodeAdded_QuietForAValidRole(t *testing.T) {
	var buf bytes.Buffer
	local := NewNode("local-1", "local-1", RoleWriter, "test-cluster")
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: "test-cluster"},
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()}),
		localNode: local,
		logger:    zerolog.New(&buf),
	}

	for _, role := range AllRoles() {
		c.onRaftNodeAdded(&raft.NodeInfo{
			ID:          "node-" + string(role),
			Name:        "node-" + string(role),
			Role:        string(role),
			ClusterName: "test-cluster",
			State:       string(StateHealthy),
		})
	}

	if strings.Contains(buf.String(), "unrecognised node role") {
		t.Errorf("a valid role must not warn, got:\n%s", buf.String())
	}
}
