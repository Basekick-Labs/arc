package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/governance"
)

// grow is a node's governance row for token tok, reported as the cluster's
// token tok (hash "h<tok>").
func grow(id, tok int64, rows int) governance.LocalRow {
	return governance.LocalRow{ID: id, TokenID: tok, MaxRowsPerQuery: rows, Token: governance.TokenPresent,
		RaftMaterialised: true, TokenFP: auth.TokenFingerprint("h" + string(rune('0'+tok))), CreatedAt: tokensCreated.Add(time.Hour)}
}

// tokensCreated is when clusterTokens' tokens were created.
var tokensCreated = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// clusterTokens: cluster tokens 1..5 with hash "h<id>".
func clusterTokens(id int64) (string, time.Time, bool) {
	if id < 1 || id > 5 {
		return "", time.Time{}, false
	}
	return auth.TokenFingerprint("h" + string(rune('0'+id))), tokensCreated, true
}

func gnode(rows ...governance.LocalRow) governance.LocalRows {
	return governance.LocalRows{Policies: rows}
}

// The merge seeds the union of the nodes' cluster-token policies, waits for
// an admin when two nodes define one token differently, keeps the IDs of a
// single source, and renumbers otherwise.
func TestPlanGovernanceSeedUnionConflictAndIDs(t *testing.T) {
	seed, why, _ := planGovernanceSeed(map[string]governance.LocalRows{
		"a": gnode(grow(4, 1, 100)), "b": gnode(grow(4, 1, 100), grow(5, 2, 50)),
	}, nil, clusterTokens)
	if why != "" || len(seed.Governance) != 2 || seed.GovernanceKeepIDs || seed.GovernanceFloor != 6 {
		t.Fatalf("union: %+v %q", seed, why)
	}
	if seed.Governance[0].TokenID != 1 || seed.Governance[0].MaxRowsPerQuery != 100 || seed.Governance[1].MaxRowsPerQuery != 50 {
		t.Fatalf("seeded %+v", seed.Governance)
	}
	keep, why, _ := planGovernanceSeed(map[string]governance.LocalRows{"a": gnode(grow(3, 1, 1)), "b": gnode()}, nil, clusterTokens)
	if why != "" || !keep.GovernanceKeepIDs || keep.Governance[0].ID != 3 {
		t.Fatalf("one source: %+v %q", keep, why)
	}
	// Another node issued ID 3 for a policy since deleted: renumbered.
	b := gnode()
	b.MaxIssuedID = 3
	if s, _, _ := planGovernanceSeed(map[string]governance.LocalRows{"a": gnode(grow(3, 1, 1)), "b": b}, nil, clusterTokens); s.GovernanceKeepIDs {
		t.Fatal("kept an ID another node issued")
	}

	// One policy on two nodes under different IDs: renumbered, even when the
	// kept ID would be above every other node's.
	if s, why, _ := planGovernanceSeed(map[string]governance.LocalRows{"a": gnode(grow(9, 1, 1)), "b": gnode(grow(2, 1, 1))}, nil, clusterTokens); why != "" || s.GovernanceKeepIDs {
		t.Fatalf("a policy held by two nodes kept an ID: %+v %q", s, why)
	}

	conflict := map[string]governance.LocalRows{"a": gnode(grow(1, 1, 10)), "b": gnode(grow(1, 1, 20))}
	if _, why, _ := planGovernanceSeed(conflict, nil, clusterTokens); !strings.Contains(why, "choose one node") {
		t.Fatalf("conflict: %q", why)
	}
	choice := &raft.PolicyAdoptChoice{Node: "b", Hash: GovernanceRowsHash(conflict["b"].Policies)}
	chosen, why, _ := planGovernanceSeed(conflict, choice, clusterTokens)
	if why != "" || len(chosen.Governance) != 1 || chosen.Governance[0].MaxRowsPerQuery != 20 {
		t.Fatalf("choice: %+v %q", chosen, why)
	}
	choice.Hash = "stale"
	if _, why, _ := planGovernanceSeed(conflict, choice, clusterTokens); !strings.Contains(why, "changed since") {
		t.Fatalf("stale choice: %q", why)
	}
}

// A row is dropped only when its token is certainly gone; every other doubt
// waits, naming the node and the token.
func TestPlanGovernanceSeedTokenRules(t *testing.T) {
	orphan := grow(7, 9, 1) // token 9: not in the cluster
	orphan.Token, orphan.RaftMaterialised, orphan.TokenFP = governance.TokenAbsent, false, ""
	seed, why, warnings := planGovernanceSeed(map[string]governance.LocalRows{"a": gnode(orphan, grow(1, 1, 5))}, nil, clusterTokens)
	if why != "" || len(seed.Governance) != 1 || len(warnings) != 1 || !strings.Contains(warnings[0], "token 9") {
		t.Fatalf("orphan: %+v %q %v", seed, why, warnings)
	}

	legacy := grow(1, 2, 1)
	legacy.RaftMaterialised = false
	collision := grow(1, 2, 1)
	collision.TokenFP = "a different token's hash"
	notHere := grow(1, 3, 1)
	notHere.Token, notHere.TokenFP = governance.TokenAbsent, ""
	unknown := grow(1, 4, 1)
	unknown.Token = governance.TokenUnknown
	older := grow(1, 5, 1) // set for an earlier token at ID 5
	older.CreatedAt = tokensCreated.Add(-24 * time.Hour)
	skewed := grow(1, 5, 1) // a node clock 30 s behind the proposer's
	skewed.CreatedAt = tokensCreated.Add(-30 * time.Second)
	elsewhere := orphan
	elsewhere.TokenID = 9
	other := grow(2, 9, 1) // node b holds token 9, which is not the cluster's
	other.RaftMaterialised = false
	for name, c := range map[string]struct {
		nodes map[string]governance.LocalRows
		want  string
	}{
		"legacy local token":         {map[string]governance.LocalRows{"a": gnode(legacy)}, "not the cluster's token 2"},
		"another token at the ID":    {map[string]governance.LocalRows{"a": gnode(collision)}, "not the cluster's token 2"},
		"cluster token not here yet": {map[string]governance.LocalRows{"a": gnode(notHere)}, "token 3 is not on node a yet"},
		"no token table":             {map[string]governance.LocalRows{"a": gnode(unknown)}, "cannot tell whether token 4"},
		"older than the token":       {map[string]governance.LocalRows{"a": gnode(older)}, "created before cluster token 5"},
		"within the clock skew":      {map[string]governance.LocalRows{"a": gnode(skewed)}, ""},
		"token only on another node": {map[string]governance.LocalRows{"a": gnode(elsewhere), "b": gnode()}, ""},
		"held as legacy elsewhere":   {map[string]governance.LocalRows{"a": gnode(elsewhere), "b": gnode(other)}, "only node b has"},
	} {
		_, why, _ := planGovernanceSeed(c.nodes, nil, clusterTokens)
		if c.want == "" {
			if why != "" {
				t.Errorf("%s: refused %q", name, why)
			}
			continue
		}
		if !strings.Contains(why, c.want) {
			t.Errorf("%s: why %q, want %q", name, why, c.want)
		}
	}

	d1 := gnode()
	d1.Defaults = &governance.Defaults{MaxRowsPerQuery: 10}
	d2 := gnode()
	d2.Defaults = &governance.Defaults{MaxRowsPerQuery: 20}
	if _, why, w := planGovernanceSeed(map[string]governance.LocalRows{"a": d1, "b": d2, "c": gnode()}, nil, clusterTokens); why != "" || len(w) != 1 || !strings.Contains(w[0], "a, b") {
		t.Fatalf("defaults: %q %v", why, w)
	}
}

// govRig: originRig with governance enabled and seeded empty, and a cluster
// token whose ID and hash fingerprint it returns.
func govRig(t *testing.T) (*Coordinator, int64, string) {
	t.Helper()
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()
	local := c.localCapabilities()
	capNodes(t, c, map[string][]string{"raft-1": local, "writer-1": local, "writer-2": local, "reader-1": local})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnablePolicyKinds()
	waitFSM(t, c.fsmOrNil().GovernancePoliciesEnabled)
	idx, err := c.proposeLeader(raft.CommandCreateToken, raft.CreateTokenPayload{Token: raft.TokenEntry{Name: "tok", Permissions: "read",
		TokenHash: "tok-hash", TokenPrefix: "tok-prefix", CreatedAtUnixNano: 1, Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := c.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindGovernance, Leader: "raft-1", Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.proposeLeader(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindGovernance, Epoch: epoch, GovernanceFloor: 1, Now: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	return c, int64(idx), auth.TokenFingerprint("tok-hash")
}

// The coordinator's source refuses a write for a token that is not the
// cluster token at that ID, and maps the cluster state's refusals to the
// API's errors.
func TestGovernanceSourceWrites(t *testing.T) {
	c, tok, fp := govRig(t)
	src := &governancePolicySource{c: c}
	ctx := context.Background()
	if err := src.Set(ctx, &governance.Policy{TokenID: tok, MaxRowsPerQuery: 5}, true, "another token"); !errors.Is(err, governance.ErrTokenNotCluster) {
		t.Fatalf("a different token at the ID: %v", err)
	}
	if err := src.Set(ctx, &governance.Policy{TokenID: 999}, true, fp); !errors.Is(err, governance.ErrTokenNotCluster) {
		t.Fatalf("no such cluster token: %v", err)
	}
	if err := src.Set(ctx, &governance.Policy{TokenID: tok, MaxRowsPerQuery: 5}, true, fp); err != nil {
		t.Fatal(err)
	}
	if p, ok := src.Get(tok); !ok || p.MaxRowsPerQuery != 5 || p.ID == 0 {
		t.Fatalf("stored %+v", p)
	}
	if err := src.Set(ctx, &governance.Policy{TokenID: tok}, true, fp); !errors.Is(err, governance.ErrPolicyExists) {
		t.Fatalf("second create: %v", err)
	}
	if err := src.Set(ctx, &governance.Policy{TokenID: tok, MaxRowsPerQuery: -1}, false, fp); !errors.Is(err, governance.ErrInvalidPolicy) {
		t.Fatalf("negative limit: %v", err)
	}
	if err := src.Delete(ctx, tok, fp); err != nil {
		t.Fatal(err)
	}
	if err := src.Delete(ctx, tok, fp); !errors.Is(err, governance.ErrPolicyNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	if err := src.Delete(ctx, tok, "x"); !errors.Is(err, governance.ErrTokenNotCluster) {
		t.Fatalf("delete for another token: %v", err)
	}
}

// A governance write taken on any node is forwarded to the leader; the seed
// and the enable are not.
func TestForwardApply_GovernanceCommands(t *testing.T) {
	c, tok, _ := govRig(t)
	fwd := func(typ raft.CommandType, p any) *protocol.ForwardApplyAck {
		raw, _ := json.Marshal(p)
		return forwardOnce(t, c, "reader-1", &raft.Command{Type: typ, Payload: raw})
	}
	if ack := fwd(raft.CommandSetGovernancePolicy, raft.SetGovernancePolicyPayload{TokenID: tok, Create: true, MaxRowsPerQuery: 3}); ack.Status != "ok" {
		t.Fatalf("set from a reader: %+v", ack)
	}
	if ack := fwd(raft.CommandDeleteGovernancePolicy, raft.DeleteGovernancePolicyPayload{TokenID: tok}); ack.Status != "ok" {
		t.Fatalf("delete from a reader: %+v", ack)
	}
	if ack := fwd(raft.CommandEnableGovernancePolicies, raft.EnableGovernancePoliciesPayload{Nodes: []string{"x"}}); ack.Status == "ok" {
		t.Fatalf("an enable was accepted from a peer: %+v", ack)
	}
}

// enableGovernance turns the governance flag on in the adoption rig, creates
// cluster token "tok" and gives each node a lister answering rows[node], with
// the token reported as the cluster's (or as legacy).
func (r *adoptRig) enableGovernance(t *testing.T, rows map[string]governance.LocalRows) int64 {
	t.Helper()
	raw, _ := json.Marshal(raft.EnableGovernancePoliciesPayload{Nodes: []string{"L", "F"}, Now: time.Now()})
	if err := r.leader.raftNode.Apply(&raft.Command{Type: raft.CommandEnableGovernancePolicies, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	idx, err := r.leader.proposeLeader(raft.CommandCreateToken, raft.CreateTokenPayload{Token: raft.TokenEntry{Name: "tok", Permissions: "read",
		TokenHash: "tok-hash", TokenPrefix: "tok-prefix", CreatedAtUnixNano: 1, Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.fsmOrNil().GovernancePoliciesEnabled() }, "follower never saw the governance flag")
	for id, c := range map[string]*Coordinator{"L": r.leader, "F": r.follower} {
		id, c := id, c
		c.SetLocalPolicyLister(raft.PolicyKindGovernance, func(context.Context) (json.RawMessage, error) {
			if c.PolicyCollecting(raft.PolicyKindGovernance) {
				r.cqCollectSeen.Store(true)
			}
			out := rows[id]
			for i := range out.Policies {
				if out.Policies[i].TokenID == 0 {
					out.Policies[i].TokenID = int64(idx)
				}
			}
			return json.Marshal(out)
		})
	}
	return int64(idx)
}

func tokRow(id int64, rows int, materialised bool) governance.LocalRow {
	return governance.LocalRow{ID: id, MaxRowsPerQuery: rows, Token: governance.TokenPresent,
		RaftMaterialised: materialised, TokenFP: auth.TokenFingerprint("tok-hash"), CreatedAt: time.Now().UTC()}
}

// End to end on a real Raft pair: the nodes' policies for a cluster token are
// adopted and the follower reads them; a node's policy for a token that is
// not the cluster's waits, without opening a collect.
func TestAdoptGovernance(t *testing.T) {
	r := newAdoptRig(t)
	tok := r.enableGovernance(t, map[string]governance.LocalRows{
		"L": gnode(tokRow(2, 40, true)), "F": gnode(tokRow(5, 40, true)),
	})
	r.leader.maybeSeedPolicies(context.Background())
	if !r.leader.GovernancePoliciesSeeded() {
		t.Fatalf("not seeded: %+v", r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindGovernance])
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.GovernancePoliciesSeeded() }, "follower never saw the seed")
	p, ok := r.follower.fsmOrNil().GovernancePolicy(tok)
	if !ok || p.MaxRowsPerQuery != 40 || p.ID < 6 {
		t.Fatalf("follower reads %+v (renumbered above every node's IDs)", p)
	}

	r2 := newAdoptRig(t)
	r2.enableGovernance(t, map[string]governance.LocalRows{"L": gnode(tokRow(1, 9, false)), "F": gnode()})
	r2.leader.maybeSeedPolicies(context.Background())
	if r2.leader.GovernancePoliciesSeeded() {
		t.Fatal("seeded a policy of a token that is not the cluster's")
	}
	if st := r2.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindGovernance]; !strings.Contains(st.LastError, "not the cluster's token") || st.Collecting {
		t.Fatalf("status %+v", st)
	}
	if r2.cqCollectSeen.Load() {
		t.Fatal("a collect opened for an adoption the probe could tell would wait")
	}
}

// A follower gets the leader's refusal as text: it is still answered as the
// refusal it is (409, 404, 400), and anything else is the cluster being
// unavailable (503).
func TestClassifyForwardedGovernanceRefusals(t *testing.T) {
	refused := func(msg string) error {
		return &ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: msg}
	}
	for _, c := range []struct {
		err  error
		want error
	}{
		{refused("set governance policy for token 5: " + raft.ErrGovernancePolicyExists.Error()), governance.ErrPolicyExists},
		{refused("set governance policy for token 5: " + raft.ErrPolicyNotFound.Error()), governance.ErrPolicyNotFound},
		{refused("set governance policy for token 5: " + raft.ErrGovernanceTokenNotCluster.Error()), governance.ErrTokenNotCluster},
		{refused("set governance policy: " + raft.ErrInvalidGovernancePolicy.Error() + ": limits must be non-negative"), governance.ErrInvalidPolicy},
		{&ForwardRejectedError{Code: protocol.ForwardCodeAuth, Message: "governance policy"}, governance.ErrClusterUnavailable},
		{errors.New("no leader"), governance.ErrClusterUnavailable},
	} {
		if got := classifyGovernanceError(c.err); !errors.Is(got, c.want) {
			t.Errorf("%v: classified %v, want %v", c.err, got, c.want)
		}
	}
}
