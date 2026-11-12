package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/hashicorp/raft"
)

// gpFSM: cluster policies and the governance flag enabled.
func gpFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := cpFSM(t)
	x.ok(CommandEnableGovernancePolicies, EnableGovernancePoliciesPayload{Nodes: allNodes, Now: t0})
	return x
}

// token creates a cluster token and returns its ID (the log index).
func (x *tkFSM) token(name string) int64 {
	x.t.Helper()
	x.ok(CommandCreateToken, CreateTokenPayload{Token: makeTokenEntry(name)})
	return int64(x.idx)
}

// seedGovernance opens a governance collect and seeds it.
func (x *tkFSM) seedGovernance(keep bool, floor int64, entries ...GovernancePolicyEntry) {
	x.t.Helper()
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindGovernance, Leader: "w1", Now: t0})
	x.ok(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindGovernance, Epoch: x.idx, Governance: entries,
		GovernanceKeepIDs: keep, GovernanceFloor: floor, Now: t0})
}

func govSet(token int64, create bool, rows int) SetGovernancePolicyPayload {
	return SetGovernancePolicyPayload{TokenID: token, Create: create, RateLimitPerMinute: 60, MaxRowsPerQuery: rows, Now: t0}
}

// Governance needs its own flag on top of the cluster policies flag, every
// node listed for it, and nothing of governance applies before it.
func TestGovernancePoliciesNeedTheirOwnFlag(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandEnableGovernancePolicies, EnableGovernancePoliciesPayload{Nodes: allNodes, Now: t0})
	x.ok(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Nodes: allNodes, Now: t0})
	tok := x.token("a")
	x.refused(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindGovernance, Leader: "w1", Now: t0})
	x.refused(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindGovernance, Node: "w1", Hash: "h"})
	x.refused(CommandAwaitAdoption, AwaitAdoptionPayload{Kind: PolicyKindGovernance, LastError: "x", Now: t0})
	x.refused(CommandEnableGovernancePolicies, EnableGovernancePoliciesPayload{Nodes: []string{"w1", "w2", "w3"}, Now: t0})
	if x.f.GovernancePoliciesEnabled() {
		t.Fatal("enabled with a node missing")
	}
	x.ok(CommandEnableGovernancePolicies, EnableGovernancePoliciesPayload{Nodes: allNodes, Now: t0})
	if !x.f.GovernancePoliciesEnabled() {
		t.Fatal("not enabled")
	}
	// Enabled is not seeded: no write until the seed.
	x.refused(CommandSetGovernancePolicy, govSet(tok, true, 10))
	x.refused(CommandDeleteGovernancePolicy, DeleteGovernancePolicyPayload{TokenID: tok})
	x.ok(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindGovernance, Node: "w1", Hash: "h", Now: t0})
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindGovernance, Leader: "w1", Now: t0})
}

// A create refuses a token that has a policy, a replace one that has none,
// and both a token the cluster state does not hold and a negative limit. A
// replace keeps the policy's ID and creation time; a deleted policy's ID is
// never reused.
func TestGovernancePolicySetAndDelete(t *testing.T) {
	x := gpFSM(t)
	a, b := x.token("a"), x.token("b")
	x.seedGovernance(false, 5)
	x.ok(CommandSetGovernancePolicy, govSet(a, true, 100))
	got, ok := x.f.GovernancePolicy(a)
	if !ok || got.ID != 5 || got.TokenID != a || got.MaxRowsPerQuery != 100 || got.RateLimitPerMinute != 60 || !got.CreatedAt.Equal(t0) {
		t.Fatalf("created %+v", got)
	}
	if res := x.apply(CommandSetGovernancePolicy, govSet(a, true, 1)); !IsGovernancePolicyExists(asErr(res)) {
		t.Fatalf("second create: %v", res)
	}
	if res := x.apply(CommandSetGovernancePolicy, govSet(b, false, 1)); !IsPolicyNotFound(asErr(res)) {
		t.Fatalf("replace of a token with no policy: %v", res)
	}
	if res := x.apply(CommandSetGovernancePolicy, govSet(9999, true, 1)); !IsGovernanceTokenNotCluster(asErr(res)) {
		t.Fatalf("a token the cluster does not hold: %v", res)
	}
	for _, bad := range []SetGovernancePolicyPayload{
		{TokenID: 0, Create: true},
		{TokenID: b, Create: true, MaxRowsPerQuery: -1},
		{TokenID: b, Create: true, MaxScanDurationSec: -1},
		{TokenID: b, Create: true, RateLimitPerHour: -1},
	} {
		if res := x.apply(CommandSetGovernancePolicy, bad); !errors.Is(asErr(res), ErrInvalidGovernancePolicy) {
			t.Fatalf("%+v: %v", bad, res)
		}
	}
	if _, ok := x.f.GovernancePolicy(b); ok {
		t.Fatal("a refused create stored a policy")
	}

	upd := govSet(a, false, 7)
	upd.RateLimitPerMinute, upd.Now = 0, t1
	x.ok(CommandSetGovernancePolicy, upd)
	got, _ = x.f.GovernancePolicy(a)
	if got.ID != 5 || got.MaxRowsPerQuery != 7 || got.RateLimitPerMinute != 0 || !got.CreatedAt.Equal(t0) || !got.UpdatedAt.Equal(t1) {
		t.Fatalf("after replace %+v", got)
	}

	x.ok(CommandDeleteGovernancePolicy, DeleteGovernancePolicyPayload{TokenID: a})
	if res := x.apply(CommandDeleteGovernancePolicy, DeleteGovernancePolicyPayload{TokenID: a}); !IsPolicyNotFound(asErr(res)) {
		t.Fatalf("second delete: %v", res)
	}
	x.ok(CommandSetGovernancePolicy, govSet(a, true, 1))
	if got, _ := x.f.GovernancePolicy(a); got.ID != 6 {
		t.Fatalf("new ID %d, want 6", got.ID)
	}
	if n := len(x.f.GovernancePolicies()); n != 1 {
		t.Fatalf("%d policies, want 1", n)
	}
}

// A seed keeps the IDs only when told to and they sit below the floor;
// otherwise it numbers every policy from the floor. The sequence never hands
// out 0. A row whose token is gone by the seed is dropped, and a seed of the
// wrong shape is refused.
func TestGovernanceSeedIDs(t *testing.T) {
	keep := gpFSM(t)
	a, b := keep.token("a"), keep.token("b")
	keep.seedGovernance(true, 8,
		GovernancePolicyEntry{ID: 3, TokenID: a, MaxRowsPerQuery: 1},
		GovernancePolicyEntry{ID: 7, TokenID: b, MaxRowsPerQuery: 2})
	if p, _ := keep.f.GovernancePolicy(a); p.ID != 3 || p.MaxRowsPerQuery != 1 || !p.CreatedAt.Equal(t0) {
		t.Fatalf("kept %+v", p)
	}
	c := keep.token("c")
	keep.ok(CommandSetGovernancePolicy, govSet(c, true, 3))
	if p, _ := keep.f.GovernancePolicy(c); p.ID != 8 {
		t.Fatalf("first new ID %d, want the floor 8", p.ID)
	}

	renum := gpFSM(t)
	a, b = renum.token("a"), renum.token("b")
	renum.seedGovernance(false, 1,
		GovernancePolicyEntry{ID: 40, TokenID: a},
		GovernancePolicyEntry{ID: 0, TokenID: b},
		GovernancePolicyEntry{ID: 41, TokenID: 9999}) // no such token: dropped
	pa, _ := renum.f.GovernancePolicy(a)
	pb, _ := renum.f.GovernancePolicy(b)
	if pa.ID != 1 || pb.ID != 2 {
		t.Fatalf("renumbered from floor 1: %d %d, want 1 2", pa.ID, pb.ID)
	}
	if _, ok := renum.f.GovernancePolicy(9999); ok {
		t.Fatal("a policy for a token not in the cluster was seeded")
	}
	if !renum.f.PoliciesSeeded(PolicyKindGovernance) {
		t.Fatal("not seeded")
	}

	bad := gpFSM(t)
	tok := bad.token("a")
	bad.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindGovernance, Leader: "w1", Now: t0})
	ep := bad.idx
	for name, p := range map[string]SeedPoliciesPayload{
		"no floor":           {GovernanceFloor: 0},
		"kept ID at floor":   {GovernanceKeepIDs: true, GovernanceFloor: 5, Governance: []GovernancePolicyEntry{{ID: 5, TokenID: tok}}},
		"kept ID 0":          {GovernanceKeepIDs: true, GovernanceFloor: 5, Governance: []GovernancePolicyEntry{{ID: 0, TokenID: tok}}},
		"token twice":        {GovernanceFloor: 5, Governance: []GovernancePolicyEntry{{TokenID: tok}, {TokenID: tok}}},
		"negative limit":     {GovernanceFloor: 5, Governance: []GovernancePolicyEntry{{TokenID: tok, MaxQueriesPerDay: -1}}},
		"no token":           {GovernanceFloor: 5, Governance: []GovernancePolicyEntry{{TokenID: 0}}},
		"retention rows":     {GovernanceFloor: 5, Retention: []RetentionPolicyEntry{{Name: "r", Database: "d", RetentionDays: 1}}},
		"continuous queries": {GovernanceFloor: 5, CQ: []CQEntry{{Name: "q"}}},
		"tiering rows":       {GovernanceFloor: 5, Tiering: []TieringPolicyEntry{{Database: "d"}}},
	} {
		p.Kind, p.Epoch, p.Now = PolicyKindGovernance, ep, t0
		if res := bad.apply(CommandSeedPolicies, p); res == nil {
			t.Fatalf("%s: seed accepted", name)
		}
	}
	if bad.f.PoliciesSeeded(PolicyKindGovernance) {
		t.Fatal("a refused seed marked governance seeded")
	}
	over := make([]GovernancePolicyEntry, MaxSeedGovernancePolicies+1)
	for i := range over {
		over[i].TokenID = int64(i + 1)
	}
	bad.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindGovernance, Epoch: ep, GovernanceFloor: 1, Governance: over, Now: t0})

	// Every other kind's seed refuses governance rows.
	gov := []GovernancePolicyEntry{{TokenID: 1}}
	r := rpFSM(t)
	r.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindRetention, Leader: "w1", Now: t0})
	r.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindRetention, Epoch: r.idx, RetentionFloor: 1, Governance: gov, Now: t0})
	q := cqFSM(t)
	q.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindCQ, Leader: "w1", Now: t0})
	q.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindCQ, Epoch: q.idx, CQFloor: 1, Governance: gov, Now: t0})
	tr := cpFSM(t)
	tr.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindTiering, Leader: "w1", Now: t0})
	tr.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Epoch: tr.idx, Governance: gov, Now: t0})
}

// Governance's state survives a snapshot: flag, seed, policies and sequence.
// A snapshot taken before the flag (or by an older build) restores none of
// it, over whatever the node held.
func TestGovernancePoliciesSnapshot(t *testing.T) {
	x := gpFSM(t)
	before := persistFSM(t, x.f)
	a := x.token("a")
	x.seedGovernance(true, 6, GovernancePolicyEntry{ID: 4, TokenID: a, RateLimitPerHour: 9, MaxScanDurationSec: 30})
	b := x.token("b")
	x.ok(CommandSetGovernancePolicy, govSet(b, true, 5))
	snap := persistFSM(t, x.f)

	f2 := newTestFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
		t.Fatal(err)
	}
	if !f2.GovernancePoliciesEnabled() || !f2.PoliciesSeeded(PolicyKindGovernance) {
		t.Fatal("flag or seed lost")
	}
	pa, _ := f2.GovernancePolicy(a)
	pb, _ := f2.GovernancePolicy(b)
	if pa.ID != 4 || pa.RateLimitPerHour != 9 || pa.MaxScanDurationSec != 30 || pb.ID != 6 || pb.MaxRowsPerQuery != 5 {
		t.Fatalf("after restore %+v %+v", pa, pb)
	}
	f2.mu.Lock()
	seq := f2.governanceSeq
	f2.mu.Unlock()
	if seq != 6 {
		t.Fatalf("sequence %d, want 6", seq)
	}
	// A restore fires the policies-changed callbacks.
	fired := 0
	f2.AddPoliciesChangedCallback(func() { fired++ })
	if err := f2.Restore(io.NopCloser(bytes.NewReader(before))); err != nil {
		t.Fatal(err)
	}
	if fired != 1 {
		t.Fatalf("restore fired %d callbacks, want 1", fired)
	}
	if !f2.GovernancePoliciesEnabled() || f2.PoliciesSeeded(PolicyKindGovernance) || len(f2.GovernancePolicies()) != 0 {
		t.Fatal("the unseeded snapshot did not replace the seeded state")
	}

	// A snapshot with no governance field at all (an older build's).
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(snap, &raw); err != nil {
		t.Fatal(err)
	}
	var cp map[string]json.RawMessage
	if err := json.Unmarshal(raw["cluster_policies"], &cp); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"governance_enabled", "governance_seq", "governance"} {
		if _, ok := cp[k]; !ok {
			t.Fatalf("snapshot lacks %q", k)
		}
		delete(cp, k)
	}
	raw["cluster_policies"], _ = json.Marshal(cp)
	old, _ := json.Marshal(raw)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
		t.Fatal(err)
	}
	if err := f2.Restore(io.NopCloser(bytes.NewReader(old))); err != nil {
		t.Fatal(err)
	}
	if f2.GovernancePoliciesEnabled() || len(f2.GovernancePolicies()) != 0 {
		t.Fatal("an older snapshot left governance state behind")
	}
	f2.mu.Lock()
	seq = f2.governanceSeq
	f2.mu.Unlock()
	if seq != 0 {
		t.Fatalf("sequence %d after an older snapshot", seq)
	}
	if !f2.ClusterPoliciesEnabled() || !f2.PoliciesSeeded(PolicyKindGovernance) {
		t.Fatal("the older snapshot's other cluster policies state was not restored")
	}
	// Seeded but without the flag (that older snapshot): no write applies.
	if res := f2.Apply(govLog(t, 1<<20, CommandSetGovernancePolicy, govSet(a, true, 1))); res == nil {
		t.Fatal("a governance write applied without the governance flag")
	}

	// A snapshot holding a policy whose token it does not hold restores
	// without it: nothing would ever drop it.
	var toks map[string]json.RawMessage
	raw = nil
	if err := json.Unmarshal(snap, &raw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw["tokens"], &toks); err != nil {
		t.Fatalf("tokens field: %v", err)
	}
	delete(toks, strconv.FormatInt(b, 10))
	raw["tokens"], _ = json.Marshal(toks)
	orphan, _ := json.Marshal(raw)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(orphan))); err != nil {
		t.Fatal(err)
	}
	if _, ok := f2.GovernancePolicy(b); ok {
		t.Fatal("a restored policy outlived its token")
	}
	if _, ok := f2.GovernancePolicy(a); !ok {
		t.Fatal("a policy whose token is restored was dropped")
	}
}

func govLog(t *testing.T, idx uint64, typ CommandType, p any) *raft.Log {
	t.Helper()
	raw, _ := json.Marshal(p)
	data, _ := json.Marshal(Command{Type: typ, Payload: raw})
	return &raft.Log{Index: idx, Data: data}
}

// A token delete drops the token's governance policy and signals the
// policies-changed callbacks; a token without one signals nothing.
func TestTokenDeleteDropsGovernancePolicy(t *testing.T) {
	x := gpFSM(t)
	a, b := x.token("a"), x.token("b")
	x.seedGovernance(false, 1, GovernancePolicyEntry{TokenID: a, MaxRowsPerQuery: 1})
	fired := 0
	x.f.AddPoliciesChangedCallback(func() { fired++ })
	x.ok(CommandDeleteToken, DeleteTokenPayload{ID: b})
	if fired != 0 {
		t.Fatalf("deleting a token without a policy fired %d callbacks", fired)
	}
	x.ok(CommandDeleteToken, DeleteTokenPayload{ID: a})
	if _, ok := x.f.GovernancePolicy(a); ok {
		t.Fatal("the policy outlived its token")
	}
	if fired != 1 {
		t.Fatalf("deleting a token with a policy fired %d callbacks, want 1", fired)
	}
	// The cluster state no longer knows the token: no policy can be set
	// for it again.
	if res := x.apply(CommandSetGovernancePolicy, govSet(a, true, 1)); !IsGovernanceTokenNotCluster(asErr(res)) {
		t.Fatalf("a policy for a deleted token: %v", res)
	}
}

// Every governance write signals the policies-changed callbacks; a refused
// one does not.
func TestGovernanceWritesSignalTheCallbacks(t *testing.T) {
	x := gpFSM(t)
	a := x.token("a")
	x.seedGovernance(false, 1)
	fired := 0
	x.f.AddPoliciesChangedCallback(func() { fired++ })
	x.ok(CommandSetGovernancePolicy, govSet(a, true, 1))
	x.refused(CommandSetGovernancePolicy, govSet(a, true, 1))
	x.ok(CommandSetGovernancePolicy, govSet(a, false, 2))
	x.ok(CommandDeleteGovernancePolicy, DeleteGovernancePolicyPayload{TokenID: a})
	if fired != 3 {
		t.Fatalf("fired %d, want 3", fired)
	}
}
