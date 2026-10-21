package raft

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

var allNodes = []string{"w1", "w2", "w3", "r1"}

var t1 = t0.Add(time.Hour)

func strp(s string) *string { return &s }

func boolp(b bool) *bool { return &b }

// rpFSM: cluster policies and retention's own flag enabled.
func rpFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := cpFSM(t)
	x.ok(CommandEnableRetentionPolicies, EnableRetentionPoliciesPayload{Nodes: allNodes, Now: t0})
	return x
}

// seedRetention opens a retention collect and seeds it.
func (x *tkFSM) seedRetention(keep bool, floor int64, entries ...RetentionPolicyEntry) {
	x.t.Helper()
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindRetention, Leader: "w1", Now: t0})
	x.ok(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindRetention, Epoch: x.idx, Retention: entries,
		RetentionKeepIDs: keep, RetentionFloor: floor, Now: t0})
}

// Retention needs its own flag on top of the cluster policies flag, every
// node listed for it, and nothing of retention applies before it: a build
// that knows only tiering never sees retention state.
func TestRetentionPoliciesNeedTheirOwnFlag(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandEnableRetentionPolicies, EnableRetentionPoliciesPayload{Nodes: allNodes, Now: t0})
	x.ok(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Nodes: allNodes, Now: t0})
	x.refused(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindRetention, Leader: "w1", Now: t0})
	x.refused(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "w1", Hash: "h"})
	x.refused(CommandAwaitAdoption, AwaitAdoptionPayload{Kind: PolicyKindRetention, LastError: "x", Now: t0})
	x.refused(CommandEnableRetentionPolicies, EnableRetentionPoliciesPayload{Nodes: []string{"w1", "w2", "w3"}, Now: t0})
	if x.f.RetentionPoliciesEnabled() {
		t.Fatal("enabled with a node missing")
	}
	x.ok(CommandEnableRetentionPolicies, EnableRetentionPoliciesPayload{Nodes: allNodes, Now: t0})
	if !x.f.RetentionPoliciesEnabled() {
		t.Fatal("not enabled")
	}
	x.refused(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "p", Database: "db", RetentionDays: 30, Now: t0})
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindRetention, Leader: "w1", Now: t0})
}

// Create numbers from the seed's floor, enforces unique names and the day
// rules; update merges only the fields it carries against the policy as it
// is, and checks the cross-field rule on the merged values.
func TestRetentionPolicyCreateUpdateDelete(t *testing.T) {
	x := rpFSM(t)
	x.seedRetention(false, 10)
	x.ok(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "a", Database: "db", RetentionDays: 30, BufferDays: 2, Measurement: strp(""), Now: t0})
	a, ok := x.f.RetentionPolicyByName("a")
	if !ok || a.ID != 10 || a.Measurement != nil || a.IsActive {
		t.Fatalf("created %+v", a)
	}
	if res := x.apply(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "a", Database: "db2", RetentionDays: 5, Now: t0}); !IsPolicyNameTaken(asErr(res)) {
		t.Fatalf("duplicate name: %v", res)
	}
	for _, bad := range []CreateRetentionPolicyPayload{
		{Name: "", Database: "db", RetentionDays: 1},
		{Name: "b", Database: "", RetentionDays: 1},
		{Name: "b", Database: "db", RetentionDays: 0},
		{Name: "b", Database: "db", RetentionDays: 5, BufferDays: -1},
	} {
		if res := x.apply(CommandCreateRetentionPolicy, bad); !errors.Is(asErr(res), ErrInvalidRetentionPolicy) {
			t.Fatalf("%+v: %v", bad, res)
		}
	}
	if res := x.apply(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "b", Database: "db", RetentionDays: 5, BufferDays: 5}); !IsRetentionNotAboveBuffer(asErr(res)) {
		t.Fatalf("retention == buffer: %v", res)
	}
	x.ok(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "b", Database: "db", RetentionDays: 7, Now: t0})

	// Two partial updates, each carrying one field, both stick.
	x.ok(CommandUpdateRetentionPolicy, UpdateRetentionPolicyPayload{ID: a.ID, IsActive: boolp(true), Now: t1})
	x.ok(CommandUpdateRetentionPolicy, UpdateRetentionPolicyPayload{ID: a.ID, SetMeasurement: true, Measurement: strp("cpu"), Now: t1})
	got, _ := x.f.RetentionPolicy(a.ID)
	if !got.IsActive || got.Measurement == nil || *got.Measurement != "cpu" || got.RetentionDays != 30 || got.BufferDays != 2 || !got.UpdatedAt.Equal(t1) || !got.CreatedAt.Equal(t0) {
		t.Fatalf("after partial updates: %+v", got)
	}
	// The cross-field rule sees the stored retention: buffer 30 alone breaks it.
	if res := x.apply(CommandUpdateRetentionPolicy, UpdateRetentionPolicyPayload{ID: a.ID, BufferDays: intp(30)}); !IsRetentionNotAboveBuffer(asErr(res)) {
		t.Fatalf("buffer above stored retention: %v", res)
	}
	if res := x.apply(CommandUpdateRetentionPolicy, UpdateRetentionPolicyPayload{ID: a.ID, Name: strp("b")}); !IsPolicyNameTaken(asErr(res)) {
		t.Fatalf("rename onto another policy: %v", res)
	}
	x.ok(CommandUpdateRetentionPolicy, UpdateRetentionPolicyPayload{ID: a.ID, Name: strp("a")}) // its own name
	x.ok(CommandUpdateRetentionPolicy, UpdateRetentionPolicyPayload{ID: a.ID, SetMeasurement: true})
	if got, _ := x.f.RetentionPolicy(a.ID); got.Measurement != nil {
		t.Fatalf("measurement null did not widen to the database: %+v", got)
	}
	if res := x.apply(CommandUpdateRetentionPolicy, UpdateRetentionPolicyPayload{ID: 999, IsActive: boolp(true)}); !IsPolicyNotFound(asErr(res)) {
		t.Fatalf("update of a missing policy: %v", res)
	}
	x.ok(CommandDeleteRetentionPolicy, DeleteRetentionPolicyPayload{ID: a.ID})
	if res := x.apply(CommandDeleteRetentionPolicy, DeleteRetentionPolicyPayload{ID: a.ID}); !IsPolicyNotFound(asErr(res)) {
		t.Fatalf("second delete: %v", res)
	}
	// A deleted policy's ID is never reused.
	x.ok(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "c", Database: "db", RetentionDays: 7, Now: t0})
	if c, _ := x.f.RetentionPolicyByName("c"); c.ID != 12 {
		t.Fatalf("new ID %d, want 12", c.ID)
	}
}

func asErr(res interface{}) error {
	if err, ok := res.(error); ok {
		return err
	}
	return nil
}

// A seed keeps the IDs only when told to and they sit below the floor;
// otherwise it numbers every policy from the floor, above any ID a node held.
func TestRetentionSeedIDs(t *testing.T) {
	keep := rpFSM(t)
	keep.seedRetention(true, 8,
		RetentionPolicyEntry{ID: 3, Name: "a", Database: "db", RetentionDays: 30},
		RetentionPolicyEntry{ID: 7, Name: "b", Database: "db", RetentionDays: 30})
	if _, ok := keep.f.RetentionPolicy(3); !ok {
		t.Fatal("kept ID 3 missing")
	}
	keep.ok(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "c", Database: "db", RetentionDays: 7, Now: t0})
	if c, _ := keep.f.RetentionPolicyByName("c"); c.ID != 8 {
		t.Fatalf("first new ID %d, want the floor 8", c.ID)
	}
	st := keep.f.RetentionMirrorState()
	if st.Floor != 8 || !st.SeededIDs[3] || !st.SeededIDs[7] || st.SeededIDs[8] {
		t.Fatalf("mirror state %+v", st)
	}

	renum := rpFSM(t)
	renum.seedRetention(false, 20,
		RetentionPolicyEntry{ID: 3, Name: "a", Database: "db", RetentionDays: 30, Sources: []RetentionSource{{Node: "w1", ID: 3}}})
	a, _ := renum.f.RetentionPolicyByName("a")
	if a.ID != 20 || len(a.Sources) != 1 || a.Sources[0].ID != 3 {
		t.Fatalf("renumbered %+v", a)
	}
	if _, ok := renum.f.RetentionPolicy(3); ok {
		t.Fatal("the old ID still names a policy")
	}

	bad := rpFSM(t)
	bad.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindRetention, Leader: "w1", Now: t0})
	ep := bad.idx
	for _, p := range []SeedPoliciesPayload{
		{RetentionKeepIDs: true, RetentionFloor: 5, Retention: []RetentionPolicyEntry{{ID: 5, Name: "a", Database: "db", RetentionDays: 1}}},
		{RetentionKeepIDs: true, RetentionFloor: 9, Retention: []RetentionPolicyEntry{{ID: 2, Name: "a", Database: "db", RetentionDays: 1}, {ID: 2, Name: "b", Database: "db", RetentionDays: 1}}},
		{RetentionFloor: 9, Retention: []RetentionPolicyEntry{{Name: "a", Database: "db", RetentionDays: 1}, {Name: "a", Database: "db", RetentionDays: 1}}},
		{RetentionFloor: 0},
		{RetentionFloor: 1, Tiering: []TieringPolicyEntry{{Database: "db"}}},
	} {
		p.Kind, p.Epoch, p.Now = PolicyKindRetention, ep, t0
		bad.refused(CommandSeedPolicies, p)
	}
	if bad.f.PoliciesSeeded(PolicyKindRetention) {
		t.Fatal("a refused seed marked retention seeded")
	}
}

// An admin's choice is retention-only, for a node in the cluster, before the
// seed; an await or the seed ends it.
func TestChoosePolicyAdoption(t *testing.T) {
	x := rpFSM(t)
	x.refused(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindTiering, Node: "w1", Hash: "h"})
	x.refused(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "nope", Hash: "h"})
	x.refused(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "w1"})
	x.ok(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "w2", Hash: "h", By: "admin", Now: t0})
	if c, ok := x.f.PolicyAdoptChoiceFor(PolicyKindRetention); !ok || c.Node != "w2" || c.Hash != "h" {
		t.Fatalf("choice %+v %v", c, ok)
	}
	x.ok(CommandAwaitAdoption, AwaitAdoptionPayload{Kind: PolicyKindRetention, LastError: "changed", Now: t0})
	if _, ok := x.f.PolicyAdoptChoiceFor(PolicyKindRetention); ok {
		t.Fatal("an await left the choice pending")
	}
	x.ok(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "w2", Hash: "h", Now: t0})
	x.seedRetention(false, 1)
	if _, ok := x.f.PolicyAdoptChoiceFor(PolicyKindRetention); ok {
		t.Fatal("the seed left the choice pending")
	}
	x.refused(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "w2", Hash: "h", Now: t0})
}

// Retention's state survives a snapshot: flag, policies, sequence, floor,
// seeded IDs and a pending choice.
func TestRetentionPoliciesSnapshot(t *testing.T) {
	x := rpFSM(t)
	x.ok(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "w2", Hash: "h", Now: t0})
	x.ok(CommandAwaitAdoption, AwaitAdoptionPayload{Kind: PolicyKindRetention, Now: t0})
	x.ok(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindRetention, Node: "w3", Hash: "h2", Now: t0})
	choiceSnap := persistFSM(t, x.f)

	x.seedRetention(true, 6, RetentionPolicyEntry{ID: 4, Name: "a", Database: "db", Measurement: strp("cpu"), RetentionDays: 30, BufferDays: 1, IsActive: true})
	x.ok(CommandCreateRetentionPolicy, CreateRetentionPolicyPayload{Name: "b", Database: "db", RetentionDays: 7, Now: t1})
	snap := persistFSM(t, x.f)

	f2 := newTestFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(choiceSnap))); err != nil {
		t.Fatal(err)
	}
	if c, ok := f2.PolicyAdoptChoiceFor(PolicyKindRetention); !ok || c.Node != "w3" {
		t.Fatalf("choice lost: %+v", c)
	}
	if err := f2.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
		t.Fatal(err)
	}
	if !f2.RetentionPoliciesEnabled() || !f2.PoliciesSeeded(PolicyKindRetention) {
		t.Fatal("flag or seed lost")
	}
	if _, ok := f2.PolicyAdoptChoiceFor(PolicyKindRetention); ok {
		t.Fatal("a stale choice came back")
	}
	a, _ := f2.RetentionPolicy(4)
	b, _ := f2.RetentionPolicyByName("b")
	if a.Name != "a" || a.Measurement == nil || *a.Measurement != "cpu" || !a.IsActive || b.ID != 6 {
		t.Fatalf("policies after restore: %+v %+v", a, b)
	}
	st := f2.RetentionMirrorState()
	if st.Floor != 6 || !st.SeededIDs[4] || len(st.SeededIDs) != 1 {
		t.Fatalf("mirror state after restore %+v", st)
	}
	f2.mu.Lock()
	seq := f2.retentionSeq
	f2.mu.Unlock()
	if seq != 6 {
		t.Fatalf("sequence %d, want 6", seq)
	}
	// A restore fires the mirrors.
	fired := 0
	f2.AddPoliciesChangedCallback(func() { fired++ })
	f2.AddPoliciesChangedCallback(func() { fired++ })
	if err := f2.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
		t.Fatal(err)
	}
	if fired != 2 {
		t.Fatalf("callbacks fired %d times, want each once", fired)
	}
}

func persistFSM(t *testing.T, f *ClusterFSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
