package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// #1324: the cluster policies flag applies only when every node in the table
// is listed as supporting it.
func TestClusterPoliciesEnableNeedsEveryNode(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Nodes: []string{"w1", "w2", "w3"}, Now: t0})
	if x.f.ClusterPoliciesEnabled() {
		t.Fatal("enabled with a node missing")
	}
	x.ok(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	if !x.f.ClusterPoliciesEnabled() {
		t.Fatal("not enabled")
	}
	// Applying it again is a no-op.
	x.ok(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Now: t0})
	if !x.f.ClusterPoliciesEnabled() {
		t.Fatal("a second enable turned it off")
	}
}

// The flag survives a snapshot, and a snapshot without the field (an older
// build, or never enabled) is off and does not carry the field.
func TestClusterPoliciesSnapshot(t *testing.T) {
	persist := func(f *ClusterFSM) []byte {
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
	x := takeoverFSM(t)
	off := persist(x.f)
	if bytes.Contains(off, []byte(`"cluster_policies"`)) {
		t.Fatal("snapshot of a cluster that never enabled the flag carries it")
	}
	x.ok(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	on := persist(x.f)

	f2 := newTestFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(on))); err != nil {
		t.Fatal(err)
	}
	if !f2.ClusterPoliciesEnabled() {
		t.Fatal("flag lost")
	}
	if err := f2.Restore(io.NopCloser(bytes.NewReader(off))); err != nil {
		t.Fatal(err)
	}
	if f2.ClusterPoliciesEnabled() {
		t.Fatal("restore of an older snapshot kept the flag")
	}
}

func intp(v int) *int { return &v }

// cpFSM: takeoverFSM with cluster policies enabled.
func cpFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := takeoverFSM(t)
	x.ok(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	return x
}

// Tiering policy commands apply only once the flag is on and tiering is
// seeded; until then each node's own table holds the policies.
func TestTieringPolicyCommandsNeedFlagAndSeed(t *testing.T) {
	x := takeoverFSM(t)
	set := SetTieringPolicyPayload{Database: "db1", HotOnly: true, Now: t0}
	x.refused(CommandSetTieringPolicy, set)
	x.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Now: t0})
	x.ok(CommandEnableClusterPolicies, EnableClusterPoliciesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	if res := x.apply(CommandSetTieringPolicy, set); res == nil || !errors.Is(res.(error), ErrPoliciesNotSeeded) {
		t.Fatalf("set before the seed: %v", res)
	}
	x.refused(CommandDeleteTieringPolicy, DeleteTieringPolicyPayload{Database: "db1"})
	if x.f.PoliciesSeeded(PolicyKindTiering) {
		t.Fatal("seeded without a seed")
	}
}

// The seed installs a kind's policies once; a second seed (a leader change
// mid-seed) changes nothing.
func TestSeedPoliciesOnce(t *testing.T) {
	x := cpFSM(t)
	x.ok(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Now: t0, Tiering: []TieringPolicyEntry{
		{Database: "db1", HotOnly: true}, {Database: "db2", HotMaxAgeDays: intp(30)},
	}})
	if !x.f.PoliciesSeeded(PolicyKindTiering) {
		t.Fatal("not seeded")
	}
	x.ok(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Now: t0, Tiering: []TieringPolicyEntry{{Database: "db9"}}})
	got := x.f.TieringPolicies()
	if len(got) != 2 || got[0].Database != "db1" || !got[0].HotOnly || got[1].HotMaxAgeDays == nil || *got[1].HotMaxAgeDays != 30 {
		t.Fatalf("policies after a second seed: %+v", got)
	}
	if got[0].ID == 0 || !got[0].CreatedAt.Equal(t0) {
		t.Fatalf("seeded entry without identity: %+v", got[0])
	}
	x.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: "nope", Now: t0})
	x2 := cpFSM(t)
	x2.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Now: t0, Tiering: []TieringPolicyEntry{{Database: "a"}, {Database: "a"}}})
	x2.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Now: t0, Tiering: []TieringPolicyEntry{{Database: ""}}})
	if x2.f.PoliciesSeeded(PolicyKindTiering) {
		t.Fatal("a refused seed marked the kind seeded")
	}
}

// Set creates or replaces (keeping the policy's identity), Delete removes, and
// a delete of a missing policy is ErrPolicyNotFound.
func TestTieringPolicySetAndDelete(t *testing.T) {
	x := cpFSM(t)
	x.ok(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Now: t0})
	x.ok(CommandSetTieringPolicy, SetTieringPolicyPayload{Database: "db1", HotOnly: true, Now: t0})
	first, ok := x.f.TieringPolicy("db1")
	if !ok || !first.HotOnly || first.ID == 0 {
		t.Fatalf("after set: %+v %v", first, ok)
	}
	later := t0.Add(time.Hour)
	x.ok(CommandSetTieringPolicy, SetTieringPolicyPayload{Database: "db1", HotMaxAgeDays: intp(14), Now: later})
	second, _ := x.f.TieringPolicy("db1")
	if second.HotOnly || second.HotMaxAgeDays == nil || *second.HotMaxAgeDays != 14 {
		t.Fatalf("a replace kept old fields: %+v", second)
	}
	if second.ID != first.ID || !second.CreatedAt.Equal(t0) || !second.UpdatedAt.Equal(later) {
		t.Fatalf("a replace changed the identity: %+v vs %+v", second, first)
	}
	// The returned copy does not alias the FSM's.
	*second.HotMaxAgeDays = 99
	if again, _ := x.f.TieringPolicy("db1"); *again.HotMaxAgeDays != 14 {
		t.Fatal("a returned policy aliases the FSM state")
	}
	x.ok(CommandDeleteTieringPolicy, DeleteTieringPolicyPayload{Database: "db1"})
	if _, ok := x.f.TieringPolicy("db1"); ok {
		t.Fatal("deleted policy still there")
	}
	res := x.apply(CommandDeleteTieringPolicy, DeleteTieringPolicyPayload{Database: "db1"})
	if err, _ := res.(error); !IsPolicyNotFound(err) {
		t.Fatalf("delete of a missing policy: %v", res)
	}
	if !IsPolicyNotFound(fmt.Errorf("forward apply: leader rejected: %s", res.(error).Error())) {
		t.Fatal("not-found not recognised in its forwarded text form")
	}
	x.refused(CommandSetTieringPolicy, SetTieringPolicyPayload{Database: "", Now: t0})
	x.refused(CommandSetTieringPolicy, SetTieringPolicyPayload{Database: strings.Repeat("d", maxPolicyDatabaseLen+1), Now: t0})
}

// Seeded kinds and policies survive a snapshot; an older snapshot restores to
// unseeded and empty.
func TestClusterPoliciesSnapshotCarriesPolicies(t *testing.T) {
	x := cpFSM(t)
	x.ok(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindTiering, Now: t0, Tiering: []TieringPolicyEntry{{Database: "db1", HotOnly: true}}})
	x.ok(CommandSetTieringPolicy, SetTieringPolicyPayload{Database: "db2", HotMaxAgeDays: intp(3), Now: t0})
	snap, err := x.f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	f2 := newTestFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if !f2.PoliciesSeeded(PolicyKindTiering) {
		t.Fatal("seeded kind lost")
	}
	got := f2.TieringPolicies()
	if len(got) != 2 || !got[0].HotOnly || got[1].HotMaxAgeDays == nil || *got[1].HotMaxAgeDays != 3 {
		t.Fatalf("restored policies: %+v", got)
	}
	old := takeoverFSM(t)
	oldSnap, _ := old.f.Snapshot()
	var ob bytes.Buffer
	if err := oldSnap.Persist(&testSnapshotSink{Writer: &ob}); err != nil {
		t.Fatal(err)
	}
	if err := f2.Restore(io.NopCloser(bytes.NewReader(ob.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if f2.PoliciesSeeded(PolicyKindTiering) || len(f2.TieringPolicies()) != 0 {
		t.Fatal("restore of an older snapshot kept policies")
	}
}
