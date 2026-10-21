package raft

import (
	"bytes"
	"io"
	"testing"
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
