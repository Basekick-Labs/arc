package raft

import (
	"bytes"
	"io"
	"testing"
)

// #1552: the cold mark only rises, is trusted only once enabled, fenced and
// seeded from one cold store, and survives a snapshot.

var coldMarkNodes = []string{"w1", "w2", "w3", "r1"}

func coldMarkFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := takeoverFSM(t)
	x.ok(CommandEnableColdMark, EnableColdMarkPayload{Nodes: coldMarkNodes})
	return x
}

func ackAll(x *tkFSM) {
	for _, n := range coldMarkNodes {
		x.ok(CommandColdMarkAck, ColdMarkAckPayload{NodeID: n})
	}
}

func TestColdMarkAddOnlyRaises(t *testing.T) {
	x := takeoverFSM(t) // Add applies before Enable too
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 100}})
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 90}})
	if day, has, _ := x.f.ColdMark("db/cpu"); !has || day != 100 {
		t.Fatalf("mark %d %v after a lower add, want 100", day, has)
	}
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 120}})
	if day, _, _ := x.f.ColdMark("db/cpu"); day != 120 {
		t.Fatalf("mark %d, want 120", day)
	}
	if !x.f.ColdMarkCovers("db/cpu", 120) || x.f.ColdMarkCovers("db/cpu", 121) || x.f.ColdMarkCovers("db/mem", 1) {
		t.Fatal("ColdMarkCovers wrong")
	}
}

// A command with one invalid scope changes nothing.
func TestColdMarkInvalidAddChangesNothing(t *testing.T) {
	x := takeoverFSM(t)
	for _, bad := range []string{"db", "db/", "/cpu", "db/cpu/x", "../cpu", "db/c\\pu"} {
		x.refused(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/ok": 5, bad: 5}})
	}
	x.refused(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/ok": -1}})
	if _, has, _ := x.f.ColdMark("db/ok"); has {
		t.Fatal("a refused add raised a mark")
	}
	big := make(map[string]int32, maxColdMarkEntries+1)
	for i := 0; i <= maxColdMarkEntries; i++ {
		big["db/m"+string(rune('a'+i%26))+string(rune('a'+i/26%26))+string(rune('a'+i/676))] = 1
	}
	x.refused(CommandColdMarkAdd, ColdMarkAddPayload{Marks: big})
}

func TestColdMarkEnableNeedsEveryNode(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandEnableColdMark, EnableColdMarkPayload{Nodes: []string{"w1", "w2", "w3"}})
	if x.f.ColdMarkStatus().Enabled {
		t.Fatal("enabled with a node missing")
	}
	x.ok(CommandEnableColdMark, EnableColdMarkPayload{Nodes: coldMarkNodes})
	if st := x.f.ColdMarkStatus(); !st.Enabled || st.Fenced || len(st.AckPending) != 4 {
		t.Fatalf("status %+v", st)
	}
}

// Fenced latches only when every node at enable acked; a removal of a node
// that never acked completes it.
func TestColdMarkFence(t *testing.T) {
	x := coldMarkFSM(t)
	x.refused(CommandColdMarkSeed, ColdMarkSeedPayload{Final: true, Backend: "s3://b/"})
	for _, n := range []string{"w1", "w2", "w3"} {
		x.ok(CommandColdMarkAck, ColdMarkAckPayload{NodeID: n})
	}
	if x.f.ColdMarkStatus().Fenced {
		t.Fatal("fenced with r1 not acked")
	}
	x.refused(CommandColdMarkAck, ColdMarkAckPayload{NodeID: "ghost"})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "r1"})
	if !x.f.ColdMarkStatus().Fenced {
		t.Fatal("removing the last unacked node did not complete the fence")
	}
	// A node joining afterwards does not reopen it.
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "r1", Role: "reader", State: "healthy"}})
	if !x.f.ColdMarkStatus().Fenced {
		t.Fatal("a join reopened the fence")
	}
}

func TestColdMarkAckBeforeEnableRefused(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandColdMarkAck, ColdMarkAckPayload{NodeID: "w1"})
}

func TestColdMarkSeedAndTrust(t *testing.T) {
	x := coldMarkFSM(t)
	ackAll(x)
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 200}})
	x.ok(CommandColdMarkSeed, ColdMarkSeedPayload{Marks: map[string]int32{"db/cpu": 150, "db/mem": 10}})
	if _, _, trusted := x.f.ColdMark("db/cpu"); trusted {
		t.Fatal("trusted before the final seed chunk")
	}
	x.refused(CommandColdMarkSeed, ColdMarkSeedPayload{Final: true}) // no backend
	x.ok(CommandColdMarkSeed, ColdMarkSeedPayload{Final: true, Backend: "s3://b/p"})
	day, has, trusted := x.f.ColdMark("db/cpu")
	if !has || day != 200 || !trusted {
		t.Fatalf("db/cpu = %d %v %v, want 200 (a seed never lowers), trusted", day, has, trusted)
	}
	if _, has, trusted := x.f.ColdMark("db/none"); has || !trusted {
		t.Fatal("an unmarked scope must read as no cold data, trusted")
	}
	// A seed from another cold store: never trusted again.
	x.ok(CommandColdMarkSeed, ColdMarkSeedPayload{Final: true, Backend: "s3://other/p"})
	if _, _, trusted := x.f.ColdMark("db/cpu"); trusted {
		t.Fatal("trusted after seeds from two cold stores")
	}
}

func TestColdMarkSnapshot(t *testing.T) {
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
	if off := persist(x.f); bytes.Contains(off, []byte(`"cold_mark"`)) {
		t.Fatal("snapshot carries an untouched cold mark")
	}
	x.ok(CommandEnableColdMark, EnableColdMarkPayload{Nodes: coldMarkNodes})
	ackAll(x)
	x.ok(CommandColdMarkSeed, ColdMarkSeedPayload{Marks: map[string]int32{"db/cpu": 7}, Final: true, Backend: "s3://b/"})

	f2 := newTestFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(persist(x.f)))); err != nil {
		t.Fatal(err)
	}
	if day, has, trusted := f2.ColdMark("db/cpu"); day != 7 || !has || !trusted {
		t.Fatalf("restored %d %v %v", day, has, trusted)
	}
	if !f2.ColdMarkAcked("w1") {
		t.Fatal("acks lost")
	}
}
