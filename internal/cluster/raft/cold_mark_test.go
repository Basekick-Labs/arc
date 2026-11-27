package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"
	"time"
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

// trustedFSM is enabled, fenced and seeded, with no Alive yet.
func trustedFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := coldMarkFSM(t)
	ackAll(x)
	x.ok(CommandColdMarkSeed, ColdMarkSeedPayload{Final: true, Backend: "s3://b/"})
	return x
}

func aliveAll(x *tkFSM, at time.Time, except ...string) {
	for _, n := range coldMarkNodes {
		if !slices.Contains(except, n) {
			x.ok(CommandColdMarkAlive, ColdMarkAlivePayload{NodeID: n, At: at})
		}
	}
}

// Trusted only while every member's Alive is fresh; a member that stops
// confirming makes it untrusted after maxAge; a removed one stops counting.
func TestColdMarkTrustNeedsFreshAliveFromEveryMember(t *testing.T) {
	x := trustedFSM(t)
	now := t0.Add(time.Hour)
	if ok, why := x.f.ColdMarkTrustedFor(now, ColdMarkAliveMaxAge, nil, "s3://b/"); ok || why == "" {
		t.Fatal("trusted with no Alive at all")
	}
	aliveAll(x, now)
	if ok, why := x.f.ColdMarkTrustedFor(now, ColdMarkAliveMaxAge, nil, "s3://b/"); !ok {
		t.Fatalf("not trusted with every Alive fresh: %s", why)
	}
	// Exactly maxAge old is still fresh; one second more is not.
	if ok, _ := x.f.ColdMarkTrustedFor(now.Add(ColdMarkAliveMaxAge), ColdMarkAliveMaxAge, nil, "s3://b/"); !ok {
		t.Fatal("untrusted at exactly maxAge")
	}
	// r1 stops confirming (a rolled-back build): untrusted after maxAge.
	later := now.Add(ColdMarkAliveMaxAge + time.Second)
	aliveAll(x, later, "r1")
	if ok, why := x.f.ColdMarkTrustedFor(later, ColdMarkAliveMaxAge, nil, "s3://b/"); ok || !strings.Contains(why, "r1") {
		t.Fatalf("trusted with r1 silent past maxAge: %v %q", ok, why)
	}
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "r1"})
	if ok, why := x.f.ColdMarkTrustedFor(later, ColdMarkAliveMaxAge, nil, "s3://b/"); !ok {
		t.Fatalf("a removed node still blocks: %s", why)
	}
	// A Raft server outside the node table counts too.
	if ok, why := x.f.ColdMarkTrustedFor(later, ColdMarkAliveMaxAge, []string{"w1", "raft-x"}, "s3://b/"); ok || !strings.Contains(why, "raft-x") {
		t.Fatalf("trusted with a silent Raft server: %v %q", ok, why)
	}
	// The latest Alive in log order wins, so a clock that ran ahead and was
	// corrected brings its Alive back.
	x.ok(CommandColdMarkAlive, ColdMarkAlivePayload{NodeID: "w1", At: now})
	if got := x.f.ColdMarkLastAlive("w1"); !got.Equal(now) {
		t.Fatalf("last alive %v, want %v", got, now)
	}
}

// An Alive in the reader's future beyond the skew bound is not alive.
func TestColdMarkFutureAliveIsNotAlive(t *testing.T) {
	x := trustedFSM(t)
	now := t0.Add(time.Hour)
	aliveAll(x, now)
	x.ok(CommandColdMarkAlive, ColdMarkAlivePayload{NodeID: "w2", At: now.Add(ColdMarkAliveSkew + time.Second)})
	if ok, why := x.f.ColdMarkTrustedFor(now, ColdMarkAliveMaxAge, nil, "s3://b/"); ok || !strings.Contains(why, "clock") {
		t.Fatalf("trusted with an Alive in the future: %v %q", ok, why)
	}
	x.ok(CommandColdMarkAlive, ColdMarkAlivePayload{NodeID: "w2", At: now.Add(ColdMarkAliveSkew)})
	if ok, why := x.f.ColdMarkTrustedFor(now, ColdMarkAliveMaxAge, nil, "s3://b/"); !ok {
		t.Fatalf("untrusted within the skew bound: %s", why)
	}
}

// The mark is trusted only for the cold store it was seeded from.
func TestColdMarkTrustNeedsTheSameColdStore(t *testing.T) {
	x := trustedFSM(t)
	now := t0.Add(time.Hour)
	aliveAll(x, now)
	for _, id := range []string{"", "s3://other/"} {
		if ok, why := x.f.ColdMarkTrustedFor(now, ColdMarkAliveMaxAge, nil, id); ok || !strings.Contains(why, "cold store") {
			t.Fatalf("identity %q: trusted %v %q", id, ok, why)
		}
	}
}

// A removed node's Alive and Ack are forgotten: rejoining, it must confirm
// again.
func TestColdMarkRemovalForgetsAliveAndAck(t *testing.T) {
	x := trustedFSM(t)
	now := t0.Add(time.Hour)
	aliveAll(x, now)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "r1"})
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "r1", Role: "reader", State: "healthy"}})
	if x.f.ColdMarkAcked("r1") || !x.f.ColdMarkLastAlive("r1").IsZero() {
		t.Fatal("a rejoined node kept its Ack or Alive")
	}
	if ok, _ := x.f.ColdMarkTrustedFor(now, ColdMarkAliveMaxAge, nil, "s3://b/"); ok {
		t.Fatal("trusted with a rejoined node that has not confirmed")
	}
}

// The status never names the cold store: read-level tokens see it.
func TestColdMarkStatusHidesTheBackend(t *testing.T) {
	x := trustedFSM(t)
	raw, err := json.Marshal(x.f.ColdMarkStatus())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "s3://") {
		t.Fatalf("status names the cold store: %s", raw)
	}
}

func TestColdMarkScopeLengthCap(t *testing.T) {
	long := strings.Repeat("m", maxColdMarkSegmentLen+1)
	if validColdMarkScope("db/"+long) || validColdMarkScope(long+"/cpu") || !validColdMarkScope("db/"+long[:maxColdMarkSegmentLen]) {
		t.Fatal("segment length cap wrong")
	}
}

func TestColdMarkAliveRefusals(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandColdMarkAlive, ColdMarkAlivePayload{NodeID: "w1", At: t0}) // not enabled
	x.ok(CommandEnableColdMark, EnableColdMarkPayload{Nodes: coldMarkNodes})
	x.refused(CommandColdMarkAlive, ColdMarkAlivePayload{NodeID: "ghost", At: t0})
	x.refused(CommandColdMarkAlive, ColdMarkAlivePayload{NodeID: "w1"})
}

func TestColdMarkAliveSnapshot(t *testing.T) {
	x := trustedFSM(t)
	aliveAll(x, t0)
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
	if got := f2.ColdMarkLastAlive("r1"); !got.Equal(t0) {
		t.Fatalf("restored alive %v, want %v", got, t0)
	}
}

// An admin reset clears the seed and a backend conflict, keeps the marks, and
// the next seed after the fence trusts again.
func TestColdMarkResetReseeds(t *testing.T) {
	x := trustedFSM(t)
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 9}})
	x.ok(CommandColdMarkSeed, ColdMarkSeedPayload{Final: true, Backend: "s3://other/"})
	if !x.f.ColdMarkStatus().BackendConflict {
		t.Fatal("setup: no conflict")
	}
	x.ok(CommandColdMarkReset, ColdMarkResetPayload{By: "admin"})
	st := x.f.ColdMarkStatus()
	if st.Seeded || st.BackendConflict || !st.Fenced {
		t.Fatalf("after reset: %+v", st)
	}
	if day, has, _ := x.f.ColdMark("db/cpu"); !has || day != 9 {
		t.Fatal("reset dropped a mark")
	}
	x.ok(CommandColdMarkSeed, ColdMarkSeedPayload{Final: true, Backend: "s3://other/"})
	now := t0.Add(time.Hour)
	aliveAll(x, now)
	if ok, why := x.f.ColdMarkTrustedFor(now, ColdMarkAliveMaxAge, nil, "s3://other/"); !ok {
		t.Fatalf("not trusted after reseed: %s", why)
	}
}

// The last raise is the proposer's time, set only when the mark moves up.
func TestColdMarkRaisedAt(t *testing.T) {
	x := takeoverFSM(t)
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 10}, At: t0})
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 9}, At: t0.Add(time.Hour)})
	if got := x.f.ColdMarkRaisedAt("db/cpu"); !got.Equal(t0) {
		t.Fatalf("raised %v after a lower add, want %v", got, t0)
	}
	x.ok(CommandColdMarkAdd, ColdMarkAddPayload{Marks: map[string]int32{"db/cpu": 11}, At: t0.Add(2 * time.Hour)})
	if got := x.f.ColdMarkRaisedAt("db/cpu"); !got.Equal(t0.Add(2 * time.Hour)) {
		t.Fatalf("raised %v", got)
	}
	x.refused(CommandColdMarkReset, ColdMarkResetPayload{}) // not enabled
}
