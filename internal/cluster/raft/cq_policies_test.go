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

// cqFSM: cluster policies and the continuous query flag enabled, w1 primary.
func cqFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := cpFSM(t)
	x.ok(CommandEnableCQPolicies, EnableCQPoliciesPayload{Nodes: allNodes, Now: t0})
	return x
}

func (x *tkFSM) seedCQ(keep bool, floor int64, entries ...CQEntry) {
	x.t.Helper()
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindCQ, Leader: "w1", Now: t0})
	x.ok(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindCQ, Epoch: x.idx, CQ: entries, CQKeepIDs: keep, CQFloor: floor, Now: t0})
}

func cqDef(name string) CreateCQPayload {
	return CreateCQPayload{Name: name, Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_1h",
		Query: "SELECT 1 FROM db.cpu WHERE time >= {start_time} AND time < {end_time}", Interval: "1h", IsActive: true, Now: t0}
}

// The CQ flag is its own: nothing of continuous queries applies before it,
// and it needs every node listed.
func TestCQPoliciesNeedTheirOwnFlag(t *testing.T) {
	x := cpFSM(t)
	x.refused(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindCQ, Leader: "w1", Now: t0})
	x.refused(CommandAwaitAdoption, AwaitAdoptionPayload{Kind: PolicyKindCQ, Now: t0})
	x.refused(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindCQ, Node: "w1", Hash: "h"})
	x.refused(CommandEnableCQPolicies, EnableCQPoliciesPayload{Nodes: []string{"w1"}, Now: t0})
	x.ok(CommandEnableCQPolicies, EnableCQPoliciesPayload{Nodes: allNodes, Now: t0})
	if !x.f.CQPoliciesEnabled() {
		t.Fatal("not enabled")
	}
	x.refused(CommandCreateCQ, cqDef("a"))
	x.ok(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindCQ, Node: "w2", Hash: "h", Now: t0})
}

// Create numbers from the floor with unique names; update merges only what
// it carries, nullable fields by their set flag; delete; IDs never reused.
func TestCQCreateUpdateDelete(t *testing.T) {
	x := cqFSM(t)
	x.seedCQ(false, 5)
	x.ok(CommandCreateCQ, cqDef("a"))
	a, ok := x.f.CQByName("a")
	if !ok || a.ID != 5 || a.LastProcessed != nil {
		t.Fatalf("created %+v", a)
	}
	if res := x.apply(CommandCreateCQ, cqDef("a")); !IsPolicyNameTaken(asErr(res)) {
		t.Fatalf("duplicate name: %v", res)
	}
	bad := cqDef("b")
	bad.Query = ""
	if res := x.apply(CommandCreateCQ, bad); !errors.Is(asErr(res), ErrInvalidCQ) {
		t.Fatalf("empty query: %v", res)
	}
	x.ok(CommandCreateCQ, cqDef("b"))
	desc := "hourly"
	x.ok(CommandUpdateCQ, UpdateCQPayload{ID: 5, SetDescription: true, Description: &desc, Interval: strp("30m"), Now: t1})
	x.ok(CommandUpdateCQ, UpdateCQPayload{ID: 5, SetTagColumns: true, TagColumns: []string{"host"}, Now: t1})
	got, _ := x.f.CQ(5)
	if got.Description == nil || *got.Description != "hourly" || got.Interval != "30m" || len(got.TagColumns) != 1 || got.Query != cqDef("a").Query || !got.IsActive {
		t.Fatalf("after partial updates: %+v", got)
	}
	x.ok(CommandUpdateCQ, UpdateCQPayload{ID: 5, SetDescription: true, Now: t1})
	if got, _ := x.f.CQ(5); got.Description != nil {
		t.Fatal("description null did not clear it")
	}
	if res := x.apply(CommandUpdateCQ, UpdateCQPayload{ID: 5, Name: strp("b")}); !IsPolicyNameTaken(asErr(res)) {
		t.Fatalf("rename onto b: %v", res)
	}
	if res := x.apply(CommandUpdateCQ, UpdateCQPayload{ID: 99, IsActive: boolp(false)}); !IsPolicyNotFound(asErr(res)) {
		t.Fatalf("update of a missing CQ: %v", res)
	}
	x.ok(CommandDeleteCQ, DeleteCQPayload{ID: 5})
	if res := x.apply(CommandDeleteCQ, DeleteCQPayload{ID: 5}); !IsPolicyNotFound(asErr(res)) {
		t.Fatalf("second delete: %v", res)
	}
	x.ok(CommandCreateCQ, cqDef("c"))
	if c, _ := x.f.CQByName("c"); c.ID != 7 {
		t.Fatalf("new ID %d, want 7", c.ID)
	}
}

func tp(t time.Time) *time.Time { return &t }

// The watermark is a compare-and-set, set only by the designated primary.
func TestCQWatermarkCompareAndSet(t *testing.T) {
	x := cqFSM(t)
	x.seedCQ(false, 1)
	x.ok(CommandCreateCQ, cqDef("a"))
	w1 := t0.Add(90*time.Minute + 500*time.Millisecond)
	// Only the primary (w1).
	if res := x.apply(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w2", ID: 1, To: w1}); !errors.Is(asErr(res), ErrCQWatermarkNotPrimary) {
		t.Fatalf("from a standby: %v", res)
	}
	x.ok(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w1", ID: 1, To: w1})
	got, _ := x.f.CQ(1)
	if got.LastProcessed == nil || !got.LastProcessed.Equal(t0.Add(90*time.Minute)) {
		t.Fatalf("watermark %v, want it at whole seconds", got.LastProcessed)
	}
	// From must be what it is now.
	if res := x.apply(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w1", ID: 1, To: t1}); !IsCQWatermarkMoved(asErr(res)) {
		t.Fatalf("from unset while set: %v", res)
	}
	if res := x.apply(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w1", ID: 1, From: tp(t0), To: t1}); !IsCQWatermarkMoved(asErr(res)) {
		t.Fatalf("from a stale value: %v", res)
	}
	// Backwards is allowed when From matches (a manual execute's end_time).
	x.ok(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w1", ID: 1, From: tp(w1), To: t0})
	if got, _ := x.f.CQ(1); !got.LastProcessed.Equal(t0) {
		t.Fatalf("watermark %v, want moved back to %v", got.LastProcessed, t0)
	}
	if res := x.apply(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w1", ID: 9, To: t1}); !IsPolicyNotFound(asErr(res)) {
		t.Fatalf("missing CQ: %v", res)
	}
	// Demoted: no designated primary, and a local-storage proposal is refused
	// whoever sends it; only a shared-storage writer gets through.
	x.ok(CommandDemoteWriter, DemoteWriterPayload{NodeID: "w1"})
	if res := x.apply(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w1", ID: 1, From: tp(t0), To: t1}); !errors.Is(asErr(res), ErrCQWatermarkNotPrimary) {
		t.Fatalf("a demoted primary: %v", res)
	}
	if res := x.apply(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "r1", Shared: true, ID: 1, From: tp(t0), To: t1}); !errors.Is(asErr(res), ErrCQWatermarkNotPrimary) {
		t.Fatalf("a reader in shared storage: %v", res)
	}
	x.ok(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w2", Shared: true, ID: 1, From: tp(t0), To: t1})
	// In shared storage a designation the state still carries (converted
	// from local storage) does not stop the leader: any writer proposal
	// carrying Shared, which only the leader's own can.
	x.ok(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w3"})
	x.ok(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w2", Shared: true, ID: 1, From: tp(t1), To: t1.Add(time.Hour)})
}

// A seed keeps or renumbers IDs like retention's, carries each CQ's
// watermark at whole seconds, and the state survives a snapshot.
func TestCQSeedAndSnapshot(t *testing.T) {
	x := cqFSM(t)
	lpt := t0.Add(1500 * time.Millisecond)
	x.seedCQ(true, 9, CQEntry{ID: 4, Name: "a", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "o",
		Query: "q", Interval: "1h", IsActive: true, LastProcessed: &lpt, TagColumns: []string{"host"}})
	x.ok(CommandCreateCQ, cqDef("b"))
	a, ok := x.f.CQ(4)
	if !ok || a.LastProcessed == nil || !a.LastProcessed.Equal(t0.Add(time.Second)) {
		t.Fatalf("seeded %+v", a)
	}
	if b, _ := x.f.CQByName("b"); b.ID != 9 {
		t.Fatalf("first new ID %d, want the floor 9", b.ID)
	}
	snap := persistFSM(t, x.f)
	f2 := newTestFSM()
	if err := f2.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
		t.Fatal(err)
	}
	if !f2.CQPoliciesEnabled() || !f2.PoliciesSeeded(PolicyKindCQ) {
		t.Fatal("flag or seed lost")
	}
	a2, _ := f2.CQ(4)
	if a2.Name != "a" || a2.LastProcessed == nil || !a2.LastProcessed.Equal(*a.LastProcessed) || len(a2.TagColumns) != 1 {
		t.Fatalf("after restore %+v", a2)
	}
	f2.mu.Lock()
	seq := f2.cqSeq
	f2.mu.Unlock()
	if seq != 9 {
		t.Fatalf("sequence %d, want 9", seq)
	}

	bad := cqFSM(t)
	bad.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindCQ, Leader: "w1", Now: t0})
	ep := bad.idx
	for _, p := range []SeedPoliciesPayload{
		{CQFloor: 0},
		{CQFloor: 5, CQKeepIDs: true, CQ: []CQEntry{{ID: 5, Name: "a", Database: "d", SourceMeasurement: "s", DestinationMeasurement: "o", Query: "q", Interval: "1h"}}},
		{CQFloor: 5, CQ: []CQEntry{{Name: "a", Database: "d", SourceMeasurement: "s", DestinationMeasurement: "o", Query: "", Interval: "1h"}}},
		{CQFloor: 5, Retention: []RetentionPolicyEntry{{Name: "r", Database: "d", RetentionDays: 1}}},
	} {
		p.Kind, p.Epoch, p.Now = PolicyKindCQ, ep, t0
		bad.refused(CommandSeedPolicies, p)
	}
	// A retention seed refuses CQ rows.
	r := rpFSM(t)
	r.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindRetention, Leader: "w1", Now: t0})
	r.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindRetention, Epoch: r.idx, RetentionFloor: 1,
		CQ: []CQEntry{{Name: "a"}}, Now: t0})
}

// A watermark change signals no mirror (it moves at every run); a definition
// change does.
func TestCQWatermarkDoesNotSignalTheMirrors(t *testing.T) {
	x := cqFSM(t)
	x.seedCQ(false, 1)
	fired := 0
	x.f.AddPoliciesChangedCallback(func() { fired++ })
	x.ok(CommandCreateCQ, cqDef("a"))
	if fired != 1 {
		t.Fatalf("create fired %d", fired)
	}
	x.ok(CommandSetCQWatermark, SetCQWatermarkPayload{Proposer: "w1", ID: 1, To: t1})
	if fired != 1 {
		t.Fatalf("a watermark change signalled the mirrors (%d)", fired)
	}
}

// The FSM refuses a seed over the byte limit too (ported from #1328).
func TestCQSeedByteLimit(t *testing.T) {
	x := cqFSM(t)
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindCQ, Leader: "w1", Now: t0})
	var entries []CQEntry
	for i := 0; i < 14; i++ {
		entries = append(entries, CQEntry{Name: fmt.Sprintf("q%d", i), Database: "d", SourceMeasurement: "s", DestinationMeasurement: "o",
			Query: strings.Repeat("x", 60<<10), Interval: "1h"})
	}
	x.refused(CommandSeedPolicies, SeedPoliciesPayload{Kind: PolicyKindCQ, Epoch: x.idx, CQ: entries, CQFloor: 1, Now: t0})
}
