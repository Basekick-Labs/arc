package raft

import (
	"bytes"
	"io"
	"testing"
)

// #1852: read fences for staged object compaction.

// cfFSM: ccFSM with fences on and an object job j1 committed over a and b.
func cfFSM(t *testing.T) (*tkFSM, StagedCompaction) {
	t.Helper()
	x := ccFSM(t)
	x.ok(CommandEnableCompactionFences, EnableCompactionFencesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	x.ok(CommandStageCompaction, ccStage("j1", true))
	x.ok(CommandCommitCompaction, ccCommit("j1"))
	s, ok := x.f.StagedCompactionByJob("j1")
	if !ok || !s.Acked || s.Phase != PhaseCommitted || s.PhaseIndex == 0 {
		t.Fatalf("committed job %+v", s)
	}
	x.claimFinisher("j1", "w2", t0)
	return x, s
}

func (x *tkFSM) fenceAll(index uint64) {
	x.t.Helper()
	for _, n := range []string{"w1", "w2", "w3", "r1"} {
		x.ok(CommandReadFence, ReadFencePayload{Node: n, Index: index})
	}
}

func (x *tkFSM) advance(from, to string) interface{} {
	x.t.Helper()
	return x.apply(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: from, To: to})
}

func TestCompactionFencesEnableNeedsCompactionCommitAndEveryNode(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandEnableCompactionFences, EnableCompactionFencesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	x.ok(CommandEnableCompactionCommit, EnableCompactionCommitPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	x.refused(CommandEnableCompactionFences, EnableCompactionFencesPayload{Nodes: []string{"w1"}, Now: t0})
	x.ok(CommandEnableCompactionFences, EnableCompactionFencesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	if !x.f.CompactionFencesEnabled() {
		t.Fatal("not enabled")
	}
}

// A job committed with the flag off keeps the legacy path; the fence
// commands refuse it.
func TestLegacyObjectJobIsNotAcked(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", true))
	x.ok(CommandCommitCompaction, ccCommit("j1"))
	x.ok(CommandEnableCompactionFences, EnableCompactionFencesPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	s, _ := x.f.StagedCompactionByJob("j1")
	if s.Acked || s.Phase != "" {
		t.Fatalf("legacy job %+v", s)
	}
	x.claimFinisher("j1", "w2", t0)
	if r := x.conflict(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: "", To: PhasePublished}); r != CompactionNotAcked {
		t.Fatalf("reason %q", r)
	}
	// Legacy finalize needs no fences.
	x.ok(CommandFinalizeCompaction, FinalizeCompactionPayload{Job: "j1", By: "w2", Output: s.Output.Path})
}

func TestReadFenceRules(t *testing.T) {
	x, s := cfFSM(t)
	x.refused(CommandReadFence, ReadFencePayload{Node: "nobody", Index: 1})
	x.refused(CommandReadFence, ReadFencePayload{Node: "w1", Index: 0})
	x.refused(CommandReadFence, ReadFencePayload{Node: "w1", Index: x.idx + 100}) // beyond the log
	x.ok(CommandReadFence, ReadFencePayload{Node: "w1", Index: s.PhaseIndex})
	x.ok(CommandReadFence, ReadFencePayload{Node: "w1", Index: 1}) // older: ignored
	if x.f.ReadFence("w1") != s.PhaseIndex {
		t.Fatalf("fence %d", x.f.ReadFence("w1"))
	}
	if got := x.f.FencesBehind(s.PhaseIndex); len(got) != 3 {
		t.Fatalf("behind %v", got)
	}
}

func TestFencedJobPhases(t *testing.T) {
	x, s := cfFSM(t)
	in := ccInputs()
	if r := x.conflict(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: PhaseCommitted, To: PhasePublished}); r != CompactionFencesBehind {
		t.Fatalf("publish before fences: %q", r)
	}
	if r := x.conflict(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w1", From: PhaseCommitted, To: PhasePublished}); r != CompactionNotFinisher {
		t.Fatalf("by another: %q", r)
	}
	if r := x.conflict(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: PhaseCommitted, To: PhaseSwitched}); r != CompactionWrongPhase {
		t.Fatalf("skip a phase: %q", r)
	}
	x.fenceAll(s.PhaseIndex)
	if res := x.advance(PhaseCommitted, PhasePublished); res != nil {
		t.Fatal(res)
	}
	if res := x.advance(PhaseCommitted, PhasePublished); res != nil {
		t.Fatalf("a retry: %v", res)
	}
	s, _ = x.f.StagedCompactionByJob("j1")
	if r := x.conflict(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: PhasePublished, To: PhaseSwitched}); r != CompactionFencesBehind {
		t.Fatalf("switch before fences: %q", r)
	}
	if outs := x.f.FenceCacheOutputs(0, x.idx); len(outs) != 1 {
		t.Fatalf("published needs a cache round: %v", outs)
	}
	x.fenceAll(s.PhaseIndex)
	x.ok(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: PhasePublished, To: PhaseSwitched})
	s, _ = x.f.StagedCompactionByJob("j1")
	x.fenceAll(s.PhaseIndex)
	x.ok(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: PhaseSwitched, To: PhaseRemoved})
	s, _ = x.f.StagedCompactionByJob("j1")
	if r := x.conflict(CommandFinalizeCompaction, FinalizeCompactionPayload{Job: "j1", By: "w2", Output: s.Output.Path}); r != CompactionFencesBehind {
		t.Fatalf("finalize before fences: %q", r)
	}
	x.fenceAll(s.PhaseIndex)
	x.ok(CommandFinalizeCompaction, FinalizeCompactionPayload{Job: "j1", By: "w2", Output: s.Output.Path})
	x.ok(CommandCleanupCompaction, CleanupCompactionPayload{Job: "j1", By: "w2"})
	if _, ok := x.f.StagedCompactionByJob("j1"); ok {
		t.Fatal("job outlived its cleanup")
	}
	for _, i := range in {
		if _, reg := x.has(i.Path); reg {
			t.Fatalf("input %s registered", i.Path)
		}
	}
}

// Before removed, every other manifest change to the job's output or inputs
// is refused.
func TestFencedJobOwnsItsPaths(t *testing.T) {
	x, s := cfFSM(t)
	x.manifestChanged(CommandDeleteFile, DeleteFilePayload{Path: s.Output.Path, Reason: "delete"})
	x.manifestChanged(CommandRegisterFile, RegisterFilePayload{File: s.Inputs[0]})
	upd := s.Output
	upd.Tier = "cold"
	x.manifestChanged(CommandUpdateFile, UpdateFilePayload{File: upd})
	res := x.apply(CommandBatchFileOps, batch(t, op(t, CommandDeleteFile, DeleteFilePayload{Path: s.Output.Path, Reason: "retention:7d"})))
	if _, ok := res.(*ManifestChangedError); !ok {
		t.Fatalf("batch: %v", res)
	}
	// From removed on the job no longer holds them.
	for _, ph := range []struct{ from, to string }{{PhaseCommitted, PhasePublished}, {PhasePublished, PhaseSwitched}, {PhaseSwitched, PhaseRemoved}} {
		cur, _ := x.f.StagedCompactionByJob("j1")
		x.fenceAll(cur.PhaseIndex)
		x.ok(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: ph.from, To: ph.to})
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: s.Output.Path, Reason: "delete"})
}

// No staged object: the output is deleted, reverting, every fence, then the
// revert registers the inputs again.
func TestFencedJobRevert(t *testing.T) {
	x, s := cfFSM(t)
	if res := x.advance(PhaseCommitted, PhaseReverting); res != nil {
		t.Fatalf("into reverting needs no fence: %v", res)
	}
	s, _ = x.f.StagedCompactionByJob("j1")
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "j1", By: "w2"}); r != CompactionFencesBehind {
		t.Fatalf("revert before fences: %q", r)
	}
	x.fenceAll(s.PhaseIndex)
	x.ok(CommandRevertCompaction, RevertCompactionPayload{Job: "j1", By: "w2"})
	for _, i := range ccInputs() {
		if _, reg := x.has(i.Path); !reg {
			t.Fatalf("input %s not registered again", i.Path)
		}
	}
	if r := x.conflict(CommandAdvanceCompaction, AdvanceCompactionPayload{Job: "j1", By: "w2", From: PhaseReverting, To: PhaseRemoved}); r != CompactionUnknownJob {
		t.Fatalf("after the revert: %q", r)
	}
}

func TestFencesNodeRemovalAndSnapshot(t *testing.T) {
	x, s := cfFSM(t)
	for _, n := range []string{"w1", "w2", "w3"} {
		x.ok(CommandReadFence, ReadFencePayload{Node: n, Index: s.PhaseIndex})
	}
	if x.f.FencesAt(s.PhaseIndex) || !x.f.ReferencesNode("w1") {
		t.Fatal("r1 has no fence")
	}
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "r1"})
	if !x.f.FencesAt(s.PhaseIndex) {
		t.Fatal("a removed node still holds the fence rule")
	}
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
	got, _ := f2.StagedCompactionByJob("j1")
	if !f2.CompactionFencesEnabled() || f2.ReadFence("w2") != s.PhaseIndex || !got.Acked || got.Phase != PhaseCommitted || got.PhaseIndex != s.PhaseIndex {
		t.Fatalf("restored: fences %v, job %+v", f2.CompactionFencesEnabled(), got)
	}
}
