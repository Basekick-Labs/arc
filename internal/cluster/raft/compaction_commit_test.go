package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// #1207 d2: compaction commit.

const ccHour = "db/cpu/2026/10/13/10/"

func ccFile(name, sha string, size int64) FileEntry {
	return FileEntry{
		Path: ccHour + name, SHA256: sha, SizeBytes: size, Database: "db", Measurement: "cpu",
		PartitionTime: time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC), OriginNodeID: "w1", Tier: "hot", CreatedAt: t0,
	}
}

// ccFSM: takeoverFSM with compaction commit on and inputs a, b registered
// (a with coverage, as a writer's file under stream takeover).
func ccFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := takeoverFSM(t)
	x.ok(CommandEnableCompactionCommit, EnableCompactionCommitPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("a.parquet", "sa", 10)})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("b.parquet", "sb", 20)})
	return x
}

func ccInputs() []CompactionInput {
	return []CompactionInput{{Path: ccHour + "a.parquet", SizeBytes: 10, SHA256: "sa"}, {Path: ccHour + "b.parquet", SizeBytes: 20, SHA256: "sb"}}
}

func ccStage(job string, object bool) StageCompactionPayload {
	out := ccFile("o-"+job+".parquet", "so-"+job, 25)
	out.OriginNodeID = "someone-else"
	return StageCompactionPayload{Job: job, Origin: "w2", Output: out, Inputs: ccInputs(), Object: object, Now: t0}
}

func ccCommit(job string) CommitCompactionPayload {
	return CommitCompactionPayload{Job: job, Origin: "w2", Output: ccHour + "o-" + job + ".parquet", SHA256: "so-" + job, Now: t0}
}

func (x *tkFSM) conflict(typ CommandType, p any) string {
	x.t.Helper()
	res := x.apply(typ, p)
	err, _ := res.(error)
	r := CompactionConflictReason(err)
	if r == "" {
		x.t.Fatalf("command %d: %v, want a compaction conflict", typ, res)
	}
	return r
}

func batch(t *testing.T, ops ...BatchFileOp) BatchFileOpsPayload {
	t.Helper()
	return BatchFileOpsPayload{Ops: ops}
}

func op(t *testing.T, typ CommandType, p any) BatchFileOp {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return BatchFileOp{Type: typ, Payload: raw}
}

func (x *tkFSM) has(path string) (*FileEntry, bool) {
	return x.f.GetFile(path)
}

func TestCompactionCommitEnableNeedsEveryNode(t *testing.T) {
	x := takeoverFSM(t)
	x.refused(CommandEnableCompactionCommit, EnableCompactionCommitPayload{Nodes: []string{"w1", "w2", "w3"}, Now: t0})
	if x.f.CompactionCommitEnabled() {
		t.Fatal("enabled with a node missing")
	}
	x.refused(CommandStageCompaction, ccStage("j1", false))
	x.ok(CommandEnableCompactionCommit, EnableCompactionCommitPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	if !x.f.CompactionCommitEnabled() {
		t.Fatal("not enabled")
	}
}

func TestStageCompactionRules(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	s, ok := x.f.StagedCompactionByJob("j1")
	if !ok || s.Origin != "w2" || s.Output.OriginNodeID != "w2" || s.Output.CompactionJob != "j1" || len(s.Inputs) != 2 ||
		s.Inputs[0].SHA256 != "sa" || s.Inputs[1].Database != "db" {
		t.Fatalf("staged: %+v", s)
	}
	// The same stage again is a no-op; another output for the job is not.
	x.ok(CommandStageCompaction, ccStage("j1", false))
	other := ccStage("j1", false)
	other.Output.SHA256 = "different"
	if r := x.conflict(CommandStageCompaction, other); r != CompactionJobExists {
		t.Fatalf("reason %s", r)
	}

	// Inputs must be registered, hot, at the size and checksum read.
	for name, mut := range map[string]func(*StageCompactionPayload){
		"absent": func(p *StageCompactionPayload) { p.Inputs[0].Path = ccHour + "zz.parquet" },
		"size":   func(p *StageCompactionPayload) { p.Inputs[0].SizeBytes = 11 },
		"sha":    func(p *StageCompactionPayload) { p.Inputs[1].SHA256 = "old" },
	} {
		p := ccStage("j-"+name, false)
		mut(&p)
		if r := x.conflict(CommandStageCompaction, p); r != CompactionInputsChanged {
			t.Fatalf("%s: reason %s", name, r)
		}
	}
	cold := ccFile("c.parquet", "sc", 5)
	cold.Tier = "cold"
	x.ok(CommandRegisterFile, RegisterFilePayload{File: cold})
	p := ccStage("j-cold", false)
	p.Inputs = append(p.Inputs, CompactionInput{Path: cold.Path, SizeBytes: 5, SHA256: "sc"})
	if r := x.conflict(CommandStageCompaction, p); r != CompactionInputsChanged {
		t.Fatalf("cold input: reason %s", r)
	}

	// Malformed stages are errors, not conflicts.
	dup := ccStage("j-dup", false)
	dup.Inputs = append(dup.Inputs, dup.Inputs[0])
	x.refused(CommandStageCompaction, dup)
	foreign := ccStage("j-foreign", false)
	foreign.Output.Measurement = "mem"
	x.refused(CommandStageCompaction, foreign)
	empty := ccStage("j-empty", false)
	empty.Inputs = nil
	x.refused(CommandStageCompaction, empty)
	bad := ccStage("j-bad", false)
	bad.Output.Path = "../etc/passwd"
	x.refused(CommandStageCompaction, bad)

	// An output path already registered.
	taken := ccStage("j-taken", false)
	taken.Output.Path = ccHour + "a.parquet"
	if r := x.conflict(CommandStageCompaction, taken); r != CompactionOutputExists {
		t.Fatalf("reason %s", r)
	}
	if len(x.f.StagedCompactions()) != 1 {
		t.Fatalf("staged: %+v", x.f.StagedCompactions())
	}
}

func TestStageCompactionBound(t *testing.T) {
	x := ccFSM(t)
	for i := 0; i < maxStagedCompactions; i++ {
		x.ok(CommandStageCompaction, ccStage(fmt.Sprintf("j%d", i), false))
	}
	if r := x.conflict(CommandStageCompaction, ccStage("one-more", false)); r != CompactionFull {
		t.Fatalf("reason %s", r)
	}
}

func TestCompactionHeld(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	x.refused(CommandCompactionHeld, CompactionHeldPayload{Job: "j1", Node: "w2", SHA256: "so-j1"})    // the origin
	x.refused(CommandCompactionHeld, CompactionHeldPayload{Job: "j1", Node: "zz", SHA256: "so-j1"})    // not a member
	x.refused(CommandCompactionHeld, CompactionHeldPayload{Job: "j1", Node: "r1", SHA256: "torn"})     // another content
	x.conflict(CommandCompactionHeld, CompactionHeldPayload{Job: "nope", Node: "r1", SHA256: "so-j1"}) // unknown job
	x.ok(CommandCompactionHeld, CompactionHeldPayload{Job: "j1", Node: "r1", SHA256: "so-j1"})
	x.ok(CommandCompactionHeld, CompactionHeldPayload{Job: "j1", Node: "w3", SHA256: "so-j1"})
	x.ok(CommandCompactionHeld, CompactionHeldPayload{Job: "j1", Node: "r1", SHA256: "so-j1"})
	s, _ := x.f.StagedCompactionByJob("j1")
	if len(s.Holders) != 2 || s.Holders[0] != "r1" || s.Holders[1] != "w3" {
		t.Fatalf("holders %v", s.Holders)
	}
	x.ok(CommandStageCompaction, ccStage("obj", true))
	x.refused(CommandCompactionHeld, CompactionHeldPayload{Job: "obj", Node: "r1", SHA256: "so-obj"})
}

func TestCommitCompactionLocal(t *testing.T) {
	x := ccFSM(t)
	var deleted []string
	var registered []string
	x.f.SetFileCallbacks(func(e *FileEntry) { registered = append(registered, e.Path) },
		func(p, reason string) { deleted = append(deleted, p+"|"+reason) })
	x.ok(CommandStageCompaction, ccStage("j1", false))
	x.ok(CommandCommitCompaction, ccCommit("j1"))
	out, ok := x.has(ccHour + "o-j1.parquet")
	if !ok || out.CompactionJob != "j1" || out.OriginNodeID != "w2" || out.LSN != x.idx || out.SHA256 != "so-j1" {
		t.Fatalf("output: %+v %v", out, ok)
	}
	for _, in := range ccInputs() {
		if _, ok := x.has(in.Path); ok {
			t.Fatalf("input %s still registered", in.Path)
		}
	}
	if len(registered) != 1 || len(deleted) != 2 || deleted[0] != ccHour+"a.parquet|compaction:j1" {
		t.Fatalf("callbacks: %v %v", registered, deleted)
	}
	if _, ok := x.f.StagedCompactionByJob("j1"); ok {
		t.Fatal("local job still staged after its commit")
	}
	// Retried after it applied (reply lost): done, nothing applied again.
	x.ok(CommandCommitCompaction, ccCommit("j1"))
	if len(registered) != 1 {
		t.Fatalf("replay applied again: %v", registered)
	}
	// Another job's commit under that name is not a replay.
	wrong := ccCommit("j1")
	wrong.SHA256 = "other"
	if r := x.conflict(CommandCommitCompaction, wrong); r != CompactionUnknownJob {
		t.Fatalf("reason %s", r)
	}
}

// A row DELETE that rewrote an input before the commit wins: the compaction
// read the input before the DELETE, so its output holds the deleted rows.
func TestCommitCompactionRefusedAfterDeleteRewrite(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	rw := ccFile("a-rw.parquet", "srw", 8)
	x.ok(CommandBatchFileOps, batch(t,
		op(t, CommandRegisterFile, RegisterFilePayload{File: rw, InheritFrom: ccHour + "a.parquet", InheritSHA: "sa"}),
		op(t, CommandDeleteFile, DeleteFilePayload{Path: ccHour + "a.parquet", Reason: "delete-rewrite", ExpectSHA: "sa"})))
	if r := x.conflict(CommandCommitCompaction, ccCommit("j1")); r != CompactionInputsChanged {
		t.Fatalf("reason %s", r)
	}
	if _, ok := x.has(ccHour + "o-j1.parquet"); ok {
		t.Fatal("output registered next to the rewrite")
	}
	if _, ok := x.has(ccHour + "b.parquet"); !ok {
		t.Fatal("refused commit deleted an input")
	}
	x.ok(CommandAbortCompaction, AbortCompactionPayload{Job: "j1", By: "w2", Reason: "inputs-changed"})
	if _, ok := x.f.StagedCompactionByJob("j1"); ok {
		t.Fatal("aborted job still staged")
	}
	x.ok(CommandAbortCompaction, AbortCompactionPayload{Job: "j1", By: "w2"}) // idempotent
}

// The other direction: the commit applied first, so the DELETE's rewrite of
// an input that is gone is refused, and the DELETE scans the output instead.
func TestDeleteRewriteRefusedAfterCommit(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	x.ok(CommandCommitCompaction, ccCommit("j1"))
	rw := ccFile("a-rw.parquet", "srw", 8)
	res := x.apply(CommandBatchFileOps, batch(t,
		op(t, CommandRegisterFile, RegisterFilePayload{File: rw, InheritFrom: ccHour + "a.parquet", InheritSHA: "sa"}),
		op(t, CommandDeleteFile, DeleteFilePayload{Path: ccHour + "a.parquet", Reason: "delete-rewrite", ExpectSHA: "sa"})))
	var me *ManifestChangedError
	if err, _ := res.(error); !errors.As(err, &me) || len(me.Paths) != 2 {
		t.Fatalf("got %v, want both ops refused", res)
	}
	if _, ok := x.has(rw.Path); ok {
		t.Fatal("rewrite registered next to the compaction output")
	}
	// Single ops too: a full-file delete of a file a compaction replaced.
	res = x.apply(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "b.parquet", Reason: "delete", ExpectSHA: "sb"})
	if err, _ := res.(error); !IsManifestChanged(err) {
		t.Fatalf("single delete: %v", res)
	}
	res = x.apply(CommandRegisterFile, RegisterFilePayload{File: rw, InheritFrom: ccHour + "b.parquet"})
	if err, _ := res.(error); !IsManifestChanged(err) {
		t.Fatalf("single register: %v", res)
	}
	// A changed checksum is refused like an absent file.
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("c.parquet", "new", 3)})
	res = x.apply(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "c.parquet", Reason: "delete", ExpectSHA: "old"})
	if err, _ := res.(error); !IsManifestChanged(err) {
		t.Fatalf("changed sha: %v", res)
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "c.parquet", Reason: "delete", ExpectSHA: "new"})
}

// With the flag off the fields are ignored, as an older build ignores them.
func TestManifestCASOffIgnoresFields(t *testing.T) {
	x := takeoverFSM(t)
	rw := ccFile("a-rw.parquet", "srw", 8)
	x.ok(CommandBatchFileOps, batch(t,
		op(t, CommandRegisterFile, RegisterFilePayload{File: rw, InheritFrom: ccHour + "gone.parquet", InheritSHA: "x"}),
		op(t, CommandDeleteFile, DeleteFilePayload{Path: ccHour + "gone.parquet", ExpectSHA: "x"})))
	if _, ok := x.has(rw.Path); !ok {
		t.Fatal("flag off: rewrite not registered")
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "a-rw.parquet", ExpectSHA: "other"})
	if _, ok := x.has(rw.Path); ok {
		t.Fatal("flag off: delete with another expected checksum not applied")
	}
}

func TestCommitCompactionRules(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	if r := x.conflict(CommandCommitCompaction, ccCommit("nope")); r != CompactionUnknownJob {
		t.Fatalf("reason %s", r)
	}
	other := ccCommit("j1")
	other.Origin = "w3"
	x.refused(CommandCommitCompaction, other)

	// Something registered the output path meanwhile.
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("o-j1.parquet", "foreign", 1)})
	if r := x.conflict(CommandCommitCompaction, ccCommit("j1")); r != CompactionOutputExists {
		t.Fatalf("reason %s", r)
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "o-j1.parquet"})

	// A pause that started after the stage refuses the commit, even resumed.
	pause := SetCompactionPausePayload{Paused: true, Generation: 1, RequestedBy: "w1", Reason: "restore", Now: t0, ExpiresAt: t0.Add(time.Minute)}
	x.ok(CommandSetCompactionPause, pause)
	if r := x.conflict(CommandCommitCompaction, ccCommit("j1")); r != CompactionPaused {
		t.Fatalf("reason %s", r)
	}
	resume := pause
	resume.Paused = false
	x.ok(CommandSetCompactionPause, resume)
	if r := x.conflict(CommandCommitCompaction, ccCommit("j1")); r != CompactionPaused {
		t.Fatalf("after resume: reason %s", r)
	}
	// A job staged after the resume commits.
	x.ok(CommandStageCompaction, ccStage("j2", false))
	x.ok(CommandCommitCompaction, ccCommit("j2"))
}

func TestCommitCompactionPauseInForce(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandSetCompactionPause, SetCompactionPausePayload{Paused: true, Generation: 1, RequestedBy: "w1", Now: t0, ExpiresAt: t0.Add(time.Minute)})
	// Staged by a compactor whose clock is past the pause's expiry (same
	// generation), committed by one that is not.
	st := ccStage("j1", false)
	st.Now = t0.Add(2 * time.Minute)
	x.ok(CommandStageCompaction, st)
	if r := x.conflict(CommandCommitCompaction, ccCommit("j1")); r != CompactionPaused {
		t.Fatalf("reason %s", r)
	}
	// Expired by the proposer's clock: not in force.
	late := ccCommit("j1")
	late.Now = t0.Add(2 * time.Minute)
	x.ok(CommandCommitCompaction, late)
}

func TestCommitCompactionObjectFinalizeAndRevert(t *testing.T) {
	x := ccFSM(t)
	cov := []InstanceCoverage{{Instance: 7, Key: "db/cpu", Ranges: [][2]uint64{{1, 2}}}}
	x.f.mu.Lock()
	x.f.files[ccHour+"a.parquet"].Coverage = cov
	x.f.mu.Unlock()
	x.ok(CommandStageCompaction, ccStage("obj", true))
	x.ok(CommandCommitCompaction, ccCommit("obj"))
	s, ok := x.f.StagedCompactionByJob("obj")
	if !ok || !s.Finalizing {
		t.Fatalf("object job: %+v %v", s, ok)
	}
	if _, ok := x.has(ccHour + "a.parquet"); ok {
		t.Fatal("input still registered")
	}
	x.ok(CommandCommitCompaction, ccCommit("obj")) // replay while finalizing
	if r := x.conflict(CommandAbortCompaction, AbortCompactionPayload{Job: "obj", By: "w2"}); r != CompactionFinalizing {
		t.Fatalf("reason %s", r)
	}

	// The output never reached the bucket: the inputs come back as they were.
	// Only the compactor, or the leader overriding for a compactor that is
	// gone, may decide that.
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w3"}); r != CompactionNotOrigin {
		t.Fatalf("reason %s", r)
	}
	x.ok(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w3", Override: true})
	if _, ok := x.has(ccHour + "o-obj.parquet"); ok {
		t.Fatal("reverted output still registered")
	}
	a, ok := x.has(ccHour + "a.parquet")
	if !ok || a.SHA256 != "sa" || a.OriginNodeID != "w1" || len(a.Coverage) != 1 || a.Coverage[0].Instance != 7 || a.LSN != x.idx {
		t.Fatalf("reverted input: %+v %v", a, ok)
	}
	if _, ok := x.f.StagedCompactionByJob("obj"); ok {
		t.Fatal("reverted job still staged")
	}

	// Finalize, then cleanup: the normal end.
	fin := FinalizeCompactionPayload{Job: "obj2", By: "w2", Output: ccHour + "o-obj2.parquet"}
	x.ok(CommandStageCompaction, ccStage("obj2", true))
	if r := x.conflict(CommandFinalizeCompaction, fin); r != CompactionNotFinalizing {
		t.Fatalf("reason %s", r)
	}
	x.ok(CommandCommitCompaction, ccCommit("obj2"))
	if r := x.conflict(CommandCleanupCompaction, CleanupCompactionPayload{Job: "obj2", By: "r1"}); r != CompactionNotFinalized {
		t.Fatalf("cleanup before finalize: %s", r)
	}
	other := fin
	other.By = "w3"
	if r := x.conflict(CommandFinalizeCompaction, other); r != CompactionNotOrigin {
		t.Fatalf("reason %s", r)
	}
	x.ok(CommandFinalizeCompaction, fin)
	s, ok = x.f.StagedCompactionByJob("obj2")
	if !ok || !s.Finalized {
		t.Fatalf("finalized: %+v %v", s, ok)
	}
	// A finalized job cannot be reverted: its input objects are being deleted.
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj2", By: "w2"}); r != CompactionFinalized {
		t.Fatalf("reason %s", r)
	}
	x.ok(CommandFinalizeCompaction, fin) // retried while finalized
	x.ok(CommandCleanupCompaction, CleanupCompactionPayload{Job: "obj2", By: "r1"})
	if _, ok := x.f.StagedCompactionByJob("obj2"); ok {
		t.Fatal("cleaned-up job still staged")
	}
	x.ok(CommandCleanupCompaction, CleanupCompactionPayload{Job: "obj2", By: "r1"}) // idempotent
	x.ok(CommandFinalizeCompaction, fin)                                            // retried after the job ended: its output is registered
	if _, ok := x.has(ccHour + "o-obj2.parquet"); !ok {
		t.Fatal("finalize removed the output")
	}
	// A finalize retried after a revert removed the output is refused: the
	// compactor must not delete the input objects.
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("a.parquet", "sa", 10)})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("b.parquet", "sb", 20)})
	x.ok(CommandStageCompaction, ccStage2("obj3"))
	x.ok(CommandCommitCompaction, ccCommit("obj3"))
	x.ok(CommandRevertCompaction, RevertCompactionPayload{Job: "obj3", By: "w2"})
	if r := x.conflict(CommandFinalizeCompaction, FinalizeCompactionPayload{Job: "obj3", By: "w2", Output: ccHour + "o-obj3.parquet"}); r != CompactionUnknownJob {
		t.Fatalf("finalize after revert: %s", r)
	}
}

// ccStage2 stages an object job over the inputs registered again by a revert.
func ccStage2(job string) StageCompactionPayload {
	return ccStage(job, true)
}

// A commit retried after it applied, while finalizing, is done even once a
// DELETE removed the output: the compactor must not take the aborted path
// (the input objects would stay in the bucket, unregistered).
func TestCommitReplayWhileFinalizingAfterOutputDeleted(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("obj", true))
	x.ok(CommandCommitCompaction, ccCommit("obj"))
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "o-obj.parquet", Reason: "delete"})
	x.ok(CommandCommitCompaction, ccCommit("obj"))
}

// Every staged-commit command is refused while the flag is off, even with
// inputs that would stage.
func TestCompactionCommandsRefusedWithFlagOff(t *testing.T) {
	x := takeoverFSM(t)
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("a.parquet", "sa", 10)})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("b.parquet", "sb", 20)})
	for typ, p := range map[CommandType]any{
		CommandStageCompaction:    ccStage("j1", false),
		CommandCompactionHeld:     CompactionHeldPayload{Job: "j1", Node: "r1", SHA256: "so-j1"},
		CommandCommitCompaction:   ccCommit("j1"),
		CommandAbortCompaction:    AbortCompactionPayload{Job: "j1", By: "w2"},
		CommandFinalizeCompaction: FinalizeCompactionPayload{Job: "j1", By: "w2"},
		CommandCleanupCompaction:  CleanupCompactionPayload{Job: "j1", By: "w2"},
		CommandRevertCompaction:   RevertCompactionPayload{Job: "j1", By: "w2"},
	} {
		res := x.apply(typ, p)
		err, _ := res.(error)
		if err == nil || !strings.Contains(err.Error(), "not enabled") {
			t.Fatalf("command %d with the flag off: %v", typ, res)
		}
	}
}

// A pause in force refuses a stage (a restore may be about to replace the
// manifest under it).
func TestStageCompactionRefusedWhilePaused(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandSetCompactionPause, SetCompactionPausePayload{Paused: true, Generation: 1, RequestedBy: "w1", Now: t0, ExpiresAt: t0.Add(time.Minute)})
	if r := x.conflict(CommandStageCompaction, ccStage("j1", false)); r != CompactionPaused {
		t.Fatalf("reason %s", r)
	}
	late := ccStage("j1", false)
	late.Now = t0.Add(2 * time.Minute)
	x.ok(CommandStageCompaction, late)
}

// Removing a node drops the jobs it staged and had not committed, and its
// holder entries; a finalizing job of it stays for the leader to finish.
func TestRemoveNodeDropsItsStagedJobs(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	other := ccStage("j2", false)
	other.Origin = "w3"
	x.ok(CommandStageCompaction, other)
	x.ok(CommandCompactionHeld, CompactionHeldPayload{Job: "j2", Node: "w2", SHA256: "so-j2"})
	x.ok(CommandStageCompaction, ccStage("obj", true))
	x.ok(CommandCommitCompaction, ccCommit("obj"))
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2"})
	if _, ok := x.f.StagedCompactionByJob("j1"); ok {
		t.Fatal("a removed node's staged job kept")
	}
	if s, ok := x.f.StagedCompactionByJob("obj"); !ok || !s.Finalizing {
		t.Fatal("a removed node's finalizing job dropped")
	}
	if s, _ := x.f.StagedCompactionByJob("j2"); len(s.Holders) != 0 {
		t.Fatalf("a removed node still holds: %v", s.Holders)
	}
}

// An update keeps the compaction job that registered the file, whatever the
// client sent.
func TestUpdateFileKeepsCompactionJob(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	x.ok(CommandCommitCompaction, ccCommit("j1"))
	e, _ := x.has(ccHour + "o-j1.parquet")
	up := *e
	up.CompactionJob, up.Tier = "", "hot"
	x.ok(CommandUpdateFile, UpdateFilePayload{File: up})
	if got, _ := x.has(up.Path); got.CompactionJob != "j1" {
		t.Fatalf("update dropped the job: %q", got.CompactionJob)
	}
	x.ok(CommandCommitCompaction, ccCommit("j1")) // still recognised
	forged := ccFile("f.parquet", "sf", 1)
	forged.CompactionJob = "jx"
	x.ok(CommandUpdateFile, UpdateFilePayload{File: forged})
	if got, _ := x.has(forged.Path); got.CompactionJob != "" {
		t.Fatalf("update took a client value: %q", got.CompactionJob)
	}
}

func TestRevertCompactionRefusals(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("obj", true))
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w3"}); r != CompactionNotFinalizing {
		t.Fatalf("reason %s", r)
	}
	x.ok(CommandCommitCompaction, ccCommit("obj"))
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w3"}); r != CompactionNotOrigin {
		t.Fatalf("reason %s", r)
	}
	// An input path registered again (a restore) refuses the revert.
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("b.parquet", "sb", 20)})
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w2"}); r != CompactionInputsRegistered {
		t.Fatalf("reason %s", r)
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "b.parquet"})
	// The output left the manifest (a DELETE or DROP): nothing to revert.
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "o-obj.parquet", Reason: "delete"})
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w2"}); r != CompactionOutputGone {
		t.Fatalf("reason %s", r)
	}
	if _, ok := x.has(ccHour + "a.parquet"); ok {
		t.Fatal("refused revert registered an input")
	}
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "zz", By: "w3"}); r != CompactionUnknownJob {
		t.Fatalf("reason %s", r)
	}
}

// Only the FSM sets CompactionJob: a client cannot make a registration look
// like a committed job (the commit's replay rule keys on it).
func TestCompactionJobClientValueIgnored(t *testing.T) {
	x := ccFSM(t)
	e := ccFile("o-j1.parquet", "so-j1", 25)
	e.CompactionJob = "j1"
	x.ok(CommandRegisterFile, RegisterFilePayload{File: e})
	if got, _ := x.has(e.Path); got.CompactionJob != "" {
		t.Fatalf("single register kept %q", got.CompactionJob)
	}
	e2 := ccFile("o-j2.parquet", "so-j2", 25)
	e2.CompactionJob = "j2"
	x.ok(CommandBatchFileOps, batch(t, op(t, CommandRegisterFile, RegisterFilePayload{File: e2})))
	if got, _ := x.has(e2.Path); got.CompactionJob != "" {
		t.Fatalf("batch register kept %q", got.CompactionJob)
	}
	if r := x.conflict(CommandCommitCompaction, ccCommit("j2")); r != CompactionUnknownJob {
		t.Fatalf("reason %s", r)
	}
}

func TestCompactionCommitSnapshot(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("j1", false))
	x.ok(CommandCompactionHeld, CompactionHeldPayload{Job: "j1", Node: "r1", SHA256: "so-j1"})
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
	if !f2.CompactionCommitEnabled() {
		t.Fatal("flag lost")
	}
	s, ok := f2.StagedCompactionByJob("j1")
	if !ok || len(s.Inputs) != 2 || len(s.Holders) != 1 || s.Output.CompactionJob != "j1" {
		t.Fatalf("restored: %+v %v", s, ok)
	}
	// A snapshot without the field (an older build, or never enabled) is off.
	x2 := takeoverFSM(t)
	snap2, _ := x2.f.Snapshot()
	var buf2 bytes.Buffer
	if err := snap2.Persist(&testSnapshotSink{Writer: &buf2}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf2.Bytes(), []byte(`"compaction_commit"`)) {
		t.Fatal("snapshot of a cluster that never enabled the flag carries it")
	}
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf2.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if f2.CompactionCommitEnabled() || len(f2.StagedCompactions()) != 0 {
		t.Fatal("restore of an older snapshot kept the flag or staged jobs")
	}
}

func TestCompactionConflictReasonText(t *testing.T) {
	err := &CompactionConflictError{Job: "j: 1", Reason: CompactionInputsChanged}
	wrapped := fmt.Errorf("forward apply: leader rejected (code=apply_failed): %s", err.Error())
	if r := CompactionConflictReason(wrapped); r != CompactionInputsChanged {
		t.Fatalf("text form: %q", r)
	}
	if CompactionConflictReason(errors.New("other")) != "" || CompactionConflictReason(nil) != "" {
		t.Fatal("non-conflict parsed")
	}
	if !IsManifestChanged(fmt.Errorf("x: %s", (&ManifestChangedError{Paths: []string{"a"}}).Error())) {
		t.Fatal("manifest changed text form")
	}
}

// The output path registered again by something else (another content, no
// job) is not this job's output: no revert, and a finalize retried after the
// job ended is not done.
func TestCompactionOutputPathReusedIsNotTheJobs(t *testing.T) {
	x := ccFSM(t)
	x.ok(CommandStageCompaction, ccStage("obj", true))
	x.ok(CommandCommitCompaction, ccCommit("obj"))
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "o-obj.parquet", Reason: "delete"})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("o-obj.parquet", "so-obj", 25)})
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w2"}); r != CompactionOutputGone {
		t.Fatalf("revert over a reused path: %s", r)
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "o-obj.parquet"})
	other := ccFile("o-obj.parquet", "other", 25)
	x.f.mu.Lock()
	x.f.files[other.Path] = &FileEntry{Path: other.Path, SHA256: "other", CompactionJob: "obj", Database: "db"}
	x.f.mu.Unlock()
	if r := x.conflict(CommandRevertCompaction, RevertCompactionPayload{Job: "obj", By: "w2"}); r != CompactionOutputGone {
		t.Fatalf("revert over another content: %s", r)
	}
	x.f.mu.Lock()
	delete(x.f.files, other.Path)
	x.f.mu.Unlock()

	// Finalize after the job ended, with the path now someone else's.
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("a.parquet", "sa", 10)})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("b.parquet", "sb", 20)})
	x.ok(CommandStageCompaction, ccStage("obj2", true))
	x.ok(CommandCommitCompaction, ccCommit("obj2"))
	x.ok(CommandRevertCompaction, RevertCompactionPayload{Job: "obj2", By: "w2"})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("o-obj2.parquet", "so-obj2", 25)})
	if r := x.conflict(CommandFinalizeCompaction, FinalizeCompactionPayload{Job: "obj2", By: "w2", Output: ccHour + "o-obj2.parquet"}); r != CompactionUnknownJob {
		t.Fatalf("finalize over a reused path: %s", r)
	}
}
