package raft

import (
	"testing"

	"github.com/rs/zerolog"
)

// orphanFinished: w2's instance 7 orphaned (new disks), claimed by w1 for
// batches 1-5, of which 1-3 were published and 4-5 could not be, and w3
// reported holding nothing: every writer finished with it.
func orphanFinished(t *testing.T) (*tkFSM, uint64) {
	t.Helper()
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{}) // 7 orphaned
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	rep := cov(7, "db/cpu", 1, 3)
	rep.ClaimEpoch = e
	if k := x.register(regPayload("r", "w1", hourA, rep)); k != "" {
		t.Fatalf("claimant file: %s", k)
	}
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e, Unreplayable: map[string]Intervals{"db/cpu": {{4, 5}}}})
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0, Claims: []ClaimSet{{Instance: 7}}})
	return x, e
}

// An orphan every writer finished with is retired, not removed: its index
// arbitrates the copies its origin may still bring back.
func TestOrphanIsRetiredWithItsIndex(t *testing.T) {
	x, e := orphanFinished(t)
	in := x.inst(7)
	if in == nil || in.Status != InstanceRetired || in.RetiredAt == 0 {
		t.Fatalf("instance %+v", in)
	}
	if x.f.Tombstoned(7) || !x.f.takeover.claims[7].active() || x.f.takeover.index[7]["db/cpu"] == nil {
		t.Fatal("retiring dropped the claim, the index or tombstoned it")
	}
	// w2 comes back with the old volume: published rows are duplicates, the
	// unpublished ones (4-5) and the ones nobody had (9) register.
	if k := x.register(regPayload("o1", "w2", hourA, cov(7, "db/cpu", 2, 2))); k != RefusedDuplicate {
		t.Fatalf("a published batch: %q", k)
	}
	if k := x.register(regPayload("o2", "w2", hourA, cov(7, "db/cpu", 4, 5))); k != "" {
		t.Fatalf("an unpublished batch: %s", k)
	}
	if k := x.register(regPayload("o3", "w2", hourA, cov(7, "db/cpu", 9, 9))); k != "" {
		t.Fatalf("a batch nobody had: %s", k)
	}
	// Nothing claims it any more, and nothing waits on it.
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{9, 9}}}}}})
	if fu := x.f.FollowUps(); len(fu) != 0 {
		t.Fatalf("follow-ups on a retired instance: %+v", fu)
	}
	// A late claimant file of the finished claim is still judged as before.
	late := cov(7, "db/cpu", 2, 2)
	late.ClaimEpoch = e
	if k := x.register(regPayload("r2", "w1", hourA, late)); k != RefusedDuplicate {
		t.Fatalf("a late claimant copy: %q", k)
	}
}

// The origin back with the volume adopts its retired instance, publishes,
// and settles it: then it is removed and tombstoned.
func TestRetiredInstanceIsAdoptedBack(t *testing.T) {
	x, _ := orphanFinished(t)
	x.announce("w2", 9, Continuity{}, 7)
	if in := x.inst(7); in.Status != InstanceAdopted || in.AdoptedBy != 9 {
		t.Fatalf("instance %+v", in)
	}
	x.ok(CommandInstanceSettled, InstanceSettledPayload{Origin: "w2", Instance: 7})
	if x.inst(7) != nil || !x.f.Tombstoned(7) {
		t.Fatal("settled instance not removed")
	}
}

// An announcement without it, or a removal of its origin, leaves it retired.
func TestRetiredInstanceStaysRetired(t *testing.T) {
	x, _ := orphanFinished(t)
	x.announce("w2", 9, Continuity{})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2"})
	if in := x.inst(7); in == nil || in.Status != InstanceRetired {
		t.Fatalf("instance %+v", in)
	}
	g := roundTrip(t, x.f)
	if in := g.takeover.instances[7]; in == nil || in.Status != InstanceRetired || in.RetiredAt == 0 || g.takeover.index[7] == nil {
		t.Fatalf("after restore: %+v", in)
	}
}

// A closed instance (same disks, held nothing) is removed as before: no
// late copy of it can exist.
func TestClosedInstanceIsRemovedNotRetired(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{WAL: 7, Journal: 7, Storage: 7})
	for _, by := range []string{"w1", "w3"} {
		x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: by, Now: t0, Claims: []ClaimSet{{Instance: 7}}})
	}
	if x.inst(7) != nil || !x.f.Tombstoned(7) {
		t.Fatalf("closed instance %+v", x.inst(7))
	}
}

// An untracked orphan has no index to keep: it is removed.
func TestUntrackedOrphanIsRemoved(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{})
	in := x.f.takeover.instances[7]
	in.Untracked, in.ReportedBy = true, []string{"w1", "w3"}
	x.f.removeIfAllReportedLocked(in)
	if x.inst(7) != nil {
		t.Fatalf("untracked orphan %+v", x.inst(7))
	}
}

func TestRetiredInstancesAreBounded(t *testing.T) {
	x := takeoverFSM(t)
	for i := 0; i <= maxRetiredInstances; i++ {
		id := uint64(1000 + i)
		in := &StreamInstance{Instance: id, Origin: "w2", Status: InstanceOrphaned}
		x.f.takeover.instances[id] = in
		x.f.applyStarted.Store(uint64(5000 - i)) // later ids retire at lower indexes
		x.f.retireLocked(in)
	}
	n := 0
	for _, in := range x.f.takeover.instances {
		if in.Status == InstanceRetired {
			n++
		}
	}
	last := uint64(1000 + maxRetiredInstances)
	if n != maxRetiredInstances || x.f.takeover.instances[last] != nil || !x.f.Tombstoned(last) {
		t.Fatalf("%d retired; lowest retire index kept: %v", n, x.f.takeover.instances[last] != nil)
	}
}

// A finished claim keeps refusing the origin's copies of an untracked
// instance: nothing else arbitrates them.
func TestFinishedClaimOfUntrackedInstanceStillRefuses(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	x.f.takeover.instances[7].Untracked = true
	if k := x.register(regPayload("o", "w2", hourA, cov(7, "db/cpu", 3, 3))); k != RefusedClaimed {
		t.Fatalf("untracked: %q", k)
	}
	// Tracked, a truncated coverage is refused too.
	y := takeoverFSM(t)
	y.announce("w2", 7, Continuity{})
	e = y.claim("w1", "w2", 7, "db/cpu", 1, 5)
	y.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	tr := InstanceCoverage{Instance: 7, Key: "db/cpu", Truncated: true}
	if k := y.register(regPayload("o", "w2", hourA, tr)); k != RefusedClaimed {
		t.Fatalf("truncated: %q", k)
	}
}

// A vanished file is listed (bounded, snapshotted), and marks an unfinished
// claim of its instance so the claimant publishes again what it had found
// registered.
func TestVanishedFilesAreListedAndMarkTheClaim(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	if k := x.register(regPayload("f", "w2", hourA, cov(7, "db/cpu", 1, 4))); k != "" {
		t.Fatal(k)
	}
	if k := x.register(regPayload("g", "w2", hourA, cov(7, "db/mem", 1, 4))); k != "" {
		t.Fatal(k)
	}
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 9)
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "g", Reason: "compaction"})
	if len(x.f.VanishedFiles()) != 0 || x.f.takeover.claims[7].VanishedAt != 0 {
		t.Fatal("an ordinary delete listed as vanished")
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "f", Reason: DeleteReasonVanished})
	if v := x.f.VanishedFiles(); len(v) != 1 || v[0].Path != "f" || v[0].SizeBytes != 1 {
		t.Fatalf("vanished %+v", v)
	}
	if k := x.f.takeover.claims[7]; k.VanishedAt != x.idx || k.Epoch != e {
		t.Fatalf("claim %+v", k)
	}
	if x.f.CoverageCovered(7, "db/cpu", CoveragePiece{CSeq: 2}, mustTime(hourA)) {
		t.Fatal("still covered")
	}
	g := roundTrip(t, x.f)
	if v := g.VanishedFiles(); len(v) != 1 || v[0].Path != "f" {
		t.Fatalf("after restore %+v", v)
	}
	// A vanished file whose batches the index no longer has does not mark it.
	before := x.f.takeover.claims[7].VanishedAt
	if k := x.register(regPayload("h", "w2", hourB, cov(7, "db/cpu", 20, 20))); k != "" {
		t.Fatal(k)
	}
	x.f.takeover.index[7]["db/cpu"].Hours = nil
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "h", Reason: DeleteReasonVanished})
	if x.f.takeover.claims[7].VanishedAt != before {
		t.Fatal("marked without anything taken back")
	}
	for i := 0; i < maxVanishedPaths+5; i++ {
		x.f.takeoverNoteVanishedLocked(FileEntry{Path: "p"})
	}
	if n := len(x.f.VanishedFiles()); n != maxVanishedPaths {
		t.Fatalf("%d vanished kept", n)
	}
}

// A DELETE rewrite's inherited coverage is marked; a client cannot set it.
func TestInheritedCoverageIsMarked(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	if k := x.register(regPayload("f", "w1", hourA, cov(7, "db/cpu", 1, 4))); k != "" {
		t.Fatal(k)
	}
	spoof := regPayload("s", "w1", hourA, cov(7, "db/cpu", 9, 9))
	spoof.File.CoverageInherited = true
	if k := x.register(spoof); k != "" {
		t.Fatal(k)
	}
	if e, _ := x.f.GetFile("s"); e.CoverageInherited {
		t.Fatal("client-set inherited flag kept")
	}
	rw := regPayload("f2", "w1", hourA)
	rw.InheritFrom = "f"
	if ref := x.batch(regOp(t, rw), delOp(t, "f", "delete-rewrite")); len(ref) != 0 {
		t.Fatalf("rewrite refused: %+v", ref)
	}
	if e, _ := x.f.GetFile("f2"); !e.CoverageInherited || len(e.Coverage) != 1 {
		t.Fatalf("rewrite %+v", e)
	}
	g := roundTrip(t, x.f)
	if e, _ := g.GetFile("f2"); !e.CoverageInherited {
		t.Fatal("lost on restore")
	}
}

// ClaimedFromOrigin mirrors the claimed rule for an origin's file.
func TestClaimedFromOrigin(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	if x.f.ClaimedFromOrigin(7, "db/cpu", 3) {
		t.Fatal("unclaimed")
	}
	e := x.claim("w1", "w2", 7, "db/cpu", 2, 4)
	if !x.f.ClaimedFromOrigin(7, "db/cpu", 3) || x.f.ClaimedFromOrigin(7, "db/cpu", 5) || x.f.ClaimedFromOrigin(7, "db/mem", 3) {
		t.Fatal("unfinished claim")
	}
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	if x.f.ClaimedFromOrigin(7, "db/cpu", 3) {
		t.Fatal("finished claim of a tracked instance")
	}
	x.f.takeover.instances[7].Untracked = true
	if !x.f.ClaimedFromOrigin(7, "db/cpu", 3) {
		t.Fatal("finished claim of an untracked instance")
	}
	x.f.takeover.claims[7].Released = true
	if x.f.ClaimedFromOrigin(7, "db/cpu", 3) {
		t.Fatal("released claim")
	}
}

// HotFilesOfInstance lists hot files with the instance's own coverage;
// InstanceKeys the keys of its index.
func TestHotFilesOfInstanceAndKeys(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.register(regPayload("b", "w2", hourA, cov(7, "db/cpu", 1, 1)))
	x.register(regPayload("a", "w2", hourA, cov(7, "db/mem", 1, 1)))
	x.register(regPayload("c", "w2", hourA, cov(8, "db/cpu", 1, 1)))
	cold := regPayload("d", "w2", hourA, cov(7, "db/disk", 1, 1))
	cold.File.Tier = "cold"
	x.register(cold)
	rw := regPayload("e", "w2", hourA)
	rw.InheritFrom = "b"
	x.batch(regOp(t, rw), delOp(t, "b", "delete-rewrite"))
	got := x.f.HotFilesOfInstance(7)
	if len(got) != 1 || got[0].Path != "a" {
		t.Fatalf("files %+v", got)
	}
	if k := x.f.InstanceKeys(7); len(k) != 3 || k[0] != "db/cpu" || k[2] != "db/mem" {
		t.Fatalf("keys %v", k)
	}
}

// A late claimant file with another epoch on a finished claim is still
// refused as claimed (only origin files pass a finished claim).
func TestFinishedClaimRefusesAnotherEpochAsClaimed(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	c := cov(7, "db/cpu", 3, 3)
	c.ClaimEpoch = e + 100
	if k := x.register(regPayload("r", "w3", hourA, c)); k != RefusedClaimed {
		t.Fatalf("kind %q", k)
	}
}

// A re-registration of an inherited file without coverage keeps the mark.
func TestReRegistrationKeepsInheritedMark(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("f", "w1", hourA, cov(7, "db/cpu", 1, 4)))
	rw := regPayload("f2", "w1", hourA)
	rw.InheritFrom = "f"
	x.batch(regOp(t, rw), delOp(t, "f", "delete-rewrite"))
	again := regPayload("f2", "w1", hourA)
	again.File.SHA256 = "other"
	x.ok(CommandRegisterFile, again)
	if e, _ := x.f.GetFile("f2"); !e.CoverageInherited || len(e.Coverage) != 1 {
		t.Fatalf("after re-registration %+v", e)
	}
}

// Removing the origin of a retired instance leaves it as it was.
func TestRetiredInstanceKeepsItsRetireIndex(t *testing.T) {
	x, _ := orphanFinished(t)
	at := x.inst(7).RetiredAt
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2"})
	if in := x.inst(7); in.RetiredAt != at || in.OriginGone {
		t.Fatalf("instance %+v, retired at %d before", in, at)
	}
}

// A takeover state holding only vanished files survives a snapshot.
func TestVanishedOnlySnapshot(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	f.takeover.vanished = []VanishedFile{{Path: "p", SizeBytes: 3}}
	if v := roundTrip(t, f).VanishedFiles(); len(v) != 1 || v[0].Path != "p" {
		t.Fatalf("vanished %+v", v)
	}
}

// VanishedAt marks an unfinished claim only when the vanished file's
// batches were in the index for that hour (whole or pieces), never a
// finished claim.
func TestVanishedAtOnlyWhenTakenBack(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.register(regPayload("a", "w2", hourA, cov(7, "db/cpu", 1, 2)))
	x.register(regPayload("p", "w2", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 9, RowLo: 0, RowHi: 1, RowTotal: 2}}}))
	if k := x.register(regPayload("b", "w2", hourA, cov(7, "db/cpu", 5, 5))); k != "" {
		t.Fatal(k)
	}
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 20)
	// Index has the hour, but not this file's batches (removed by hand).
	x.f.takeover.index[7]["db/cpu"].Hours[mustTime(hourA).Unix()].Whole = Intervals{{1, 2}}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "b", Reason: DeleteReasonVanished})
	if x.f.takeover.claims[7].VanishedAt != 0 {
		t.Fatal("marked though nothing was taken back")
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "p", Reason: DeleteReasonVanished})
	if x.f.takeover.claims[7].VanishedAt != x.idx {
		t.Fatal("a vanished piece not marked")
	}
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "a", Reason: DeleteReasonVanished})
	if x.f.takeover.claims[7].VanishedAt == x.idx {
		t.Fatal("a finished claim marked")
	}
}

// Retiring during an announcement can remove another retired instance
// that sorts after it: the announcement must not touch it afterwards.
func TestAnnounceSurvivesARetireEviction(t *testing.T) {
	x := takeoverFSM(t)
	for i := 0; i < maxRetiredInstances; i++ {
		id := uint64(1000 + i)
		x.f.takeover.instances[id] = &StreamInstance{Instance: id, Origin: "w3", Status: InstanceRetired, RetiredAt: 1}
	}
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 2)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	x.f.takeover.instances[7].ReportedBy = []string{"w3"}
	x.announce("w2", 8, Continuity{}) // 7 orphaned -> retired -> 1000 evicted
	if in := x.inst(7); in == nil || in.Status != InstanceRetired || x.inst(1000) != nil {
		t.Fatalf("7 %+v, 1000 %+v", in, x.inst(1000))
	}
}

// A live instance is never untracked for the cluster-wide index bound while
// a retired one can make room.
func TestRetiredIndexMakesRoomForALiveOne(t *testing.T) {
	x, _ := orphanFinished(t)
	x.announce("w3", 30, Continuity{})
	room := maxIndexEntries - x.f.takeover.entries
	x.f.takeover.entries += room // full
	if k := x.register(regPayload("live", "w3", hourA, cov(30, "db/cpu", 1, 1))); k != "" {
		t.Fatal(k)
	}
	if in := x.inst(30); in.Untracked || x.inst(7) != nil || !x.f.Tombstoned(7) {
		t.Fatalf("live %+v, retired %+v", in, x.inst(7))
	}
	// Claimant files see the room too.
	y, _ := orphanFinished(t)
	y.announce("w3", 30, Continuity{})
	e := y.claim("w1", "w3", 30, "db/cpu", 1, 5)
	y.f.takeover.entries += maxIndexEntries - y.f.takeover.entries
	c := cov(30, "db/cpu", 1, 1)
	c.ClaimEpoch = e
	if k := y.register(regPayload("r30", "w1", hourA, c)); k != "" {
		t.Fatalf("claimant file refused: %s", k)
	}
}

// A late copy of a retired instance indexed while the index is full makes
// room from other retired instances, never by removing itself mid-apply.
func TestRetiredInstanceDoesNotEvictItself(t *testing.T) {
	x, _ := orphanFinished(t)
	x.f.takeover.entries += maxIndexEntries - x.f.takeover.entries
	if k := x.register(regPayload("late", "w2", hourA, cov(7, "db/cpu", 9, 9))); k != "" {
		t.Fatal(k)
	}
	if in := x.inst(7); in == nil || !in.Untracked {
		t.Fatalf("instance %+v", in)
	}
}
