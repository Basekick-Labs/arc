package raft

// #1207 S4b1c2a2: follow-up claims, tombstones and abandon.

import "testing"

// After a claim finishes (or is released), any other writer may claim what
// its own copy holds; a writer claims an instance at most once.
func TestFollowUpClaims(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	// The first claim of a current instance is the designated primary's.
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{1, 5}}}}}})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	// Unfinished: nobody else.
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{6, 8}}}}}})
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	// Finished: w1 may not claim it again; w3 (not the primary) may.
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{6, 8}}}}}})
	e3 := x.claim("w3", "w2", 7, "db/cpu", 6, 8)
	k := x.f.takeover.claims[7]
	if k.By != "w3" || k.Epoch != e3 || len(k.FinishedBy) != 1 || k.FinishedBy[0] != "w1" || k.Done {
		t.Fatalf("follow-up claim %+v", k)
	}
	// The origin is still claimed (it exits for a restart), and w1's late
	// claimant file of the first epoch is stale.
	if !x.f.CurrentInstanceClaimed("w2") {
		t.Fatal("follow-up claim does not count as a claim")
	}
	c := cov(7, "db/cpu", 2, 2)
	c.ClaimEpoch = e
	if kind := x.register(regPayload("late", "w1", hourA, c)); kind != RefusedStale {
		t.Fatalf("first claimant's late file: %q", kind)
	}
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w3", Epoch: e3})
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{9, 9}}}}}})
	if k := x.f.takeover.claims[7]; len(k.FinishedBy) != 1 || !k.finished("w3") || !k.finished("w1") {
		t.Fatalf("finished %+v", k)
	}

	// Released (not finished): the claimant may claim again, so may others.
	y := takeoverFSM(t)
	y.announce("w2", 7, Continuity{})
	e = y.claim("w1", "w2", 7, "db/cpu", 1, 5)
	y.ok(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 7, Epoch: e})
	y.claim("w3", "w2", 7, "db/cpu", 1, 5)
	if k := y.f.takeover.claims[7]; len(k.FinishedBy) != 0 {
		t.Fatalf("a released claim counted as finished: %+v", k)
	}
}

// An adopted instance may be followed up on only while its adopter is
// claimed (the origin is still taken over).
func TestFollowUpClaimOfAnAdoptedInstance(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{WAL: 7, Journal: 7, Storage: 7}, 7) // 8 adopts 7
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{1, 5}}}}}})
	e8 := x.claim("w1", "w2", 8, "db/cpu", 1, 5)
	x.ok(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 8, Epoch: e8})
	// 8 released: the origin may be back; only the primary may claim 7.
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{1, 5}}}}}})
	e8 = x.claim("w1", "w2", 8, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 8, By: "w1", Epoch: e8})
	// 8 claimed (finished): the origin is taken over; any writer may claim 7.
	x.claim("w3", "w2", 7, "db/cpu", 1, 5)
}

// A follow-up of a live instance needs its origin still taken over: not
// back with a new process, and not the designated primary again.
func TestFollowUpNeedsTheOriginStillTakenOver(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	// w2 restarts with its volume: 9 adopts 7, unclaimed. w2 publishes 7
	// itself; w3 (cut off from w2, say) may not claim it.
	x.announce("w2", 9, Continuity{WAL: 7, Journal: 7, Storage: 7}, 7)
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{6, 7}}}}}})
	// Nor report on it (nothing to follow up).
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0, Claims: []ClaimSet{{Instance: 7}}})
	if len(x.f.FollowUps()) != 0 {
		t.Fatalf("follow-ups %+v", x.f.FollowUps())
	}

	// A released claim of a current instance whose origin is the designated
	// primary again: nobody else may claim it.
	y := takeoverFSM(t)
	y.announce("w2", 7, Continuity{})
	e = y.claim("w1", "w2", 7, "db/cpu", 1, 5)
	y.ok(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 7, Epoch: e})
	y.ok(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w2"})
	y.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{1, 5}}}}}})
}

// Writers answer an ended claim of a live instance (a follow-up claim, or a
// report of holding nothing); FollowUps lists who has.
func TestFollowUpsAndReports(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e, Now: t0})
	fu := x.f.FollowUps()
	if len(fu) != 1 || fu[0].Instance != 7 || len(fu[0].Answered) != 1 || fu[0].Answered[0] != "w1" || !fu[0].EndedAt.Equal(t0) || fu[0].Keys[0] != "db/cpu" {
		t.Fatalf("follow-ups %+v", fu)
	}
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0, Claims: []ClaimSet{{Instance: 7}}})
	if fu = x.f.FollowUps(); len(fu[0].Answered) != 2 || x.inst(7) == nil {
		t.Fatalf("report on a live instance: %+v", fu)
	}
}

// A closed instance with a finished claim waits for every writer: one that
// holds more claims it; once all have answered it goes, and its late copies
// are refused.
func TestRemovedInstanceIsTombstoned(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{WAL: 7, Journal: 7, Storage: 7}) // 7 closed
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	if x.inst(7) == nil {
		t.Fatal("removed before w3 answered")
	}
	e3 := x.claim("w3", "w2", 7, "db/cpu", 6, 9)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w3", Epoch: e3})
	if x.inst(7) != nil {
		t.Fatal("kept after every writer finished a claim")
	}
	// w2 comes back with an old volume holding batches of 7.
	if kind := x.register(regPayload("old", "w2", hourA, cov(7, "db/cpu", 10, 12))); kind != RefusedStale {
		t.Fatalf("late copy of a removed instance: %q", kind)
	}
	// An instance never known is not refused (it is not tracked).
	if kind := x.register(regPayload("other", "w2", hourA, cov(99, "db/cpu", 1, 1))); kind != "" {
		t.Fatalf("unknown instance: %q", kind)
	}
	// Survives a snapshot.
	g := roundTrip(t, x.f)
	if !g.takeover.removedSet[7] || len(g.takeover.removed) != 1 {
		t.Fatalf("tombstones after restore: %v", g.takeover.removed)
	}
}

func TestTombstonesAreBounded(t *testing.T) {
	x := takeoverFSM(t)
	for i := 0; i < maxTakeoverTombstones+3; i++ {
		x.f.takeover.instances[uint64(1000+i)] = &StreamInstance{Instance: uint64(1000 + i), Origin: "w2"}
		x.f.removeInstanceLocked(uint64(1000 + i))
	}
	if len(x.f.takeover.removed) != maxTakeoverTombstones || len(x.f.takeover.removedSet) != maxTakeoverTombstones || x.f.takeover.removedSet[1000] {
		t.Fatalf("%d tombstones, oldest kept %v", len(x.f.takeover.removed), x.f.takeover.removedSet[1000])
	}
}

// An operator abandons a claim its claimant cannot finish: every batch
// still claimed becomes unreplayable, and the claimant counts as finished.
func TestAbandonClaim(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.refused(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e, Abandon: true}) // operator only
	x.refused(CommandClaimDone, ClaimDonePayload{Instance: 7, Epoch: e + 1, Abandon: true})
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, Epoch: e, Abandon: true})
	k := x.f.takeover.claims[7]
	if !k.Done || !k.Abandoned || len(k.Set) != 0 || k.Unreplayable["db/cpu"].Count() != 5 {
		t.Fatalf("abandoned %+v", k)
	}
	x.refused(CommandClaimDone, ClaimDonePayload{Instance: 7, Epoch: e, Abandon: true}) // finished
	// The origin's own copies may register again: the set is empty.
	if kind := x.register(regPayload("o", "w2", hourA, cov(7, "db/cpu", 2, 2))); kind != "" {
		t.Fatalf("origin copy after abandon: %q", kind)
	}
	// w1 does not claim it again; w3 may.
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{3, 3}}}}}})
	x.claim("w3", "w2", 7, "db/cpu", 3, 3)
}

// FinishedBy accumulates across follow-ups: a writer that finished stays
// barred after later claims replace its own.
func TestFinishedByCarries(t *testing.T) {
	x := takeoverFSM(t)
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w4", Role: "writer", State: "healthy"}})
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	e = x.claim("w3", "w2", 7, "db/cpu", 6, 7)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w3", Epoch: e})
	x.claim("w4", "w2", 7, "db/cpu", 8, 9)
	if k := x.f.takeover.claims[7]; len(k.FinishedBy) != 2 || !k.finished("w1") || !k.finished("w3") {
		t.Fatalf("finished %+v", k.FinishedBy)
	}
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{10, 10}}}}}})
}

// A follow-up replaces a claim: at the claims cap it is still accepted.
func TestFollowUpAtTheClaimsCap(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	for i := len(x.f.takeover.claims); i < maxTakeoverClaims; i++ {
		x.f.takeover.claims[uint64(9000+i)] = &StreamClaim{Instance: uint64(9000 + i)}
	}
	x.claim("w3", "w2", 7, "db/cpu", 6, 7)
}

// FinishedBy, Abandoned and EndedAt survive a snapshot.
func TestFollowUpStateSurvivesSnapshot(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, Epoch: e, Abandon: true, Now: t0})
	x.claim("w3", "w2", 7, "db/cpu", 6, 6)
	g := roundTrip(t, x.f)
	k := g.takeover.claims[7]
	if k.By != "w3" || len(k.FinishedBy) != 1 || k.FinishedBy[0] != "w1" {
		t.Fatalf("restored %+v", k)
	}
	y := takeoverFSM(t)
	y.announce("w2", 7, Continuity{})
	e = y.claim("w1", "w2", 7, "db/cpu", 1, 5)
	y.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, Epoch: e, Abandon: true, Now: t0})
	if k := roundTrip(t, y.f).takeover.claims[7]; !k.Abandoned || !k.EndedAt.Equal(t0) {
		t.Fatalf("restored %+v", k)
	}
}
