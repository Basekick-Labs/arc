package raft

// #1207 S4b1c2: node lifecycle and vanished files under stream takeover.

import (
	"testing"
	"time"
)

func (x *tkFSM) claimEpoch(by string, inst uint64) uint64 {
	x.t.Helper()
	for _, c := range x.f.ClaimsBy(by) {
		if c.Instance == inst {
			return c.Epoch
		}
	}
	x.t.Fatalf("no claim of %x by %s", inst, by)
	return 0
}

// A graceful leave keeps the writer's instances and claims: it is usually a
// restart. The designation still goes (failover proceeds).
func TestLeaveKeepsTakeoverState(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.announce("w3", 9, Continuity{})
	x.announce("w3", 10, Continuity{}) // 9 orphaned: any writer may claim it
	x.claim("w2", "w3", 9, "db/cpu", 1, 5)
	at := t0.Add(time.Minute)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w1", Reason: RemoveReasonLeave, At: at})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2", Reason: RemoveReasonLeave, At: at})
	if in := x.inst(7); in == nil || in.Status != InstanceCurrent || in.OriginGone {
		t.Fatalf("left origin's instance: %+v", in)
	}
	if c := x.f.ClaimsBy("w2"); len(c) != 1 || c[0].Released {
		t.Fatalf("left claimant's claim: %+v", c)
	}
	if x.f.GetPrimaryWriterID() != "" || x.f.PrimaryWriterIDFast() != "" {
		t.Fatal("designation kept")
	}
	if d := x.f.DepartedWriters(); len(d) != 2 || !d["w1"].Equal(at) {
		t.Fatalf("departed %v", d)
	}
	// It survives a snapshot.
	g := roundTrip(t, x.f)
	if d := g.DepartedWriters(); len(d) != 2 {
		t.Fatalf("departed after restore %v", d)
	}
	// Back: the departure is forgotten, and it adopts its instance.
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w1", Role: "writer", State: "healthy"}})
	if _, ok := x.f.DepartedWriters()["w1"]; ok {
		t.Fatal("re-joined writer still departed")
	}
	x.announce("w1", 8, Continuity{}, 7)
	if in := x.inst(7); in.Status != InstanceAdopted || in.AdoptedBy != 8 {
		t.Fatalf("after the restart: %+v", in)
	}
}

// An operator removal (no reason) orphans and releases, as before; an expiry
// does the same, but only for the departure the leader saw.
func TestOperatorRemovalAndExpiry(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	x.announce("w3", 10, Continuity{}) // 9 orphaned: any writer may claim it
	x.claim("w2", "w3", 9, "db/cpu", 1, 5)
	at := t0.Add(time.Minute)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonLeave, At: at})
	// An expiry for another departure time is a no-op.
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonDepartedExpired, At: at.Add(time.Second)})
	if in := x.inst(10); in.Status != InstanceCurrent {
		t.Fatalf("expired a departure that was not seen: %+v", in)
	}
	// Back and gone again: the earlier expiry no longer applies.
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w3", Role: "writer", State: "healthy"}})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonDepartedExpired, At: at})
	if _, member := x.f.GetNode("w3"); !member {
		t.Fatal("an expiry removed a re-joined writer")
	}
	at2 := at.Add(time.Hour)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonLeave, At: at2})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonDepartedExpired, At: at2})
	if in := x.inst(10); in == nil || in.Status != InstanceOrphaned || !in.OriginGone {
		t.Fatalf("expired origin's instance: %+v", in)
	}
	if _, ok := x.f.DepartedWriters()["w3"]; ok {
		t.Fatal("still departed after the expiry")
	}
	// An operator removal of the claimant releases its claim.
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2"})
	if c := x.f.ClaimsBy("w2"); len(c) != 1 || !c[0].Released {
		t.Fatalf("operator-removed claimant's claim: %+v", c)
	}
}

// A departed writer may hold the only copy: an instance is not removed on
// the others' empty reports until it reports too (or its departure expires).
func TestDepartedWriterMustReport(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	x.announce("w3", 10, Continuity{}) // 9 orphaned (no continuity, not held)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2", Reason: RemoveReasonLeave, At: t0})
	report := func(by string) {
		x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w3", By: by, Now: t0, Claims: []ClaimSet{{Instance: 9}}})
	}
	report("w1")
	if x.inst(9) == nil {
		t.Fatal("removed while the departed w2 had not reported")
	}
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w2", Role: "writer", State: "healthy"}})
	report("w2")
	if x.inst(9) != nil {
		t.Fatal("not removed once every writer reported")
	}
}

// Only an instance listed with no keys reports holding nothing; keys the
// floor trims to nothing claim nothing and report nothing.
func TestTrimmedSetIsNotAReport(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 10}}})
	x.announce("w3", 10, Continuity{}) // 9 orphaned
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w3", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 9, Keys: map[string]Intervals{"db/cpu": {{1, 10}}}}}})
	if in := x.inst(9); in == nil || len(in.ReportedBy) != 0 || len(x.f.ClaimsBy("w1")) != 0 {
		t.Fatalf("trimmed set: %+v claims %v", in, x.f.ClaimsBy("w1"))
	}
}

// An instance whose claim finished and that its origin then closes (restart,
// nothing held) is removed once every other writer finished a claim of it
// or reported holding nothing, as on ClaimDone.
func TestClosedAfterDoneIsRemoved(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	e := x.claim("w1", "w3", 9, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 9, By: "w1", Epoch: e})
	if x.inst(9) == nil {
		t.Fatal("current instance removed on ClaimDone")
	}
	if !x.f.CurrentInstanceClaimed("w3") {
		t.Fatal("a finished claim on the current instance is still a claim")
	}
	x.announce("w3", 10, Continuity{WAL: 9, Journal: 9, Storage: 9})
	if x.inst(9) == nil {
		t.Fatal("closed instance removed while w2 may hold batches the claimant lacked")
	}
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w3", By: "w2", Now: t0, Claims: []ClaimSet{{Instance: 9}}})
	if x.inst(9) != nil {
		t.Fatal("closed instance with a finished claim kept after every writer answered")
	}
	if x.f.CurrentInstanceClaimed("w3") {
		t.Fatal("the new instance is not claimed")
	}
}

// A file that vanished below the floor gives back its rows in its hour only:
// claimable, republishable once, other hours and other rows still settled.
func TestVanishedFileBelowTheFloor(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	hA, hB := mustHour(t, hourA), mustHour(t, hourB)
	// Batch 3 has rows in hour A (file a) and hour B (file b); batch 4 rows
	// [0,5) of 10 in hour A (file c), [5,10) in hour A (file d).
	for _, r := range []RegisterFilePayload{
		regPayload("a", "w3", hourA, cov(9, "db/cpu", 3, 3)),
		regPayload("b", "w3", hourB, cov(9, "db/cpu", 3, 3)),
		regPayload("c", "w3", hourA, InstanceCoverage{Instance: 9, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 4, RowLo: 0, RowHi: 5, RowTotal: 10}}}),
		regPayload("d", "w3", hourA, InstanceCoverage{Instance: 9, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 4, RowLo: 5, RowHi: 10, RowTotal: 10}}}),
	} {
		if k := x.register(r); k != "" {
			t.Fatal(k)
		}
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 10}}})
	if got := x.f.ClaimableSet(9, "db/cpu", Intervals{{1, 12}}); len(got) != 1 || got[0] != [2]uint64{11, 12} {
		t.Fatalf("claimable before %v", got)
	}
	e := x.claim("w1", "w3", 9, "db/cpu", 11, 12)
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "a", Reason: DeleteReasonVanished})
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "c", Reason: DeleteReasonVanished})
	if got := x.f.ClaimableSet(9, "db/cpu", Intervals{{1, 12}}); len(got) != 2 || got[0] != [2]uint64{3, 4} {
		t.Fatalf("claimable after %v", got)
	}
	// The unfinished claim grew to take them.
	if c := x.f.ClaimsBy("w1")[0]; !c.Set["db/cpu"].Contains(3) || !c.Set["db/cpu"].Contains(4) {
		t.Fatalf("claim set %v", c.Set)
	}
	if st, _, _, _ := x.f.CoveredRows(9, "db/cpu", 3, 0, 1, hB); st != CoveredAll {
		t.Fatal("the other hour of batch 3 is no longer covered")
	}
	if st, r, _, _ := x.f.CoveredRows(9, "db/cpu", 3, 0, 1, hA); st != CoveredRanges || len(r) != 0 {
		t.Fatalf("vanished hour of batch 3: %v %v", st, r)
	}
	if st, r, tot, _ := x.f.CoveredRows(9, "db/cpu", 4, 0, 1, hA); st != CoveredRanges || len(r) != 1 || r[0] != [2]int{5, 10} || tot != 10 {
		t.Fatalf("batch 4 in hour A: %v %v %d", st, r, tot)
	}
	if x.f.CoverageCovered(9, "db/cpu", CoveragePiece{CSeq: 4}, hA) || !x.f.CoverageCovered(9, "db/cpu", CoveragePiece{CSeq: 4, RowLo: 5, RowHi: 10, RowTotal: 10}, hA) {
		t.Fatal("partly vanished batch 4: covered as a whole, or its rows in file d not covered")
	}
	if got := x.f.UncoveredBatches(9, "db/cpu", 1, 10, hA); len(got) != 1 || got[0] != [2]uint64{3, 4} {
		t.Fatalf("uncovered %v", got)
	}
	// The origin's copy is refused; the claimant republishes each once.
	if k := x.register(regPayload("o", "w3", hourA, cov(9, "db/cpu", 3, 3))); k != RefusedClaimed {
		t.Fatalf("origin copy: %q", k)
	}
	rows := func(path string, lo, hi int) RegisterFilePayload {
		return regPayload(path, "w1", hourA, InstanceCoverage{Instance: 9, Key: "db/cpu", ClaimEpoch: e, Partial: []CoveragePiece{{CSeq: 4, RowLo: lo, RowHi: hi, RowTotal: 10}}})
	}
	if k := x.register(rows("r4-dup", 5, 10)); k != RefusedDuplicate {
		t.Fatalf("rows still in file d: %q", k)
	}
	if k := x.register(regPayload("r3", "w1", hourA, epochCov(cov(9, "db/cpu", 3, 3), e))); k != "" {
		t.Fatalf("republish 3: %q", k)
	}
	if k := x.register(rows("r4", 0, 5)); k != "" {
		t.Fatalf("republish 4: %q", k)
	}
	if k := x.register(regPayload("r3-again", "w1", hourA, epochCov(cov(9, "db/cpu", 3, 3), e))); k != RefusedDuplicate {
		t.Fatalf("second copy of 3: %q", k)
	}
	if st, _, _, _ := x.f.CoveredRows(9, "db/cpu", 3, 0, 1, hA); st != CoveredAll {
		t.Fatal("republished batch 3 not covered")
	}
	if !x.f.CoverageCovered(9, "db/cpu", CoveragePiece{CSeq: 4}, hA) {
		t.Fatal("republished batch 4 not covered")
	}
	// Nothing vanished is left; the claim no longer needs to take them.
	if got := x.f.ClaimableSet(9, "db/cpu", Intervals{{1, 10}}); len(got) != 0 {
		t.Fatalf("claimable after republishing %v", got)
	}
	// It all survives a snapshot.
	if g := roundTrip(t, x.f); len(g.ClaimableSet(9, "db/cpu", Intervals{{1, 10}})) != 0 {
		t.Fatal("restored state differs")
	}
}

// A vanished piece survives a later floor collapse, and its hour's index
// keeps the republished pieces.
func TestVanishedSurvivesSettle(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	if k := x.register(regPayload("a", "w3", hourA, cov(9, "db/cpu", 3, 3))); k != "" {
		t.Fatal(k)
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 5}}})
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "a", Reason: DeleteReasonVanished})
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 8}}})
	if got := x.f.ClaimableSet(9, "db/cpu", Intervals{{1, 8}}); len(got) != 1 || got[0] != [2]uint64{3, 3} {
		t.Fatalf("claimable %v", got)
	}
}

// Every assignment of the designation updates its lock-free mirror,
// including a restore.
func TestPrimaryMirror(t *testing.T) {
	x := takeoverFSM(t)
	if x.f.PrimaryWriterIDFast() != "w1" {
		t.Fatal("promote")
	}
	x.ok(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w2", OldPrimaryID: "w1"})
	if x.f.PrimaryWriterIDFast() != "w2" {
		t.Fatal("failover")
	}
	if g := roundTrip(t, x.f); g.PrimaryWriterIDFast() != "w2" {
		t.Fatal("restore")
	}
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2"})
	if x.f.PrimaryWriterIDFast() != "" {
		t.Fatal("remove")
	}
}

// A node record rewritten (UpdateNode) is back too.
func TestUpdateNodeEndsTheDeparture(t *testing.T) {
	x := takeoverFSM(t)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2", Reason: RemoveReasonLeave, At: t0})
	x.ok(CommandUpdateNode, UpdateNodePayload{Node: NodeInfo{ID: "w2", Role: "writer", State: "healthy"}})
	if len(x.f.DepartedWriters()) != 0 {
		t.Fatal("still departed after its record was rewritten")
	}
}

// The origin need not report on its own instance, departed or not.
func TestDepartedOriginNeedNotReport(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	x.announce("w3", 10, Continuity{}) // 9 orphaned
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonLeave, At: t0})
	for _, by := range []string{"w1", "w2"} {
		x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w3", By: by, Now: t0, Claims: []ClaimSet{{Instance: 9}}})
	}
	if x.inst(9) != nil {
		t.Fatal("waited for the departed origin itself to report")
	}
}

// Past the bound a leave is applied as an operator removal.
func TestDepartedBound(t *testing.T) {
	x := takeoverFSM(t)
	for i := 0; i <= maxDepartedWriters; i++ {
		id := "x" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: id, Role: "writer", State: "healthy"}})
		x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: id, Reason: RemoveReasonLeave, At: t0.Add(time.Duration(i) * time.Second)})
	}
	d := x.f.DepartedWriters()
	if len(d) != maxDepartedWriters {
		t.Fatalf("departed %d, want the bound %d", len(d), maxDepartedWriters)
	}
	if _, ok := d["xaa"]; ok {
		t.Fatal("the oldest departure was kept")
	}
	if _, ok := d["x"+string(rune('a'+maxDepartedWriters%26))+string(rune('a'+maxDepartedWriters/26))]; !ok {
		t.Fatal("the newest departure was dropped")
	}
}

// Departures alone survive a snapshot.
func TestOnlyDeparturesSurviveASnapshot(t *testing.T) {
	x := takeoverFSM(t)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2", Reason: RemoveReasonLeave, At: t0})
	if g := roundTrip(t, x.f); len(g.DepartedWriters()) != 1 {
		t.Fatal("departure lost in a snapshot")
	}
}

// A partly republished vanished batch keeps its republished rows indexed
// through a floor collapse, so a second copy is still refused.
func TestPartlyRepublishedSurvivesSettle(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	if k := x.register(regPayload("a", "w3", hourA, cov(9, "db/cpu", 4, 4))); k != "" {
		t.Fatal(k)
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 5}}})
	e := x.claim("w1", "w3", 9, "db/cpu", 6, 9)
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "a", Reason: DeleteReasonVanished})
	half := func(path string) RegisterFilePayload {
		return regPayload(path, "w1", hourA, InstanceCoverage{Instance: 9, Key: "db/cpu", ClaimEpoch: e, Partial: []CoveragePiece{{CSeq: 4, RowLo: 0, RowHi: 5, RowTotal: 10}}})
	}
	if x.f.CoverageCovered(9, "db/cpu", CoveragePiece{CSeq: 4}, mustHour(t, hourA)) {
		t.Fatal("vanished batch reported covered")
	}
	if k := x.register(half("h1")); k != "" {
		t.Fatalf("first half: %q", k)
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 8}}})
	if k := x.register(half("h2")); k != RefusedDuplicate {
		t.Fatalf("second copy of the half after a settle: %q", k)
	}
}

// Republished vanished batches count toward the room check like any batch.
func TestRoomCheckCountsVanishedBatches(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	small := InstanceCoverage{Instance: 9, Key: "db/cpu"}
	for i := uint64(0); i < 10; i++ {
		small.Ranges = append(small.Ranges, [2]uint64{2*i + 1, 2*i + 1})
	}
	big := InstanceCoverage{Instance: 9, Key: "db/cpu"}
	for i := uint64(0); i < maxKeyIntervals-6; i++ {
		big.Ranges = append(big.Ranges, [2]uint64{1001 + 2*i, 1001 + 2*i})
	}
	if x.register(regPayload("v", "w3", hourA, small)) != "" {
		t.Fatal("setup")
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 1000}}})
	if x.register(regPayload("big", "w3", hourA, big)) != "" || x.inst(9).Untracked {
		t.Fatal("setup")
	}
	e := x.claim("w1", "w3", 9, "db/cpu", 1<<20, 1<<20+5) // the vanished batches join it
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "v", Reason: DeleteReasonVanished})
	small.ClaimEpoch = e
	if k := x.register(regPayload("r", "w1", hourA, small)); k != RefusedFull || x.inst(9).Untracked {
		t.Fatalf("over the room: %q untracked %v", k, x.inst(9).Untracked)
	}
}

// Too many vanished pieces on one key untrack the instance.
func TestVanishedBound(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	c := InstanceCoverage{Instance: 9, Key: "db/cpu"}
	for i := uint64(0); i <= maxKeyLost; i++ {
		c.Ranges = append(c.Ranges, [2]uint64{2*i + 1, 2*i + 1})
	}
	if x.register(regPayload("v", "w3", hourA, c)) != "" {
		t.Fatal("setup")
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 1 << 20}}})
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "v", Reason: DeleteReasonVanished})
	if !x.inst(9).Untracked {
		t.Fatal("vanished pieces over their bound kept the instance tracked")
	}
}

// A whole vanished batch republished in row pieces is settled again: no
// longer claimable, no longer uncovered.
func TestVanishedWholeRepublishedInPieces(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	hA := mustHour(t, hourA)
	if x.register(regPayload("a", "w3", hourA, cov(9, "db/cpu", 4, 4))) != "" {
		t.Fatal("setup")
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w3", Instance: 9, Keys: map[string]KeySettlement{"db/cpu": {Floor: 5}}})
	e := x.claim("w1", "w3", 9, "db/cpu", 6, 9)
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "a", Reason: DeleteReasonVanished})
	for i, r := range [][2]int{{0, 5}, {5, 10}} {
		p := regPayload("p"+string(rune('0'+i)), "w1", hourA, InstanceCoverage{Instance: 9, Key: "db/cpu", ClaimEpoch: e,
			Partial: []CoveragePiece{{CSeq: 4, RowLo: r[0], RowHi: r[1], RowTotal: 10}}})
		if k := x.register(p); k != "" {
			t.Fatalf("piece %d: %q", i, k)
		}
	}
	if got := x.f.UncoveredBatches(9, "db/cpu", 4, 4, hA); len(got) != 0 {
		t.Fatalf("uncovered %v", got)
	}
	if got := x.f.ClaimableSet(9, "db/cpu", Intervals{{4, 4}}); len(got) != 0 {
		t.Fatalf("claimable %v", got)
	}
}

// Back in another role, a departed writer gives up its takeover state.
func TestRejoinAsAnotherRoleGivesUpInstances(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonLeave, At: t0})
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w3", Role: "reader", State: "healthy"}})
	if in := x.inst(9); in == nil || in.Status != InstanceOrphaned || !in.OriginGone {
		t.Fatalf("instance of a writer back as a reader: %+v", in)
	}
}

// A second leave for a departed writer changes nothing.
func TestRepeatedLeaveIsANoOp(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w3", 9, Continuity{})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonLeave, At: t0})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3", Reason: RemoveReasonLeave, At: t0.Add(time.Minute)})
	if in := x.inst(9); in.Status != InstanceCurrent {
		t.Fatalf("instance after a repeated leave: %+v", in)
	}
	if at := x.f.DepartedWriters()["w3"]; !at.Equal(t0) {
		t.Fatalf("departure %v", at)
	}
}
