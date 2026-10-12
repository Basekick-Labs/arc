package raft

// #1207 S4b1c: the FSM side of the claimant (extend, self-release, the
// untracked rule, CoveredRows).

import (
	"testing"
	"time"
)

func epochCov(c InstanceCoverage, e uint64) InstanceCoverage { c.ClaimEpoch = e; return c }

func TestExtendStreamClaim(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w2", Instance: 7, Keys: map[string]KeySettlement{"db/mem": {Floor: 4}}})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 10)
	ext := func(by string, epoch uint64, keys map[string]Intervals) ExtendStreamClaimPayload {
		return ExtendStreamClaimPayload{Instance: 7, By: by, Epoch: epoch, Keys: keys}
	}
	x.refused(CommandExtendStreamClaim, ext("w3", e, map[string]Intervals{"db/mem": {{1, 9}}}))
	x.refused(CommandExtendStreamClaim, ext("w1", e+1, map[string]Intervals{"db/mem": {{1, 9}}}))
	x.ok(CommandExtendStreamClaim, ext("w1", e, map[string]Intervals{"db/mem": {{1, 9}}, "db/cpu": {{20, 30}}}))
	c := x.f.ClaimsBy("w1")
	if len(c) != 1 || c[0].ExtendedAt != x.idx {
		t.Fatalf("claims %+v", c)
	}
	// The floor is subtracted as on the claim; the existing key is unioned.
	if got := c[0].Set["db/mem"]; len(got) != 1 || got[0] != [2]uint64{5, 9} {
		t.Fatalf("db/mem %v", got)
	}
	if got := c[0].Set["db/cpu"]; len(got) != 2 {
		t.Fatalf("db/cpu %v", got)
	}
	// The origin's copy of an added batch is refused from here on; the
	// claimant's is published.
	if k := x.register(regPayload("o", "w2", hourA, cov(7, "db/mem", 6, 6))); k != RefusedClaimed {
		t.Fatalf("origin after extend: %q", k)
	}
	if k := x.register(regPayload("r", "w1", hourA, epochCov(cov(7, "db/mem", 6, 6), e))); k != "" {
		t.Fatalf("claimant after extend: %q", k)
	}
	// A finished claim is not extended.
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	x.refused(CommandExtendStreamClaim, ext("w1", e, map[string]Intervals{"db/disk": {{1, 1}}}))
}

func TestExtendStreamClaimBounds(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 1)
	many := Intervals{}
	for i := uint64(0); i < maxClaimIntervalsPerKey; i++ {
		many = append(many, [2]uint64{10 + 2*i, 10 + 2*i})
	}
	// Each half is in bounds, the union is not.
	x.ok(CommandExtendStreamClaim, ExtendStreamClaimPayload{Instance: 7, By: "w1", Epoch: e, Keys: map[string]Intervals{"db/cpu": many[:maxClaimIntervalsPerKey/2]}})
	x.refused(CommandExtendStreamClaim, ExtendStreamClaimPayload{Instance: 7, By: "w1", Epoch: e, Keys: map[string]Intervals{"db/cpu": many[maxClaimIntervalsPerKey/2:]}})
	x.refused(CommandExtendStreamClaim, ExtendStreamClaimPayload{Instance: 7, By: "w1", Epoch: e, Keys: map[string]Intervals{"": {{1, 1}}}})
}

// The claimant releases its own claim; another writer cannot.
func TestClaimantReleasesItsOwnClaim(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 10)
	x.refused(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 7, Epoch: e, By: "w3"})
	x.ok(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 7, Epoch: e, By: "w1"})
	if c := x.f.ClaimsBy("w1"); len(c) != 1 || !c[0].Released {
		t.Fatalf("claims %+v", c)
	}
}

// A claimant file the index has no room for is refused, and the instance
// stays tracked: the origin's copies are still arbitrated. A file of the
// origin itself is not refused for room (it untracks, as before).
func TestClaimantFileOverTheIndexBoundIsRefused(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 1<<20)
	if k := x.register(regPayload("a", "w1", hourA, epochCov(cov(7, "db/cpu", 1, 1), e))); k != "" {
		t.Fatalf("tracked: %q", k)
	}
	big := InstanceCoverage{Instance: 7, Key: "db/cpu", ClaimEpoch: e}
	for i := uint64(0); i <= maxKeyIntervals; i++ {
		big.Ranges = append(big.Ranges, [2]uint64{2*i + 3, 2*i + 3})
	}
	if k := x.register(regPayload("big", "w1", hourA, big)); k != RefusedFull {
		t.Fatalf("the file that would overflow the index: %q", k)
	}
	if x.inst(7).Untracked {
		t.Fatal("untracked by a refused file")
	}
	// Partial pieces, split across two ops of one batch: each fits alone,
	// together they do not.
	half := func(path string, from int) BatchFileOp {
		c := InstanceCoverage{Instance: 7, Key: "db/cpu", ClaimEpoch: e}
		for i := 0; i < maxKeyPartial/2+1; i++ {
			c.Partial = append(c.Partial, CoveragePiece{CSeq: uint64(100000 + from + i), RowLo: 0, RowHi: 1, RowTotal: 2})
		}
		return regOp(t, regPayload(path, "w1", hourB, c))
	}
	if ops := x.batch(half("p1", 0), half("p2", maxKeyPartial)); len(ops) != 1 || ops[0].Op != 1 || ops[0].Kind != RefusedFull {
		t.Fatalf("batch refusals %+v", ops)
	}
	// The claimant's tracked files still publish.
	if k := x.register(regPayload("b", "w1", hourB, epochCov(cov(7, "db/cpu", 2, 2), e))); k != "" {
		t.Fatalf("after the refusals: %q", k)
	}
}

// A claimant file of an instance the index stopped tracking (its origin
// overflowed it) is refused: nothing could arbitrate it.
func TestClaimantFileOfAnUntrackedInstanceIsStale(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 1<<20)
	big := InstanceCoverage{Instance: 7, Key: "db/mem"}
	for i := uint64(0); i <= maxKeyIntervals; i++ {
		big.Ranges = append(big.Ranges, [2]uint64{2*i + 3, 2*i + 3})
	}
	// The origin's file of a key outside the claim overflows the index.
	if k := x.register(regPayload("big", "w2", hourA, big)); k != "" {
		t.Fatalf("origin file: %q", k)
	}
	if !x.inst(7).Untracked {
		t.Fatal("not untracked")
	}
	if k := x.register(regPayload("b", "w1", hourB, epochCov(cov(7, "db/cpu", 2, 2), e))); k != RefusedStale {
		t.Fatalf("untracked claimant file: %q", k)
	}
	if st, _, _, _ := x.f.CoveredRows(7, "db/cpu", 2, 0, 1, mustHour(t, hourB)); st != CoveredUntracked {
		t.Fatalf("CoveredRows %v", st)
	}
}

func mustHour(t *testing.T, s string) time.Time {
	t.Helper()
	h, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestCoveredRows(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	hA, hB := mustHour(t, hourA), mustHour(t, hourB)
	rows := func(path, hour string, cseq uint64, lo, hi int) RegisterFilePayload {
		return regPayload(path, "w1", hour, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: cseq, RowLo: lo, RowHi: hi, RowTotal: 100}}})
	}
	check := func(name string, cseq uint64, chunk, chunks uint16, h time.Time, want CoveredState, wantRanges [][2]int, wantErr bool) {
		t.Helper()
		st, r, _, err := x.f.CoveredRows(7, "db/cpu", cseq, chunk, chunks, h)
		if st != want || (err != nil) != wantErr || len(r) != len(wantRanges) {
			t.Fatalf("%s: state %v ranges %v err %v", name, st, r, err)
		}
		for i := range r {
			if r[i] != wantRanges[i] {
				t.Fatalf("%s: ranges %v", name, r)
			}
		}
	}
	if st, _, _, _ := x.f.CoveredRows(99, "db/cpu", 1, 0, 1, hA); st != CoveredUntracked {
		t.Fatal("unknown instance")
	}
	check("nothing registered", 3, 0, 1, hA, CoveredRanges, nil, false)
	for _, p := range []RegisterFilePayload{rows("a", hourA, 3, 50, 100), rows("b", hourA, 3, 0, 20), rows("c", hourB, 3, 0, 50)} {
		if k := x.register(p); k != "" {
			t.Fatal(k)
		}
	}
	// Per hour, sorted, in the record's row coordinates (Chunks 1 = unsplit).
	check("hour A", 3, 0, 1, hA, CoveredRanges, [][2]int{{0, 20}, {50, 100}}, false)
	check("hour B", 3, 0, 1, hB, CoveredRanges, [][2]int{{0, 50}}, false)
	check("chunk coordinates", 3, 1, 2, hA, CoveredRanges, nil, true)
	if k := x.register(regPayload("w", "w1", hourA, cov(7, "db/cpu", 4, 4))); k != "" {
		t.Fatal(k)
	}
	check("whole batch", 4, 0, 1, hA, CoveredAll, nil, false)
	check("whole batch, other hour", 4, 0, 1, hB, CoveredRanges, nil, false)
	chunk := regPayload("k", "w1", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 5, Chunk: 1, Chunks: 3}}})
	if k := x.register(chunk); k != "" {
		t.Fatal(k)
	}
	check("whole chunk", 5, 1, 3, hA, CoveredAll, nil, false)
	check("another chunk", 5, 0, 3, hA, CoveredRanges, nil, false)
	if k := x.register(rows("t", hourB, 3, 60, 70)); k != "" {
		t.Fatal(k)
	}
	if k := x.register(regPayload("t2", "w1", hourB, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 3, RowLo: 80, RowHi: 90, RowTotal: 90}}})); k != "" {
		t.Fatal(k)
	}
	check("differing totals", 3, 0, 1, hB, CoveredRanges, nil, true)
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 3}}})
	check("below the floor", 3, 0, 1, hB, CoveredAll, nil, false)
}

// The room check counts the index as it would grow: a claimant file that
// fills the holes between the origin's intervals merges them and fits, one
// that adds a separate interval to a full key does not.
func TestClaimantRoomIsCountedAfterMerging(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	odd := InstanceCoverage{Instance: 7, Key: "db/cpu"}
	for i := uint64(0); i < maxKeyIntervals; i++ {
		odd.Ranges = append(odd.Ranges, [2]uint64{2*i + 1, 2*i + 1})
	}
	if k := x.register(regPayload("origin", "w2", hourA, odd)); k != "" || x.inst(7).Untracked {
		t.Fatalf("origin file: %q", k)
	}
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 1<<20)
	extra := epochCov(cov(7, "db/cpu", 1<<19, 1<<19), e)
	if k := x.register(regPayload("one-more", "w1", hourA, extra)); k != RefusedFull {
		t.Fatalf("a separate interval on a full key: %q", k)
	}
	holes := InstanceCoverage{Instance: 7, Key: "db/cpu", ClaimEpoch: e}
	for i := uint64(1); i < maxKeyIntervals; i++ {
		holes.Ranges = append(holes.Ranges, [2]uint64{2 * i, 2 * i})
	}
	if k := x.register(regPayload("holes", "w1", hourA, holes)); k != "" || x.inst(7).Untracked {
		t.Fatalf("the holes: %q, untracked %v", k, x.inst(7).Untracked)
	}
	if k := x.register(regPayload("now-fits", "w1", hourA, extra)); k != "" {
		t.Fatalf("after merging: %q", k)
	}
}
