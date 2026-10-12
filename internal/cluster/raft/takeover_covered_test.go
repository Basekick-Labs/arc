package raft

import "testing"

// CoverageCovered needs containment: overlap is enough to refuse a
// duplicate, never to delete or release the only other copy.
func TestCoverageCoveredNeedsEveryRow(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	part := func(path string, c uint64, lo, hi int) {
		x.register(regPayload(path, "w1", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: c, RowLo: lo, RowHi: hi, RowTotal: 10}}}))
	}
	chunk := func(path string, c uint64, k uint16) {
		x.register(regPayload(path, "w1", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: c, Chunk: k, Chunks: 2}}}))
	}
	covered := func(p CoveragePiece) bool { return x.f.CoverageCovered(7, "db/cpu", p, mustTime(hourA)) }
	part("a", 3, 0, 5)
	if covered(CoveragePiece{CSeq: 3}) || !x.f.CoverageSettled(7, "db/cpu", CoveragePiece{CSeq: 3}, mustTime(hourA)) {
		t.Fatal("half the rows: settled (overlap) but must not be covered")
	}
	if !covered(CoveragePiece{CSeq: 3, RowLo: 1, RowHi: 4, RowTotal: 10}) {
		t.Fatal("a range inside the registered rows is covered")
	}
	part("b", 3, 5, 10)
	if !covered(CoveragePiece{CSeq: 3}) {
		t.Fatal("every row registered: covered")
	}
	chunk("c", 9, 0)
	if covered(CoveragePiece{CSeq: 9}) || !covered(CoveragePiece{CSeq: 9, Chunk: 0, Chunks: 2}) {
		t.Fatal("one chunk of two")
	}
	chunk("d", 9, 1)
	if !covered(CoveragePiece{CSeq: 9}) {
		t.Fatal("both chunks registered: covered")
	}
	if covered(CoveragePiece{CSeq: 9, RowLo: 0, RowHi: 1, RowTotal: 5}) {
		t.Fatal("rows in batch coordinates are not covered by chunk pieces")
	}
	x.register(regPayload("e", "w1", hourA, cov(7, "db/cpu", 20, 21)))
	if !covered(CoveragePiece{CSeq: 21}) || covered(CoveragePiece{CSeq: 22}) || x.f.CoverageCovered(7, "db/cpu", CoveragePiece{CSeq: 21}, mustTime(hourB)) {
		t.Fatal("whole batches")
	}
	if got := x.f.CoveringFiles("db", "cpu", mustTime(hourA), 7, "db/cpu", CoveragePiece{CSeq: 3}); len(got) != 2 {
		t.Fatalf("covering files %v", got)
	}
}

func TestUncoveredBatchesAndCoveringFilesOf(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 5, 8)))
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 3, Lost: Intervals{{2, 2}}}}})
	got := x.f.UncoveredBatches(7, "db/cpu", 1, 10, mustTime(hourA))
	if len(got) != 3 || got[0] != [2]uint64{2, 2} || got[1] != [2]uint64{4, 4} || got[2] != [2]uint64{9, 10} {
		t.Fatalf("uncovered %v", got)
	}
	if got := x.f.UncoveredBatches(9, "db/cpu", 1, 2, mustTime(hourA)); len(got) != 1 {
		t.Fatalf("unknown instance: %v", got)
	}
	if files := x.f.CoveringFilesOf("db", "cpu", mustTime(hourA), 7, "db/cpu", Intervals{{8, 20}}); len(files) != 1 || files[0].Path != "a" {
		t.Fatalf("covering %v", files)
	}
	if files := x.f.CoveringFilesOf("db", "cpu", mustTime(hourA), 7, "db/cpu", Intervals{{9, 20}}); len(files) != 0 {
		t.Fatalf("covering %v", files)
	}
}
