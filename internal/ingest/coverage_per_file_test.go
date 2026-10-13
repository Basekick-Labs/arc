package ingest

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #1207 S4b: a flush spanning several partition hours writes one file per
// hour, and each file's coverage lists only the batches with rows in it, so
// the cluster manifest knows exactly which batches each file holds.
func TestCoverageIsExactPerHourFile(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	h1 := time.Date(2026, 10, 12, 10, 30, 0, 0, time.UTC).UnixMicro()
	h2 := time.Date(2026, 10, 12, 11, 30, 0, 0, time.UTC).UnixMicro()
	h3 := time.Date(2026, 10, 12, 12, 30, 0, 0, time.UTC).UnixMicro()
	for _, times := range [][]interface{}{{h1, h1}, {h1, h2}, {h2}, {h3}} {
		vals := make([]interface{}, len(times))
		for i := range vals {
			vals[i] = 1.0
		}
		if err := b.WriteColumnarRecord(context.Background(), "db", coverageRecordFrom("cpu", times, vals)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := reg.snapshot()
	if len(files) != 3 {
		t.Fatalf("%d files, want one per hour", len(files))
	}
	got := map[int][]uint64{} // hour -> cseqs
	for _, f := range files {
		ranges, partial, exact := f.cov.FilePieces()
		if !exact || len(partial) != 0 {
			t.Fatalf("%s: exact %v partial %v", f.path, exact, partial)
		}
		hour := hourOfPath(t, f.path)
		for _, r := range ranges {
			for c := r.Lo; c <= r.Hi; c++ {
				got[hour] = append(got[hour], c)
			}
		}
		if f.cov.Files != 3 || len(f.cov.Ranges) == 0 {
			t.Fatalf("%s: group view lost: %+v", f.path, f.cov)
		}
	}
	want := map[int][]uint64{10: {1, 2}, 11: {2, 3}, 12: {4}}
	for h, cs := range want {
		g := got[h]
		sort.Slice(g, func(i, j int) bool { return g[i] < g[j] })
		if len(g) != len(cs) {
			t.Fatalf("hour %d: batches %v, want %v", h, g, cs)
		}
		for i := range cs {
			if g[i] != cs[i] {
				t.Fatalf("hour %d: batches %v, want %v", h, g, cs)
			}
		}
	}
}

// A one-file group keeps the group coverage as the file's.
func TestCoverageOfASingleHourFile(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	for i := 0; i < 2; i++ {
		if err := b.WriteColumnarRecord(context.Background(), "db", coverageRecord("cpu", 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := reg.snapshot()
	if len(files) != 1 || files[0].cov.File != nil {
		t.Fatalf("files %+v", files)
	}
	ranges, _, exact := files[0].cov.FilePieces()
	if !exact || len(ranges) != 1 || ranges[0].Lo != 1 || ranges[0].Hi != 2 {
		t.Fatalf("ranges %+v", ranges)
	}
}

// hourOfPath reads the hour directory of db/m/yyyy/mm/dd/hh/file.parquet.
func hourOfPath(t *testing.T, path string) int {
	t.Helper()
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		t.Fatalf("path %q", path)
	}
	h, err := strconv.Atoi(parts[len(parts)-2])
	if err != nil {
		t.Fatalf("path %q: %v", path, err)
	}
	return h
}

// Row spans follow mergeBatches: an uncovered batch (a legacy map) between
// covered ones still takes its rows, so later batches keep their offsets.
func TestCoverageRowSpansSkipUncoveredBatches(t *testing.T) {
	recs := []interface{}{
		&TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1, 2}}, Coverage: []CoveragePiece{{Instance: 1, CSeq: 1}}},
		map[string]interface{}{"time": []int64{3, 4, 5}},
		&TypedColumnBatch{Data: map[string]interface{}{"time": []int64{6}}, Coverage: []CoveragePiece{{Instance: 1, CSeq: 2}}},
	}
	g := &flushGroup{spans: coverageRowSpans(recs)}
	if got := g.piecesAt([]int{2, 3, 4}); len(got) != 0 {
		t.Fatalf("rows of the uncovered batch mapped to %v", got)
	}
	if got := g.piecesAt([]int{1, 5}); len(got) != 2 || got[0].CSeq != 1 || got[1].CSeq != 2 {
		t.Fatalf("pieces %v", got)
	}
}

// A refused group keeps its WAL entries until every refused file of it is
// resolved (its rows registered elsewhere), then releases them.
func TestRefusedGroupReleasesOnceResolved(t *testing.T) {
	refused := CoverageRefused
	reg := &coverageRegistrar{autoSettle: &refused}
	b, w := newCoverageBuffer(t, 1000, reg)
	h1 := time.Date(2026, 10, 12, 10, 30, 0, 0, time.UTC).UnixMicro()
	h2 := time.Date(2026, 10, 12, 11, 30, 0, 0, time.UTC).UnixMicro()
	if err := b.WriteColumnarRecord(context.Background(), "db", coverageRecordFrom("cpu", []interface{}{h1, h2}, []interface{}{1.0, 2.0})); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := reg.snapshot()
	if len(files) != 2 || w.PendingUnflushedCount() == 0 {
		t.Fatalf("files %d, pending %d", len(files), w.PendingUnflushedCount())
	}
	files[0].cov.Settle(files[0].path, CoverageDeleted)
	if w.PendingUnflushedCount() == 0 {
		t.Fatal("released with a refused file still unresolved")
	}
	files[1].cov.Settle(files[1].path, CoverageResolved)
	if w.PendingUnflushedCount() != 0 {
		t.Fatalf("still %d pending once every refused file resolved", w.PendingUnflushedCount())
	}
}

func TestAccountedFloorMovesPastLostBatches(t *testing.T) {
	tr := newCoverageTracker(9)
	piece := func(c uint64) []CoveragePiece { return []CoveragePiece{{Instance: 9, CSeq: c}} }
	for _, c := range []uint64{1, 2, 5, 6, 9} {
		tr.settleKey("k", piece(c), true)
	}
	tr.settleKey("k", piece(3), false)
	tr.settleKey("k", piece(4), false)
	got := tr.accounted(nil)["k"]
	if got.Floor != 6 || len(got.Lost) != 1 || got.Lost[0] != [2]uint64{3, 4} {
		t.Fatalf("accounted %+v, want floor 6, lost [3 4]", got)
	}
	if tr.progress()["k"].Floor != 2 {
		t.Fatal("the registered floor moved past a lost batch")
	}
	// Batch 7 unsettled: the accounted floor waits there.
	tr.settleKey("k", piece(8), false)
	if got := tr.accounted(nil)["k"]; got.Floor != 6 {
		t.Fatalf("accounted floor %d passed an unsettled batch", got.Floor)
	}
}

// From a floor already reported, only lost batches above it are listed: the
// report does not grow, and the lost cap applies per report.
func TestAccountedFromAReportedFloor(t *testing.T) {
	tr := newCoverageTracker(9)
	piece := func(c uint64) []CoveragePiece { return []CoveragePiece{{Instance: 9, CSeq: c}} }
	tr.settleKey("k", piece(1), false)
	for c := uint64(2); c <= 5; c++ {
		tr.settleKey("k", piece(c), true)
	}
	tr.settleKey("k", piece(6), false)
	tr.settleKey("k", piece(7), true)
	got := tr.accounted(map[string]uint64{"k": 5})["k"]
	if got.Floor != 7 || len(got.Lost) != 1 || got.Lost[0] != [2]uint64{6, 6} {
		t.Fatalf("from 5: %+v", got)
	}
}
