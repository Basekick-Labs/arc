package ingest

import (
	"context"
	"testing"
)

// A flush puts batches another writer claimed and batches nobody claimed in
// separate files, each with its own coverage.
func TestFlushSplitsClaimedBatches(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000000, reg)
	b.SetClaimedBatch(func(_ uint64, key string, cseq uint64) bool { return key == "db/cpu" && cseq == 2 })
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := b.WriteColumnarRecord(ctx, "db", coverageRecord("cpu", float64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	files := reg.snapshot()
	if len(files) != 2 {
		t.Fatalf("files %d", len(files))
	}
	got := map[uint64]int{}
	for i, f := range files {
		for _, r := range f.cov.Ranges {
			for c := r.Lo; c <= r.Hi; c++ {
				got[c] = i
			}
		}
	}
	if len(got) != 3 || got[1] != got[3] || got[2] == got[1] {
		t.Fatalf("batches by file %v", got)
	}
}

// Batches of different instances (WAL recovery replays an earlier one into
// the same key) never share a group; one class needs no split.
func TestSplitForCoverageByInstance(t *testing.T) {
	b, _ := newCoverageBuffer(t, 1000000, &coverageRegistrar{})
	batch := func(inst, cseq uint64) *TypedColumnBatch {
		return &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1, 2}}, Coverage: []CoveragePiece{{Instance: inst, CSeq: cseq}}}
	}
	parts := b.splitForCoverage("db/cpu", []interface{}{batch(7, 1), batch(9, 1), batch(7, 2)})
	if len(parts) != 2 || len(parts[0].records) != 2 || len(parts[1].records) != 1 || parts[0].rows != 4 {
		t.Fatalf("parts %+v", parts)
	}
	if parts[0].group == nil || len(parts[0].group.pieces) != 2 || parts[1].group.pieces[0].Instance != 9 {
		t.Fatal("groups")
	}
	if b.splitForCoverage("db/cpu", []interface{}{batch(7, 1), batch(7, 2)}) != nil {
		t.Fatal("one class split")
	}
	// Uncovered rows form their own class.
	if p := b.splitForCoverage("db/cpu", []interface{}{batch(7, 1), map[string]interface{}{"time": []int64{1}}}); len(p) != 2 {
		t.Fatalf("uncovered parts %d", len(p))
	}
}

// The size-triggered (asynchronous) flush splits too.
func TestAsyncFlushSplitsClaimedBatches(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 3, reg)
	b.SetClaimedBatch(func(_ uint64, _ string, cseq uint64) bool { return cseq == 2 })
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := b.WriteColumnarRecord(ctx, "db", coverageRecord("cpu", float64(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.FlushAllAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if files := reg.snapshot(); len(files) != 2 {
		t.Fatalf("files %d", len(files))
	}
}
