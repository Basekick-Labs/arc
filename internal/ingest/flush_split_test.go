package ingest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
)

func TestSplitFlushRecordsByStringColumnBytes(t *testing.T) {
	b := &ArrowBuffer{}
	records := []interface{}{
		&TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}, "left": []string{"1234"}}, WALHashes: []string{"wal-1"}},
		&TypedColumnBatch{Data: map[string]interface{}{"time": []int64{2}, "right": []string{"5678"}}, WALHashes: []string{"wal-2"}},
	}
	if parts := b.splitFlushRecordsByStringBytes("db/cpu", records, 5); parts != nil {
		t.Fatalf("different string columns should not be added together: got %d parts", len(parts))
	}

	records = append(records, &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{3}, "left": []string{"99"}}, WALHashes: []string{"wal-3"}})
	parts := b.splitFlushRecordsByStringBytes("db/cpu", records, 5)
	if len(parts) != 2 || parts[0].rows != 2 || parts[1].rows != 1 {
		t.Fatalf("parts = %+v, want two parts with 2 and 1 rows", parts)
	}
	if len(parts[0].walHashes) != 2 || parts[0].walHashes[0] != "wal-1" || parts[0].walHashes[1] != "wal-2" || len(parts[1].walHashes) != 1 || parts[1].walHashes[0] != "wal-3" {
		t.Fatalf("part WAL hashes = %v and %v", parts[0].walHashes, parts[1].walHashes)
	}
}

func TestSplitFlushRecordsAccountsForSanitizedMessagePackStrings(t *testing.T) {
	decoder := newTypedTestDecoder(true)
	invalidString := strings.Repeat("\xff", 128)
	decodeBatch := func(ts int64) (*TypedColumnBatch, int64) {
		t.Helper()
		payload, err := msgpack.Marshal(map[string]interface{}{
			"m": "cpu",
			"columns": map[string]interface{}{
				"time": []interface{}{ts},
				"host": []interface{}{invalidString},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		record, ok := decoder.tryDecodeColumnarTyped(payload)
		if !ok {
			t.Fatal("typed decoder rejected a valid columnar payload")
		}
		host := record.Batch.Data["host"].([]string)[0]
		if len(host) != len(invalidString)*3 {
			t.Fatalf("sanitized host length = %d, want %d", len(host), len(invalidString)*3)
		}
		// Use the same raw-plus-decoded estimate as the columnar writer.
		record.Batch.estimatedBytes = estimateTypedColumnStringBytesWithRawPayload(record.Batch, record.RawPayload)
		return record.Batch, int64(len(record.RawPayload))
	}

	first, firstEstimate := decodeBatch(1)
	second, secondEstimate := decodeBatch(2)
	records := []interface{}{first, second}
	estimate := firstEstimate + secondEstimate
	actualStringBytes := int64(len(first.Data["host"].([]string)[0]) + len(second.Data["host"].([]string)[0]))
	if estimate >= actualStringBytes {
		t.Fatalf("fixture does not expose estimate undercount: estimated %d, string data %d", estimate, actualStringBytes)
	}
	maxBytes := estimate + (actualStringBytes-estimate)/2
	if estimate > maxBytes || actualStringBytes <= maxBytes {
		t.Fatalf("invalid threshold: estimate %d, actual %d, max %d", estimate, actualStringBytes, maxBytes)
	}

	parts := (&ArrowBuffer{}).splitFlushRecordsByStringBytes("db/cpu", records, maxBytes)
	if len(parts) != 2 {
		t.Fatalf("got %d parts, want split for %d sanitized string bytes above limit %d", len(parts), actualStringBytes, maxBytes)
	}
}

func TestAsyncFlushSplitsStringColumnsAndKeepsCoverage(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	base := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC).UnixMicro()
	values := []string{"abc", "de", "klmn", "o", "pqrst"}
	records := make([]interface{}, 0, len(values))
	for i, value := range values {
		records = append(records, &TypedColumnBatch{
			Data: map[string]interface{}{
				"time":  []int64{base + int64(i)},
				"value": []float64{float64(i)},
				"text":  []string{value},
			},
			Coverage: []CoveragePiece{{Instance: 7, CSeq: uint64(i + 1)}},
		})
	}
	group := b.newFlushGroup("db/cpu", records, nil)
	if err := b.flushRecordsAsyncWithStringLimit(context.Background(), "db/cpu", "db", "cpu", records, len(values), nil, group, 5); err != nil {
		t.Fatal(err)
	}

	files := reg.snapshot()
	if len(files) != 3 {
		t.Fatalf("registered %d files, want 3 bounded groups", len(files))
	}
	want := [][2]uint64{{1, 2}, {3, 4}, {5, 5}}
	for i, file := range files {
		ranges, partial, exact := file.cov.FilePieces()
		if !exact || len(partial) != 0 || len(ranges) != 1 || ranges[0].Lo != want[i][0] || ranges[0].Hi != want[i][1] {
			t.Fatalf("file %d coverage = ranges %v partial %v exact %v; want cseqs %v", i, ranges, partial, exact, want[i])
		}
		if file.cov.Files != 1 {
			t.Fatalf("file %d belongs to a %d-file group, want 1", i, file.cov.Files)
		}
	}
}

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

// Batches a DELETE record holds and batches it does not never share a file
// (#1207 d1): the record is applied to a file as a whole.
func TestFlushSplitsByDeleteScope(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000000, reg)
	b.SetDeleteScope(func(_ uint64, key string, cseq uint64) []uint64 {
		if key == "db/cpu" && cseq <= 2 {
			return []uint64{40}
		}
		return nil
	})
	ctx := context.Background()
	for i := 0; i < 4; i++ {
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
	if len(got) != 4 || got[1] != got[2] || got[3] != got[4] || got[1] == got[3] {
		t.Fatalf("batches by file %v", got)
	}
}

// A DELETE flushes only its database's (or measurement's) buffers, and the
// marks are the batches acknowledged so far per key.
func TestFlushScopeAndWaitAndMarks(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000000, reg)
	ctx := context.Background()
	for _, m := range []string{"cpu", "cpu", "mem"} {
		if err := b.WriteColumnarRecord(ctx, "db", coverageRecord(m, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.WriteColumnarRecord(ctx, "dbx", coverageRecord("cpu", 1)); err != nil {
		t.Fatal(err)
	}
	m, err := b.FlushScopeAndWait(ctx, "db", "cpu")
	if err != nil || len(m) != 1 || m["db/cpu"] != 2 {
		t.Fatalf("marks %v %v", m, err)
	}
	if files := reg.snapshot(); len(files) != 1 || files[0].cov.Key != "db/cpu" {
		t.Fatalf("flushed %d files", len(files))
	}
	// db/cpu has nothing buffered any more: its mark still comes back.
	m, err = b.FlushScopeAndWait(ctx, "db", "")
	if err != nil || len(m) != 2 || m["db/mem"] != 1 || m["db/cpu"] != 2 {
		t.Fatalf("database marks %v %v", m, err)
	}
	files := reg.snapshot()
	if len(files) != 2 || files[1].cov.Key != "db/mem" {
		t.Fatalf("database flush: %d files", len(files))
	}
}
