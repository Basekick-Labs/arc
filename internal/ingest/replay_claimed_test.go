package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

var (
	rcH1 = time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	rcH2 = rcH1.Add(time.Hour)
)

func usAt(h time.Time, sec int) int64 { return h.Add(time.Duration(sec) * time.Second).UnixMicro() }

func columnarEntry(db, m string, times []int64, values []interface{}) *wal.Entry {
	t := make([]interface{}, len(times))
	for i, v := range times {
		t[i] = v
	}
	return &wal.Entry{ColumnarData: &wal.ColumnarEntry{Database: db, Measurement: m, Columns: map[string][]interface{}{"time": t, "value": values}}}
}

func rowEntry(db, m string, times []int64) *wal.Entry {
	var rows []map[string]interface{}
	for i, t := range times {
		rows = append(rows, map[string]interface{}{"_database": db, "_measurement": m, "time": t, "value": float64(i)})
	}
	return &wal.Entry{Records: rows}
}

func keepAll(_ time.Time, total int) ([][2]int, error) { return [][2]int{{0, total}}, nil }

// filesByHour returns the registered files' exact coverage, keyed by the hour
// in the path.
func coverageOf(t *testing.T, f coveredFile) (ranges []CoverageRange, partial []CoveragePiece) {
	t.Helper()
	r, p, exact := f.cov.FilePieces()
	if !exact {
		t.Fatalf("%s: inexact coverage", f.path)
	}
	return r, p
}

func outcomeOf(out []HourOutcome, rec int, hour time.Time) ReplayOutcome {
	for _, o := range out {
		if o.Record == rec && o.Hour.Equal(hour) {
			return o.Outcome
		}
	}
	return 0
}

// Two records, one spanning two hours: one file per hour, each naming the
// claim, and holding exactly the batches with rows in it. Nothing reaches
// the WAL.
func TestReplayClaimedPublishesPerHourWithTheClaim(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	r := ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 0x77, Epoch: 42, Records: []ClaimedRecord{
		{CSeq: 5, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1), usAt(rcH2, 1)}, []interface{}{1.0, 2.0}), Keep: keepAll},
		{CSeq: 6, Chunks: 1, Entry: rowEntry("db", "cpu", []int64{usAt(rcH1, 2)}), Keep: keepAll},
	}}
	out, err := b.ReplayClaimed(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if outcomeOf(out, 0, rcH1) != ReplayWritten || outcomeOf(out, 0, rcH2) != ReplayWritten || outcomeOf(out, 1, rcH1) != ReplayWritten || len(out) != 3 {
		t.Fatalf("outcomes %+v", out)
	}
	files := reg.snapshot()
	if len(files) != 2 {
		t.Fatalf("files %d", len(files))
	}
	sort.Slice(files, func(i, j int) bool { return files[i].path < files[j].path })
	for i, want := range []string{"[{119 5 6}]", "[{119 5 5}]"} {
		ranges, partial := coverageOf(t, files[i])
		if fmt.Sprint(ranges) != want || len(partial) != 0 || files[i].cov.Epochs[0x77] != 42 {
			t.Fatalf("file %d %s: ranges %v partial %v epochs %v", i, files[i].path, ranges, partial, files[i].cov.Epochs)
		}
	}
	if n := len(readWALCoverage(t, filepath.Dir(w.CurrentFile()))); n != 0 {
		t.Fatalf("%d WAL entries written by a claimed replay", n)
	}
}

// Keep filters per hour: a registered hour writes nothing, a partly
// registered one writes the remaining rows as a row-range piece, and an hour
// whose rows are all inside the registered ranges reports nothing left.
func TestReplayClaimedKeepsOnlyUnregisteredRows(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	times := []int64{usAt(rcH1, 1), usAt(rcH1, 2), usAt(rcH2, 1), usAt(rcH2, 2)}
	keep := func(h time.Time, total int) ([][2]int, error) {
		if total != 4 {
			return nil, fmt.Errorf("total %d", total)
		}
		if h.Equal(rcH1) {
			return [][2]int{{1, 4}}, nil // row 0 registered at H1
		}
		return [][2]int{{0, 2}}, nil // rows 2-3 registered at H2: none of 0-1 are in H2
	}
	covered := func(time.Time, int) ([][2]int, error) { return nil, nil }
	r := ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 0x77, Epoch: 42, Records: []ClaimedRecord{
		{CSeq: 5, Chunks: 1, Entry: columnarEntry("db", "cpu", times, []interface{}{1.0, 2.0, 3.0, 4.0}), Keep: keep},
		{CSeq: 6, Chunk: 1, Chunks: 3, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 3)}, []interface{}{5.0}), Keep: keepAll},
		{CSeq: 7, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 4)}, []interface{}{6.0}), Keep: covered},
	}}
	out, err := b.ReplayClaimed(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if outcomeOf(out, 0, rcH1) != ReplayWritten || outcomeOf(out, 0, rcH2) != ReplayNothingLeft ||
		outcomeOf(out, 1, rcH1) != ReplayWritten || outcomeOf(out, 2, rcH1) != ReplayCovered {
		t.Fatalf("outcomes %+v", out)
	}
	files := reg.snapshot()
	if len(files) != 1 {
		t.Fatalf("files %d", len(files))
	}
	_, partial := coverageOf(t, files[0])
	want := "[{119 5 0 0 1 4 4 true 42} {119 6 1 3 0 0 0 true 42}]"
	if fmt.Sprint(partial) != want {
		t.Fatalf("partial %v\nwant    %s", partial, want)
	}
}

// Records that cannot be published are reported, and the rest still is.
func TestReplayClaimedUnreplayableRecords(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	noTime := &wal.Entry{ColumnarData: &wal.ColumnarEntry{Database: "db", Measurement: "cpu", Columns: map[string][]interface{}{"value": {1.0}}}}
	badKeep := func(time.Time, int) ([][2]int, error) { return nil, errors.New("other coordinates") }
	r := ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 0x77, Epoch: 42, Records: []ClaimedRecord{
		{CSeq: 1, Chunks: 1, Entry: columnarEntry("db", "mem", []int64{usAt(rcH1, 1)}, []interface{}{1.0}), Keep: keepAll},
		{CSeq: 2, Chunks: 1, Entry: rowEntry("other", "cpu", []int64{usAt(rcH1, 1)}), Keep: keepAll},
		{CSeq: 3, Chunks: 1, Entry: noTime, Keep: keepAll},
		{CSeq: 4, Chunks: 1, Entry: &wal.Entry{DecodeErr: errors.New("bad")}, Keep: keepAll},
		{CSeq: 5, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1)}, []interface{}{1.0}), Keep: badKeep},
		{CSeq: 6, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1)}, []interface{}{1.0}), Keep: keepAll},
	}}
	out, err := b.ReplayClaimed(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if outcomeOf(out, i, time.Time{}) != ReplayUnreplayable {
			t.Fatalf("record %d: %+v", i, out)
		}
	}
	if outcomeOf(out, 4, rcH1) != ReplayUnreplayable || outcomeOf(out, 5, rcH1) != ReplayWritten {
		t.Fatalf("outcomes %+v", out)
	}
	if len(reg.snapshot()) != 1 {
		t.Fatalf("files %d", len(reg.snapshot()))
	}
}

// Columns of differing types go to separate files instead of panicking in
// the merge.
func TestReplayClaimedSplitsByColumnTypes(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	r := ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 0x77, Epoch: 42, Records: []ClaimedRecord{
		{CSeq: 1, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1)}, []interface{}{int64(1)}), Keep: keepAll},
		{CSeq: 2, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 2)}, []interface{}{"x"}), Keep: keepAll},
	}}
	if _, err := b.ReplayClaimed(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if len(reg.snapshot()) != 2 {
		t.Fatalf("files %d", len(reg.snapshot()))
	}
}

func TestReplayClaimedRefusals(t *testing.T) {
	one := func(cseq uint64) ClaimedRecord {
		return ClaimedRecord{CSeq: cseq, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1)}, []interface{}{1.0}), Keep: keepAll}
	}
	off, _ := newCoverageBuffer(t, 1000, nil)
	if _, err := off.ReplayClaimed(context.Background(), ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 1, Epoch: 1, Records: []ClaimedRecord{one(1)}}); err != ErrReplayCoverageOff {
		t.Fatalf("coverage off: %v", err)
	}
	plainCov, _ := newCoverageBuffer(t, 1000, nil)
	plainCov.SetFileRegistrar(&coverageRegistrar{}) // a registrar, coverage off
	if _, err := plainCov.ReplayClaimed(context.Background(), ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 1, Epoch: 1, Records: []ClaimedRecord{one(1)}}); err != ErrReplayCoverageOff {
		t.Fatalf("registrar without coverage: %v", err)
	}
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	call := func(recs ...ClaimedRecord) error {
		_, err := b.ReplayClaimed(context.Background(), ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 1, Epoch: 1, Records: recs})
		return err
	}
	if err := call(one(1), one(1)); err != ErrReplayDuplicate {
		t.Fatalf("duplicate: %v", err)
	}
	var many []ClaimedRecord
	for i := 0; i <= maxReplayRecords; i++ {
		many = append(many, one(uint64(i+1)))
	}
	if err := call(many...); err != ErrReplayTooLarge {
		t.Fatalf("records: %v", err)
	}
	big := one(1)
	big.PayloadBytes = maxReplayBytes + 1
	if err := call(big); err != nil {
		t.Fatalf("one record over the byte bound is still replayable alone: %v", err)
	}
	if err := call(one(2), big); err != ErrReplayTooLarge {
		t.Fatalf("bytes: %v", err)
	}
	// Too many partial pieces for one file's exact coverage.
	var parts []ClaimedRecord
	for i := 0; i <= maxReplayGroupPartials/2; i++ {
		rec := one(uint64(i + 1))
		rec.Chunk, rec.Chunks = 0, 2
		parts = append(parts, rec)
		rec.Chunk = 1
		parts = append(parts, rec)
	}
	if len(parts) > maxReplayRecords {
		parts = parts[:maxReplayRecords]
	}
	if err := call(parts...); err != nil {
		t.Fatalf("within the record bound: %v", err)
	}
	if len(reg.snapshot()) == 0 {
		t.Fatal("nothing written")
	}
	if err := replayGroupFits(manyPartialSubs(maxReplayGroupPartials + 1)); err != ErrReplayTooLarge {
		t.Fatalf("partials: %v", err)
	}
	b.closing.Store(true)
	if err := call(one(9)); err != ErrReplayClosed {
		t.Fatalf("closing: %v", err)
	}
}

func manyPartialSubs(n int) []replaySub {
	var g []replaySub
	for i := 0; i < n; i++ {
		g = append(g, replaySub{batch: &TypedColumnBatch{Coverage: []CoveragePiece{{Instance: 1, CSeq: uint64(i + 1), Chunk: 0, Chunks: 2}}}})
	}
	return g
}

// A group whose pieces of one instance name different claims carries the
// conflict marker, which no claim has: the manifest refuses the file.
func TestClaimEpochsConflict(t *testing.T) {
	got := claimEpochs([]CoveragePiece{{Instance: 1, CSeq: 1, claimEpoch: 5}, {Instance: 1, CSeq: 2, claimEpoch: 6},
		{Instance: 2, CSeq: 1, claimEpoch: 7}, {Instance: 2, CSeq: 2}, {Instance: 3, CSeq: 1}, {Instance: 4, CSeq: 1}, {Instance: 4, CSeq: 2, claimEpoch: 8}})
	if got[1] != ConflictEpoch || got[2] != ConflictEpoch || got[4] != ConflictEpoch || len(got) != 3 {
		t.Fatalf("epochs %v", got)
	}
	if _, ok := got[3]; ok {
		t.Fatalf("an unclaimed instance has an epoch: %v", got)
	}
}

func metricInt(name string) int64 { return metrics.Get().Snapshot()[name].(int64) }

// A failed flush reports ReplayFailed, registers nothing, and counts no rows
// lost: the claimant replays them again from its store.
func TestReplayClaimedFlushFailure(t *testing.T) {
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: t.TempDir(), SyncMode: wal.SyncModeAsync, Logger: zerolog.New(io.Discard)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy", ShardCount: 4,
		FlushWorkers: 2, FlushQueueSize: 64}, &failingStorageBackend{err: errors.New("disk gone")}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	b.SetWAL(w)
	reg := &coverageRegistrar{}
	b.SetFileRegistrar(reg)
	if err := b.EnableCoverage(); err != nil {
		t.Fatal(err)
	}
	lost := metricInt("wal_coverage_lost")
	out, err := b.ReplayClaimed(context.Background(), ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 0x77, Epoch: 42,
		Records: []ClaimedRecord{{CSeq: 1, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1)}, []interface{}{1.0}), Keep: keepAll}}})
	if err != nil || len(out) != 1 || out[0].Outcome != ReplayFailed || out[0].Err == nil {
		t.Fatalf("outcomes %+v, %v", out, err)
	}
	if len(reg.snapshot()) != 0 || metricInt("wal_coverage_lost") != lost {
		t.Fatal("a failed claimed replay registered a file or counted rows lost")
	}
}

// A claimed replay file the manifest refuses, or that is lost, settles
// without touching this writer's WAL coverage accounting.
func TestReplayClaimedRefusedOrLostFile(t *testing.T) {
	for _, outcome := range []CoverageOutcome{CoverageRefused, CoverageLost} {
		reg := &coverageRegistrar{autoSettle: &outcome}
		b, _ := newCoverageBuffer(t, 1000, reg)
		refused, lost := metricInt("wal_coverage_refused"), metricInt("wal_coverage_lost")
		if _, err := b.ReplayClaimed(context.Background(), ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 0x77, Epoch: 42,
			Records: []ClaimedRecord{{CSeq: 1, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1)}, []interface{}{1.0}), Keep: keepAll}}}); err != nil {
			t.Fatal(err)
		}
		if metricInt("wal_coverage_refused") != refused || metricInt("wal_coverage_lost") != lost {
			t.Fatalf("%v: WAL coverage metrics moved", outcome)
		}
	}
}

// Two keep ranges of one record in one hour: one outcome, one file with two
// row-range pieces.
func TestReplayClaimedTwoRangesInOneHour(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	keep := func(time.Time, int) ([][2]int, error) { return [][2]int{{0, 1}, {2, 3}}, nil }
	out, err := b.ReplayClaimed(context.Background(), ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 0x77, Epoch: 42,
		Records: []ClaimedRecord{{CSeq: 1, Chunks: 1, Entry: columnarEntry("db", "cpu", []int64{usAt(rcH1, 1), usAt(rcH1, 2), usAt(rcH1, 3)},
			[]interface{}{1.0, 2.0, 3.0}), Keep: keep}}})
	if err != nil || len(out) != 1 || out[0].Outcome != ReplayWritten {
		t.Fatalf("outcomes %+v, %v", out, err)
	}
	files := reg.snapshot()
	if len(files) != 1 {
		t.Fatalf("files %d", len(files))
	}
	if _, partial := coverageOf(t, files[0]); fmt.Sprint(partial) != "[{119 1 0 0 0 1 3 true 42} {119 1 0 0 2 3 3 true 42}]" {
		t.Fatalf("partial %v", partial)
	}
}
