package ingest

// #1207 S1: coverage ids minted at buffer commit, carried through the WAL and
// the flush into the registrar, and the WAL held until registration.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

type coveredFile struct {
	path string
	cov  *GroupCoverage
}

// coverageRegistrar records covered registrations. With autoSettle set it
// settles each file with that outcome as it is registered (before the flush
// checkpoints, the order a fast outbox drain produces).
type coverageRegistrar struct {
	mu         sync.Mutex
	files      []coveredFile
	plain      int
	autoSettle *CoverageOutcome
}

func (r *coverageRegistrar) RegisterFile(_, _, _ string, _ time.Time, _ int64, _ string) {
	r.mu.Lock()
	r.plain++
	r.mu.Unlock()
}

func (r *coverageRegistrar) RegisterCoveredFile(_, _, path string, _ time.Time, _ int64, _ string, cov *GroupCoverage) {
	r.mu.Lock()
	r.files = append(r.files, coveredFile{path, cov})
	auto := r.autoSettle
	r.mu.Unlock()
	if auto != nil {
		cov.Settle(path, *auto)
	}
}

func (r *coverageRegistrar) SyncRegistrations() error { return nil }

func (r *coverageRegistrar) snapshot() []coveredFile {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]coveredFile(nil), r.files...)
}

func (r *coverageRegistrar) settleAll(outcome CoverageOutcome) {
	for _, f := range r.snapshot() {
		f.cov.Settle(f.path, outcome)
	}
}

func newCoverageBuffer(t *testing.T, maxBufferSize int, reg *coverageRegistrar) (*ArrowBuffer, *wal.Writer) {
	t.Helper()
	dir := t.TempDir()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: dir, SyncMode: wal.SyncModeAsync, Logger: zerolog.New(io.Discard)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	cfg := &config.IngestConfig{
		MaxBufferSize:  maxBufferSize,
		MaxBufferAgeMS: 600000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   2,
		FlushQueueSize: 64,
	}
	b := NewArrowBuffer(cfg, &mockStorageBackend{}, zerolog.New(io.Discard))
	b.SetWAL(w)
	if reg != nil {
		b.SetFileRegistrar(reg)
		if err := b.EnableCoverage(); err != nil {
			t.Fatal(err)
		}
	}
	return b, w
}

func coverageRecord(m string, v float64) *models.ColumnarRecord {
	return coverageRecordFrom(m, []interface{}{time.Now().UTC().UnixMicro()}, []interface{}{v})
}

func coverageRecordFrom(m string, times, values []interface{}) *models.ColumnarRecord {
	cols := map[string][]interface{}{"time": times, "value": values}
	raw, err := msgpack.Marshal(map[string]interface{}{"m": m, "columns": cols})
	if err != nil {
		panic(err)
	}
	return &models.ColumnarRecord{
		Measurement: m,
		Columnar:    true,
		Columns:     cols,
		TimeUnit:    "us",
		RawPayload:  raw,
	}
}

func readWALCoverage(t *testing.T, dir string) []wal.Entry {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	sort.Strings(files)
	var out []wal.Entry
	for _, f := range files {
		entries, err := wal.NewReader(f, zerolog.New(io.Discard)).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if len(e.CheckpointHashes) == 0 {
				out = append(out, e)
			}
		}
	}
	return out
}

func walDir(w *wal.Writer) string { return filepath.Dir(w.CurrentFile()) }

// Concurrent writers to one key: every group's coverage is one contiguous
// range, ranges are disjoint, and together they are exactly 1..N. The WAL
// holds every cseq once, in cseq order.
func TestCoverageContiguousPerKeyUnderConcurrency(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 7, reg) // flushes asynchronously every ~7 batches
	const writers, per = 8, 50
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", float64(j)), false, ""); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	seen := map[uint64]bool{}
	for _, f := range reg.snapshot() {
		if f.cov.Key != "db/cpu" || len(f.cov.Partial) != 0 || len(f.cov.Ranges) != 1 {
			t.Fatalf("group coverage = %+v, want one range for db/cpu", f.cov)
		}
		r := f.cov.Ranges[0]
		for c := r.Lo; c <= r.Hi; c++ {
			if seen[c] {
				t.Fatalf("cseq %d covered by two groups", c)
			}
			seen[c] = true
		}
	}
	if len(seen) != writers*per {
		t.Fatalf("covered %d cseqs, want %d", len(seen), writers*per)
	}
	for c := uint64(1); c <= writers*per; c++ {
		if !seen[c] {
			t.Fatalf("cseq %d not covered", c)
		}
	}
	entries := readWALCoverage(t, walDir(w))
	if len(entries) != writers*per {
		t.Fatalf("WAL data entries = %d, want %d", len(entries), writers*per)
	}
	for i, e := range entries {
		if e.Coverage == nil || e.Coverage.CSeq != uint64(i+1) || e.Coverage.Key != "db/cpu" {
			t.Fatalf("WAL entry %d coverage = %+v, want cseq %d in file order", i, e.Coverage, i+1)
		}
	}
}

// Purge on registration: the flush checkpoints but its WAL entries stay in
// the purge floor until every file of the group has settled.
func TestCoverageHoldsWALUntilRegistered(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	for i := 0; i < 3; i++ {
		if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", float64(i)), false, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(reg.snapshot()) == 0 {
		t.Fatal("no covered registration")
	}
	if got := w.PendingUnflushedCount(); got != 3 {
		t.Fatalf("pending after checkpoint = %d, want 3 (held until registered)", got)
	}
	if f := b.RegisteredFloors()["db/cpu"]; f != 0 {
		t.Fatalf("floor before registration = %d, want 0", f)
	}
	reg.settleAll(CoverageRegistered)
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending after registration = %d, want 0", got)
	}
	if f := b.RegisteredFloors()["db/cpu"]; f != 3 {
		t.Fatalf("floor after registration = %d, want 3", f)
	}
	reg.settleAll(CoverageRegistered) // a repeated settle changes nothing
	if f := b.RegisteredFloors()["db/cpu"]; f != 3 {
		t.Fatalf("floor after a repeated settle = %d, want 3", f)
	}
	_ = b.Close()
}

// The outbox can settle a fresh file before the flush reaches its
// checkpoint; the release waits for both.
func TestCoverageSettleBeforeCheckpoint(t *testing.T) {
	registered := CoverageRegistered
	reg := &coverageRegistrar{autoSettle: &registered}
	b, w := newCoverageBuffer(t, 1000, reg)
	if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", 1), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	if f := b.RegisteredFloors()["db/cpu"]; f != 1 {
		t.Fatalf("floor = %d, want 1", f)
	}
	_ = b.Close()
}

// A file that settles without being registered releases the WAL hold but
// stops the floor below its batches, and counts as lost.
func TestCoverageLostHoldsFloor(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	before := metrics.Get().Snapshot()["wal_coverage_lost"]
	write := func() {
		if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", 1), false, ""); err != nil {
			t.Fatal(err)
		}
		if err := b.FlushAll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	write() // cseq 1
	reg.settleAll(CoverageLost)
	write() // cseq 2
	files := reg.snapshot()
	files[len(files)-1].cov.Settle(files[len(files)-1].path, CoverageRegistered)
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending = %d, want 0: a lost file must not pin the WAL", got)
	}
	if f := b.RegisteredFloors()["db/cpu"]; f != 0 {
		t.Fatalf("floor = %d, want 0 (cseq 1 lost)", f)
	}
	if after := metrics.Get().Snapshot()["wal_coverage_lost"]; after.(int64) <= before.(int64) {
		t.Fatalf("wal_coverage_lost did not increase (%v -> %v)", before, after)
	}
	_ = b.Close()
}

// A write refused under the shard lock mints nothing and leaves nothing in
// the WAL; the next accepted write gets cseq 1.
func TestCoverageRefusedWriteWritesNothing(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	shard := b.getShard("db/cpu")
	shard.mu.Lock()
	shard.swept = true
	shard.mu.Unlock()
	if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", 1), false, ""); !errors.Is(err, ErrBufferClosing) {
		t.Fatalf("err = %v, want ErrBufferClosing", err)
	}
	if got := b.walOnlyRecords.Load(); got != 0 {
		t.Fatalf("walOnlyRecords = %d, want 0: nothing was written to the WAL", got)
	}
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	shard.mu.Lock()
	shard.swept = false
	shard.mu.Unlock()
	if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", 2), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	entries := readWALCoverage(t, walDir(w))
	if len(entries) != 1 || entries[0].Coverage == nil || entries[0].Coverage.CSeq != 1 {
		t.Fatalf("WAL entries = %+v, want exactly the accepted write as cseq 1", entries)
	}
	_ = b.Close()
}

// Coverage off: the WAL keeps writing 0x03 entries, batches carry no
// coverage, and the checkpoint releases the floor as before.
func TestCoverageOffUnchanged(t *testing.T) {
	b, w := newCoverageBuffer(t, 1000, nil)
	if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", 1), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	for _, e := range readWALCoverage(t, walDir(w)) {
		if e.Coverage != nil {
			t.Fatalf("coverage off wrote a covered entry: %+v", e.Coverage)
		}
	}
	_ = b.Close()
}

// Without a coverage registrar EnableCoverage refuses.
func TestEnableCoverageNeedsCoverageRegistrar(t *testing.T) {
	b, _ := newCoverageBuffer(t, 1000, nil)
	b.SetFileRegistrar(&syncingRegistrar{log: &orderLog{}})
	if err := b.EnableCoverage(); err == nil {
		t.Fatal("EnableCoverage accepted a registrar that cannot carry coverage")
	}
	_ = b.Close()
}

// A replayed covered entry keeps its original coverage, with the row range of
// a recovery split.
func TestCoverageReplayCarriesPiece(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	inst := b.coverage.Load().instance
	ctx := wal.WithReplayCoverage(context.Background(), wal.ReplayCoverage{
		Coverage: wal.Coverage{Instance: inst, Key: "db/cpu", CSeq: 5, Chunks: 1},
		RowTotal: 4,
	})
	identity := "arc:rows:v1:" + "00000000000000010000000000000002" + ":0:2"
	cols := map[string][]interface{}{"time": {int64(1), int64(2)}, "value": {1.0, 2.0}}
	if err := b.WriteColumnarDirectReplay(ctx, "db", "cpu", cols, identity); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := reg.snapshot()
	if len(files) != 1 || len(files[0].cov.Partial) != 1 {
		t.Fatalf("registrations = %+v, want one with one partial piece", files)
	}
	p := files[0].cov.Partial[0]
	if p.CSeq != 5 || p.RowLo != 0 || p.RowHi != 2 || p.RowTotal != 4 {
		t.Fatalf("piece = %+v, want cseq 5 rows [0,2) of 4", p)
	}
	_ = b.Close()
}

func TestCoverageTrackerFloor(t *testing.T) {
	tr := newCoverageTracker(7)
	whole := func(c ...uint64) []CoveragePiece {
		var out []CoveragePiece
		for _, x := range c {
			out = append(out, CoveragePiece{Instance: 7, CSeq: x})
		}
		return out
	}
	tr.settleKey("k", whole(2, 3), true)
	if f := tr.floors()["k"]; f != 0 {
		t.Fatalf("floor with 1 missing = %d, want 0", f)
	}
	tr.settleKey("k", whole(1), true)
	if f := tr.floors()["k"]; f != 3 {
		t.Fatalf("floor = %d, want 3", f)
	}
	// Another instance's pieces do not move this instance's floor.
	tr.settleKey("k", []CoveragePiece{{Instance: 8, CSeq: 4}}, true)
	if f := tr.floors()["k"]; f != 3 {
		t.Fatalf("floor after a foreign piece = %d, want 3", f)
	}
	// cseq 4 in two chunks, the second in two row ranges.
	tr.settleKey("k", []CoveragePiece{{Instance: 7, CSeq: 4, Chunk: 0, Chunks: 2}}, true)
	tr.settleKey("k", []CoveragePiece{{Instance: 7, CSeq: 4, Chunk: 1, Chunks: 2, RowLo: 0, RowHi: 5, RowTotal: 10}}, true)
	if f := tr.floors()["k"]; f != 3 {
		t.Fatalf("floor with cseq 4 half-settled = %d, want 3", f)
	}
	tr.settleKey("k", []CoveragePiece{{Instance: 7, CSeq: 4, Chunk: 1, Chunks: 2, RowLo: 5, RowHi: 10, RowTotal: 10}}, true)
	if f := tr.floors()["k"]; f != 4 {
		t.Fatalf("floor = %d, want 4", f)
	}
	// A lost cseq holds the floor for good.
	tr.settleKey("k", whole(5), false)
	tr.settleKey("k", whole(5, 6), true)
	if f := tr.floors()["k"]; f != 4 {
		t.Fatalf("floor past a lost cseq = %d, want 4", f)
	}
}

func TestFoldPieces(t *testing.T) {
	ranges, partial := foldPieces([]CoveragePiece{
		{Instance: 1, CSeq: 3}, {Instance: 1, CSeq: 1}, {Instance: 1, CSeq: 2},
		{Instance: 1, CSeq: 5}, {Instance: 2, CSeq: 6},
		{Instance: 1, CSeq: 9, RowLo: 0, RowHi: 1, RowTotal: 2},
	})
	want := []CoverageRange{{1, 1, 3}, {1, 5, 5}, {2, 6, 6}}
	if len(ranges) != len(want) {
		t.Fatalf("ranges = %+v, want %+v", ranges, want)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("ranges = %+v, want %+v", ranges, want)
		}
	}
	if len(partial) != 1 || partial[0].CSeq != 9 {
		t.Fatalf("partial = %+v", partial)
	}
}

// A failed flush: batches with a complete WAL copy stay unsettled for
// recovery to replay; a batch with none is lost and stops the floor.
func TestCoverageFailedFlush(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	b.storage = &failingStorageBackend{err: errors.New("storage unavailable")}
	if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", 1), false, ""); err != nil {
		t.Fatal(err)
	}
	_ = b.FlushAll(context.Background())
	if got := w.PendingUnflushedCount(); got != 1 {
		t.Fatalf("pending after a failed flush = %d, want 1 (kept for recovery)", got)
	}
	if len(reg.snapshot()) != 0 {
		t.Fatal("a failed flush registered a file")
	}

	cs := b.coverage.Load()
	before := metrics.Get().Snapshot()["wal_coverage_lost"].(int64)
	g := &flushGroup{b: b, cs: cs, key: "db/x", settled: map[string]struct{}{}, pieces: []CoveragePiece{
		{Instance: cs.instance, CSeq: 1, replayable: true},
		{Instance: cs.instance, CSeq: 2},
	}}
	g.failed()
	if after := metrics.Get().Snapshot()["wal_coverage_lost"].(int64); after != before+1 {
		t.Fatalf("wal_coverage_lost %d -> %d, want +1 for the batch with no WAL copy", before, after)
	}
	cs.tracker.settleKey("db/x", []CoveragePiece{{Instance: cs.instance, CSeq: 1}, {Instance: cs.instance, CSeq: 3}}, true)
	if f := b.RegisteredFloors()["db/x"]; f != 1 {
		t.Fatalf("floor = %d, want 1 (cseq 2 lost)", f)
	}
	b.storage = &mockStorageBackend{}
	_ = b.Close()
}

// The typed write path (TLE and other pre-typed parsers) mints and commits
// like the columnar one.
func TestCoverageTypedPath(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	batch := &TypedColumnBatch{Data: map[string]interface{}{
		"time":  []int64{time.Now().UTC().UnixMicro()},
		"value": []float64{1},
	}}
	if err := b.WriteTypedColumnarDirect(context.Background(), "db", "sat", batch, 1); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	files := reg.snapshot()
	if len(files) != 1 || files[0].cov.Key != "db/sat" || len(files[0].cov.Ranges) != 1 || files[0].cov.Ranges[0].Lo != 1 {
		t.Fatalf("registrations = %+v, want db/sat cseq 1", files)
	}
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	entries := readWALCoverage(t, walDir(w))
	if len(entries) != 1 || entries[0].Coverage == nil || entries[0].Coverage.Key != "db/sat" {
		t.Fatalf("WAL entries = %+v, want one covered db/sat entry", entries)
	}
	if got := w.PendingUnflushedCount(); got != 1 {
		t.Fatalf("pending = %d, want 1 until registered", got)
	}
	reg.settleAll(CoverageRegistered)
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending after registration = %d, want 0", got)
	}
	_ = b.Close()
}

// A registrar that cannot carry coverage (swapped in after coverage was
// enabled) settles the file at once, or the group would hold its WAL.
func TestCoverageRegistrarWithoutCoverageReleases(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	b.SetFileRegistrar(&syncingRegistrar{log: &orderLog{}})
	if err := b.writeColumnarInternal(context.Background(), "db", coverageRecord("cpu", 1), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	_ = b.Close()
}

// A group that never announced a file still releases at its checkpoint.
func TestCoverageGroupWithoutFilesReleases(t *testing.T) {
	reg := &coverageRegistrar{}
	b, w := newCoverageBuffer(t, 1000, reg)
	cs := b.coverage.Load()
	p, err := w.PrepareCovered("db", "db/cpu", coverageRecord("cpu", 1).RawPayload)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := p.Commit(1)
	if err != nil {
		t.Fatal(err)
	}
	g := &flushGroup{b: b, cs: cs, key: "db/cpu", tokens: tokens, settled: map[string]struct{}{},
		pieces: []CoveragePiece{{Instance: cs.instance, CSeq: 1}}}
	g.checkpointDone()
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	_ = b.Close()
}

// A journaled coverage is capped and says how much it left out.
func TestGroupCoverageJournalCap(t *testing.T) {
	g := GroupCoverage{ID: "x", Key: "k"}
	for i := 0; i < maxJournalRanges+5; i++ {
		g.Ranges = append(g.Ranges, CoverageRange{Instance: 1, Lo: uint64(2 * i), Hi: uint64(2 * i)})
	}
	raw, err := json.Marshal(&g)
	if err != nil {
		t.Fatal(err)
	}
	var back GroupCoverage
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Ranges) != maxJournalRanges || back.RangesOmitted != 5 {
		t.Fatalf("ranges=%d omitted=%d, want %d and 5", len(back.Ranges), back.RangesOmitted, maxJournalRanges)
	}
	if len(raw) > 1<<20 {
		t.Fatalf("capped record is %d bytes, over the 1 MiB an older reader allows", len(raw))
	}
}

// A replayed entry whose recorded key is not the key it replays into carries
// no coverage.
func TestReplayPieceRejectsAnotherKey(t *testing.T) {
	ctx := wal.WithReplayCoverage(context.Background(), wal.ReplayCoverage{Coverage: wal.Coverage{Instance: 1, Key: "db/a", CSeq: 3, Chunks: 1}})
	if _, ok := replayPiece(ctx, "db/b", "00000000000000010000000000000002"); ok {
		t.Fatal("accepted coverage minted under another key")
	}
	if p, ok := replayPiece(ctx, "db/a", "00000000000000010000000000000002"); !ok || p.CSeq != 3 || !p.whole() {
		t.Fatalf("piece = %+v, %v", p, ok)
	}
}

func TestIntervalSet(t *testing.T) {
	var s intervalSet
	for _, x := range []uint64{5, 3, 4, 10, 8, 9, 1} {
		if !s.add(x) {
			t.Fatalf("add(%d) reported a duplicate", x)
		}
	}
	if s.add(4) {
		t.Fatal("add(4) twice reported new")
	}
	want := intervalSet{{1, 1}, {3, 5}, {8, 10}}
	if len(s) != len(want) {
		t.Fatalf("set = %v, want %v", s, want)
	}
	for i := range want {
		if s[i] != want[i] {
			t.Fatalf("set = %v, want %v", s, want)
		}
	}
	if !s.contains(9) || s.contains(6) || s.contains(2) {
		t.Fatalf("contains wrong on %v", s)
	}
}

// After a lost cseq stops a floor, later settled batches do not pile up: the
// set stays one interval, and the lag gauge returns to 0.
func TestCoverageTrackerBoundedAfterLoss(t *testing.T) {
	tr := newCoverageTracker(7)
	for c := uint64(1); c <= 1000; c++ {
		tr.minted()
	}
	tr.settleKey("k", []CoveragePiece{{Instance: 7, CSeq: 1}}, false)
	for c := uint64(2); c <= 1000; c++ {
		tr.settleKey("k", []CoveragePiece{{Instance: 7, CSeq: c}}, true)
	}
	kf := tr.keys["k"]
	if kf.floor != 0 || len(kf.done) != 1 || len(kf.lost) != 1 {
		t.Fatalf("floor=%d done=%v lost=%v", kf.floor, kf.done, kf.lost)
	}
	if lag := metrics.Get().Snapshot()["wal_registered_floor_lag"].(int64); lag != 1 {
		t.Fatalf("lag = %d, want 1 (the lost batch)", lag)
	}
}
