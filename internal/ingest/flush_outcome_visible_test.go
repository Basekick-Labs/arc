package ingest

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// Tests for a visible write's outcome (#1759, flush_outcome_visible.go).

// visibleCtx is a write context carrying a new visible outcome, with the
// future-stamp check off.
func visibleCtx() (context.Context, *FlushOutcome) {
	o := NewVisibleOutcome(time.Now(), false, 0)
	return WithFlushOutcome(context.Background(), o), o
}

// waitVisible waits on o for rows written rows, on storage that can confirm.
func waitVisible(o *FlushOutcome, rows int64, budget time.Duration) VisibleVerdict {
	return WaitVisible(context.Background(), o, VisibleWait{WrittenRows: rows, WrittenRowsKnown: true, Budget: budget, StorageConfirmable: true})
}

type visibleCounts struct {
	marked, markedRows, settled, settledRows, failedNoWAL, failedWAL, unconfirmed int64
}

func countsOf(o *FlushOutcome) visibleCounts {
	v := o.vis
	return visibleCounts{
		marked: v.markedBatches.Load(), markedRows: v.markedRows.Load(),
		settled: v.settledBatches.Load(), settledRows: v.settledRows.Load(),
		failedNoWAL: v.failedNoWALBatches.Load(), failedWAL: v.failedWALBatches.Load(),
		unconfirmed: v.unconfirmedBatches.Load(),
	}
}

// visibleBatch is a batch of rows rows in the current hour, carrying o
// (marked as the append sites mark it).
func visibleBatch(o *FlushOutcome, rows int, hasWALCopy bool) *TypedColumnBatch {
	start := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	times := make([]int64, rows)
	vals := make([]float64, rows)
	for i := range times {
		times[i], vals[i] = start+int64(i), float64(i)
	}
	data := map[string]interface{}{"time": times, "value": vals}
	batch := &TypedColumnBatch{Data: data, Signature: getColumnSignature(data), hasWALCopy: hasWALCopy}
	countOutcome(o, batch, rows, visibleMinFuture(o, batch))
	return batch
}

// tierStub is the tiering metadata store: RecordFile fails on the calls in
// fail (1-based), and records what the outcome had settled at each call.
type tierStub struct {
	mu            sync.Mutex
	calls         int
	fail          map[int]bool
	o             *FlushOutcome
	settledAtCall []int64
}

func (s *tierStub) RecordFile(_ context.Context, _ *tiering.FileMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.o != nil {
		s.settledAtCall = append(s.settledAtCall, s.o.vis.settledBatches.Load())
	}
	if s.fail[s.calls] {
		return errors.New("injected RecordFile failure")
	}
	return nil
}

func columnarVisibleRecord(m string, rows int) *models.ColumnarRecord {
	start := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	times := make([]interface{}, rows)
	vals := make([]interface{}, rows)
	for i := range times {
		times[i], vals[i] = start+int64(i), float64(i)
	}
	return &models.ColumnarRecord{Measurement: m, Columnar: true, TimeUnit: "us",
		Columns: map[string][]interface{}{"time": times, "value": vals}}
}

// visibleWALBuffer is a WAL-on buffer over st: every write has a WAL copy.
func visibleWALBuffer(t *testing.T, st *pathFailStorage, cfg *config.IngestConfig) *ArrowBuffer {
	t.Helper()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(t.TempDir(), "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	buf := NewArrowBuffer(cfg, st, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = buf.Close(); _ = w.Close() })
	buf.SetWAL(w)
	return buf
}

func oneWorkerConfig(maxBuffer int) *config.IngestConfig {
	return &config.IngestConfig{
		MaxBufferSize: maxBuffer, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 16, FlushTimeoutSeconds: 5,
	}
}

// --- settle sites (§9 #1) ---

// The worker (size trigger): flushRecordsAsync settles, through the typed
// append site.
func TestVisibleSettlesThroughTheWorker1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 5)
	ctx, o := visibleCtx()
	writeOutcomeBatch(t, buf, ctx, "mworker", 5) // 5 rows: the size trigger queues it
	if v := waitVisible(o, 5, 10*time.Second); !v.Visible {
		t.Fatalf("verdict %+v, want visible", v)
	}
	if c := countsOf(o); c.marked != 1 || c.markedRows != 5 || c.settled != 1 || c.settledRows != 5 {
		t.Fatalf("counts %+v", c)
	}
}

// The aged sweep (writeExtracted), through the columnar append site.
func TestVisibleSettlesThroughTheAgedSweep1759(t *testing.T) {
	buf := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 100, Compression: "snappy",
		ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 16,
	}, &pathFailStorage{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = buf.Close() })
	ctx, o := visibleCtx()
	if err := buf.WriteColumnarRecord(ctx, "db", columnarVisibleRecord("maged", 3)); err != nil {
		t.Fatal(err)
	}
	if v := waitVisible(o, 3, 10*time.Second); !v.Visible {
		t.Fatalf("verdict %+v, want visible", v)
	}
	if c := countsOf(o); c.marked != 1 || c.markedRows != 3 || c.settled != 1 || c.settledRows != 3 {
		t.Fatalf("counts %+v", c)
	}
}

// Close step 1 (flushBufferLocked → writeExtracted).
func TestVisibleSettlesThroughCloseFlush1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	ctx, o := visibleCtx()
	writeOutcomeBatch(t, buf, ctx, "mclose", 4)
	o.Seal()
	if o.Resolved() {
		t.Fatal("resolved before any flush")
	}
	if err := buf.Close(); err != nil {
		t.Fatal(err)
	}
	if !o.Resolved() || countsOf(o).settled != 1 {
		t.Fatalf("after Close: resolved %v, counts %+v", o.Resolved(), countsOf(o))
	}
}

// Close's drain of a queued task (drainAndFlushQueue → flushRecordsAsync).
func TestVisibleSettlesThroughCloseDrain1759(t *testing.T) {
	gate := make(chan struct{})
	st := &pathFailStorage{gate: gate}
	buf := NewArrowBuffer(oneWorkerConfig(5), st, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = buf.Close() })
	writeOutcomeBatch(t, buf, context.Background(), "mfirst", 5) // the only worker takes it and blocks
	awaitBufferRecordMetrics(t, 0, 5)
	ctx, o := visibleCtx()
	writeOutcomeBatch(t, buf, ctx, "mqueued", 5) // waits in the queue
	awaitBufferRecordMetrics(t, 5, 5)
	done := make(chan struct{})
	go func() {
		buf.drainAndFlushQueue(time.Now().Add(time.Minute))
		close(done)
	}()
	close(gate)
	<-done
	o.Seal()
	if !o.Resolved() || countsOf(o).settled != 1 {
		t.Fatalf("after the drain: resolved %v, counts %+v", o.Resolved(), countsOf(o))
	}
}

// A coverage split: the synchronous flush (flushPartSync) and the async one
// (flushRecordsAsync per part) settle every part.
func TestVisibleSettlesThroughASplitFlush1759(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "async"}[async], func(t *testing.T) {
			reg := &coverageRegistrar{}
			maxBuffer := 1000000
			if async {
				maxBuffer = 3
			}
			b, _ := newCoverageBuffer(t, maxBuffer, reg)
			b.SetClaimedBatch(func(_ uint64, _ string, cseq uint64) bool { return cseq == 2 })
			ctx, o := visibleCtx()
			for i := 0; i < 3; i++ {
				if err := b.WriteColumnarRecord(ctx, "db", coverageRecord("cpu", float64(i))); err != nil {
					t.Fatal(err)
				}
			}
			if async {
				if err := b.FlushAllAndWait(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if err := b.FlushAll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if files := reg.snapshot(); len(files) != 2 {
				t.Fatalf("files %d, want the split's 2", len(files))
			}
			if v := waitVisible(o, 3, 10*time.Second); !v.Visible {
				t.Fatalf("verdict %+v, counts %+v", v, countsOf(o))
			}
			if c := countsOf(o); c.settled != 3 || c.settledRows != 3 {
				t.Fatalf("counts %+v", c)
			}
		})
	}
}

// --- registration failures (§9 #2-#5) ---

// Single hour: RecordFile fails → unconfirmed, not visible; the flush itself
// still succeeds. And the settle follows the registration: inside RecordFile
// nothing is settled yet (observed from inside the window).
func TestVisibleSingleHourRegistrationFailure1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	ctx, o := visibleCtx()
	stub := &tierStub{fail: map[int]bool{1: true}, o: o}
	buf.tierRecorder = stub
	writeOutcomeBatch(t, buf, ctx, "mreg", 3)
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatalf("flush: %v (a registration failure must not fail it)", err)
	}
	if v := waitVisible(o, 3, 5*time.Second); v.Reason != VisibleReasonNotConfirmed {
		t.Fatalf("verdict %+v, want not_confirmed", v)
	}
	if c := countsOf(o); c.unconfirmed != 1 || c.settled != 0 {
		t.Fatalf("counts %+v", c)
	}
	if len(stub.settledAtCall) != 1 || stub.settledAtCall[0] != 0 {
		t.Fatalf("settled inside RecordFile: %v", stub.settledAtCall)
	}
}

// Placement on success: RecordFile sees nothing settled; once the flush
// returns, the batch is.
func TestVisibleSettlesAfterRegistration1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	ctx, o := visibleCtx()
	stub := &tierStub{o: o}
	buf.tierRecorder = stub
	writeOutcomeBatch(t, buf, ctx, "morder", 3)
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(stub.settledAtCall) != 1 || stub.settledAtCall[0] != 0 {
		t.Fatalf("settled inside RecordFile: %v", stub.settledAtCall)
	}
	if v := waitVisible(o, 3, 5*time.Second); !v.Visible {
		t.Fatalf("verdict %+v", v)
	}
}

// Multi-hour, WAL off: every hour registers as stored (the early path). One
// hour's RecordFile fails → unconfirmed.
func TestVisibleMultiHourEarlyRegistrationFailure1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 100000)
	ctx, o := visibleCtx()
	stub := &tierStub{fail: map[int]bool{2: true}, o: o}
	buf.tierRecorder = stub
	if err := buf.writeColumnarInternal(ctx, "db", hoursRecord1692(4, 1), true, ""); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stub.calls != 4 {
		t.Fatalf("RecordFile calls %d, want one per hour", stub.calls)
	}
	if v := waitVisible(o, 4, 5*time.Second); v.Reason != VisibleReasonNotConfirmed {
		t.Fatalf("verdict %+v, want not_confirmed", v)
	}
	for i, s := range stub.settledAtCall {
		if s != 0 {
			t.Fatalf("settled inside RecordFile call %d", i+1)
		}
	}
}

// Multi-hour, WAL on, every hour deferred (#1697): all register after the
// last hour (the post-loop pass). One fails → unconfirmed; none failing →
// visible.
func TestVisibleMultiHourPostLoopRegistration1759(t *testing.T) {
	for _, failOne := range []bool{false, true} {
		t.Run(map[bool]string{false: "ok", true: "fails"}[failOne], func(t *testing.T) {
			b := walBuffer1697(t, newMemStore1697(0))
			ctx, o := visibleCtx()
			stub := &tierStub{o: o}
			if failOne {
				stub.fail = map[int]bool{3: true}
			}
			b.tierRecorder = stub
			if err := b.writeColumnarInternalWithWALCopy(ctx, "db", hoursRecord1692(4, 2), true, []string{tokC, tokD}, true); err != nil {
				t.Fatal(err)
			}
			if err, _ := flushWithin(t, b, 20*time.Second); err != nil {
				t.Fatal(err)
			}
			if stub.calls != 4 {
				t.Fatalf("RecordFile calls %d", stub.calls)
			}
			v := waitVisible(o, 4, 5*time.Second)
			if failOne && v.Reason != VisibleReasonNotConfirmed || !failOne && !v.Visible {
				t.Fatalf("verdict %+v, counts %+v", v, countsOf(o))
			}
		})
	}
}

// --- failures (§9 #6, #7, #12) ---

// Without a WAL a failed flush is a no-WAL failure: rows at risk, counted as
// before.
func TestVisibleNoWALFailure1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{fail: "/mfail/"}, 1000)
	ctx, o := visibleCtx()
	writeOutcomeBatch(t, buf, ctx, "mfail", 3)
	if err := buf.FlushAll(context.Background()); err == nil {
		t.Fatal("the flush did not fail")
	}
	v := waitVisible(o, 3, 5*time.Second)
	if v.Reason != VisibleReasonRowsAtRisk || v.RowsAtRisk != 3 {
		t.Fatalf("verdict %+v", v)
	}
	if c := countsOf(o); c.failedNoWAL != 1 || c.failedWAL != 0 {
		t.Fatalf("counts %+v", c)
	}
}

// With a WAL copy a failed flush is a WAL failure, and rows at risk stay 0:
// the import semantics are unchanged.
func TestVisibleWALFailure1759(t *testing.T) {
	buf := visibleWALBuffer(t, &pathFailStorage{fail: "/mwal/"}, oneWorkerConfig(1000))
	ctx, o := visibleCtx()
	writeOutcomeBatch(t, buf, ctx, "mwal", 3)
	if err := buf.FlushAll(context.Background()); err == nil {
		t.Fatal("the flush did not fail")
	}
	v := waitVisible(o, 3, 5*time.Second)
	if v.Reason != VisibleReasonWALReplay {
		t.Fatalf("verdict %+v", v)
	}
	if c := countsOf(o); c.failedWAL != 1 || c.failedNoWAL != 0 || o.RowsAtRisk() != 0 || o.vis.walRowsFailed.Load() != 3 {
		t.Fatalf("counts %+v, rows at risk %d", c, o.RowsAtRisk())
	}
}

// Close abandoning a queued task with a WAL copy: failed (WAL copy).
func TestVisibleCloseAbandonCountsWALFailure1759(t *testing.T) {
	gate := make(chan struct{})
	st := &pathFailStorage{gate: gate}
	buf := visibleWALBuffer(t, st, oneWorkerConfig(5))
	defer close(gate)
	writeOutcomeBatch(t, buf, context.Background(), "mfirst", 5) // the worker holds it
	awaitBufferRecordMetrics(t, 0, 5)
	ctx, o := visibleCtx()
	writeOutcomeBatch(t, buf, ctx, "mabandon", 5)
	awaitBufferRecordMetrics(t, 5, 5)
	buf.drainAndFlushQueue(time.Now().Add(-time.Second)) // no budget: abandoned
	if c := countsOf(o); c.failedWAL != 1 || c.failedNoWAL != 0 || o.RowsAtRisk() != 0 {
		t.Fatalf("counts %+v", c)
	}
	if v := waitVisible(o, 5, time.Second); v.Reason != VisibleReasonWALReplay {
		t.Fatalf("verdict %+v", v)
	}
}

// A batch whose rows are not known (no []int64 time column; not reachable
// through the append path, whose signature check and conversion always give
// one, so built directly): unconfirmed on success, and failed without a WAL
// copy on failure, though it adds no rows at risk.
func TestVisibleUnknownRowBatch1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	o := NewVisibleOutcome(time.Now(), false, 0)
	unknown := &TypedColumnBatch{Data: map[string]interface{}{"value": []float64{1}}}
	countOutcome(o, unknown, 1, math.MaxInt64)
	known := visibleBatch(o, 2, false)
	tally := outcomeTallyOf([]interface{}{unknown, known})
	buf.noteOutcomeSettled(tally, true)
	if c := countsOf(o); c.settled != 1 || c.settledRows != 2 || c.unconfirmed != 1 {
		t.Fatalf("success: counts %+v", c)
	}

	o2 := NewVisibleOutcome(time.Now(), false, 0)
	unknown2 := &TypedColumnBatch{Data: map[string]interface{}{"value": []float64{1}}}
	countOutcome(o2, unknown2, 1, math.MaxInt64)
	o2.Seal()
	buf.noteOutcomeFailure(outcomeTallyOf([]interface{}{unknown2}), errors.New("x"))
	if c := countsOf(o2); c.failedNoWAL != 1 || o2.RowsAtRisk() != 0 {
		t.Fatalf("failure: counts %+v, rows at risk %d", c, o2.RowsAtRisk())
	}
	if v := o2.visibleVerdict(VisibleWait{WrittenRows: 1, WrittenRowsKnown: true, StorageConfirmable: true}, VisibleReasonNone, time.Now()); v.Reason != VisibleReasonRowsAtRisk {
		t.Fatalf("verdict %+v", v)
	}
}

// --- seal, wake-up, abandoned, over-resolved (§9 #8, #11) ---

// Two appends to one key with a completed flush between them: not resolved
// before the seal, nor until the second batch settles.
func TestVisibleSeal1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	ctx, o := visibleCtx()
	writeOutcomeBatch(t, buf, ctx, "mseal", 2)
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c := countsOf(o); c.settled != 1 || c.marked != 1 {
		t.Fatalf("counts %+v", c)
	}
	if o.Resolved() {
		t.Fatal("resolved before the seal, with the write still appending")
	}
	writeOutcomeBatch(t, buf, ctx, "mseal", 2)
	o.Seal()
	if o.Resolved() {
		t.Fatal("resolved with the second batch still buffered")
	}
	if v := waitVisible(o, 4, 100*time.Millisecond); v.Reason != VisibleReasonTimeout {
		t.Fatalf("verdict %+v, want timeout", v)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := waitVisible(o, 4, time.Second); !v.Visible {
		t.Fatalf("verdict %+v", v)
	}
}

// A blocked waiter wakes within 200 ms of the settle.
func TestVisibleWaiterWakesOnSettle1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	o := NewVisibleOutcome(time.Now(), false, 0)
	batch := visibleBatch(o, 1, false)
	done := make(chan VisibleVerdict, 1)
	go func() { done <- waitVisible(o, 1, 10*time.Second) }()
	time.Sleep(50 * time.Millisecond)
	settledAt := time.Now()
	buf.noteOutcomeSettled(outcomeTallyOf([]interface{}{batch}), true)
	select {
	case v := <-done:
		if !v.Visible {
			t.Fatalf("verdict %+v", v)
		}
		if d := time.Since(settledAt); d > 200*time.Millisecond {
			t.Fatalf("woke %s after the settle", d)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("the waiter did not wake on the settle")
	}
}

// An outcome nobody waits on (an error or partial answer) keeps counting:
// the signal never blocks a flush.
func TestVisibleAbandonedOutcomeNeverBlocks1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	o := NewVisibleOutcome(time.Now(), false, 0)
	batches := make([]interface{}, 100)
	for i := range batches {
		batches[i] = visibleBatch(o, 1, false)
	}
	done := make(chan struct{})
	go func() {
		for i := range batches {
			buf.noteOutcomeSettled(outcomeTallyOf(batches[i:i+1]), true)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("settling an outcome nobody waits on blocked")
	}
	if c := countsOf(o); c.settled != 100 {
		t.Fatalf("counts %+v", c)
	}
}

// More resolved than marked (a batch counted twice) is never resolved and is
// reported once.
func TestVisibleOverResolved1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	o := NewVisibleOutcome(time.Now(), false, 0)
	batch := visibleBatch(o, 1, false)
	o.Seal()
	before := metrics.Get().Snapshot()["ingest_wait_visible_over_resolved_total"].(int64)
	tally := outcomeTallyOf([]interface{}{batch})
	buf.noteOutcomeSettled(tally, true)
	if !o.Resolved() {
		t.Fatal("not resolved after its one batch settled")
	}
	buf.noteOutcomeSettled(tally, true)
	buf.noteOutcomeSettled(tally, true)
	if o.Resolved() {
		t.Fatal("resolved with more batches settled than marked")
	}
	if d := metrics.Get().Snapshot()["ingest_wait_visible_over_resolved_total"].(int64) - before; d != 1 {
		t.Fatalf("over-resolved counted %d times, want once", d)
	}
	if v := waitVisible(o, 1, 50*time.Millisecond); v.Visible {
		t.Fatalf("verdict %+v", v)
	}
}

// --- panic (§9 #9) ---

type panicStorage struct{ pathFailStorage }

func (s *panicStorage) Write(context.Context, string, []byte) error { panic("injected storage panic") }

// A flush that panics leaves the batch unresolved: no settle on the way down.
func TestVisiblePanicLeavesTheBatchUnresolved1759(t *testing.T) {
	buf := NewArrowBuffer(oneWorkerConfig(1000), &panicStorage{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = buf.Close() })
	o := NewVisibleOutcome(time.Now(), false, 0)
	batch := visibleBatch(o, 3, false)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the flush did not panic")
			}
		}()
		_ = buf.flushRecordsAsync(context.Background(), "db/mpanic", "db", "mpanic", []interface{}{batch}, 3, nil, nil, time.Now())
	}()
	o.Seal()
	if c := countsOf(o); c.settled != 0 || c.unconfirmed != 0 || c.failedNoWAL != 0 || c.marked != 1 || o.Resolved() {
		t.Fatalf("counts %+v after a panic", c)
	}
}

// --- AST checks (§9 #9, #10) ---

func parseIngestSources(t *testing.T) (*token.FileSet, []*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return fset, files
}

func callName(n ast.Node) string {
	stmt, ok := n.(*ast.ExprStmt)
	if !ok {
		return ""
	}
	call, ok := stmt.X.(*ast.CallExpr)
	if !ok {
		return ""
	}
	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fn.Sel.Name
	case *ast.Ident:
		return fn.Name
	}
	return ""
}

func containsCall(n ast.Node, name string) bool {
	found := false
	ast.Inspect(n, func(m ast.Node) bool {
		if call, ok := m.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == name {
				found = true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == name {
				found = true
			}
		}
		return !found
	})
	return found
}

// noteOutcomeSettled is called only directly in the bodies of the three
// success paths: never in a defer, a closure or a goroutine, so a flush that
// panics never settles. And there, in the same block as completeFlush and
// after it.
func TestVisibleSettleIsNeverDeferred1759(t *testing.T) {
	fset, files := parseIngestSources(t)
	want := map[string]bool{"flushRecordsAsync": false, "flushPartSync": false, "writeExtracted": false}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.DeferStmt, *ast.GoStmt, *ast.FuncLit:
				if containsCall(n, "noteOutcomeSettled") {
					t.Errorf("%s: noteOutcomeSettled inside a defer, goroutine or closure", fset.Position(n.Pos()))
				}
			case *ast.FuncDecl:
				if n.Body == nil || !containsCall(n.Body, "noteOutcomeSettled") || n.Name.Name == "noteOutcomeSettled" {
					return true
				}
				if _, ok := want[n.Name.Name]; !ok {
					t.Errorf("%s: noteOutcomeSettled called from %s", fset.Position(n.Pos()), n.Name.Name)
					return true
				}
				ast.Inspect(n.Body, func(m ast.Node) bool {
					block, ok := m.(*ast.BlockStmt)
					if !ok {
						return true
					}
					complete, settle := -1, -1
					for i, s := range block.List {
						switch callName(s) {
						case "completeFlush":
							complete = i
						case "noteOutcomeSettled":
							settle = i
						}
					}
					if settle >= 0 {
						if complete < 0 || settle < complete {
							t.Errorf("%s: in %s noteOutcomeSettled is not after completeFlush in its block", fset.Position(block.Pos()), n.Name.Name)
						}
						want[n.Name.Name] = true
					}
					return true
				})
			}
			return true
		})
	}
	for fn, ok := range want {
		if !ok {
			t.Errorf("%s does not settle after completeFlush", fn)
		}
	}
}

// Every markFlushFailure() has a noteOutcomeFailure before it in its block.
func TestVisibleEveryFlushFailureNotesTheOutcome1759(t *testing.T) {
	fset, files := parseIngestSources(t)
	sites := 0
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			block, ok := n.(*ast.BlockStmt)
			if !ok {
				return true
			}
			noted := false
			for _, s := range block.List {
				switch callName(s) {
				case "noteOutcomeFailure":
					noted = true
				case "markFlushFailure":
					sites++
					if !noted {
						t.Errorf("%s: markFlushFailure without noteOutcomeFailure before it", fset.Position(s.Pos()))
					}
				}
			}
			return true
		})
	}
	if sites < 6 {
		t.Fatalf("found %d markFlushFailure sites, want at least 6", sites)
	}
}

// --- imports unchanged (§9 #13) ---

// An import's outcome through every site: no vis, rows at risk exactly as
// before (#1888): only known rows without a WAL copy, only on failure.
func TestImportOutcomeUnchanged1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	o := &FlushOutcome{}
	noWAL := visibleBatch(o, 3, false)
	withWAL := visibleBatch(o, 4, true)
	unknown := &TypedColumnBatch{Data: map[string]interface{}{"value": []float64{1}}}
	countOutcome(o, unknown, 1, math.MaxInt64)
	if o.vis != nil || o.NoWALRows() != 4 { // 3 + the unknown batch's 1
		t.Fatalf("vis %v, no-WAL rows %d", o.vis, o.NoWALRows())
	}
	tally := outcomeTallyOf([]interface{}{noWAL, withWAL, unknown})
	buf.noteOutcomeSettled(tally, true)
	buf.noteOutcomeSettled(tally, false)
	if o.RowsAtRisk() != 0 {
		t.Fatalf("a success put %d rows at risk", o.RowsAtRisk())
	}
	buf.noteOutcomeFailure(tally, errors.New("x"))
	if o.RowsAtRisk() != 3 {
		t.Fatalf("rows at risk %d, want 3 (the known no-WAL rows)", o.RowsAtRisk())
	}
	o.Seal()
	if o.Resolved() {
		t.Fatal("an import outcome resolved")
	}
	if v := WaitVisible(context.Background(), o, VisibleWait{}); v.Visible || v.Reason != VisibleReasonNotConfirmed {
		t.Fatalf("verdict %+v", v)
	}
}

// A live write's records carry no outcome: no tally, no allocation.
func TestOutcomeTallyOfLiveIsNil1759(t *testing.T) {
	records, _ := liveFlushBatches(10, 2)
	if outcomeTallyOf(records) != nil {
		t.Fatal("a live flush has a tally")
	}
	if n := testing.AllocsPerRun(100, func() { _ = outcomeTallyOf(records) }); n != 0 {
		t.Fatalf("%v allocations", n)
	}
}

// --- verdict (§3.6, §3.9) ---

func TestVisibleVerdictPrecedence1759(t *testing.T) {
	resolved := func(f func(v *visibleOutcome)) *FlushOutcome {
		o := NewVisibleOutcome(time.Now(), false, 0)
		o.vis.markedBatches.Store(2)
		o.vis.markedRows.Store(5)
		o.vis.settledBatches.Store(2)
		o.vis.settledRows.Store(5)
		o.vis.sealed.Store(true)
		if f != nil {
			f(o.vis)
		}
		return o
	}
	ok := VisibleWait{WrittenRows: 5, WrittenRowsKnown: true, StorageConfirmable: true}
	cases := []struct {
		name  string
		o     *FlushOutcome
		w     VisibleWait
		ended VisibleReason
		want  VisibleReason
	}{
		{"visible", resolved(nil), ok, "", VisibleReasonNone},
		{"no-WAL failure first", resolved(func(v *visibleOutcome) {
			v.settledBatches.Store(0)
			v.failedNoWALBatches.Store(1)
			v.failedWALBatches.Store(1)
		}), ok, VisibleReasonTimeout, VisibleReasonRowsAtRisk},
		{"WAL failure before timeout", resolved(func(v *visibleOutcome) {
			v.settledBatches.Store(0)
			v.failedWALBatches.Store(1)
		}), ok, VisibleReasonTimeout, VisibleReasonWALReplay},
		{"timeout", resolved(func(v *visibleOutcome) { v.settledBatches.Store(1) }), ok, VisibleReasonTimeout, VisibleReasonTimeout},
		{"shutdown", resolved(func(v *visibleOutcome) { v.settledBatches.Store(1) }), ok, VisibleReasonShutdown, VisibleReasonShutdown},
		{"not resolved without a reason", resolved(func(v *visibleOutcome) { v.sealed.Store(false) }), ok, "", VisibleReasonTimeout},
		{"unconfirmed", resolved(func(v *visibleOutcome) {
			v.settledBatches.Store(1)
			v.unconfirmedBatches.Store(1)
		}), ok, "", VisibleReasonNotConfirmed},
		{"rows written not marked", resolved(nil), VisibleWait{WrittenRows: 6, WrittenRowsKnown: true, StorageConfirmable: true}, "", VisibleReasonNotConfirmed},
		{"rows not known", resolved(nil), VisibleWait{WrittenRows: 5, StorageConfirmable: true}, "", VisibleReasonNotConfirmed},
		{"settled rows short", resolved(func(v *visibleOutcome) { v.settledRows.Store(4) }), ok, "", VisibleReasonNotConfirmed},
		{"storage", resolved(nil), VisibleWait{WrittenRows: 5, WrittenRowsKnown: true}, "", VisibleReasonStorageNotConfirmable},
		{"unconfirmed before storage", resolved(func(v *visibleOutcome) {
			v.settledBatches.Store(1)
			v.unconfirmedBatches.Store(1)
		}), VisibleWait{WrittenRows: 5, WrittenRowsKnown: true}, "", VisibleReasonNotConfirmed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := c.o.visibleVerdict(c.w, c.ended, time.Now())
			if v.Reason != c.want || v.Visible != (c.want == VisibleReasonNone) {
				t.Fatalf("verdict %+v, want %q", v, c.want)
			}
		})
	}
}

// Nothing written and nothing marked: visible at once. Rows written but
// nothing marked (the outcome lost on the way): not visible. A marked batch
// of no rows: waited for, not short-cut.
func TestVisibleEmptyAndLostOutcome1759(t *testing.T) {
	o := NewVisibleOutcome(time.Now(), false, 0)
	start := time.Now()
	if v := waitVisible(o, 0, 10*time.Second); !v.Visible || !v.Empty || time.Since(start) > time.Second {
		t.Fatalf("empty: verdict %+v after %s", v, time.Since(start))
	}

	lost := NewVisibleOutcome(time.Now(), false, 0)
	if v := waitVisible(lost, 5, time.Second); v.Visible || v.Reason != VisibleReasonNotConfirmed {
		t.Fatalf("lost outcome: verdict %+v", v)
	}

	zero := NewVisibleOutcome(time.Now(), false, 0)
	zero.vis.mark(0, math.MaxInt64)
	if v := waitVisible(zero, 0, 100*time.Millisecond); v.Visible || v.Reason != VisibleReasonTimeout {
		t.Fatalf("zero-row marked batch: verdict %+v", v)
	}
}

// Shutdown (Done) and a cancelled context end the wait.
func TestVisibleWaitShutdown1759(t *testing.T) {
	o := NewVisibleOutcome(time.Now(), false, 0)
	o.vis.mark(1, math.MaxInt64)
	done := make(chan struct{})
	close(done)
	if v := WaitVisible(context.Background(), o, VisibleWait{WrittenRows: 1, WrittenRowsKnown: true, Budget: 10 * time.Second, Done: done, StorageConfirmable: true}); v.Reason != VisibleReasonShutdown {
		t.Fatalf("verdict %+v", v)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if v := WaitVisible(ctx, o, VisibleWait{WrittenRows: 1, WrittenRowsKnown: true, Budget: 10 * time.Second, StorageConfirmable: true}); v.Reason != VisibleReasonShutdown {
		t.Fatalf("verdict %+v", v)
	}
}

// --- future-stamped rows (§3.7, §9 #14) ---

// The verdict and countFutureStamped use one window: a row is flagged by the
// verdict (verdict at admission) exactly when the flush-time metric counts
// it (flushed at admission).
func TestVisibleFutureStampWindowShared1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	margin := 5 * time.Second
	buf.SetFutureStampMargin(margin)
	tAdm := time.Now().UTC().Truncate(time.Hour).Add(10 * time.Minute)
	lo, hi := futureStampWindow(tAdm, margin)
	for _, tc := range []struct {
		t       int64
		flagged bool
	}{{lo, false}, {lo + 1, true}, {hi - 1, true}, {hi, false}, {tAdm.UnixMicro(), false}} {
		before := metrics.Get().Snapshot()["ingest_future_stamped_rows_total"].(int64)
		buf.countFutureStamped("db", "m", []int64{tc.t}, tAdm)
		counted := metrics.Get().Snapshot()["ingest_future_stamped_rows_total"].(int64) - before

		o := NewVisibleOutcome(tAdm, true, margin)
		batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{tc.t}}}
		countOutcome(o, batch, 1, visibleMinFuture(o, batch))
		o.vis.settledBatches.Store(1)
		o.vis.settledRows.Store(1)
		o.Seal()
		v := o.visibleVerdict(VisibleWait{WrittenRows: 1, WrittenRowsKnown: true, StorageConfirmable: true}, VisibleReasonNone, tAdm)
		if (v.Reason == VisibleReasonFutureStamped) != tc.flagged || (counted == 1) != tc.flagged {
			t.Fatalf("row at lo%+dµs: verdict %+v, counted %d, want flagged %v", tc.t-lo, v, counted, tc.flagged)
		}
	}
}

// Through the append path: a row 30 s ahead in the same hour, margin 1 s →
// future_stamped; a row in the next hour (kept as anomalous) → visible when
// the verdict is in the admission hour; the check off → visible.
func TestVisibleFutureStampedRows1759(t *testing.T) {
	hourStart := time.Now().UTC().Truncate(time.Hour)
	tAdm := hourStart.Add(10 * time.Minute)
	defer func(f func() time.Time) { visibleNow = f }(visibleNow)
	visibleNow = func() time.Time { return tAdm.Add(time.Second) }
	for _, tc := range []struct {
		name  string
		t     time.Time
		check bool
		want  VisibleReason
	}{
		{"same hour, ahead", tAdm.Add(30 * time.Second), true, VisibleReasonFutureStamped},
		{"within the margin", tAdm.Add(500 * time.Millisecond), true, VisibleReasonNone},
		{"next hour", hourStart.Add(time.Hour + 5*time.Second), true, VisibleReasonNone},
		{"check off", tAdm.Add(30 * time.Second), false, VisibleReasonNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
			o := NewVisibleOutcome(tAdm, tc.check, 0) // raised to 1 s
			data := map[string]interface{}{"time": []int64{tAdm.UnixMicro(), tc.t.UnixMicro()}, "value": []float64{1, 2}}
			batch := &TypedColumnBatch{Data: data, Signature: getColumnSignature(data)}
			if err := buf.WriteTypedColumnarDirect(WithFlushOutcome(context.Background(), o), "db", "mfuture", batch, 2); err != nil {
				t.Fatal(err)
			}
			if err := buf.FlushAll(context.Background()); err != nil {
				t.Fatal(err)
			}
			if v := waitVisible(o, 2, 5*time.Second); v.Reason != tc.want {
				t.Fatalf("verdict %+v, want %q", v, tc.want)
			}
		})
	}
}

// --- BatchRows (§3.6, §9 #15) ---

func TestBatchRows1759(t *testing.T) {
	typed := &TypedColumnarRecord{Measurement: "m", NumRecords: 4}
	n, ok := BatchRows([]interface{}{columnarVisibleRecord("m", 3), typed, &models.Record{Measurement: "m"}})
	if !ok || n != 8 {
		t.Fatalf("rows %d, %v", n, ok)
	}
	if n, ok := BatchRows(nil); !ok || n != 0 {
		t.Fatalf("empty: %d, %v", n, ok)
	}
	if _, ok := BatchRows([]interface{}{columnarVisibleRecord("m", 1), "unknown"}); ok {
		t.Fatal("an unknown record type was counted")
	}
	var nilRec *models.ColumnarRecord
	if _, ok := BatchRows([]interface{}{nilRec}); ok {
		t.Fatal("a nil record was counted")
	}
}

// Both append sites mark, and a live write (no outcome) is untouched.
func TestVisibleBothAppendSitesMark1759(t *testing.T) {
	buf := outcomeBuffer(t, &pathFailStorage{}, 1000)
	ctx, o := visibleCtx()
	if err := buf.WriteColumnarRecord(ctx, "db", columnarVisibleRecord("msites", 3)); err != nil {
		t.Fatal(err)
	}
	writeOutcomeBatch(t, buf, ctx, "msites2", 2)
	writeOutcomeBatch(t, buf, context.Background(), "msites2", 7) // live, same key
	if c := countsOf(o); c.marked != 2 || c.markedRows != 5 {
		t.Fatalf("counts %+v", c)
	}
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := waitVisible(o, 5, 5*time.Second); !v.Visible {
		t.Fatalf("verdict %+v, counts %+v", v, countsOf(o))
	}
}

// BenchmarkOutcomeTallyOf is the success-path tally of a live flush of 1000
// batches: no outcome, nothing allocated.
func BenchmarkOutcomeTallyOf(b *testing.B) {
	records, _ := liveFlushBatches(1000, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if outcomeTallyOf(records) != nil {
			b.Fatal("tally")
		}
	}
}
