package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1778: ingest appends of several tables at once.

// batchSource is the storage source with a hook on the append path's first
// call, which is where a batch's workers are observed and steered.
type batchSource struct {
	*StorageWalkSource
	mu       sync.Mutex
	listed   map[string]int // reconcile-pass listings per measurement
	inside   int            // workers now inside AppendRefs
	most     int            // most at once
	entered  chan struct{}  // one send per worker entering, when set
	hold     chan struct{}  // workers wait for it to close, when set
	failFor  map[string]error
	panicFor map[string]bool
	wedgeFor map[string]bool // wait for the append's own deadline
	onEnter  func(m Measurement)
}

func (b *batchSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	b.mu.Lock()
	b.listed[m.Measurement]++
	b.mu.Unlock()
	return b.StorageWalkSource.FilesAndLocal(ctx, m)
}

func (b *batchSource) AppendRefs(ctx context.Context, m Measurement, keys []string, sizes map[string]int64) ([]FileRef, bool, error) {
	b.mu.Lock()
	b.inside++
	if b.inside > b.most {
		b.most = b.inside
	}
	entered, hold, onEnter := b.entered, b.hold, b.onEnter
	fail, panics, wedged := b.failFor[m.Measurement], b.panicFor[m.Measurement], b.wedgeFor[m.Measurement]
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inside--
		b.mu.Unlock()
	}()
	if onEnter != nil {
		onEnter(m)
	}
	if entered != nil {
		entered <- struct{}{}
	}
	if hold != nil {
		<-hold
	}
	if wedged {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	if panics {
		panic("injected panic in an append")
	}
	if fail != nil {
		return nil, false, fail
	}
	return b.StorageWalkSource.AppendRefs(ctx, m, keys, sizes)
}

func (b *batchSource) passes(measurement string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listed[measurement]
}

// batchRig is n tables on local storage, each exported once by a pass that
// settled, with a scheduler that runs `concurrency` appends at once. The
// catalog database is opened as production opens it: WAL, a busy timeout and
// ONE connection, which every append's statements queue on.
type batchRig struct {
	t      *testing.T
	root   string
	exp    *Exporter
	src    *batchSource
	sched  *Scheduler
	tables []string
	files  map[string][]string
	seq    int
}

func newBatchRig(t *testing.T, n, concurrency int, appendOn bool) *batchRig {
	t.Helper()
	withSpacing(t, 0)
	root := t.TempDir()
	be, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db")+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, be, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := &batchSource{StorageWalkSource: NewStorageWalkSource(be, "arc", zerolog.Nop()), listed: map[string]int{}}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Interval: time.Hour, Logger: zerolog.Nop(),
		IngestInterval: time.Millisecond, IngestAppend: appendOn, IngestConcurrency: concurrency})
	sched.started = time.Now().Add(-time.Second)
	t.Cleanup(func() { metrics.Get().SetIcebergOldestPendingExport(time.Time{}) })
	r := &batchRig{t: t, root: root, exp: exp, src: src, sched: sched, files: map[string][]string{}}
	for i := 0; i < n; i++ {
		r.tables = append(r.tables, fmt.Sprintf("m%02d", i))
	}
	// Each table's first export is a pass.
	for _, m := range r.tables {
		r.report(m, r.newFile(m))
	}
	r.serve(4 * n)
	for _, m := range r.tables {
		if appendOn && !sched.appendOK["mydb\x00"+m] {
			t.Fatalf("table %s does not take appends after its first export", m)
		}
		if got := src.passes(m); got != 1 {
			t.Fatalf("table %s had %d passes in the setup, want 1", m, got)
		}
	}
	return r
}

func (r *batchRig) newFile(measurement string) string {
	r.t.Helper()
	r.seq++
	name := ingestName(measurement, time.Now().Add(time.Duration(r.seq)*time.Microsecond))
	writeArcStyleParquet(r.t, filepath.Join(r.root, "mydb", measurement, "2026", "07", "14", "15", name), 1_700_000_000_000_000, 5)
	r.files[measurement] = append(r.files[measurement], name)
	return name
}

func (r *batchRig) report(measurement, name string) {
	r.sched.RequestIngestAppend("mydb", measurement, "mydb/"+measurement+"/2026/07/14/15/"+name, 0)
}

// serve runs the ingest service once the spacing has passed.
func (r *batchRig) serve(max int) {
	time.Sleep(5 * time.Millisecond)
	r.sched.serviceIngest(context.Background(), max)
}

// entries is the table's live entries by file name, WITH repeats.
func (r *batchRig) entries(measurement string) map[string]int {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", measurement))
	if err != nil {
		r.t.Fatal(err)
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	out := map[string]int{}
	for _, task := range tasks {
		out[filepath.Base(task.File.FilePath())]++
	}
	return out
}

// wantEachOnce checks every table holds exactly the files written for it.
func (r *batchRig) wantEachOnce() {
	r.t.Helper()
	for _, m := range r.tables {
		got := r.entries(m)
		want := append([]string(nil), r.files[m]...)
		sort.Strings(want)
		if len(got) != len(want) {
			r.t.Fatalf("table %s has %d files %v, want %d", m, len(got), got, len(want))
		}
		for _, f := range want {
			if got[f] != 1 {
				r.t.Fatalf("table %s holds %s %d times, want once", m, f, got[f])
			}
		}
	}
}

func (r *batchRig) reportOneEach() {
	for _, m := range r.tables {
		r.report(m, r.newFile(m))
	}
}

// The appends of a batch run at once: every worker is inside before any is
// let go. With one at a time, never two.
func TestIngestBatch_AppendsOfSeveralTablesRunAtOnce(t *testing.T) {
	for _, tc := range []struct{ tables, concurrency, wantAtOnce int }{
		{8, 8, 8},
		{8, 3, 3},
		{4, 1, 1},
	} {
		t.Run(fmt.Sprintf("%d tables, %d at once", tc.tables, tc.concurrency), func(t *testing.T) {
			r := newBatchRig(t, tc.tables, tc.concurrency, true)
			appendsBefore := snapshotInt(t, "iceberg_ingest_appends")
			fallbacksBefore := snapshotInt(t, "iceberg_ingest_append_fallbacks")
			// A batch is let go only once it is full, so a scheduler that ran
			// fewer at once than asked would hang here, and time out.
			entered, hold := make(chan struct{}, tc.tables), make(chan struct{})
			r.src.mu.Lock()
			r.src.entered, r.src.hold, r.src.most = entered, hold, 0
			r.src.mu.Unlock()
			release := make(chan struct{})
			go func() {
				defer close(release)
				for done := 0; done < tc.tables; {
					n := min(tc.wantAtOnce, tc.tables-done)
					for i := 0; i < n; i++ {
						select {
						case <-entered:
						case <-time.After(10 * time.Second):
							t.Errorf("only %d of %d appends started together", i, n)
							close(hold)
							return
						}
					}
					hold <- struct{}{} // not closed: let exactly this batch go
					for i := 1; i < n; i++ {
						hold <- struct{}{}
					}
					done += n
				}
			}()
			r.reportOneEach()
			r.serve(2 * tc.tables)
			<-release
			r.src.mu.Lock()
			most := r.src.most
			r.src.entered, r.src.hold = nil, nil
			r.src.mu.Unlock()
			if most != tc.wantAtOnce {
				t.Errorf("%d appends at once, want %d", most, tc.wantAtOnce)
			}
			r.wantEachOnce()
			if got := snapshotInt(t, "iceberg_ingest_appends") - appendsBefore; got != int64(tc.tables) {
				t.Errorf("%d appends counted, want %d", got, tc.tables)
			}
			if got := snapshotInt(t, "iceberg_ingest_append_fallbacks") - fallbacksBefore; got != 0 {
				t.Errorf("%d appends fell back to a pass with several at once on one catalog connection", got)
			}
			for _, m := range r.tables {
				if got := r.src.passes(m); got != 1 {
					t.Errorf("table %s had %d passes, want only its first export", m, got)
				}
				if got := r.sched.appendsSince["mydb\x00"+m]; got != 1 {
					t.Errorf("table %s counts %d appends since its pass, want 1", m, got)
				}
			}
		})
	}
}

// Many rounds, several at once, on one catalog connection: every file once,
// nothing falls back.
func TestIngestBatch_ManyRoundsOnOneCatalogConnection(t *testing.T) {
	r := newBatchRig(t, 8, 8, true)
	fallbacksBefore := snapshotInt(t, "iceberg_ingest_append_fallbacks")
	for round := 0; round < 6; round++ {
		r.reportOneEach()
		r.serve(16)
	}
	r.wantEachOnce()
	if got := snapshotInt(t, "iceberg_ingest_append_fallbacks") - fallbacksBefore; got != 0 {
		t.Errorf("%d appends fell back to a pass", got)
	}
	for _, m := range r.tables {
		if got := r.src.passes(m); got != 1 {
			t.Errorf("table %s had %d passes, want only its first export", m, got)
		}
	}
}

// One table's append failing, or panicking, in a batch: that table runs a
// pass and ends right; the others' appends stand.
func TestIngestBatch_OneTableFailingDoesNotTouchTheOthers(t *testing.T) {
	for _, how := range []string{"an error", "a panic"} {
		t.Run(how, func(t *testing.T) {
			r := newBatchRig(t, 4, 4, true)
			bad := r.tables[2]
			r.src.mu.Lock()
			if how == "a panic" {
				r.src.panicFor = map[string]bool{bad: true}
			} else {
				r.src.failFor = map[string]error{bad: errors.New("injected append failure")}
			}
			r.src.mu.Unlock()
			fallbacksBefore := snapshotInt(t, "iceberg_ingest_append_fallbacks")
			r.reportOneEach()
			r.serve(8)
			r.wantEachOnce()
			if got := snapshotInt(t, "iceberg_ingest_append_fallbacks") - fallbacksBefore; got != 1 {
				t.Errorf("%d fallbacks, want 1", got)
			}
			for _, m := range r.tables {
				want := 1
				if m == bad {
					want = 2 // its first export and the pass after the failed append
				}
				if got := r.src.passes(m); got != want {
					t.Errorf("table %s had %d passes, want %d", m, got, want)
				}
			}
			if !r.sched.appendOK["mydb\x00"+bad] {
				t.Error("the table whose append failed does not take appends after the pass that settled it")
			}
		})
	}
}

// The service does not return while an append is running, whatever the
// context says: the caller's bookkeeping, and a shutdown that closes the
// catalog once the loop has exited, need every append finished.
func TestIngestBatch_TheServiceWaitsForEveryAppend(t *testing.T) {
	r := newBatchRig(t, 4, 4, true)
	entered, hold := make(chan struct{}, 4), make(chan struct{})
	r.src.mu.Lock()
	r.src.entered, r.src.hold = entered, hold
	r.src.mu.Unlock()
	r.reportOneEach()
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer close(returned)
		r.sched.serviceIngest(ctx, 8)
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("the batch did not start")
		}
	}
	cancel()
	select {
	case <-returned:
		t.Fatal("the service returned with appends still running")
	case <-time.After(150 * time.Millisecond):
	}
	close(hold)
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("the service did not return after its appends ended")
	}
	r.src.mu.Lock()
	inside := r.src.inside
	r.src.entered, r.src.hold = nil, nil
	r.src.mu.Unlock()
	if inside != 0 {
		t.Fatalf("%d appends still running after the service returned", inside)
	}
	// The appends were cut short by the cancelled context: each table is
	// marked for a pass, and a pass leaves every file in its table once.
	for _, m := range r.tables {
		if r.sched.appendOK["mydb\x00"+m] {
			t.Errorf("table %s still takes appends after one that did not finish", m)
		}
		if _, err := r.sched.reconcileOneRecovered(context.Background(), Measurement{Database: "mydb", Measurement: m}, "mydb\x00"+m); err != nil {
			t.Fatal(err)
		}
	}
	r.wantEachOnce()
}

// Between two measurements of a timed pass the service runs one batch of
// appends and at most one reconcile pass, however many are due.
func TestIngestBatch_BetweenMeasurementsOneBatchAndOnePass(t *testing.T) {
	t.Run("appends: one batch", func(t *testing.T) {
		r := newBatchRig(t, 6, 4, true)
		before := snapshotInt(t, "iceberg_ingest_appends")
		r.reportOneEach()
		r.serve(1)
		if got := snapshotInt(t, "iceberg_ingest_appends") - before; got != 4 {
			t.Fatalf("%d appends between two measurements, want one batch of 4", got)
		}
		r.serve(1)
		if got := snapshotInt(t, "iceberg_ingest_appends") - before; got != 6 {
			t.Fatalf("%d appends after the second service, want 6", got)
		}
		r.wantEachOnce()
	})
	t.Run("append off: one pass, as with one at a time", func(t *testing.T) {
		r := newBatchRig(t, 4, 4, false)
		passes := func() (n int) {
			for _, m := range r.tables {
				n += r.src.passes(m)
			}
			return n
		}
		// newBatchRig's check that tables take appends does not apply; the
		// tables were exported by passes.
		before := passes()
		r.reportOneEach()
		r.serve(1)
		if got := passes() - before; got != 1 {
			t.Fatalf("%d passes between two measurements with the append path off, want 1", got)
		}
	})
	t.Run("failed appends: one pass now, the rest due at once", func(t *testing.T) {
		r := newBatchRig(t, 4, 4, true)
		fail := errors.New("injected append failure")
		r.src.mu.Lock()
		r.src.failFor = map[string]error{r.tables[0]: fail, r.tables[1]: fail, r.tables[3]: fail}
		r.src.mu.Unlock()
		passesOf := func() (n int) {
			for _, m := range r.tables {
				n += r.src.passes(m)
			}
			return n
		}
		before := passesOf()
		r.reportOneEach()
		r.serve(1)
		if got := passesOf() - before; got != 1 {
			t.Fatalf("%d passes between two measurements after three appends failed, want 1", got)
		}
		// The bookkeeping of all three is done, though only one pass ran.
		for _, m := range []string{r.tables[0], r.tables[1], r.tables[3]} {
			if r.src.passes(m) == 1 && r.sched.appendOK["mydb\x00"+m] {
				t.Errorf("table %s still takes appends after its append failed and before its pass", m)
			}
		}
		r.sched.reqMu.Lock()
		due := len(r.sched.ingestDue)
		r.sched.reqMu.Unlock()
		if due != 2 {
			t.Fatalf("%d requests waiting after the service, want the 2 whose pass did not run", due)
		}
		// They are taken at once by the next services, without waiting out
		// the spacing, and every table ends with its files once.
		r.sched.serviceIngest(context.Background(), 1)
		r.sched.serviceIngest(context.Background(), 1)
		if got := passesOf() - before; got != 3 {
			t.Fatalf("%d passes after two more services, want 3", got)
		}
		r.wantEachOnce()
	})
}

// A file reported while a batch is running is not lost and not added twice:
// it is appended by the table's next export.
func TestIngestBatch_AFileReportedDuringABatchIsAppendedOnce(t *testing.T) {
	r := newBatchRig(t, 4, 4, true)
	late := r.newFile(r.tables[1])
	var once sync.Once
	r.src.mu.Lock()
	r.src.onEnter = func(m Measurement) {
		if m.Measurement == r.tables[1] {
			once.Do(func() { r.report(r.tables[1], late) })
		}
	}
	r.src.mu.Unlock()
	r.reportOneEach()
	r.serve(8)
	r.src.mu.Lock()
	r.src.onEnter = nil
	r.src.mu.Unlock()
	if got := r.entries(r.tables[1])[late]; got != 0 {
		t.Fatalf("the file reported during the batch is in the table %d times before its own export", got)
	}
	r.serve(8)
	r.wantEachOnce()
	for _, m := range r.tables {
		if got := r.src.passes(m); got != 1 {
			t.Errorf("table %s had %d passes, want only its first export", m, got)
		}
	}
}

// Each append of a batch has its own exporter state: one that cannot publish
// (its gate closes) does not mark the others, which publish.
func TestIngestBatch_EachAppendHasItsOwnExporterState(t *testing.T) {
	e := newBatchRig(t, 1, 1, true).exp
	a, b := e.forAppend(), e.forAppend()
	if a.exporterShared != e.exporterShared || b.exporterShared != e.exporterShared {
		t.Fatal("an append's exporter does not share the process's state")
	}
	if a.passLoaded != -1 || a.passHint != -1 {
		t.Fatalf("an append starts from loaded=%d hint=%d, want -1, -1 (not known)", a.passLoaded, a.passHint)
	}
	a.setPassGate(func() bool { return false })
	a.noteHintFailure()
	a.passLag = time.Minute
	if b.hintFailed() || e.hintFailed() {
		t.Error("one append's failed publish is seen by another export")
	}
	if b.passGate != nil || e.passGate != nil {
		t.Error("one append's gate is seen by another export")
	}
	if b.passLag != 0 {
		t.Error("one append's lag is seen by another export")
	}
	// What is shared is shared: the count the next append of a table reads.
	a.setSmallManifests("mydb\x00m00", 3)
	if n, ok := b.smallManifests("mydb\x00m00"); !ok || n != 3 {
		t.Errorf("the small-manifest count set through one append reads %d, %v through another", n, ok)
	}
}

// An append that does not come back holds its batch only for the append's
// own deadline, then runs a pass with a budget of its own.
func TestIngestBatch_AWedgedAppendIsCutOffAndRunsAPass(t *testing.T) {
	old := appendTimeout
	appendTimeout = 100 * time.Millisecond
	t.Cleanup(func() { appendTimeout = old })
	r := newBatchRig(t, 4, 4, true)
	stuck := r.tables[0]
	r.src.mu.Lock()
	r.src.wedgeFor = map[string]bool{stuck: true}
	r.src.mu.Unlock()
	r.reportOneEach()
	began := time.Now()
	r.serve(8)
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("the service took %v with one wedged append: it waited on the pass's budget, not the append's", took)
	}
	r.wantEachOnce()
	for _, m := range r.tables {
		want := 1
		if m == stuck {
			want = 2
		}
		if got := r.src.passes(m); got != want {
			t.Errorf("table %s had %d passes, want %d", m, got, want)
		}
	}
}
