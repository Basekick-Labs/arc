package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1786: reconcile passes of several tables at once.

// passSource is the storage source with a hook on a pass's first call, its
// listing, which is where a batch's workers are observed and steered.
type passSource struct {
	*StorageWalkSource
	mu        sync.Mutex
	listed    map[string]int
	inside    int
	most      int
	entered   chan struct{}
	hold      chan struct{}
	failFor   map[string]error
	panicFor  map[string]bool
	onList    func(m Measurement)
	onListCtx func(ctx context.Context, m Measurement)
	coldGone  []string // databases whose cold listing was invalidated
}

func (p *passSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	p.mu.Lock()
	p.listed[m.Measurement]++
	p.inside++
	if p.inside > p.most {
		p.most = p.inside
	}
	entered, hold, onList, onListCtx := p.entered, p.hold, p.onList, p.onListCtx
	fail, panics := p.failFor[m.Measurement], p.panicFor[m.Measurement]
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.inside--
		p.mu.Unlock()
	}()
	if onList != nil {
		onList(m)
	}
	if onListCtx != nil {
		onListCtx(ctx, m)
	}
	if entered != nil {
		entered <- struct{}{}
	}
	if hold != nil {
		<-hold
	}
	if panics {
		panic("injected panic in a reconcile pass")
	}
	if fail != nil {
		return nil, nil, fail
	}
	return p.StorageWalkSource.FilesAndLocal(ctx, m)
}

func (p *passSource) InvalidateCold(database string) {
	p.mu.Lock()
	p.coldGone = append(p.coldGone, database)
	p.mu.Unlock()
}

func (p *passSource) passes(measurement string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.listed[measurement]
}

// passRig is n measurements of one database on local storage, each with one
// file and no table yet, and a scheduler that runs `passes` reconcile passes
// and `appends` appends at once. The catalog database is opened as
// production opens it: WAL, a busy timeout and ONE connection.
type passRig struct {
	t      *testing.T
	root   string
	exp    *Exporter
	src    *passSource
	sched  *Scheduler
	tables []string
	files  map[string][]string
	seq    int
}

func newPassRig(t *testing.T, n, passes, appends int) *passRig {
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
	src := &passSource{StorageWalkSource: NewStorageWalkSource(be, "arc", zerolog.Nop()), listed: map[string]int{}}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Interval: time.Hour, Logger: zerolog.Nop(),
		IngestInterval: time.Millisecond, IngestAppend: true, IngestConcurrency: appends, ReconcileConcurrency: passes})
	sched.started = time.Now().Add(-time.Second)
	t.Cleanup(func() { metrics.Get().SetIcebergOldestPendingExport(time.Time{}) })
	r := &passRig{t: t, root: root, exp: exp, src: src, sched: sched, files: map[string][]string{}}
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("p%02d", i)
		r.tables = append(r.tables, m)
		r.newFile(m)
	}
	return r
}

func (r *passRig) path(measurement, name string) string {
	return filepath.Join(r.root, "mydb", measurement, "2026", "07", "14", "15", name)
}

func (r *passRig) newFile(measurement string) string {
	r.t.Helper()
	r.seq++
	name := ingestName(measurement, time.Now().Add(time.Duration(r.seq)*time.Microsecond))
	writeArcStyleParquet(r.t, r.path(measurement, name), 1_700_000_000_000_000, 5)
	r.files[measurement] = append(r.files[measurement], name)
	return name
}

func (r *passRig) report(measurement, name string) {
	r.sched.RequestIngestAppend("mydb", measurement, "mydb/"+measurement+"/2026/07/14/15/"+name, 0)
}

func (r *passRig) entries(measurement string) map[string]int {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", measurement))
	if err != nil {
		r.t.Fatalf("table %s: %v", measurement, err)
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

func (r *passRig) wantEachOnce(tables ...string) {
	r.t.Helper()
	if len(tables) == 0 {
		tables = r.tables
	}
	for _, m := range tables {
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

// gate lets groups of `atOnce` listings go, each only once it is full: a
// scheduler that ran fewer at once than asked would stop here.
func (r *passRig) gate(total, atOnce int) (done chan struct{}) {
	entered, hold := make(chan struct{}, total), make(chan struct{})
	r.src.mu.Lock()
	r.src.entered, r.src.hold, r.src.most = entered, hold, 0
	r.src.mu.Unlock()
	done = make(chan struct{})
	go func() {
		defer close(done)
		for served := 0; served < total; {
			n := min(atOnce, total-served)
			for i := 0; i < n; i++ {
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					r.t.Errorf("only %d of %d passes started together", i, n)
					close(hold)
					return
				}
			}
			for i := 0; i < n; i++ {
				hold <- struct{}{}
			}
			served += n
		}
	}()
	return done
}

func (r *passRig) ungate() int {
	r.src.mu.Lock()
	defer r.src.mu.Unlock()
	r.src.entered, r.src.hold = nil, nil
	return r.src.most
}

// The timed pass reconciles several tables at once: every pass of a group is
// inside before any is let go. With one at a time, never two. The tables are
// new and in one database, so their creation, and the namespace's, overlap.
func TestPassBatch_TheTimedPassReconcilesSeveralTablesAtOnce(t *testing.T) {
	for _, tc := range []struct{ tables, passes, wantAtOnce int }{
		{8, 4, 4},
		{6, 6, 6},
		{4, 1, 1},
	} {
		t.Run(fmt.Sprintf("%d tables, %d at once", tc.tables, tc.passes), func(t *testing.T) {
			r := newPassRig(t, tc.tables, tc.passes, 1)
			done := r.gate(tc.tables, tc.wantAtOnce)
			r.sched.runPass(context.Background())
			<-done
			if most := r.ungate(); most != tc.wantAtOnce {
				t.Errorf("%d passes at once, want %d", most, tc.wantAtOnce)
			}
			r.wantEachOnce()
			for _, m := range r.tables {
				key := "mydb\x00" + m
				if _, settled := r.sched.state[key]; !settled {
					t.Errorf("table %s has no cached state after a pass that settled", m)
				}
				if !r.sched.appendOK[key] {
					t.Errorf("table %s does not take appends after a pass that settled", m)
				}
				if r.sched.lastReconciled[key].IsZero() {
					t.Errorf("table %s has no time of last change", m)
				}
				if got := r.src.passes(m); got != 1 {
					t.Errorf("table %s was listed %d times, want 1", m, got)
				}
			}
			// A second pass finds every table as it left it.
			r.sched.runPass(context.Background())
			r.wantEachOnce()
		})
	}
}

// One table failing, or panicking, in a batch: that table is counted failed
// and keeps the state it had; the others are reconciled and remembered.
func TestPassBatch_OneTableFailingDoesNotTouchTheOthers(t *testing.T) {
	for _, how := range []string{"an error", "a panic"} {
		t.Run(how, func(t *testing.T) {
			r := newPassRig(t, 4, 4, 1)
			r.sched.runPass(context.Background()) // all four exported
			bad := r.tables[1]
			before := r.sched.state["mydb\x00"+bad].fingerprint
			for _, m := range r.tables {
				r.newFile(m)
			}
			r.src.mu.Lock()
			if how == "a panic" {
				r.src.panicFor = map[string]bool{bad: true}
			} else {
				r.src.failFor = map[string]error{bad: errors.New("injected listing failure")}
			}
			r.src.mu.Unlock()
			r.sched.runPass(context.Background())
			for _, m := range r.tables {
				if m == bad {
					continue
				}
				r.wantEachOnce(m)
			}
			if got := len(r.entries(bad)); got != 1 {
				t.Fatalf("the table whose pass failed has %d files, want the 1 it had", got)
			}
			if got := r.sched.state["mydb\x00"+bad].fingerprint; got != before {
				t.Errorf("the table whose pass failed lost or changed its cached state (%q, was %q)", got, before)
			}
			// Healed by the next pass.
			r.src.mu.Lock()
			r.src.failFor, r.src.panicFor = nil, nil
			r.src.mu.Unlock()
			r.sched.runPass(context.Background())
			r.wantEachOnce()
		})
	}
}

// What a pass starts from is what the loop left right before it ran. An
// append served between two groups marks its table so that the table's pass
// runs whatever its listing is; a pass that started from a copy taken before
// that append would find the listing unchanged and skip, and a file the
// append added that has since left storage would stay in the table.
func TestPassBatch_APassStartsFromWhatTheLoopLeftRightBeforeIt(t *testing.T) {
	r := newPassRig(t, 4, 4, 2)
	r.sched.runPass(context.Background())
	r.wantEachOnce()
	// A new file of the first table, reported: the timed pass serves the
	// append before the group that holds the table.
	target := r.tables[0]
	late := r.newFile(target)
	r.report(target, late)
	time.Sleep(5 * time.Millisecond)
	// The file leaves storage after the append and before the pass lists.
	var once sync.Once
	r.src.mu.Lock()
	r.src.onList = func(m Measurement) {
		if m.Measurement == target {
			once.Do(func() {
				if got := r.entries(target)[late]; got != 1 {
					t.Errorf("the append did not run before the table's pass (the file is in the table %d times)", got)
				}
				if err := os.Remove(r.path(target, late)); err != nil {
					t.Error(err)
				}
			})
		}
	}
	r.src.mu.Unlock()
	r.sched.runPass(context.Background())
	if got := r.entries(target)[late]; got != 0 {
		t.Fatalf("a file that left storage is still in the table after a timed pass: the pass started from what the scheduler remembered before the append")
	}
}

// The passes an ingest batch leaves to run (here: appends that failed) run
// at once too.
func TestPassBatch_ThePassesAfterAnIngestBatchRunAtOnce(t *testing.T) {
	r := newPassRig(t, 4, 4, 4)
	r.sched.runPass(context.Background())
	fail := errors.New("injected append failure")
	src := &failingAppends{passSource: r.src, fail: fail}
	r.sched.source = src
	for _, m := range r.tables {
		r.report(m, r.newFile(m))
	}
	time.Sleep(5 * time.Millisecond)
	done := r.gate(4, 4)
	r.sched.serviceIngest(context.Background(), ingestBatch)
	<-done
	if most := r.ungate(); most != 4 {
		t.Errorf("%d fallback passes at once, want 4", most)
	}
	r.wantEachOnce()
	for _, m := range r.tables {
		if !r.sched.appendOK["mydb\x00"+m] {
			t.Errorf("table %s does not take appends after the pass that followed its failed append", m)
		}
	}
	// The passes covered the files reported before them: nothing waits.
	r.sched.reqMu.Lock()
	waiting, due := len(r.sched.ingestPending), len(r.sched.ingestDue)
	r.sched.reqMu.Unlock()
	if waiting != 0 || due != 0 {
		t.Errorf("%d tables with files counted as waiting and %d requests queued after passes that settled", waiting, due)
	}
}

// Between two measurements of a timed pass one ingest request may run a
// reconcile pass, however many passes may run at once.
func TestPassBatch_BetweenMeasurementsStillOneIngestPass(t *testing.T) {
	r := newPassRig(t, 4, 4, 4)
	r.sched.runPass(context.Background())
	r.sched.source = &failingAppends{passSource: r.src, fail: errors.New("injected append failure")}
	passes := func() (n int) {
		for _, m := range r.tables {
			n += r.src.passes(m)
		}
		return n
	}
	before := passes()
	for _, m := range r.tables {
		r.report(m, r.newFile(m))
	}
	time.Sleep(5 * time.Millisecond)
	r.sched.serviceIngest(context.Background(), 1)
	if got := passes() - before; got != 1 {
		t.Fatalf("%d passes between two measurements after four appends failed, want 1", got)
	}
	r.sched.reqMu.Lock()
	due := len(r.sched.ingestDue)
	r.sched.reqMu.Unlock()
	if due != 3 {
		t.Fatalf("%d requests waiting, want the 3 whose pass did not run", due)
	}
}

type failingAppends struct {
	*passSource
	fail error
}

func (f *failingAppends) AppendRefs(context.Context, Measurement, []string, map[string]int64) ([]FileRef, bool, error) {
	return nil, false, f.fail
}

// The loop does not go on while a pass is running, whatever the context says.
func TestPassBatch_TheLoopWaitsForEveryPass(t *testing.T) {
	r := newPassRig(t, 4, 4, 1)
	entered, hold := make(chan struct{}, 4), make(chan struct{})
	r.src.mu.Lock()
	r.src.entered, r.src.hold = entered, hold
	r.src.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		r.sched.runPass(ctx)
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
		t.Fatal("the timed pass returned with passes still running")
	case <-time.After(150 * time.Millisecond):
	}
	close(hold)
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("the timed pass did not return after its passes ended")
	}
	r.src.mu.Lock()
	inside := r.src.inside
	r.src.entered, r.src.hold = nil, nil
	r.src.mu.Unlock()
	if inside != 0 {
		t.Fatalf("%d passes still running after the timed pass returned", inside)
	}
}

// A cold listing a pass found stale is invalidated by the loop, after the
// batch, not by the pass: another pass of the same database may be listing
// the cold tier at that moment and would write its stale listing back.
func TestPassBatch_AStaleColdListingIsInvalidatedByTheLoop(t *testing.T) {
	r := newPassRig(t, 1, 4, 1)
	job := r.sched.preparePass(Measurement{Database: "mydb", Measurement: r.tables[0]}, "mydb\x00"+r.tables[0])
	job.droppedCold, job.commits = true, 2
	r.sched.finishPass(job)
	r.src.mu.Lock()
	gone := append([]string(nil), r.src.coldGone...)
	r.src.mu.Unlock()
	if len(gone) != 1 || gone[0] != "mydb" {
		t.Fatalf("cold listings invalidated: %v, want mydb once", gone)
	}
	if r.sched.passCommits != 2 {
		t.Errorf("passCommits = %d after finishing a pass that committed twice", r.sched.passCommits)
	}
}

// What a pass decided about its table's cached state: set, dropped, or left.
func TestPassBatch_FinishAppliesTheStateThePassDecided(t *testing.T) {
	r := newPassRig(t, 3, 4, 1)
	keyOf := func(i int) string { return "mydb\x00" + r.tables[i] }
	for i := range r.tables {
		r.sched.state[keyOf(i)] = measurementState{fingerprint: "old"}
	}
	jobs := make([]*passJob, 3)
	for i := range jobs {
		jobs[i] = r.sched.preparePass(Measurement{Database: "mydb", Measurement: r.tables[i]}, keyOf(i))
		if !jobs[i].cached || jobs[i].prev.fingerprint != "old" {
			t.Fatalf("job %d started from %+v, cached=%v", i, jobs[i].prev, jobs[i].cached)
		}
	}
	jobs[0].setState(measurementState{fingerprint: "new"})
	jobs[0].appended = true
	jobs[1].dropState()
	// jobs[2] decides nothing (a listing error keeps what was cached).
	for _, job := range jobs {
		r.sched.finishPass(job)
	}
	if got := r.sched.state[keyOf(0)].fingerprint; got != appendedFingerprint {
		t.Errorf("a pass that set its state and asked for the next pass to run left fingerprint %q", got)
	}
	if _, cached := r.sched.state[keyOf(1)]; cached {
		t.Error("a pass that dropped its state left it cached")
	}
	if got := r.sched.state[keyOf(2)].fingerprint; got != "old" {
		t.Errorf("a pass that decided nothing left fingerprint %q, want the one it had", got)
	}
}

// A table twice in one batch is never two exports of it at once: the repeat
// runs after the batch.
func TestPassBatch_ATableTwiceInABatchRunsOneAfterTheOther(t *testing.T) {
	r := newPassRig(t, 2, 4, 1)
	a := Measurement{Database: "mydb", Measurement: r.tables[0]}
	b := Measurement{Database: "mydb", Measurement: r.tables[1]}
	var mu sync.Mutex
	insideA, mostA := 0, 0
	r.src.mu.Lock()
	r.src.onListCtx = func(_ context.Context, m Measurement) {
		if m.Measurement != a.Measurement {
			return
		}
		mu.Lock()
		insideA++
		mostA = max(mostA, insideA)
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		insideA--
		mu.Unlock()
	}
	r.src.mu.Unlock()
	jobs := []*passJob{
		r.sched.preparePass(a, "mydb\x00"+a.Measurement),
		r.sched.preparePass(b, "mydb\x00"+b.Measurement),
		r.sched.preparePass(a, "mydb\x00"+a.Measurement),
	}
	r.sched.runPasses(context.Background(), jobs)
	if mostA != 1 {
		t.Fatalf("%d passes of one table at once", mostA)
	}
	if got := r.src.passes(a.Measurement); got != 2 {
		t.Fatalf("the table that was twice in the batch was listed %d times, want 2", got)
	}
	for _, job := range jobs {
		if job.err != nil {
			t.Fatalf("pass of %s: %v", job.key, job.err)
		}
		r.sched.finishPass(job)
	}
	r.wantEachOnce()
}

// The passes of a batch share their storage reads and the footers they
// hold; a pass on the loop goroutine has neither limit beyond its own.
func TestPassBatch_TheBatchSharesItsReadsAndItsFooters(t *testing.T) {
	r := newPassRig(t, 3, 4, 1)
	var mu sync.Mutex
	var seen []*batchReads
	r.src.mu.Lock()
	r.src.onListCtx = func(ctx context.Context, _ Measurement) {
		mu.Lock()
		defer mu.Unlock()
		if er := exportReadsFrom(ctx); er != nil {
			seen = append(seen, er.batch)
		} else {
			seen = append(seen, nil)
		}
	}
	r.src.mu.Unlock()
	r.sched.runPass(context.Background())
	if len(seen) != 3 || seen[0] == nil || seen[0] != seen[1] || seen[1] != seen[2] {
		t.Fatalf("the three passes of one batch saw budgets %v, want one shared by all", seen)
	}
	if got := cap(seen[0].gate); got != batchReadsAtOnce {
		t.Errorf("the batch may make %d storage reads at once, want %d", got, batchReadsAtOnce)
	}
	// One table at a time: no shared budget.
	seen = nil
	r.sched.passConcurrency = 1
	r.newFile(r.tables[0])
	r.sched.runPass(context.Background())
	for _, b := range seen {
		if b != nil {
			t.Fatal("a pass that ran alone was given a batch's budget")
		}
	}
}

// The footers the exports of a batch hold together are bounded by what one
// export alone may hold, and a tail taken gives its bytes back.
func TestExportReads_ABatchSharesOneFooterBudget(t *testing.T) {
	shared := newBatchReads()
	a := exportReadsFrom(withBatchReads(context.Background(), shared))
	b := exportReadsFrom(withBatchReads(context.Background(), shared))
	big := make([]byte, exportReadsMaxTailBytes-10)
	a.putTail("a", fileTail{data: big})
	b.putTail("b", fileTail{data: make([]byte, 11)}) // 1 over, together
	if _, ok := b.takeTail("b"); ok {
		t.Error("a tail past the batch's budget was kept")
	}
	b.putTail("c", fileTail{data: make([]byte, 10)})
	if _, ok := b.takeTail("c"); !ok {
		t.Error("a tail that fits the batch's budget was not kept")
	}
	if _, ok := a.takeTail("a"); !ok {
		t.Fatal("the first tail was not kept")
	}
	if shared.tailBytes != 0 {
		t.Errorf("%d bytes still counted against the batch after every tail was taken", shared.tailBytes)
	}
	// A read waits for one of the batch's slots and gives it back.
	ctx := withBatchReads(context.Background(), shared)
	releases := make([]func(), 0, batchReadsAtOnce)
	for i := 0; i < batchReadsAtOnce; i++ {
		releases = append(releases, acquireRead(ctx))
	}
	blocked := make(chan struct{})
	go func() { acquireRead(ctx)(); close(blocked) }()
	select {
	case <-blocked:
		t.Fatal("a read past the batch's limit did not wait")
	case <-time.After(50 * time.Millisecond):
	}
	releases[0]()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("a waiting read did not get the slot that was released")
	}
	for _, rel := range releases[1:] {
		rel()
	}
	acquireRead(context.Background())() // outside a batch: no wait
}

// A database deleted through the API (the exporter's DropDatabase, on the
// request's goroutine) while passes of its tables run at once: no race, no
// panic, and the next pass leaves every table that still has files right.
func TestPassBatch_ADatabaseDroppedWhileItsTablesReconcile(t *testing.T) {
	r := newPassRig(t, 6, 6, 1)
	r.sched.runPass(context.Background())
	for _, m := range r.tables {
		r.newFile(m)
	}
	var once sync.Once
	dropped := make(chan error, 1)
	r.src.mu.Lock()
	r.src.onListCtx = func(context.Context, Measurement) {
		once.Do(func() {
			go func() { dropped <- r.exp.DropDatabase(context.Background(), "mydb") }()
		})
	}
	r.src.mu.Unlock()
	r.sched.runPass(context.Background())
	select {
	case <-dropped: // whatever it returned: the passes and the drop did not corrupt each other
	case <-time.After(30 * time.Second):
		t.Fatal("the drop did not return")
	}
	r.src.mu.Lock()
	r.src.onListCtx = nil
	r.src.mu.Unlock()
	// The files are still in storage (only the tables were dropped): two
	// more passes bring every table back with its files once.
	r.sched.runPass(context.Background())
	r.sched.runPass(context.Background())
	r.wantEachOnce()
}
