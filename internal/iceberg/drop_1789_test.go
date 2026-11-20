package iceberg

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// #1789: a database's tables are not exported and dropped at once.

func TestDatabaseLock_ExportsShareItAndADropHoldsItAlone(t *testing.T) {
	e := newPassRig(t, 1, 1, 1).exp
	ctx := context.Background()
	// Exports of one database, and of others, do not wait for each other.
	r1, err := e.holdDatabase(ctx, "db")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e.holdDatabase(ctx, "db")
	if err != nil {
		t.Fatal(err)
	}
	// A drop of it waits for both.
	got := make(chan func(), 1)
	go func() {
		rel, err := e.lockDatabaseForDrop(ctx, "db")
		if err != nil {
			t.Error(err)
		}
		got <- rel
	}()
	select {
	case <-got:
		t.Fatal("a drop got the database while two exports of it were running")
	case <-time.After(50 * time.Millisecond):
	}
	// Another database is not held up by any of it.
	if rel, err := e.lockDatabaseForDrop(ctx, "other"); err != nil {
		t.Fatalf("a drop of another database: %v", err)
	} else {
		rel()
	}
	r1()
	select {
	case <-got:
		t.Fatal("a drop got the database with one export of it still running")
	case <-time.After(50 * time.Millisecond):
	}
	r2()
	var release func()
	select {
	case release = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("a drop did not get the database once its exports had ended")
	}
	// While it is dropped, an export of it waits, for as long as it is given.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := e.holdDatabase(short, "db"); err == nil {
		t.Fatal("an export started on a database that was being dropped")
	}
	held := make(chan func(), 1)
	go func() {
		rel, err := e.holdDatabase(ctx, "db")
		if err != nil {
			t.Error(err)
		}
		held <- rel
	}()
	release()
	select {
	case rel := <-held:
		rel()
	case <-time.After(5 * time.Second):
		t.Fatal("an export did not start once the drop had ended")
	}
}

// On an edge-sync hub a DELETE drops a whole spoke: the exports of its
// "{spoke}/{db}" databases hold the spoke's lock.
func TestDatabaseLock_ASpokesDatabasesShareTheSpokesLock(t *testing.T) {
	e := newPassRig(t, 1, 1, 1).exp
	ctx := context.Background()
	for database, want := range map[string]string{"db": "db", "spoke/db": "spoke", "spoke/other": "spoke"} {
		if got := databaseLockKey(database); got != want {
			t.Errorf("databaseLockKey(%q) = %q, want %q", database, got, want)
		}
	}
	rel, err := e.holdDatabase(ctx, "spoke/db")
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := e.lockDatabaseForDrop(short, "spoke"); err == nil {
		t.Fatal("a spoke was dropped while one of its databases was being exported")
	}
	rel()
	if rel, err := e.lockDatabaseForDrop(ctx, "spoke"); err != nil {
		t.Fatal(err)
	} else {
		rel()
	}
}

// A drop that cannot start because an export of the database does not end
// drops nothing, says so, and leaves the database as it was.
func TestDrop1789_ADropThatCannotStartDropsNothing(t *testing.T) {
	old := dropLockWait
	dropLockWait = 100 * time.Millisecond
	t.Cleanup(func() { dropLockWait = old })
	r := newPassRig(t, 2, 1, 1)
	r.sched.runPass(context.Background())
	rel, err := r.exp.holdDatabase(context.Background(), "mydb") // an export that does not end
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	err = r.exp.DropDatabase(context.Background(), "mydb")
	if err == nil || !strings.Contains(err.Error(), "repeat the DELETE") {
		t.Fatalf("DropDatabase with an export running: %v, want it refused with what to do", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the refusal does not say it ran out of time: %v", err)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("the drop waited %v, want about %v", took, dropLockWait)
	}
	r.wantEachOnce() // nothing was dropped
	rel()
	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatalf("the DELETE repeated: %v", err)
	}
	for _, m := range r.tables {
		if exists, err := r.exp.tableExists(context.Background(), "mydb", m); err != nil || exists {
			t.Errorf("table %s after the drop: exists=%v err=%v", m, exists, err)
		}
	}
}

// The drop waits for the pass that is running, and the pass ends whole.
func TestDrop1789_ADropWaitsForThePassInProgress(t *testing.T) {
	r := newPassRig(t, 2, 2, 1)
	r.sched.runPass(context.Background())
	for _, m := range r.tables {
		r.newFile(m)
	}
	entered, hold := make(chan struct{}, 2), make(chan struct{})
	r.src.mu.Lock()
	r.src.entered, r.src.hold = entered, hold
	r.src.mu.Unlock()
	passDone := make(chan struct{})
	go func() { defer close(passDone); r.sched.runPass(context.Background()) }()
	<-entered
	<-entered // both passes are inside, holding the database
	dropped := make(chan error, 1)
	go func() { dropped <- r.exp.DropDatabase(context.Background(), "mydb") }()
	select {
	case err := <-dropped:
		t.Fatalf("the drop returned (%v) while passes of the database's tables were running", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(hold)
	select {
	case err := <-dropped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the drop did not run once the passes had ended")
	}
	<-passDone
	r.src.mu.Lock()
	r.src.entered, r.src.hold = nil, nil
	r.src.mu.Unlock()
	for _, m := range r.tables {
		if exists, err := r.exp.tableExists(context.Background(), "mydb", m); err != nil || exists {
			t.Errorf("table %s after the drop: exists=%v err=%v", m, exists, err)
		}
	}
}

// After a drop the scheduler forgets what it remembered of the database's
// tables, and of no other database's; the exporter forgets its own.
func TestDrop1789_TheSchedulerAndTheExporterForgetTheDatabase(t *testing.T) {
	r := newPassRig(t, 2, 1, 1)
	r.sched.runPass(context.Background())
	for _, m := range r.tables {
		r.report(m, r.newFile(m))
	}
	time.Sleep(5 * time.Millisecond)
	r.sched.serviceIngest(context.Background(), ingestBatch) // appends: the exporter counts manifests
	key := "mydb\x00" + r.tables[0]
	// Something of another database, which must stay.
	r.sched.state["otherdb\x00cpu"] = measurementState{fingerprint: "kept"}
	r.sched.appendOK["otherdb\x00cpu"] = true
	r.exp.setSmallManifests("otherdb\x00cpu", 2)
	r.exp.markPublished(r.exp.tableIdent("otherdb", "cpu"), "kept")
	if _, ok := r.exp.smallManifests(key); !ok {
		t.Fatal("no manifest count for the table before the drop: the test's premise")
	}
	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	// The exporter's memory, at once.
	if _, ok := r.exp.smallManifests(key); ok {
		t.Error("the exporter kept the dropped table's manifest count")
	}
	r.exp.publishedMu.Lock()
	for k := range r.exp.published {
		if strings.HasPrefix(k, "arc_mydb\x00") {
			t.Errorf("the exporter still counts %q as published here", k)
		}
	}
	keptPublished := len(r.exp.published)
	r.exp.publishedMu.Unlock()
	if keptPublished != 1 {
		t.Errorf("%d tables counted as published after the drop, want the other database's 1", keptPublished)
	}
	if n, ok := r.exp.smallManifests("otherdb\x00cpu"); !ok || n != 2 {
		t.Error("the exporter forgot another database's manifest count")
	}
	// The scheduler's, at its next service of drops; it was told.
	if !r.sched.otherRequestsQueued() {
		t.Fatal("the scheduler was not told of the drop")
	}
	r.sched.serviceDrops(context.Background())
	for _, m := range r.tables {
		k := "mydb\x00" + m
		if _, ok := r.sched.state[k]; ok {
			t.Errorf("the scheduler kept the cached state of dropped table %s", m)
		}
		if r.sched.appendOK[k] {
			t.Errorf("dropped table %s still takes appends", m)
		}
		if len(r.sched.recentAdds[k]) != 0 {
			t.Errorf("the scheduler kept the recent files of dropped table %s", m)
		}
	}
	if r.sched.state["otherdb\x00cpu"].fingerprint != "kept" || !r.sched.appendOK["otherdb\x00cpu"] {
		t.Error("the scheduler forgot another database")
	}
	if r.sched.otherRequestsQueued() {
		t.Error("the drop is still waiting to be forgotten after a service of drops")
	}
}

// Appends of the database's tables in flight are waited for as passes are,
// and a database written again right after is exported whole.
func TestDrop1789_ADeleteDuringAppendsLeavesARecreatedDatabaseRight(t *testing.T) {
	r := newPassRig(t, 4, 2, 4)
	r.sched.runPass(context.Background())
	for _, m := range r.tables {
		r.report(m, r.newFile(m))
	}
	time.Sleep(5 * time.Millisecond)
	src := &dropDuringAppends{passSource: r.src}
	var once sync.Once
	dropped := make(chan error, 1)
	src.onAppend = func() {
		once.Do(func() { go func() { dropped <- r.exp.DropDatabase(context.Background(), "mydb") }() })
		time.Sleep(20 * time.Millisecond) // the drop is asked for while the appends run
	}
	r.sched.source = src
	r.sched.serviceIngest(context.Background(), ingestBatch)
	select {
	case err := <-dropped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the drop did not return")
	}
	r.sched.source = r.src
	// The tables are gone; the files are still there (only the tables were
	// dropped), and the next passes export them whole.
	r.sched.runPass(context.Background())
	r.sched.runPass(context.Background())
	r.wantEachOnce()
}

type dropDuringAppends struct {
	*passSource
	onAppend func()
}

func (d *dropDuringAppends) AppendRefs(ctx context.Context, m Measurement, keys []string, sizes map[string]int64) ([]FileRef, bool, error) {
	d.onAppend()
	return d.passSource.AppendRefs(ctx, m, keys, sizes)
}

// The drop waits for the append that is running, as it does for a pass.
func TestDrop1789_ADropWaitsForTheAppendInProgress(t *testing.T) {
	r := newPassRig(t, 2, 1, 2)
	r.sched.runPass(context.Background())
	for _, m := range r.tables {
		r.report(m, r.newFile(m))
	}
	time.Sleep(5 * time.Millisecond)
	entered, hold := make(chan struct{}, 2), make(chan struct{})
	src := &dropDuringAppends{passSource: r.src}
	src.onAppend = func() {
		entered <- struct{}{}
		<-hold
	}
	r.sched.source = src
	served := make(chan struct{})
	go func() { defer close(served); r.sched.serviceIngest(context.Background(), ingestBatch) }()
	<-entered
	<-entered // both appends are running, holding the database
	dropped := make(chan error, 1)
	go func() { dropped <- r.exp.DropDatabase(context.Background(), "mydb") }()
	select {
	case err := <-dropped:
		t.Fatalf("the drop returned (%v) while appends to the database's tables were running", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(hold)
	select {
	case err := <-dropped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the drop did not run once the appends had ended")
	}
	<-served
	for _, m := range r.tables {
		if exists, err := r.exp.tableExists(context.Background(), "mydb", m); err != nil || exists {
			t.Errorf("table %s after the drop: exists=%v err=%v", m, exists, err)
		}
	}
}
