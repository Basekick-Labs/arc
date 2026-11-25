package iceberg

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1722: a compaction's window, where its output and its inputs are both in
// storage, and the reconcile a finished compaction asks for.

// replacingExclusions is a fake api.ReplacementExcluder: the inputs it leaves
// out and the outputs they belong to, as compaction.OpenJobs reports them.
type replacingExclusions struct {
	keys, outputs []string
}

func (r *replacingExclusions) under(list []string, prefix string) []string {
	var out []string
	for _, k := range list {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

func (r *replacingExclusions) ExcludedForStrict(_ context.Context, prefix string) ([]string, error) {
	return r.under(r.keys, prefix), nil
}

func (r *replacingExclusions) ExcludedWithOutputsStrict(_ context.Context, prefix string) ([]string, []string, error) {
	return r.under(r.keys, prefix), r.under(r.outputs, prefix), nil
}

const (
	winIn1 = hourDir + "cpu_in1.parquet"
	winIn2 = hourDir + "cpu_in2.parquet"
	winOut = hourDir + "cpu_compacted.parquet"
)

func pendingOf(keys ...string) func(context.Context, string) (map[string]struct{}, error) {
	return func(context.Context, string) (map[string]struct{}, error) {
		out := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			out[k] = struct{}{}
		}
		return out, nil
	}
}

// In the window the export must hold the job's rows exactly once: the output
// when the job that leaves its inputs out names it, the inputs otherwise.
func TestFilesAndLocal_CompactionWindowKeepsTheJobsRowsOnce(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		ex   Exclusions
		want []string
	}{
		{"no exclusions: the inputs, output pending", nil, []string{"cpu_in1.parquet", "cpu_in2.parquet"}},
		{"the job leaves its inputs out and names its output", &replacingExclusions{keys: []string{winIn1, winIn2}, outputs: []string{winOut}}, []string{"cpu_compacted.parquet"}},
		// An input left out for another reason (a DELETE rewrote it) does not
		// make the pending output a replacement: the output stays out.
		{"an input left out by something else", &replacingExclusions{keys: []string{winIn1}}, []string{"cpu_in2.parquet"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := NewStorageWalkSource(localRig(t, winIn1, winIn2, winOut), "arc", zerolog.Nop())
			src.SetPendingOutputs(pendingOf(winOut))
			if tc.ex != nil {
				src.SetExclusions(tc.ex)
			}
			files, _, err := src.FilesAndLocal(ctx, cpu)
			if err != nil {
				t.Fatal(err)
			}
			if got := keysOf(files); strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("files = %v, want %v", got, tc.want)
			}
		})
	}
}

type exportRig struct {
	root  string
	be    storage.Backend
	exp   *Exporter
	src   *StorageWalkSource
	sched *Scheduler
}

func newExportRig(t *testing.T, interval time.Duration) *exportRig {
	t.Helper()
	withSpacing(t, 0)
	root := t.TempDir()
	be, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, be, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := NewStorageWalkSource(be, "arc", zerolog.Nop())
	return &exportRig{root: root, be: be, exp: exp, src: src,
		sched: NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Interval: interval, Logger: zerolog.Nop()})}
}

// live reads the table's current data files without writing: EnsureTable can
// commit heals, which would make the test a second writer next to the loop.
func (r *exportRig) live(t *testing.T) []string {
	t.Helper()
	lt, err := r.exp.catalog.LoadTable(context.Background(), r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		return nil // not created yet
	}
	have, err := r.exp.tableDataFiles(context.Background(), lt)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for p := range have {
		names = append(names, filepath.Base(p))
	}
	sort.Strings(names)
	return names
}

func (r *exportRig) write(t *testing.T, key string, rows int) {
	t.Helper()
	writeArcStyleParquet(t, filepath.Join(r.root, key), 1_700_000_000_000_000, rows)
}

// Through the exporter: a pass that lands in the window, here one whose job's
// delete failed, publishes the output in place of the inputs instead of a
// table without the job's rows (before #1722 it committed neither).
func TestScheduler_CompactionWindowPublishesTheOutput(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.write(t, winIn1, 20)
	r.write(t, winIn2, 30)
	r.sched.runPass(ctx)
	if got := r.live(t); strings.Join(got, ",") != "cpu_in1.parquet,cpu_in2.parquet" {
		t.Fatalf("pass 1 live = %v, want the two inputs", got)
	}
	r.write(t, winOut, 50)
	r.src.SetPendingOutputs(pendingOf(winOut))
	r.src.SetExclusions(&replacingExclusions{keys: []string{winIn1, winIn2}, outputs: []string{winOut}})
	r.sched.runPass(ctx)
	if got := r.live(t); strings.Join(got, ",") != "cpu_compacted.parquet" {
		t.Fatalf("window pass live = %v, want the output only", got)
	}
}

// A finished compaction's request is served by the loop without waiting for
// the timer: the table drops the deleted inputs at once (#1722).
func TestScheduler_RequestReconcileServesACompactionNow(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.write(t, winIn1, 20)
	r.write(t, winIn2, 30)
	r.sched.Start()
	t.Cleanup(r.sched.Stop)
	waitLive(t, r, "cpu_in1.parquet,cpu_in2.parquet")

	// The compaction: output in place, inputs deleted, then the request.
	r.write(t, winOut, 50)
	for _, k := range []string{winIn1, winIn2} {
		if err := r.be.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	r.sched.RequestReconcile("mydb", "cpu")
	r.sched.RequestReconcile("mydb", "cpu") // coalesced
	waitLive(t, r, "cpu_compacted.parquet")
}

func waitLive(t *testing.T, r *exportRig, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = r.live(t)
		if strings.Join(got, ",") == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("table = %v, want %s (an hour-long timer never fired: only a request could)", got, want)
}

// Requests coalesce; a gated-off node drops them; a measurement awaiting its
// namespace migration is left to the pass.
func TestScheduler_RequestsCoalesceAndRespectTheGate(t *testing.T) {
	s := NewScheduler(SchedulerConfig{Logger: zerolog.Nop()})
	s.RequestReconcile("db", "m")
	s.RequestReconcile("db", "m")
	s.RequestReconcile("db", "n")
	s.RequestReconcile("", "m")
	if got := s.takeRequests(); len(got) != 2 || got[0].Measurement != "m" || got[1].Measurement != "n" {
		t.Fatalf("requests = %v, want db/m and db/n once each", got)
	}
	if got := s.takeRequests(); len(got) != 0 {
		t.Fatalf("second take = %v, want none", got)
	}

	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.write(t, winIn1, 20)
	r.sched.runPass(ctx)
	r.write(t, winIn2, 30)
	gate := &flagGate{}
	r.sched.gate = gate
	listed := &orderSource{StorageWalkSource: r.src, asked: true}
	r.sched.source = listed
	r.sched.RequestReconcile("mydb", "cpu")
	r.sched.serviceRequests(ctx)
	if got := r.live(t); strings.Join(got, ",") != "cpu_in1.parquet" || len(listed.order) != 0 {
		t.Fatalf("gated off: live = %v, listings %v; want unchanged and nothing listed", got, listed.order)
	}
	gate.on = true
	r.sched.lastBlocked = map[string]struct{}{"mydb\x00cpu": {}}
	r.sched.RequestReconcile("mydb", "cpu")
	r.sched.serviceRequests(ctx)
	if got := r.live(t); strings.Join(got, ",") != "cpu_in1.parquet" {
		t.Fatalf("awaiting migration: live = %v, want unchanged", got)
	}
	r.sched.lastBlocked = nil
	r.sched.RequestReconcile("mydb", "cpu")
	r.sched.serviceRequests(ctx)
	if got := r.live(t); strings.Join(got, ",") != "cpu_in1.parquet,cpu_in2.parquet" {
		t.Fatalf("served: live = %v, want both files", got)
	}
}

type flagGate struct{ on bool }

func (g *flagGate) CanRun() bool { return g.on }

// orderSource records the order the scheduler lists measurements in, and asks
// for "aaa" while "cpu" is being reconciled, as a compaction finishing
// mid-pass would.
type orderSource struct {
	*StorageWalkSource
	sched *Scheduler
	order []string
	asked bool
}

func (o *orderSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	o.order = append(o.order, m.Measurement)
	if m.Measurement == "cpu" && !o.asked {
		o.asked = true
		o.sched.RequestReconcile("mydb", "aaa")
	}
	return o.StorageWalkSource.FilesAndLocal(ctx, m)
}

// A request that arrives during a pass is served before the pass's next
// measurement, not after the whole pass (#1722).
func TestScheduler_ServesARequestBetweenMeasurementsOfAPass(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	for _, m := range []string{"aaa", "cpu", "zzz"} {
		writeArcStyleParquet(t, filepath.Join(r.root, "mydb/"+m+"/2026/07/14/15/x.parquet"), 1_700_000_000_000_000, 5)
	}
	src := &orderSource{StorageWalkSource: r.src}
	r.sched.source = src
	src.sched = r.sched
	r.sched.runPass(ctx)
	if got := strings.Join(src.order, ","); got != "aaa,cpu,aaa,zzz" {
		t.Fatalf("listing order %s, want aaa,cpu,aaa,zzz (the request served before zzz)", got)
	}
}

func withSpacing(t *testing.T, d time.Duration) {
	t.Helper()
	old := requestMinSpacing
	requestMinSpacing = d
	t.Cleanup(func() { requestMinSpacing = old })
}

// A request inside requestMinSpacing of the table's last change waits out the
// rest of it instead of reconciling back to back, then is served.
func TestScheduler_RequestsAreSpacedPerMeasurement(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	withSpacing(t, 300*time.Millisecond)
	r.exp.retain = 1 // spaced only where a quick second reconcile could prune a resolved version
	r.write(t, winIn1, 20)
	r.sched.runPass(ctx)
	r.write(t, winIn2, 30)
	r.sched.RequestReconcile("mydb", "cpu")
	r.sched.serviceRequests(ctx)
	if got := r.live(t); strings.Join(got, ",") != "cpu_in1.parquet" {
		t.Fatalf("inside the spacing: live = %v, want unchanged", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.sched.serviceRequests(ctx)
		if got := r.live(t); strings.Join(got, ",") == "cpu_in1.parquet,cpu_in2.parquet" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the deferred request was never served: live = %v", r.live(t))
}

// A served request ends a busy stretch, as a non-busy pass does: a busy clock
// left running would escalate a measurement that is committing.
func TestScheduler_RequestEndsABusyStretch(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	r.write(t, winIn1, 20)
	r.sched.runPass(ctx)
	r.write(t, winIn2, 30)
	r.sched.busySince["mydb\x00cpu"] = time.Now().Add(-29 * time.Minute)
	r.sched.RequestReconcile("mydb", "cpu")
	r.sched.serviceRequests(ctx)
	if _, running := r.sched.busySince["mydb\x00cpu"]; running {
		t.Fatal("busy clock still running after a request committed")
	}
}

// passCountingSource counts timed passes (each starts with Measurements).
type passCountingSource struct {
	*StorageWalkSource
	mu     sync.Mutex
	passes int
}

func (c *passCountingSource) Measurements(ctx context.Context) ([]Measurement, error) {
	c.mu.Lock()
	c.passes++
	c.mu.Unlock()
	return c.StorageWalkSource.Measurements(ctx)
}

// Requests never push the timed pass back: under a steady stream of them the
// passes still run on their interval.
func TestScheduler_RequestsDoNotStarveTheTimedPass(t *testing.T) {
	r := newExportRig(t, 300*time.Millisecond)
	r.write(t, winIn1, 20)
	src := &passCountingSource{StorageWalkSource: r.src}
	r.sched.source = src
	r.sched.Start()
	t.Cleanup(r.sched.Stop)
	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		r.sched.RequestReconcile("mydb", "cpu")
		time.Sleep(20 * time.Millisecond)
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.passes < 4 {
		t.Fatalf("%d timed passes in 2 s at a 300 ms interval under a stream of requests, want at least 4", src.passes)
	}
}

// At the default retain the spacing does not apply: the kept versions outlast
// a burst, and a later job's deleted inputs leave the table at once.
func TestScheduler_RequestsAreNotSpacedAtTheDefaultRetain(t *testing.T) {
	ctx := context.Background()
	r := newExportRig(t, time.Hour)
	withSpacing(t, time.Hour)
	r.exp.retain = 10
	r.write(t, winIn1, 20)
	r.sched.runPass(ctx)
	r.write(t, winIn2, 30)
	r.sched.RequestReconcile("mydb", "cpu")
	r.sched.serviceRequests(ctx)
	if got := r.live(t); strings.Join(got, ",") != "cpu_in1.parquet,cpu_in2.parquet" {
		t.Fatalf("retain 10: live = %v, want the request served at once", got)
	}
}
