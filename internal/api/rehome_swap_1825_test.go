package api

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/readgate"
	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
)

// swapRig is a standalone node's storage with one misfiled file in it.
type swapRig struct {
	t       *testing.T
	root    string
	scratch string
	backend storage.Backend
	reg     *rehome.Registry
	gate    *readgate.Gate
	h       *RehomeHandler
	app     *fiber.App
	swap    *RehomeSwap
	key     string
}

const swapKey = "db/cpu/2026/11/18/23/cpu_20261119_000009_000000000.parquet"

// newSwapRig writes a file in 2026-11-18 23h holding six rows: two of that
// hour, one of 11-19 00h, three of 11-19 05h.
func newSwapRig(t *testing.T) *swapRig {
	t.Helper()
	r := &swapRig{t: t, root: t.TempDir(), scratch: t.TempDir(), key: swapKey}
	full := filepath.Join(r.root, r.key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT * FROM (VALUES
		(TIMESTAMPTZ '2026-11-18 23:10:00+00', 1), (TIMESTAMPTZ '2026-11-19 05:00:00+00', 2), (TIMESTAMPTZ '2026-11-18 23:20:00+00', 4),
		(TIMESTAMPTZ '2026-11-19 00:00:01+00', 8), (TIMESTAMPTZ '2026-11-19 05:30:00+00', 16), (TIMESTAMPTZ '2026-11-19 05:59:59+00', 32)) v("time", n))
		TO '%s' (FORMAT PARQUET, KV_METADATA {'arc:tags': 'host'})`, full)); err != nil {
		t.Fatal(err)
	}
	r.open()
	return r
}

// open builds the process's side: storage, registry (read from the journal),
// handler. Called again to stand for a restart.
func (r *swapRig) open() {
	r.t.Helper()
	backend, err := storage.NewLocalBackend(r.root, zerolog.Nop())
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { _ = backend.Close() })
	r.backend = backend
	r.reg = rehome.NewRegistry(backend)
	if err := r.reg.Seed(context.Background()); err != nil {
		r.t.Fatal(err)
	}
	r.gate = &readgate.Gate{}
	r.h = NewRehomeHandler(backend, nil, r.scratch, zerolog.Nop())
	r.t.Cleanup(func() { _ = r.h.Shutdown(context.Background()) })
	r.swap = &RehomeSwap{Registry: r.reg, Reads: r.gate}
	r.h.SetSwap(r.swap)
	r.app = fiber.New()
	r.h.RegisterRoutes(r.app)
}

// start posts a real job and returns its id, or the refusal.
func (r *swapRig) start() (id string, code int, reason string) {
	r.t.Helper()
	code, out := rehomeDo(r.t, r.app, "POST", "/api/v1/rehome", "", fmt.Sprintf(`{"path":%q}`, r.key))
	if code != 202 {
		return "", code, fmt.Sprint(out["error"])
	}
	return out["id"].(string), code, ""
}

func (r *swapRig) wait(id string) map[string]interface{} {
	r.t.Helper()
	var job map[string]interface{}
	for i := 0; i < 600; i++ {
		_, job = rehomeDo(r.t, r.app, "GET", "/api/v1/rehome/"+id, "", "")
		if job["state"] != "running" {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.t.Fatalf("job %s did not end: %v", id, job)
	return nil
}

// reads is what a query of the measurement returns right now: every data
// file under it, less what the registry says to leave out. rows, the sum of
// n (each row is a distinct power of two, so the sum names the rows), and
// the files read.
func (r *swapRig) reads() (rows, sum int, files []string) {
	r.t.Helper()
	ctx := context.Background()
	all, err := r.backend.List(ctx, "db/cpu/")
	if err != nil {
		r.t.Fatal(err)
	}
	out := map[string]bool{}
	for _, k := range r.reg.ExcludedFor(ctx, "db/cpu/") {
		out[k] = true
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		r.t.Fatal(err)
	}
	defer db.Close()
	for _, k := range all {
		if out[k] || !strings.HasSuffix(k, ".parquet") {
			continue
		}
		files = append(files, k)
		var n, s int
		if err := db.QueryRow(fmt.Sprintf(`SELECT count(*), coalesce(sum(n), 0) FROM read_parquet('%s')`, filepath.Join(r.root, k))).Scan(&n, &s); err != nil {
			r.t.Fatal(err)
		}
		rows, sum = rows+n, sum+s
	}
	return rows, sum, files
}

func (r *swapRig) leftovers() (journal, staged int) {
	r.t.Helper()
	j, _ := r.backend.List(context.Background(), rehome.JournalRoot+"/")
	s, _ := r.backend.List(context.Background(), rehome.StagedRoot+"/")
	return len(j), len(s)
}

// requireOnce fails unless a read sees each of the six rows once.
func (r *swapRig) requireOnce(when string) {
	r.t.Helper()
	if rows, sum, files := r.reads(); rows != 6 || sum != 63 {
		r.t.Fatalf("%s: a read sees %d rows (sum %d), want the six rows once (sum 63); files %v", when, rows, sum, files)
	}
}

// The swap: the rows end in the directories they belong to, in files that
// keep Arc's metadata, the file is gone, and nothing of the job is left.
func TestSwapMovesTheRows1825(t *testing.T) {
	r := newSwapRig(t)
	var steps []string
	var mu sync.Mutex
	r.h.afterStep = func(s string) {
		mu.Lock()
		steps = append(steps, s)
		mu.Unlock()
		// Whatever the step, a read sees every row once.
		r.requireOnce("after step " + s)
	}
	published := 0
	r.swap.FilePublished = func(string, string, time.Time) { published++ }
	cleared := ""
	r.swap.ClearMisfiled = func(p string) { cleared = p }

	id, code, why := r.start()
	if code != 202 {
		t.Fatalf("start: %d %s", code, why)
	}
	job := r.wait(id)
	if job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	_, _, files := r.reads()
	dirs := map[string]int{}
	for _, f := range files {
		dirs[filepath.Dir(f)]++
	}
	if len(files) != 3 || dirs["db/cpu/2026/11/18/23"] != 1 || dirs["db/cpu/2026/11/19/00"] != 1 || dirs["db/cpu/2026/11/19/05"] != 1 {
		t.Fatalf("files after the swap: %v", files)
	}
	r.requireOnce("after the swap")
	if _, err := os.Stat(filepath.Join(r.root, r.key)); !os.IsNotExist(err) {
		t.Fatalf("the file that was moved is still there (%v)", err)
	}
	if j, s := r.leftovers(); j != 0 || s != 0 || r.reg.Active() {
		t.Fatalf("left behind: %d journal file(s), %d staged file(s), registry active %v", j, s, r.reg.Active())
	}
	if left, _ := os.ReadDir(r.scratch); len(left) != 0 {
		t.Fatalf("scratch left: %v", left)
	}
	if _, err := os.Stat(filepath.Join(r.root, rehome.StagedDir(id))); !os.IsNotExist(err) {
		t.Fatalf("the job's staging directory is still there (%v)", err)
	}
	want := "staged committed moved moved moved latched drained removed"
	if got := strings.Join(steps, " "); got != want {
		t.Fatalf("steps = %q, want %q", got, want)
	}
	// Once per output, and once more with an hour that is never the current
	// one, so the measurement's stamp moves whatever the outputs are.
	if published != 4 || cleared != r.key {
		t.Fatalf("query caches told of %d outputs, misfiled list cleared for %q", published, cleared)
	}
	// Each output keeps the dedup metadata and is named like an ingest file.
	db, _ := sql.Open("duckdb", "")
	defer db.Close()
	for _, f := range files {
		var v []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT value FROM parquet_kv_metadata('%s') WHERE key = 'arc:tags'`, filepath.Join(r.root, f))).Scan(&v); err != nil || string(v) != "host" {
			t.Errorf("%s: arc:tags = %q (%v)", f, v, err)
		}
		if _, err := rehome.ParseKey(f); err != nil || !strings.HasPrefix(filepath.Base(f), "cpu_") || strings.Contains(f, "compacted") {
			t.Errorf("output name %s", f)
		}
	}
	// The report names the outputs; a second request finds nothing to move.
	rep := job["report"].(map[string]interface{})
	if rep["misfiled_rows"] != float64(4) || len(rep["outputs"].([]interface{})) != 3 {
		t.Fatalf("report: %v", rep)
	}
	r.key = files[0]
	id, _, _ = r.start()
	if again := r.wait(id); again["state"] != "refused" || !strings.Contains(fmt.Sprint(again["reason"]), "nothing to move") {
		t.Fatalf("a well-filed file: %v", again)
	}
	r.requireOnce("after the refused job")
}

// A stop at any step, then a restart: a read sees every row once before the
// restart's recovery and after it, and nothing of the job is left.
func TestSwapStoppedAtEachStepRecovers1825(t *testing.T) {
	for _, stopAt := range []string{"staged", "committed", "moved#1", "moved#2", "moved#3", "latched", "drained", "removed"} {
		t.Run(stopAt, func(t *testing.T) {
			r := newSwapRig(t)
			step, nth, _ := strings.Cut(stopAt, "#")
			seen := 0
			r.h.afterStep = func(s string) {
				if s != step {
					return
				}
				seen++
				if nth == "" || fmt.Sprint(seen) == nth {
					panic("stopped at " + stopAt)
				}
			}
			id, _, _ := r.start()
			if job := r.wait(id); job["state"] != "failed" {
				t.Fatalf("job: %v", job)
			}
			r.requireOnce("after the stop, in the stopped process")

			// The restart: memory is gone, the journal is read.
			r.open()
			r.requireOnce("after the restart, before recovery")
			if err := r.h.Recover(context.Background()); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			r.requireOnce("after recovery")
			if j, s := r.leftovers(); j != 0 || s != 0 || r.reg.Active() {
				t.Fatalf("after recovery: %d journal file(s), %d staged file(s), registry active %v", j, s, r.reg.Active())
			}
			_, _, files := r.reads()
			_, err := os.Stat(filepath.Join(r.root, swapKey))
			if step == "staged" {
				// Nothing had changed for readers: the job is dropped.
				if len(files) != 1 || err != nil {
					t.Fatalf("a job stopped before it moved anything: files %v, source %v", files, err)
				}
				return
			}
			if len(files) != 3 || !os.IsNotExist(err) {
				t.Fatalf("a job stopped part way was not finished: files %v, source %v", files, err)
			}
			// A second recovery finds nothing.
			if err := r.h.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			r.requireOnce("after a second recovery")
		})
	}
}

// The job waits for the reads in flight twice: before it moves anything
// (their SQL has no exclusions) and before it removes the file (their SQL
// reads it).
func TestSwapWaitsForReadsInFlight1825(t *testing.T) {
	r := newSwapRig(t)
	var at atomic.Value
	at.Store("")
	r.h.afterStep = func(s string) { at.Store(s) }
	// reaches waits for the job to get to a step, then gives it time to go
	// past it: a job that does not wait would.
	reaches := func(step string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for at.Load() != step && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(300 * time.Millisecond)
	}
	leave := r.gate.Enter() // a query from before the job, still on its way
	id, _, _ := r.start()
	reaches("committed")
	if got := at.Load(); got != "committed" {
		t.Fatalf("with a read from before the job in flight the job is at %q, want it waiting after committed", got)
	}
	if _, _, files := r.reads(); len(files) != 1 || files[0] != swapKey {
		t.Fatalf("an output was put in place while a read from before the job was in flight: %v", files)
	}
	// A read that enters now, and stays: it is from before the latch.
	second := r.gate.Enter()
	leave()
	reaches("latched")
	if got := at.Load(); got != "latched" {
		t.Fatalf("with a read from before the latch in flight the job is at %q, want it waiting after latched", got)
	}
	if _, err := os.Stat(filepath.Join(r.root, swapKey)); err != nil {
		t.Fatalf("the file was removed while a read from before the latch was in flight: %v", err)
	}
	r.requireOnce("latched, waiting")
	second()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	r.requireOnce("done")
}

// What a job refuses, and that a refusal leaves everything as it was.
func TestSwapRefusals1825(t *testing.T) {
	for name, tc := range map[string]struct {
		set     func(r *swapRig)
		atStart bool // refused by the request, not by the job
		want    string
	}{
		"the deployment":   {func(r *swapRig) { r.swap.Refusal = func() string { return "not on a cluster" } }, true, "not on a cluster"},
		"a backup running": {func(r *swapRig) { r.swap.BackupRunning = func() bool { return true } }, true, "backup"},
		"a compaction open on the measurement": {func(r *swapRig) {
			r.swap.CompactionOpen = func(p string) bool { return p == "db/cpu/" }
		}, false, "compaction"},
		"the file already left out of reads": {func(r *swapRig) {
			r.swap.OtherExclusions = func(context.Context, string) ([]string, error) { return []string{swapKey}, nil }
		}, false, "already left out"},
		"the file owned by another job": {func(r *swapRig) {
			_ = r.reg.Begin(&rehome.Job{ID: "ff", Database: "db", Measurement: "cpu", Source: rehome.FileRef{Key: swapKey}})
		}, false, "cannot be moved now"},
		"the file claimed by retention": {func(r *swapRig) { _, _ = r.reg.Claim(swapKey) }, false, "cannot be moved now"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newSwapRig(t)
			tc.set(r)
			id, code, why := r.start()
			if tc.atStart {
				if code != 409 || !strings.Contains(why, tc.want) {
					t.Fatalf("start: %d %q, want 409 naming %q", code, why, tc.want)
				}
			} else {
				job := r.wait(id)
				if job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), tc.want) {
					t.Fatalf("job: %v, want refused naming %q", job, tc.want)
				}
			}
			if _, _, files := r.reads(); len(files) != 1 || files[0] != swapKey {
				t.Fatalf("a refused job changed the files: %v", files)
			}
			if j, s := r.leftovers(); j != 0 || s != 0 {
				t.Fatalf("a refused job left %d journal and %d staged file(s)", j, s)
			}
		})
	}
	// Without SetSwap, and on storage that is not local, a real job is refused.
	r := newSwapRig(t)
	r.h.SetSwap(nil)
	if _, code, why := r.start(); code != 409 || !strings.Contains(why, "dry_run") {
		t.Fatalf("no swap wired: %d %q", code, why)
	}
}

// While a job is in flight the registry owns its files, and the DELETE check
// runs under the rewrite lock before the split.
func TestSwapOwnsItsFilesAndTakesTheRewriteLock1825(t *testing.T) {
	r := newSwapRig(t)
	locked := 0
	r.swap.RewriteLock = func(fn func()) {
		locked++
		if !r.reg.Owns(swapKey) {
			t.Error("the rewrite lock was taken before the job owned its file")
		}
		fn()
	}
	owned := map[string]bool{}
	r.h.afterStep = func(s string) {
		for _, j := range r.reg.Jobs() {
			if !r.reg.Owns(j.Source.Key) {
				t.Errorf("at %s the source is not owned", s)
			}
			for _, o := range j.Outputs {
				if !r.reg.Owns(o.Key) {
					t.Errorf("at %s the output %s is not owned", s, o.Key)
				}
				owned[o.Key] = true
			}
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "done" || locked != 1 || len(owned) != 3 {
		t.Fatalf("job %v, rewrite lock taken %d time(s), outputs owned %d", job["state"], locked, len(owned))
	}
	for k := range owned {
		if r.reg.Owns(k) {
			t.Errorf("%s is still owned after the job ended", k)
		}
	}
}

// The staging root backup skips is the one the job stages under.
func TestBackupSkipsTheStagingRoot1825(t *testing.T) {
	if rehome.StagedRoot != "_rehome_staged" || !storage.IsReservedRootDir(rehome.StagedRoot) || !storage.IsReservedRootDir(rehome.JournalRoot) {
		t.Fatalf("staging root %q, journal root %q: backup.rehomeStagedRoot must match and both must be reserved", rehome.StagedRoot, rehome.JournalRoot)
	}
}

// A file a DELETE rewrote, with the rewrite still beside it, is refused: one
// of the two holds rows that DELETE removed, and only a DELETE can tell which.
func TestSwapRefusesAFileWithItsRewriteBesideIt1825(t *testing.T) {
	r := newSwapRig(t)
	rewrite := strings.TrimSuffix(swapKey, ".parquet") + "_rewrite_1795000000000000000.parquet"
	data, err := os.ReadFile(filepath.Join(r.root, swapKey))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.backend.Write(context.Background(), rewrite, data); err != nil {
		t.Fatal(err)
	}
	if storage.RewriteFamily(rewrite) != storage.RewriteFamily(swapKey) {
		t.Fatalf("the test's rewrite name is not of the file's family: %s", rewrite)
	}
	for _, key := range []string{swapKey, rewrite} {
		r.key = key
		id, _, _ := r.start()
		job := r.wait(id)
		if job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "rewritten by a DELETE") {
			t.Fatalf("%s: %v", key, job)
		}
	}
	if left, _ := r.backend.List(context.Background(), "db/cpu/"); len(left) != 2 {
		t.Fatalf("a refused job changed the files: %v", left)
	}
}

// A job that stopped part way in a running process is taken up again by a
// resume request, and its files stay owned until then.
func TestSwapResume1825(t *testing.T) {
	r := newSwapRig(t)
	moved := 0
	r.h.afterStep = func(s string) {
		if s == "moved" {
			if moved++; moved == 2 {
				panic("stopped after the second move")
			}
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "failed" {
		t.Fatalf("job: %v", job)
	}
	r.requireOnce("stopped part way")
	if !r.reg.Owns(swapKey) {
		t.Fatal("a job stopped part way no longer owns its file")
	}
	if code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome", "", fmt.Sprintf(`{"path":%q}`, swapKey)); code == 202 {
		if again := r.wait(out["id"].(string)); again["state"] != "refused" {
			t.Fatalf("a second job on a file a stopped job owns: %v", again)
		}
	}
	r.h.afterStep = nil
	if code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", ""); code != 202 {
		t.Fatalf("resume: %d %v", code, out)
	}
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("after resume: %v", job)
	}
	r.requireOnce("after resume")
	if _, _, files := r.reads(); len(files) != 3 || r.reg.Active() {
		t.Fatalf("after resume: files %v, registry active %v", files, r.reg.Active())
	}
	if code, _ := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", ""); code != 404 {
		t.Fatalf("resume of a job that ended: %d, want 404", code)
	}
}

// stopAt runs a job on r until the named step, then stands for a restart
// without recovering.
func stopAt(t *testing.T, r *swapRig, step string, nth int) {
	t.Helper()
	seen := 0
	r.h.afterStep = func(s string) {
		if s == step {
			if seen++; seen == nth {
				panic("stopped")
			}
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "failed" {
		t.Fatalf("job: %v", job)
	}
	r.open()
}

// What recovery refuses to guess about: it keeps the job as recorded, and
// lists it.
func TestRecoveryKeepsAJobItCannotFinish1825(t *testing.T) {
	ctx := context.Background()
	stuck := func(t *testing.T, r *swapRig) {
		t.Helper()
		if err := r.h.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if !r.reg.Active() {
			t.Fatal("the job was not kept")
		}
		_, list := rehomeDo(t, r.app, "GET", "/api/v1/rehome", "", "")
		jobs := list["jobs"].([]interface{})
		if len(jobs) != 1 || jobs[0].(map[string]interface{})["state"] != "stuck" {
			t.Fatalf("list: %v", list)
		}
	}
	t.Run("the file being moved changed while the job was committed", func(t *testing.T) {
		r := newSwapRig(t)
		stopAt(t, r, "moved", 1)
		if err := os.WriteFile(filepath.Join(r.root, swapKey), []byte("truncated"), 0o600); err != nil {
			t.Fatal(err)
		}
		stuck(t, r)
		if _, err := os.Stat(filepath.Join(r.root, swapKey)); err != nil {
			t.Fatalf("the changed file was removed: %v", err)
		}
	})
	t.Run("an output is gone once reads were switched to the outputs", func(t *testing.T) {
		r := newSwapRig(t)
		stopAt(t, r, "latched", 1)
		job := r.reg.Jobs()[0]
		if err := os.Remove(filepath.Join(r.root, job.Outputs[1].Key)); err != nil {
			t.Fatal(err)
		}
		stuck(t, r)
		if _, err := os.Stat(filepath.Join(r.root, swapKey)); err != nil {
			t.Fatalf("the file was removed though an output is missing: %v", err)
		}
	})
	t.Run("another file sits at an output's key", func(t *testing.T) {
		r := newSwapRig(t)
		stopAt(t, r, "committed", 1)
		job := r.reg.Jobs()[0]
		other := filepath.Join(r.root, job.Outputs[0].Key)
		if err := os.MkdirAll(filepath.Dir(other), 0o700); err != nil {
			t.Fatal(err)
		}
		// The same size as the output, other bytes: only the hash tells.
		theirs, err := os.ReadFile(filepath.Join(r.root, job.Outputs[0].Staged))
		if err != nil {
			t.Fatal(err)
		}
		theirs[len(theirs)/2] ^= 0xff
		if err := os.WriteFile(other, theirs, 0o600); err != nil {
			t.Fatal(err)
		}
		stuck(t, r)
		if got, _ := os.ReadFile(other); string(got) != string(theirs) {
			t.Fatal("the file at the output's key was overwritten")
		}
		if _, err := os.Stat(filepath.Join(r.root, swapKey)); err != nil {
			t.Fatalf("the file being moved was removed though another file sits at an output's key: %v", err)
		}
	})
	t.Run("an output is lost before it was moved: the job is undone", func(t *testing.T) {
		r := newSwapRig(t)
		stopAt(t, r, "moved", 1)
		job := r.reg.Jobs()[0]
		if err := os.Remove(filepath.Join(r.root, job.Outputs[2].Staged)); err != nil {
			t.Fatal(err)
		}
		if err := r.h.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		if _, _, files := r.reads(); r.reg.Active() || len(files) != 1 || files[0] != swapKey {
			t.Fatalf("after recovery: registry active %v, files %v; want the job undone and the file as it was", r.reg.Active(), files)
		}
		r.requireOnce("undone")
	})
}

// A journal left by a run under another configuration: on a deployment that
// no longer runs such jobs, a job that had not switched reads is undone, and
// one that had is kept as recorded.
func TestRecoveryOnADeploymentThatRefusesJobs1825(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		step     string
		nth      int
		wantKept bool
	}{{"staged", 1, false}, {"moved", 2, false}, {"latched", 1, true}} {
		t.Run(tc.step, func(t *testing.T) {
			r := newSwapRig(t)
			stopAt(t, r, tc.step, tc.nth)
			r.swap.Refusal = func() string { return "this node is now part of a cluster" }
			if err := r.h.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			r.requireOnce("after recovery")
			_, _, files := r.reads()
			if tc.wantKept {
				if !r.reg.Active() || len(files) != 3 {
					t.Fatalf("a job that had switched reads: registry active %v, files %v; want it kept, reading the outputs", r.reg.Active(), files)
				}
				if _, err := os.Stat(filepath.Join(r.root, swapKey)); err != nil {
					t.Fatalf("the old file was removed on a deployment that refuses jobs: %v", err)
				}
				return
			}
			if r.reg.Active() || len(files) != 1 || files[0] != swapKey {
				t.Fatalf("registry active %v, files %v; want the job undone and the file as it was", r.reg.Active(), files)
			}
			if j, s := r.leftovers(); j != 0 || s != 0 {
				t.Fatalf("left %d journal and %d staged file(s)", j, s)
			}
		})
	}
}
