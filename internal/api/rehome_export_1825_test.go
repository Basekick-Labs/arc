//go:build duckdb_arrow

package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeExport1825 stands for the Iceberg export: it records what it was
// asked and answers from a script.
type fakeExport1825 struct {
	mu     sync.Mutex
	asked  []string // the file asked about, per question
	answer func(n int, fileKey string) (RehomeExportAnswer, error)
}

func (f *fakeExport1825) ask(_ context.Context, database, measurement, fileKey string) (RehomeExportAnswer, error) {
	f.mu.Lock()
	f.asked = append(f.asked, fileKey)
	n := len(f.asked)
	f.mu.Unlock()
	if database != "db" || measurement != "cpu" {
		return RehomeExportAnswer{}, fmt.Errorf("asked about %s.%s", database, measurement)
	}
	return f.answer(n, fileKey)
}

func (f *fakeExport1825) questions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...)
}

func withExport1825(t *testing.T, r *swapRig, answer func(n int, fileKey string) (RehomeExportAnswer, error)) *fakeExport1825 {
	t.Helper()
	old, oldMax := rehomeExportRetry, rehomeExportRetryMax
	rehomeExportRetry, rehomeExportRetryMax = 5*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { rehomeExportRetry, rehomeExportRetryMax = old, oldMax })
	f := &fakeExport1825{answer: answer}
	r.swap.ExportExpected = true
	r.h.SetExport(f.ask)
	return f
}

func (r *swapRig) sourceThere() bool {
	_, err := os.Stat(filepath.Join(r.root, swapKey))
	return err == nil
}

// With the export on, the old file stays until the export says the published
// table does not name it, and reads are right all the while.
func TestSwapKeepsTheOldFileUntilTheTableDoesNotNameIt1825(t *testing.T) {
	r := newSwapRig(t)
	release := make(chan struct{})
	f := withExport1825(t, r, func(n int, fileKey string) (RehomeExportAnswer, error) {
		if fileKey == "" {
			return RehomeExportAnswer{Clear: true}, nil // before the job: publishing
		}
		select {
		case <-release:
			return RehomeExportAnswer{Clear: true}, nil
		default:
			return RehomeExportAnswer{Why: "the published table still names the file"}, nil
		}
	})
	var steps []string
	var mu sync.Mutex
	r.h.afterStep = func(s string) { mu.Lock(); steps = append(steps, s); mu.Unlock() }
	id, _, _ := r.start()
	// Asked several times, and still waiting.
	deadline := time.Now().Add(20 * time.Second)
	for len(f.questions()) < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("the export was asked %v", f.questions())
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, job := rehomeDo(t, r.app, "GET", "/api/v1/rehome/"+id, "", "")
	if job["state"] != "running" || !strings.Contains(fmt.Sprint(job["waiting_for"]), "Iceberg export") {
		t.Fatalf("while the table names the file: %v", job)
	}
	if !r.sourceThere() {
		t.Fatal("the old file was removed while the table names it")
	}
	r.requireOnce("while waiting for the export")
	if _, _, files := r.reads(); len(files) != 3 {
		t.Fatalf("while waiting, reads use %v; want the three new files", files)
	}
	close(release)
	if job := r.wait(id); job["state"] != "done" || job["waiting_for"] != nil {
		t.Fatalf("job: %v", job)
	}
	if r.sourceThere() || r.reg.Active() {
		t.Fatal("after the export answered, the old file or the job's record is still there")
	}
	q := f.questions()
	if q[0] != "" {
		t.Fatalf("the first question named %q; want the one before the job, about no file", q[0])
	}
	for _, k := range q[1:] {
		if k != swapKey {
			t.Fatalf("asked about %q, want the old file", k)
		}
	}
	// The question before the job comes before reads leave anything out, and
	// the ones after come once reads from before the switch have ended.
	mu.Lock()
	got := strings.Join(steps, " ")
	mu.Unlock()
	if !strings.HasPrefix(got, "staged export-ready committed ") || !strings.HasSuffix(got, " latched drained exported removed") {
		t.Fatalf("steps: %s", got)
	}
}

// An export that cannot publish the table refuses the job before anything
// changed for readers.
func TestSwapRefusedWhenTheExportCannotPublish1825(t *testing.T) {
	for name, answer := range map[string]func(int, string) (RehomeExportAnswer, error){
		"not publishing": func(int, string) (RehomeExportAnswer, error) {
			return RehomeExportAnswer{Why: "the last pass of the table failed: schema conflict"}, nil
		},
		"no answer": func(int, string) (RehomeExportAnswer, error) { return RehomeExportAnswer{}, context.DeadlineExceeded },
	} {
		t.Run(name, func(t *testing.T) {
			r := newSwapRig(t)
			withExport1825(t, r, answer)
			committed := false
			r.h.afterStep = func(s string) {
				if s == "committed" {
					committed = true
				}
			}
			id, _, _ := r.start()
			job := r.wait(id)
			if job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "Iceberg export cannot publish db.cpu") {
				t.Fatalf("job: %v", job)
			}
			if name == "not publishing" && !strings.Contains(fmt.Sprint(job["reason"]), "schema conflict") {
				t.Fatalf("the reason does not carry the export's: %v", job["reason"])
			}
			if committed {
				t.Fatal("reads were changed before the export was asked")
			}
			if _, _, files := r.reads(); r.reg.Active() || len(files) != 1 || files[0] != swapKey {
				t.Fatalf("registry active %v, files %v; want the file as it was", r.reg.Active(), files)
			}
			if j, s := r.leftovers(); j != 0 || s != 0 {
				t.Fatalf("left %d journal and %d staged file(s)", j, s)
			}
		})
	}
}

// The export is enabled and nothing bound it (it is not running on this
// node): a new job is refused, not left waiting.
func TestSwapRefusedWhenTheExportIsNotRunning1825(t *testing.T) {
	r := newSwapRig(t)
	r.swap.ExportExpected = true
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "not running on this node") {
		t.Fatalf("job: %v", job)
	}
	if !r.sourceThere() || r.reg.Active() {
		t.Fatal("a refused job changed something")
	}
}

// The wait is waived by request, once the export has answered.
func TestSwapExportWaitWaived1825(t *testing.T) {
	r := newSwapRig(t)
	answerNow := make(chan struct{})
	f := withExport1825(t, r, func(n int, fileKey string) (RehomeExportAnswer, error) {
		if fileKey == "" {
			return RehomeExportAnswer{Clear: true}, nil
		}
		select {
		case <-answerNow:
			return RehomeExportAnswer{Why: "the Iceberg export is paused"}, nil
		default:
			// No answer: the export did not get to the question.
			return RehomeExportAnswer{}, errors.New("no answer")
		}
	})
	id, _, _ := r.start()
	waiting := func() bool {
		_, job := rehomeDo(t, r.app, "GET", "/api/v1/rehome/"+id, "", "")
		return job["waiting_for"] != nil
	}
	for i := 0; !waiting(); i++ {
		if i > 2000 {
			t.Fatal("the job never waited for the export")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Another job's id, or a job that is not waiting: refused.
	if code, _ := rehomeDo(t, r.app, "POST", "/api/v1/rehome/nosuch/resume", "", `{"skip_export_wait":true}`); code != 404 {
		t.Fatalf("waiving for an unknown job: %d", code)
	}
	if code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", `{"skip_export_wait":true}`); code != 202 {
		t.Fatalf("waiving: %d %v", code, out)
	}
	// Waived, and the export has not answered once since reads were
	// switched: the old file stays.
	asked := len(f.questions())
	for len(f.questions()) < asked+5 {
		time.Sleep(5 * time.Millisecond)
	}
	if !r.sourceThere() || !waiting() {
		t.Fatal("the old file was removed before the export answered once")
	}
	close(answerNow)
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	if r.sourceThere() || r.reg.Active() {
		t.Fatal("after the waiver and an answer, the old file or the record is still there")
	}
	r.requireOnce("after the waiver")
	if code, _ := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", `{"skip_export_wait":true}`); code != 404 {
		t.Fatalf("waiving for a job that ended: %d, want 404", code)
	}
}

// A job found at start past its commit, with the export on: recovery leaves
// it until the export is bound, then it ends as any job does.
func TestRecoveryWaitsForTheExport1825(t *testing.T) {
	for _, tc := range []struct {
		step string
		nth  int
	}{{"moved", 2}, {"latched", 1}} {
		t.Run(tc.step, func(t *testing.T) {
			r := newSwapRig(t)
			stopAt(t, r, tc.step, tc.nth)
			r.swap.ExportExpected = true
			old, oldMax := rehomeExportRetry, rehomeExportRetryMax
			rehomeExportRetry, rehomeExportRetryMax = 5*time.Millisecond, 5*time.Millisecond
			t.Cleanup(func() { rehomeExportRetry, rehomeExportRetryMax = old, oldMax })
			done := make(chan error, 1)
			go func() { done <- r.h.Recover(context.Background()) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("Recover did not return: it waits for an export that is bound after it")
			}
			// Unbound: nothing is done to the job, not even its moves; the
			// old file stays, reads are right.
			state := r.reg.Jobs()[0].State
			time.Sleep(150 * time.Millisecond)
			if got := r.reg.Jobs()[0].State; got != state {
				t.Fatalf("with the export not yet bound, the job went from %s to %s", state, got)
			}
			deadline := time.Now().Add(20 * time.Second)
			for {
				_, list := rehomeDo(t, r.app, "GET", "/api/v1/rehome", "", "")
				jobs, _ := list["jobs"].([]interface{})
				if len(jobs) == 1 && jobs[0].(map[string]interface{})["waiting_for"] != nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the job never waited for the export: %v", list)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !r.sourceThere() || !r.reg.Active() {
				t.Fatal("with the export not yet bound, the job removed the old file")
			}
			r.requireOnce("before the export is bound")
			if code, _ := rehomeDo(t, r.app, "POST", "/api/v1/rehome", "", fmt.Sprintf(`{"path":%q,"dry_run":true}`, swapKey)); code != 409 {
				t.Fatalf("a second job while the recovered one waits: %d, want 409", code)
			}
			f := &fakeExport1825{answer: func(int, string) (RehomeExportAnswer, error) { return RehomeExportAnswer{Clear: true}, nil }}
			r.h.SetExport(f.ask)
			id := r.reg.Jobs()[0].ID
			if job := r.wait(id); job["state"] != "done" {
				t.Fatalf("job: %v", job)
			}
			if r.sourceThere() || r.reg.Active() {
				t.Fatal("after the export was bound, the old file or the record is still there")
			}
			r.requireOnce("after recovery")
			for _, k := range f.questions() {
				if k != swapKey {
					t.Fatalf("a recovered job asked about %q; the question before a job is not for one past its commit", k)
				}
			}
		})
	}
}

// A journal from a run with the export on, found with it off: nothing
// maintains a table, and the job ends at start without asking.
func TestRecoveryWithTheExportTurnedOff1825(t *testing.T) {
	r := newSwapRig(t)
	stopAt(t, r, "latched", 1)
	if err := r.h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.sourceThere() || r.reg.Active() {
		t.Fatal("with the export off, a latched job was not finished at start")
	}
	r.requireOnce("after recovery")
}

// Rows with no time have no day, and the export leaves a file of them out of
// the table: with the export on, a file holding some is not split.
func TestSwapRefusesRowsWithNoTimeWhenExported1825(t *testing.T) {
	r := newSwapRig(t)
	const key = "db/cpu/2026/11/18/23/cpu_20261119_000010_000000000.parquet"
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT * FROM (VALUES
		(TIMESTAMPTZ '2026-11-19 05:00:00+00', 1), (NULL, 2)) v("time", n))
		TO '%s' (FORMAT PARQUET)`, filepath.Join(r.root, key))); err != nil {
		t.Fatal(err)
	}
	r.key = key
	// Without the export the file is split, the row with no time staying
	// where it is: asserted by the dry run's plan, which changes nothing.
	withExport1825(t, r, func(int, string) (RehomeExportAnswer, error) { return RehomeExportAnswer{Clear: true}, nil })
	id, _, _ := r.start()
	job := r.wait(id)
	if job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "NULL or infinite time") {
		t.Fatalf("job: %v", job)
	}
	if _, err := os.Stat(filepath.Join(r.root, key)); err != nil || r.reg.Active() {
		t.Fatalf("a refused job changed something: %v", err)
	}
	// The same file with the export off is moved.
	r.swap.ExportExpected = false
	id, _, _ = r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("with the export off: %v", job)
	}
}

// A job found latched with its old file already gone (a stop between the
// removal and the record's) has nothing to keep for the table: it ends
// without asking the export.
func TestRecoveryOfAJobWhoseOldFileIsGoneDoesNotWait1825(t *testing.T) {
	r := newSwapRig(t)
	stopAt(t, r, "removed", 1)
	if r.sourceThere() || !r.reg.Active() {
		t.Fatal("the rig is not a latched job with its old file gone")
	}
	f := withExport1825(t, r, func(int, string) (RehomeExportAnswer, error) {
		return RehomeExportAnswer{Why: "the Iceberg export is paused"}, nil
	})
	if err := r.h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := r.reg.Jobs()
	deadline := time.Now().Add(20 * time.Second)
	for r.reg.Active() {
		if time.Now().After(deadline) {
			t.Fatalf("the job still waits (%v); asked the export %d time(s)", id, len(f.questions()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(f.questions()); n != 0 {
		t.Fatalf("the export was asked %d time(s) about a file that is gone", n)
	}
	r.requireOnce("after recovery")
}

// The waiver is read as JSON whatever the request says its body is.
func TestSwapExportWaitWaivedWithoutAContentType1825(t *testing.T) {
	r := newSwapRig(t)
	withExport1825(t, r, func(n int, fileKey string) (RehomeExportAnswer, error) {
		if fileKey == "" {
			return RehomeExportAnswer{Clear: true}, nil
		}
		return RehomeExportAnswer{Why: "the Iceberg export is paused"}, nil
	})
	id, _, _ := r.start()
	for i := 0; ; i++ {
		_, job := rehomeDo(t, r.app, "GET", "/api/v1/rehome/"+id, "", "")
		if job["waiting_for"] != nil {
			break
		}
		if i > 2000 {
			t.Fatal("the job never waited for the export")
		}
		time.Sleep(5 * time.Millisecond)
	}
	req := httptest.NewRequest("POST", "/api/v1/rehome/"+id+"/resume", strings.NewReader(`{"skip_export_wait":true}`))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := r.app.Test(req, -1)
	if err != nil || resp.StatusCode != 202 {
		t.Fatalf("waiving with a form content type: %v %v", resp.StatusCode, err)
	}
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
}
