//go:build duckdb_arrow

package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
)

// fakeTiering1825 stands for the tiering manager: tier rows in a map.
type fakeTiering1825 struct {
	mu       sync.Mutex
	rows     map[string]bool // path -> has a hot row
	checked  []string
	checkErr error
	noRecord string // a directory prefix that cannot be recorded
	recErr   error
	// owns is the registry's answer; unowned records a file asked about
	// before the job owned it, when tiering could still take it.
	owns    func(path string) bool
	unowned []string
}

func (f *fakeTiering1825) CheckRehomeSource(_ context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checked = append(f.checked, path)
	if f.owns != nil && !f.owns(path) {
		f.unowned = append(f.unowned, path)
	}
	return f.checkErr
}

func (f *fakeTiering1825) CanRecord(path string) error {
	if f.noRecord != "" && strings.HasPrefix(path, f.noRecord) {
		return errors.New("year out of range")
	}
	return nil
}

func (f *fakeTiering1825) RecordRehomedFiles(_ context.Context, files []tiering.RehomedFile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recErr != nil {
		return f.recErr
	}
	for _, x := range files {
		if x.SizeBytes <= 0 {
			return fmt.Errorf("%s recorded with size %d", x.Path, x.SizeBytes)
		}
		f.rows[x.Path] = true
	}
	return nil
}

func (f *fakeTiering1825) ForgetRehomedFiles(_ context.Context, paths []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range paths {
		delete(f.rows, p)
	}
	return nil
}

// The fake has no cold tier; rehome_cold_1825_test.go has one that does.
func (f *fakeTiering1825) ColdBackend() storage.Backend { return nil }
func (f *fakeTiering1825) CheckRehomeColdSource(context.Context, string) (int64, string, error) {
	return 0, "", errors.New("no cold tier")
}
func (f *fakeTiering1825) ColdSourceStat(context.Context, string, string) (int64, error) {
	return 0, errors.New("no cold tier")
}
func (f *fakeTiering1825) RemoveRehomedColdFile(context.Context, string, string) error {
	return errors.New("no cold tier")
}

func (f *fakeTiering1825) hot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for p := range f.rows {
		out = append(out, p)
	}
	return out
}

func withTiering1825(t *testing.T, r *swapRig) *fakeTiering1825 {
	t.Helper()
	f := &fakeTiering1825{rows: map[string]bool{swapKey: true}, owns: r.reg.Owns}
	r.swap.TieringExpected, r.swap.ColdConfigured = true, true
	r.h.SetTiering(f)
	r.h.Wired()
	return f
}

// The outputs have their tier rows before reads are sent to them, and the
// old file's row goes once it is removed.
func TestSwapWritesTierRows1825(t *testing.T) {
	r := newSwapRig(t)
	f := withTiering1825(t, r)
	var atLatch, atCommit []string
	r.h.afterStep = func(s string) {
		switch s {
		case "committed":
			atCommit = f.hot()
		case "latched":
			atLatch = f.hot()
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	if len(atCommit) != 1 {
		t.Fatalf("rows when reads still use the old file: %v; want only its own", atCommit)
	}
	if len(atLatch) != 4 {
		t.Fatalf("rows when reads are switched: %v; want the old file's and the three new ones", atLatch)
	}
	rows := f.hot()
	if len(rows) != 3 {
		t.Fatalf("rows after the job: %v; want the three new files", rows)
	}
	for _, p := range rows {
		if p == swapKey {
			t.Fatal("the removed file kept its row")
		}
	}
	if len(f.checked) != 1 || f.checked[0] != swapKey {
		t.Fatalf("tiering was asked about %v, want the file once", f.checked)
	}
	if len(f.unowned) != 0 {
		t.Fatalf("tiering was asked about %v before the job owned it: a migration could take the file after the answer", f.unowned)
	}
	r.requireOnce("after the job")
}

func TestSwapRefusedByTiering1825(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"moving":          {fmt.Errorf("%w: x", tiering.ErrFileMoving), "being moved to cold storage"},
		"a cold copy":     {fmt.Errorf("%w: x", tiering.ErrRehomeColdCopy), "cold storage holds it too"},
		"cannot be asked": {fmt.Errorf("%w: x", tiering.ErrColdCheckFailed), "could not be asked"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newSwapRig(t)
			f := withTiering1825(t, r)
			f.checkErr = tc.err
			id, _, _ := r.start()
			job := r.wait(id)
			if job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), tc.want) {
				t.Fatalf("job: %v", job)
			}
			if !r.sourceThere() || r.reg.Active() || len(f.hot()) != 1 {
				t.Fatal("a refused job changed something")
			}
		})
	}
}

// Tiering configured with a cold tier and not running: cold storage cannot
// be asked, so nothing is split. With no cold tier configured there is
// nothing to ask.
func TestSwapWithTieringConfiguredAndNotRunning1825(t *testing.T) {
	r := newSwapRig(t)
	r.swap.TieringExpected, r.swap.ColdConfigured = true, true
	r.h.Wired()
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "not running on this node") {
		t.Fatalf("job: %v", job)
	}
	if !r.sourceThere() || r.reg.Active() {
		t.Fatal("a refused job changed something")
	}
	r.swap.ColdConfigured = false
	id, _, _ = r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("with no cold tier configured: %v", job)
	}
}

// Before the wiring is complete a job is refused: it could not ask tiering.
func TestSwapRefusedWhileStarting1825(t *testing.T) {
	r := newSwapRig(t)
	r.swap.TieringExpected = true
	if _, code, reason := r.start(); code != 409 || !strings.Contains(reason, "still starting") {
		t.Fatalf("before Wired: %d %q", code, reason)
	}
	r.h.Wired()
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("after Wired: %v", job)
	}
}

// A destination tiering cannot record is refused before anything is staged.
func TestSwapRefusesADestinationTieringCannotRecord1825(t *testing.T) {
	r := newSwapRig(t)
	f := withTiering1825(t, r)
	f.noRecord = "db/cpu/2026/11/19/05"
	id, _, _ := r.start()
	job := r.wait(id)
	if job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "tiered storage cannot record") {
		t.Fatalf("job: %v", job)
	}
	if j, s := r.leftovers(); !r.sourceThere() || r.reg.Active() || j != 0 || s != 0 {
		t.Fatalf("a refused job left something: journal %d staged %d", j, s)
	}
}

// A job that cannot write its rows stops before reads are switched, and goes
// on when it can.
func TestSwapStopsWhenTierRowsCannotBeWritten1825(t *testing.T) {
	r := newSwapRig(t)
	f := withTiering1825(t, r)
	f.recErr = errors.New("database is locked")
	latched := false
	r.h.afterStep = func(s string) {
		if s == "latched" {
			latched = true
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "stopped" {
		t.Fatalf("job: %v", job)
	}
	if latched || !r.sourceThere() {
		t.Fatal("reads were switched without tier rows for the new files")
	}
	r.requireOnce("stopped before the switch")
	f.mu.Lock()
	f.recErr = nil
	f.mu.Unlock()
	if code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", ""); code != 202 {
		t.Fatalf("resume: %d %v", code, out)
	}
	if job := r.wait(id); job["state"] != "done" || len(f.hot()) != 3 {
		t.Fatalf("after resume: %v, rows %v", job, f.hot())
	}
}

// An undone job leaves no rows for files it removed.
func TestUndoForgetsTierRows1825(t *testing.T) {
	r := newSwapRig(t)
	stopAt(t, r, "moved", 2)
	f := withTiering1825(t, r)
	// As after a scan that saw the outputs in place.
	for _, o := range r.reg.Jobs()[0].Outputs {
		f.rows[o.Key] = true
	}
	f.checkErr = fmt.Errorf("%w: x", tiering.ErrRehomeColdCopy)
	if err := r.h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for r.reg.Active() {
		if time.Now().After(deadline) {
			t.Fatal("the job was not undone")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if rows := f.hot(); len(rows) != 1 || rows[0] != swapKey {
		t.Fatalf("rows after the undo: %v; want only the file's own", rows)
	}
	if _, _, files := r.reads(); len(files) != 1 || files[0] != swapKey {
		t.Fatalf("files after the undo: %v", files)
	}
	r.requireOnce("after the undo")
}

// Recovery with tiering configured does nothing to a job past its commit
// until the wiring is complete, then records the rows a latched job lacks.
func TestRecoveryWaitsForTiering1825(t *testing.T) {
	r := newSwapRig(t)
	stopAt(t, r, "latched", 1)
	r.swap.TieringExpected = true
	if err := r.h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if !r.sourceThere() || !r.reg.Active() {
		t.Fatal("recovery finished the job before tiering was bound")
	}
	f := &fakeTiering1825{rows: map[string]bool{swapKey: true}}
	r.h.SetTiering(f)
	r.h.Wired()
	id := r.reg.Jobs()[0].ID
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	if rows := f.hot(); len(rows) != 3 || r.sourceThere() {
		t.Fatalf("rows %v; want the three new files, and the old one gone", rows)
	}
	r.requireOnce("after recovery")
}

// The export bound and never started: a job gets that as the export's answer
// at once.
func TestExportNotStarted1825(t *testing.T) {
	r := newSwapRig(t)
	asked := 0
	r.swap.ExportExpected = true
	r.h.SetExport(func(context.Context, string, string, string) (RehomeExportAnswer, error) {
		asked++
		return RehomeExportAnswer{}, context.DeadlineExceeded
	})
	r.h.SetExportNotStarted("the Iceberg export is not started on this node")
	r.h.Wired()
	start := time.Now()
	id, _, _ := r.start()
	job := r.wait(id)
	if job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "not started on this node") {
		t.Fatalf("job: %v", job)
	}
	if asked != 0 || time.Since(start) > 30*time.Second {
		t.Fatalf("the export was asked %d time(s) in %s", asked, time.Since(start))
	}

	// A latched job found at start: it waits, and the waiver is honoured,
	// since "not started" is an answer.
	r2 := newSwapRig(t)
	stopAt(t, r2, "latched", 1)
	old, oldMax := rehomeExportRetry, rehomeExportRetryMax
	rehomeExportRetry, rehomeExportRetryMax = 5*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { rehomeExportRetry, rehomeExportRetryMax = old, oldMax })
	r2.swap.ExportExpected = true
	r2.h.SetExport(func(context.Context, string, string, string) (RehomeExportAnswer, error) {
		return RehomeExportAnswer{}, context.DeadlineExceeded
	})
	r2.h.SetExportNotStarted("the Iceberg export is not started on this node")
	if err := r2.h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	r2.h.Wired()
	id2 := r2.reg.Jobs()[0].ID
	for i := 0; ; i++ {
		_, j := rehomeDo(t, r2.app, "GET", "/api/v1/rehome/"+id2, "", "")
		if strings.Contains(fmt.Sprint(j["waiting_for"]), "Iceberg export, to publish") {
			break
		}
		if i > 2000 {
			t.Fatalf("the job never waited for the export: %v", j)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !r2.sourceThere() {
		t.Fatal("the old file was removed with the export not started")
	}
	if code, out := rehomeDo(t, r2.app, "POST", "/api/v1/rehome/"+id2+"/resume", "", `{"skip_export_wait":true}`); code != 202 {
		t.Fatalf("waiver: %d %v", code, out)
	}
	if j := r2.wait(id2); j["state"] != "done" || r2.sourceThere() {
		t.Fatalf("after the waiver: %v", j)
	}
}

// #1848 item 3: a job found at start that had not switched reads, with the
// export bound and not started this time, is undone: going on would switch
// reads and then wait for an export that never runs.
func TestRecoveryUndoesACommittedJobWhenTheExportDidNotStart1848(t *testing.T) {
	for _, step := range []string{"committed", "moved"} {
		t.Run(step, func(t *testing.T) {
			r := newSwapRig(t)
			nth := 1
			if step == "moved" {
				nth = 2
			}
			stopAt(t, r, step, nth)
			r.swap.ExportExpected = true
			asked := 0
			r.h.SetExport(func(context.Context, string, string, string) (RehomeExportAnswer, error) {
				asked++
				return RehomeExportAnswer{}, context.DeadlineExceeded
			})
			r.h.SetExportNotStarted("the Iceberg export is not started on this node")
			if err := r.h.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			r.h.Wired()
			for i := 0; r.reg.Active(); i++ {
				if i > 4000 {
					t.Fatalf("the job is still recorded: %+v", r.reg.Jobs()[0])
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !r.sourceThere() || asked != 0 {
				t.Fatalf("source there %v, export asked %d time(s)", r.sourceThere(), asked)
			}
			r.requireOnce("after the undo")
		})
	}
}

// At start, a job that had not switched reads and whose file tiering cannot
// be asked about is kept as recorded, not undone: nothing is known to be
// wrong. A resume takes it up once tiering answers.
func TestRecoveryKeepsAJobTieringCannotBeAskedAbout1825(t *testing.T) {
	r := newSwapRig(t)
	stopAt(t, r, "moved", 2)
	f := withTiering1825(t, r)
	f.checkErr = fmt.Errorf("%w: x", tiering.ErrColdCheckFailed)
	if err := r.h.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := r.reg.Jobs()[0].ID
	if job := r.wait(id); job["state"] != "stopped" || !strings.Contains(fmt.Sprint(job["reason"]), "could not be asked") {
		t.Fatalf("job: %v", job)
	}
	if !r.reg.Active() || !r.sourceThere() {
		t.Fatal("the job was not kept as recorded")
	}
	r.requireOnce("kept as recorded")
	// A resume while tiering still cannot be asked: the job stays where it
	// is. It must not go on to switch reads without the answer.
	latched := false
	r.h.afterStep = func(s string) {
		if s == "latched" {
			latched = true
		}
	}
	if code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", ""); code != 202 {
		t.Fatalf("resume: %d %v", code, out)
	}
	if job := r.wait(id); job["state"] != "stopped" || !strings.Contains(fmt.Sprint(job["reason"]), "could not be asked") {
		t.Fatalf("a resume while tiering cannot be asked: %v", job)
	}
	if latched || !r.reg.Active() || !r.sourceThere() || len(r.reg.Jobs()) != 1 || r.reg.Jobs()[0].State != "committed" {
		t.Fatal("a resume went on without tiering's answer")
	}
	f.mu.Lock()
	f.checkErr = nil
	f.mu.Unlock()
	if code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", ""); code != 202 {
		t.Fatalf("resume: %d %v", code, out)
	}
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("after resume: %v", job)
	}
	r.requireOnce("after resume")
}

// A latched job found at start is finished whatever tiering says of its old
// file: reads already use the new files.
func TestRecoveryFinishesALatchedJobWhateverTieringSays1825(t *testing.T) {
	for name, cerr := range map[string]error{
		"a cold copy":     fmt.Errorf("%w: x", tiering.ErrRehomeColdCopy),
		"cannot be asked": fmt.Errorf("%w: x", tiering.ErrColdCheckFailed),
	} {
		t.Run(name, func(t *testing.T) {
			r := newSwapRig(t)
			stopAt(t, r, "latched", 1)
			f := withTiering1825(t, r)
			f.checkErr = cerr
			if err := r.h.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			id := r.reg.Jobs()[0].ID
			if job := r.wait(id); job["state"] != "done" {
				t.Fatalf("job: %v", job)
			}
			if r.sourceThere() || r.reg.Active() || len(f.hot()) != 3 {
				t.Fatalf("after recovery: source there %v, active %v, rows %v", r.sourceThere(), r.reg.Active(), f.hot())
			}
			r.requireOnce("after recovery")
		})
	}
}

// A resume of a job that has not switched reads, when cold storage turns out
// to hold its file: the job is undone, not finished.
func TestResumeUndoesAJobWhoseFileColdStorageHolds1825(t *testing.T) {
	r := newSwapRig(t)
	f := withTiering1825(t, r)
	f.recErr = errors.New("database is locked") // stops the job committed, outputs in place
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "stopped" {
		t.Fatalf("job: %v", job)
	}
	f.mu.Lock()
	f.recErr, f.checkErr = nil, fmt.Errorf("%w: x", tiering.ErrRehomeColdCopy)
	f.mu.Unlock()
	if code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+id+"/resume", "", ""); code != 202 {
		t.Fatalf("resume: %d %v", code, out)
	}
	if job := r.wait(id); job["state"] != "refused" || !strings.Contains(fmt.Sprint(job["reason"]), "cold storage holds it too") {
		t.Fatalf("job: %v", job)
	}
	if _, _, files := r.reads(); r.reg.Active() || len(files) != 1 || files[0] != swapKey {
		t.Fatalf("registry active %v, files %v; want the job undone and the file as it was", r.reg.Active(), files)
	}
	if rows := f.hot(); len(rows) != 1 || rows[0] != swapKey {
		t.Fatalf("rows after the undo: %v", rows)
	}
	r.requireOnce("after the undo")
}
