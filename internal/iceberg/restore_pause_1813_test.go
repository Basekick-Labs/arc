package iceberg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// A restore that stages a catalog pauses every export and drop (#1813): it
// waits for the exports in flight, then keeps new ones and drops out until
// resumed.
func TestPauseExportForRestore(t *testing.T) {
	e := newPassRig(t, 1, 1, 1).exp
	ctx := context.Background()

	inFlight, err := e.holdDatabase("db")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan func(), 1)
	go func() {
		resume, err := e.PauseExportForRestore(ctx, "restore b1")
		if err != nil {
			t.Error(err)
		}
		got <- resume
	}()
	select {
	case <-got:
		t.Fatal("the pause was taken while an export was running")
	case <-time.After(50 * time.Millisecond):
	}
	inFlight()
	var resume func()
	select {
	case resume = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the pause was not taken once the export ended")
	}

	// An export gives way at once, as to a drop.
	began := time.Now()
	if _, err := e.holdDatabase("other"); !errors.Is(err, errExportPaused) || errors.Is(err, errDatabaseBeingDropped) {
		t.Fatalf("an export while paused: %v", err)
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("an export waited %v for the pause", took)
	}
	// A drop is refused and says to repeat it after the restart.
	if _, err := e.lockDatabaseForDrop(ctx, "db"); !errors.Is(err, errExportPaused) || !strings.Contains(err.Error(), "after the restart") {
		t.Fatalf("a drop while paused: %v", err)
	}

	resume()
	resume() // idempotent
	rel, err := e.holdDatabase("db")
	if err != nil {
		t.Fatalf("an export after the pause was lifted: %v", err)
	}
	rel()
	if rel, err := e.lockDatabaseForDrop(ctx, "db"); err != nil {
		t.Fatalf("a drop after the pause was lifted: %v", err)
	} else {
		rel()
	}
}

// A pause that cannot be taken in time is refused and leaves nothing held.
func TestPauseExportForRestoreGivesUp(t *testing.T) {
	e := newPassRig(t, 1, 1, 1).exp
	old := restorePauseWait
	restorePauseWait = 50 * time.Millisecond
	t.Cleanup(func() { restorePauseWait = old })
	rel, err := e.holdDatabase("db")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.PauseExportForRestore(context.Background(), "restore b1"); err == nil {
		t.Fatal("paused while an export ran past the wait")
	}
	rel()
	other, err := e.holdDatabase("db")
	if err != nil {
		t.Fatalf("an export after a refused pause: %v", err)
	}
	other()
}

// A second restore before the restart is not held up by the first one's
// pause, which is never lifted (the last restore before the restart wins);
// its resume does not lift the first one's pause.
func TestPauseExportForRestoreTwice(t *testing.T) {
	e := newPassRig(t, 1, 1, 1).exp
	old := restorePauseWait
	restorePauseWait = 2 * time.Second
	t.Cleanup(func() { restorePauseWait = old })
	if _, err := e.PauseExportForRestore(context.Background(), "restore b1"); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	resume2, err := e.PauseExportForRestore(context.Background(), "restore b2")
	if err != nil {
		t.Fatalf("a second restore's pause: %v", err)
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("a second restore waited %v for the first one's pause", took)
	}
	resume2() // b2 failed before staging
	if _, err := e.holdDatabase("db"); !errors.Is(err, errExportPaused) {
		t.Fatalf("an export after the second restore's resume: %v, want still paused for b1", err)
	}
}

// While paused, a pass does nothing and the waiting ingest requests go: the
// restart reconciles every table.
func TestSchedulerSkipsWhilePausedForRestore(t *testing.T) {
	r := newPassRig(t, 1, 1, 1)
	if r.sched.pausedForRestore() {
		t.Fatal("paused before any restore")
	}
	if _, err := r.exp.PauseExportForRestore(context.Background(), "restore b1"); err != nil {
		t.Fatal(err)
	}
	if !r.sched.pausedForRestore() || !r.sched.pauseWarned {
		t.Fatal("not reported paused")
	}
	if partial := r.sched.runPass(context.Background()); partial {
		t.Fatal("a pass while paused reported partial work")
	}
}
