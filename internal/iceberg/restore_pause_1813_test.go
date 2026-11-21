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
	if _, err := e.holdDatabase("other"); !errors.Is(err, errDatabaseBeingDropped) || !errors.Is(err, errExportPaused) {
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
