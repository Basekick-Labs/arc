package api

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1551: a cold DELETE or retention run is bracketed from before its first
// object delete until after it retired the rows, so the cold sync's
// missing-object check never counts a row it is about to retire.

type removalWindow struct {
	active, begun int
	atRetire      []int
}

func (w *removalWindow) BeginColdRemoval() { w.active++; w.begun++ }
func (w *removalWindow) EndColdRemoval()   { w.active-- }

type guardedDeleteColdTier struct {
	*fakeDeleteColdTier
	*removalWindow
}

func (g guardedDeleteColdTier) RetireColdRows(ctx context.Context, paths []string) (int, error) {
	g.atRetire = append(g.atRetire, g.active)
	return g.fakeDeleteColdTier.RetireColdRows(ctx, paths)
}

type guardedRetentionColdTier struct {
	*fakeColdTier
	*removalWindow
}

func (g guardedRetentionColdTier) RetireColdRows(ctx context.Context, paths []string) (int, error) {
	g.atRetire = append(g.atRetire, g.active)
	return g.fakeColdTier.RetireColdRows(ctx, paths)
}

func checkWindow(t *testing.T, w *removalWindow, atDelete []int) {
	t.Helper()
	if w.begun == 0 || w.active != 0 {
		t.Fatalf("begun %d, still active %d: want a closed bracket", w.begun, w.active)
	}
	if len(w.atRetire) == 0 || w.atRetire[0] != 1 {
		t.Fatalf("active at row retirement = %v, want 1", w.atRetire)
	}
	if len(atDelete) == 0 || atDelete[0] != 1 {
		t.Fatalf("active at the object delete = %v, want 1", atDelete)
	}
}

// edgeWindow records whether the cold file still existed when the bracket
// began and ended: it must begin before the object delete and end after it.
type edgeWindow struct {
	*removalWindow
	exists                func() bool
	existedAtBegin, atEnd []bool
}

func (e *edgeWindow) BeginColdRemoval() {
	e.existedAtBegin = append(e.existedAtBegin, e.exists())
	e.removalWindow.BeginColdRemoval()
}

func (e *edgeWindow) EndColdRemoval() {
	e.atEnd = append(e.atEnd, e.exists())
	e.removalWindow.EndColdRemoval()
}

type edgeDeleteColdTier struct {
	guardedDeleteColdTier
	*edgeWindow
}

func (g edgeDeleteColdTier) BeginColdRemoval() { g.edgeWindow.BeginColdRemoval() }
func (g edgeDeleteColdTier) EndColdRemoval()   { g.edgeWindow.EndColdRemoval() }

func TestColdDeleteIsBracketedForTheMissingObjectCheck(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0", "h0")
	w := &removalWindow{}
	e := &edgeWindow{removalWindow: w, exists: func() bool { return r.coldExists(coldWhole) }}
	r.h.coldTier = edgeDeleteColdTier{guardedDeleteColdTier{r.tier, w}, e}

	if code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`); code != 200 || !out.Success {
		t.Fatalf("got %d %+v", code, out)
	}
	if r.coldExists(coldWhole) {
		t.Fatal("cold file not deleted")
	}
	if len(e.existedAtBegin) == 0 || !e.existedAtBegin[0] || e.atEnd[0] {
		t.Fatalf("object present at begin %v, at end %v: want the bracket around the delete", e.existedAtBegin, e.atEnd)
	}
	checkWindow(t, w, []int{1})
}

func TestColdRetentionIsBracketedForTheMissingObjectCheck(t *testing.T) {
	r := newColdRetentionRig(t)
	r.cold(t, "cpu", "testdb/cpu/2020/01/01/00/expired.parquet", expiredTS)
	w := &removalWindow{}
	r.h.coldTier = guardedRetentionColdTier{r.tier, w}
	var atDelete []int
	r.h.coldDeleteHook = func(ctx context.Context, b storage.Backend, path string) error {
		atDelete = append(atDelete, w.active)
		return b.Delete(ctx, path)
	}

	if _, err := r.h.ExecutePolicy(context.Background(), r.policy(t, "")); err != nil {
		t.Fatal(err)
	}
	checkWindow(t, w, atDelete)
}

// A dry run deletes nothing and is not bracketed.
func TestColdRetentionDryRunIsNotBracketed(t *testing.T) {
	r := newColdRetentionRig(t)
	w := &removalWindow{}
	r.h.coldTier = guardedRetentionColdTier{r.tier, w}
	c := &coldRetention{backend: r.tier.backend}
	if _, _, err := r.h.deleteOldColdFiles(context.Background(), c, "cpu", time.Now(), true); err != nil {
		t.Fatal(err)
	}
	if w.begun != 0 {
		t.Fatalf("a dry run began %d removal brackets", w.begun)
	}
}

// A tier without the guard (the test fakes, any other implementation) runs
// unbracketed.
func TestBeginColdRemovalWithoutAGuard(t *testing.T) {
	beginColdRemoval(&fakeColdTier{})()
	beginColdRemoval(nil)()
}
