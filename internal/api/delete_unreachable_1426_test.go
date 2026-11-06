package api

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// #1426: with allow_unreachable a DELETE still waited the full
// deleteLocalWait (30 s) for a manifest file no node holds any more, once per
// scan, before answering 200. It now gives such a file up after
// deleteGiveUpAfter, once per request, and keeps waiting for a file
// something is still delivering.

// shortGiveUp sets deleteGiveUpAfter for one test.
func shortGiveUp(t *testing.T, d time.Duration) {
	t.Helper()
	old := deleteGiveUpAfter
	deleteGiveUpAfter = d
	t.Cleanup(func() { deleteGiveUpAfter = old })
}

func TestMissingTracker_Issue1426(t *testing.T) {
	shortGiveUp(t, 5*time.Second)
	arriving := map[string]bool{"b": true}
	tr := &missingTracker{arriving: func(p string) bool { return arriving[p] }, since: map[string]time.Time{}, givenUp: map[string]bool{}}
	t0 := time.Unix(1000, 0)

	if got := tr.waitFor([]string{"a", "b"}, t0); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("first sight: %v", got)
	}
	// Short of deleteGiveUpAfter: both still waited for.
	if got := tr.waitFor([]string{"a", "b"}, t0.Add(4*time.Second)); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("before the give-up time: %v", got)
	}
	// Past it: a, which nothing delivers, is given up; b, arriving, is not.
	if got := tr.waitFor([]string{"a", "b"}, t0.Add(5*time.Second)); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("after the give-up time: %v", got)
	}
	if !tr.givenUp["a"] || tr.givenUp["b"] {
		t.Fatalf("given up %v, want a only", tr.givenUp)
	}
	// b stops arriving (its pull failed for good): given up at once, its
	// clock having started at first sight.
	arriving["b"] = false
	if got := tr.waitFor([]string{"b"}, t0.Add(6*time.Second)); len(got) != 0 {
		t.Fatalf("b no longer arriving: %v", got)
	}
	// A file seen for the first time late starts its own clock: other files'
	// history does not give it up.
	if got := tr.waitFor([]string{"c"}, t0.Add(60*time.Second)); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("new file: %v", got)
	}
	// Reported: what is missing now plus what was given up, within keep.
	if got := tr.withGivenUp([]string{"c"}, func(p string) bool { return p != "b" }); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("withGivenUp: %v", got)
	}

	// A nil tracker (no flag) waits for everything.
	var none *missingTracker
	if got := none.waitFor([]string{"a"}, t0.Add(time.Hour)); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("nil tracker: %v", got)
	}
	// Without an arrival checker nothing counts as arriving.
	if tr := newMissingTracker(nil); tr.arriving != nil {
		t.Fatal("arrival checker from nil")
	}
	if tr := newMissingTracker(&fsmCoordinator{}); tr.arriving == nil {
		t.Fatal("the coordinator's FileArriving is not used")
	}
}

// With compaction commit on, the scan's own wait gives up a manifest file no
// node holds after deleteGiveUpAfter, not deleteLocalWait.
func TestDeleteAllowUnreachableStopsWaitingForAFileNothingDelivers_Issue1426(t *testing.T) {
	old := deleteLocalWait
	deleteLocalWait = 10 * time.Second
	t.Cleanup(func() { deleteLocalWait = old })
	shortGiveUp(t, 200*time.Millisecond)
	_, h, root := casCluster(t, casA, casO)
	if err := os.Remove(filepath.Join(root, casO)); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true,"allow_unreachable":true}`)
	took := time.Since(began)
	if code != 200 || out.DeletedCount != 2 || !slices.Equal(out.UnreachableFiles, []string{casO}) {
		t.Fatalf("%d: %s", code, raw)
	}
	if took > 3*time.Second {
		t.Fatalf("took %v: waited for a file nothing is delivering", took)
	}
}

// A missing file something is still delivering is waited for up to
// deleteLocalWait, as without the flag.
func TestDeleteAllowUnreachableWaitsForAFileOnItsWay_Issue1426(t *testing.T) {
	old := deleteLocalWait
	deleteLocalWait = 1500 * time.Millisecond
	t.Cleanup(func() { deleteLocalWait = old })
	shortGiveUp(t, 100*time.Millisecond)
	c, h, root := casCluster(t, casA, casO)
	if err := os.Remove(filepath.Join(root, casO)); err != nil {
		t.Fatal(err)
	}
	c.arriving = map[string]bool{casO: true}
	began := time.Now()
	code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true,"allow_unreachable":true}`)
	took := time.Since(began)
	if code != 200 || !slices.Equal(out.UnreachableFiles, []string{casO}) {
		t.Fatalf("%d: %s", code, raw)
	}
	if took < deleteLocalWait {
		t.Fatalf("took %v: gave up a file still on its way before deleteLocalWait", took)
	}
}

// With stream takeover, the waits before and after recording share one
// account: a file given up in the first is not waited for in the second.
func TestDeleteAllowUnreachableGivesUpOncePerRequest_Issue1426(t *testing.T) {
	old := deleteLocalWait
	deleteLocalWait = 10 * time.Second
	t.Cleanup(func() { deleteLocalWait = old })
	shortGiveUp(t, 300*time.Millisecond)
	root, backend, db := recordsDuckDB(t)
	writeRecordsParquet(t, db, root, "db/cpu/2026/10/11/10/cpu_a.parquet")
	events := &[]string{}
	c := &recordingCoordinator{active: true, events: events, missing: []string{"db/cpu/2026/10/11/10/gone.parquet"}}
	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	h.SetCoordinator(c)
	h.SetFlusher(&recordingFlusher{events: events})

	began := time.Now()
	code, out, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true,"allow_unreachable":true}`)
	took := time.Since(began)
	if code != 200 || len(out.UnreachableFiles) != 1 {
		t.Fatalf("%d: %s", code, raw)
	}
	if took > 2*deleteGiveUpAfter+time.Second {
		t.Fatalf("took %v: the second wait waited for the file the first gave up", took)
	}
	// The record was made, and the wait after it checked once: the file
	// given up before it is not polled for again.
	joined := strings.Join(*events, "|")
	before, after, recorded := strings.Cut(joined, "record db/cpu")
	if !recorded || strings.Count(before, "wait db/cpu") < 2 || strings.Count(after, "wait db/cpu") != 1 {
		t.Fatalf("events %s", joined)
	}
}
