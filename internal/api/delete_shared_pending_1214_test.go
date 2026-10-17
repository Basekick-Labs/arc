package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/basekick-labs/arc/internal/storage"
)

// #1214: on a shared object store with a manifest and compaction commit off
// (a cluster mid-upgrade), a file the manifest does not list and this node is
// not registering is, as a rule, another writer's file still pending its
// registration. Replacing it left the rewrite unregistered, for the
// reconciler to remove with the rows the DELETE kept, and the writer's own
// registration of the original then settled as gone. The DELETE leaves it
// and answers 503 instead.

// legacySharedCoordinator is a cluster on a shared object store with a Raft
// manifest, compaction commit off.
type legacySharedCoordinator struct{ *fsmCoordinator }

func (c *legacySharedCoordinator) HasRaft() bool           { return true }
func (c *legacySharedCoordinator) SharedObjectStore() bool { return true }

func TestDeleteSharedStoreCommitOffLeavesAnUnregisteredFile(t *testing.T) {
	c, h, root := casCluster(t, casA)
	c.active = false // compaction commit off
	h.SetCoordinator(&legacySharedCoordinator{c})
	writeRecordsParquet(t, c.db, root, casB) // a peer's file, pending its registration

	code, out, raw := sendDelete(t, h, deleteH1)
	if code != 503 {
		t.Fatalf("want 503 for a file not registered yet, got %d %s", code, raw)
	}
	if len(out.FailedFiles) != 1 || out.FailedFiles[0] != filepath.Base(casB) {
		t.Fatalf("failed files %v, want the unregistered one: %s", out.FailedFiles, raw)
	}
	if out.DeletedCount != 2 || out.RewrittenFiles != 1 {
		t.Fatalf("the registered file was not processed: %s", raw)
	}
	// The original is kept whole and authoritative, and no rewrite of it
	// is left behind.
	if n := countParquet(t, c.db, filepath.Join(root, casB), "host = 'h1'"); n != 2 {
		t.Fatalf("the unregistered file was changed: %d rows of h1 left", n)
	}
	for _, p := range onDiskPaths(t, root) {
		if p == casB {
			continue
		}
		if _, reg := c.fsm.GetFile(p); !reg {
			t.Fatalf("%s is on disk and not registered: an orphan the reconciler removes", p)
		}
		if strings.Contains(p, "cpu_b") {
			t.Fatalf("a rewrite of the unregistered file was left: %s", p)
		}
	}
}

// Off a shared object store (or without a manifest) the legacy behaviour
// stays: the file is rewritten in place.
func TestDeleteCommitOffWithoutSharedStoreRewritesAnUnregisteredFile(t *testing.T) {
	c, h, root := casCluster(t, casA)
	c.active = false
	writeRecordsParquet(t, c.db, root, casB)
	code, _, raw := sendDelete(t, h, deleteH1)
	if code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	for _, p := range onDiskPaths(t, root) {
		if p == casB {
			t.Fatal("the unregistered original was kept")
		}
	}
}

// An unregistered object older than the reconciler's grace floor is an orphan
// to the reconciler too: rewritten as before, not a 503 every retry.
func TestDeleteSharedStoreCommitOffRewritesAnOldUnregisteredFile(t *testing.T) {
	c, h, root := casCluster(t, casA)
	c.active = false
	h.SetCoordinator(&legacySharedCoordinator{c})
	writeRecordsParquet(t, c.db, root, casB)
	old := time.Now().Add(-2 * reconciliation.MinGraceWindow)
	if err := os.Chtimes(filepath.Join(root, casB), old, old); err != nil {
		t.Fatal(err)
	}
	code, _, raw := sendDelete(t, h, deleteH1)
	if code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	for _, p := range onDiskPaths(t, root) {
		if p == casB {
			t.Fatal("the old unregistered original was kept")
		}
	}
}

// listless hides the store's ObjectLister, as a backend without one would.
type listless struct{ storage.Backend }

// Without a listing, the age comes from the flush time in the file name; a
// name that carries none, or a rewrite's, is of unknown age: 503's side.
func TestDeleteUnregisteredFileAgeFromItsName(t *testing.T) {
	h := &DeleteHandler{storage: listless{}}
	at := func(d time.Duration) string {
		ts := time.Now().UTC().Add(-d)
		return fmt.Sprintf("db/cpu/2026/10/11/10/cpu_%s_%09d.parquet", ts.Format("20060102_150405"), 123)
	}
	for name, tc := range map[string]struct {
		path string
		want bool
	}{
		"recent flush":    {at(time.Minute), true},
		"old flush":       {at(3 * time.Hour), false},
		"no time":         {"db/cpu/2026/10/11/10/cpu_b.parquet", true},
		"a rewrite of it": {rewritePath(at(3 * time.Hour)), true},
	} {
		if got := h.mayBePendingElsewhere(context.Background(), tc.path); got != tc.want {
			t.Errorf("%s (%s): %v, want %v", name, tc.path, got, tc.want)
		}
	}
}

// A real failure is not hidden behind the retryable 503 of files not
// registered yet.
func TestDeleteOutcomePrefersTheGeneralFailure(t *testing.T) {
	for _, tc := range []struct {
		failed, notRegistered int
		want                  bool
	}{
		{0, 0, true},
		{2, 2, true},
		{3, 2, false},
		{1, 0, false},
	} {
		if got := onlyNotRegistered(tc.failed, tc.notRegistered); got != tc.want {
			t.Errorf("failed %d, not registered %d: only not registered %v, want %v", tc.failed, tc.notRegistered, got, tc.want)
		}
	}
}
