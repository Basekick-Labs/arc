package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1207: a DELETE on a shared object store scans what a query reads there,
// the bucket listing, and a manifest entry whose object is gone is gone.

type sharedCoordinator struct {
	*fsmCoordinator
	held     map[string]bool // finalizing outputs
	owned    map[string]bool // staged compaction inputs and outputs
	noAdopt  bool
	rewrites []string
}

var _ sharedObjectScanner = (*sharedCoordinator)(nil)

func (c *sharedCoordinator) SharedObjectStore() bool            { return true }
func (c *sharedCoordinator) ManifestAdoption() bool             { return !c.noAdopt }
func (c *sharedCoordinator) CompactionOwns(p string) bool       { return c.owned[p] }
func (c *sharedCoordinator) CompactionOutputHeld(p string) bool { return c.held[p] }
func (c *sharedCoordinator) AdoptRewrite(_ context.Context, p string, size int64, sha string) (raft.FileEntry, error) {
	e := casEntry(c.t, c.root, p)
	if e.SizeBytes != size || e.SHA256 != sha {
		c.t.Errorf("rewrite %s registered as %d %s, is %d %s", p, size, sha, e.SizeBytes, e.SHA256)
	}
	if err := c.apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: e, Adopt: true}); err != nil {
		return raft.FileEntry{}, err
	}
	c.rewrites = append(c.rewrites, p)
	return e, nil
}

func sharedCluster(t *testing.T, files ...string) (*sharedCoordinator, *DeleteHandler, string) {
	t.Helper()
	c, h, root := casCluster(t, files...)
	s := &sharedCoordinator{fsmCoordinator: c, held: map[string]bool{}, owned: map[string]bool{}}
	h.SetCoordinator(s)
	return s, h, root
}

const casHour = "db/cpu/2026/10/11/10/"

// onDiskPaths is onDisk as storage paths.
func onDiskPaths(t *testing.T, root string) []string {
	var out []string
	for _, n := range onDisk(t, root) {
		out = append(out, casHour+n)
	}
	return out
}

const deleteH1 = `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`

// A listed file whose object is gone is not waited for, unless it is a
// finalizing compaction output (copied into place by its finisher).
func TestDeleteSharedStoreGoneEntry(t *testing.T) {
	shortDeleteWait(t)
	c, h, root := sharedCluster(t, casA, casO)
	if err := os.Remove(filepath.Join(root, casO)); err != nil {
		t.Fatal(err)
	}
	c.held[casO] = true
	if code, out, raw := sendDelete(t, h, deleteH1); code != 503 || len(out.UnreachableFiles) != 1 {
		t.Fatalf("finalizing output: %d %s", code, raw)
	}
	c.held[casO] = false
	code, out, raw := sendDelete(t, h, deleteH1)
	if code != 200 || out.DeletedCount != 2 || len(out.UnreachableFiles) != 0 {
		t.Fatalf("%d %s", code, raw)
	}
}

// An unlisted object is scanned (queries read it): its rewrite is registered
// (else the reconciler would remove it with the rows kept), the object
// deleted; deleted whole when every row matches, with no manifest change.
func TestDeleteSharedStoreUnlistedObject(t *testing.T) {
	c, h, root := sharedCluster(t, casA)
	writeRecordsParquet(t, c.db, root, casB)
	ageObject(t, root, casB)
	code, out, raw := sendDelete(t, h, deleteH1)
	if code != 200 || out.DeletedCount != 4 || out.AffectedFiles != 2 {
		t.Fatalf("%d %s", code, raw)
	}
	if len(c.rewrites) != 1 {
		t.Fatalf("rewrites registered: %v", c.rewrites)
	}
	for _, p := range onDiskPaths(t, root) {
		if p == casB {
			t.Fatal("the unlisted object was kept")
		}
		if _, reg := c.fsm.GetFile(p); !reg {
			t.Fatalf("%s not registered", p)
		}
		if n := countParquet(t, c.db, filepath.Join(root, p), "host = 'h1'"); n != 0 {
			t.Fatalf("%s keeps deleted rows", p)
		}
	}
	writeRecordsParquet(t, c.db, root, casO)
	ageObject(t, root, casO)
	code, _, raw = sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"v > 0","confirm":true}`)
	if code != 200 {
		t.Fatalf("whole: %d %s", code, raw)
	}
	if left := onDisk(t, root); len(left) != 0 {
		t.Fatalf("left %v", left)
	}
	if len(c.rewrites) != 1 {
		t.Fatalf("a whole-file delete registered something: %v", c.rewrites)
	}
}

// ageObject backdates an object past unregisteredFreshAge: no writer's
// outbox registers it without looking at it again.
func ageObject(t *testing.T, root, p string) {
	t.Helper()
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(root, p), old, old); err != nil {
		t.Fatal(err)
	}
}

// #1265: an unlisted object younger than unregisteredFreshAge is another
// writer's flush its outbox may register without looking at it again:
// rewritten or removed now, it would be registered after it is gone. It is
// left as it is and the DELETE answers 503; once older it is deleted from.
func TestDeleteSharedStoreLeavesAFreshUnlistedObject(t *testing.T) {
	for _, where := range []string{"host = 'h1'", "v > 0"} { // a rewrite, and a whole-file delete
		t.Run(where, func(t *testing.T) {
			c, h, root := sharedCluster(t, casA)
			writeRecordsParquet(t, c.db, root, casB)
			before := countParquet(t, c.db, filepath.Join(root, casB), where)
			body := `{"database":"db","measurement":"cpu","where":"` + where + `","confirm":true}`
			code, out, raw := sendDelete(t, h, body)
			if code != 503 || len(out.FailedFiles) != 1 || out.FailedFiles[0] != "cpu_b.parquet" {
				t.Fatalf("%d %s", code, raw)
			}
			if n := countParquet(t, c.db, filepath.Join(root, casB), where); n != before {
				t.Fatalf("the fresh object was changed: %d matching rows left", n)
			}
			if len(c.rewrites) != 0 {
				t.Fatalf("rewrites registered: %v", c.rewrites)
			}
			ageObject(t, root, casB)
			if code, _, raw := sendDelete(t, h, body); code != 200 {
				t.Fatalf("aged: %d %s", code, raw)
			}
			if _, err := os.Stat(filepath.Join(root, casB)); err == nil {
				t.Fatal("the aged object was kept")
			}
		})
	}
}

// Where a rewrite could not be registered (stream claims on), an unlisted
// object is left alone, as before #1207.
func TestDeleteSharedStoreWithoutAdoption(t *testing.T) {
	c, h, root := sharedCluster(t, casA)
	c.noAdopt = true
	writeRecordsParquet(t, c.db, root, casB)
	if code, out, raw := sendDelete(t, h, deleteH1); code != 200 || out.AffectedFiles != 1 {
		t.Fatalf("%d %s", code, raw)
	}
	if n := countParquet(t, c.db, filepath.Join(root, casB), "host = 'h1'"); n != 2 {
		t.Fatalf("rewritten: %d", n)
	}
}

// The input of a finalizing compaction is out of the manifest on purpose:
// its rows are in the output, and its finisher removes it.
func TestDeleteSharedStoreSkipsAFinalizingInput(t *testing.T) {
	c, h, root := sharedCluster(t, casA)
	writeRecordsParquet(t, c.db, root, casB)
	c.owned[casB] = true
	if code, out, raw := sendDelete(t, h, deleteH1); code != 200 || out.AffectedFiles != 1 {
		t.Fatalf("%d %s", code, raw)
	}
	if n := countParquet(t, c.db, filepath.Join(root, casB), "host = 'h1'"); n != 2 {
		t.Fatalf("the input was rewritten: %d", n)
	}
}
