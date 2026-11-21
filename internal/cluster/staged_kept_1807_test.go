package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
)

// faultyStore is the rig's local backend with MoveObject and StatFile that
// can be made to fail.
type faultyStore struct {
	*storage.LocalBackend
	failMove atomic.Bool
	failStat atomic.Bool
	moves    atomic.Int64
}

func (f *faultyStore) MoveObject(ctx context.Context, src, dst string) error {
	f.moves.Add(1)
	if f.failMove.Load() {
		return errors.New("injected move failure")
	}
	return f.LocalBackend.MoveObject(ctx, src, dst)
}

func (f *faultyStore) StatFile(ctx context.Context, path string) (int64, error) {
	if f.failStat.Load() {
		return 0, errors.New("injected stat failure")
	}
	return f.LocalBackend.StatFile(ctx, path)
}

func faultyRig(t *testing.T) (*stagedRigT, *faultyStore) {
	t.Helper()
	r := stagedRig(t)
	f := &faultyStore{LocalBackend: r.backend}
	r.c.storage = f
	return r, f
}

// A committed output whose staged copy here could not be moved into place
// is kept for the next sweep, not removed (#1807): it may be the only copy.
// The rig wires no compactor hook, as on a node whose watcher did not build.
func TestStagedCopyKeptWhenTheMoveFails(t *testing.T) {
	r, f := faultyRig(t)
	sc := r.c.StagedCommitter()
	ctx := context.Background()
	job := r.job("j1")
	if out, _ := sc.Stage(ctx, job); out != compaction.StagedOK {
		t.Fatal("stage")
	}
	s, _ := r.c.raftNode.FSM().StagedCompactionByJob("j1")
	r.held("j1", "reader-1", job.Output.SHA256)
	if out, _ := sc.Commit(ctx, job); out != compaction.StagedOK {
		t.Fatal("commit")
	}
	f.failMove.Store(true)

	r.c.endStagedCopy(s)
	if !r.exists(job.StagedKey) {
		t.Fatal("endStagedCopy removed the only copy of a committed output after a failed move")
	}
	r.c.sweepStagedDirs(ctx)
	if !r.exists(job.StagedKey) {
		t.Fatal("the sweep removed the only copy of a committed output after a failed move")
	}

	// A failed check keeps it too.
	f.failMove.Store(false)
	f.failStat.Store(true)
	r.c.sweepStagedDirs(ctx)
	if !r.exists(job.StagedKey) {
		t.Fatal("removed on a failed stat")
	}

	// The next sweep that can move it does.
	f.failStat.Store(false)
	r.c.sweepStagedDirs(ctx)
	if !r.exists(job.Output.Path) || r.exists(compaction.StagedJobDir("j1")) {
		t.Fatal("the kept copy was not moved into place by the next sweep")
	}
}

// Removed on positive proof only: a partial copy, an output that left the
// manifest, an output the path already holds.
func TestStagedCopyRemovedWhenUseless(t *testing.T) {
	r, f := faultyRig(t)
	ctx := context.Background()
	f.failMove.Store(true)

	partial := r.job("partial")
	r.commitJob(partial)
	if err := os.Truncate(filepath.Join(r.root, partial.StagedKey), 3); err != nil {
		t.Fatal(err)
	}

	gone := r.job("gone")
	r.commitJob(gone)
	if err := r.c.raftNode.DeleteFile(gone.Output.Path, "delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}

	present := r.job("present")
	r.commitJob(present)
	r.write(present.Output.Path, "output of present") // same bytes, same size

	r.c.sweepStagedDirs(ctx)
	for _, j := range []string{"partial", "gone", "present"} {
		if r.exists(compaction.StagedJobDir(j)) {
			t.Errorf("staged copy of %q kept", j)
		}
	}
}

// An output that left the manifest before the move is not moved: its
// partition directories are not recreated only to delete it again (#1807).
func TestAdoptStagedChecksTheManifestBeforeTheMove(t *testing.T) {
	r, f := faultyRig(t)
	ctx := context.Background()
	job := r.job("j1")
	r.commitJob(job)
	e, _ := r.c.raftNode.FSM().GetFile(job.Output.Path)
	if err := r.c.raftNode.DeleteFile(job.Output.Path, "delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	ok, err := r.c.adoptStaged(ctx, e)
	if ok || err == nil {
		t.Fatalf("adoptStaged = %v, %v; want false with an error", ok, err)
	}
	if n := f.moves.Load(); n != 0 {
		t.Fatalf("MoveObject called %d times for an output that left the manifest", n)
	}
	if r.exists(job.Output.Path) {
		t.Fatal("output moved into place")
	}
}

// A stale copy replaced between the identity check and the rename is put
// back; when a remover took the emptied partition directory in between, the
// directory is recreated and the link retried, not the file kept aside for a
// manual move (#1807).
func TestStaleLocalPutBackRecreatesARemovedDirectory(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	full := filepath.Join(r.root, key)
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.pass()

	var replaced, removed bool
	staleBeforeAside = func(p string) {
		if p != full || replaced {
			return
		}
		replaced = true
		_ = os.Remove(p)
		r.write(key, "rows A") // same size, new file: put back after the rename
	}
	origLink := staleLinkFn
	staleLinkFn = func(old, new string) error {
		if new == full && !removed {
			removed = true
			if err := os.Remove(filepath.Dir(new)); err != nil {
				t.Errorf("remove the emptied partition directory: %v", err)
			}
		}
		return origLink(old, new)
	}
	t.Cleanup(func() { staleBeforeAside, staleLinkFn = nil, origLink })

	r.pass()
	if !replaced || !removed {
		t.Fatalf("put-back path not reached (replaced %v, removed %v)", replaced, removed)
	}
	got, err := os.ReadFile(full)
	if err != nil || string(got) != "rows A" {
		t.Fatalf("file at the path = %q, %v; want the replacement put back", got, err)
	}
	kept, _ := filepath.Glob(filepath.Join(r.root, staleLocalAsideDir, staleLocalKeepAside+"*"))
	if len(kept) != 0 {
		t.Fatalf("file kept aside: %v", kept)
	}
}
