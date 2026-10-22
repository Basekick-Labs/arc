package compaction

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1207 d2a2b: the watcher's staged commit on an object store.

// objStore is an object store over a local directory recording checksums.
type objStore struct {
	*storage.LocalBackend
	mu     sync.Mutex
	meta   map[string]string
	shaErr error
}

func (o *objStore) Type() string { return "s3" }
func (o *objStore) WriteReaderWithSHA(ctx context.Context, p string, r io.Reader, size int64, sha string) error {
	if err := o.LocalBackend.WriteReader(ctx, p, r, size); err != nil {
		return err
	}
	o.mu.Lock()
	o.meta[p] = sha
	o.mu.Unlock()
	return nil
}
func (o *objStore) CopyObject(context.Context, string, string, int64, string) error { return nil }
func (o *objStore) ObjectSHA(ctx context.Context, p string) (int64, string, error) {
	if o.shaErr != nil {
		return -1, "", o.shaErr
	}
	size, err := o.LocalBackend.StatFile(ctx, p)
	if err != nil || size < 0 {
		return size, "", err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return size, o.meta[p], nil
}

type objectStagedRig struct {
	*stagedRig
	store    *objStore
	finished int
}

// newObjectStagedRig: newStagedRig with an object store and an object job.
func newObjectStagedRig(t *testing.T, created time.Time) *objectStagedRig {
	t.Helper()
	r := newStagedRig(t)
	store := &objStore{LocalBackend: r.backend, meta: map[string]string{r.job.StagedKey: r.job.Output.SHA256}}
	or := &objectStagedRig{stagedRig: r, store: store}
	r.w.cfg.Storage = store
	r.w.cfg.OnFinished = func() { or.finished++ }
	r.job.Object, r.job.CreatedAt = true, created
	r.fc.status = StagedStatus{}
	o := r.job.Output
	if err := writeCompletionManifest(r.dir, &CompletionManifest{JobID: stagedJob, State: CompletionStateStaged,
		StagedKey: r.job.StagedKey, StagedOutput: &o, Inputs: r.job.Inputs, StagedObject: true,
		CreatedAt: created, UpdatedAt: created}); err != nil {
		t.Fatal(err)
	}
	return or
}

// Staged and committed in one step (no holders); the observers run once at
// the commit; the inputs and the staged object stay for the finisher; the
// manifest goes once the cluster ended the job.
func TestStagedObjectCommit(t *testing.T) {
	r := newObjectStagedRig(t, time.Now())
	r.poll()
	if r.calls() != "stage "+stagedJob+"|commit "+stagedJob {
		t.Fatalf("calls %s", r.calls())
	}
	if len(r.before) != 1 || len(r.committed) != 1 {
		t.Fatalf("observers %v %v", r.before, r.committed)
	}
	if !r.exists(r.job.StagedKey) || !r.exists(r.job.Inputs[0].Path) || !r.manifestKept() {
		t.Fatal("removed what the finisher owns, or the manifest")
	}
	if n, _, _ := r.w.PendingCommits(); n != 1 {
		t.Fatalf("pending commits %d", n)
	}
	r.poll() // still finalizing
	if len(r.committed) != 1 || !r.manifestKept() || r.finished != 0 {
		t.Fatal("acted while finalizing")
	}
	r.fc.status = StagedStatus{Committed: true} // the finisher ended it
	r.poll()
	if r.manifestKept() || r.finished != 1 || len(r.committed) != 1 {
		t.Fatalf("manifest %v finished %d committed %d", r.manifestKept(), r.finished, len(r.committed))
	}
	if n, _, _ := r.w.PendingCommits(); n != 0 {
		t.Fatalf("pending commits %d", n)
	}
	// Committed here, ended with the output gone: the manifest goes too.
	r2 := newObjectStagedRig(t, time.Now())
	r2.poll()
	r2.fc.status = StagedStatus{}
	r2.poll()
	if r2.manifestKept() || !r2.exists(r2.job.Inputs[0].Path) {
		t.Fatal("ended job: manifest kept, or an input deleted")
	}
}

// Every end before the commit drops the staged object and the manifest and
// never touches an input object.
func TestStagedObjectJobEnds(t *testing.T) {
	cases := map[string]struct {
		run   func(r *objectStagedRig)
		abort bool
	}{
		"stage refused":  {func(r *objectStagedRig) { r.fc.stageOut = StagedRefused; r.poll() }, false},
		"commit refused": {func(r *objectStagedRig) { r.fc.commitOut = StagedRefused; r.poll() }, true},
		"paused": {func(r *objectStagedRig) {
			r.fc.commitOut = StagedRetry
			r.poll()
			r.paused = true
			r.poll()
		}, true},
		"dropped by the cluster": {func(r *objectStagedRig) {
			r.fc.commitOut = StagedRetry
			r.poll()
			r.fc.status.Staged = false
			r.poll()
		}, false},
		"checksum differs": {func(r *objectStagedRig) { r.store.meta[r.job.StagedKey] = "other"; r.poll() }, false},
		"staged object gone": {func(r *objectStagedRig) {
			_ = os.Remove(filepath.Join(r.root, r.job.StagedKey))
			r.poll()
		}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := newObjectStagedRig(t, time.Now())
			tc.run(r)
			if r.manifestKept() || r.exists(r.job.StagedKey) {
				t.Fatalf("manifest %v staged %v (calls %s)", r.manifestKept(), r.exists(r.job.StagedKey), r.calls())
			}
			for _, in := range r.job.Inputs {
				if !r.exists(in.Path) {
					t.Fatalf("input %s removed", in.Path)
				}
			}
			if tc.abort != strings.Contains(r.calls(), "abort") {
				t.Fatalf("abort expected %v: %s", tc.abort, r.calls())
			}
		})
	}
}

// A staged output older than stagedObjectMaxAge is never staged: the bucket
// sweep may be removing it.
func TestStagedObjectTooOldIsNotStaged(t *testing.T) {
	r := newObjectStagedRig(t, time.Now().Add(-stagedObjectMaxAge-time.Minute))
	r.poll()
	if strings.Contains(r.calls(), "stage") || r.manifestKept() || r.exists(r.job.StagedKey) {
		t.Fatalf("calls %s", r.calls())
	}
}

// A pause does not touch a committed job: it drains through it.
func TestStagedObjectFinalizingThroughAPause(t *testing.T) {
	r := newObjectStagedRig(t, time.Now())
	r.poll()
	r.paused = true
	r.poll()
	if strings.Contains(r.calls(), "abort") || !r.manifestKept() || !r.exists(r.job.StagedKey) {
		t.Fatalf("calls %s", r.calls())
	}
}

// A stage answered "committed" (an earlier run committed it) re-reads the
// job: still finalizing, the manifest stays.
func TestStagedObjectStageAnsweredCommitted(t *testing.T) {
	r := newObjectStagedRig(t, time.Now())
	r.fc.stageOut = StagedCommitted
	r.fc.afterStage = &StagedStatus{Committed: true, Finalizing: true}
	r.poll()
	if !r.manifestKept() || r.finished != 0 {
		t.Fatalf("manifest %v finished %d", r.manifestKept(), r.finished)
	}
}

// A failed check of the staged output is retried, not taken for damage.
func TestStagedObjectCheckFailureRetries(t *testing.T) {
	r := newObjectStagedRig(t, time.Now())
	r.store.shaErr = errors.New("store unreachable")
	r.poll()
	if !r.manifestKept() || !r.exists(r.job.StagedKey) || strings.Contains(r.calls(), "stage") {
		t.Fatalf("calls %s", r.calls())
	}
	r.store.shaErr = nil
	r.poll()
	if !strings.Contains(r.calls(), "commit") {
		t.Fatalf("calls %s", r.calls())
	}
}
