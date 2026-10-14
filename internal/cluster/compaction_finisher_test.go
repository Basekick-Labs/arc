package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
)

// #1207 d2a2b: the staged compaction commit on an object store, cluster side.

// fakeObjectStore is an object store over a local directory: objects record
// a checksum as metadata, and copies run in this process.
type fakeObjectStore struct {
	*storage.LocalBackend
	mu         sync.Mutex
	meta       map[string]string
	failDelete map[string]error
	onCopy     func()
	copyErr    error
	copies     int
}

var _ storage.ObjectCopier = (*fakeObjectStore)(nil)

func (f *fakeObjectStore) Type() string { return "s3" }

func (f *fakeObjectStore) WriteReaderWithSHA(ctx context.Context, path string, r io.Reader, size int64, sha string) error {
	if err := f.LocalBackend.WriteReader(ctx, path, r, size); err != nil {
		return err
	}
	f.mu.Lock()
	f.meta[path] = sha
	f.mu.Unlock()
	return nil
}

func (f *fakeObjectStore) CopyObject(ctx context.Context, src, dst string, size int64, sha string) error {
	var buf bytes.Buffer
	if err := f.LocalBackend.ReadTo(ctx, src, &buf); err != nil {
		return err
	}
	if err := f.WriteReaderWithSHA(ctx, dst, &buf, size, sha); err != nil {
		return err
	}
	f.mu.Lock()
	f.copies++
	hook, cerr := f.onCopy, f.copyErr
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return cerr
}

func (f *fakeObjectStore) ObjectSHA(ctx context.Context, path string) (int64, string, error) {
	size, err := f.LocalBackend.StatFile(ctx, path)
	if err != nil || size < 0 {
		return size, "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return size, f.meta[path], nil
}

func (f *fakeObjectStore) Delete(ctx context.Context, path string) error {
	f.mu.Lock()
	err := f.failDelete[path]
	f.mu.Unlock()
	if err != nil {
		return err
	}
	f.mu.Lock()
	delete(f.meta, path)
	f.mu.Unlock()
	return f.LocalBackend.Delete(ctx, path)
}

type objectRigT struct {
	*stagedRigT
	store *fakeObjectStore
}

// objectRig: stagedRig with an object store.
func objectRig(t *testing.T) *objectRigT {
	t.Helper()
	r := stagedRig(t)
	store := &fakeObjectStore{LocalBackend: r.backend, meta: map[string]string{}, failDelete: map[string]error{}}
	r.c.storage = store
	return &objectRigT{stagedRigT: r, store: store}
}

// objectJob stages an object job over two fresh inputs and commits it.
func (r *objectRigT) objectJob(id string) compaction.StagedJob {
	r.t.Helper()
	job := r.job(id)
	job.Object = true
	body, err := os.ReadFile(filepath.Join(r.root, job.StagedKey))
	if err != nil {
		r.t.Fatal(err)
	}
	r.store.mu.Lock()
	r.store.meta[job.StagedKey] = job.Output.SHA256
	r.store.mu.Unlock()
	_ = body
	sc := r.c.StagedCommitter()
	ctx := context.Background()
	if out, err := sc.Stage(ctx, job); out != compaction.StagedOK {
		r.t.Fatalf("stage: %v %v", out, err)
	}
	if out, err := sc.Commit(ctx, job); out != compaction.StagedOK {
		r.t.Fatalf("commit: %v %v", out, err)
	}
	if st := sc.Status(job); !st.Finalizing || !st.Committed {
		r.t.Fatalf("after the commit: %+v", st)
	}
	return job
}

func (r *objectRigT) staged(job string) (raft.StagedCompaction, bool) {
	return r.c.raftNode.FSM().StagedCompactionByJob(job)
}

func (r *objectRigT) deleteFile(path, reason string) {
	r.t.Helper()
	raw, _ := json.Marshal(raft.DeleteFilePayload{Path: path, Reason: reason})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandDeleteFile, Payload: raw}, 5*time.Second); err != nil {
		r.t.Fatal(err)
	}
}

// The normal end: the output copied into place with its checksum, the input
// objects and the staged object deleted, the job ended.
func TestFinishObjectJob(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended")
	}
	if size, sha, _ := r.store.ObjectSHA(context.Background(), job.Output.Path); size != job.Output.SizeBytes || sha != job.Output.SHA256 {
		t.Fatalf("output at its path: %d %q", size, sha)
	}
	for _, in := range job.Inputs {
		if r.exists(in.Path) {
			t.Fatalf("input %s still in the bucket", in.Path)
		}
	}
	if r.exists(job.StagedKey) {
		t.Fatal("staged object left")
	}
	if e, ok := r.c.raftNode.FSM().GetFile(job.Output.Path); !ok || e.CompactionJob != "j1" {
		t.Fatal("output not registered")
	}
}

// A failure at any step leaves the job finalizing; the next run finishes it
// without copying again.
func TestFinishObjectJobResumes(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.store.failDelete[job.Inputs[1].Path] = errors.New("store down")
	if err := r.c.finishObjectJob(context.Background(), "j1"); err == nil {
		t.Fatal("finished with an input delete failing")
	}
	if s, ok := r.staged("j1"); !ok || !s.Finalizing || s.Finalized {
		t.Fatalf("after a failed delete: %+v %v", s, ok)
	}
	delete(r.store.failDelete, job.Inputs[1].Path)
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok || r.store.copies != 1 {
		t.Fatalf("resumed: copies %d", r.store.copies)
	}
}

// Another node's live lease: nothing happens here.
func TestFinishObjectJobLease(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	raw, _ := json.Marshal(raft.ClaimFinisherPayload{Job: "j1", By: "writer-1", Now: time.Now().UTC()})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandClaimFinisher, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); !errors.Is(err, errFinisherLease) {
		t.Fatalf("err %v", err)
	}
	if r.store.copies != 0 || !r.exists(job.Inputs[0].Path) {
		t.Fatal("acted without the lease")
	}
}

// The output left the manifest before the finisher ran. A DELETE accounts
// for its rows: the inputs go, nothing is registered again. The reconciler's
// orphan sweep does not: the inputs are registered again.
func TestFinishObjectJobOutputLeft(t *testing.T) {
	for _, tc := range []struct {
		reason string
		revert bool
	}{{"delete-rewrite", false}, {"retention:3", false}, {"reconcile-orphan-manifest", true}} {
		r := objectRig(t)
		job := r.objectJob("j1")
		r.deleteFile(job.Output.Path, tc.reason)
		if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
			t.Fatalf("%s: %v", tc.reason, err)
		}
		if _, ok := r.staged("j1"); ok {
			t.Fatalf("%s: job not ended", tc.reason)
		}
		if r.exists(job.StagedKey) || r.exists(job.Output.Path) || r.store.copies != 0 {
			t.Fatalf("%s: staged or final object left, or copied", tc.reason)
		}
		_, registered := r.c.raftNode.FSM().GetFile(job.Inputs[0].Path)
		if registered != tc.revert || r.exists(job.Inputs[0].Path) != tc.revert {
			t.Fatalf("%s: input registered %v, in the bucket %v", tc.reason, registered, r.exists(job.Inputs[0].Path))
		}
	}
}

// The output left during the copy (a DELETE removed it): the copy just made
// is removed again, and the inputs go.
func TestFinishObjectJobOutputLeftDuringTheCopy(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.store.onCopy = func() { r.deleteFile(job.Output.Path, "delete") }
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok || r.exists(job.Output.Path) || r.exists(job.Inputs[0].Path) {
		t.Fatal("left behind")
	}
}

// No staged object to copy: the inputs are registered again (their objects
// are all there); with one missing too, the job is wedged for an operator,
// who can end it.
func TestFinishObjectJobWithoutAStagedObject(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	if err := r.store.Delete(context.Background(), job.StagedKey); err != nil {
		t.Fatal(err)
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(job.Inputs[0].Path); !ok {
		t.Fatal("inputs not registered again")
	}
	if _, ok := r.c.raftNode.FSM().GetFile(job.Output.Path); ok {
		t.Fatal("output still registered")
	}

	job2 := r.objectJob("j2")
	_ = r.store.Delete(context.Background(), job2.StagedKey)
	_ = r.store.Delete(context.Background(), job2.Inputs[0].Path)
	if err := r.c.finishObjectJob(context.Background(), "j2"); err != nil {
		t.Fatal(err)
	}
	s, ok := r.staged("j2")
	if !ok || !s.Wedged {
		t.Fatalf("not wedged: %+v %v", s, ok)
	}
	if n := r.c.FinalizingCompactions(); n != 0 {
		t.Fatalf("a wedged job holds the pause: %d", n)
	}
	if st := r.c.CompactionCommitStatus(); len(st.Wedged) != 1 || st.Wedged[0] != "j2" {
		t.Fatalf("status %+v", st)
	}
	if err := r.c.AbortWedgedCompaction(context.Background(), "j1"); !errors.Is(err, ErrCompactionJobNotFound) {
		t.Fatalf("ended job: %v", err)
	}
	if err := r.c.AbortWedgedCompaction(context.Background(), "j2"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j2"); ok {
		t.Fatal("wedged job not aborted")
	}
	job3 := r.objectJob("j3")
	if err := r.c.AbortWedgedCompaction(context.Background(), "j3"); !errors.Is(err, ErrCompactionJobNotWedged) {
		t.Fatalf("finalizing job: %v", err)
	}
	_ = job3
}

// An input path the manifest lists again (a restore) is not deleted.
func TestFinishObjectJobKeepsAReregisteredInput(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	e := raft.FileEntry{Path: job.Inputs[0].Path, SHA256: job.Inputs[0].SHA256, SizeBytes: job.Inputs[0].SizeBytes,
		Database: "testdb", Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if !r.exists(job.Inputs[0].Path) || r.exists(job.Inputs[1].Path) {
		t.Fatal("deleted a registered input, or kept an unregistered one")
	}
}

// What the DELETE, backup, tiering and the reconciler read of a finalizing
// job.
func TestFinalizingObjectJobViews(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	if ins := r.c.FinalizingObjectInputs(job.Output.Path); len(ins) != 2 {
		t.Fatalf("inputs %v", ins)
	}
	if key, ok := r.c.StagedOutputKey(job.Output.Path); !ok || key != job.StagedKey {
		t.Fatalf("staged key %q %v", key, ok)
	}
	if !r.c.CompactionOwns(job.Inputs[0].Path) || !r.c.CompactionOwns(job.Output.Path) || r.c.CompactionOwns(stHour+"other.parquet") {
		t.Fatal("owns")
	}
	if !r.c.CompactionOutputHeld(job.Output.Path) {
		t.Fatal("the output not held while finalizing")
	}
	// Held by the job itself, not by its staged object being there.
	body, _ := os.ReadFile(filepath.Join(r.root, job.StagedKey))
	_ = os.Remove(filepath.Join(r.root, job.StagedKey))
	if !r.c.CompactionOutputHeld(job.Output.Path) {
		t.Fatal("the output not held once its staged object is gone")
	}
	r.write(job.StagedKey, string(body))
	if r.c.FinalizingCompactions() != 1 {
		t.Fatal("finalizing count")
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if ins := r.c.FinalizingObjectInputs(job.Output.Path); len(ins) != 0 {
		t.Fatalf("inputs after the end %v", ins)
	}
	if _, ok := r.c.StagedOutputKey(job.Output.Path); ok {
		t.Fatal("staged key after the end")
	}
}

// The leader's sweep removes old staged objects no job owns, and keeps a
// job's.
func TestSweepStagedObjects(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	orphan := compaction.StagedKey("gone", stHour+"x.parquet")
	young := compaction.StagedKey("young", stHour+"y.parquet")
	r.write(orphan, "old")
	r.write(young, "new")
	old := time.Now().Add(-objectStagedSweepAge - time.Hour)
	for _, k := range []string{orphan, job.StagedKey} {
		if err := os.Chtimes(filepath.Join(r.root, k), old, old); err != nil {
			t.Fatal(err)
		}
	}
	r.c.sweepStagedObjects(context.Background())
	if r.exists(orphan) || !r.exists(young) || !r.exists(job.StagedKey) {
		t.Fatalf("orphan %v young %v job %v", r.exists(orphan), r.exists(young), r.exists(job.StagedKey))
	}
}

// The output path registered again by something else after the output left:
// the finisher does not delete that object.
func TestFinishObjectJobKeepsAReusedOutputPath(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.deleteFile(job.Output.Path, "delete")
	r.write(job.Output.Path, "someone else's")
	e := raft.FileEntry{Path: job.Output.Path, SHA256: "other", SizeBytes: int64(len("someone else's")), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if !r.exists(job.Output.Path) {
		t.Fatal("deleted an object the manifest lists")
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended")
	}
}

// A staged object at the right size but another checksum is not the output:
// nothing is copied, the inputs are registered again.
func TestFinishObjectJobRefusesAStagedObjectWithAnotherChecksum(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.store.mu.Lock()
	r.store.meta[job.StagedKey] = "other"
	r.store.mu.Unlock()
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if r.store.copies != 0 || r.exists(job.Output.Path) {
		t.Fatal("copied a staged object with another checksum")
	}
	if _, ok := r.c.raftNode.FSM().GetFile(job.Inputs[0].Path); !ok {
		t.Fatal("inputs not registered again")
	}
}

// An input path registered again is not handed to a DELETE to remove.
func TestFinalizingObjectInputsSkipsAReregisteredInput(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	e := raft.FileEntry{Path: job.Inputs[0].Path, SHA256: job.Inputs[0].SHA256, SizeBytes: job.Inputs[0].SizeBytes,
		Database: "testdb", Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if ins := r.c.FinalizingObjectInputs(job.Output.Path); len(ins) != 1 || ins[0] != job.Inputs[1].Path {
		t.Fatalf("inputs %v", ins)
	}
}

// Who finishes a job: its compactor and its lease holder at once, the
// leader once it has been neither committed nor claimed for the takeover
// time; nobody a wedged job.
func TestFinisherEligible(t *testing.T) {
	now := time.Now()
	s := raft.StagedCompaction{Job: "j1", Origin: "w1", Object: true, Finalizing: true, FinalizingAt: now.Add(-time.Minute)}
	for _, tc := range []struct {
		name   string
		mod    func(*raft.StagedCompaction)
		self   string
		leader bool
		want   bool
	}{
		{"origin", nil, "w1", false, true},
		{"other", nil, "w2", false, false},
		{"leader early", nil, "w2", true, false},
		{"leader late", func(s *raft.StagedCompaction) { s.FinalizingAt = now.Add(-objectFinisherTakeover - time.Second) }, "w2", true, true},
		{"leader, lease renewed", func(s *raft.StagedCompaction) {
			s.FinalizingAt, s.FinisherNode, s.ClaimedAt = now.Add(-time.Hour), "w1", now.Add(-time.Minute)
		}, "w2", true, false},
		{"lease holder", func(s *raft.StagedCompaction) { s.FinisherNode = "w3" }, "w3", false, true},
		{"wedged", func(s *raft.StagedCompaction) { s.Wedged = true }, "w1", true, false},
		{"not finalizing", func(s *raft.StagedCompaction) { s.Finalizing = false }, "w1", true, false},
		{"local", func(s *raft.StagedCompaction) { s.Object = false }, "w1", true, false},
	} {
		x := s
		if tc.mod != nil {
			tc.mod(&x)
		}
		if got := finisherEligible(x, tc.self, tc.leader, now); got != tc.want {
			t.Fatalf("%s: %v", tc.name, got)
		}
	}
}

// The lease is renewed during a long copy; a renewal refused (another node
// took the job) cancels the copy, and what it left at the output's path is
// removed once the job no longer owns it.
func TestFinishObjectJobLosesTheLeaseDuringTheCopy(t *testing.T) {
	old := finisherRenewEvery
	finisherRenewEvery = 50 * time.Millisecond
	t.Cleanup(func() { finisherRenewEvery = old })
	r := objectRig(t)
	job := r.objectJob("j1")
	r.store.onCopy = func() {
		// While the copy runs: another node takes the expired lease and ends
		// the job (a DELETE removed the output).
		r.deleteFile(job.Output.Path, "delete")
		raw, _ := json.Marshal(raft.ClaimFinisherPayload{Job: "j1", By: "writer-1", Now: time.Now().UTC().Add(time.Hour)})
		if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandClaimFinisher, Payload: raw}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		raw, _ = json.Marshal(raft.CleanupCompactionPayload{Job: "j1", By: "writer-1", InputsDeleted: true})
		if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandCleanupCompaction, Payload: raw}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond) // a renewal falls in the copy
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); err == nil {
		t.Fatal("finished without the lease")
	}
	if r.exists(job.Output.Path) {
		t.Fatal("a copy made after the job ended is left readable")
	}
}

// A copy whose renewal fails mid-way is cancelled.
func TestWithLeaseCancelsOnARefusedRenewal(t *testing.T) {
	old := finisherRenewEvery
	finisherRenewEvery = 20 * time.Millisecond
	t.Cleanup(func() { finisherRenewEvery = old })
	r := objectRig(t)
	r.objectJob("j1")
	raw, _ := json.Marshal(raft.ClaimFinisherPayload{Job: "j1", By: "writer-1", Now: time.Now().UTC()})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandClaimFinisher, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	err := r.c.withLease(context.Background(), "j1", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	})
	if err == nil || !strings.Contains(err.Error(), "not renewed") {
		t.Fatalf("err %v", err)
	}
}

// A copy that fails while the job still owns its output leaves what it wrote
// for the next run to verify; it is not removed as a stray.
func TestFinishObjectJobFailedCopyKeepsAnOwnedOutput(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.store.copyErr = errors.New("connection reset")
	if err := r.c.finishObjectJob(context.Background(), "j1"); err == nil {
		t.Fatal("finished with a failed copy")
	}
	if !r.exists(job.Output.Path) {
		t.Fatal("removed the output of a job that still owns it")
	}
	r.store.copyErr = nil
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended")
	}
}

// A revert only follows the removal of the output object: if that fails,
// nothing is registered again (a crash between the two must not leave the
// output readable next to the inputs).
func TestRevertObjectJobRemovesTheOutputFirst(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.write(job.Output.Path, "copied")
	_ = r.store.Delete(context.Background(), job.StagedKey)
	r.store.failDelete[job.Output.Path] = errors.New("store down")
	if err := r.c.finishObjectJob(context.Background(), "j1"); err == nil {
		t.Fatal("reverted with the output object still there")
	}
	if _, ok := r.c.raftNode.FSM().GetFile(job.Inputs[0].Path); ok {
		t.Fatal("inputs registered again before the output object went")
	}
}

// An output replaced in place by another registration: the job cannot be
// reverted and is wedged, and the object now registered at that path is
// left alone.
func TestFinishObjectJobOutputReplacedInPlace(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.write(job.Output.Path, "someone else's")
	e := raft.FileEntry{Path: job.Output.Path, SHA256: "other", SizeBytes: int64(len("someone else's")), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	s, ok := r.staged("j1")
	if !ok || !s.Wedged {
		t.Fatalf("not wedged: %+v %v", s, ok)
	}
	if !r.exists(job.Output.Path) {
		t.Fatal("deleted the object registered at the output path")
	}
}

// Aborting an object job that never committed removes its staged object.
func TestAbortObjectJobRemovesItsStagedObject(t *testing.T) {
	r := objectRig(t)
	job := r.job("j1")
	job.Object = true
	sc := r.c.StagedCommitter()
	if out, err := sc.Stage(context.Background(), job); out != compaction.StagedOK {
		t.Fatalf("stage: %v %v", out, err)
	}
	if err := sc.Abort(context.Background(), "j1", "test"); err != nil {
		t.Fatal(err)
	}
	if r.exists(job.StagedKey) {
		t.Fatal("staged object kept")
	}
}
