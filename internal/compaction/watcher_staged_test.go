package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1207 d2a2a: the watcher's staged commit, against a fake cluster.

type fakeCommitter struct {
	backend    *storage.LocalBackend
	status     StagedStatus
	stageOut   StagedOutcome
	commitOut  StagedOutcome
	syncErr    error
	adoptErr   error
	registered map[string]bool
	held       map[string]bool
	own        []string
	calls      []string
	// afterStage: the status once a stage applies (a commit by an earlier
	// run of this job).
	afterStage *StagedStatus
}

func (f *fakeCommitter) Stage(_ context.Context, job StagedJob) (StagedOutcome, error) {
	f.calls = append(f.calls, "stage "+job.JobID)
	if f.stageOut == StagedOK {
		f.status.Staged = true
	}
	if f.afterStage != nil {
		f.status = *f.afterStage
	}
	return f.stageOut, errors.New("stage answer")
}
func (f *fakeCommitter) Commit(_ context.Context, job StagedJob) (StagedOutcome, error) {
	f.calls = append(f.calls, "commit "+job.JobID)
	if f.commitOut == StagedOK || f.commitOut == StagedCommitted {
		f.status = StagedStatus{Committed: true, Finalizing: job.Object}
		for _, in := range job.Inputs {
			delete(f.registered, in.Path)
		}
	}
	return f.commitOut, errors.New("commit answer")
}
func (f *fakeCommitter) Abort(_ context.Context, job, reason string) error {
	f.calls = append(f.calls, "abort "+job)
	f.status.Staged = false
	return nil
}
func (f *fakeCommitter) Status(StagedJob) StagedStatus      { return f.status }
func (f *fakeCommitter) SyncManifest(context.Context) error { return f.syncErr }
func (f *fakeCommitter) Registered(p string) bool           { return f.registered[p] }
func (f *fakeCommitter) HoldsRegistration(p string) bool    { return f.held[p] }
func (f *fakeCommitter) Adopt(ctx context.Context, job StagedJob) error {
	f.calls = append(f.calls, "adopt "+job.JobID)
	if f.adoptErr != nil {
		return f.adoptErr
	}
	return f.backend.MoveObject(ctx, job.StagedKey, job.Output.Path)
}
func (f *fakeCommitter) ForcePull(job StagedJob) { f.calls = append(f.calls, "forcepull "+job.JobID) }
func (f *fakeCommitter) OwnStaged() []string     { return f.own }

type stagedRig struct {
	t         *testing.T
	root, dir string
	backend   *storage.LocalBackend
	fc        *fakeCommitter
	w         *CompletionWatcher
	paused    bool
	before    []string
	committed [][]string
	commitErr error
	job       StagedJob
}

const stagedJob = "db_2026_10_13_10_1_b1"

func newStagedRig(t *testing.T) *stagedRig {
	t.Helper()
	r := &stagedRig{t: t, root: t.TempDir(), dir: t.TempDir()}
	b, err := storage.NewLocalBackend(r.root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	r.backend = b
	r.fc = &fakeCommitter{backend: b, registered: map[string]bool{}, held: map[string]bool{}, status: StagedStatus{MinHolders: 1}}
	w, err := NewCompletionWatcher(CompletionWatcherConfig{
		Dir: r.dir, Bridge: &fakeBridge{}, PollInterval: time.Hour, Logger: zerolog.Nop(),
		PauseGate: func() bool { return r.paused },
		Staged:    r.fc, Storage: b,
		BeforeCommit: func(o string) { r.before = append(r.before, o) },
		OnCommitted: func(in []string) error {
			if r.commitErr != nil {
				return r.commitErr
			}
			r.committed = append(r.committed, in)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.w = w
	// Two inputs on disk, a staged output, and its manifest.
	out := "db/cpu/2026/10/13/10/cpu_o.parquet"
	var inputs []StagedInput
	for _, n := range []string{"a", "b"} {
		p := "db/cpu/2026/10/13/10/cpu_" + n + ".parquet"
		r.write(p, "input "+n)
		inputs = append(inputs, StagedInput{Path: p, SizeBytes: int64(len("input " + n)), SHA256: "x"})
		r.fc.registered[p] = true
	}
	key := StagedKey(stagedJob, out)
	r.write(key, "the output")
	sum := sha256.Sum256([]byte("the output"))
	r.job = StagedJob{JobID: stagedJob, StagedKey: key, Inputs: inputs, Output: CompactedOutput{
		Path: out, SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len("the output")), Database: "db", Measurement: "cpu"}}
	o := r.job.Output
	if err := writeCompletionManifest(r.dir, &CompletionManifest{JobID: stagedJob, State: CompletionStateStaged,
		StagedKey: key, StagedOutput: &o, Inputs: inputs, CreatedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *stagedRig) write(p, body string) {
	r.t.Helper()
	full := filepath.Join(r.root, p)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

func (r *stagedRig) exists(p string) bool {
	_, err := os.Stat(filepath.Join(r.root, p))
	return err == nil
}

func (r *stagedRig) manifestKept() bool { return r.w.HasStagedJob(stagedJob) }

func (r *stagedRig) poll() { r.w.poll(context.Background()) }

func (r *stagedRig) calls() string { return strings.Join(r.fc.calls, "|") }

func TestStagedCommitHappyPath(t *testing.T) {
	r := newStagedRig(t)
	r.poll()
	if r.calls() != "stage "+stagedJob {
		t.Fatalf("first poll: %s", r.calls())
	}
	if !r.w.LocallyStagedInput(r.job.Inputs[0].Path) {
		t.Fatal("inputs of a local staged manifest not reported")
	}
	r.poll() // no holder yet: waits
	if r.calls() != "stage "+stagedJob {
		t.Fatalf("committed without a holder: %s", r.calls())
	}
	r.fc.status.Holders = 1
	r.poll()
	want := "stage " + stagedJob + "|commit " + stagedJob + "|adopt " + stagedJob
	if r.calls() != want {
		t.Fatalf("calls %s", r.calls())
	}
	if !r.exists(r.job.Output.Path) || r.exists(StagedJobDir(stagedJob)) {
		t.Fatal("output not in place, or staged directory kept")
	}
	for _, in := range r.job.Inputs {
		if r.exists(in.Path) {
			t.Fatalf("input %s kept", in.Path)
		}
	}
	if len(r.before) != 1 || r.before[0] != r.job.Output.Path || len(r.committed) != 1 || len(r.committed[0]) != 2 {
		t.Fatalf("observers: %v %v", r.before, r.committed)
	}
	if r.manifestKept() {
		t.Fatal("manifest kept after the commit")
	}
	if n, _, _ := r.w.PendingCommits(); n != 0 {
		t.Fatalf("pending commits %d", n)
	}
}

// An input the manifest lists again, or this node's outbox owns, is not the
// job's to remove.
func TestStagedCommitKeepsInputsOthersOwn(t *testing.T) {
	r := newStagedRig(t)
	r.fc.status = StagedStatus{Committed: true}
	delete(r.fc.registered, r.job.Inputs[1].Path)
	r.fc.held[r.job.Inputs[1].Path] = true
	r.poll()
	if !r.exists(r.job.Inputs[0].Path) || !r.exists(r.job.Inputs[1].Path) || r.manifestKept() {
		t.Fatalf("inputs %v %v manifest %v", r.exists(r.job.Inputs[0].Path), r.exists(r.job.Inputs[1].Path), r.manifestKept())
	}
}

func TestStagedJobEnds(t *testing.T) {
	cases := map[string]func(r *stagedRig){
		"stage refused": func(r *stagedRig) { r.fc.stageOut = StagedRefused; r.poll() },
		"paused while staged": func(r *stagedRig) {
			r.poll()
			r.paused = true
			r.poll()
		},
		"dropped by the cluster": func(r *stagedRig) {
			r.poll()
			r.fc.status.Staged = false
			r.poll()
		},
		"staged copy missing": func(r *stagedRig) {
			_ = os.Remove(filepath.Join(r.root, r.job.StagedKey))
			r.poll()
		},
		"staged copy damaged": func(r *stagedRig) { r.write(r.job.StagedKey, "the 0utput"); r.poll() },
		"commit refused": func(r *stagedRig) {
			r.poll()
			r.fc.status.Holders = 1
			r.fc.commitOut = StagedRefused
			r.poll()
		},
		"no holder in time": func(r *stagedRig) {
			r.poll()
			r.w.stagedJobs[stagedJob].stagedAt = time.Now().Add(-stagedHolderWait - time.Minute)
			r.poll()
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			r := newStagedRig(t)
			run(r)
			if r.manifestKept() || r.exists(StagedJobDir(stagedJob)) {
				t.Fatalf("manifest %v staged dir %v (calls %s)", r.manifestKept(), r.exists(StagedJobDir(stagedJob)), r.calls())
			}
			for _, in := range r.job.Inputs {
				if !r.exists(in.Path) {
					t.Fatalf("input %s removed", in.Path)
				}
			}
			if r.exists(r.job.Output.Path) {
				t.Fatal("output at its final path")
			}
			needsAbort := name == "paused while staged" || name == "commit refused" || name == "no holder in time"
			if needsAbort != strings.Contains(r.calls(), "abort") {
				t.Fatalf("abort expected %v: %s", needsAbort, r.calls())
			}
		})
	}
}

func TestStagedJobWaits(t *testing.T) {
	r := newStagedRig(t)
	r.fc.stageOut = StagedRetry
	r.poll()
	r.fc.syncErr = errors.New("no leader")
	r.poll()
	r.fc.syncErr = nil
	r.fc.stageOut = StagedOK
	r.poll()
	r.fc.status.Holders = 1
	r.fc.commitOut = StagedRetry
	r.poll()
	if !r.manifestKept() || !r.exists(r.job.StagedKey) || r.calls() != "stage "+stagedJob+"|stage "+stagedJob+"|commit "+stagedJob {
		t.Fatalf("calls %s", r.calls())
	}
	// A deferred stage under a pause is not retried: the job ends.
	r2 := newStagedRig(t)
	r2.paused = true
	r2.poll()
	if r2.manifestKept() || strings.Contains(r2.calls(), "stage") {
		t.Fatalf("paused: %s", r2.calls())
	}
}

// Committed, but the output cannot be adopted here: kept, and pulled from a
// holder after a while, once.
func TestStagedCommitPullsAnOutputItCannotAdopt(t *testing.T) {
	r := newStagedRig(t)
	r.fc.status = StagedStatus{Committed: true}
	r.fc.registered = map[string]bool{}
	r.fc.adoptErr = errors.New("gone")
	r.poll()
	if !r.manifestKept() || strings.Contains(r.calls(), "forcepull") {
		t.Fatalf("calls %s", r.calls())
	}
	r.w.stagedJobs[stagedJob].committedAt = time.Now().Add(-stagedForcePullAfter - time.Second)
	r.poll()
	r.poll()
	if strings.Count(r.calls(), "forcepull") != 1 {
		t.Fatalf("calls %s", r.calls())
	}
	// Pulled again a period later: the first pull may have been dropped.
	r.w.stagedJobs[stagedJob].forcedAt = time.Now().Add(-stagedForcePullAfter - time.Second)
	r.poll()
	if strings.Count(r.calls(), "forcepull") != 2 {
		t.Fatalf("not pulled again: %s", r.calls())
	}
	if n, _, _ := r.w.PendingCommits(); n != 1 {
		t.Fatalf("pending commits %d, want the committing job", n)
	}
	// The output arrives (pulled): the job finishes.
	r.write(r.job.Output.Path, "the output")
	r.poll()
	if r.manifestKept() {
		t.Fatal("manifest kept once the output arrived")
	}
}

// An observer failure keeps the job; the next poll finishes it.
func TestStagedCommitRetriesItsObservers(t *testing.T) {
	r := newStagedRig(t)
	r.fc.status = StagedStatus{Committed: true}
	r.fc.registered = map[string]bool{}
	r.commitErr = errors.New("ledger down")
	r.poll()
	if !r.manifestKept() {
		t.Fatal("manifest removed with the observers failed")
	}
	r.commitErr = nil
	r.poll()
	if r.manifestKept() || len(r.committed) != 1 {
		t.Fatalf("manifest %v committed %v", r.manifestKept(), r.committed)
	}
}

// Jobs this node staged and holds no manifest for are aborted; one it holds
// a manifest for is not.
func TestStagedOrphansAborted(t *testing.T) {
	r := newStagedRig(t)
	r.fc.status.Staged = true
	r.fc.status.Holders = 0
	r.fc.own = []string{stagedJob, "lost_job"}
	r.poll()
	if !strings.Contains(r.calls(), "abort lost_job") || strings.Contains(r.calls(), "abort "+stagedJob) {
		t.Fatalf("calls %s", r.calls())
	}
	n := strings.Count(r.calls(), "abort lost_job")
	r.poll() // paced
	if strings.Count(r.calls(), "abort lost_job") != n {
		t.Fatalf("orphan sweep not paced: %s", r.calls())
	}
}

func TestStagedManifestMalformed(t *testing.T) {
	r := newStagedRig(t)
	o := r.job.Output
	// A staged key the job id does not name, holding the very output.
	r.write("elsewhere/x.parquet", "the output")
	if err := writeCompletionManifest(r.dir, &CompletionManifest{JobID: stagedJob, State: CompletionStateStaged,
		StagedKey: "elsewhere/x.parquet", StagedOutput: &o, Inputs: r.job.Inputs}); err != nil {
		t.Fatal(err)
	}
	r.poll()
	if r.manifestKept() || strings.Contains(r.calls(), "stage") {
		t.Fatalf("malformed: %s", r.calls())
	}
	for _, in := range r.job.Inputs {
		if !r.exists(in.Path) {
			t.Fatal("input removed")
		}
	}
}

// The staged copy damaged after the stage: no commit, the job ends.
func TestStagedCopyVerifiedBeforeTheCommit(t *testing.T) {
	r := newStagedRig(t)
	r.poll()
	r.write(r.job.StagedKey, "the 0utput")
	r.w.stagedJobs[stagedJob].verified = false
	r.fc.status.Holders = 1
	r.poll()
	if strings.Contains(r.calls(), "commit") || !strings.Contains(r.calls(), "abort") || r.manifestKept() {
		t.Fatalf("calls %s", r.calls())
	}
}

// A job that ends finds an input the manifest no longer lists (a commit of it
// before a crash, then a DELETE of its output): the input's local copy goes,
// or the node reads rows the cluster deleted. One the outbox owns, or the
// manifest lists, stays.
func TestStagedEndRemovesInputsOthersRemoved(t *testing.T) {
	r := newStagedRig(t)
	delete(r.fc.registered, r.job.Inputs[0].Path)
	delete(r.fc.registered, r.job.Inputs[1].Path)
	r.fc.held[r.job.Inputs[1].Path] = true
	r.fc.stageOut = StagedRefused
	r.poll()
	if r.exists(r.job.Inputs[0].Path) || !r.exists(r.job.Inputs[1].Path) || r.manifestKept() {
		t.Fatalf("input a %v, b %v, manifest %v", r.exists(r.job.Inputs[0].Path), r.exists(r.job.Inputs[1].Path), r.manifestKept())
	}
	// Not caught up with the manifest: nothing removed, retried.
	r2 := newStagedRig(t)
	delete(r2.fc.registered, r2.job.Inputs[0].Path)
	r2.fc.stageOut = StagedRefused
	calls := 0
	r2.fc.syncErr = nil
	r2.w.cfg.Staged = &syncFailsAfter{fakeCommitter: r2.fc, n: 1, calls: &calls}
	r2.poll()
	if !r2.exists(r2.job.Inputs[0].Path) || !r2.manifestKept() {
		t.Fatal("ended without catching up with the manifest")
	}
}

// syncFailsAfter fails SyncManifest after its first n calls.
type syncFailsAfter struct {
	*fakeCommitter
	n     int
	calls *int
}

func (s *syncFailsAfter) SyncManifest(context.Context) error {
	*s.calls++
	if *s.calls > s.n {
		return errors.New("no leader")
	}
	return nil
}

// Verified once by checksum, then truncated: the size check still catches it
// before the commit.
func TestStagedCopyTruncatedAfterItsCheck(t *testing.T) {
	r := newStagedRig(t)
	r.poll()
	if !r.w.stagedJobs[stagedJob].verified {
		t.Fatal("precondition: verified at the stage")
	}
	r.write(r.job.StagedKey, "the out")
	r.fc.status.Holders = 1
	r.poll()
	if strings.Contains(r.calls(), "commit") || r.manifestKept() {
		t.Fatalf("calls %s", r.calls())
	}
}
