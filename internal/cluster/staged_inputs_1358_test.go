package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeHolder is this node's registration outbox as the finisher sees it: one
// path's state, with a confirmation time compared as the outbox compares it.
type fakeHolder struct {
	mu        sync.Mutex
	path      string
	pending   bool
	parked    bool
	confirmed time.Time // zero: no recent confirmation
}

func (h *fakeHolder) HoldsRegistration(path string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if path != h.path {
		return false
	}
	return h.pending || h.parked || (!h.confirmed.IsZero() && time.Since(h.confirmed) < outboxRecentWindow)
}

func (h *fakeHolder) RegistrationHoldAt(path string, since time.Time) RegistrationHold {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case path != h.path:
		return HoldNone
	case h.pending:
		return HoldPending
	case h.parked:
		return HoldParked
	case !h.confirmed.IsZero() && time.Since(h.confirmed) < outboxRecentWindow && !h.confirmed.Before(since):
		return HoldRecent
	}
	return HoldNone
}

func (h *fakeHolder) set(f func(h *fakeHolder)) {
	h.mu.Lock()
	f(h)
	h.mu.Unlock()
}

func (r *objectRigT) holder(path string) *fakeHolder {
	h := &fakeHolder{path: path}
	r.c.SetStagedCompactionHooks(nil, h)
	return h
}

// #1358: an input whose registration this node's outbox confirmed seconds
// before the commit was kept, and the job ended anyway: its object stayed in
// the bucket unlisted, and every node read its rows next to the output's, for
// good. A confirmation recorded before the finisher's sync is of a
// registration the commit has since removed: the object goes.
func TestFinishObjectJobDeletesAnInputConfirmedBeforeTheCommit(t *testing.T) {
	r := objectRig(t)
	confirmed := time.Now()
	job := r.objectJob("j1")
	r.holder(job.Inputs[0].Path).set(func(h *fakeHolder) { h.confirmed = confirmed })

	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended")
	}
	for _, in := range job.Inputs {
		if r.exists(in.Path) {
			t.Fatalf("input %s left in the bucket after its job ended", in.Path)
		}
	}
}

// The time the rule compares against is taken BEFORE the sync with the
// leader. A registration confirmed while the sync runs may not be applied
// here yet; deleting its object would leave a live manifest entry without
// one. It defers the job instead, input kept, until it settles.
func TestFinishObjectJobDefersOnAConfirmationRecordedDuringTheSync(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	h := r.holder(job.Inputs[0].Path)
	prev := finisherSync
	t.Cleanup(func() { finisherSync = prev })
	finisherSync = func(ctx context.Context, c *Coordinator) error {
		h.set(func(h *fakeHolder) { h.confirmed = time.Now() })
		time.Sleep(2 * time.Millisecond)
		return prev(ctx, c)
	}

	err := r.c.finishObjectJob(context.Background(), "j1")
	if !errors.Is(err, errInputsSettling) {
		t.Fatalf("finish = %v, want errInputsSettling", err)
	}
	if _, ok := r.staged("j1"); !ok {
		t.Fatal("job ended while an input's registration was settling")
	}
	for _, in := range job.Inputs {
		if !r.exists(in.Path) {
			t.Fatalf("input %s deleted while a registration of the job's inputs was settling", in.Path)
		}
	}
}

// A pending registration of an input may still register it (kept) or be
// withdrawn (deleted): the job waits for it rather than ending with the object
// left, and finishes once it settles.
func TestFinishObjectJobWaitsForAPendingInputAndDeletesItOnceWithdrawn(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	h := r.holder(job.Inputs[0].Path)
	h.set(func(h *fakeHolder) { h.pending = true })

	if err := r.c.finishObjectJob(context.Background(), "j1"); !errors.Is(err, errInputsSettling) {
		t.Fatalf("finish = %v, want errInputsSettling", err)
	}
	if !r.exists(job.Inputs[0].Path) || !r.exists(job.Inputs[1].Path) {
		t.Fatal("an input was deleted while the job waits: every input is classified before any goes")
	}

	h.set(func(h *fakeHolder) { h.pending = false }) // withdrawn
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended once its input settled")
	}
	for _, in := range job.Inputs {
		if r.exists(in.Path) {
			t.Fatalf("input %s left after its pending registration was withdrawn", in.Path)
		}
	}
}

// A parked input waits for an operator: kept, and the job ends as before.
func TestFinishObjectJobKeepsAParkedInputAndEnds(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	r.holder(job.Inputs[0].Path).set(func(h *fakeHolder) { h.parked = true })

	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended")
	}
	if !r.exists(job.Inputs[0].Path) || r.exists(job.Inputs[1].Path) {
		t.Fatal("deleted a parked input, or kept an unparked one")
	}
}

// The outbox asks before registering a path again: an input of a committed
// job, with that content, is consumed; another content, a registered path or
// an ended job is not.
func TestConsumedByCompaction(t *testing.T) {
	r := objectRig(t)
	job := r.objectJob("j1")
	in := job.Inputs[0]
	if !r.c.ConsumedByCompaction(in.Path, in.SizeBytes, in.SHA256) {
		t.Fatal("an input of a committed job is not reported consumed")
	}
	if r.c.ConsumedByCompaction(in.Path, in.SizeBytes, "other") {
		t.Fatal("another content at the input's path is reported consumed")
	}
	if !r.c.ConsumedByCompaction(in.Path, in.SizeBytes, "") {
		t.Fatal("no checksum on the record: the size must decide")
	}
	if r.c.ConsumedByCompaction(job.Output.Path, job.Output.SizeBytes, job.Output.SHA256) {
		t.Fatal("the output is reported consumed")
	}
	if err := r.c.finishObjectJob(context.Background(), "j1"); err != nil {
		t.Fatal(err)
	}
	if r.c.ConsumedByCompaction(in.Path, in.SizeBytes, in.SHA256) {
		t.Fatal("an input of an ended job is still reported consumed")
	}
}

// The outbox's own answer, one case each.
func TestRegistrationHoldAt(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	before := time.Now().Add(-time.Second)
	since := time.Now()
	after := since.Add(time.Millisecond)
	r.mu.Lock()
	r.recent["old"] = recentConfirm{at: before}
	r.recent["new"] = recentConfirm{at: after}
	r.recent["stale"] = recentConfirm{at: since.Add(-2 * outboxRecentWindow)}
	r.parked["parked"] = &parkedEntry{}
	r.byPath["pending"] = &outboxEntry{}
	r.mu.Unlock()
	for path, want := range map[string]RegistrationHold{
		"old": HoldNone, "new": HoldRecent, "stale": HoldNone, "parked": HoldParked, "pending": HoldPending, "none": HoldNone,
	} {
		if got := r.RegistrationHoldAt(path, since); got != want {
			t.Errorf("%s: %v, want %v", path, got, want)
		}
	}
	// HoldsRegistration keeps its own view: any confirmation of the window.
	if !r.HoldsRegistration("old") {
		t.Error("HoldsRegistration changed: a confirmation of the last minute no longer holds")
	}
}

// consumingManifest is the outbox's manifest with a committed compaction that
// consumed one path.
type consumingManifest struct {
	*fakeOutboxManifest
	consumed string
}

func (m *consumingManifest) ConsumedByCompaction(path string, _ int64, _ string) bool {
	return path == m.consumed
}

// #1358 review (H3): a pending registration of a file a committed compaction
// consumed (an apply that timed out but committed, an ack lost and replayed)
// would register it again next to the output, and count its rows twice for
// good. It settles as registered instead, and nothing is sent.
func TestOutboxDoesNotRegisterAConsumedInputAgain(t *testing.T) {
	r := loadPending(t, t.TempDir(), map[string]int64{outPathA: 10, outPathB: 20})
	m := &consumingManifest{fakeOutboxManifest: newFakeOutboxManifest(), consumed: outPathA}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}, "writer-1")
	waitOutbox(t, "both settled", func() bool { return r.Pending() == 0 })
	if m.isRegistered(outPathA) {
		t.Fatal("a consumed input was registered again")
	}
	if !m.isRegistered(outPathB) {
		t.Fatal("an unrelated file was not registered")
	}
}
