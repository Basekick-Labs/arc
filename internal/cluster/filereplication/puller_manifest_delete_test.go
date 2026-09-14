package filereplication

// Tests for the query-gate self-heal on manifest delete (#759, #795).
//
// The catch-up batch can learn that an entry is gone from the manifest at
// three different moments relative to its own pull: after the failure was
// recorded, while the pull is queued or in flight, or before the walker has
// even tagged the entry. Each ordering has a test here; the last one is the
// widest window in production (the walker waits mid-page at queue high water).

import (
	"context"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// fakeManifest is a concurrency-safe stand-in for the FSM membership lookup
// the coordinator wires into Config.ManifestHas.
type fakeManifest struct {
	mu    sync.Mutex
	paths map[string]bool
}

func newFakeManifest(paths ...string) *fakeManifest {
	m := &fakeManifest{paths: make(map[string]bool, len(paths))}
	for _, p := range paths {
		m.paths[p] = true
	}
	return m
}

func (m *fakeManifest) has(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.paths[path]
}

func (m *fakeManifest) add(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paths[path] = true
}

func (m *fakeManifest) remove(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.paths, path)
}

// singlePage returns a RunCatchUp page function that serves entries once.
func singlePage(entries ...*raft.FileEntry) func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
	return func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		if cursor != "" {
			return nil, "", nil
		}
		return entries, "", nil
	}
}

func waitFor(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Ordering 1: the pull failed first, then the entry left the manifest. The
// recorded failure (and a recorded drop for another path) must clear and the
// gate must reopen. This is the part #790 got right.
func TestPuller_OnManifestDelete_ClearsRecordedFailureAndDrop(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{})
	const failedPath = "db/cpu/2026/09/14/17/failed.parquet"
	const droppedPath = "db/cpu/2026/09/14/17/dropped.parquet"

	p.markCatchUp(failedPath)
	p.inflightAdd(failedPath)
	p.finishEntry(failedPath, enqueueSourceCatchUp, true, false, true)
	p.recordCatchUpDrop(droppedPath)
	p.catchupCompletedAt.Store(time.Now().Unix())
	if !p.markQuarantinedForLog(failedPath) {
		t.Fatal("first quarantine mark should be reported")
	}

	if s := p.CatchUpStatus(); s["catchup_failed"] != 1 || s["catchup_dropped"] != 1 || s["catchup_inflight"] != 0 {
		t.Fatalf("precondition: want failed=1 dropped=1 inflight=0, got %+v", s)
	}
	if p.FullyCaughtUp() {
		t.Fatal("precondition: gate must be red with a recorded failure and drop")
	}

	p.OnManifestDelete(failedPath)
	if s := p.CatchUpStatus(); s["catchup_failed"] != 0 || s["catchup_dropped"] != 1 {
		t.Fatalf("after deleting the failed entry: want failed=0 dropped=1, got %+v", s)
	}
	if p.FullyCaughtUp() {
		t.Fatal("gate must stay red while the dropped path is still recorded")
	}

	p.OnManifestDelete(droppedPath)
	if s := p.CatchUpStatus(); s["catchup_failed"] != 0 || s["catchup_dropped"] != 0 || s["catchup_inflight"] != 0 {
		t.Fatalf("after deleting both: want all zero, got %+v", s)
	}
	if !p.FullyCaughtUp() {
		t.Fatal("gate must reopen once both entries left the manifest")
	}
	if !p.markQuarantinedForLog(failedPath) {
		t.Error("quarantine log marker must be forgotten on delete so a re-registered path logs again")
	}
}

// Ordering 2 (#795): the entry leaves the manifest while its catch-up pull is
// in flight. The naive fix (clear the recorded failure only) leaves the tag in
// place, the worker then records the failure, and nothing can ever clear it.
// Removing the tag on delete means the worker's later finishEntry has nothing
// to count, and the counters must stay exact (no -1, no double decrement).
func TestPuller_OnManifestDelete_WhileCatchUpPullInflight(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{})
	const path = "db/cpu/2026/09/14/17/inflight.parquet"

	p.markCatchUp(path)
	p.inflightAdd(path)
	p.catchupCompletedAt.Store(time.Now().Unix())
	if s := p.CatchUpStatus(); s["catchup_inflight"] != 1 || s["inflight_count"] != 1 {
		t.Fatalf("precondition: want catchup_inflight=1 inflight_count=1, got %+v", s)
	}

	p.OnManifestDelete(path) // lands mid-pull: nothing recorded yet
	if s := p.CatchUpStatus(); s["catchup_inflight"] != 0 || s["inflight_count"] != 1 {
		t.Fatalf("after delete: tag gone but worker still owns the slot; want catchup_inflight=0 inflight_count=1, got %+v", s)
	}
	if !p.FullyCaughtUp() {
		t.Fatal("gate must reopen as soon as the entry leaves the manifest, not after the remaining retries")
	}

	// The worker gives up later. Even with a stale "still wanted" verdict the
	// missing tag means nothing is recorded, and the tag removal must not
	// decrement a second time.
	p.finishEntry(path, enqueueSourceCatchUp, true, false, true)
	s := p.CatchUpStatus()
	if s["catchup_failed"] != 0 || s["catchup_inflight"] != 0 || s["inflight_count"] != 0 {
		t.Fatalf("after the worker gave up: want failed=0 catchup_inflight=0 inflight_count=0, got %+v", s)
	}
	if !p.FullyCaughtUp() {
		t.Fatal("gate must stay open after the abandoned pull finishes")
	}
}

func TestPuller_OnManifestDelete_UnknownPathIsNoop(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{})
	p.catchupCompletedAt.Store(time.Now().Unix())
	p.OnManifestDelete("db/cpu/2026/09/14/17/never-seen.parquet")
	s := p.CatchUpStatus()
	for _, k := range []string{"catchup_failed", "catchup_dropped", "catchup_inflight", "inflight_count"} {
		if s[k] != 0 {
			t.Errorf("%s: want 0 after deleting an unknown path, got %d", k, s[k])
		}
	}
	if !p.FullyCaughtUp() {
		t.Error("gate must be open with nothing recorded")
	}
}

// Ordering 2 through the real worker: the fetch is blocked, the delete lands,
// the gate reopens immediately, and when the fetch is released the worker
// finds the entry gone before its next attempt and records nothing.
func TestPuller_OnManifestDelete_DuringRealWorkerPull(t *testing.T) {
	backend := newFakeBackend()
	release := make(chan struct{})
	fetcher := &blockingFetcher{release: release}
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"peer:9100"}, ok: true}
	p := newTestPuller(t, backend, fetcher, resolver)

	const path = "db/cpu/2026/09/14/17/inflight-real.parquet"
	manifest := newFakeManifest(path)
	p.cfg.ManifestHas = manifest.has

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Stop()

	p.RunCatchUp(ctx, singlePage(makeEntry(path, "writer-1", 64)))
	waitFor(t, "worker to block in Fetch", func() bool { return fetcher.calls.Load() == 1 })
	if s := p.CatchUpStatus(); s["catchup_inflight"] != 1 || s["completed_at"] == 0 {
		t.Fatalf("precondition: walker done and one catch-up pull in flight, got %+v", s)
	}
	if p.FullyCaughtUp() {
		t.Fatal("precondition: gate must be red while the catch-up pull is in flight")
	}

	manifest.remove(path)
	p.OnManifestDelete(path)
	if !p.FullyCaughtUp() {
		t.Fatalf("gate must reopen the moment the entry leaves the manifest, got %+v", p.CatchUpStatus())
	}

	close(release) // attempt 1 fails; attempt 2 must be skipped, not retried
	waitFor(t, "worker to finish", func() bool { return p.Stats()["inflight_count"] == 0 })
	s := p.Stats()
	if s["catchup_failed"] != 0 || s["failed"] != 0 {
		t.Errorf("a pull for a deleted entry must not count anywhere: catchup_failed=%d failed=%d", s["catchup_failed"], s["failed"])
	}
	if s["skipped_gone"] != 1 {
		t.Errorf("skipped_gone: want 1 (second attempt skipped), got %d", s["skipped_gone"])
	}
	if fetcher.calls.Load() != 1 {
		t.Errorf("fetch attempts: want 1 (no retry for a deleted entry), got %d", fetcher.calls.Load())
	}
	if !p.FullyCaughtUp() {
		t.Errorf("gate must stay open, got %+v", p.CatchUpStatus())
	}
}

// Ordering 3: the delete landed BEFORE the walker tagged the entry (the walker
// can wait minutes mid-page at queue high water). The callback found nothing
// to clear, so the membership check at pull time is the only defence: the
// worker must skip the pull and finishEntry must record nothing.
func TestPuller_ManifestHas_DeletedBeforeTagIsNotCounted(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher() // any call would fail with "no scripted results"
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"peer:9100"}, ok: true}
	p := newTestPuller(t, backend, fetcher, resolver)

	const path = "db/cpu/2026/09/14/17/deleted-before-tag.parquet"
	manifest := newFakeManifest() // already gone
	p.cfg.ManifestHas = manifest.has
	p.OnManifestDelete(path) // what the FSM callback did earlier: a no-op

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Stop()

	p.RunCatchUp(ctx, singlePage(makeEntry(path, "writer-1", 64)))
	waitFor(t, "worker to finish", func() bool { return p.Stats()["inflight_count"] == 0 && p.Stats()["catchup_inflight"] == 0 })
	s := p.Stats()
	if fetcher.calls.Load() != 0 {
		t.Errorf("no fetch must be attempted for a deleted entry, got %d", fetcher.calls.Load())
	}
	if s["catchup_failed"] != 0 || s["failed"] != 0 || s["skipped_gone"] != 1 {
		t.Errorf("want catchup_failed=0 failed=0 skipped_gone=1, got %+v", s)
	}
	if !p.FullyCaughtUp() {
		t.Errorf("gate must be open, got %+v", p.CatchUpStatus())
	}
}

// The finishEntry condition on its own: a failure for an entry the manifest
// no longer wants is not recorded even when the tag is still present.
func TestPuller_FinishEntry_NotWantedFailureIsNotRecorded(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{})
	const path = "db/cpu/2026/09/14/17/not-wanted.parquet"
	p.markCatchUp(path)
	p.inflightAdd(path)
	p.catchupCompletedAt.Store(time.Now().Unix())

	p.finishEntry(path, enqueueSourceCatchUp, true, false, false)
	s := p.CatchUpStatus()
	if s["catchup_failed"] != 0 || s["catchup_inflight"] != 0 || s["inflight_count"] != 0 {
		t.Fatalf("want nothing recorded and all state released, got %+v", s)
	}
	if !p.FullyCaughtUp() {
		t.Fatal("gate must be open")
	}

	// And the same call with stillWanted=true still records, so the
	// condition is doing the work rather than the test.
	p.markCatchUp(path)
	p.inflightAdd(path)
	p.finishEntry(path, enqueueSourceCatchUp, true, false, true)
	if s := p.CatchUpStatus(); s["catchup_failed"] != 1 {
		t.Fatalf("control: a wanted failure must be recorded, got %+v", s)
	}
}

// Queue-full drops had the same delete-before-record hole: the drop is now
// recorded in the same critical section that removes the tag, only for a
// tagged catch-up entry the manifest still contains.
func TestPuller_CatchUpDrop_ExactAgainstManifestDelete(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{})
	// Not started: the queue (cap 8) fills and the ninth enqueue drops.
	manifest := newFakeManifest()
	p.cfg.ManifestHas = manifest.has

	for i := 0; i < 8; i++ {
		path := "db/cpu/2026/09/14/17/queued-" + string(rune('a'+i)) + ".parquet"
		manifest.add(path)
		p.markCatchUp(path)
		if got := p.enqueue(makeEntry(path, "writer-1", 8), enqueueSourceCatchUp); got != enqueueResultEnqueued {
			t.Fatalf("fill %d: want enqueued, got %v", i, got)
		}
	}
	p.catchupCompletedAt.Store(time.Now().Unix())

	// (a) delete landed before the drop: nothing recorded, tag released.
	const goneFirst = "db/cpu/2026/09/14/17/gone-first.parquet"
	p.markCatchUp(goneFirst) // manifest never had it: the delete came first
	if got := p.enqueue(makeEntry(goneFirst, "writer-1", 8), enqueueSourceCatchUp); got != enqueueResultDropped {
		t.Fatalf("want dropped, got %v", got)
	}
	if s := p.CatchUpStatus(); s["catchup_dropped"] != 0 || s["catchup_inflight"] != 8 {
		t.Fatalf("(a) want catchup_dropped=0 catchup_inflight=8, got %+v", s)
	}

	// (b) delete lands after the drop: recorded, then cleared by the callback.
	const goneAfter = "db/cpu/2026/09/14/17/gone-after.parquet"
	manifest.add(goneAfter)
	p.markCatchUp(goneAfter)
	if got := p.enqueue(makeEntry(goneAfter, "writer-1", 8), enqueueSourceCatchUp); got != enqueueResultDropped {
		t.Fatalf("want dropped, got %v", got)
	}
	if s := p.CatchUpStatus(); s["catchup_dropped"] != 1 {
		t.Fatalf("(b) a wanted catch-up drop must be recorded, got %+v", s)
	}
	manifest.remove(goneAfter)
	p.OnManifestDelete(goneAfter)
	if s := p.CatchUpStatus(); s["catchup_dropped"] != 0 {
		t.Fatalf("(b) delete must clear the recorded drop, got %+v", s)
	}

	// (c) a reactive (untagged) drop never touches the catch-up counters.
	const reactive = "db/cpu/2026/09/14/17/reactive.parquet"
	if got := p.enqueue(makeEntry(reactive, "writer-1", 8), enqueueSourceReactive); got != enqueueResultDropped {
		t.Fatalf("want dropped, got %v", got)
	}
	s := p.Stats()
	if s["catchup_dropped"] != 0 || s["dropped"] != 3 {
		t.Fatalf("(c) want catchup_dropped=0 dropped=3, got %+v", s)
	}
}

// A follower restoring from a Raft snapshot fires no delete callbacks. The
// periodic tick prunes recorded state for paths the manifest no longer has.
func TestPuller_PruneStaleCatchUpState(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{})
	const gone = "db/cpu/2026/09/14/17/gone.parquet"
	const goneDrop = "db/cpu/2026/09/14/17/gone-drop.parquet"
	const kept = "db/cpu/2026/09/14/17/kept.parquet"
	manifest := newFakeManifest(kept)
	p.cfg.ManifestHas = manifest.has

	for _, path := range []string{gone, kept} {
		p.markCatchUp(path)
		p.inflightAdd(path)
		p.finishEntry(path, enqueueSourceCatchUp, true, false, true)
	}
	p.recordCatchUpDrop(goneDrop)
	p.catchupCompletedAt.Store(time.Now().Unix())
	if s := p.CatchUpStatus(); s["catchup_failed"] != 2 || s["catchup_dropped"] != 1 {
		t.Fatalf("precondition: want failed=2 dropped=1, got %+v", s)
	}

	p.pruneStaleCatchUpState()
	s := p.CatchUpStatus()
	if s["catchup_failed"] != 1 || s["catchup_dropped"] != 0 {
		t.Fatalf("prune must clear only paths the manifest lost: want failed=1 dropped=0, got %+v", s)
	}
	if p.FullyCaughtUp() {
		t.Fatal("the still-present failed path must keep the gate red")
	}

	// Without a hook the prune is a no-op (today's behaviour).
	p.cfg.ManifestHas = nil
	p.pruneStaleCatchUpState()
	if s := p.CatchUpStatus(); s["catchup_failed"] != 1 {
		t.Fatalf("nil hook must not prune, got %+v", s)
	}
}

// blockingSuccessFetcher blocks until released, then serves the body, so a
// pull can be made to complete AFTER the entry left the manifest.
type blockingSuccessFetcher struct {
	release chan struct{}
	body    []byte
	calls   atomic.Int64
}

func (b *blockingSuccessFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer, byteOffset int64, prefixHasher hash.Hash) (int64, error) {
	b.calls.Add(1)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.release:
	}
	n, err := dst.Write(b.body)
	return int64(n), err
}

// A pull whose bytes arrive after the entry left the manifest must not leave
// an orphan behind: this node's delete worker may already have unlinked the
// path, and a finalized copy would be served by the read glob. The worker
// discards it and counts the pull as abandoned, not as pulled.
func TestPuller_PullCompletingAfterDeleteIsDiscarded(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("bytes that arrived too late")
	fetcher := &blockingSuccessFetcher{release: make(chan struct{}), body: body}
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"peer:9100"}, ok: true}
	p := newTestPuller(t, backend, fetcher, resolver)

	const path = "db/cpu/2026/09/14/17/late.parquet"
	manifest := newFakeManifest(path)
	p.cfg.ManifestHas = manifest.has

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	defer p.Stop()

	p.RunCatchUp(ctx, singlePage(makeEntry(path, "writer-1", int64(len(body)))))
	waitFor(t, "worker to block in Fetch", func() bool { return fetcher.calls.Load() == 1 })

	manifest.remove(path)
	p.OnManifestDelete(path)
	close(fetcher.release) // bytes land now, after the delete

	waitFor(t, "worker to finish", func() bool { return p.Stats()["inflight_count"] == 0 })
	s := p.Stats()
	if s["pulled"] != 0 || s["skipped_gone"] != 1 {
		t.Errorf("want pulled=0 skipped_gone=1, got pulled=%d skipped_gone=%d", s["pulled"], s["skipped_gone"])
	}
	if exists, _ := backend.Exists(ctx, path); exists {
		t.Error("the late copy must be removed from local storage")
	}
	if backend.deleteCount() == 0 {
		t.Error("the worker must delete the late copy itself; the delete worker may already have run")
	}
	if !p.FullyCaughtUp() {
		t.Errorf("gate must be open, got %+v", p.CatchUpStatus())
	}
}
