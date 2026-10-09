package cluster

// The Phase 4 local delete workers: what the FSM delete callback hands them
// is never dropped, a stop drains everything that is pending, a path the
// manifest lists again is left alone, and a disk that does not answer cannot
// hang the shutdown.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// recordingBackend notes every Delete it is asked for and can fail one key
// transiently, so the worker's three outcomes can be told apart.
type recordingBackend struct {
	*memBackend
	mu       sync.Mutex
	deleted  []string
	flakyKey string
}

func (b *recordingBackend) Delete(ctx context.Context, path string) error {
	b.mu.Lock()
	b.deleted = append(b.deleted, path)
	flaky := b.flakyKey == path
	b.mu.Unlock()
	if flaky {
		return errors.New("RequestTimeout: connection reset by peer")
	}
	return b.memBackend.Delete(ctx, path)
}

func (b *recordingBackend) deleteCalls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.deleted))
	copy(out, b.deleted)
	return out
}

// newDeleteRig builds a coordinator with only what the delete path touches
// and starts the real workers. The coordinator's own context is cancelled
// up front when asked — the state Stop puts it in before it reaches the
// workers — so a worker that still watched that context would show it.
func newDeleteRig(t *testing.T, backend storage.Backend, ctxCancelled bool, has func(string) bool) *Coordinator {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if ctxCancelled {
		cancel()
	} else {
		t.Cleanup(cancel)
	}
	c := &Coordinator{
		storage:           backend,
		logger:            zerolog.Nop(),
		ctx:               ctx,
		cancel:            cancel,
		deleteManifestHas: has,
	}
	c.startDeleteWorkers()
	t.Cleanup(func() {
		if c.deleteStop != nil {
			c.stopDeleteWorkers(c.deleteStop, c.deleteWg)
			c.deleteStop = nil
		}
	})
	return c
}

func waitForDeletes(t *testing.T, backend *recordingBackend, want int, within time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(backend.deleteCalls()) >= want {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return backend.deleteCalls()
}

func TestDeleteWorkerRefreshesManifestEntryWhenUnlinkIsSkipped(t *testing.T) {
	const path = "testdb/cpu/2026/10/08/reregistered.parquet"
	ctx := context.Background()
	mem := newMemBackend()
	oldBody := []byte("old same-size payload")
	if err := mem.Write(ctx, path, oldBody); err != nil {
		t.Fatal(err)
	}
	backend := &recordingBackend{memBackend: mem}
	var refreshed string
	c := &Coordinator{storage: backend, logger: zerolog.Nop()}

	c.unlinkOne(
		deleteRequest{path: path, reason: "test"},
		func(got string) bool { return got == path },
		func(got string) { refreshed = got },
	)

	if refreshed != path {
		t.Fatalf("re-registered path refresh = %q, want %q", refreshed, path)
	}
	if calls := backend.deleteCalls(); len(calls) != 0 {
		t.Fatalf("manifest-listed path was unlinked: %v", calls)
	}
	got, err := mem.Read(ctx, path)
	if err != nil || !bytes.Equal(got, oldBody) {
		t.Fatalf("old local copy must remain readable until refresh succeeds: got %q, err=%v", got, err)
	}
}

// TestDeleteWorkerClassifiesAnUnusableKey covers the worker's three outcomes.
// The item is out of the work set either way and nothing retries it; what
// was wrong before #747 is that a permanent condition was logged as an
// ordinary delete failure, indistinguishable from a backend hiccup an
// operator should wait out.
func TestDeleteWorkerClassifiesAnUnusableKey(t *testing.T) {
	const good = "testdb/cpu/2026/04/11/14/good.parquet"
	const flaky = "testdb/cpu/2026/04/11/14/flaky.parquet"
	const unusable = `testdb\cpu/2026/04/11/14/bad.parquet`

	mem := newMemBackend()
	ctx := context.Background()
	for _, k := range []string{good, flaky} {
		if err := mem.Write(ctx, k, []byte("bytes")); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}
	backend := &recordingBackend{memBackend: mem, flakyKey: flaky}
	before := metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64)

	c := newDeleteRig(t, backend, false, nil)
	c.enqueueLocalDelete(good, "test")
	c.enqueueLocalDelete(unusable, "test")
	c.enqueueLocalDelete(flaky, "test")

	calls := waitForDeletes(t, backend, 3, 5*time.Second)
	if len(calls) != 3 {
		t.Fatalf("worker issued %d deletes, want 3: %v. One bad item must not stop the batch", len(calls), calls)
	}
	if ok, _ := mem.Exists(ctx, good); ok {
		t.Error("the deletable local copy survived")
	}
	if got := metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64) - before; got != 1 {
		t.Errorf("quarantine counter moved by %d, want exactly 1 (the unusable key only)", got)
	}
}

// A burst far larger than the old 1024-slot channel, handed over as fast as
// the FSM applies a retention chunk: every local copy must go. Before the
// pending list, item 1025 onward was dropped with a log that promised a
// restart reconcile that did not exist, and the replicas stayed forever.
func TestLocalDeleteNeverDrops(t *testing.T) {
	const n = 3000
	mem := newMemBackend()
	ctx := context.Background()
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("testdb/cpu/2026/04/11/14/burst-%04d.parquet", i)
		if err := mem.Write(ctx, paths[i], []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	backend := &recordingBackend{memBackend: mem}
	c := newDeleteRig(t, backend, false, nil)

	for _, p := range paths {
		c.enqueueLocalDelete(p, "retention:test")
	}
	calls := waitForDeletes(t, backend, n, 10*time.Second)
	if len(calls) != n {
		t.Fatalf("workers issued %d deletes, want %d: the burst was not fully drained", len(calls), n)
	}
	for _, p := range paths {
		if ok, _ := mem.Exists(ctx, p); ok {
			t.Fatalf("%s survived the burst", p)
		}
	}
	if got := metrics.Get().Snapshot()["cluster_local_delete_pending"].(int64); got != 0 {
		t.Errorf("pending gauge = %d after the drain, want 0", got)
	}
}

// Stop cancels the coordinator's context before it reaches the workers.
// A worker parked in its grace used to return on that cancellation with the
// item it held, and whatever was still buffered was closed away with the
// channel. Now stop is the only exit, and it drains: every pending delete
// runs. The order is the real one — the burst arrives on live workers, they
// enter their grace, the context is cancelled, then the stop follows.
func TestStopDrainsPendingDeletes(t *testing.T) {
	const n = 50
	mem := newMemBackend()
	ctx := context.Background()
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("testdb/cpu/2026/04/11/14/stop-%02d.parquet", i)
		if err := mem.Write(ctx, paths[i], []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	backend := &recordingBackend{memBackend: mem}
	c := newDeleteRig(t, backend, false, nil)

	for _, p := range paths {
		c.enqueueLocalDelete(p, "compaction:test")
	}
	time.Sleep(50 * time.Millisecond) // the workers are now inside their grace
	c.cancel()                        // what Stop does first
	// No grace was waited out: the stop must still get every item.
	start := time.Now()
	c.stopDeleteWorkers(c.deleteStop, c.deleteWg)
	c.deleteStop = nil
	took := time.Since(start)

	calls := backend.deleteCalls()
	if len(calls) != n {
		t.Fatalf("stop drained %d deletes, want %d (took %s)", len(calls), n, took)
	}
	if took > 5*time.Second {
		t.Fatalf("stop took %s; the drain must skip the grace and finish promptly", took)
	}
	for _, p := range paths {
		if ok, _ := mem.Exists(ctx, p); ok {
			t.Fatalf("%s survived the stop", p)
		}
	}
}

// A path the manifest lists again is not unlinked: the manifest asked for
// the delete, and the manifest has since changed its mind.
func TestDeleteWorkerSkipsPathBackInManifest(t *testing.T) {
	const back = "testdb/cpu/2026/04/11/14/back.parquet"
	const gone1 = "testdb/cpu/2026/04/11/14/gone-1.parquet"
	const gone2 = "testdb/cpu/2026/04/11/14/gone-2.parquet"
	mem := newMemBackend()
	ctx := context.Background()
	for _, k := range []string{back, gone1, gone2} {
		if err := mem.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	backend := &recordingBackend{memBackend: mem}
	c := newDeleteRig(t, backend, false, func(path string) bool { return path == back })

	for _, k := range []string{gone1, back, gone2} {
		c.enqueueLocalDelete(k, "test")
	}
	calls := waitForDeletes(t, backend, 2, 5*time.Second)
	if len(calls) != 2 {
		t.Fatalf("worker issued %d deletes, want 2 (the re-listed path skipped): %v", len(calls), calls)
	}
	if ok, _ := mem.Exists(ctx, back); !ok {
		t.Fatal("the path the manifest lists again was unlinked")
	}
	for _, k := range []string{gone1, gone2} {
		if ok, _ := mem.Exists(ctx, k); ok {
			t.Fatalf("%s survived", k)
		}
	}
}

// blockingBackend holds every Delete until released, as an unreachable disk
// would, so the stop bound is what returns control.
type blockingBackend struct {
	*memBackend
	release chan struct{}
}

func (b *blockingBackend) Delete(ctx context.Context, path string) error {
	<-b.release
	return b.memBackend.Delete(ctx, path)
}

// A stop whose disk does not answer returns at the bound instead of hanging
// the shutdown, and what it reports is what is still on disk: the entries a
// stuck worker holds count, not only the untaken ones (a worker takes the
// whole list, so "untaken" alone would read zero in exactly this case).
func TestStopBoundsTheDrain(t *testing.T) {
	old := deleteStopDrainBound
	deleteStopDrainBound = 300 * time.Millisecond
	t.Cleanup(func() { deleteStopDrainBound = old })

	backend := &blockingBackend{memBackend: newMemBackend(), release: make(chan struct{})}
	t.Cleanup(func() { close(backend.release) }) // let the parked worker finish on every path
	ctx := context.Background()
	const stuck = "testdb/cpu/2026/04/11/14/stuck.parquet"
	const behind = "testdb/cpu/2026/04/11/14/behind.parquet"
	for _, k := range []string{stuck, behind} {
		if err := backend.memBackend.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	c := newDeleteRig(t, backend, true, nil)
	c.enqueueLocalDelete(stuck, "test")
	c.enqueueLocalDelete(behind, "test")

	start := time.Now()
	c.stopDeleteWorkers(c.deleteStop, c.deleteWg)
	c.deleteStop = nil
	took := time.Since(start)
	if took < deleteStopDrainBound || took > 3*time.Second {
		t.Fatalf("stop returned after %s; want about the %s bound", took, deleteStopDrainBound)
	}
	if got := metrics.Get().Snapshot()["cluster_local_delete_pending"].(int64); got != 2 {
		t.Fatalf("pending gauge = %d at the bound, want 2: both entries are still on disk, held by the stuck worker", got)
	}
	if left := int64(len(c.deletePending)) + c.deleteInFlight.Load(); left != 2 {
		t.Fatalf("left on disk = %d, want 2", left)
	}
}
