package cluster

import (
	"context"
	"errors"
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

// runDeleteWorkerOnce drives the real worker over one batch and returns when it
// has drained. The worker waits 500ms for its grace period before deleting
// anything, so the poll deadline allows for that.
func runDeleteWorkerOnce(t *testing.T, backend storage.Backend, reqs ...deleteRequest) *Coordinator {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c := &Coordinator{
		storage:     backend,
		logger:      zerolog.Nop(),
		ctx:         ctx,
		deleteQueue: make(chan deleteRequest, len(reqs)+1),
	}
	for _, r := range reqs {
		c.deleteQueue <- r
	}
	c.deleteWg.Add(1)
	go c.runDeleteWorker()
	t.Cleanup(func() {
		close(c.deleteQueue)
		c.deleteWg.Wait()
	})
	return c
}

// TestDeleteWorkerClassifiesAnUnusableKey covers the Phase 4 local delete
// worker. It is fire-and-forget, so the entry was already out of the work set
// and nothing retried it; what was wrong is that a permanent condition was
// logged as an ordinary delete failure, indistinguishable from a backend
// hiccup an operator should wait out. Nothing here can ever remove that local
// copy.
//
// The three outcomes are driven together because the branch was rewritten from
// a plain `if err != nil` into a three-way classification, which is the shape
// where the nil case is easiest to break.
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

	runDeleteWorkerOnce(t, backend,
		deleteRequest{path: good, reason: "test"},
		deleteRequest{path: unusable, reason: "test"},
		deleteRequest{path: flaky, reason: "test"},
	)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(backend.deleteCalls()) == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	calls := backend.deleteCalls()
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
