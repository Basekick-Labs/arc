package filereplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

type issue798BlockingFetcher struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int64
	oldBody []byte
	newBody []byte
}

func (f *issue798BlockingFetcher) Fetch(
	ctx context.Context,
	_ string,
	entry *raft.FileEntry,
	dst io.Writer,
	offset int64,
	_ hash.Hash,
) (int64, error) {
	call := f.calls.Add(1)

	if call == 1 {
		close(f.started)
		select {
		case <-f.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	if offset != 0 {
		return 0, fmt.Errorf("unexpected resume offset: %d", offset)
	}

	var body []byte
	switch entry.LSN {
	case 1:
		body = f.oldBody
	case 2:
		body = f.newBody
	default:
		return 0, fmt.Errorf("unexpected manifest version: %d", entry.LSN)
	}

	n, err := dst.Write(body)
	return int64(n), err
}

func TestPullerUpdatedVersionIsNotLostIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/rewrite.parquet"

	// Equal lengths are deliberate: size alone cannot identify a version.
	oldBody := []byte("old-payload")
	newBody := []byte("new-payload")

	if len(oldBody) != len(newBody) {
		t.Fatal("test setup: payloads must have equal lengths")
	}

	oldEntry := makeEntry(path, "writer-1", int64(len(oldBody)))
	oldEntry.LSN = 1
	oldHash := sha256.Sum256(oldBody)
	oldEntry.SHA256 = fmt.Sprintf("%x", oldHash)

	newEntry := *oldEntry
	newEntry.LSN = 2
	newHash := sha256.Sum256(newBody)
	newEntry.SHA256 = fmt.Sprintf("%x", newHash)

	backend := newFakeBackend()
	fetcher := &issue798BlockingFetcher{
		started: make(chan struct{}),
		release: make(chan struct{}),
		oldBody: oldBody,
		newBody: newBody,
	}

	p := newTestPuller(
		t,
		backend,
		fetcher,
		staticResolver{
			nodeID: "writer-1",
			addrs:  []string{"peer:9100"},
			ok:     true,
		},
	)

	var releaseOnce sync.Once
	releaseFirst := func() {
		releaseOnce.Do(func() { close(fetcher.release) })
	}

	t.Cleanup(func() {
		releaseFirst()
		p.Stop()
	})

	p.Start(context.Background())
	p.Enqueue(oldEntry)

	// Synchronize on the first fetch actually being in progress.
	// No timing guesses or sleeps are needed to trigger the race.
	select {
	case <-fetcher.started:
	case <-p.ctx.Done():
		t.Fatal("puller stopped before first fetch started")
	}

	// Same path, new manifest version, different checksum, same size.
	// The original code discards this callback as a duplicate.
	p.Enqueue(&newEntry)
	releaseFirst()

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return fetcher.calls.Load() >= 2 &&
			s["inflight_count"] == 0
	})

	if calls := fetcher.calls.Load(); calls < 2 {
		t.Fatalf(
			"updated version was lost: only %d fetch call(s); stats=%v",
			calls,
			stats,
		)
	}

	if stats["inflight_count"] != 0 {
		t.Fatalf("puller did not finish: stats=%v", stats)
	}

	actual, err := backend.Read(context.Background(), path)
	if err != nil {
		t.Fatalf("read replicated file: %v", err)
	}
	if !bytes.Equal(actual, newBody) {
		t.Fatalf(
			"updated version was lost: got %q, want %q",
			actual,
			newBody,
		)
	}
}

// A second update can arrive after the previous pull has fully finished.
// The local file's matching size must not hide a different manifest version.
func TestPullerSequentialSameSizeUpdateIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/sequential-rewrite.parquet"

	oldBody := []byte("old-payload")
	newBody := []byte("new-payload")
	if len(oldBody) != len(newBody) {
		t.Fatal("test setup: bodies must have the same size")
	}

	oldEntry := makeEntry(path, "writer-1", int64(len(oldBody)))
	oldEntry.LSN = 1
	oldHash := sha256.Sum256(oldBody)
	oldEntry.SHA256 = fmt.Sprintf("%x", oldHash)

	newEntry := *oldEntry
	newEntry.LSN = 2
	newHash := sha256.Sum256(newBody)
	newEntry.SHA256 = fmt.Sprintf("%x", newHash)

	backend := newFakeBackend()
	fetcher := newFakeFetcher(
		fakeFetchResult{body: oldBody},
		fakeFetchResult{body: newBody},
	)

	p := newTestPuller(t, backend, fetcher, staticResolver{
		nodeID: "writer-1",
		addrs:  []string{"peer:9100"},
		ok:     true,
	})

	p.Start(context.Background())
	defer p.Stop()

	// Allow the first version to finish completely before sending v2.
	p.Enqueue(oldEntry)
	first := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1 && s["inflight_count"] == 0
	})
	if first["pulled"] != 1 || first["inflight_count"] != 0 {
		t.Fatalf("initial pull did not finish: %v", first)
	}

	p.Enqueue(&newEntry)

	last := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 2 && s["inflight_count"] == 0
	})
	if last["pulled"] != 2 || last["inflight_count"] != 0 {
		t.Fatalf(
			"same-size update was skipped: fetches=%d, stats=%v",
			fetcher.calls.Load(),
			last,
		)
	}

	got, err := backend.Read(context.Background(), path)
	if err != nil {
		t.Fatalf("read updated file: %v", err)
	}
	if !bytes.Equal(got, newBody) {
		t.Fatalf("wrong final version: got %q, want %q", got, newBody)
	}

	// An older callback arriving late must not roll back the file.
	p.Enqueue(oldEntry)
	if duplicates := p.Stats()["skipped_dup"]; duplicates != 1 {
		t.Fatalf("stale callback was not rejected: skipped_dup=%d", duplicates)
	}

	// Repeating the already-completed current version must retain the
	// original already-local optimization instead of downloading again.
	p.Enqueue(&newEntry)
	final := waitStats(t, p, func(s map[string]int64) bool {
		return s["skipped_local"] == 1 && s["inflight_count"] == 0
	})
	if final["skipped_local"] != 1 || fetcher.calls.Load() != 2 {
		t.Fatalf("duplicate current version caused another fetch: stats=%v, calls=%d",
			final, fetcher.calls.Load())
	}

	got, err = backend.Read(context.Background(), path)
	if err != nil || !bytes.Equal(got, newBody) {
		t.Fatalf("final file changed after duplicate callbacks: body=%q, err=%v", got, err)
	}
}

// A failed refresh must remain pending logically. Receiving the same
// manifest version again should retry rather than trust the old local file.
func TestPullerFailedRefreshRetriesSameVersionIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/retry-rewrite.parquet"

	oldBody := []byte("old-payload")
	newBody := []byte("new-payload")

	if len(oldBody) != len(newBody) {
		t.Fatal("test setup: versions must have equal sizes")
	}

	oldEntry := makeEntry(path, "writer-1", int64(len(oldBody)))
	oldEntry.LSN = 1
	oldHash := sha256.Sum256(oldBody)
	oldEntry.SHA256 = fmt.Sprintf("%x", oldHash)

	newEntry := *oldEntry
	newEntry.LSN = 2
	newHash := sha256.Sum256(newBody)
	newEntry.SHA256 = fmt.Sprintf("%x", newHash)

	backend := newFakeBackend()

	fetcher := newFakeFetcher(
		fakeFetchResult{body: oldBody},
		fakeFetchResult{err: errors.New("temporary peer failure")},
		fakeFetchResult{body: newBody},
	)

	p := newTestPuller(t, backend, fetcher, staticResolver{
		nodeID: "writer-1",
		addrs:  []string{"peer:9100"},
		ok:     true,
	})

	// One attempt per request makes failure and the subsequent
	// independent retry observable as separate operations.
	p.cfg.RetryMaxAttempts = 1

	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(oldEntry)

	first := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1 && s["inflight_count"] == 0
	})
	if first["pulled"] != 1 || first["inflight_count"] != 0 {
		t.Fatalf("initial version did not finish: %v", first)
	}

	p.Enqueue(&newEntry)

	failed := waitStats(t, p, func(s map[string]int64) bool {
		return s["failed"] == 1 && s["inflight_count"] == 0
	})
	if failed["failed"] != 1 || failed["inflight_count"] != 0 {
		t.Fatalf("refresh failure did not settle: %v", failed)
	}

	if calls := fetcher.calls.Load(); calls != 2 {
		t.Fatalf("expected two fetch attempts before retry, got %d", calls)
	}

	// The same manifest entry arrives again. A size-only local-file
	// check would incorrectly skip the required refresh.
	p.Enqueue(&newEntry)

	retried := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 2 && s["inflight_count"] == 0
	})

	if retried["pulled"] != 2 || retried["inflight_count"] != 0 {
		t.Fatalf("failed refresh was not retried: %v", retried)
	}

	if calls := fetcher.calls.Load(); calls != 3 {
		t.Fatalf("expected three fetch attempts, got %d", calls)
	}

	got, err := backend.Read(context.Background(), path)
	if err != nil {
		t.Fatalf("read refreshed file: %v", err)
	}
	if !bytes.Equal(got, newBody) {
		t.Fatalf("refresh retained stale content: got %q, want %q",
			got, newBody)
	}
}

// Observed versions should not remain in memory after their manifest entry
// is deleted. A subsequent registration starts a fresh lifecycle.
func TestPullerManifestDeleteClearsObservedVersionIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/deleted-rewrite.parquet"

	body := []byte("initial-file")
	entry := makeEntry(path, "writer-1", int64(len(body)))

	hash := sha256.Sum256(body)
	entry.SHA256 = fmt.Sprintf("%x", hash)

	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: body})

	p := newTestPuller(t, backend, fetcher, staticResolver{
		nodeID: "writer-1",
		addrs:  []string{"peer:9100"},
		ok:     true,
	})

	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1 && s["inflight_count"] == 0
	})
	if stats["pulled"] != 1 || stats["inflight_count"] != 0 {
		t.Fatalf("initial pull did not complete: %v", stats)
	}

	p.inflightMu.Lock()
	_, existedBefore := p.observed[path]
	p.inflightMu.Unlock()

	if !existedBefore {
		t.Fatal("completed manifest version was not recorded")
	}

	p.OnManifestDelete(path)

	p.inflightMu.Lock()
	_, existsAfter := p.observed[path]
	p.inflightMu.Unlock()

	if existsAfter {
		t.Fatal("deleted manifest entry leaked observed-version state")
	}
}

// When reconciliation supersedes a catch-up request, the successor must
// retain ownership of the original catch-up tag until the path settles.
func TestPullerSupersededCatchUpClearsTagIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/catchup-rewrite.parquet"

	oldBody := []byte("old-payload")
	newBody := []byte("new-payload")

	oldEntry := makeEntry(path, "writer-1", int64(len(oldBody)))
	oldEntry.LSN = 1
	oldHash := sha256.Sum256(oldBody)
	oldEntry.SHA256 = fmt.Sprintf("%x", oldHash)

	newEntry := *oldEntry
	newEntry.LSN = 2
	newHash := sha256.Sum256(newBody)
	newEntry.SHA256 = fmt.Sprintf("%x", newHash)

	backend := newFakeBackend()
	fetcher := &issue798BlockingFetcher{
		started: make(chan struct{}),
		release: make(chan struct{}),
		oldBody: oldBody,
		newBody: newBody,
	}

	p := newTestPuller(t, backend, fetcher, staticResolver{
		nodeID: "writer-1",
		addrs:  []string{"peer:9100"},
		ok:     true,
	})

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(fetcher.release) })
	}
	t.Cleanup(func() {
		release()
		p.Stop()
	})

	p.Start(context.Background())

	// Model the startup walker: tag before submitting its request.
	p.markCatchUp(path)
	if result := p.enqueue(oldEntry, enqueueSourceCatchUp); result != enqueueResultEnqueued {
		t.Fatalf("catch-up enqueue result: %v", result)
	}

	select {
	case <-fetcher.started:
	case <-time.After(3 * time.Second):
		t.Fatal("initial catch-up fetch did not start")
	}

	// The walker has completed, but its tagged request is still in flight.
	p.catchupCompletedAt.Store(time.Now().Unix())

	// A newer reconciliation version takes over the existing slot.
	if result := p.enqueue(&newEntry, enqueueSourceReconciliation); result != enqueueResultEnqueued {
		t.Fatalf("superseding enqueue result: %v", result)
	}

	release()

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 2 && s["inflight_count"] == 0
	})
	if stats["pulled"] != 2 || stats["inflight_count"] != 0 {
		t.Fatalf("superseding pull did not complete: %v", stats)
	}

	if stats["catchup_inflight"] != 0 || !p.FullyCaughtUp() {
		t.Fatalf(
			"catch-up tag leaked after superseding reconciliation: %v",
			stats,
		)
	}

	got, err := backend.Read(context.Background(), path)
	if err != nil || !bytes.Equal(got, newBody) {
		t.Fatalf("wrong final file: got %q, err=%v", got, err)
	}
}
