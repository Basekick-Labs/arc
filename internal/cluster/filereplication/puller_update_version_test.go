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

type synchronizedManifestEntry struct {
	mu    sync.RWMutex
	entry raft.FileEntry
}

func (m *synchronizedManifestEntry) get(string) (raft.FileEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.entry, true
}

func (m *synchronizedManifestEntry) set(entry raft.FileEntry) {
	m.mu.Lock()
	m.entry = entry
	m.mu.Unlock()
}

func TestManifestSupersedesUsesContentAndVersionTogether(t *testing.T) {
	tests := []struct {
		name     string
		request  raft.FileEntry
		current  raft.FileEntry
		supersed bool
	}{
		{
			name:     "same content at newer LSN",
			request:  raft.FileEntry{LSN: 7, SHA256: "same", SizeBytes: 10},
			current:  raft.FileEntry{LSN: 8, SHA256: "same", SizeBytes: 10},
			supersed: false,
		},
		{
			name:     "stale known version",
			request:  raft.FileEntry{LSN: 6, SHA256: "old", SizeBytes: 10},
			current:  raft.FileEntry{LSN: 7, SHA256: "new", SizeBytes: 10},
			supersed: true,
		},
		{
			name:     "older current version",
			request:  raft.FileEntry{LSN: 7, SHA256: "request", SizeBytes: 10},
			current:  raft.FileEntry{LSN: 3, SHA256: "old", SizeBytes: 10},
			supersed: false,
		},
		{
			name:     "equal batch LSN with different content",
			request:  raft.FileEntry{LSN: 42, SHA256: "request", SizeBytes: 10},
			current:  raft.FileEntry{LSN: 42, SHA256: "current", SizeBytes: 10},
			supersed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := manifestSupersedes(&tt.current, &tt.request); got != tt.supersed {
				t.Fatalf("manifestSupersedes() = %v, want %v", got, tt.supersed)
			}
		})
	}
}

// The FSM emits the ordinary register callback before its content-changed
// callback. With an equal-sized stale local file, the first request alone
// would skip locally; the forced equal-version successor must still run.
func TestPullerFSMCallbackPairForcesEqualVersionIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/fsm-callback-pair.parquet"
	oldBody := []byte("old-payload")
	newBody := []byte("new-payload")
	if len(oldBody) != len(newBody) {
		t.Fatal("test setup: bodies must have the same size")
	}

	entry := makeEntry(path, "writer-1", int64(len(newBody)))
	entry.LSN = 7
	hash := sha256.Sum256(newBody)
	entry.SHA256 = fmt.Sprintf("%x", hash)
	backend := newFakeBackend()
	if err := backend.Write(context.Background(), path, oldBody); err != nil {
		t.Fatalf("write stale local file: %v", err)
	}
	fetcher := newFakeFetcher(fakeFetchResult{body: newBody})
	p := newTestPuller(t, backend, fetcher, staticResolver{
		nodeID: "writer-1",
		addrs:  []string{"peer:9100"},
		ok:     true,
	})
	manifest := &synchronizedManifestEntry{entry: *entry}
	p.cfg.ManifestEntry = manifest.get

	// Queue both callbacks before starting workers, exactly preserving the
	// synchronous FSM callback order without allowing timing to choose a path.
	if got := p.enqueue(entry, enqueueSourceReactive, false); got != enqueueResultEnqueued {
		t.Fatalf("register callback result: %v", got)
	}
	if got := p.enqueue(entry, enqueueSourceReactive, true); got != enqueueResultEnqueued {
		t.Fatalf("content-change callback result: %v", got)
	}
	p.Start(context.Background())
	defer p.Stop()

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1 && s["inflight_count"] == 0
	})
	if stats["pulled"] != 1 || stats["skipped_local"] != 1 || fetcher.calls.Load() != 1 {
		t.Fatalf("callback pair failed to refresh equal-size content: stats=%v calls=%d", stats, fetcher.calls.Load())
	}
	got, err := backend.Read(context.Background(), path)
	if err != nil || !bytes.Equal(got, newBody) {
		t.Fatalf("stale bytes remained after callback pair: body=%q err=%v", got, err)
	}
}

type issue798BlockingFetcher struct {
	started  chan struct{}
	release  chan struct{}
	calls    atomic.Int64
	oldBody  []byte
	newBody  []byte
	firstErr error
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
		if f.firstErr != nil {
			return 0, f.firstErr
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
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		oldBody:  oldBody,
		newBody:  newBody,
		firstErr: ErrChecksumMismatch,
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
	manifest := &synchronizedManifestEntry{entry: *oldEntry}
	p.cfg.ManifestEntry = manifest.get

	var releaseOnce sync.Once
	releaseFirst := func() {
		releaseOnce.Do(func() { close(fetcher.release) })
	}

	t.Cleanup(func() {
		releaseFirst()
		p.Stop()
	})

	p.Start(context.Background())
	p.markCatchUp(path)
	if result := p.enqueue(oldEntry, enqueueSourceCatchUp, false); result != enqueueResultEnqueued {
		t.Fatalf("initial catch-up enqueue result: %v", result)
	}

	// Synchronize on the first fetch actually being in progress.
	// No timing guesses or sleeps are needed to trigger the race.
	select {
	case <-fetcher.started:
	case <-p.ctx.Done():
		t.Fatal("puller stopped before first fetch started")
	}

	// Same path, new manifest version, different checksum, same size.
	// The FSM content-change callback forces this refresh.
	manifest.set(newEntry)
	p.EnqueueContentChanged(&newEntry)
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
	if stats["failed"] != 0 || stats["catchup_failed"] != 0 {
		t.Fatalf("superseded checksum mismatch was counted as a failure: stats=%v", stats)
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

func TestPullerCompletedStalePullKeepsCopyUntilSuccessorIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/completed-stale-pull.parquet"
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
	manifest := &synchronizedManifestEntry{entry: *oldEntry}
	p.cfg.ManifestEntry = manifest.get
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(oldEntry)
	select {
	case <-fetcher.started:
	case <-time.After(3 * time.Second):
		t.Fatal("initial fetch did not start")
	}
	manifest.set(newEntry)
	p.EnqueueContentChanged(&newEntry)
	close(fetcher.release)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1 && s["inflight_count"] == 0
	})
	if fetcher.calls.Load() != 2 || stats["pulled"] != 1 {
		t.Fatalf("new version was not handed off: calls=%d stats=%v", fetcher.calls.Load(), stats)
	}
	if deleted := backend.deleteCount(); deleted != 0 {
		t.Fatalf("valid stale-generation bytes were deleted before replacement: deletes=%d", deleted)
	}
	got, err := backend.Read(context.Background(), path)
	if err != nil || !bytes.Equal(got, newBody) {
		t.Fatalf("wrong final content: got %q err=%v", got, err)
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
	manifest := &synchronizedManifestEntry{entry: *oldEntry}
	p.cfg.ManifestEntry = manifest.get

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

	manifest.set(newEntry)
	p.EnqueueContentChanged(&newEntry)

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
	stale := waitStats(t, p, func(s map[string]int64) bool {
		return s["skipped_superseded"] == 1 && s["inflight_count"] == 0
	})
	if superseded := stale["skipped_superseded"]; superseded != 1 {
		t.Fatalf("stale callback was not rejected: skipped_superseded=%d", superseded)
	}

	// Repeating the already-completed current version must retain the
	// original already-local optimization instead of downloading again.
	p.Enqueue(&newEntry)
	final := waitStats(t, p, func(s map[string]int64) bool {
		return s["skipped_local"] == 2 && s["inflight_count"] == 0
	})
	if final["skipped_local"] != 2 || fetcher.calls.Load() != 2 {
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
	manifest := &synchronizedManifestEntry{entry: *oldEntry}
	p.cfg.ManifestEntry = manifest.get

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

	manifest.set(newEntry)
	p.EnqueueContentChanged(&newEntry)

	failed := waitStats(t, p, func(s map[string]int64) bool {
		return s["failed"] == 1 && s["inflight_count"] == 0
	})
	if failed["failed"] != 1 || failed["inflight_count"] != 0 {
		t.Fatalf("refresh failure did not settle: %v", failed)
	}

	if calls := fetcher.calls.Load(); calls != 2 {
		t.Fatalf("expected two fetch attempts before retry, got %d", calls)
	}

	// A later metadata-only manifest version carries identical bytes but must
	// still retry the unresolved forced refresh.
	retryEntry := newEntry
	retryEntry.LSN = 3
	manifest.set(retryEntry)
	p.Enqueue(&retryEntry)

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

// A genuinely failed forced refresh retains retry state, and manifest deletion
// clears it. This proves the deletion assertion is not vacuous.
func TestPullerManifestDeleteClearsRefreshPendingIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/deleted-rewrite.parquet"

	oldBody := []byte("old-payload")
	newBody := []byte("new-payload")
	entry := makeEntry(path, "writer-1", int64(len(newBody)))
	entry.LSN = 2
	hash := sha256.Sum256(newBody)
	entry.SHA256 = fmt.Sprintf("%x", hash)

	backend := newFakeBackend()
	if err := backend.Write(context.Background(), path, oldBody); err != nil {
		t.Fatalf("write stale local bytes: %v", err)
	}
	fetcher := newFakeFetcher(fakeFetchResult{err: errors.New("peer unavailable")})

	p := newTestPuller(t, backend, fetcher, staticResolver{
		nodeID: "writer-1",
		addrs:  []string{"peer:9100"},
		ok:     true,
	})
	p.cfg.RetryMaxAttempts = 1
	p.cfg.ManifestEntry = func(string) (raft.FileEntry, bool) { return *entry, true }

	p.Start(context.Background())
	defer p.Stop()

	p.EnqueueContentChanged(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["failed"] == 1 && s["inflight_count"] == 0
	})
	if stats["failed"] != 1 || stats["inflight_count"] != 0 {
		t.Fatalf("forced refresh did not fail as arranged: %v", stats)
	}

	p.inflightMu.Lock()
	_, existedBefore := p.refreshPending[path]
	p.inflightMu.Unlock()
	if !existedBefore {
		t.Fatal("failed forced refresh did not retain retry state")
	}

	p.OnManifestDelete(path)

	p.inflightMu.Lock()
	_, existsAfter := p.refreshPending[path]
	p.inflightMu.Unlock()

	if existsAfter {
		t.Fatal("deleted manifest entry leaked refresh-pending state")
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
	if result := p.enqueue(oldEntry, enqueueSourceCatchUp, false); result != enqueueResultEnqueued {
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
	if result := p.enqueue(&newEntry, enqueueSourceReconciliation, true); result != enqueueResultEnqueued {
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

func TestPullerCatchUpSupersededEntryQueuesCurrentVersionIssue798(t *testing.T) {
	const path = "db/cpu/2026/09/19/01/stale-catchup-page.parquet"
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
	if err := backend.Write(context.Background(), path, oldBody); err != nil {
		t.Fatalf("write stale same-size catch-up copy: %v", err)
	}
	fetcher := newFakeFetcher(fakeFetchResult{body: newBody})
	p := newTestPuller(t, backend, fetcher, staticResolver{
		nodeID: "writer-1",
		addrs:  []string{"peer:9100"},
		ok:     true,
	})
	p.cfg.ManifestEntry = func(string) (raft.FileEntry, bool) { return newEntry, true }
	p.Start(context.Background())
	defer p.Stop()

	p.RunCatchUp(context.Background(), singlePage(oldEntry))
	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1 && s["inflight_count"] == 0 && s["catchup_inflight"] == 0
	})
	if fetcher.calls.Load() != 1 || stats["catchup_failed"] != 0 || !p.FullyCaughtUp() {
		t.Fatalf("stale catch-up entry did not hand off to current version: calls=%d stats=%v", fetcher.calls.Load(), stats)
	}
	got, err := backend.Read(context.Background(), path)
	if err != nil || !bytes.Equal(got, newBody) {
		t.Fatalf("current manifest bytes were not installed: got %q err=%v", got, err)
	}
}
