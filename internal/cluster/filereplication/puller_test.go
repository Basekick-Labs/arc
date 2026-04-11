package filereplication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// --- Fakes ---------------------------------------------------------------

// fakeBackend is an in-memory storage.Backend used by the puller tests. Only
// the methods the puller calls are implemented; the rest panic.
type fakeBackend struct {
	mu    sync.Mutex
	files map[string][]byte
	// If nonzero, Exists returns this value before looking at the map.
	forceExists *bool
	// If nonempty, the backend simulates a write error.
	writeErr error
	// Track delete calls for assertions.
	deletedPaths []string
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{files: make(map[string][]byte)}
}

func (f *fakeBackend) Write(ctx context.Context, path string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		return f.writeErr
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.files[path] = cp
	return nil
}

func (f *fakeBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	f.mu.Lock()
	writeErr := f.writeErr
	f.mu.Unlock()
	if writeErr != nil {
		return writeErr
	}
	// Read the full body to simulate a real backend.
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[path] = data
	return nil
}

func (f *fakeBackend) Read(ctx context.Context, path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.files[path]
	if !ok {
		return nil, errors.New("not found")
	}
	return data, nil
}

func (f *fakeBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	data, err := f.Read(ctx, path)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

func (f *fakeBackend) List(ctx context.Context, prefix string) ([]string, error) {
	panic("not used")
}

func (f *fakeBackend) Delete(ctx context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, path)
	f.deletedPaths = append(f.deletedPaths, path)
	return nil
}

func (f *fakeBackend) Exists(ctx context.Context, path string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forceExists != nil {
		return *f.forceExists, nil
	}
	_, ok := f.files[path]
	return ok, nil
}

func (f *fakeBackend) Close() error         { return nil }
func (f *fakeBackend) Type() string         { return "fake" }
func (f *fakeBackend) ConfigJSON() string   { return "{}" }
func (f *fakeBackend) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deletedPaths)
}

// fakeFetcher implements Fetcher with a scripted behavior. By default each
// Fetch call consumes the next scripted result and returns an error after
// the script runs out — useful for tests that care about exact per-call
// sequencing. Setting repeat=true causes the last scripted result to be
// returned on all subsequent calls, which is what tests that care about
// "same behavior forever, just count calls" (dedup, catch-up walk) want.
type fakeFetcher struct {
	mu sync.Mutex
	// Per-call scripted results. index == call number (0-based).
	results []fakeFetchResult
	// If true, the last result is returned on every call past len(results).
	// If false, exhausted calls return an error (catches unexpected retries).
	repeat bool
	calls  atomic.Int64
}

type fakeFetchResult struct {
	body []byte
	err  error
}

func newFakeFetcher(results ...fakeFetchResult) *fakeFetcher {
	return &fakeFetcher{results: results}
}

// newRepeatingFetcher returns a fakeFetcher that yields the same successful
// body on every call, forever. Used by catch-up tests that drive many
// distinct entries through a single fetcher and care only about counts.
func newRepeatingFetcher(body []byte) *fakeFetcher {
	return &fakeFetcher{
		results: []fakeFetchResult{{body: body}},
		repeat:  true,
	}
}

func (f *fakeFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer) (int64, error) {
	idx := f.calls.Add(1) - 1
	f.mu.Lock()
	defer f.mu.Unlock()
	if int(idx) >= len(f.results) {
		if f.repeat && len(f.results) > 0 {
			// Replay the last scripted result.
			idx = int64(len(f.results) - 1)
		} else {
			// Unscripted overflow — tests that care about exact sequencing
			// want to see this as an error rather than a silent no-op.
			return 0, errors.New("fake fetcher: no more scripted results")
		}
	}
	r := f.results[idx]
	if r.err != nil {
		// For checksum mismatches, still "write" the bad body so the backend
		// observes the corrupt bytes that need cleanup.
		if errors.Is(r.err, ErrChecksumMismatch) && len(r.body) > 0 {
			_, _ = dst.Write(r.body)
		}
		return 0, r.err
	}
	n, err := dst.Write(r.body)
	return int64(n), err
}

// perPeerFetcher dispatches Fetch calls based on the peer address. Used to
// exercise the multi-peer fallback loop inside processEntry: script one
// behavior for peer A and another for peer B, then observe whether the
// puller correctly falls through when A fails.
type perPeerFetcher struct {
	mu       sync.Mutex
	handlers map[string]func(dst io.Writer) (int64, error)
	// calls keeps a per-peer call count so tests can assert exact behavior.
	calls map[string]*atomic.Int64
	// total is the aggregate call count across all peers.
	total atomic.Int64
}

func newPerPeerFetcher() *perPeerFetcher {
	return &perPeerFetcher{
		handlers: make(map[string]func(dst io.Writer) (int64, error)),
		calls:    make(map[string]*atomic.Int64),
	}
}

func (f *perPeerFetcher) handle(peerAddr string, fn func(dst io.Writer) (int64, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[peerAddr] = fn
	if _, ok := f.calls[peerAddr]; !ok {
		f.calls[peerAddr] = new(atomic.Int64)
	}
}

func (f *perPeerFetcher) callsFor(peerAddr string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.calls[peerAddr]; ok {
		return c.Load()
	}
	return 0
}

func (f *perPeerFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer) (int64, error) {
	f.total.Add(1)
	f.mu.Lock()
	fn, ok := f.handlers[peerAddr]
	counter := f.calls[peerAddr]
	f.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("perPeerFetcher: no handler registered for %s", peerAddr)
	}
	if counter != nil {
		counter.Add(1)
	}
	return fn(dst)
}

// multiPeerResolver returns a fixed list of peer addresses, regardless of
// origin or path. Used by the multi-peer fallback tests.
type multiPeerResolver struct {
	addrs []string
}

func (r multiPeerResolver) ResolvePeers(_, _ string) []string {
	return r.addrs
}

// staticResolver maps a single origin node ID to a fixed list of addresses.
// ok=false simulates "origin not in registry"; the puller treats an empty
// slice as no candidates and defers.
type staticResolver struct {
	nodeID string
	addrs  []string
	ok     bool
}

func (s staticResolver) ResolvePeers(originNodeID, _ string) []string {
	if !s.ok || originNodeID != s.nodeID {
		return nil
	}
	return s.addrs
}

// --- Helpers -------------------------------------------------------------

func makeEntry(path, origin string, size int64) *raft.FileEntry {
	return &raft.FileEntry{
		Path:          path,
		SizeBytes:     size,
		Database:      "testdb",
		Measurement:   "cpu",
		OriginNodeID:  origin,
		Tier:          "hot",
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC),
		LSN:           1,
		SHA256:        "deadbeef",
	}
}

func newTestPuller(t *testing.T, backend *fakeBackend, fetcher Fetcher, resolver PeerResolver) *Puller {
	t.Helper()
	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             1,
		QueueSize:           8,
		RetryMaxAttempts:    3,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New puller: %v", err)
	}
	return p
}

// waitStats spins briefly until the predicate is true or the deadline is hit.
// Avoids flaky sleeps while keeping tests deterministic.
func waitStats(t *testing.T, p *Puller, pred func(map[string]int64) bool) map[string]int64 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := p.Stats()
		if pred(stats) {
			return stats
		}
		time.Sleep(10 * time.Millisecond)
	}
	return p.Stats()
}

// --- Tests ---------------------------------------------------------------

func TestPullerHappyPath(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("parquet body bytes")
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/file-1.parquet", "writer-1", int64(len(body)))
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 {
		t.Fatalf("expected 1 pulled, got %+v", stats)
	}
	if stats["failed"] != 0 {
		t.Errorf("expected 0 failed, got %d", stats["failed"])
	}
	// File should exist in the fake backend
	got, err := backend.Read(context.Background(), entry.Path)
	if err != nil {
		t.Fatalf("Read after pull: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("body mismatch: got %q, want %q", got, body)
	}
}

func TestPullerSkipsSelfOrigin(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher() // no scripted results — any call is a bug
	resolver := staticResolver{nodeID: "reader-1", addrs: []string{"self:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	// Origin == SelfNodeID ("reader-1")
	entry := makeEntry("testdb/cpu/file-2.parquet", "reader-1", 100)
	p.Enqueue(entry)

	// Give the puller a chance to actually skip; enqueue is synchronous for skipped-self.
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["skipped_self"] == 1 })
	if stats["skipped_self"] != 1 {
		t.Errorf("expected skipped_self=1, got %+v", stats)
	}
	if stats["enqueued"] != 0 {
		t.Errorf("skipped-self should not increment enqueued, got %d", stats["enqueued"])
	}
	if fetcher.calls.Load() != 0 {
		t.Errorf("fetcher was called for self-origin entry")
	}
}

func TestPullerSkipsAlreadyLocalFile(t *testing.T) {
	backend := newFakeBackend()
	// Pre-populate: the file already exists locally.
	_ = backend.Write(context.Background(), "testdb/cpu/existing.parquet", []byte("old bytes"))
	fetcher := newFakeFetcher() // should never be called
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/existing.parquet", "writer-1", 100)
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["skipped_local"] == 1 })
	if stats["skipped_local"] != 1 {
		t.Errorf("expected skipped_local=1, got %+v", stats)
	}
	if fetcher.calls.Load() != 0 {
		t.Errorf("fetcher should not be called when file exists locally")
	}
}

func TestPullerRetriesAndGivesUp(t *testing.T) {
	backend := newFakeBackend()
	// All 3 attempts return errors — none are ErrChecksumMismatch.
	fetcher := newFakeFetcher(
		fakeFetchResult{err: errors.New("dial refused")},
		fakeFetchResult{err: errors.New("dial refused")},
		fakeFetchResult{err: errors.New("dial refused")},
	)
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/file-err.parquet", "writer-1", 100)
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["failed"] == 1 })
	if stats["failed"] != 1 {
		t.Fatalf("expected failed=1 after retry exhaustion, got %+v", stats)
	}
	if stats["pulled"] != 0 {
		t.Errorf("expected pulled=0, got %d", stats["pulled"])
	}
	if got := fetcher.calls.Load(); got != 3 {
		t.Errorf("expected 3 fetch attempts, got %d", got)
	}
}

func TestPullerChecksumMismatchDeletesAndCounts(t *testing.T) {
	backend := newFakeBackend()
	// First attempt: bad checksum. Second: good.
	goodBody := []byte("good parquet")
	fetcher := newFakeFetcher(
		fakeFetchResult{body: []byte("corrupt"), err: ErrChecksumMismatch},
		fakeFetchResult{body: goodBody},
	)
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/file-checksum.parquet", "writer-1", int64(len(goodBody)))
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 {
		t.Fatalf("expected recovery on retry, got %+v", stats)
	}
	if stats["checksum_mismatch"] != 1 {
		t.Errorf("expected checksum_mismatch=1, got %d", stats["checksum_mismatch"])
	}
	if backend.deleteCount() == 0 {
		t.Errorf("expected backend.Delete called after checksum mismatch, got 0")
	}
	// Final file should be the good body.
	got, err := backend.Read(context.Background(), entry.Path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != string(goodBody) {
		t.Errorf("final body mismatch: got %q, want %q", got, goodBody)
	}
}

func TestPullerPeerLookupFailure(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher() // never called
	// Resolver returns ok=false for everything.
	resolver := staticResolver{nodeID: "unknown", addrs: nil, ok: false}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/orphan.parquet", "writer-1", 100)
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["failed"] == 1 })
	if stats["peer_lookup_failure"] < 1 {
		t.Errorf("expected peer_lookup_failure>=1, got %+v", stats)
	}
	if fetcher.calls.Load() != 0 {
		t.Errorf("fetcher should not be called when peer lookup fails")
	}
}

func TestPullerQueueDropUnderOverload(t *testing.T) {
	backend := newFakeBackend()
	// Blocking fetcher: never returns until we signal. This keeps the worker
	// busy so the queue fills up.
	blockCh := make(chan struct{})
	fetcher := &blockingFetcher{release: blockCh}
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             1,
		QueueSize:           2, // small queue so we can overflow
		RetryMaxAttempts:    1,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        5 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer func() {
		// Unblock the fetcher before Stop to let the worker exit gracefully.
		close(blockCh)
		p.Stop()
	}()

	// Enqueue many DISTINCT paths, more than QueueSize+inflight (1). With
	// Workers=1 and QueueSize=2, at most ~3 entries can land before drops.
	// Distinct paths matter now: Phase 3 dedups identical paths via the
	// inflight set, so we need unique paths to exercise queue overflow.
	const totalEnqueued = 50
	for i := 0; i < totalEnqueued; i++ {
		entry := makeEntry(fmt.Sprintf("testdb/cpu/overload-%04d.parquet", i), "writer-1", 100)
		p.Enqueue(entry)
	}

	stats := p.Stats()
	if stats["dropped"] == 0 {
		t.Errorf("expected some dropped entries under overload, got %+v", stats)
	}
	if stats["enqueued"]+stats["dropped"] != int64(totalEnqueued) {
		t.Errorf("enqueued(%d) + dropped(%d) != total(%d)", stats["enqueued"], stats["dropped"], totalEnqueued)
	}
	if stats["skipped_dup"] != 0 {
		t.Errorf("distinct paths should not dedup, got skipped_dup=%d", stats["skipped_dup"])
	}
}

// TestPullerEnqueueDedup verifies that concurrent enqueues of the same path
// are deduplicated via the inflight set — essential for Phase 3 so the
// catch-up walker and reactive FSM callbacks don't double-pull a file when
// they race on a new write committing mid-walk.
func TestPullerEnqueueDedup(t *testing.T) {
	backend := newFakeBackend()
	// Blocking fetcher keeps the single worker busy on the first entry so
	// the inflight slot is held while we hammer Enqueue with duplicates.
	blockCh := make(chan struct{})
	fetcher := &blockingFetcher{release: blockCh}
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             1,
		QueueSize:           16,
		RetryMaxAttempts:    1,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        5 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer func() {
		close(blockCh)
		p.Stop()
	}()

	const dupCount = 50
	for i := 0; i < dupCount; i++ {
		entry := makeEntry("testdb/cpu/dedup.parquet", "writer-1", 100)
		p.Enqueue(entry)
	}

	stats := p.Stats()
	// Exactly one enqueue should succeed; the rest should be deduped.
	if stats["enqueued"] != 1 {
		t.Errorf("expected exactly 1 enqueued, got %d (stats=%+v)", stats["enqueued"], stats)
	}
	if stats["skipped_dup"] != int64(dupCount-1) {
		t.Errorf("expected %d skipped_dup, got %d", dupCount-1, stats["skipped_dup"])
	}
	if stats["dropped"] != 0 {
		t.Errorf("expected 0 dropped (queue is larger than dup count), got %d", stats["dropped"])
	}
}

// blockingFetcher blocks until release is closed. Used to simulate a worker
// stuck on a slow fetch so the queue overflows.
type blockingFetcher struct {
	release chan struct{}
	calls   atomic.Int64
}

func (b *blockingFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer) (int64, error) {
	b.calls.Add(1)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.release:
		return 0, errors.New("blocking fetcher released")
	}
}

func TestPullerStartStopIdempotent(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher()
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	p.Start(context.Background()) // second call should be a no-op
	p.Stop()
	p.Stop() // second stop should be a no-op
}

// --- Phase 3 tests: multi-peer fallback in processEntry ---

// TestPullerMultiPeerFallbackOnNotFound is the core Phase 3 test: the first
// peer in the resolver's list returns ErrFileNotOnPeer (the Kubernetes
// rotation case — original writer is gone or replaced) and the second peer
// succeeds. The puller must call peer-2 within the same attempt, not after
// a full retry cycle.
func TestPullerMultiPeerFallbackOnNotFound(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("fallback body")
	fetcher := newPerPeerFetcher()
	// Peer 1: the "dead" origin — doesn't have the file.
	fetcher.handle("1.1.1.1:9100", func(dst io.Writer) (int64, error) {
		return 0, fmt.Errorf("%w: file not found on local backend", ErrFileNotOnPeer)
	})
	// Peer 2: a healthy fallback peer that does have the file.
	fetcher.handle("2.2.2.2:9100", func(dst io.Writer) (int64, error) {
		n, _ := dst.Write(body)
		return int64(n), nil
	})
	resolver := multiPeerResolver{addrs: []string{"1.1.1.1:9100", "2.2.2.2:9100"}}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/fallback.parquet", "writer-1", int64(len(body)))
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1
	})
	if stats["pulled"] != 1 {
		t.Errorf("pulled: got %d, want 1", stats["pulled"])
	}
	if stats["failed"] != 0 {
		t.Errorf("failed: got %d, want 0 (fallback should succeed)", stats["failed"])
	}
	if c := fetcher.callsFor("1.1.1.1:9100"); c != 1 {
		t.Errorf("peer-1 calls: got %d, want 1", c)
	}
	if c := fetcher.callsFor("2.2.2.2:9100"); c != 1 {
		t.Errorf("peer-2 calls: got %d, want 1", c)
	}
}

// TestPullerMultiPeerFallbackOnTransportError verifies that a dial error
// (or any non-checksum, non-not-found error) also triggers fallback to the
// next candidate peer, not just the "not found" signal.
func TestPullerMultiPeerFallbackOnTransportError(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("transport body")
	fetcher := newPerPeerFetcher()
	// Peer 1: transport error (dial failure, network timeout, etc).
	fetcher.handle("1.1.1.1:9100", func(dst io.Writer) (int64, error) {
		return 0, errors.New("dial tcp 1.1.1.1:9100: connect: connection refused")
	})
	// Peer 2: healthy.
	fetcher.handle("2.2.2.2:9100", func(dst io.Writer) (int64, error) {
		n, _ := dst.Write(body)
		return int64(n), nil
	})
	resolver := multiPeerResolver{addrs: []string{"1.1.1.1:9100", "2.2.2.2:9100"}}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/transport.parquet", "writer-1", int64(len(body)))
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1
	})
	if stats["pulled"] != 1 {
		t.Errorf("pulled: got %d, want 1 (should have fallen through to peer-2)", stats["pulled"])
	}
	if c := fetcher.callsFor("2.2.2.2:9100"); c != 1 {
		t.Errorf("peer-2 calls: got %d, want 1", c)
	}
}

// TestPullerMultiPeerAllFailMaxAttempts verifies that when every candidate
// peer returns ErrFileNotOnPeer, the puller counts it as one failed attempt
// (tries all peers within that attempt) and eventually gives up after
// RetryMaxAttempts attempts.
func TestPullerMultiPeerAllFailMaxAttempts(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newPerPeerFetcher()
	fetcher.handle("1.1.1.1:9100", func(dst io.Writer) (int64, error) {
		return 0, fmt.Errorf("%w: file not found on local backend", ErrFileNotOnPeer)
	})
	fetcher.handle("2.2.2.2:9100", func(dst io.Writer) (int64, error) {
		return 0, fmt.Errorf("%w: file not found on local backend", ErrFileNotOnPeer)
	})
	resolver := multiPeerResolver{addrs: []string{"1.1.1.1:9100", "2.2.2.2:9100"}}

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             1,
		QueueSize:           4,
		RetryMaxAttempts:    2,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/lost.parquet", "writer-1", 100)
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["failed"] == 1
	})
	if stats["failed"] != 1 {
		t.Errorf("failed: got %d, want 1", stats["failed"])
	}
	// 2 attempts × 2 peers each = 4 total fetcher calls.
	total := fetcher.total.Load()
	if total != 4 {
		t.Errorf("total fetcher calls: got %d, want 4 (2 attempts × 2 peers)", total)
	}
}

// TestPullerMultiPeerCtxCanceledEarlyExit verifies that when the puller is
// stopped mid-attempt, the peer fallback loop exits immediately on the
// first context.Canceled error rather than iterating through every
// remaining candidate. Regression test for the Gemini Phase 3 review
// finding — without the explicit check, a 10-peer cluster would burn
// 10 wasted dial attempts + 10 debug log lines during shutdown.
func TestPullerMultiPeerCtxCanceledEarlyExit(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newPerPeerFetcher()
	// Peer 1: returns context.Canceled — simulates pullOnce observing
	// that the puller's own context was cancelled mid-fetch.
	fetcher.handle("1.1.1.1:9100", func(dst io.Writer) (int64, error) {
		return 0, fmt.Errorf("simulated mid-fetch cancel: %w", context.Canceled)
	})
	// Peers 2 and 3: would fall through if called. The puller must NOT
	// call either within the same attempt after seeing the cancel.
	fetcher.handle("2.2.2.2:9100", func(dst io.Writer) (int64, error) {
		return 0, fmt.Errorf("%w: file not found on local backend", ErrFileNotOnPeer)
	})
	fetcher.handle("3.3.3.3:9100", func(dst io.Writer) (int64, error) {
		return 0, fmt.Errorf("%w: file not found on local backend", ErrFileNotOnPeer)
	})
	resolver := multiPeerResolver{addrs: []string{"1.1.1.1:9100", "2.2.2.2:9100", "3.3.3.3:9100"}}

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             1,
		QueueSize:           4,
		RetryMaxAttempts:    1,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/ctxcancel.parquet", "writer-1", 100)
	p.Enqueue(entry)

	// Wait for the single attempt to complete. The walker returns from
	// processEntry on ctx.Canceled without touching peers 2 or 3.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fetcher.total.Load() > 0 && p.Stats()["enqueued"] == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Give the worker a brief moment in case it were (incorrectly) iterating.
	time.Sleep(50 * time.Millisecond)

	if c := fetcher.callsFor("1.1.1.1:9100"); c != 1 {
		t.Errorf("peer-1 calls: got %d, want 1", c)
	}
	if c := fetcher.callsFor("2.2.2.2:9100"); c != 0 {
		t.Errorf("peer-2 calls: got %d, want 0 (ctx.Canceled must break the peer loop immediately)", c)
	}
	if c := fetcher.callsFor("3.3.3.3:9100"); c != 0 {
		t.Errorf("peer-3 calls: got %d, want 0 (ctx.Canceled must break the peer loop immediately)", c)
	}
}

// TestPullerMultiPeerChecksumMismatchDoesNotFallThrough is the regression
// test for the "checksum mismatch breaks the peer loop" decision in the
// Phase 3 plan. A corrupt body from peer-1 is a data integrity signal —
// the puller should NOT then try peer-2 within the same attempt (which
// would pull-and-corrupt from every healthy peer in turn). Instead it
// delete-and-retries on the next attempt.
func TestPullerMultiPeerChecksumMismatchDoesNotFallThrough(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newPerPeerFetcher()
	// Peer 1: returns a checksum mismatch. The puller must short-circuit
	// out of the per-attempt peer loop.
	fetcher.handle("1.1.1.1:9100", func(dst io.Writer) (int64, error) {
		return 0, ErrChecksumMismatch
	})
	// Peer 2: would succeed if called. The puller must NOT call it within
	// the same attempt.
	peer2Called := atomic.Int64{}
	fetcher.handle("2.2.2.2:9100", func(dst io.Writer) (int64, error) {
		peer2Called.Add(1)
		return 0, ErrChecksumMismatch
	})
	resolver := multiPeerResolver{addrs: []string{"1.1.1.1:9100", "2.2.2.2:9100"}}

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             1,
		QueueSize:           4,
		RetryMaxAttempts:    1, // one attempt — no retries, so we can count peers precisely
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/corrupt.parquet", "writer-1", 100)
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["failed"] == 1
	})
	if stats["failed"] != 1 {
		t.Errorf("failed: got %d, want 1", stats["failed"])
	}
	if stats["checksum_mismatch"] != 1 {
		t.Errorf("checksum_mismatch: got %d, want 1", stats["checksum_mismatch"])
	}
	// Critical assertion: peer-2 must NOT have been called within the same
	// attempt after peer-1's checksum mismatch.
	if c := fetcher.callsFor("1.1.1.1:9100"); c != 1 {
		t.Errorf("peer-1 calls: got %d, want 1", c)
	}
	if c := fetcher.callsFor("2.2.2.2:9100"); c != 0 {
		t.Errorf("peer-2 calls: got %d, want 0 (checksum mismatch must break the peer loop)", c)
	}
}

func TestPullerConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:    "missing backend",
			mutate:  func(c *Config) { c.Backend = nil },
			wantErr: "Backend",
		},
		{
			name:    "missing fetcher",
			mutate:  func(c *Config) { c.Fetcher = nil },
			wantErr: "Fetcher",
		},
		{
			name:    "missing resolver",
			mutate:  func(c *Config) { c.PeerResolver = nil },
			wantErr: "PeerResolver",
		},
		{
			name:    "missing self node id",
			mutate:  func(c *Config) { c.SelfNodeID = "" },
			wantErr: "SelfNodeID",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				SelfNodeID:   "reader-1",
				Backend:      newFakeBackend(),
				Fetcher:      newFakeFetcher(),
				PeerResolver: staticResolver{},
				Logger:       zerolog.Nop(),
			}
			tc.mutate(&cfg)
			_, err := New(cfg)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
