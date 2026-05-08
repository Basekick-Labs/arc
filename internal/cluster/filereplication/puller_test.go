package filereplication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
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
	// Atomic: read all bytes into a staging key. On success, promote to final key.
	// On error, leave bytes in the ".part" staging key (same as real LocalBackend)
	// so resume tests can discover them via StatFile/ReadTo.
	stagingKey := path + ".part"
	var buf []byte
	tmp := make([]byte, 4096)
	var readErr error
	for {
		n, err := reader.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			readErr = err
			break
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if readErr == io.EOF || readErr == nil {
		// Full transfer: commit to final path and clear staging.
		if len(buf) > 0 {
			f.files[path] = buf
		}
		delete(f.files, stagingKey)
		return nil
	}
	// Partial transfer: store under staging key; final key is not written.
	if len(buf) > 0 {
		f.files[stagingKey] = buf
	}
	return readErr
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
	// Fall back to staging file so tryResumeFromPartial can hash a partial prefix.
	f.mu.Lock()
	data, ok := f.files[path]
	if !ok {
		data, ok = f.files[path+".part"]
	}
	f.mu.Unlock()
	if !ok {
		return errors.New("not found")
	}
	_, err := writer.Write(data)
	return err
}

func (f *fakeBackend) ReadToAt(ctx context.Context, path string, writer io.Writer, offset int64) error {
	f.mu.Lock()
	data, ok := f.files[path]
	if !ok {
		data, ok = f.files[path+".part"]
	}
	f.mu.Unlock()
	if !ok {
		return errors.New("not found")
	}
	if offset < 0 || offset > int64(len(data)) {
		return fmt.Errorf("offset %d out of range for file size %d", offset, len(data))
	}
	_, err := writer.Write(data[offset:])
	return err
}

func (f *fakeBackend) StatFile(ctx context.Context, path string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if data, ok := f.files[path]; ok {
		return int64(len(data)), nil
	}
	// Check staging file (mirrors LocalBackend behaviour).
	if data, ok := f.files[path+".part"]; ok {
		return int64(len(data)), nil
	}
	return -1, nil
}

func (f *fakeBackend) AppendReader(ctx context.Context, path string, reader io.Reader, appendSize int64) error {
	f.mu.Lock()
	writeErr := f.writeErr
	f.mu.Unlock()
	if writeErr != nil {
		return writeErr
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	stagingKey := path + ".part"
	f.mu.Lock()
	defer f.mu.Unlock()
	merged := append(f.files[stagingKey], tail...)
	if int64(len(tail)) == appendSize {
		// Full tail received — promote staging → final.
		f.files[path] = merged
		delete(f.files, stagingKey)
	} else {
		f.files[stagingKey] = merged
	}
	return nil
}

func (f *fakeBackend) Delete(ctx context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, path)
	delete(f.files, path+".part") // also clean up staging file
	f.deletedPaths = append(f.deletedPaths, path)
	return nil
}

func (f *fakeBackend) List(ctx context.Context, prefix string) ([]string, error) {
	panic("not used")
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

func (f *fakeFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer, byteOffset int64, prefixHasher hash.Hash) (int64, error) {
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

func (f *perPeerFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer, byteOffset int64, prefixHasher hash.Hash) (int64, error) {
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
	// Pre-populate: the file already exists locally, size matches entry.SizeBytes.
	fileData := bytes.Repeat([]byte("x"), 100)
	_ = backend.Write(context.Background(), "testdb/cpu/existing.parquet", fileData)
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

func (b *blockingFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer, byteOffset int64, prefixHasher hash.Hash) (int64, error) {
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

// --- Resume / ByteOffset tests -------------------------------------------

// resumeAwareFetcher is a Fetcher that records the byteOffset passed to each
// Fetch call and writes only the tail of body (body[byteOffset:]) into dst.
// This lets puller resume tests verify that pullOnce passes the right offset
// on the second attempt.
type resumeAwareFetcher struct {
	mu          sync.Mutex
	body        []byte // full file bytes
	offsets     []int64
	errors      []error // per-call scripted errors; nil = success
	callCount   atomic.Int64
}

func newResumeAwareFetcher(body []byte, errs ...error) *resumeAwareFetcher {
	return &resumeAwareFetcher{body: body, errors: errs}
}

func (f *resumeAwareFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer, byteOffset int64, prefixHasher hash.Hash) (int64, error) {
	idx := int(f.callCount.Add(1) - 1)
	f.mu.Lock()
	f.offsets = append(f.offsets, byteOffset)
	var fetchErr error
	if idx < len(f.errors) {
		fetchErr = f.errors[idx]
	}
	f.mu.Unlock()

	// On a transport error (not ErrBadOffset/ErrChecksumMismatch), write a
	// partial body before returning the error to simulate a mid-transfer drop.
	if fetchErr != nil && !errors.Is(fetchErr, ErrBadOffset) && !errors.Is(fetchErr, ErrChecksumMismatch) {
		partial := f.body[byteOffset:]
		if len(partial) > 4 {
			partial = partial[:len(partial)/2] // write half of the tail
		}
		_, _ = dst.Write(partial)
		return int64(len(partial)), fetchErr
	}
	if fetchErr != nil {
		return 0, fetchErr
	}
	tail := f.body[byteOffset:]
	n, writeErr := dst.Write(tail)
	return int64(n), writeErr
}

func (f *resumeAwareFetcher) lastOffset() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.offsets) == 0 {
		return -1
	}
	return f.offsets[len(f.offsets)-1]
}

// TestPuller_ResumeOnRetry verifies that when a partial file is already on
// disk, the second Fetch call uses byteOffset == len(partial).
func TestPuller_ResumeOnRetry(t *testing.T) {
	fullBody := []byte("0123456789abcdefghijklmnopqrstuvwxyz") // 36 bytes
	entry := makeEntry("db/cpu/resume.parquet", "writer-1", int64(len(fullBody)))

	backend := newFakeBackend()

	// Attempt 1: the fetcher writes half the tail then returns a transport error,
	// leaving a partial file on disk. Attempt 2 resumes from the partial offset.
	fetcher := newResumeAwareFetcher(fullBody, errors.New("transport: connection reset"))

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        multiPeerResolver{addrs: []string{"peer-1:9999"}},
		Workers:             1,
		QueueSize:           8,
		RetryMaxAttempts:    2,
		RetryInitialBackoff: 1 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(entry)

	// Wait for both Fetch calls.
	waitStats(t, p, func(s map[string]int64) bool {
		return fetcher.callCount.Load() >= 2
	})

	if fetcher.callCount.Load() < 2 {
		t.Fatalf("expected at least 2 Fetch calls, got %d", fetcher.callCount.Load())
	}
	// Second call should use a non-zero byte offset (the partial written by attempt 1).
	if fetcher.lastOffset() <= 0 {
		t.Errorf("second Fetch offset: got %d, want > 0 (should resume from partial)", fetcher.lastOffset())
	}
}

// TestPuller_BadOffsetDeletesPartialAndRetries verifies that when the fetcher
// returns ErrBadOffset, the puller deletes the partial file, increments the
// bad_offset counter, and continues to retry from zero.
func TestPuller_BadOffsetDeletesPartialAndRetries(t *testing.T) {
	fullBody := []byte("full file content here")
	entry := makeEntry("db/cpu/badoffset.parquet", "writer-1", int64(len(fullBody)))

	backend := newFakeBackend()

	// First call returns ErrBadOffset (server rejects offset 0 — unusual but
	// possible if the file was replaced between manifest registration and fetch).
	// Second call succeeds from offset 0.
	fetcher := newResumeAwareFetcher(fullBody, ErrBadOffset)

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        multiPeerResolver{addrs: []string{"peer-1:9999"}},
		Workers:             1,
		QueueSize:           8,
		RetryMaxAttempts:    3,
		RetryInitialBackoff: 1 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1
	})

	if stats["pulled"] != 1 {
		t.Fatalf("expected pulled=1, got %v (stats=%v)", stats["pulled"], stats)
	}
	if stats["bad_offset_server"] != 1 {
		t.Errorf("expected bad_offset_server=1, got %v", stats)
	}
	// Full file should be on disk after the successful second attempt.
	data, readErr := backend.Read(context.Background(), entry.Path)
	if readErr != nil {
		t.Fatalf("Read after resume: %v", readErr)
	}
	if string(data) != string(fullBody) {
		t.Errorf("file content mismatch: got %q, want %q", data, fullBody)
	}
}

// nonAppendingBackend delegates all Backend methods to fakeBackend but
// deliberately omits AppendReader, so the puller's AppendingBackend
// type-assertion fails and ErrResumeNotSupported is returned.
type nonAppendingBackend struct {
	inner *fakeBackend
}

func (b *nonAppendingBackend) Write(ctx context.Context, path string, data []byte) error {
	return b.inner.Write(ctx, path, data)
}
func (b *nonAppendingBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return b.inner.WriteReader(ctx, path, r, size)
}
func (b *nonAppendingBackend) Read(ctx context.Context, path string) ([]byte, error) {
	return b.inner.Read(ctx, path)
}
func (b *nonAppendingBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	return b.inner.ReadTo(ctx, path, w)
}
func (b *nonAppendingBackend) ReadToAt(ctx context.Context, path string, w io.Writer, offset int64) error {
	return b.inner.ReadToAt(ctx, path, w, offset)
}
func (b *nonAppendingBackend) StatFile(ctx context.Context, path string) (int64, error) {
	return b.inner.StatFile(ctx, path)
}
func (b *nonAppendingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return b.inner.List(ctx, prefix)
}
func (b *nonAppendingBackend) Delete(ctx context.Context, path string) error {
	return b.inner.Delete(ctx, path)
}
func (b *nonAppendingBackend) Exists(ctx context.Context, path string) (bool, error) {
	return b.inner.Exists(ctx, path)
}
func (b *nonAppendingBackend) Close() error       { return nil }
func (b *nonAppendingBackend) Type() string       { return "non-appending" }
func (b *nonAppendingBackend) ConfigJSON() string { return "{}" }

// TestPuller_NonAppendingBackendFallback verifies that when the backend does
// not implement AppendingBackend (e.g. S3/Azure), a resume attempt falls back
// to a full re-fetch: the bad_offset_backend counter increments, the partial
// file is deleted, and the next attempt fetches from offset 0.
func TestPuller_NonAppendingBackendFallback(t *testing.T) {

	inner := newFakeBackend()
	backend := &nonAppendingBackend{inner: inner}

	fullBody := bytes.Repeat([]byte("z"), 40)
	entry := makeEntry("testdb/cpu/resume_fallback.parquet", "writer-1", int64(len(fullBody)))

	// Attempt 1: transport error + partial write (simulates mid-transfer drop).
	// Attempt 2: puller detects partial → type-asserts AppendingBackend → fails
	//            → ErrResumeNotSupported → bad_offset_backend++ → partial deleted.
	//            The fetcher is called but the write goroutine closes the pipe
	//            immediately, so the fetcher gets a broken-pipe error (scripted).
	// Attempt 3: no partial on disk → fresh full fetch from offset 0 → success.
	fetcher := newResumeAwareFetcher(fullBody,
		fmt.Errorf("transport error"),    // attempt 1: mid-transfer drop
		fmt.Errorf("write: broken pipe"), // attempt 2: pipe closed by write side
		nil,                              // attempt 3: success (fresh fetch from zero)
	)

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        multiPeerResolver{addrs: []string{"peer-1:9999"}},
		Workers:             1,
		QueueSize:           8,
		RetryMaxAttempts:    4,
		RetryInitialBackoff: 1 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1
	})

	if stats["pulled"] != 1 {
		t.Fatalf("expected pulled=1, got %v", stats)
	}
	if stats["bad_offset_backend"] != 1 {
		t.Errorf("expected bad_offset_backend=1, got %v", stats)
	}
	data, readErr := inner.Read(context.Background(), entry.Path)
	if readErr != nil {
		t.Fatalf("Read: %v", readErr)
	}
	if string(data) != string(fullBody) {
		t.Errorf("file content mismatch after fallback")
	}
}

// --- FullyCaughtUp tests -----------------------------------------------------

// TestPuller_FullyCaughtUp_FalseBeforeWalker covers the trivial case: a fresh
// puller with no startup catch-up run is not "fully caught up" because we have
// no signal that the walker has even seen the manifest yet.
func TestPuller_FullyCaughtUp_FalseBeforeWalker(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	if p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp before walker run: got true, want false")
	}
}

// TestPuller_FullyCaughtUp_FalseWhileCatchupBatchInflight covers the
// residual gap that shipped in Phase 3: CatchUpCompleted only signals that
// the walker is done enqueueing — it does not wait for the catch-up
// batch to settle. A reader in this state would still serve incomplete
// results if the query path consulted only CatchUpCompleted. FullyCaughtUp
// must return false while any catch-up-tagged path is still in flight.
//
// Note: only catch-up-tagged paths trigger the gate — see
// TestPuller_FullyCaughtUp_IgnoresSteadyStateInflight for the
// complementary case.
func TestPuller_FullyCaughtUp_FalseWhileCatchupBatchInflight(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	// Simulate the walker finishing and tagging two paths as catch-up.
	p.catchupCompletedAt.Store(time.Now().Unix())
	p.markCatchUp("db/cpu/2026/05/07/14/file-1.parquet")
	p.markCatchUp("db/cpu/2026/05/07/14/file-2.parquet")

	if got := p.catchupInflight.Load(); got != 2 {
		t.Fatalf("catchupInflight: got %d, want 2 (two tagged paths still in flight)", got)
	}
	if p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with 2 catch-up paths in flight: got true, want false")
	}
}

// TestPuller_FullyCaughtUp_FalseAfterCatchupFailedPull covers the silent-
// partial-results condition #392 closes: a walker-enqueued pull that
// exhausted retries and gave up means the file is missing on this reader,
// but processEntry's defer cleared the inflight slot. A naive "inflight==0"
// predicate would say "ready" while data is missing. FullyCaughtUp must
// check catchupFailed == 0.
func TestPuller_FullyCaughtUp_FalseAfterCatchupFailedPull(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	// Walker finished, no catch-up inflight, but a catch-up-tagged pull gave
	// up after retries.
	p.catchupCompletedAt.Store(time.Now().Unix())
	p.catchupFailed.Store(1)

	if p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with catchupFailed=1: got true, want false (file missing on reader)")
	}
}

// TestPuller_FullyCaughtUp_FalseAfterCatchupDroppedPull covers the same
// silent-partial-results gap from a different cause: a walker-enqueued
// entry was dropped because the queue was full. The inflight slot was
// released, but the file was never pulled. FullyCaughtUp must check
// catchupDropped == 0.
func TestPuller_FullyCaughtUp_FalseAfterCatchupDroppedPull(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	// Walker finished, nothing in catch-up inflight, but a catch-up entry was
	// dropped earlier when the queue was full.
	p.catchupCompletedAt.Store(time.Now().Unix())
	p.catchupDropped.Store(1)

	if p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with catchupDropped=1: got true, want false (file missing on reader)")
	}
}

// TestPuller_FullyCaughtUp_IgnoresSteadyStateFailures covers the gemini
// review finding (PR #419, MEDIUM) that gating on cumulative totalFailed /
// totalDropped would keep the gate red forever after any transient
// steady-state failure. The catch-up-scoped counters fix this: cumulative
// failures/drops outside the catch-up batch must NOT close the gate once
// it has cleared.
func TestPuller_FullyCaughtUp_IgnoresSteadyStateFailures(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	// Catch-up batch has settled cleanly.
	p.catchupCompletedAt.Store(time.Now().Unix())

	// A steady-state pull failed (e.g. transient network issue, peer rebooted).
	// totalFailed bumps but the catch-up-scoped counter does not.
	p.totalFailed.Store(3)
	p.totalDropped.Store(2)

	if !p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with steady-state totalFailed=3 / totalDropped=2 but clean catch-up batch: got false, want true (gate must not be sticky on cumulative counters)")
	}
}

// TestPuller_FullyCaughtUp_IgnoresSteadyStateInflight covers the most
// important behavioral change from the gemini review: in a busy cluster a
// writer flushes files constantly, the puller enqueues steady-state pulls
// continuously, and a naive "inflightCount == 0" predicate would mean the
// reader returns 503 every few seconds in normal operation. The gate must
// scope its inflight check to the catch-up batch only.
func TestPuller_FullyCaughtUp_IgnoresSteadyStateInflight(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	// Catch-up batch has settled cleanly; inflightCount is 0 and so is the
	// catch-up subset.
	p.catchupCompletedAt.Store(time.Now().Unix())

	// Simulate steady-state ingest: a reactive FSM callback fires Enqueue,
	// which adds a path to inflight. catchupInflight stays 0 because this
	// path was NOT walker-enqueued.
	p.inflightAdd("steady-state/db/m/2026/05/07/14/file.parquet")
	if got := p.inflightCount.Load(); got != 1 {
		t.Fatalf("inflightCount: got %d, want 1 (steady-state add)", got)
	}
	if got := p.catchupInflight.Load(); got != 0 {
		t.Fatalf("catchupInflight: got %d, want 0 (steady-state add must not affect catch-up scope)", got)
	}

	if !p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with steady-state inflight=1 but catchupInflight=0: got false, want true (gate must ignore steady-state ingest)")
	}
}

// TestPuller_FullyCaughtUp_TrueWhenAllSettled covers the success path: walker
// done, no inflight entries, no failures, no drops. This is the steady state
// that the query gate consumes.
func TestPuller_FullyCaughtUp_TrueWhenAllSettled(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	// Mark walker complete with no work ever enqueued: inflightCount is 0,
	// totalFailed is 0, totalDropped is 0. FullyCaughtUp should be true.
	p.catchupCompletedAt.Store(time.Now().Unix())

	if !p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with walker done + clean counters: got false, want true")
	}
}

// TestPuller_InflightCount_MirrorsMap verifies the atomic counter and the
// inflight map cannot diverge under add/remove. The atomic is the hot-path
// reader for FullyCaughtUp and Stats; if it drifts from the map the gate's
// correctness guarantee is broken.
func TestPuller_InflightCount_MirrorsMap(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	// Empty: counter == map.
	if got := p.inflightCount.Load(); got != 0 {
		t.Errorf("inflightCount on empty puller: got %d, want 0", got)
	}

	// Add three distinct paths.
	p.inflightAdd("a")
	p.inflightAdd("b")
	p.inflightAdd("c")
	if got := p.inflightCount.Load(); got != 3 {
		t.Errorf("inflightCount after 3 adds: got %d, want 3", got)
	}

	// Re-add one (dedup): counter must not double-count.
	if added := p.inflightAdd("a"); added {
		t.Errorf("inflightAdd duplicate: got true, want false")
	}
	if got := p.inflightCount.Load(); got != 3 {
		t.Errorf("inflightCount after duplicate add: got %d, want 3 (dedup)", got)
	}

	// Remove existing.
	p.inflightRemove("a")
	if got := p.inflightCount.Load(); got != 2 {
		t.Errorf("inflightCount after remove: got %d, want 2", got)
	}

	// Remove non-existent: must not decrement below current.
	p.inflightRemove("missing")
	if got := p.inflightCount.Load(); got != 2 {
		t.Errorf("inflightCount after no-op remove: got %d, want 2", got)
	}

	// Drain.
	p.inflightRemove("b")
	p.inflightRemove("c")
	if got := p.inflightCount.Load(); got != 0 {
		t.Errorf("inflightCount after drain: got %d, want 0", got)
	}
}

// TestPuller_CatchUpFailure_SelfHeals verifies that a successful pull for
// a previously-failed catch-up path decrements catchupFailed, so the gate
// can clear without a process restart. Pins the recovery semantics gemini
// flagged on the third review pass.
func TestPuller_CatchUpFailure_SelfHeals(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher(fakeFetchResult{body: []byte("ignored")})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)

	const path = "db/m/2026/05/07/14/flaky.parquet"

	// Walker finished. Catch-up-tagged pull failed.
	p.catchupCompletedAt.Store(time.Now().Unix())
	p.recordCatchUpFailure(path)

	if got := p.catchupFailed.Load(); got != 1 {
		t.Fatalf("catchupFailed after first failure: got %d, want 1", got)
	}
	if p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with catchupFailed=1: got true, want false")
	}

	// A reactive FSM callback fires later, the underlying issue has cleared,
	// and the pull succeeds. The gate must self-heal.
	p.clearCatchUpFailure(path)

	if got := p.catchupFailed.Load(); got != 0 {
		t.Errorf("catchupFailed after self-heal: got %d, want 0", got)
	}
	if !p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp after clearCatchUpFailure: got false, want true (gate must self-heal)")
	}
}

// TestPuller_CatchUpFailure_RecordIsIdempotent verifies that recording the
// same failure twice (which can happen if a path is re-enqueued, fails
// again, and the worker calls recordCatchUpFailure a second time) does not
// double-count. Without this, the counter could drift above the actual
// number of failed paths and clearCatchUpFailure would never bring it
// back to zero.
func TestPuller_CatchUpFailure_RecordIsIdempotent(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("ignored")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})

	const path = "db/m/2026/05/07/14/file.parquet"

	p.recordCatchUpFailure(path)
	p.recordCatchUpFailure(path)

	if got := p.catchupFailed.Load(); got != 1 {
		t.Errorf("catchupFailed after duplicate record: got %d, want 1 (idempotent)", got)
	}

	p.clearCatchUpFailure(path)
	if got := p.catchupFailed.Load(); got != 0 {
		t.Errorf("catchupFailed after clear: got %d, want 0", got)
	}

	// Clearing a path that is not failed must not underflow.
	p.clearCatchUpFailure(path)
	if got := p.catchupFailed.Load(); got != 0 {
		t.Errorf("catchupFailed after clear of non-failed path: got %d, want 0 (must not underflow)", got)
	}
}

// TestPuller_CatchUpFailure_ConcurrentWorkerIsolation verifies that a
// catchupFailed bump is attributable to the specific worker that hit the
// give-up path, not to whichever worker happens to be in processEntry's
// defer when ANY worker increments totalFailed. This is the bug gemini's
// HIGH-priority finding caught: using totalFailed.Load() > snapshot would
// cause cross-pollination between workers.
//
// The test exercises the helper directly rather than racing real workers
// so it's deterministic. Pins the contract: catchupFailed only increments
// for paths recorded via recordCatchUpFailure.
func TestPuller_CatchUpFailure_ConcurrentWorkerIsolation(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("ignored")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})

	// Simulate the racey scenario: a steady-state worker bumps totalFailed
	// (a non-catch-up path failed). Another worker is concurrently in its
	// defer for a catch-up-tagged path that succeeded — the global counter
	// has changed, but the local 'failed' bool is false, so the defer must
	// not bump catchupFailed.
	//
	// We model this by directly mirroring what the defer does today: only
	// bumping when the local 'failed' is true, regardless of any global
	// counter motion.
	p.totalFailed.Add(1) // Pretend worker B failed a steady-state path.

	if got := p.catchupFailed.Load(); got != 0 {
		t.Errorf("catchupFailed after totalFailed bump from non-catch-up worker: got %d, want 0 (no recordCatchUpFailure called)", got)
	}

	// Now record an actual catch-up failure and confirm it lands.
	p.recordCatchUpFailure("catchup/path.parquet")
	if got := p.catchupFailed.Load(); got != 1 {
		t.Errorf("catchupFailed after recordCatchUpFailure: got %d, want 1", got)
	}
}

// TestPuller_MarkCatchUp_DropCompensation pins the CRITICAL fix from PR
// #419 review pass 4: when RunCatchUp pre-marks a path then Enqueue
// returns "dropped" (queue full), the walker must unmark the catch-up
// tag — otherwise no worker will ever run inflightRemove for that path,
// catchupInflight leaks upward, and FullyCaughtUp never returns true
// (gate permanently closed).
func TestPuller_MarkCatchUp_DropCompensation(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("ignored")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})

	const path = "db/m/2026/05/07/14/dropped.parquet"

	// Walker pre-marks the path.
	added := p.markCatchUp(path)
	if !added {
		t.Fatalf("markCatchUp first call: got false, want true (path was not previously tagged)")
	}
	if got := p.catchupInflight.Load(); got != 1 {
		t.Fatalf("catchupInflight after mark: got %d, want 1", got)
	}

	// Simulate Enqueue dropping (queue full). Walker must compensate by
	// unmarking. Without this, catchupInflight stays at 1 forever.
	p.unmarkCatchUp(path)
	if got := p.catchupInflight.Load(); got != 0 {
		t.Errorf("catchupInflight after unmark on drop: got %d, want 0 (compensation must succeed)", got)
	}

	// Walker also records the drop separately so the gate stays red until
	// a reactive callback resolves the missing file. The unmark and the
	// drop record are independent bookkeeping — gate closes via
	// catchupDropped, not via catchupInflight.
	p.recordCatchUpDrop(path)
	if got := p.catchupDropped.Load(); got != 1 {
		t.Errorf("catchupDropped after recordCatchUpDrop: got %d, want 1", got)
	}
}

// TestPuller_CatchUpDrop_SelfHeals pins the MEDIUM fix from PR #419
// review pass 4: a catch-up drop must not require a process restart to
// clear. When a reactive FSM callback later succeeds for a previously-
// dropped path, catchupDropped decrements and the gate re-opens.
func TestPuller_CatchUpDrop_SelfHeals(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("ignored")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})

	const path = "db/m/2026/05/07/14/dropped.parquet"

	// Walker recorded the path as dropped during catch-up.
	p.catchupCompletedAt.Store(time.Now().Unix())
	p.recordCatchUpDrop(path)

	if got := p.catchupDropped.Load(); got != 1 {
		t.Fatalf("catchupDropped after record: got %d, want 1", got)
	}
	if p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp with catchupDropped=1: got true, want false")
	}

	// Reactive FSM callback later succeeds for the same path. Self-heal
	// fires — the gate clears without a process restart.
	p.clearCatchUpDrop(path)

	if got := p.catchupDropped.Load(); got != 0 {
		t.Errorf("catchupDropped after self-heal: got %d, want 0", got)
	}
	if !p.FullyCaughtUp() {
		t.Errorf("FullyCaughtUp after clearCatchUpDrop: got false, want true")
	}
}

// TestPuller_CatchUpDrop_RecordIsIdempotent verifies that recording the
// same drop twice does not double-count. Mirrors the failure-path
// idempotency contract.
func TestPuller_CatchUpDrop_RecordIsIdempotent(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("ignored")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})

	const path = "db/m/file.parquet"

	p.recordCatchUpDrop(path)
	p.recordCatchUpDrop(path)

	if got := p.catchupDropped.Load(); got != 1 {
		t.Errorf("catchupDropped after duplicate record: got %d, want 1 (idempotent)", got)
	}

	p.clearCatchUpDrop(path)
	if got := p.catchupDropped.Load(); got != 0 {
		t.Errorf("catchupDropped after clear: got %d, want 0", got)
	}

	// Clearing a path that is not dropped must not underflow.
	p.clearCatchUpDrop(path)
	if got := p.catchupDropped.Load(); got != 0 {
		t.Errorf("catchupDropped after clear of non-dropped path: got %d, want 0 (must not underflow)", got)
	}
}

// TestPuller_ProcessEntry_LateCatchUpTag pins gemini's HIGH-priority race
// fix from PR #419 review pass 5. Scenario:
//
//  1. A reactive FSM callback enqueues path X. Worker A starts processEntry.
//  2. The catch-up walker starts iterating, sees X in the manifest, calls
//     markCatchUp(X). The tag is added AFTER worker A's processEntry began.
//  3. Worker A's pull fails.
//
// Buggy (snapshot-at-entry) behavior: wasCatchUp captured at step 1 was
// false; defer doesn't call recordCatchUpFailure even though the path is
// now a catch-up path. inflightRemove decrements catchupInflight (which
// markCatchUp incremented) but no failure is recorded — the file is
// missing AND the gate clears. Silent partial results, exactly the
// condition #392 closes.
//
// Fixed (check-in-defer) behavior: defer reads isCatchUpPath at outcome
// time, sees the late tag, calls recordCatchUpFailure. Gate stays red.
//
// Test exercises the helper sequence directly to be deterministic — the
// race is real but observing it with goroutines is flaky.
func TestPuller_ProcessEntry_LateCatchUpTag(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("ignored")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})

	const path = "db/m/2026/05/07/14/late-tag.parquet"

	// Step 1: reactive enqueue puts the path in inflight (no catch-up tag).
	p.inflightAdd(path)
	if p.isCatchUpPath(path) {
		t.Fatalf("path tagged as catch-up before walker ran: precondition violated")
	}

	// Step 2: walker tags the path AFTER the worker has begun processing.
	// (Modeling the late-arriving markCatchUp call.)
	p.markCatchUp(path)
	if !p.isCatchUpPath(path) {
		t.Fatalf("markCatchUp did not tag the path")
	}

	// Step 3: worker's pull fails. The defer's check-in-defer logic must
	// see the late tag and call recordCatchUpFailure.
	//
	// We model the defer's exact decision: if isCatchUpPath returns true
	// at outcome-time, recordCatchUpFailure runs. (The whole processEntry
	// flow is exercised by other tests; here we're pinning the late-tag
	// observation specifically.)
	if p.isCatchUpPath(path) {
		p.recordCatchUpFailure(path)
	}

	if got := p.catchupFailed.Load(); got != 1 {
		t.Errorf("catchupFailed after late-tag failure: got %d, want 1 (defer must see late tag)", got)
	}

	// Cleanup parity: the worker's defer also runs inflightRemove which
	// would clear the tag. Verify that inflightRemove drains both inflight
	// AND catchupPaths so subsequent FullyCaughtUp checks are correct.
	p.inflightRemove(path)
	if p.isCatchUpPath(path) {
		t.Errorf("isCatchUpPath after inflightRemove: got true, want false (tag must clear)")
	}
	if got := p.catchupInflight.Load(); got != 0 {
		t.Errorf("catchupInflight after inflightRemove: got %d, want 0", got)
	}
	// catchupFailed remains 1 — the failure stuck. Self-heal happens via
	// a subsequent successful pull, not via inflightRemove.
	if got := p.catchupFailed.Load(); got != 1 {
		t.Errorf("catchupFailed after inflightRemove: got %d, want 1 (failure must persist)", got)
	}
}

// TestPuller_CatchUpStatus_KeySemantics pins the API contract for
// /api/v1/cluster/status: pre-#392 keys (failed, dropped, skipped_dup,
// pulled) keep their original cumulative whole-puller-lifetime semantics
// so existing dashboards don't silently lose visibility into steady-state
// problems. New catchup_* keys carry the catch-up-batch-scoped values
// FullyCaughtUp consumes.
func TestPuller_CatchUpStatus_KeySemantics(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("ignored")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})

	// Distinguishable values so we can verify which counter populates which key.
	p.catchupFailed.Store(3)
	p.catchupDropped.Store(5)
	p.totalFailed.Store(10)
	p.totalDropped.Store(20)

	status := p.CatchUpStatus()

	// Pre-#392 keys must exist with cumulative semantics intact.
	for _, k := range []string{"started_at", "completed_at", "entries_walked", "enqueued", "skipped_local", "skipped_dup", "pulled", "failed", "dropped"} {
		if _, ok := status[k]; !ok {
			t.Errorf("CatchUpStatus missing pre-#392 key %q", k)
		}
	}
	if got := status["failed"]; got != 10 {
		t.Errorf("status[failed]: got %d, want 10 (cumulative — pre-#392 dashboards rely on this)", got)
	}
	if got := status["dropped"]; got != 20 {
		t.Errorf("status[dropped]: got %d, want 20 (cumulative — pre-#392 dashboards rely on this)", got)
	}

	// New catch-up-scoped keys for the gate.
	if got := status["catchup_failed"]; got != 3 {
		t.Errorf("status[catchup_failed]: got %d, want 3 (catch-up-scoped)", got)
	}
	if got := status["catchup_dropped"]; got != 5 {
		t.Errorf("status[catchup_dropped]: got %d, want 5 (catch-up-scoped)", got)
	}
	if _, ok := status["catchup_inflight"]; !ok {
		t.Errorf("CatchUpStatus missing catchup_inflight key")
	}

	// Live depths.
	if _, ok := status["queue_depth"]; !ok {
		t.Errorf("CatchUpStatus missing queue_depth key")
	}
	if _, ok := status["inflight_count"]; !ok {
		t.Errorf("CatchUpStatus missing inflight_count key")
	}
}
