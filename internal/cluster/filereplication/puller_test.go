package filereplication

import (
	"context"
	"errors"
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

// fakeFetcher implements Fetcher with a scripted behavior.
type fakeFetcher struct {
	mu sync.Mutex
	// Per-call scripted results. index == call number (0-based).
	results []fakeFetchResult
	calls   atomic.Int64
}

type fakeFetchResult struct {
	body []byte
	err  error
}

func newFakeFetcher(results ...fakeFetchResult) *fakeFetcher {
	return &fakeFetcher{results: results}
}

func (f *fakeFetcher) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer) (int64, error) {
	idx := f.calls.Add(1) - 1
	f.mu.Lock()
	defer f.mu.Unlock()
	if int(idx) >= len(f.results) {
		// Default: return error to force retries to stop
		return 0, errors.New("fake fetcher: no more scripted results")
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

// staticResolver maps a single node ID to a fixed address.
type staticResolver struct {
	nodeID string
	addr   string
	ok     bool
}

func (s staticResolver) ResolvePeer(nodeID string) (string, bool) {
	if nodeID != s.nodeID {
		return "", false
	}
	return s.addr, s.ok
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
	resolver := staticResolver{nodeID: "writer-1", addr: "1.2.3.4:9100", ok: true}

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
	resolver := staticResolver{nodeID: "reader-1", addr: "self:9100", ok: true}

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
	resolver := staticResolver{nodeID: "writer-1", addr: "1.2.3.4:9100", ok: true}

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
	resolver := staticResolver{nodeID: "writer-1", addr: "1.2.3.4:9100", ok: true}

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
	resolver := staticResolver{nodeID: "writer-1", addr: "1.2.3.4:9100", ok: true}

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
	resolver := staticResolver{nodeID: "unknown", addr: "", ok: false}

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
	resolver := staticResolver{nodeID: "writer-1", addr: "1.2.3.4:9100", ok: true}

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

	// Enqueue many more than QueueSize+inflight (1).
	// With Workers=1 and QueueSize=2, at most ~3 entries can land before drops.
	const totalEnqueued = 50
	for i := 0; i < totalEnqueued; i++ {
		entry := makeEntry("testdb/cpu/overload.parquet", "writer-1", 100)
		p.Enqueue(entry)
	}

	stats := p.Stats()
	if stats["dropped"] == 0 {
		t.Errorf("expected some dropped entries under overload, got %+v", stats)
	}
	if stats["enqueued"]+stats["dropped"] != int64(totalEnqueued) {
		t.Errorf("enqueued(%d) + dropped(%d) != total(%d)", stats["enqueued"], stats["dropped"], totalEnqueued)
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
	resolver := staticResolver{nodeID: "writer-1", addr: "1.2.3.4:9100", ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	p.Start(context.Background()) // second call should be a no-op
	p.Stop()
	p.Stop() // second stop should be a no-op
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
