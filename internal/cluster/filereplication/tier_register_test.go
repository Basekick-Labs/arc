package filereplication

// The puller reports every file it pulled and kept to this node's tier
// metadata. Without that, a replicated file has no tier row until the next
// tier scan, and the query layer routes reads from those rows: a measurement
// with no row loses partition pruning, and one with a cold row but no hot row
// has its local files left out of the read entirely.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

type tierReport struct {
	path string
	size int64
}

// tierReportRecorder is the RecordPulledFile hook as the coordinator supplies
// it: called on a pull worker, so it must be safe to call concurrently.
type tierReportRecorder struct {
	mu      sync.Mutex
	reports []tierReport
}

func (r *tierReportRecorder) record(path string, sizeBytes int64) bool {
	r.mu.Lock()
	r.reports = append(r.reports, tierReport{path: path, size: sizeBytes})
	r.mu.Unlock()
	return true
}

func (r *tierReportRecorder) snapshot() []tierReport {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]tierReport, len(r.reports))
	copy(out, r.reports)
	return out
}

func newTierPuller(t *testing.T, backend *fakeBackend, fetcher Fetcher, resolver PeerResolver, rec *tierReportRecorder, manifestEntry func(string) (raft.FileEntry, bool)) *Puller {
	t.Helper()
	cfg := Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             1,
		QueueSize:           8,
		RetryMaxAttempts:    3,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		ManifestEntry:       manifestEntry,
		Logger:              zerolog.Nop(),
	}
	if rec != nil {
		cfg.RecordPulledFile = rec.record
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New puller: %v", err)
	}
	return p
}

func TestPuller_ReportsPulledFileToTierMetadata(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("parquet body bytes")
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}
	rec := &tierReportRecorder{}

	p := newTierPuller(t, backend, fetcher, resolver, rec, nil)
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/2026/04/11/14/a.parquet", "writer-1", int64(len(body)))
	p.Enqueue(entry)

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 {
		t.Fatalf("pulled = %d, want 1", stats["pulled"])
	}
	if stats["tier_registered"] != 1 {
		t.Fatalf("tier_registered = %d, want 1 — the counter is how an operator sees a wedged recorder", stats["tier_registered"])
	}

	reports := rec.snapshot()
	if len(reports) != 1 {
		t.Fatalf("got %d tier reports, want 1: %+v", len(reports), reports)
	}
	if reports[0].path != entry.Path {
		t.Fatalf("reported path = %q, want %q", reports[0].path, entry.Path)
	}
	// The size the row records has to be the size of the file on this disk.
	// WriteReader was given exactly entry.SizeBytes and the bytes were checked
	// against the manifest checksum, so the entry's count is that size.
	if reports[0].size != int64(len(body)) {
		t.Fatalf("reported size = %d, want %d", reports[0].size, len(body))
	}
}

// A pull whose entry leaves the manifest while the bytes are in flight is
// unlinked again by processEntry. Reporting it would leave a tier row for a
// file that is not on this node's disk — and a hot row with no file is how a
// measurement ends up claiming local data it does not have.
func TestPuller_DoesNotReportAFileUnlinkedMidPull(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("parquet body bytes")
	fetcher := &blockingSuccessFetcher{release: make(chan struct{}), body: body}
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}
	rec := &tierReportRecorder{}

	var present sync.Map
	entry := makeEntry("testdb/cpu/2026/04/11/14/gone.parquet", "writer-1", int64(len(body)))
	present.Store(entry.Path, true)

	p := newTierPuller(t, backend, fetcher, resolver, rec, func(path string) (raft.FileEntry, bool) {
		v, ok := present.Load(path)
		return raft.FileEntry{Path: path}, ok && v.(bool)
	})
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(entry)

	// Let the fetch start, drop the entry from the manifest, then let the
	// bytes land.
	deadline := time.Now().Add(2 * time.Second)
	for fetcher.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if fetcher.calls.Load() == 0 {
		t.Fatal("fetch never started")
	}
	present.Store(entry.Path, false)
	close(fetcher.release)

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["skipped_gone"] == 1 })
	if stats["skipped_gone"] != 1 {
		t.Fatalf("skipped_gone = %d, want 1", stats["skipped_gone"])
	}
	if stats["pulled"] != 0 {
		t.Fatalf("pulled = %d, want 0", stats["pulled"])
	}
	if reports := rec.snapshot(); len(reports) != 0 {
		t.Fatalf("reported a file that was unlinked mid-pull: %+v", reports)
	}
	if stats["tier_registered"] != 0 {
		t.Fatalf("tier_registered = %d, want 0", stats["tier_registered"])
	}
}

// The already-local skip is deliberately not a report: a reconciliation walk
// on a node that holds everything would otherwise queue one event per manifest
// entry on every pass. Files already on disk are the startup tier scan's job.
func TestPuller_DoesNotReportAnAlreadyLocalFile(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("parquet body bytes")
	const path = "testdb/cpu/2026/04/11/14/local.parquet"
	if err := backend.Write(context.Background(), path, body); err != nil {
		t.Fatalf("seed backend: %v", err)
	}
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}
	rec := &tierReportRecorder{}

	p := newTierPuller(t, backend, fetcher, resolver, rec, nil)
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(makeEntry(path, "writer-1", int64(len(body))))

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["skipped_local"] == 1 })
	if stats["skipped_local"] != 1 {
		t.Fatalf("skipped_local = %d, want 1", stats["skipped_local"])
	}
	if reports := rec.snapshot(); len(reports) != 0 {
		t.Fatalf("reported an already-local file: %+v", reports)
	}
}

// A hook that reports "nobody took it" is what a node without tiering looks
// like: the coordinator wires the hook for its whole life, and only the
// recorder behind it is absent. tier_registered must stay at zero there, or it
// is just a second name for pulled.
func TestPuller_HookThatAcceptsNothingDoesNotCount(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("parquet body bytes")
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

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
		RecordPulledFile:    func(string, int64) bool { return false },
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New puller: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(makeEntry("testdb/cpu/2026/04/11/14/a.parquet", "writer-1", int64(len(body))))

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 {
		t.Fatalf("pulled = %d, want 1", stats["pulled"])
	}
	if stats["tier_registered"] != 0 {
		t.Fatalf("tier_registered = %d, want 0 — the hook accepted nothing", stats["tier_registered"])
	}
}

// Nil means the hook itself was never wired, which no shipped configuration
// produces today; kept so the wrapper's nil guard stays honest.
func TestPuller_NilRecorderDoesNotPanic(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("parquet body bytes")
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTierPuller(t, backend, fetcher, resolver, nil, nil)
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(makeEntry("testdb/cpu/2026/04/11/14/a.parquet", "writer-1", int64(len(body))))

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 {
		t.Fatalf("pulled = %d, want 1", stats["pulled"])
	}
	if stats["tier_registered"] != 0 {
		t.Fatalf("tier_registered = %d, want 0 with no hook wired", stats["tier_registered"])
	}
}
