package filereplication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// TestRunCatchUpEnqueuesAllWhenQueueLarge verifies the happy path: a walker
// fed 10 distinct entries enqueues all of them when the queue is big enough
// to absorb everything without backpressure.
func TestRunCatchUpEnqueuesAllWhenQueueLarge(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("catchup body")
	// Repeating fetcher yields the body on every call — the default
	// non-repeating fakeFetcher would error on calls 2..N because the
	// script is exhausted.
	fetcher := newRepeatingFetcher(body)
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             2,
		QueueSize:           64,
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

	entries := make([]*raft.FileEntry, 10)
	for i := range entries {
		entries[i] = makeEntry(fmt.Sprintf("testdb/cpu/catchup-%02d.parquet", i), "writer-1", int64(len(body)))
	}

	p.RunCatchUp(context.Background(), entries)

	// Wait for workers to drain.
	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 10
	})
	if stats["catchup_entries_walked"] != 10 {
		t.Errorf("catchup_entries_walked: got %d, want 10", stats["catchup_entries_walked"])
	}
	if stats["catchup_enqueued"] != 10 {
		t.Errorf("catchup_enqueued: got %d, want 10", stats["catchup_enqueued"])
	}
	if stats["catchup_completed_at"] == 0 {
		t.Errorf("catchup_completed_at should be non-zero")
	}
	if stats["pulled"] != 10 {
		t.Errorf("pulled: got %d, want 10", stats["pulled"])
	}
	if stats["dropped"] != 0 {
		t.Errorf("dropped: got %d, want 0", stats["dropped"])
	}
}

// TestRunCatchUpThrottlesAtHighWater verifies the walker pauses enqueueing
// when the queue is above the high-water mark. We use a blocking fetcher to
// hold the single worker busy so the queue fills up, then confirm the
// walker's sleeps happen (detectable via wall-clock elapsed time).
func TestRunCatchUpThrottlesAtHighWater(t *testing.T) {
	backend := newFakeBackend()
	blockCh := make(chan struct{})
	fetcher := &blockingFetcher{release: blockCh}
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p, err := New(Config{
		SelfNodeID:            "reader-1",
		Backend:               backend,
		Fetcher:               fetcher,
		PeerResolver:          resolver,
		Workers:               1,
		QueueSize:             4,
		CatchUpQueueHighWater: 0.5, // pause when > 2 entries in queue
		RetryMaxAttempts:      1,
		RetryInitialBackoff:   10 * time.Millisecond,
		FetchTimeout:          5 * time.Second,
		Logger:                zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer func() {
		close(blockCh)
		p.Stop()
	}()

	entries := make([]*raft.FileEntry, 5)
	for i := range entries {
		entries[i] = makeEntry(fmt.Sprintf("testdb/cpu/throttle-%02d.parquet", i), "writer-1", 100)
	}

	// Run catch-up in a goroutine so we can interrupt it if it doesn't
	// throttle properly (infinite loop protection).
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		p.RunCatchUp(context.Background(), entries)
	}()

	// Let the walker try to enqueue for a bit. With Workers=1 blocked, the
	// queue fills to QueueSize=4 (cap) within microseconds, then the walker
	// hits the high-water mark and starts sleeping in 50ms ticks. Release
	// the fetcher after 150ms so the walker can drain.
	time.Sleep(150 * time.Millisecond)
	close(blockCh)
	blockCh = make(chan struct{}) // reset for defer close (which is now a no-op)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunCatchUp did not return within 3s")
	}
	elapsed := time.Since(start)
	// Without throttling, all 5 enqueues complete in microseconds. With
	// throttling (QueueSize=4, high-water=2), the walker should sleep at
	// least once (50ms) while waiting for drain.
	if elapsed < 50*time.Millisecond {
		t.Errorf("RunCatchUp completed in %v, expected >= 50ms indicating throttling", elapsed)
	}
}

// TestRunCatchUpHonorsContextCancel verifies the walker exits promptly when
// its ctx is cancelled mid-walk.
func TestRunCatchUpHonorsContextCancel(t *testing.T) {
	backend := newFakeBackend()
	blockCh := make(chan struct{})
	fetcher := &blockingFetcher{release: blockCh}
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p, err := New(Config{
		SelfNodeID:            "reader-1",
		Backend:               backend,
		Fetcher:               fetcher,
		PeerResolver:          resolver,
		Workers:               1,
		QueueSize:             2,
		CatchUpQueueHighWater: 0.5,
		RetryMaxAttempts:      1,
		RetryInitialBackoff:   10 * time.Millisecond,
		FetchTimeout:          5 * time.Second,
		Logger:                zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer func() {
		close(blockCh)
		p.Stop()
	}()

	entries := make([]*raft.FileEntry, 100) // huge batch
	for i := range entries {
		entries[i] = makeEntry(fmt.Sprintf("testdb/cpu/cancel-%04d.parquet", i), "writer-1", 100)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.RunCatchUp(ctx, entries)
	}()

	// Give the walker time to start throttling on the full queue, then cancel.
	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RunCatchUp did not return within 500ms of ctx cancel")
	}
	// Walker should not have processed all 100 entries.
	stats := p.Stats()
	if stats["catchup_entries_walked"] >= 100 {
		t.Errorf("walker processed all entries despite cancel: walked=%d", stats["catchup_entries_walked"])
	}
}

// TestRunCatchUpOnceOnly verifies the single-shot guard: a second call to
// RunCatchUp on the same puller is a no-op.
func TestRunCatchUpOnceOnly(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("one-shot")
	fetcher := newRepeatingFetcher(body)
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        resolver,
		Workers:             2,
		QueueSize:           16,
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

	entries1 := []*raft.FileEntry{
		makeEntry("testdb/cpu/once-a.parquet", "writer-1", int64(len(body))),
		makeEntry("testdb/cpu/once-b.parquet", "writer-1", int64(len(body))),
	}
	entries2 := []*raft.FileEntry{
		makeEntry("testdb/cpu/once-c.parquet", "writer-1", int64(len(body))),
		makeEntry("testdb/cpu/once-d.parquet", "writer-1", int64(len(body))),
	}

	p.RunCatchUp(context.Background(), entries1)
	firstWalked := p.Stats()["catchup_entries_walked"]
	firstStartedAt := p.Stats()["catchup_started_at"]

	// Second call should be a no-op — no new entries walked, started_at
	// unchanged.
	p.RunCatchUp(context.Background(), entries2)
	secondWalked := p.Stats()["catchup_entries_walked"]
	secondStartedAt := p.Stats()["catchup_started_at"]

	if firstWalked != secondWalked {
		t.Errorf("second RunCatchUp walked more entries: first=%d second=%d", firstWalked, secondWalked)
	}
	if firstStartedAt != secondStartedAt {
		t.Errorf("second RunCatchUp changed started_at: first=%d second=%d", firstStartedAt, secondStartedAt)
	}
}

// TestRunCatchUpDedupsWithReactiveEnqueue verifies the Phase 3 dedup
// contract: if a reactive FSM callback enqueues path X while the walker is
// also processing X, the fetch handler is called exactly once. This is the
// test that justifies the inflight set.
func TestRunCatchUpDedupsWithReactiveEnqueue(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("dedup body")
	// Repeating fetcher so we can assert exact call counts via .calls.
	fetcher := newRepeatingFetcher(body)
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
		FetchTimeout:        2 * time.Second,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/dedup-catchup.parquet", "writer-1", int64(len(body)))

	// Fire catch-up and reactive enqueue in parallel, both for the same path.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.RunCatchUp(context.Background(), []*raft.FileEntry{entry})
	}()
	go func() {
		defer wg.Done()
		// Simulate many reactive callbacks firing concurrently.
		for i := 0; i < 20; i++ {
			p.Enqueue(entry)
		}
	}()
	wg.Wait()

	// Wait for the worker to drain.
	waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 1
	})

	// Exactly one fetch should have been issued to the peer.
	if got := fetcher.calls.Load(); got != 1 {
		t.Errorf("Fetcher.Fetch called %d times, want 1 (catch-up and reactive should dedup)", got)
	}
	stats := p.Stats()
	if stats["pulled"] != 1 {
		t.Errorf("pulled: got %d, want 1", stats["pulled"])
	}
	if stats["skipped_dup"] == 0 {
		t.Errorf("skipped_dup should be non-zero (parallel enqueues should dedup)")
	}
}
