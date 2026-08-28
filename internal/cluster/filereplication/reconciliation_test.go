package filereplication

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func emptyManifest(cursor string, limit int) ([]*raft.FileEntry, string, error) {
	return nil, "", nil
}

// TestPeriodicReconciliationWaitsForStartupAndRepeats verifies that the
// periodic loop cannot race the startup walk and continues to run after the
// startup attempt has completed.
func TestPeriodicReconciliationWaitsForStartupAndRepeats(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("unused")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())

	startupStarted := make(chan struct{})
	startupRelease := make(chan struct{})
	go p.RunCatchUp(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		select {
		case <-startupStarted:
		default:
			close(startupStarted)
		}
		<-startupRelease
		return nil, "", nil
	})

	var periodicCalls atomic.Int64
	p.startPeriodicReconciliation(func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		periodicCalls.Add(1)
		return nil, "", nil
	}, 10*time.Millisecond)

	select {
	case <-startupStarted:
	case <-time.After(time.Second):
		t.Fatal("startup catch-up did not start")
	}
	time.Sleep(40 * time.Millisecond)
	if got := periodicCalls.Load(); got != 0 {
		t.Fatalf("periodic reconciliation ran before startup finished: calls=%d", got)
	}

	close(startupRelease)
	deadline := time.Now().Add(time.Second)
	for periodicCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := periodicCalls.Load(); got < 2 {
		t.Fatalf("periodic reconciliation did not repeat: calls=%d", got)
	}
	p.Stop()
}

// TestRunReconciliationPullsManifestEntryMissedAtStartup verifies the core
// recovery case: a file absent from the startup snapshot is found by a later
// paginated pass and still uses the normal worker path.
func TestRunReconciliationPullsManifestEntryMissedAtStartup(t *testing.T) {
	body := []byte("periodic body")
	backend := newFakeBackend()
	fetcher := newRepeatingFetcher(body)
	p := newTestPuller(t, backend, fetcher,
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), emptyManifest)

	entry := makeEntry("testdb/cpu/missed-after-startup.parquet", "writer-1", int64(len(body)))
	var manifestCalls atomic.Int64
	fetch := func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		manifestCalls.Add(1)
		if cursor != "" {
			return nil, "", nil
		}
		return []*raft.FileEntry{entry}, "", nil
	}

	if !p.RunReconciliation(context.Background(), fetch) {
		t.Fatal("first reconciliation was unexpectedly skipped")
	}
	waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if !p.RunReconciliation(context.Background(), fetch) {
		t.Fatal("second reconciliation was unexpectedly skipped")
	}
	if got := manifestCalls.Load(); got != 2 {
		t.Errorf("manifest fetch calls: got %d, want 2 (one per reconciliation pass)", got)
	}
	if got := p.Stats()["pulled"]; got != 1 {
		t.Errorf("pulled after repeated reconciliation: got %d, want 1", got)
	}
}

// TestPeriodicReconciliationDoesNotReopenStartupReadiness verifies that a
// periodic pull is not added to the startup catch-up scope. A healthy reader
// therefore remains ready while a later drift repair is in flight.
func TestPeriodicReconciliationDoesNotReopenStartupReadiness(t *testing.T) {
	backend := newFakeBackend()
	block := make(chan struct{})
	fetcher := &blockingFetcher{release: block}
	p := newTestPuller(t, backend, fetcher,
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer func() {
		close(block)
		p.Stop()
	}()
	p.RunCatchUp(context.Background(), emptyManifest)

	before := p.Stats()
	entry := makeEntry("testdb/cpu/periodic-readiness.parquet", "writer-1", 100)
	if !p.RunReconciliation(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		return []*raft.FileEntry{entry}, "", nil
	}) {
		t.Fatal("reconciliation was unexpectedly skipped")
	}
	waitStats(t, p, func(s map[string]int64) bool { return s["inflight_count"] == 1 })

	if !p.FullyCaughtUp() {
		t.Fatal("periodic in-flight pull reopened startup readiness")
	}
	after := p.Stats()
	for _, key := range []string{
		"catchup_started_at", "catchup_completed_at", "catchup_entries_walked",
		"catchup_enqueued", "catchup_skipped_local", "catchup_inflight",
		"catchup_failed", "catchup_dropped",
	} {
		if after[key] != before[key] {
			t.Errorf("periodic pass changed startup metric %s: before=%d after=%d", key, before[key], after[key])
		}
	}
}

// TestPeriodicReconciliationSelfHealsCatchUpBookkeeping verifies that a later
// successful periodic pull clears startup failures and drops for the same
// path, without making periodic work part of the startup scope.
func TestPeriodicReconciliationSelfHealsCatchUpBookkeeping(t *testing.T) {
	body := []byte("recovered periodic body")
	p := newTestPuller(t, newFakeBackend(), newRepeatingFetcher(body),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), emptyManifest)

	entry := makeEntry("testdb/cpu/periodic-self-heal.parquet", "writer-1", int64(len(body)))
	p.recordCatchUpFailure(entry.Path)
	p.recordCatchUpDrop(entry.Path)
	if p.FullyCaughtUp() {
		t.Fatal("precondition failed: catch-up bookkeeping should close readiness")
	}

	if !p.RunReconciliation(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		return []*raft.FileEntry{entry}, "", nil
	}) {
		t.Fatal("reconciliation was unexpectedly skipped")
	}
	waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	stats := p.Stats()
	if stats["catchup_failed"] != 0 || stats["catchup_dropped"] != 0 {
		t.Errorf("periodic success did not self-heal startup bookkeeping: failed=%d dropped=%d", stats["catchup_failed"], stats["catchup_dropped"])
	}
	if !p.FullyCaughtUp() {
		t.Error("FullyCaughtUp remained false after successful periodic self-heal")
	}
}

// TestRunReconciliationDeduplicatesReactiveEnqueue verifies that a reactive
// callback already in flight wins the race with a periodic manifest pass.
func TestRunReconciliationDeduplicatesReactiveEnqueue(t *testing.T) {
	block := make(chan struct{})
	fetcher := &blockingFetcher{release: block}
	p := newTestPuller(t, newFakeBackend(), fetcher,
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer func() {
		close(block)
		p.Stop()
	}()
	p.RunCatchUp(context.Background(), emptyManifest)

	entry := makeEntry("testdb/cpu/reactive-periodic-dedup.parquet", "writer-1", 100)
	p.Enqueue(entry)
	waitStats(t, p, func(s map[string]int64) bool { return fetcher.calls.Load() == 1 })
	if !p.RunReconciliation(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		return []*raft.FileEntry{entry}, "", nil
	}) {
		t.Fatal("reconciliation was unexpectedly skipped")
	}
	if got := fetcher.calls.Load(); got != 1 {
		t.Errorf("fetch calls after reactive/periodic race: got %d, want 1", got)
	}
	if got := p.Stats()["skipped_dup"]; got == 0 {
		t.Error("periodic pass did not observe the reactive inflight duplicate")
	}
}

// TestRunReconciliationNoOverlap verifies the explicit walk guard rather than
// relying only on the single goroutine used by the periodic loop.
func TestRunReconciliationNoOverlap(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("unused")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()

	entered := make(chan struct{})
	release := make(chan struct{})
	fetch := func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		close(entered)
		<-release
		return nil, "", nil
	}
	done := make(chan bool)
	go func() { done <- p.RunReconciliation(context.Background(), fetch) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first reconciliation did not start")
	}
	if p.RunReconciliation(context.Background(), emptyManifest) {
		t.Fatal("overlapping reconciliation was allowed")
	}
	close(release)
	if !<-done {
		t.Fatal("first reconciliation was unexpectedly skipped")
	}
}

// TestPeriodicReconciliationStopsOnCancellation verifies that a pass blocked
// in manifest fetch exits when Puller.Stop cancels its lifecycle context.
func TestPeriodicReconciliationStopsOnCancellation(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("unused")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	p.RunCatchUp(context.Background(), emptyManifest)

	started := make(chan struct{})
	var once atomic.Bool
	p.startPeriodicReconciliation(func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		if once.CompareAndSwap(false, true) {
			close(started)
		}
		<-p.ctx.Done()
		return nil, "", p.ctx.Err()
	}, time.Millisecond)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("periodic reconciliation did not start")
	}
	stopDone := make(chan struct{})
	go func() {
		p.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Puller.Stop did not cancel periodic reconciliation")
	}
}
