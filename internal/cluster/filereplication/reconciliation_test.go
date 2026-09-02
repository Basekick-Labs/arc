package filereplication

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
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

// TestPeriodicReconciliationResetsIntervalAfterSlowPass verifies that the
// next pass is scheduled after a slow pass completes, not from the original
// tick and not from a buffered ticker event.
func TestPeriodicReconciliationResetsIntervalAfterSlowPass(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("unused")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), emptyManifest)

	const interval = 20 * time.Millisecond
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }) }()
	var calls atomic.Int64
	p.startPeriodicReconciliation(func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		switch calls.Add(1) {
		case 1:
			close(firstStarted)
			<-release
		case 2:
			close(secondStarted)
		}
		return nil, "", nil
	}, interval)

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first periodic reconciliation did not start")
	}
	// Keep the first pass running well beyond its interval. A ticker-based
	// loop would have a follow-up tick waiting by the time this is released.
	time.Sleep(5 * interval)
	releasedAt := time.Now()
	releaseOnce.Do(func() { close(release) })

	select {
	case <-secondStarted:
		t.Fatalf("second reconciliation started immediately after slow pass: elapsed=%v", time.Since(releasedAt))
	case <-time.After(interval / 2):
	}
	select {
	case <-secondStarted:
	case <-time.After(2 * interval):
		t.Fatal("second periodic reconciliation did not start after reset interval")
	}
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

// TestRunReconciliationContinuesAfterEmptyPage verifies that a page with no
// entries does not terminate the walk when the manifest supplies another
// cursor. This can happen when entries disappear between page snapshots.
func TestRunReconciliationContinuesAfterEmptyPage(t *testing.T) {
	body := []byte("empty page continuation")
	p := newTestPuller(t, newFakeBackend(), newRepeatingFetcher(body),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), emptyManifest)

	entry := makeEntry("testdb/cpu/after-empty-page.parquet", "writer-1", int64(len(body)))
	var calls atomic.Int64
	if !p.RunReconciliation(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		switch calls.Add(1) {
		case 1:
			return nil, "after-empty-page", nil
		case 2:
			if cursor != "after-empty-page" {
				t.Errorf("second page cursor: got %q, want %q", cursor, "after-empty-page")
			}
			return []*raft.FileEntry{entry}, "", nil
		default:
			return nil, "", nil
		}
	}) {
		t.Fatal("reconciliation was unexpectedly skipped")
	}

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if got := calls.Load(); got != 2 {
		t.Errorf("manifest fetch calls: got %d, want 2", got)
	}
	if stats["replication_recheck_entries_walked"] != 1 {
		t.Errorf("recheck entries walked: got %d, want 1", stats["replication_recheck_entries_walked"])
	}
	if stats["replication_recheck_enqueued"] != 1 {
		t.Errorf("recheck enqueued: got %d, want 1", stats["replication_recheck_enqueued"])
	}
}

// TestRunReconciliationAbortsOnCursorStall verifies that a non-empty page
// cannot make the walker spin forever by returning the same continuation
// cursor repeatedly.
func TestRunReconciliationAbortsOnCursorStall(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(),
		newFakeFetcher(fakeFetchResult{body: []byte("unused")}),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), emptyManifest)

	entry := makeEntry("testdb/cpu/stalled-cursor.parquet", "writer-1", 1)
	var calls atomic.Int64
	if !p.RunReconciliation(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		switch calls.Add(1) {
		case 1:
			return []*raft.FileEntry{entry}, "stalled-cursor", nil
		case 2:
			if cursor != "stalled-cursor" {
				t.Errorf("stalled page cursor: got %q, want %q", cursor, "stalled-cursor")
			}
			return []*raft.FileEntry{entry}, "stalled-cursor", nil
		default:
			return nil, "", nil
		}
	}) {
		t.Fatal("reconciliation was unexpectedly skipped")
	}

	stats := p.Stats()
	if calls.Load() != 2 {
		t.Errorf("manifest fetch calls: got %d, want 2", calls.Load())
	}
	if stats["replication_recheck_aborted"] != 1 {
		t.Errorf("aborted rechecks: got %d, want 1", stats["replication_recheck_aborted"])
	}
	if stats["replication_recheck_completed"] != 0 {
		t.Errorf("completed rechecks: got %d, want 0", stats["replication_recheck_completed"])
	}
}

// TestPeriodicReconciliationSchedulerRestoresDeletedManifestFile exercises
// the actual periodic scheduler rather than calling RunReconciliation
// directly. A local file is deleted without an FSM callback, then the next
// scheduled manifest pass must restore it through the normal pull path.
func TestPeriodicReconciliationSchedulerRestoresDeletedManifestFile(t *testing.T) {
	body := []byte("scheduled periodic body")
	backend := newFakeBackend()
	p, err := New(Config{
		SelfNodeID:             "reader-1",
		Backend:                backend,
		Fetcher:                newRepeatingFetcher(body),
		PeerResolver:           staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true},
		Workers:                1,
		QueueSize:              8,
		RetryMaxAttempts:       1,
		RetryInitialBackoff:    time.Millisecond,
		FetchTimeout:           2 * time.Second,
		ReconciliationInterval: 10 * time.Millisecond,
		Logger:                 zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/deleted-without-callback.parquet", "writer-1", int64(len(body)))
	manifest := func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		if cursor == "" {
			return []*raft.FileEntry{entry}, "", nil
		}
		return nil, "", nil
	}
	p.RunCatchUp(context.Background(), manifest)
	waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })

	if err := backend.Delete(context.Background(), entry.Path); err != nil {
		t.Fatalf("delete local file: %v", err)
	}
	before := p.Stats()
	p.StartPeriodicReconciliation(manifest)
	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["replication_recheck_completed"] >= 1 && s["pulled"] == 2
	})

	if _, err := backend.Read(context.Background(), entry.Path); err != nil {
		t.Fatalf("periodic reconciliation did not restore file: %v", err)
	}
	if stats["replication_recheck_enqueued"] < 1 {
		t.Errorf("recheck enqueued: got %d, want at least 1", stats["replication_recheck_enqueued"])
	}
	for _, key := range []string{
		"catchup_started_at", "catchup_completed_at", "catchup_entries_walked",
		"catchup_enqueued", "catchup_skipped_local", "catchup_inflight",
		"catchup_failed", "catchup_dropped",
	} {
		if stats[key] != before[key] {
			t.Errorf("scheduled periodic pass changed startup metric %s: before=%d after=%d", key, before[key], stats[key])
		}
	}
}

// TestRunReconciliationGatesAndAbortsOnEligibilityChange verifies that a
// gated tick does not fetch and that a pass already in progress aborts when
// the node becomes ineligible before the next page.
func TestRunReconciliationGatesAndAbortsOnEligibilityChange(t *testing.T) {
	var eligible atomic.Bool
	eligible.Store(true)
	p := newTestPuller(t, newFakeBackend(), newRepeatingFetcher([]byte("gated body")),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.cfg.ReconciliationGate = eligible.Load
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), emptyManifest)

	eligible.Store(false)
	var calls atomic.Int64
	if !p.RunReconciliation(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		calls.Add(1)
		return nil, "", nil
	}) {
		t.Fatal("gated reconciliation was unexpectedly skipped")
	}
	stats := p.Stats()
	if calls.Load() != 0 {
		t.Errorf("gated reconciliation fetched manifest %d times, want 0", calls.Load())
	}
	if stats["replication_recheck_gated"] != 1 {
		t.Errorf("gated rechecks: got %d, want 1", stats["replication_recheck_gated"])
	}
	if stats["replication_recheck_completed"] != 0 {
		t.Errorf("gated rechecks completed: got %d, want 0", stats["replication_recheck_completed"])
	}

	eligible.Store(true)
	entry := makeEntry("testdb/cpu/gate-changed-mid-pass.parquet", "writer-1", int64(len("gated body")))
	if !p.RunReconciliation(context.Background(), func(cursor string, limit int) ([]*raft.FileEntry, string, error) {
		calls.Add(1)
		eligible.Store(false)
		return []*raft.FileEntry{entry}, "next-page", nil
	}) {
		t.Fatal("eligible reconciliation was unexpectedly skipped")
	}
	stats = p.Stats()
	if stats["replication_recheck_aborted"] != 1 {
		t.Errorf("aborted rechecks: got %d, want 1", stats["replication_recheck_aborted"])
	}
	if stats["replication_recheck_gated"] != 2 {
		t.Errorf("gated rechecks after mid-pass change: got %d, want 2", stats["replication_recheck_gated"])
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

// TestPeriodicReconciliationDoesNotModifyCatchUpBookkeeping verifies that a
// later periodic pull does not clear or reopen startup readiness state for the
// same path. Startup bookkeeping is healed only by startup/reactive work.
func TestPeriodicReconciliationDoesNotModifyCatchUpBookkeeping(t *testing.T) {
	body := []byte("recovered periodic body")
	p := newTestPuller(t, newFakeBackend(), newRepeatingFetcher(body),
		staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), emptyManifest)

	entry := makeEntry("testdb/cpu/periodic-self-heal.parquet", "writer-1", int64(len(body)))
	p.recordCatchUpFailure(entry.Path)
	p.recordCatchUpDrop(entry.Path)
	p.markCatchUp(entry.Path)
	before := p.Stats()
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
	for _, key := range []string{"catchup_failed", "catchup_dropped", "catchup_inflight", "catchup_completed_at"} {
		if stats[key] != before[key] {
			t.Errorf("periodic success changed startup bookkeeping %s: before=%d after=%d", key, before[key], stats[key])
		}
	}
	if p.FullyCaughtUp() {
		t.Error("periodic success unexpectedly reopened startup readiness")
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
