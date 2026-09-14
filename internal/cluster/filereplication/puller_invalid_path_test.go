package filereplication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
)

// invalidEntryPath is accepted by raft.ValidateManifestPath and refused by
// storage.ValidateKey, which is how it reaches the puller from the cluster
// manifest in the first place. The full reachable set is pinned in
// internal/cluster/raft/manifest_path_reachability_test.go; backslash is used
// here because it is the one spelling that also survives the coordinator's
// sanitizeFetchPath, so the same key exercises every site in #747.
const invalidEntryPath = `testdb\cpu/2026/04/11/14/file-1.parquet`

// TestPullerQuarantinesUnusableKeyWithoutTouchingPeers is the core of the
// replication half of #747.
//
// Before this, the pre-pull StatFile failed, the worker went to a peer anyway,
// streamed the whole file body, and only then failed at WriteReader with the
// same permanent error. That repeated for every candidate peer, every attempt,
// on every catch-up walk and every reconciliation pass, forever.
//
// The load-bearing assertion is fetcher.calls == 0. Everything else could be
// satisfied by a fix that still pays for the transfer.
func TestPullerQuarantinesUnusableKeyWithoutTouchingPeers(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher() // no scripted results: any call is a bug
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	before := metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64)
	p.Enqueue(makeEntry(invalidEntryPath, "writer-1", 128))

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["invalid_path"] == 1 })
	if stats["invalid_path"] != 1 {
		t.Fatalf("invalid_path = %d, want 1: %+v", stats["invalid_path"], stats)
	}
	// The puller has its own counter, but it must also move the shared one:
	// Stats() is JSON-only, so without this the highest-frequency site is
	// invisible to the Prometheus alert this change ships.
	if got := metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64) - before; got != 1 {
		t.Errorf("shared quarantine counter moved by %d, want 1", got)
	}
	if got := fetcher.calls.Load(); got != 0 {
		t.Errorf("fetcher was called %d times; a key no backend can accept must never cost peer bandwidth", got)
	}
	if stats["pulled"] != 0 {
		t.Errorf("pulled = %d, want 0", stats["pulled"])
	}

	// deleteCount pins the claim that tryResumeFromPartial and deleteFile
	// become unreachable for a quarantined entry, rather than only documenting
	// it in a comment.
	if got := backend.deleteCount(); got != 0 {
		t.Errorf("backend saw %d Delete calls; the quarantine must return before any partial-file handling", got)
	}
}

// TestPullerQuarantineKeepsTheQueryGateClosed is the decision this fix turns
// on, pinned so it cannot be "simplified" later.
//
// The entry is unsatisfiable, so it is tempting to stop counting it as a
// failure and let the reader go ready. That would be wrong. The read path is a
// glob, so an absent file contributes no rows and raises nothing, and an
// operator who set query.gate_on_catchup (off by default) asked for a 503 over
// exactly that silence. An entry no peer holds is equally unsatisfiable and
// reds the gate today; this one gets no exemption. What the quarantine removes
// is the wasted work, not the signal.
func TestPullerQuarantineKeepsTheQueryGateClosed(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher()
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	// Tag the path as catch-up work and mark the walker finished, which is the
	// state FullyCaughtUp is written against.
	p.markCatchUp(invalidEntryPath)
	p.enqueue(makeEntry(invalidEntryPath, "writer-1", 128), enqueueSourceCatchUp)
	p.catchupCompletedAt.Store(time.Now().Unix())

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["invalid_path"] == 1 })
	if stats["failed"] != 1 {
		t.Errorf("failed = %d, want 1: the file really is absent, so the gate must stay closed", stats["failed"])
	}

	// The catch-up tag must still be released, or catchupInflight never drains
	// and the reason the gate is red becomes unreadable.
	if got := p.catchupInflight.Load(); got != 0 {
		t.Errorf("catchupInflight = %d, want 0: the quarantine must still clear its in-flight bookkeeping", got)
	}
	if p.FullyCaughtUp() {
		t.Error("FullyCaughtUp() is true while a manifest entry has no local copy; queries would silently return fewer rows")
	}

	// The operator reading the 503 body must be able to see why it will never
	// clear on its own.
	if got := p.CatchUpStatus()["invalid_path"]; got != 1 {
		t.Errorf("CatchUpStatus()[invalid_path] = %d, want 1", got)
	}
}

// TestPullerQuarantineDoesNotPoisonOtherEntries pins that one bad entry among
// good ones costs only itself. The bad entry is at index 1 of 3 because the
// worker pool makes a quarantine that aborted its loop order-sensitive.
func TestPullerQuarantineDoesNotPoisonOtherEntries(t *testing.T) {
	backend := newFakeBackend()
	body := []byte("parquet body bytes")
	fetcher := newRepeatingFetcher(body)
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	good1 := makeEntry("testdb/cpu/2026/04/11/14/a.parquet", "writer-1", int64(len(body)))
	bad := makeEntry(invalidEntryPath, "writer-1", int64(len(body)))
	good2 := makeEntry("testdb/cpu/2026/04/11/14/c.parquet", "writer-1", int64(len(body)))
	for _, e := range []*raft.FileEntry{good1, bad, good2} {
		p.Enqueue(e)
	}

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["pulled"] == 2 && s["invalid_path"] == 1
	})
	if stats["pulled"] != 2 {
		t.Fatalf("pulled = %d, want 2: %+v", stats["pulled"], stats)
	}
	// waitStats returns the last snapshot on timeout without failing, so the
	// quarantine half of the predicate has to be re-asserted here or this test
	// passes on a build where the quarantine never fires.
	if stats["invalid_path"] != 1 {
		t.Fatalf("invalid_path = %d, want 1: %+v", stats["invalid_path"], stats)
	}
	for _, e := range []*raft.FileEntry{good1, good2} {
		if _, err := backend.Read(context.Background(), e.Path); err != nil {
			t.Errorf("Read(%s) after pull: %v", e.Path, err)
		}
	}
}

// TestPullerQuarantineIsIdempotentAcrossReenqueues covers the shape that makes
// the log-dedupe set necessary: the same path is offered by the reactive FSM
// callback, the startup catch-up walker and the periodic reconciler, none of
// which can know another already reported it.
//
// The counter advances once per arrival, which is the honest measure of wasted
// work; only the Error line is deduplicated. What must not happen is in-flight
// state leaking, since that would pin the gate red for a reason unrelated to
// the entry itself.
func TestPullerQuarantineIsIdempotentAcrossReenqueues(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newFakeFetcher()
	resolver := staticResolver{nodeID: "writer-1", addrs: []string{"1.2.3.4:9100"}, ok: true}

	p := newTestPuller(t, backend, fetcher, resolver)
	p.Start(context.Background())
	defer p.Stop()

	const rounds = 50
	for i := 0; i < rounds; i++ {
		p.Enqueue(makeEntry(invalidEntryPath, "writer-1", 128))
		// Wait for this arrival to settle before the next, so the inflight
		// dedup does not silently collapse them into one.
		waitStats(t, p, func(s map[string]int64) bool { return s["invalid_path"] >= int64(i+1) })
	}

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["invalid_path"] == rounds })
	if stats["invalid_path"] != rounds {
		t.Fatalf("invalid_path = %d, want %d", stats["invalid_path"], rounds)
	}
	if got := stats["inflight_count"]; got != 0 {
		t.Errorf("inflight_count = %d, want 0: quarantined entries must not leak in-flight slots", got)
	}
	if got := fetcher.calls.Load(); got != 0 {
		t.Errorf("fetcher was called %d times across %d re-enqueues", got, rounds)
	}

	// The dedupe set holds one path, not one entry per arrival.
	p.inflightMu.Lock()
	tracked := len(p.quarantinedPaths)
	p.inflightMu.Unlock()
	if tracked != 1 {
		t.Errorf("quarantinedPaths holds %d entries after %d arrivals of one path, want 1", tracked, rounds)
	}
}

// TestQuarantineLogSetIsBounded exercises the branch nobody runs in practice:
// past maxQuarantineLogPaths the set stops growing and every arrival logs
// again. Repeating a log line is recoverable; an unbounded map fed by manifest
// entries is not.
func TestQuarantineLogSetIsBounded(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{ok: false})

	for i := 0; i < maxQuarantineLogPaths+10; i++ {
		p.markQuarantinedForLog(pathN(i))
	}
	p.inflightMu.Lock()
	size := len(p.quarantinedPaths)
	p.inflightMu.Unlock()
	if size != maxQuarantineLogPaths {
		t.Fatalf("quarantinedPaths grew to %d, want it capped at %d", size, maxQuarantineLogPaths)
	}
	// Past the cap, a path that was never recorded still reports "log it",
	// rather than being silently suppressed.
	if !p.markQuarantinedForLog("testdb/cpu/2026/04/11/14/past-cap.parquet") {
		t.Error("past the cap the helper suppressed a log line for an unseen path")
	}
	// A path recorded before the cap is still deduplicated.
	if p.markQuarantinedForLog(pathN(0)) {
		t.Error("a path already recorded was reported as new")
	}
}

func pathN(i int) string {
	return fmt.Sprintf("testdb/cpu/2026/04/11/14/file-%d.parquet", i)
}
