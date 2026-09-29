package filereplication

// A node on per-node storage that comes back with an empty data disk
// originated files it no longer holds. With RepullMissingSelfOrigin the
// catch-up and reconciliation walks let the disk decide for a self-origin
// entry — present at the manifest's size: skipped as before; missing or
// short: pulled from a peer's replica like any other entry — while a
// reactive register of a self-origin file (this node just wrote it) is still
// never pulled (#959).

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

const selfNode = "reader-1"

// newRepullPuller is newTestPuller with the self-origin re-pull switched on,
// as the coordinator does for a local backend.
func newRepullPuller(t *testing.T, backend *fakeBackend, fetcher Fetcher, resolver PeerResolver) *Puller {
	t.Helper()
	p, err := New(Config{
		SelfNodeID:              selfNode,
		RepullMissingSelfOrigin: true,
		Backend:                 backend,
		Fetcher:                 fetcher,
		PeerResolver:            resolver,
		Workers:                 1,
		QueueSize:               8,
		RetryMaxAttempts:        3,
		RetryInitialBackoff:     10 * time.Millisecond,
		FetchTimeout:            2 * time.Second,
		Logger:                  zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New puller: %v", err)
	}
	return p
}

// selfResolver serves a pull whose origin is this node from a peer address —
// the shape a self-origin re-pull takes, since the origin is self and the
// bytes are on a peer's replica.
func selfResolver() staticResolver {
	return staticResolver{nodeID: selfNode, addrs: []string{"peer-1:9100"}, ok: true}
}

func TestCatchUpPullsMissingSelfOriginFile(t *testing.T) {
	backend := newFakeBackend() // empty disk
	body := bytes.Repeat([]byte("x"), 100)
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	p := newRepullPuller(t, backend, fetcher, selfResolver())
	p.Start(context.Background())
	defer p.Stop()

	entry := makeEntry("testdb/cpu/2026/04/11/15/mine.parquet", selfNode, 100)
	p.RunCatchUp(context.Background(), sliceFetcher([]*raft.FileEntry{entry}))

	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 || stats["catchup_enqueued"] != 1 || stats["skipped_self"] != 0 {
		t.Fatalf("a missing self-origin file must be catch-up-enqueued and pulled: %+v", stats)
	}
	got, err := backend.Read(context.Background(), entry.Path)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("file not restored from the peer: err=%v len=%d", err, len(got))
	}
	waitStats(t, p, func(s map[string]int64) bool { return s["catchup_inflight"] == 0 })
	if !p.FullyCaughtUp() {
		t.Fatalf("catch-up must converge once the own file is back: %+v", p.CatchUpStatus())
	}
	if fetcher.calls.Load() != 1 {
		t.Errorf("fetcher calls = %d, want 1", fetcher.calls.Load())
	}
}

func TestCatchUpPullsShortSelfOriginFile(t *testing.T) {
	backend := newFakeBackend()
	const path = "testdb/cpu/2026/04/11/15/short.parquet"
	// A staging file shorter than the manifest size: a pull interrupted
	// before the restore. Seeded straight into the fake's map — the public
	// write refuses the reserved staging suffix, as the real backend does —
	// which is what a crash mid-pull leaves behind. Not "present"; fully
	// re-pulled.
	backend.mu.Lock()
	backend.files[path+".part"] = bytes.Repeat([]byte("y"), 40)
	backend.mu.Unlock()
	body := bytes.Repeat([]byte("x"), 100)
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	p := newRepullPuller(t, backend, fetcher, selfResolver())
	p.Start(context.Background())
	defer p.Stop()

	p.RunCatchUp(context.Background(), sliceFetcher([]*raft.FileEntry{makeEntry(path, selfNode, 100)}))
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 {
		t.Fatalf("a short self-origin file must be re-pulled: %+v", stats)
	}
	got, err := backend.Read(context.Background(), path)
	if err != nil || len(got) != 100 {
		t.Fatalf("final file: err=%v len=%d, want 100", err, len(got))
	}
}

func TestCatchUpPullsFullSizeStagingFile(t *testing.T) {
	backend := newFakeBackend()
	const path = "testdb/cpu/2026/04/11/15/staged.parquet"
	// A crash after the last byte reached the staging file but before the
	// rename: the staging file is exactly the manifest size, the final file
	// absent. That is not "present".
	body := bytes.Repeat([]byte("x"), 100)
	backend.mu.Lock()
	backend.files[path+".part"] = body
	backend.mu.Unlock()
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	p := newRepullPuller(t, backend, fetcher, selfResolver())
	p.Start(context.Background())
	defer p.Stop()

	p.RunCatchUp(context.Background(), sliceFetcher([]*raft.FileEntry{makeEntry(path, selfNode, 100)}))
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 || stats["skipped_local"] != 0 {
		t.Fatalf("a full-size staging file must be re-pulled, not skipped: %+v", stats)
	}
	got, err := backend.Read(context.Background(), path)
	if err != nil || len(got) != 100 {
		t.Fatalf("final file: err=%v len=%d, want 100", err, len(got))
	}
	backend.mu.Lock()
	_, stale := backend.files[path+".part"]
	backend.mu.Unlock()
	if stale {
		t.Errorf("staging file still present after the pull")
	}
}

func TestWorkerPullsFullSizeStagingFile(t *testing.T) {
	backend := newFakeBackend()
	const path = "testdb/cpu/2026/04/11/15/peer-staged.parquet"
	body := bytes.Repeat([]byte("x"), 100)
	backend.mu.Lock()
	backend.files[path+".part"] = body
	backend.mu.Unlock()
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	p := newRepullPuller(t, backend, fetcher, staticResolver{nodeID: "peer-node", addrs: []string{"peer-1:9100"}, ok: true})
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(makeEntry(path, "peer-node", 100))
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 || s["skipped_local"] == 1 })
	if stats["pulled"] != 1 || stats["skipped_local"] != 0 {
		t.Fatalf("the worker must pull past a full-size staging file: %+v", stats)
	}
	if got, err := backend.Read(context.Background(), path); err != nil || len(got) != 100 {
		t.Fatalf("final file: err=%v len=%d, want 100", err, len(got))
	}
}

func TestCatchUpSkipsPresentSelfOriginFile(t *testing.T) {
	backend := newFakeBackend()
	const path = "testdb/cpu/2026/04/11/15/present.parquet"
	if err := backend.Write(context.Background(), path, bytes.Repeat([]byte("x"), 100)); err != nil {
		t.Fatal(err)
	}
	fetcher := newFakeFetcher() // any call is a bug
	p := newRepullPuller(t, backend, fetcher, selfResolver())
	p.Start(context.Background())
	defer p.Stop()

	p.RunCatchUp(context.Background(), sliceFetcher([]*raft.FileEntry{makeEntry(path, selfNode, 100)}))
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["skipped_self"] == 1 })
	if stats["skipped_self"] != 1 || stats["catchup_skipped_local"] != 1 || stats["catchup_enqueued"] != 0 {
		t.Fatalf("a present self-origin file must be skipped without a tag: %+v", stats)
	}
	if fetcher.calls.Load() != 0 {
		t.Errorf("fetcher was called for a present self-origin file")
	}
	if !p.FullyCaughtUp() {
		t.Errorf("nothing was pending; catch-up must be converged: %+v", p.CatchUpStatus())
	}
}

func TestReconciliationWalkPullsMissingSelfOriginFile(t *testing.T) {
	backend := newFakeBackend()
	body := bytes.Repeat([]byte("x"), 100)
	fetcher := newFakeFetcher(fakeFetchResult{body: body})
	p := newRepullPuller(t, backend, fetcher, selfResolver())
	p.Start(context.Background())
	defer p.Stop()
	p.RunCatchUp(context.Background(), sliceFetcher(nil)) // startup done, empty manifest

	entry := makeEntry("testdb/cpu/2026/04/11/15/mine.parquet", selfNode, 100)
	if !p.RunReconciliation(context.Background(), sliceFetcher([]*raft.FileEntry{entry})) {
		t.Fatal("RunReconciliation refused to run")
	}
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["pulled"] == 1 })
	if stats["pulled"] != 1 || stats["replication_recheck_enqueued"] != 1 {
		t.Fatalf("the periodic walk must re-pull a missing self-origin file: %+v", stats)
	}
	if got, err := backend.Read(context.Background(), entry.Path); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("file not restored: err=%v len=%d", err, len(got))
	}
	// Now present: the next walk skips it.
	if !p.RunReconciliation(context.Background(), sliceFetcher([]*raft.FileEntry{entry})) {
		t.Fatal("second RunReconciliation refused to run")
	}
	stats = waitStats(t, p, func(s map[string]int64) bool { return s["replication_recheck_skipped"] == 1 })
	if stats["replication_recheck_skipped"] != 1 || stats["pulled"] != 1 {
		t.Fatalf("a present self-origin file must be skipped by the periodic walk: %+v", stats)
	}
}

// The reactive path keeps its exemption with the re-pull on: a self-origin
// register means this node just wrote the file, and the FSM callback that
// delivers it must do no I/O. Only the walks consult the disk.
func TestPullerSkipsSelfOriginReactiveWithRepull(t *testing.T) {
	backend := newFakeBackend() // the file is missing; the reactive path must not care
	fetcher := newFakeFetcher()
	p := newRepullPuller(t, backend, fetcher, selfResolver())
	p.Start(context.Background())
	defer p.Stop()

	p.Enqueue(makeEntry("testdb/cpu/2026/04/11/15/just-written.parquet", selfNode, 100))
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["skipped_self"] == 1 })
	if stats["skipped_self"] != 1 || stats["enqueued"] != 0 || fetcher.calls.Load() != 0 {
		t.Fatalf("a reactive self-origin register must be skipped with no pull even with the re-pull on: %+v calls=%d", stats, fetcher.calls.Load())
	}
}

// With the re-pull off (shared backends), the walks keep the old assumption:
// a self-origin entry is never pulled, present or not.
func TestCatchUpKeepsSelfOriginFastPathWithoutRepull(t *testing.T) {
	backend := newFakeBackend() // the file is missing, and it does not matter
	fetcher := newFakeFetcher()
	p := newTestPuller(t, backend, fetcher, selfResolver())
	p.Start(context.Background())
	defer p.Stop()

	p.RunCatchUp(context.Background(), sliceFetcher([]*raft.FileEntry{makeEntry("testdb/cpu/2026/04/11/15/mine.parquet", selfNode, 100)}))
	stats := waitStats(t, p, func(s map[string]int64) bool { return s["skipped_self"] == 1 })
	if stats["skipped_self"] != 1 || stats["catchup_skipped_local"] != 1 || stats["catchup_enqueued"] != 0 || fetcher.calls.Load() != 0 {
		t.Fatalf("with the re-pull off a self-origin entry must be skipped as before: %+v calls=%d", stats, fetcher.calls.Load())
	}
}
