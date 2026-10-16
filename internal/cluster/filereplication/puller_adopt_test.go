package filereplication

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1207 d2a2a: a committed compaction output held staged is moved into place
// instead of fetched.

func TestPullerAdoptsAStagedCopyBeforeFetching(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newRepeatingFetcher([]byte("abcd"))
	p := newTestPuller(t, backend, fetcher, staticResolver{nodeID: "writer-1", addrs: []string{"w1:9100"}, ok: true})
	var adopts atomic.Int64
	p.cfg.AdoptStaged = func(_ context.Context, e *raft.FileEntry) (bool, error) {
		adopts.Add(1)
		if e.CompactionJob != "j1" {
			return false, nil
		}
		_ = backend.Write(context.Background(), e.Path, []byte("abcd"))
		return true, nil
	}
	p.Start(context.Background())
	defer p.Stop()

	out := makeEntry("db/cpu/2026/10/13/10/o.parquet", "writer-1", 4)
	out.CompactionJob = "j1"
	p.Enqueue(out)
	waitStats(t, p, func(s map[string]int64) bool { return adopts.Load() == 1 })
	if adopts.Load() != 1 || fetcher.calls.Load() != 0 {
		t.Fatalf("fetched %d times an output it held staged", fetcher.calls.Load())
	}
	// No staged copy: fetched as any file.
	other := makeEntry("db/cpu/2026/10/13/10/o2.parquet", "writer-1", 4)
	other.CompactionJob = "j2"
	p.Enqueue(other)
	waitStats(t, p, func(map[string]int64) bool { return fetcher.calls.Load() == 1 })
	if fetcher.calls.Load() != 1 {
		t.Fatal("an output with no staged copy was not fetched")
	}
	// A plain file never asks.
	before := adopts.Load()
	p.Enqueue(makeEntry("db/cpu/2026/10/13/10/plain.parquet", "writer-1", 4))
	waitStats(t, p, func(map[string]int64) bool { return fetcher.calls.Load() == 2 })
	if fetcher.calls.Load() != 2 || adopts.Load() != before {
		t.Fatal("asked to adopt a file no compaction job registered")
	}
}

// AdoptNow holds the path's slot; an output that left the manifest while it
// moved is removed again.
func TestPullerAdoptNow(t *testing.T) {
	backend := newFakeBackend()
	p := newTestPuller(t, backend, newFakeFetcher(), staticResolver{})
	manifest := newFakeManifest("db/o.parquet")
	p.cfg.ManifestEntry = manifest.entry
	p.Start(context.Background())
	defer p.Stop()
	var inside bool
	p.cfg.AdoptStaged = func(_ context.Context, e *raft.FileEntry) (bool, error) {
		inside = !p.inflightAdd(e.Path) // the slot is ours while we adopt
		_ = backend.Write(context.Background(), e.Path, []byte("x"))
		return true, nil
	}
	e := &raft.FileEntry{Path: "db/o.parquet", CompactionJob: "j1", SizeBytes: 1}
	if !p.AdoptNow(e) || !inside {
		t.Fatalf("adopt now: inside=%v", inside)
	}
	if !p.inflightAdd(e.Path) {
		t.Fatal("slot kept after the adoption")
	}
	p.inflightRemove(e.Path)
	// Taken slot: not adopted here (the request holding it adopts).
	p.inflightAdd(e.Path)
	if p.AdoptNow(e) {
		t.Fatal("adopted beside a pull of the same path")
	}
	p.inflightRemove(e.Path)
	manifest.remove("db/o.parquet")
	if p.AdoptNow(e) {
		t.Fatal("an output that left the manifest counts as adopted")
	}
	if ok, _ := backend.Exists(context.Background(), "db/o.parquet"); ok {
		t.Fatal("an output that left the manifest was left in place")
	}
}

// A failed adoption hands the path back to a normal pull: holding the slot
// dropped the registration's own pull as a duplicate.
func TestPullerAdoptNowFallsBackToAPull(t *testing.T) {
	backend := newFakeBackend()
	fetcher := newRepeatingFetcher([]byte("abcd"))
	p := newTestPuller(t, backend, fetcher, staticResolver{nodeID: "writer-1", addrs: []string{"w1:9100"}, ok: true})
	p.cfg.AdoptStaged = func(context.Context, *raft.FileEntry) (bool, error) { return false, nil }
	p.Start(context.Background())
	defer p.Stop()
	out := makeEntry("db/cpu/2026/10/13/10/o.parquet", "writer-1", 4)
	out.CompactionJob = "j1"
	if p.AdoptNow(out) {
		t.Fatal("adopted without a copy")
	}
	waitStats(t, p, func(map[string]int64) bool { return fetcher.calls.Load() == 1 })
	if fetcher.calls.Load() != 1 {
		t.Fatal("the output was not pulled after a failed adoption")
	}
}
