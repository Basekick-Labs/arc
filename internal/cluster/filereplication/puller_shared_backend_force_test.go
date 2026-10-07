package filereplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// On a shared backend every node reads the writer's own object, so a content
// change is just a registration: a forced refresh would download the object
// from a peer and upload it back over the same key. With ForceContentRefresh
// off (what the coordinator sets for non-local storage) the FSM's callback
// pair must leave the same-size copy alone and fetch nothing.
func TestPullerContentChangeIsNotForcedWithoutForceContentRefresh(t *testing.T) {
	const path = "db/cpu/2026/10/05/12/shared.parquet"
	oldBody := []byte("generation-one")
	newBody := []byte("generation-two")
	if len(oldBody) != len(newBody) {
		t.Fatal("test setup: bodies must have the same size")
	}
	entry := makeEntry(path, "writer-1", int64(len(newBody)))
	entry.LSN = 7
	hash := sha256.Sum256(newBody)
	entry.SHA256 = fmt.Sprintf("%x", hash)
	backend := newFakeBackend()
	if err := backend.Write(context.Background(), path, oldBody); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	fetcher := newFakeFetcher(fakeFetchResult{body: newBody})
	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             backend,
		Fetcher:             fetcher,
		PeerResolver:        staticResolver{nodeID: "writer-1", addrs: []string{"peer:9100"}, ok: true},
		Workers:             1,
		QueueSize:           8,
		RetryMaxAttempts:    3,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		// ForceContentRefresh deliberately left false: a shared backend.
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("New puller: %v", err)
	}
	manifest := &synchronizedManifestEntry{entry: *entry}
	p.cfg.ManifestEntry = manifest.get

	p.Enqueue(entry)
	p.EnqueueContentChanged(entry)
	p.Start(context.Background())
	defer p.Stop()

	stats := waitStats(t, p, func(s map[string]int64) bool {
		return s["skipped_local"] == 1 && s["inflight_count"] == 0
	})
	if stats["skipped_local"] != 1 || stats["pulled"] != 0 || fetcher.calls.Load() != 0 {
		t.Fatalf("content change was forced on a shared backend: stats=%v calls=%d", stats, fetcher.calls.Load())
	}
	got, err := backend.Read(context.Background(), path)
	if err != nil || !bytes.Equal(got, oldBody) {
		t.Fatalf("shared object was rewritten: body=%q err=%v", got, err)
	}
}
