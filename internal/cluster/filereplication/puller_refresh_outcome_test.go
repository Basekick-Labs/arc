package filereplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestEnqueueContentChangedReportsItsOutcome: the delete worker keeps a path
// the manifest lists again and logs whether its refresh will run, so every
// outcome must be told apart. The puller is not started, so nothing drains
// its queue and a full queue is deterministic.
func TestEnqueueContentChangedReportsItsOutcome(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{ok: false})

	if p.Stopped() {
		t.Fatal("a puller that was never started reports stopped")
	}
	if got := p.EnqueueContentChanged(makeEntry("db/m/self.parquet", "reader-1", 10)); got != RefreshSkippedSelf {
		t.Fatalf("self-origin outcome = %v, want RefreshSkippedSelf", got)
	}
	if got := p.EnqueueContentChanged(nil); got != RefreshInvalid {
		t.Fatalf("nil entry outcome = %v, want RefreshInvalid", got)
	}
	for i := 0; i < 8; i++ {
		if got := p.EnqueueContentChanged(makeEntry(fmt.Sprintf("db/m/p%d.parquet", i), "writer-1", 10)); got != RefreshQueued {
			t.Fatalf("enqueue %d outcome = %v, want RefreshQueued", i, got)
		}
	}
	if got := p.EnqueueContentChanged(makeEntry("db/m/p0.parquet", "writer-1", 10)); got != RefreshQueued {
		t.Fatalf("a forced request already waiting for the path = %v, want RefreshQueued", got)
	}
	if got := p.EnqueueContentChanged(makeEntry("db/m/full.parquet", "writer-1", 10)); got != RefreshDropped {
		t.Fatalf("full queue outcome = %v, want RefreshDropped", got)
	}

	p.Start(context.Background())
	if p.Stopped() {
		t.Fatal("a running puller reports stopped")
	}
	p.Stop()
	if !p.Stopped() {
		t.Fatal("a stopped puller does not report stopped")
	}
	if got := p.EnqueueContentChanged(makeEntry("db/m/late.parquet", "writer-1", 10)); got != RefreshStopped {
		t.Fatalf("outcome after Stop = %v, want RefreshStopped", got)
	}
}

// newVerifyPuller is a started puller over a real LocalBackend (whose ReadTo
// reads only the committed file) whose one peer serves body.
func newVerifyPuller(t *testing.T, body []byte) (*Puller, *storage.LocalBackend, *fakeFetcher) {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })
	fetcher := newRepeatingFetcher(body)
	p, err := New(Config{
		SelfNodeID:          "reader-1",
		Backend:             local,
		Fetcher:             fetcher,
		PeerResolver:        staticResolver{nodeID: "writer-1", addrs: []string{"peer:1"}, ok: true},
		Workers:             1,
		QueueSize:           8,
		RetryMaxAttempts:    2,
		RetryInitialBackoff: 10 * time.Millisecond,
		FetchTimeout:        2 * time.Second,
		ForceContentRefresh: true,
		Logger:              zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Start(context.Background())
	t.Cleanup(p.Stop)
	return p, local, fetcher
}

func entryFor(path string, body []byte) *raft.FileEntry {
	e := makeEntry(path, "writer-1", int64(len(body)))
	sum := sha256.Sum256(body)
	e.SHA256 = hex.EncodeToString(sum[:])
	return e
}

// TestVerifyRefreshSkipsTheDownloadOnlyWhenTheLocalCopyMatches: the delete
// worker's refresh of a relisted path hashes a same-size local copy first. A
// copy that already is the manifest's version is kept without a download;
// any other copy is replaced.
func TestVerifyRefreshSkipsTheDownloadOnlyWhenTheLocalCopyMatches(t *testing.T) {
	const path = "db/m/2026/10/12/00/verify.parquet"
	oldBody := []byte("old same-size payload")
	newBody := []byte("new same-size payload")
	ctx := context.Background()
	settled := func(s map[string]int64) bool { return s["inflight_count"] == 0 && s["queue_depth"] == 0 }

	cases := []struct {
		name         string
		local        []byte
		staged       []byte // a ".part" next to the committed file
		verify       bool
		wantFetches  int64
		wantVerified int64
		wantForced   int64
	}{
		{"already the new version", newBody, nil, true, 0, 1, 0},
		{"still the old version", oldBody, nil, true, 1, 0, 1},
		{"old committed, new staged", oldBody, newBody, true, 1, 0, 1},
		{"plain forced refresh never hashes", newBody, nil, false, 1, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, local, fetcher := newVerifyPuller(t, newBody)
			if err := local.Write(ctx, path, tc.local); err != nil {
				t.Fatal(err)
			}
			if tc.staged != nil {
				if err := os.WriteFile(filepath.Join(local.GetBasePath(), path+storage.PartSuffix), tc.staged, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			entry := entryFor(path, newBody)
			var got RefreshOutcome
			if tc.verify {
				got = p.EnqueueVerifyContent(entry)
			} else {
				got = p.EnqueueContentChanged(entry)
			}
			if got != RefreshQueued {
				t.Fatalf("outcome = %v, want queued", got)
			}
			stats := waitStats(t, p, func(s map[string]int64) bool {
				return settled(s) && s["pulled"]+s["refresh_verified"] >= 1
			})
			if n := fetcher.calls.Load(); n != tc.wantFetches {
				t.Fatalf("fetches = %d, want %d; stats=%v", n, tc.wantFetches, stats)
			}
			if stats["refresh_verified"] != tc.wantVerified || stats["forced_refresh"] != tc.wantForced {
				t.Fatalf("refresh_verified=%d forced_refresh=%d, want %d and %d; stats=%v",
					stats["refresh_verified"], stats["forced_refresh"], tc.wantVerified, tc.wantForced, stats)
			}
			body, err := local.Read(ctx, path)
			if err != nil || !bytes.Equal(body, newBody) {
				t.Fatalf("local copy = %q, err=%v, want the new version", body, err)
			}
		})
	}
}

// TestRefreshPendingGauge: the forced refresh a full queue drops is remembered
// by path, and Stats shows how many are waiting without taking a lock.
func TestRefreshPendingGauge(t *testing.T) {
	p := newTestPuller(t, newFakeBackend(), newFakeFetcher(), staticResolver{ok: false})
	for i := 0; i < 8; i++ {
		p.EnqueueContentChanged(makeEntry(fmt.Sprintf("db/m/p%d.parquet", i), "writer-1", 10))
	}
	if got := p.EnqueueContentChanged(makeEntry("db/m/full.parquet", "writer-1", 10)); got != RefreshDropped {
		t.Fatalf("outcome = %v, want dropped", got)
	}
	p.EnqueueContentChanged(makeEntry("db/m/full.parquet", "writer-1", 10)) // dropped again: counted once
	if n := p.Stats()["refresh_pending"]; n != 1 {
		t.Fatalf("refresh_pending = %d, want 1", n)
	}
	if n := p.CatchUpStatus()["refresh_pending"]; n != 1 {
		t.Fatalf("cluster status refresh_pending = %d, want 1", n)
	}
	p.OnManifestDelete("db/m/full.parquet")
	if n := p.Stats()["refresh_pending"]; n != 0 {
		t.Fatalf("refresh_pending after the manifest delete = %d, want 0", n)
	}
}

// TestLocalCopyMatchesFailsClosed: the verify hash may only skip a download
// on a positive match. No checksum to compare against, or a context that is
// done (LocalBackend.ReadTo does not watch it; the hasher's writer does),
// downloads instead.
func TestLocalCopyMatchesFailsClosed(t *testing.T) {
	const path = "db/m/2026/10/12/00/closed.parquet"
	body := []byte("same bytes on disk and in the manifest")
	p, local, _ := newVerifyPuller(t, body)
	if err := local.Write(context.Background(), path, body); err != nil {
		t.Fatal(err)
	}
	entry := entryFor(path, body)
	if !p.localCopyMatches(zerolog.Nop(), entry) {
		t.Fatal("precondition: a matching copy must match")
	}
	noSum := *entry
	noSum.SHA256 = ""
	if p.localCopyMatches(zerolog.Nop(), &noSum) {
		t.Fatal("an entry without a checksum was treated as matching")
	}
	p.Stop()
	if p.localCopyMatches(zerolog.Nop(), entry) {
		t.Fatal("a hash read on a stopped puller was treated as matching")
	}
}
