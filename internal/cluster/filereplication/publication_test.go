package filereplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/stretchr/testify/require"
)

func TestPullerPublishesExactVersionBeforeCatchUpSucceeds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		local []byte
		fail  bool
	}{
		{name: "fresh_pull"},
		{name: "existing_verified_copy", local: []byte("correct")},
		{name: "same_size_wrong_copy", local: []byte("corrupt")},
		{name: "publication_failure", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			backend := newFakeBackend()
			body := []byte("correct")
			entry := makeEntry("testdb/cpu/2026/04/11/14/source.parquet", "writer-1", int64(len(body)))
			digest := sha256.Sum256(body)
			entry.SHA256 = hex.EncodeToString(digest[:])
			entry.WALCoverage = []replicaview.PartitionCoverage{{Hour: 1, Coverage: replicaview.Coverage{{Instance: 12, First: 1, Last: 3}}}}
			entry.Replaces = []string{"testdb/cpu/2026/04/11/14/old.parquet"}
			if tc.local != nil {
				require.NoError(t, backend.Write(ctx, entry.Path, tc.local))
			}
			fetcher := newRepeatingFetcher(body)
			resolver := staticResolver{nodeID: "writer-1", addrs: []string{"peer:9100"}, ok: true}
			p := newTierPuller(t, backend, fetcher, resolver, nil, func(string) (raft.FileEntry, bool) { return *entry, true })
			var fail atomic.Bool
			fail.Store(tc.fail)
			var published atomic.Int64
			var wrongVersion atomic.Bool
			p.cfg.PublishLocalFile = func(ctx context.Context, got raft.FileEntry) error {
				if got.Path != entry.Path || got.SHA256 != entry.SHA256 || got.LSN != entry.LSN || len(got.WALCoverage) != 1 || len(got.Replaces) != 1 {
					wrongVersion.Store(true)
					return errors.New("wrong publication version")
				}
				if fail.Load() {
					return errors.New("pin unavailable")
				}
				contents, err := backend.Read(ctx, got.Path)
				if err != nil {
					return err
				}
				if !bytes.Equal(contents, body) {
					return errors.New("local checksum mismatch")
				}
				published.Add(1)
				return nil
			}
			p.Start(ctx)
			defer p.Stop()
			p.catchupCompletedAt.Store(time.Now().Unix())
			p.markCatchUp(entry.Path)
			p.Enqueue(entry)
			require.Eventually(t, func() bool {
				return p.Stats()["inflight_count"] == 0 && (published.Load() > 0 || p.Stats()["failed"] > 0)
			}, 3*time.Second, time.Millisecond)
			require.False(t, wrongVersion.Load())
			if tc.fail {
				require.False(t, p.FullyCaughtUp(), "bytes on disk are insufficient when publication failed")
				require.Equal(t, int64(0), published.Load())
				require.Equal(t, int64(0), p.Stats()["pulled"])
				fail.Store(false)
				p.Enqueue(entry)
				require.Eventually(t, p.FullyCaughtUp, 3*time.Second, time.Millisecond)
				require.Equal(t, int64(1), published.Load(), "already-local retry must publish and repair readiness")
			} else {
				require.True(t, p.FullyCaughtUp())
				require.Equal(t, int64(1), published.Load())
				if bytes.Equal(tc.local, body) {
					require.Equal(t, int64(0), fetcher.calls.Load())
				} else {
					require.Equal(t, int64(1), fetcher.calls.Load())
				}
			}
		})
	}
}

func TestPullerPublishesOwnFilesDuringCatchUpAndRegistration(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend()
	body := []byte("own file")
	entry := makeEntry("testdb/cpu/own.parquet", "reader-1", int64(len(body)))
	require.NoError(t, backend.Write(ctx, entry.Path, body))
	fetcher := newFakeFetcher()
	p := newTierPuller(t, backend, fetcher, staticResolver{}, nil, nil)
	var published atomic.Int64
	p.cfg.PublishLocalFile = func(context.Context, raft.FileEntry) error { published.Add(1); return nil }
	p.Start(ctx)
	defer p.Stop()
	p.RunCatchUp(ctx, sliceFetcher([]*raft.FileEntry{entry}))
	require.Eventually(t, p.FullyCaughtUp, time.Second, time.Millisecond)
	require.Equal(t, int64(1), published.Load(), "startup must publish the own-origin file before catch-up completes")
	p.Enqueue(entry)
	require.Eventually(t, func() bool { return published.Load() == 2 && p.Stats()["inflight_count"] == 0 }, time.Second, time.Millisecond)
	require.Equal(t, int64(0), fetcher.calls.Load(), "already verified local bytes need no network fetch")
}

func TestPullerPublicationRetainsQueuedCoverageVersion(t *testing.T) {
	body := []byte("queued file")
	entry := makeEntry("testdb/cpu/queued.parquet", "writer-1", int64(len(body)))
	entry.WALCoverage = []replicaview.PartitionCoverage{{Hour: 1, Coverage: replicaview.Coverage{{Instance: 12, First: 1, Last: 3}}}}
	entry.Replaces = []string{"original"}
	p := newTierPuller(t, newFakeBackend(), newRepeatingFetcher(body), staticResolver{nodeID: "writer-1", addrs: []string{"peer:9100"}, ok: true}, nil, nil)
	got := make(chan raft.FileEntry, 1)
	p.cfg.PublishLocalFile = func(_ context.Context, entry raft.FileEntry) error { got <- entry; return nil }
	p.Enqueue(entry)
	// The caller owns its slices after Enqueue returns. Mutating them must
	// not rewrite the identity coverage associated with a queued checksum.
	entry.WALCoverage[0].Coverage[0].Last = 99
	entry.Replaces[0] = "changed"
	p.Start(context.Background())
	defer p.Stop()
	select {
	case actual := <-got:
		require.Equal(t, uint64(3), actual.WALCoverage[0].Coverage[0].Last)
		require.Equal(t, []string{"original"}, actual.Replaces)
	case <-time.After(time.Second):
		t.Fatal("no publication")
	}
}

func TestPullerDoesNotPublishFileDeletedDuringTransfer(t *testing.T) {
	body := []byte("obsolete file")
	entry := makeEntry("testdb/cpu/gone.parquet", "writer-1", int64(len(body)))
	fetcher := &blockingSuccessFetcher{release: make(chan struct{}), body: body}
	var present atomic.Bool
	present.Store(true)
	p := newTierPuller(t, newFakeBackend(), fetcher, staticResolver{nodeID: "writer-1", addrs: []string{"peer:9100"}, ok: true}, nil, func(string) (raft.FileEntry, bool) { return *entry, present.Load() })
	var published atomic.Int64
	p.cfg.PublishLocalFile = func(context.Context, raft.FileEntry) error { published.Add(1); return nil }
	p.Start(context.Background())
	defer p.Stop()
	p.Enqueue(entry)
	require.Eventually(t, func() bool { return fetcher.calls.Load() > 0 }, time.Second, time.Millisecond)
	present.Store(false)
	close(fetcher.release)
	require.Eventually(t, func() bool { return p.Stats()["inflight_count"] == 0 }, time.Second, time.Millisecond)
	require.Equal(t, int64(0), published.Load(), "manifest advertisement cannot resurrect a deleted file")
	require.Equal(t, int64(1), p.Stats()["skipped_gone"])
}
