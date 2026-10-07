package tiering

import (
	"context"
	"time"
)

// ManifestCoordinator keeps the cluster file manifest in step with the hot
// tier. The manifest is what peer file replication pulls from: an entry that
// outlives its hot copy makes every node fetch the file back from a peer,
// and a restarted node fails catch-up on it once no peer has it. It lives
// here rather than in the cluster package so tiering has no compile-time
// dependency on it — the adapter in cmd/arc wraps the coordinator. Nil means
// no cluster: every use is skipped.
type ManifestCoordinator interface {
	// DeleteFilesFromManifest proposes ONE Raft entry removing paths from the
	// manifest. Callers keep a call to manifestChunk paths: every node
	// applies the entry synchronously and offers each removed local copy to
	// a bounded unlink queue. Manifest-before-storage: nothing is deleted
	// from hot storage until this returns nil. The adapter retries transient
	// leader errors itself; an error here is final for the cycle.
	DeleteFilesFromManifest(ctx context.Context, paths []string, reason string) error
	// ManifestEntry reports whether the hot path is still registered and, if
	// so, the size the manifest recorded for it.
	ManifestEntry(path string) (size int64, ok bool)
}

// manifestChunk bounds one manifest proposal, and manifestChunkPause is
// the least time between two proposals from this node, whatever their
// source (a migration batch, orphan reconciliation, the sweep) — the
// manager paces them in deleteFromManifest. Every node's FSM applies a
// batch synchronously and offers each removed local copy to a 1024-slot
// unlink queue that drops on overflow; its worker drains the queue after a
// 500 ms grace. 200 per entry, one entry per second, stays well under the
// cap by construction.
const (
	manifestChunk      = 200
	manifestChunkPause = time.Second
)

// manifestSettle is how old a cold row must be before the manifest sweep
// trusts it: a row the sync recorded from a cold object another node is
// still in the middle of migrating must not have its replicas unlinked yet.
// The sync stamps migrated_at from the object's own timestamp.
const manifestSettle = time.Hour

// manifestReasonPrefix is what every reason tiering itself proposes starts
// with. A node receiving a manifest delete uses it to decide whether the
// removal is worth one cold-tier existence check (see Manager.applyUnlinked);
// it is a hint, never proof, because the operator manifest-delete endpoint
// passes an arbitrary caller-supplied reason.
const manifestReasonPrefix = "tiering:"

const (
	manifestReasonMigrated  = manifestReasonPrefix + "migrated"
	manifestReasonReconcile = manifestReasonPrefix + "reconcile"
	manifestReasonSweep     = manifestReasonPrefix + "sweep"
)

// RetryTransient runs fn up to attempts times, waiting base, 2·base, 4·base…
// between attempts (attempts−1 waits) while isTransient reports the error as
// worth retrying and ctx is live; a context that ends mid-wait is reported
// as such. The adapter in cmd/arc uses it for the leader-election blips a
// manifest proposal can hit mid-cycle; anything else is final on the first
// try.
func RetryTransient(ctx context.Context, attempts int, base time.Duration, fn func() error, isTransient func(error) bool) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil || !isTransient(err) {
			return err
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(base << i):
		}
	}
	return err
}

// sleepCtx pauses for d unless ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// chunkPaths splits paths into consecutive slices of at most n.
func chunkPaths(paths []string, n int) [][]string {
	var chunks [][]string
	for len(paths) > n {
		chunks = append(chunks, paths[:n])
		paths = paths[n:]
	}
	if len(paths) > 0 {
		chunks = append(chunks, paths)
	}
	return chunks
}
