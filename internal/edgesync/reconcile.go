package edgesync

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/basekick-labs/arc/internal/storage"
)

// MaxReconcileEntriesDefault bounds one reconcile batch.
//
// §5.1 wants the whole pending set in a single round-trip, and notes 100k
// entries ≈ 20MB. Arc cannot honor that literally: the Fiber app runs with
// StreamRequestBody=false (api/server.go) so fasthttp buffers the entire
// request body before routing — and therefore before authentication. An
// unbounded reconcile would let anyone able to reach the port make the hub
// hold tens of megabytes per connection.
//
// Capping and letting the spoke page keeps the property that actually matters:
// discovery costs O(batches), not O(files), so 5,000 pending files is one
// request rather than 5,000. At ~200 bytes per entry this is ~2MB.
const MaxReconcileEntriesDefault = 10_000

// ErrReconcileTooLarge is returned when a batch exceeds the configured cap.
// The spoke's remedy is to split the batch, not to retry it unchanged.
var ErrReconcileTooLarge = errors.New("edgesync: reconcile batch exceeds the configured maximum")

// ReconcileEntry is one file a spoke is asking about.
type ReconcileEntry struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size,omitempty"`
}

// Reconciler answers "which of these files do you already have?" for a spoke.
type Reconciler struct {
	index      *HubIndex
	backend    storage.Backend
	maxEntries int
}

// ReconcilerConfig configures a Reconciler.
type ReconcilerConfig struct {
	Index *HubIndex

	// Backend confirms a file the index claims still exists in storage.
	//
	// Required, and the reason is data loss rather than tidiness. The index
	// records what the hub RECEIVED; it does not learn about deletions.
	// Anything that removes a file from the hub — Arc retention pointed at a
	// spoke's namespace (its prefix is an operator-chosen string, so
	// `database = "rocket-01"` sweeps that spoke), a cold-tier migration, an
	// operator with rm — leaves the index asserting a file the hub no longer
	// has. Reporting that as `present` makes the spoke mark it synced, and a
	// spoke configured to reclaim space would then delete its only copy.
	//
	// Confirming costs one stat per candidate the index claims (~3µs on local
	// disk, so ~31ms for a 10k batch) and no parquet reads, which keeps §5.1's
	// actual promise: reconcile does not read file contents.
	Backend storage.Backend

	// MaxEntries caps one batch. Zero uses MaxReconcileEntriesDefault.
	MaxEntries int
}

// NewReconciler validates configuration and returns a ready Reconciler.
func NewReconciler(cfg ReconcilerConfig) (*Reconciler, error) {
	if cfg.Index == nil {
		return nil, errors.New("edgesync: reconciler requires a hub index")
	}
	if cfg.Backend == nil {
		return nil, errors.New("edgesync: reconciler requires a storage backend to confirm indexed files still exist")
	}
	max := cfg.MaxEntries
	if max <= 0 {
		max = MaxReconcileEntriesDefault
	}
	return &Reconciler{index: cfg.Index, backend: cfg.Backend, maxEntries: max}, nil
}

// MaxEntries reports the configured per-batch cap, so a handler can reject an
// oversized request before decoding it.
func (r *Reconciler) MaxEntries() int { return r.maxEntries }

// Reconcile partitions a spoke's pending set into what the hub is missing,
// what it already holds, and what disagrees.
//
// Answered entirely from the hub index — one batched SQLite lookup, no reads
// of parquet bytes — which is what makes this affordable for a spoke returning
// from a long outage. §6.1's identity rule decides each entry: absent is
// missing, a matching digest is present, and a differing digest is a conflict.
func (r *Reconciler) Reconcile(ctx context.Context, spokeID string, entries []ReconcileEntry) (*ReconcileResult, error) {
	if err := validateSpokeID(spokeID); err != nil {
		return nil, err
	}
	if len(entries) > r.maxEntries {
		return nil, fmt.Errorf("%w: %d entries, maximum %d", ErrReconcileTooLarge, len(entries), r.maxEntries)
	}

	// Validate before looking anything up. A malformed entry means the spoke
	// is confused or hostile, and answering part of a bad batch would leave it
	// unable to tell which entries were actually considered.
	paths := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		if err := validateSyncPath(e.Path); err != nil {
			return nil, fmt.Errorf("edgesync: reconcile entry %d: %w", i, err)
		}
		if !isHexSHA256(e.SHA256) {
			return nil, fmt.Errorf("edgesync: reconcile entry %d: sha256 %q is not a 64-character hex digest", i, e.SHA256)
		}
		// A duplicate path within one batch has no correct answer — the same
		// path cannot be both present and conflicted — and would produce a
		// result that ReconcileResult.Validate rejects.
		if _, dup := seen[e.Path]; dup {
			return nil, fmt.Errorf("edgesync: reconcile batch contains %q twice", e.Path)
		}
		seen[e.Path] = struct{}{}
		paths = append(paths, e.Path)
	}

	held, err := r.index.Lookup(ctx, spokeID, paths)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReceiveInternal, err)
	}

	// Confirm the indexed files still exist before vouching for them, and do
	// it concurrently. The index records receipts, not deletions, so a stale
	// row would otherwise make the hub claim a file it no longer holds — see
	// ReconcilerConfig.Backend.
	stale, err := r.confirmPresent(ctx, spokeID, entries, held)
	if err != nil {
		return nil, err
	}

	// One batched delete rather than one per stale row. These land on the
	// SQLite handle shared with ingest file-registration and auth, so a spoke
	// reconciling after a retention sweep must not turn into a write storm on
	// the single writer.
	if len(stale) > 0 {
		if err := r.index.ForgetBatch(ctx, spokeID, stale); err != nil {
			return nil, fmt.Errorf("%w: forget stale entries: %w", ErrReceiveInternal, err)
		}
		for _, p := range stale {
			delete(held, p)
		}
	}

	res := &ReconcileResult{}
	for _, e := range entries {
		existing, ok := held[e.Path]

		switch {
		case !ok:
			res.Missing = append(res.Missing, e.Path)
		case existing == e.SHA256:
			res.Present = append(res.Present, e.Path)
		default:
			// Same path, different content. Surfaced here for the whole
			// backlog at once rather than discovered one 409 at a time during
			// transfer — which is the point of doing this proactively.
			res.Conflicts = append(res.Conflicts, Conflict{
				Path:        e.Path,
				TheirSHA256: existing,
			})
		}
	}

	// The hub must never emit a result its own validator would reject; a spoke
	// calls Validate on everything it receives.
	if err := res.Validate(); err != nil {
		return nil, fmt.Errorf("%w: built an invalid reconcile result: %w", ErrReceiveInternal, err)
	}
	return res, nil
}

// confirmExistenceConcurrency bounds how many existence checks run at once.
//
// On local disk a check is a ~5µs stat and concurrency barely matters. On S3
// it is a HeadObject round-trip, and serially that is fatal: 10,000 entries at
// a 20ms RTT is 200 seconds against a 2-minute request timeout, so an
// S3-backed hub could not answer a full batch at all. The checks are
// independent, so fanning them out turns that into a few seconds.
//
// 32 is deliberately modest — enough to hide per-request latency without
// making one spoke's reconcile monopolize the backend's connection pool while
// other spokes are transferring files.
const confirmExistenceConcurrency = 32

// confirmPresent checks that every file the index claims is still in storage,
// and returns the paths that are not.
func (r *Reconciler) confirmPresent(ctx context.Context, spokeID string, entries []ReconcileEntry, held map[string]string) ([]string, error) {
	candidates := make([]string, 0, len(held))
	for _, e := range entries {
		if _, ok := held[e.Path]; ok {
			candidates = append(candidates, e.Path)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	var (
		mu    sync.Mutex
		stale []string
		wg    sync.WaitGroup
	)
	sem := make(chan struct{}, confirmExistenceConcurrency)
	// Cancelled on the first failure so in-flight checks stop rather than
	// finishing work whose result is already discarded.
	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	for _, p := range candidates {
		wg.Add(1)
		sem <- struct{}{}
		go func(path string) {
			defer wg.Done()
			defer func() { <-sem }()

			present, err := r.backend.Exists(checkCtx, NamespacedPath(spokeID, path))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("%w: confirm %q: %w", ErrReceiveInternal, path, err)
					cancel()
				}
				return
			}
			if !present {
				stale = append(stale, path)
			}
		}(p)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return stale, nil
}
