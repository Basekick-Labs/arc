package edgesync

import (
	"context"
	"fmt"
	"sync"

	"github.com/basekick-labs/arc/internal/storage"
)

// VerifyResult is the hub's answer to a verify reconcile (edge sync phase 3a,
// delete_after_sync): which of a spoke's SYNCED files the hub provably holds.
type VerifyResult struct {
	// Held: a plain receipt (neither compacted nor removed) with the spoke's
	// digest, whose own file exists in this hub's storage and, on a cluster,
	// is listed in the manifest. The only answer that lets a spoke delete its
	// copy.
	Held []string
	// Missing: no receipt at all for a permitted entry. The hub forgot it
	// (the admin forget route) or never had it; the spoke re-offers it.
	Missing []string
	// HeldCompacted: a compacted receipt with the spoke's digest whose
	// recorded holder exists at verify time (#1683). Reported apart from Held
	// because hub compaction transforms rows (drops exact duplicates, widens
	// mixed types, nulls unparseable times): a spoke deletes these only when
	// it opted in.
	HeldCompacted []string
}

// Verify answers which entries the hub holds, for a spoke deciding whether
// it may delete its own copy. Unlike Reconcile it CHANGES NOTHING: no stale
// receipt is forgotten and none is marked, because the entries are files the
// spoke already marked synced, and the trade-offs elsewhere (cluster_manifest.go)
// rest on reconcile only ever acting on files a spoke has not.
//
// A compacted receipt is never in Held. It is in HeldCompacted when it
// records its holder (compacted_into, #1683) and that holder exists at
// verify time; a receipt with no recorded holder is neither. Removed
// receipts are never held: the hub deleted the content.
// An entry the caller may not write (allow) is neither held nor missing, so
// RBAC never reads as a forgotten file.
func (r *Reconciler) Verify(ctx context.Context, spokeID string, entries []ReconcileEntry, allow ReconcileAllow) (*VerifyResult, error) {
	if err := validateSpokeID(spokeID); err != nil {
		return nil, err
	}
	if len(entries) > r.maxEntries {
		return nil, fmt.Errorf("%w: %d entries, maximum %d", ErrReconcileTooLarge, len(entries), r.maxEntries)
	}
	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		if err := validateSyncPathForSpoke(spokeID, e.Path); err != nil {
			return nil, fmt.Errorf("edgesync: verify entry %d: %w", i, err)
		}
		if !isHexSHA256(e.SHA256) {
			return nil, fmt.Errorf("edgesync: verify entry %d: sha256 %q is not a 64-character hex digest", i, e.SHA256)
		}
		if _, dup := seen[e.Path]; dup {
			return nil, fmt.Errorf("edgesync: verify batch contains %q twice", e.Path)
		}
		seen[e.Path] = struct{}{}
	}

	permitted := make([]ReconcileEntry, 0, len(entries))
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		if allow != nil {
			ok, err := allow(e.Path)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		permitted = append(permitted, e)
		paths = append(paths, e.Path)
	}

	held, err := r.index.Lookup(ctx, spokeID, paths)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrReceiveInternal, err)
	}

	res := &VerifyResult{Held: []string{}, Missing: []string{}, HeldCompacted: []string{}}
	plain := make(map[string]HeldFile, len(held))
	candidates := make([]ReconcileEntry, 0, len(permitted))
	var compacted []ReconcileEntry
	for _, e := range permitted {
		hf, ok := held[e.Path]
		switch {
		case !ok:
			res.Missing = append(res.Missing, e.Path)
		case hf.Removed || hf.SHA256 != e.SHA256:
			// Removed on purpose, or different content: never held.
		case hf.Compacted:
			if hf.CompactedInto != "" {
				compacted = append(compacted, e)
			}
			// A compacted receipt with no recorded holder (one compacted
			// before #1683, or rewritten by a row-level DELETE) is not held.
		default:
			plain[e.Path] = hf
			candidates = append(candidates, e)
		}
	}
	if len(candidates) == 0 && len(compacted) == 0 {
		return res, nil
	}

	// ORDER MATTERS on a cluster: every existence check first, then the
	// manifest catch-up (a Raft barrier), then the manifest lookup. Cluster
	// deletes update the manifest before storage, so a file that existed
	// before the barrier and is still listed after it was not removed before
	// the check. Catching up first would let a removal committed between the
	// barrier and the stat, not yet applied here, pass as held.
	gone := map[string]struct{}{}
	if len(candidates) > 0 {
		stale, err := r.confirmPresent(ctx, spokeID, candidates, plain)
		if err != nil {
			return nil, err
		}
		for _, p := range stale {
			gone[p] = struct{}{}
		}
	}
	holders, err := r.holderHotExists(ctx, compacted, held)
	if err != nil {
		return nil, err
	}
	if r.manifest != nil {
		if err := r.manifest.Sync(ctx); err != nil {
			return nil, fmt.Errorf("%w: catch up with the cluster manifest before verifying: %w", ErrReceiveInternal, err)
		}
	}
	for _, e := range candidates {
		if _, g := gone[e.Path]; g {
			continue
		}
		if r.manifest != nil && !r.manifest.Lists(NamespacedPath(spokeID, e.Path)) {
			continue
		}
		res.Held = append(res.Held, e.Path)
	}
	if err := r.verifyCompacted(ctx, compacted, held, holders, res); err != nil {
		return nil, err
	}
	return res, nil
}

// holderHotExists stats each distinct recorded holder on hot storage, with
// the bounded concurrency confirmPresent uses: many receipts share one
// output, and a long backlog can name thousands of outputs.
func (r *Reconciler) holderHotExists(ctx context.Context, entries []ReconcileEntry, held map[string]HeldFile) (map[string]bool, error) {
	out := make(map[string]bool)
	for _, e := range entries {
		out[held[e.Path].CompactedInto] = false
	}
	if len(out) == 0 {
		return out, nil
	}
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		firstErr error
	)
	holders := make([]string, 0, len(out))
	for h := range out {
		holders = append(holders, h)
	}
	sem := make(chan struct{}, confirmExistenceConcurrency)
	for _, holder := range holders {
		wg.Add(1)
		sem <- struct{}{}
		go func(h string) {
			defer wg.Done()
			defer func() { <-sem }()
			ok, err := r.backend.Exists(ctx, h)
			if storage.IsStoreNotFound(err) {
				ok, err = false, nil
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("%w: check holder %q: %w", ErrReceiveInternal, h, err)
				}
				return
			}
			out[h] = ok
		}(holder)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// verifyCompacted answers which compacted receipts' recorded holders exist
// (#1683). A holder is a hub storage path whose name is never reused
// (compaction outputs carry a UnixNano timestamp; a cold copy keeps the hot
// key), so an object there is the content. Hot: it existed before the
// manifest catch-up and, on a cluster, the manifest lists it after (a staged
// output not yet copied to its final key does not count). Cold: it exists in
// the cold tier, which the manifest does not list.
func (r *Reconciler) verifyCompacted(ctx context.Context, entries []ReconcileEntry, held map[string]HeldFile, hot map[string]bool, res *VerifyResult) error {
	decided := make(map[string]bool)
	for _, e := range entries {
		holder := held[e.Path].CompactedInto
		ok, seen := decided[holder]
		if !seen {
			ok = hot[holder] && (r.manifest == nil || r.manifest.Lists(holder))
			if !ok && r.coldExists != nil {
				var err error
				ok, err = r.coldExists(ctx, holder)
				if err != nil {
					return fmt.Errorf("%w: check cold holder %q: %w", ErrReceiveInternal, holder, err)
				}
			}
			decided[holder] = ok
		}
		if ok {
			res.HeldCompacted = append(res.HeldCompacted, e.Path)
		}
	}
	return nil
}
