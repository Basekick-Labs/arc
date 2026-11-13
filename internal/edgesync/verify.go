package edgesync

import (
	"context"
	"fmt"
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
}

// Verify answers which entries the hub holds, for a spoke deciding whether
// it may delete its own copy. Unlike Reconcile it CHANGES NOTHING: no stale
// receipt is forgotten and none is marked, because the entries are files the
// spoke already marked synced, and the trade-offs elsewhere (cluster_manifest.go)
// rest on reconcile only ever acting on files a spoke has not.
//
// Compacted receipts are never held: their content lives in a file the
// receipt does not name, and nothing on a cluster or outside Arc keeps that
// claim true. Removed receipts are never held: the hub deleted the content.
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

	res := &VerifyResult{Held: []string{}, Missing: []string{}}
	plain := make(map[string]HeldFile, len(held))
	candidates := make([]ReconcileEntry, 0, len(permitted))
	for _, e := range permitted {
		hf, ok := held[e.Path]
		switch {
		case !ok:
			res.Missing = append(res.Missing, e.Path)
		case hf.Vouched() || hf.SHA256 != e.SHA256:
			// Compacted, removed, or different content: not provably held.
		default:
			plain[e.Path] = hf
			candidates = append(candidates, e)
		}
	}
	if len(candidates) == 0 {
		return res, nil
	}

	// The existence check reconcile uses, without its forgetting.
	stale, err := r.confirmPresent(ctx, spokeID, candidates, plain)
	if err != nil {
		return nil, err
	}
	gone := make(map[string]struct{}, len(stale))
	for _, p := range stale {
		gone[p] = struct{}{}
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
	return res, nil
}
