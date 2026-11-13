package edgesync

import (
	"context"
	"fmt"
)

// ClusterManifest is the replicated file manifest of a cluster hub, as the
// receipt checks need it (#1667). Nil off a cluster.
//
// Receipts live in each node's own arc.db, and a spoke syncs to one node,
// while the operations that remove spoke content (database delete,
// retention, a row-level DELETE) run on the writer and mark the writer's
// receipts, which hold none of that spoke's. So on a cluster a receipt whose
// file is gone cannot tell, by itself, a file the cluster removed (the spoke
// must not re-send it) from a copy this node lost (it should). The manifest
// can: the network receive path registers a file before it records the
// receipt, and a cluster-wide removal is a manifest delete. The manifest is
// replicated and survives snapshots, so the answer holds after a crash, a
// restart or a snapshot install, which an in-memory queue of delete events
// would not.
//
// Known trade-offs: a receipt whose file left the manifest for a reason other
// than an intended removal is marked removed too, so the spoke does not heal
// it. That covers manifest repair or the reconciliation sweep dropping a
// lost object, an air-gap import whose batched registration never flushed,
// and a receipt recorded before the hub joined a cluster. All are narrow
// (a spoke only reconciles files it has not marked synced), and the admin
// forget route recovers them.
type ClusterManifest interface {
	// Sync brings this node's view of the manifest up to everything the
	// cluster committed before the call (a Raft barrier), bounded.
	Sync(ctx context.Context) error
	// Lists reports whether the manifest lists a hub storage path.
	Lists(path string) bool
}

// RemovedByManifest marks a receipt whose file the cluster manifest no longer
// lists (#1667): removed by an operation on another node, whose own reason
// this node never saw. Conservative for anything that later reads the
// reason: the content may survive elsewhere (a migration, a compaction that
// ran on another node), but the receipt never claims it does.
const RemovedByManifest = "manifest_removed"

// settleStale decides what to do with receipts whose files are gone from this
// node's storage. Off a cluster every one is forgotten, so the spoke re-sends,
// as before. On a cluster the manifest decides: a path it still lists is a
// local copy that is missing and is forgotten (the re-send heals it); a path
// it no longer lists was removed cluster-wide, and its receipt is marked
// removed and kept, so the spoke does not resurrect it. Returns the paths to
// forget; the rest are now vouched. An error leaves every receipt as it was.
func settleStale(ctx context.Context, idx *HubIndex, m ClusterManifest, spokeID string, stale []string) (forget []string, err error) {
	if m == nil || len(stale) == 0 {
		return stale, nil
	}
	if err := m.Sync(ctx); err != nil {
		return nil, fmt.Errorf("edgesync: catch up with the cluster manifest before forgetting receipts: %w", err)
	}
	var removed []string
	for _, p := range stale {
		if m.Lists(NamespacedPath(spokeID, p)) {
			forget = append(forget, p)
		} else {
			removed = append(removed, p)
		}
	}
	if len(removed) > 0 {
		if err := idx.MarkRemoved(ctx, spokeID, removed, RemovedByManifest); err != nil {
			return nil, err
		}
	}
	return forget, nil
}
