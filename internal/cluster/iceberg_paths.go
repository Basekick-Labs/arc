package cluster

import (
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Iceberg export notifications from the cluster side (#1728).
//
// In a cluster, DELETE, retention and tiering run on the primary writer while
// the Iceberg export runs on the compaction-lease holder, so the writer's own
// reconcile requests (#1729) are dropped at the export's gate. These are the
// exporter node's own signals that files it lists changed:
//
//   - Local storage: this node's delete worker unlinked its copy. Never early:
//     by the unlink the export's listing has lost the file (or excludes it).
//     Not to be moved to apply time: a DELETE rewrite's original unlinked
//     before its rewrite was pulled would publish a table without the
//     surviving rows; that case reports when the rewrite lands instead.
//   - Shared bucket: a manifest delete, by reason, now or after a delay
//     (the writer deletes the object after the manifest).
//   - Tiering: the replicated tier-event drain's unlinked/missing events.
//
// Each reaches the scheduler's RequestReconcilePaths through icebergPaths,
// which main.go wires; unset (Iceberg off) it costs nothing, and on a node
// that does not export the scheduler's gate drops the request.

// icebergNotifier holds the export's path callbacks and the unlinks waiting
// to be reported.
type icebergNotifier struct {
	now   atomic.Pointer[func(keys []string)]
	later atomic.Pointer[func(keys []string)] // delayed, no cold re-list

	mu       sync.Mutex
	unlinked []string
}

// unlinkReportBatch flushes the unlink report within a long batch too: a
// delete worker batch can take minutes (each unlink may wait for staged
// adoption).
const unlinkReportBatch = 1000

// SetIcebergPathCallbacks wires the Iceberg export's requests: now for a
// change already visible to this node's listing, later for a shared-bucket
// manifest delete whose object the writer removes after the manifest (main.go:
// the scheduler's RequestReconcilePaths and RequestReconcilePathsLater). Safe
// to call on a running coordinator.
func (c *Coordinator) SetIcebergPathCallbacks(now, later func(keys []string)) {
	if now != nil {
		c.iceberg.now.Store(&now)
	}
	if later != nil {
		c.iceberg.later.Store(&later)
	}
}

// dirsOf collapses storage keys to their parent directories, the form the
// scheduler maps at least cost: a batch of 1000 files in one hour is one key.
func dirsOf(paths []string) []string {
	seen := make(map[string]bool, len(paths))
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		d := path.Dir(p) + "/"
		if d == "./" || seen[d] {
			continue
		}
		seen[d] = true
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

func (c *Coordinator) icebergNow(paths []string) {
	if fn := c.iceberg.now.Load(); fn != nil && len(paths) > 0 {
		(*fn)(dirsOf(paths))
	}
}

func (c *Coordinator) icebergLater(paths []string) {
	if fn := c.iceberg.later.Load(); fn != nil && len(paths) > 0 {
		(*fn)(dirsOf(paths))
	}
}

// noteUnlinked records a local copy the delete worker removed, for the next
// flush.
func (c *Coordinator) noteUnlinked(p string) {
	if c.iceberg.now.Load() == nil {
		return
	}
	c.iceberg.mu.Lock()
	c.iceberg.unlinked = append(c.iceberg.unlinked, p)
	var full []string
	if len(c.iceberg.unlinked) >= unlinkReportBatch {
		full, c.iceberg.unlinked = c.iceberg.unlinked, nil
	}
	c.iceberg.mu.Unlock()
	c.icebergNow(full)
}

// flushUnlinked reports the unlinks recorded since the last flush.
func (c *Coordinator) flushUnlinked() {
	c.iceberg.mu.Lock()
	paths := c.iceberg.unlinked
	c.iceberg.unlinked = nil
	c.iceberg.mu.Unlock()
	c.icebergNow(paths)
}

// icebergSharedDelete tells the export about a manifest delete on a shared
// bucket, by reason (#1728):
//
//   - a DELETE rewrite's original is left out of every read from now on
//     (observeSharedManifestDelete), so the table can drop it now;
//   - retention and a whole-file DELETE remove the object after the
//     manifest, so a request now would still list it (and keep it, as an
//     object the manifest does not know): asked later instead;
//   - a database delete drops its tables itself, and a reconcile racing
//     that drop could recreate one; a compaction's commit asks on its own
//     (#1726); tiering is the drain's (recordManifestUnlinkedFile); a
//     snapshot restore is rare and left to the timer. None ask here.
func (c *Coordinator) icebergSharedDelete(p, reason string) {
	switch {
	case reason == deleteRewriteReason:
		c.icebergNow([]string{p})
	case reason == "delete" || strings.HasPrefix(reason, "retention:"):
		c.icebergLater([]string{p})
	}
}
