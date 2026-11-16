package cluster

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/cluster/raft"
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
//   - Cold tier: the primary writer proposes a Raft notice after it removed
//     cold objects (ProposeIcebergColdRemoved); every node hands the
//     directories to its export, which drops its cached cold listing first.
//   - A database delete never asks, on any topology: a reconcile racing the
//     drop could recreate a table. (The drop runs on the writer and drops the
//     writer's catalog only; the exporter's tables are #1731.)
//
// Each reaches the scheduler's RequestReconcilePaths through icebergPaths,
// which main.go wires; unset (Iceberg off) it costs nothing, and on a node
// that does not export the scheduler's gate drops the request.

// icebergNotifier holds the export's path callbacks and the unlinks waiting
// to be reported.
type icebergNotifier struct {
	now   atomic.Pointer[func(keys []string)]
	later atomic.Pointer[func(keys []string)] // delayed, no cold re-list
	cold  atomic.Pointer[func(keys []string)] // drops the cold listing first

	blockersLogged atomic.Bool

	mu       sync.Mutex
	unlinked []string
}

// unlinkReportBatch flushes the unlink report within a long batch too: a
// delete worker batch can take minutes (each unlink may wait for staged
// adoption).
const unlinkReportBatch = 1000

// SetIcebergPathCallbacks wires the Iceberg export's requests: now for a
// hot change already visible to this node's listing, later for a
// shared-bucket manifest delete whose object the writer removes after the
// manifest, cold for an applied cold-removal notice (main.go: the
// scheduler's RequestReconcilePathsHot, RequestReconcilePathsLater and
// RequestReconcilePaths). Safe to call on a running coordinator.
func (c *Coordinator) SetIcebergPathCallbacks(now, later, cold func(keys []string)) {
	if now != nil {
		c.iceberg.now.Store(&now)
	}
	if later != nil {
		c.iceberg.later.Store(&later)
	}
	if cold != nil {
		c.iceberg.cold.Store(&cold)
	}
}

// onIcebergNotice is the FSM's observer of applied cold-removal notices. Runs
// on the apply path: an atomic load and a non-blocking request.
func (c *Coordinator) onIcebergNotice(keys []string) {
	if fn := c.iceberg.cold.Load(); fn != nil && len(keys) > 0 {
		(*fn)(keys)
	}
}

// measurementDirOf cuts a cold object key to its measurement directory: the
// export maps per measurement and drops cold listings per database, so the
// hour directories would only multiply the notice's keys. A partitioned key
// ends in {year}/{month}/{day}/{hour}/{file} (on a hub, under
// "{spoke}/{db}/{m}/"); anything shorter keeps its own directory.
func measurementDirOf(key string) string {
	key = strings.TrimLeft(key, "/")
	segs := strings.Split(key, "/")
	if len(segs) >= 7 {
		return strings.Join(segs[:len(segs)-5], "/") + "/"
	}
	if d := path.Dir(key); d != "." {
		return d + "/"
	}
	return ""
}

// ProposeIcebergColdRemoved tells every node's Iceberg export that these cold
// objects are gone (#1728). Called on the primary writer after the deletes,
// from a goroutine: errors are returned for the caller to log, never to fail
// the removal. Does nothing without Raft, and until every node and Raft
// server advertises CapabilityIcebergNotice.
func (c *Coordinator) ProposeIcebergColdRemoved(ctx context.Context, paths []string) error {
	if c == nil || c.raftNode == nil || len(paths) == 0 {
		return nil
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return nil
	}
	if _, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityIcebergNotice); len(blockers) > 0 {
		if c.iceberg.blockersLogged.CompareAndSwap(false, true) {
			c.logger.Info().Strs("nodes", blockers).
				Msg("Iceberg cold-removal notices are off until every node runs a build that applies them; the export catches cold removals at its timed pass meanwhile")
		}
		return nil
	}
	seen := map[string]bool{}
	var keys []string
	for _, p := range paths {
		if k := measurementDirOf(p); k != "" && !seen[k] && raft.ValidIcebergNoticeKey(k) {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for len(keys) > 0 {
		n, bytes := 0, 0
		for n < len(keys) && n < raft.MaxIcebergNoticeKeys && bytes+len(keys[n]) <= raft.MaxIcebergNoticeKeyBytes {
			bytes += len(keys[n])
			n++
		}
		raw, err := json.Marshal(raft.IcebergPathsChangedPayload{Keys: keys[:n]})
		if err != nil {
			return err
		}
		if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandIcebergPathsChanged, Payload: raw}); err != nil {
			return err
		}
		keys = keys[n:]
	}
	return nil
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

// databaseDeleteReason is the manifest reason of a database delete (the api
// package's databaseDeleteManifestReason; the packages do not import each
// other).
const databaseDeleteReason = "database:delete"
