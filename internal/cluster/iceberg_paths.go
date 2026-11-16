package cluster

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
//   - Cold tier: a writer proposes a Raft notice after it removed cold
//     objects (NoteIcebergColdRemoved); every node hands the measurement
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

	// Cold-removal keys waiting for the next notice (NoteIcebergColdRemoved).
	coldMu      sync.Mutex
	coldPending map[string]bool
	coldArmed   bool

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
// day or hour directories would only multiply the notice's keys. A
// partitioned key is {db}/{m}/{yyyy}/{mm}/... (hourly and daily files alike;
// on a hub "{spoke}/{db}/{m}/..."): cut before the first year/month pair
// after the database and measurement. A key without one keeps its own
// directory.
func measurementDirOf(key string) string {
	key = strings.TrimLeft(key, "/")
	segs := strings.Split(key, "/")
	for i := 2; i+2 < len(segs); i++ {
		if isDigits(segs[i], 4) && isDigits(segs[i+1], 2) {
			return strings.Join(segs[:i], "/") + "/"
		}
	}
	if d := path.Dir(key); d != "." {
		return d + "/"
	}
	return ""
}

func isDigits(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// coldNoticeDelay gathers cold removals into one notice: the migrator reports
// per file, and a retention run per run. A variable so tests can shorten it.
var coldNoticeDelay = time.Second

// coldNoticeTimeout bounds one notice's proposals.
const coldNoticeTimeout = 10 * time.Second

// NoteIcebergColdRemoved queues cold objects this node removed for the next
// notice, proposed coldNoticeDelay after the first: one Raft entry for a
// burst, never one per file. Never blocks or fails the caller; a failed
// notice is logged, and the export then sees the removal when its cached
// cold listing expires (iceberg.cold_rescan_interval).
func (c *Coordinator) NoteIcebergColdRemoved(paths []string) {
	if c == nil || c.raftNode == nil {
		return
	}
	n := &c.iceberg
	n.coldMu.Lock()
	defer n.coldMu.Unlock()
	if n.coldPending == nil {
		n.coldPending = make(map[string]bool)
	}
	for _, p := range paths {
		if k := measurementDirOf(p); k != "" {
			n.coldPending[k] = true
		}
	}
	if !n.coldArmed && len(n.coldPending) > 0 {
		n.coldArmed = true
		time.AfterFunc(coldNoticeDelay, c.flushColdNotice)
	}
}

func (c *Coordinator) flushColdNotice() {
	n := &c.iceberg
	n.coldMu.Lock()
	keys := make([]string, 0, len(n.coldPending))
	for k := range n.coldPending {
		keys = append(keys, k)
	}
	clear(n.coldPending)
	n.coldArmed = false
	n.coldMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), coldNoticeTimeout)
	defer cancel()
	if err := c.ProposeIcebergColdRemoved(ctx, keys); err != nil {
		c.logger.Warn().Err(err).Int("measurements", len(keys)).
			Msg("Could not tell the Iceberg exporter about removed cold objects; it sees them when its cached cold listing expires (iceberg.cold_rescan_interval)")
	}
}

// ProposeIcebergColdRemoved tells every node's Iceberg export that these cold
// objects are gone (#1728). Proposed on a writer after the deletes, off the
// removal's path (flushColdNotice): errors are for the caller to log, never
// to fail the removal. Does nothing without Raft, and until every node and
// Raft server advertises CapabilityIcebergNotice. A writer cannot tell
// whether any node exports, so a cluster without Iceberg still gets one
// small entry per burst.
func (c *Coordinator) ProposeIcebergColdRemoved(ctx context.Context, paths []string) error {
	if c == nil || c.raftNode == nil || len(paths) == 0 {
		return nil
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return nil
	}
	if _, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityIcebergNotice); len(blockers) > 0 {
		// "raft configuration unavailable" is a transient blocker, not an
		// older build: not worth the once-only log.
		if !(len(blockers) == 1 && blockers[0] == "raft configuration unavailable") &&
			c.iceberg.blockersLogged.CompareAndSwap(false, true) {
			c.logger.Info().Strs("nodes", blockers).
				Msg("Iceberg cold-removal notices are off until every node runs a build that applies them; meanwhile the export sees cold removals when its cached cold listing expires (iceberg.cold_rescan_interval)")
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
