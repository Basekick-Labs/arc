package cluster

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// A row DELETE commits "register R, delete L" in one manifest batch. On a
// node with its own storage, the FSM delete callback queues the unlink of
// this node's copy of L at once, while R reaches the node only when its pull
// completes. Unlinking L first leaves the node with neither copy, and its
// reads miss every surviving row of the file until R lands (a continuous
// query in that window keeps the gap).
//
// So the unlink of a DELETE rewrite's old file waits (deferred, never
// sleeping in a worker) until a registered member of its rewrite family is
// on this node's disk at its registered size, or none is registered any
// more (R left the manifest: its rows went elsewhere or were deleted), or L
// is registered again. Reads keep counting L meanwhile: R is not here, and
// the read-side exclusion (open_compactions.go) leaves L out only once it is.
//
// A deferred unlink is re-queued when the puller lands a file of the family
// (recordPulledFileInTiering), by a sweep every deferredUnlinkSweepEvery, or
// at once if it became ready while it was being deferred. It is never
// dropped for age: unlinking early trades correct reads for disk. A stop
// leaves deferred copies on disk; the next start re-derives them from the
// stale-local journal (rederiveRewriteUnlinks).

// deferredUnlinkSweepEvery spaces the deferred set's readiness sweep (a
// variable so tests can shrink it).
var deferredUnlinkSweepEvery = 10 * time.Second

type deferredUnlink struct {
	reason string
	since  time.Time
}

// deferredUnlinks is guarded by deletePendingMu.
type deferredUnlinks struct {
	items    map[string]deferredUnlink
	byFamily map[string]map[string]struct{}
}

func (d *deferredUnlinks) add(path string, u deferredUnlink) {
	if d.items == nil {
		d.items = map[string]deferredUnlink{}
		d.byFamily = map[string]map[string]struct{}{}
	}
	d.items[path] = u
	f := storage.RewriteFamily(path)
	if d.byFamily[f] == nil {
		d.byFamily[f] = map[string]struct{}{}
	}
	d.byFamily[f][path] = struct{}{}
}

func (d *deferredUnlinks) remove(path string) (deferredUnlink, bool) {
	u, ok := d.items[path]
	if !ok {
		return u, false
	}
	delete(d.items, path)
	f := storage.RewriteFamily(path)
	if m := d.byFamily[f]; m != nil {
		delete(m, path)
		if len(m) == 0 {
			delete(d.byFamily, f)
		}
	}
	return u, true
}

// publishDeferredLocked stores the deferred gauge. Caller holds
// deletePendingMu.
func (c *Coordinator) publishDeferredLocked() {
	metrics.Get().SetClusterLocalDeleteDeferred(int64(len(c.deferred.items)))
}

// rewriteArrived reports whether the unlink of path (a DELETE rewrite's old
// file) may go ahead: a registered member of its family is on this node's
// disk at its registered size, or none is registered.
func (c *Coordinator) rewriteArrived(ctx context.Context, path string) bool {
	sibs := c.RewriteSiblings(path)
	if len(sibs) == 0 {
		return true
	}
	for _, e := range sibs {
		if n, err := c.storage.StatFile(ctx, e.Path); err == nil && n == e.SizeBytes {
			return true
		}
	}
	return false
}

// deferRewriteUnlink reports whether item's unlink must wait for its
// rewrite, and if so parks it. Only a DELETE rewrite's old file this node
// holds, while compaction commit is on (otherwise legacy compaction lists
// storage and could take the lone copy), and never while stopping.
func (c *Coordinator) deferRewriteUnlink(ctx context.Context, item deleteRequest) bool {
	if item.reason != deleteRewriteReason || c.deleteStopping.Load() || !c.StagedCompactionMode() {
		return false
	}
	if n, err := c.storage.StatFile(ctx, item.path); err != nil || n < 0 {
		return false
	}
	if c.rewriteArrived(ctx, item.path) {
		return false
	}
	c.deletePendingMu.Lock()
	since := time.Now()
	if u, ok := c.deferred.items[item.path]; ok {
		since = u.since
	}
	c.deferred.add(item.path, deferredUnlink{reason: item.reason, since: since})
	c.publishDeferredLocked()
	c.deletePendingMu.Unlock()
	// Inserted first, checked again: a rewrite that landed between the check
	// above and the insert has already fired its wake.
	if c.rewriteArrived(ctx, item.path) {
		c.requeueDeferred(item.path)
	}
	return true
}

// requeueDeferred moves path from the deferred set back to the pending list
// (the worker checks it again). Only the caller that removes it re-queues
// it, so a wake and the sweep never both do.
func (c *Coordinator) requeueDeferred(path string) {
	c.deletePendingMu.Lock()
	u, ok := c.deferred.remove(path)
	c.publishDeferredLocked()
	c.deletePendingMu.Unlock()
	if ok {
		c.enqueueLocalDelete(path, u.reason)
	}
}

// dropDeferred forgets path without unlinking it (the manifest lists it
// again).
func (c *Coordinator) dropDeferred(path string) {
	c.deletePendingMu.Lock()
	c.deferred.remove(path)
	c.publishDeferredLocked()
	c.deletePendingMu.Unlock()
}

// unlinkDeferred reports whether path's unlink is deferred.
func (c *Coordinator) unlinkDeferred(path string) bool {
	c.deletePendingMu.Lock()
	defer c.deletePendingMu.Unlock()
	_, ok := c.deferred.items[path]
	return ok
}

// wakeDeferredFamily re-queues the deferred unlinks of pulled's family that
// are ready now (the puller just landed pulled).
func (c *Coordinator) wakeDeferredFamily(pulled string) {
	c.deletePendingMu.Lock()
	var paths []string
	for p := range c.deferred.byFamily[storage.RewriteFamily(pulled)] {
		paths = append(paths, p)
	}
	c.deletePendingMu.Unlock()
	for _, p := range paths {
		if p != pulled && c.rewriteArrived(c.ctxOrBackground(), p) {
			c.requeueDeferred(p)
		}
	}
}

// sweepDeferredUnlinks re-queues every deferred unlink that is ready, or
// whose copy is gone, until stop is closed.
func (c *Coordinator) sweepDeferredUnlinks(stop <-chan struct{}) {
	t := time.NewTicker(deferredUnlinkSweepEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		c.deletePendingMu.Lock()
		paths := make([]string, 0, len(c.deferred.items))
		for p := range c.deferred.items {
			paths = append(paths, p)
		}
		c.deletePendingMu.Unlock()
		ctx := c.ctxOrBackground()
		for _, p := range paths {
			n, err := c.storage.StatFile(ctx, p)
			if (err == nil && n < 0) || c.rewriteArrived(ctx, p) {
				c.requeueDeferred(p)
			}
		}
	}
}

// rederiveRewriteUnlinks: after a restart the deferred set is empty, but the
// copies it held are still on disk. Every copy this node saw registered,
// that the manifest no longer lists and that has a registered rewrite
// sibling, is a DELETE rewrite's old file whose unlink never ran: track it
// for reads and queue its unlink (deferred again if the rewrite is still not
// here).
func (c *Coordinator) rederiveRewriteUnlinks(seen *seenLocal) {
	if seen == nil || c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return
	}
	n := 0
	for p := range seen.unlisted(fsm.HasFile) {
		if !fsm.HasRewriteSibling(p) {
			continue
		}
		if _, ok := localIdentity(c.stalePath(p)); !ok {
			continue
		}
		c.noteLocalDeleted(p, deleteRewriteReason)
		c.enqueueLocalDelete(p, deleteRewriteReason)
		n++
	}
	if n > 0 {
		c.logger.Info().Int("files", n).Msg("Queued the unlinks of files DELETE rewrites replaced that a previous run left on disk")
	}
}
