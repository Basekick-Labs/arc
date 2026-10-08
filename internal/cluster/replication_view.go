package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/replicaview"
)

// SetReplicationStore is called before Start, including before Raft restore.
// FSM callbacks never enter the store; disk verification runs on pull workers
// and on the refresh loop, outside coordinator and FSM locks.
func (c *Coordinator) SetReplicationStore(store *replicaview.LocalStore) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replicationStore = store
	c.replicationViewRevision = ^uint64(0)
}

func canonicalFile(entry raft.FileEntry) replicaview.Canonical {
	return replicaview.Canonical{Path: entry.Path, SHA256: entry.SHA256, SizeBytes: entry.SizeBytes, Database: entry.Database, Measurement: entry.Measurement, Hour: entry.PartitionTime.Unix() / 3600, Partitions: entry.WALCoverage, Replaces: entry.Replaces}
}

// RefreshReplicationView is also called before a query takes its first source
// snapshot. Once startup has synchronized with the leader, an unchanged FSM
// revision costs only a lock/read; no manifest allocation or filesystem walk.
func (c *Coordinator) RefreshReplicationView(ctx context.Context) error {
	c.replicationViewMu.Lock()
	defer c.replicationViewMu.Unlock()
	c.mu.RLock()
	store, node := c.replicationStore, c.raftNode
	c.mu.RUnlock()
	if store == nil {
		return nil
	}
	if node == nil || !c.replicationViewSynced.Load() {
		return replicaview.ErrManifestNotReady
	}
	fsm := node.FSM()
	if fsm == nil {
		return replicaview.ErrManifestNotReady
	}
	revision, files, retired, changed := fsm.ReplicationState(c.replicationViewRevision)
	if !changed {
		return nil
	}
	desired := make([]replicaview.Canonical, len(files))
	for i, entry := range files {
		desired[i] = canonicalFile(entry)
	}
	retirements := make([]replicaview.Retirement, len(retired))
	for i, r := range retired {
		retirements[i] = replicaview.Retirement{Database: r.Database, Measurement: r.Measurement, Hour: r.Hour, Coverage: r.Coverage}
	}
	if err := store.Reconcile(ctx, desired, retirements); err != nil {
		return err
	}
	c.replicationViewRevision = revision
	return nil
}

func (c *Coordinator) runReplicationView(ctx context.Context, node *raft.Node, store *replicaview.LocalStore, done chan struct{}) {
	defer close(done)
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for {
		if !c.replicationViewSynced.Load() {
			// Stale local Raft state is not enough to decide that an offline reader's
			// old WAL rows are live. Keep its query view closed until the leader's
			// committed deletion history has reached this node.
			if err := c.waitForManifestSync(ctx, node, 5*time.Second); err == nil {
				c.replicationViewSynced.Store(true)
			}
		}
		if c.replicationViewSynced.Load() {
			if err := c.RefreshReplicationView(ctx); err != nil {
				c.logger.Warn().Err(err).Msg("Replica view reconciliation deferred")
			} else if err := store.Collect(ctx); err != nil {
				c.logger.Warn().Err(err).Msg("Replica file cleanup deferred")
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (c *Coordinator) publishPulledCanonical(ctx context.Context, entry raft.FileEntry) error {
	// This callback is only installed when a store was configured before Start.
	if c.replicationStore == nil {
		return fmt.Errorf("replication publication store is unavailable")
	}
	return c.replicationStore.PublishCanonical(ctx, canonicalFile(entry))
}

// WaitForReplicationView gates startup replay on the leader's committed
// retirement history. Local Raft restore alone is insufficient after downtime.
func (c *Coordinator) WaitForReplicationView(ctx context.Context) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := c.RefreshReplicationView(ctx)
		if !errors.Is(err, replicaview.ErrManifestNotReady) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
