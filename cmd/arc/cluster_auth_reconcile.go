package main

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// Keeping each node's auth and RBAC SQLite in step with the cluster (#1336).
//
// The apply callbacks cover the commands this node applies while they are
// wired. They miss two things: a snapshot install, which replaces the FSM's
// maps without firing them, and everything the FSM applies at boot before the
// callbacks are wired. Either left revoked, deleted or rotated credentials,
// and withdrawn RBAC grants, valid on this node.
//
// startAuthReconcile covers both. It installs the reconciler the FSM calls at
// the end of a snapshot install, then reconciles once at boot — but only after
// the FSM has caught up with the leader. At boot the FSM holds the last local
// snapshot, up to a snapshot interval behind what SQLite already has; a
// reconcile against it would put back every token revoked since.

// authReconcileFSM is the part of *clusterraft.ClusterFSM this needs.
type authReconcileFSM interface {
	SetAuthReconciler(func(clusterraft.AuthSnapshot))
	ReconcileAuthNow() bool
}

// authReconcileCluster is the part of *cluster.Coordinator this needs.
type authReconcileCluster interface {
	SyncManifest(ctx context.Context) error
	RaftTerm() uint64
}

// authReconcileRetry is how long the boot reconcile waits between attempts
// to sync with the leader.
const authReconcileRetry = 15 * time.Second

// startAuthReconcile wires the reconcile and starts the boot one. rm may be
// nil (no RBAC manager); the RBAC tables are then left alone. The returned
// channel closes when the boot reconcile has run or ctx was cancelled first.
func startAuthReconcile(ctx context.Context, fsm authReconcileFSM, coord authReconcileCluster, db *sql.DB,
	am *auth.AuthManager, rm *auth.RBACManager, retry time.Duration, logger zerolog.Logger) <-chan struct{} {

	// The boot run passes the Raft term so a rebuilt history is detected; a
	// snapshot install passes none. Read and cleared inside the reconciler,
	// which runs with the auth callbacks held off.
	var bootTerm atomic.Uint64
	fsm.SetAuthReconciler(func(snap clusterraft.AuthSnapshot) {
		runAuthReconcile(db, am, rm, cluster.ToAuthClusterState(snap, rm != nil, bootTerm.Swap(0)), logger)
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			err := coord.SyncManifest(ctx)
			if err == nil {
				break
			}
			logger.Warn().Err(err).Dur("retry_in", retry).
				Msg("Cluster auth reconcile: not caught up with the leader yet; this node's tokens and RBAC grants are reconciled once it is")
			select {
			case <-ctx.Done():
				return
			case <-time.After(retry):
			}
		}
		if ctx.Err() != nil {
			return
		}
		bootTerm.Store(coord.RaftTerm())
		fsm.ReconcileAuthNow()
	}()
	return done
}

// runAuthReconcile reconciles and reports. It never returns an error: on the
// snapshot path a failure must not fail the install.
func runAuthReconcile(db *sql.DB, am *auth.AuthManager, rm *auth.RBACManager, st auth.ClusterAuthState, logger zerolog.Logger) {
	rep, err := auth.ReconcileClusterState(context.Background(), db, st, logger)
	// Whatever committed, the caches must not keep what came before it.
	am.InvalidateCache()
	if rm != nil {
		rm.InvalidateAllCache()
	}
	if err != nil {
		metrics.Get().RecordClusterAuthReconcile(true, 0, 0, 0)
		logger.Error().Err(err).
			Msg("Cluster auth reconcile failed: this node's tokens and RBAC grants may still differ from the cluster's until the next restart or snapshot install")
		return
	}
	var local, collisions, kept int
	ev := logger.Info()
	for table, t := range rep.Tables {
		local += t.Unreplicated
		collisions += t.Collisions
		kept += t.Protected
		ev = ev.Dict(table, zerolog.Dict().
			Int("inserted", t.Inserted).Int("updated", t.Updated).Int("deleted", t.Deleted).
			Int("adopted", t.Adopted).Int("local_only", t.Unreplicated).Int("collisions", t.Collisions).
			Int("kept_disabled", t.KeptDisabled).Int("kept", t.Protected).Int("errors", t.Errors))
	}
	metrics.Get().RecordClusterAuthReconcile(false, local, collisions, kept)
	ev.Uint64("applied_index", st.AppliedIndex).Bool("history_rebuilt", rep.HistoryRebuilt).
		Strs("delete_breaker_tripped", rep.BreakerTripped).Dur("duration", rep.Duration).
		Msg("Cluster auth reconciled with the cluster state")
	if rep.HistoryRebuilt {
		logger.Error().
			Msg("Cluster auth reconcile: this node's Raft history is older than the one its auth database last saw (Raft data rebuilt?). Its cluster tokens and grants are now treated as local-only and nothing was deleted; re-issue them through the API and clean up per operations/pre-26.06.1-token-upgrade.md")
	}
}
