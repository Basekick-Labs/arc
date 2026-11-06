package main

import (
	"context"
	"fmt"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

func reconcileLicenseAuth(ctx context.Context, fsm *raft.ClusterFSM, term uint64, am *auth.AuthManager, rm *auth.RBACManager, logger zerolog.Logger) error {
	return fsm.ReconcileAuthChecked(func(snapshot raft.AuthSnapshot) error {
		report, err := auth.ReconcileClusterState(ctx, am.GetDB(), cluster.ToAuthClusterState(snapshot, rm != nil, term), logger)
		am.InvalidateCache()
		if rm != nil {
			rm.InvalidateAllCache()
		}
		if err != nil {
			return err
		}
		if report == nil || report.Errors() > 0 || report.HistoryRebuilt || len(report.BreakerTripped) > 0 {
			return fmt.Errorf("auth/RBAC reconciliation incomplete; license handoff remains fenced")
		}
		for _, table := range report.Tables {
			if table.Collisions > 0 || table.Protected > 0 {
				return fmt.Errorf("auth/RBAC reconciliation has unresolved rows; license handoff remains fenced")
			}
		}
		return nil
	})
}
