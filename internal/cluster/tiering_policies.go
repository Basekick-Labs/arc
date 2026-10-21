package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/tiering"
)

// TieringPolicySource adapts the coordinator's tiering policies to
// tiering.ClusterPolicySource (#1324).
type TieringPolicySource struct{ c *Coordinator }

// NewTieringPolicySource returns the cluster's copy of the tiering policies,
// or nil without Raft (a standalone node keeps its local table).
func NewTieringPolicySource(c *Coordinator) tiering.ClusterPolicySource {
	if c == nil || c.raftNode == nil {
		return nil
	}
	return &TieringPolicySource{c: c}
}

// Seeded implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Seeded() bool { return s.c.PoliciesSeeded(raft.PolicyKindTiering) }

// Collecting implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Collecting() bool {
	return s.c.PolicyCollecting(raft.PolicyKindTiering)
}

// Get implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Get(database string) (*tiering.DatabasePolicy, bool) {
	e, ok := s.c.TieringPolicy(database)
	if !ok {
		return nil, false
	}
	p := tieringPolicyFromEntry(e)
	return &p, true
}

// List implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) List() []tiering.DatabasePolicy {
	entries := s.c.TieringPolicies()
	out := make([]tiering.DatabasePolicy, 0, len(entries))
	for _, e := range entries {
		out = append(out, tieringPolicyFromEntry(e))
	}
	return out
}

// Set implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Set(ctx context.Context, p *tiering.DatabasePolicy) error {
	return classifyPolicyError(s.c.SetTieringPolicy(ctx, p.Database, p.HotOnly, p.HotMaxAgeDays))
}

// Delete implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Delete(ctx context.Context, database string) error {
	return classifyPolicyError(s.c.DeleteTieringPolicy(ctx, database))
}

// classifyPolicyError sorts a policy write's error for the API: a missing
// policy, a policy the cluster state refused (invalid, not worth retrying),
// or anything else, which is the cluster not taking writes right now
// (retry). A refusal reaches a follower as the leader's text, so it is
// recognised by the apply-failed code and the FSM's own prefix.
func classifyPolicyError(err error) error {
	if err == nil {
		return nil
	}
	if raft.IsPolicyNotFound(err) {
		return tiering.ErrPolicyNotFound
	}
	refused := errors.Is(err, raft.ErrCommandRefused)
	var fr *ForwardRejectedError
	if errors.As(err, &fr) && fr.Code == protocol.ForwardCodeApplyFailed && strings.Contains(fr.Message, "tiering policy") {
		refused = true
	}
	if refused {
		return fmt.Errorf("%w: %v", tiering.ErrInvalidPolicy, err)
	}
	return fmt.Errorf("%w: %v", tiering.ErrClusterUnavailable, err)
}

func tieringPolicyFromEntry(e raft.TieringPolicyEntry) tiering.DatabasePolicy {
	return tiering.DatabasePolicy{
		ID: e.ID, Database: e.Database, HotOnly: e.HotOnly, HotMaxAgeDays: e.HotMaxAgeDays,
		CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt,
	}
}

// TieringLocalLister reads this node's own tiering policies for adoption,
// under the store's write lock (tiering.PolicyStore.LocalPolicies), with the
// default an unset hot_max_age_days means here.
func TieringLocalLister(store *tiering.PolicyStore, defaultHotMaxAgeDays int) LocalPolicyLister {
	return func(ctx context.Context) (json.RawMessage, error) {
		policies, err := store.LocalPolicies(ctx)
		if err != nil {
			return nil, err
		}
		rows := TieringLocalRows{DefaultHotMaxAgeDays: defaultHotMaxAgeDays, Policies: make([]raft.TieringPolicyEntry, 0, len(policies))}
		for _, p := range policies {
			rows.Policies = append(rows.Policies, raft.TieringPolicyEntry{
				Database: p.Database, HotOnly: p.HotOnly, HotMaxAgeDays: p.HotMaxAgeDays,
				CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt,
			})
		}
		return json.Marshal(rows)
	}
}

// MirrorTieringPolicies keeps store's own table equal to the cluster's tiering
// policies once they are seeded, so a backup of this node's database carries
// them (#1324). It runs on a worker: the FSM only signals it, and the worker
// rewrites the table from the FSM's current state, so bursts coalesce and the
// FSM goroutine never waits on SQLite. It mirrors once at start, which covers
// a restart and a snapshot installed before the worker existed.
func (c *Coordinator) MirrorTieringPolicies(store *tiering.PolicyStore) {
	if store == nil {
		return
	}
	c.RunPolicyMirror("tiering", func(ctx context.Context) error {
		if !c.PoliciesSeeded(raft.PolicyKindTiering) {
			return nil
		}
		entries := c.TieringPolicies()
		policies := make([]tiering.DatabasePolicy, 0, len(entries))
		for _, e := range entries {
			policies = append(policies, tieringPolicyFromEntry(e))
		}
		return store.MirrorClusterPolicies(ctx, policies)
	})
}

// RunPolicyMirror runs mirror on a worker after every policy change applied
// here (and a snapshot restore), and once at start, which covers a restart
// and a snapshot installed before the worker existed. The FSM only signals
// the worker, so bursts coalesce and the FSM goroutine never waits on SQLite.
func (c *Coordinator) RunPolicyMirror(kind string, mirror func(ctx context.Context) error) {
	fsm := c.fsmOrNil()
	if fsm == nil || mirror == nil {
		return
	}
	signal := make(chan struct{}, 1)
	notify := func() {
		select {
		case signal <- struct{}{}:
		default:
		}
	}
	fsm.AddPoliciesChangedCallback(notify)
	notify()
	ctx := c.ctxOrBackground()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-signal:
			}
			if err := mirror(ctx); err != nil {
				c.logger.Warn().Err(err).Str("kind", kind).Msg("Could not mirror the cluster's policies into this node's database; a backup taken now may carry older ones")
				// Try again without waiting for the next policy change.
				time.AfterFunc(policyMirrorRetry, notify)
			}
		}
	}()
}
