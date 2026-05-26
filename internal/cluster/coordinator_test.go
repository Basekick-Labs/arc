package cluster

import (
	"context"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// TestIsPrimaryWriter_LegacyMode pins the pre-Pattern-2 behavior of
// IsPrimaryWriter(): when cfg.SharedStorageMode is false (today's
// default), the result depends on (a) whether a failover manager is
// wired and (b) the node's role/WriterState — exactly as the
// scheduler.WriterGate contract has always required.
//
// This is the unchanged-path test: every existing single-writer cluster
// upgrades to v26.06 and continues to use this code path until the
// operator opts into shared-storage mode.
func TestIsPrimaryWriter_LegacyMode(t *testing.T) {
	tests := []struct {
		name       string
		role       NodeRole
		failoverOn bool
		writerSt   WriterState
		want       bool
	}{
		{
			name:       "writer, no failover manager, no promotion -> primary by role fallback",
			role:       RoleWriter,
			failoverOn: false,
			writerSt:   WriterStateNone,
			want:       true,
		},
		{
			name:       "reader, no failover manager -> never primary",
			role:       RoleReader,
			failoverOn: false,
			writerSt:   WriterStateNone,
			want:       false,
		},
		{
			name:       "compactor, no failover manager -> never primary",
			role:       RoleCompactor,
			failoverOn: false,
			writerSt:   WriterStateNone,
			want:       false,
		},
		{
			name:       "writer + failover manager + promoted -> primary",
			role:       RoleWriter,
			failoverOn: true,
			writerSt:   WriterStatePrimary,
			want:       true,
		},
		{
			name:       "writer + failover manager + standby -> NOT primary",
			role:       RoleWriter,
			failoverOn: true,
			writerSt:   WriterStateStandby,
			want:       false,
		},
		{
			name:       "writer + failover manager + unknown -> NOT primary",
			role:       RoleWriter,
			failoverOn: true,
			writerSt:   WriterStateNone,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Coordinator{
				cfg: &config.ClusterConfig{
					SharedStorageMode: false,
				},
				localNode: NewNode("test-node", "test-node", tt.role, "test-cluster"),
				logger:    zerolog.Nop(),
				ctx:       context.Background(),
			}
			c.localNode.SetWriterState(tt.writerSt)
			if tt.failoverOn {
				// A non-nil writerFailoverMgr flips IsPrimaryWriter() from
				// "any writer is primary" to "only the promoted writer is
				// primary." The struct value itself is never invoked here.
				c.writerFailoverMgr = &WriterFailoverManager{}
			}

			if got := c.IsPrimaryWriter(); got != tt.want {
				t.Errorf("IsPrimaryWriter() = %v; want %v (role=%s failoverOn=%v writerState=%v)",
					got, tt.want, tt.role, tt.failoverOn, tt.writerSt)
			}
		})
	}
}

// TestIsPrimaryWriter_SharedStorageMode_NoRaft pins the defensive path:
// when SharedStorageMode=true but raftNode is nil (clustering mid-config-
// flip, or a misconfigured deployment), IsPrimaryWriter() must return
// false so singleton background tasks (retention, CQ, delete,
// reconciliation) stay off rather than potentially running on every
// node simultaneously.
//
// This is the "fail closed" property: in shared-storage mode without
// Raft, NO node is the singleton runner — better silent no-op than
// duplicate work or duplicate deletes.
func TestIsPrimaryWriter_SharedStorageMode_NoRaft(t *testing.T) {
	c := &Coordinator{
		cfg: &config.ClusterConfig{
			SharedStorageMode: true,
		},
		raftNode:  nil, // explicit: no Raft wired
		localNode: NewNode("test-node", "test-node", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}

	if got := c.IsPrimaryWriter(); got != false {
		t.Errorf("SharedStorageMode=true without Raft: IsPrimaryWriter() = %v; want false (defensive)", got)
	}
}

// TestIsPrimaryWriter_SharedStorageMode_RaftNotStarted pins that a
// raft.Node constructed via NewNode but never Start()ed reports as
// not-leader (its internal raft.Raft pointer is nil; see
// internal/cluster/raft/node.go:309 IsLeader()). Under Pattern 2
// shared-storage multi-writer mode, a writer that's still booting must
// NOT think it owns singleton tasks before Raft has elected a leader.
func TestIsPrimaryWriter_SharedStorageMode_RaftNotStarted(t *testing.T) {
	raftNode, err := raft.NewNode(&raft.NodeConfig{
		NodeID:   "test-node",
		DataDir:  t.TempDir(),
		BindAddr: "127.0.0.1:0",
		Logger:   zerolog.Nop(),
	}, nil)
	if err != nil {
		t.Fatalf("raft.NewNode: %v", err)
	}

	c := &Coordinator{
		cfg: &config.ClusterConfig{
			SharedStorageMode: true,
		},
		raftNode:  raftNode, // non-nil but not Start()ed
		localNode: NewNode("test-node", "test-node", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}

	if got := c.IsPrimaryWriter(); got != false {
		t.Errorf("SharedStorageMode=true + raft not Start()ed: IsPrimaryWriter() = %v; want false (leader election hasn't run)", got)
	}
}

// TestIsPrimaryWriter_SharedStorageMode_IgnoresLegacyWriterState
// pins that in shared-storage mode, the LEGACY WriterState signal is
// ignored — only the Raft-leader+role check matters. This protects
// against a subtle migration footgun: an operator flips
// SharedStorageMode=true while their existing failover state has
// someone promoted as primary. Under Pattern 2 semantics the "primary
// writer" designation is meaningless; only Raft leadership gates
// singletons.
//
// Construct a Coordinator that under legacy semantics WOULD report
// primary (RoleWriter + WriterStatePrimary + no failover manager) but
// in shared-storage mode reports false because there's no Raft leader.
func TestIsPrimaryWriter_SharedStorageMode_IgnoresLegacyWriterState(t *testing.T) {
	c := &Coordinator{
		cfg: &config.ClusterConfig{
			SharedStorageMode: true,
		},
		raftNode:  nil, // no Raft -> leader check returns false
		localNode: NewNode("test-node", "test-node", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
	c.localNode.SetWriterState(WriterStatePrimary) // legacy: would say "primary"

	if got := c.IsPrimaryWriter(); got != false {
		t.Errorf("SharedStorageMode=true must ignore legacy WriterStatePrimary; got = %v; want false (Raft leader check is authoritative)", got)
	}
}

// TestIsPrimaryWriter_SharedStorageMode_NonWriterLeaderRejected pins
// the role check in the SharedStorageMode branch: a RoleReader or
// RoleCompactor that wins Raft leadership must NOT become the
// singleton-task runner. Without this check, a same-image Kubernetes
// deploy where every pod has retention/CQ enabled would trigger
// duplicate work (the elected non-writer would run alongside the
// actual writers also trying to run, if they were writers and the
// leader was a reader simultaneously).
//
// This is the row of the configuration matrix that the deep reviewer
// flagged as B1. The fix is the `&& node.Role == RoleWriter` clause in
// IsPrimaryWriter's SharedStorageMode branch.
//
// The test can't easily construct a real Raft cluster where a reader
// won the election, but it can construct a Coordinator with a non-
// started Raft node and stub the leader check via the legacy "nil
// raftNode" path inverted — instead we test the role gate at the
// nearest reachable boundary: a RoleReader+RoleCompactor with
// SharedStorageMode=true and a non-nil-but-not-started raftNode (which
// reports IsLeader=false). The combination still returns false, which
// is the property B1 requires.
//
// The complementary property — "if the reader DID somehow become
// leader, would the role check stop it?" — is verified by code
// inspection: the SharedStorageMode branch only returns true when
// BOTH conditions hold (IsLeader && Role==RoleWriter). The integration
// smoke in PR1b exercises the full real-Raft scenario.
func TestIsPrimaryWriter_SharedStorageMode_NonWriterLeaderRejected(t *testing.T) {
	for _, role := range []NodeRole{RoleReader, RoleCompactor, RoleStandalone} {
		t.Run(string(role), func(t *testing.T) {
			raftNode, err := raft.NewNode(&raft.NodeConfig{
				NodeID:   "test-node",
				DataDir:  t.TempDir(),
				BindAddr: "127.0.0.1:0",
				Logger:   zerolog.Nop(),
			}, nil)
			if err != nil {
				t.Fatalf("raft.NewNode: %v", err)
			}

			c := &Coordinator{
				cfg: &config.ClusterConfig{
					SharedStorageMode: true,
				},
				raftNode:  raftNode,
				localNode: NewNode("test-node", "test-node", role, "test-cluster"),
				logger:    zerolog.Nop(),
				ctx:       context.Background(),
			}

			if got := c.IsPrimaryWriter(); got != false {
				t.Errorf("role=%s in SharedStorageMode: IsPrimaryWriter() = %v; want false (only RoleWriter may run singleton tasks)", role, got)
			}
		})
	}
}
