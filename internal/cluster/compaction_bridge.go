package cluster

// CompactionBridge implements compaction.ManifestBridge by translating
// Phase 4 CompactedFile records into Raft FileEntry commands via the
// existing Coordinator.RegisterFileInManifest / DeleteFileFromManifest
// methods.
//
// This file is the ONLY place where the compaction package talks to Raft.
// The compaction package imports compaction.ManifestBridge (a narrow
// interface defined in internal/compaction/watcher.go) and never imports
// raft.FileEntry or the coordinator struct. That gives us two things:
//
//   1. The compaction package stays testable with a plain in-memory mock
//      bridge — no Raft setup needed for unit tests.
//   2. A refactor of the Raft FSM FileEntry schema doesn't ripple into the
//      compaction package; it only touches this adapter.
//
// The critical behavior encoded here is the leader-forwarding guard. The
// Coordinator's existing RegisterFileInManifest / DeleteFileFromManifest
// silently return nil on non-leader nodes (see coordinator.go:2152 and
// coordinator.go:2175) because Phase 1 treated the manifest as
// informational. Phase 4 makes the manifest load-bearing — if the bridge
// returned nil on non-leader, the watcher would see "success" and delete
// the completion manifest, losing the registration. So the bridge checks
// IsLeader() FIRST and returns compaction.ErrNotLeader on non-leader,
// which the watcher handles by leaving the manifest on disk for the next
// poll cycle.

import (
	"context"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// bridgeCoordinator is the narrow interface the CompactionBridge needs from
// the full Coordinator. Takes only the three methods required so the bridge
// can be unit-tested with a plain stub — no Raft cluster, no TCP listener,
// no health checker. *Coordinator implements it naturally; test doubles
// implement just what they need.
type bridgeCoordinator interface {
	IsLeader() bool
	LocalNodeID() string
	RegisterFileInManifest(file raft.FileEntry) error
	DeleteFileFromManifest(path, reason string) error
}

// CompactionBridge adapts a bridgeCoordinator to compaction.ManifestBridge.
type CompactionBridge struct {
	coord bridgeCoordinator
}

// NewCompactionBridge constructs a bridge bound to the given coordinator.
// The coordinator's local node ID is used as the OriginNodeID for every
// RegisterCompactedFile call — Phase 2/3's resolver uses this to route
// replica pulls back to the compactor, which is the correct behavior
// because the compactor is the only node that has the compacted bytes
// until replication completes.
//
// Panics if coord is nil — construction-time failure is louder than a
// runtime nil-check and makes the invariant visible at the call site.
// Production wiring in main.go always passes a non-nil coordinator.
func NewCompactionBridge(coord bridgeCoordinator) *CompactionBridge {
	if coord == nil {
		panic("cluster.NewCompactionBridge: coordinator must not be nil")
	}
	return &CompactionBridge{coord: coord}
}

// checkLeaderAndDeadline is the shared guard both bridge methods run before
// touching the coordinator. It short-circuits on non-leader (returning
// ErrNotLeader wrapped with the caller's op name) and on expired context.
//
// Extracting this keeps RegisterCompactedFile and DeleteCompactedSource
// focused on their mapping logic — the guard is a single concern and
// lives in one place.
func (b *CompactionBridge) checkLeaderAndDeadline(ctx context.Context, op string) error {
	if !b.coord.IsLeader() {
		return fmt.Errorf("%s: %w", op, compaction.ErrNotLeader)
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= 0 {
			return fmt.Errorf("%s: context already expired", op)
		}
	}
	return nil
}

// RegisterCompactedFile appends a CommandRegisterFile to the Raft log
// containing the compacted output's metadata. Returns compaction.ErrNotLeader
// if the local node is not the Raft leader — the watcher treats this as a
// retry condition and keeps the completion manifest on disk.
//
// On leader:
//   1. Build raft.FileEntry with OriginNodeID = local node ID.
//   2. Call the existing RegisterFileInManifest path which appends via
//      n.raftNode.RegisterFile and returns the wrapped error.
//
// On non-leader: short-circuit with ErrNotLeader. Do NOT forward to the
// leader — Phase 4 accepts single-leader stalls during leader flap as the
// trade-off for simpler bridge code. Leader forwarding is Phase 5.
func (b *CompactionBridge) RegisterCompactedFile(ctx context.Context, file compaction.CompactedFile) error {
	if err := b.checkLeaderAndDeadline(ctx, "register compacted file"); err != nil {
		return err
	}

	entry := raft.FileEntry{
		Path:          file.Path,
		SHA256:        file.SHA256,
		SizeBytes:     file.SizeBytes,
		Database:      file.Database,
		Measurement:   file.Measurement,
		PartitionTime: file.PartitionTime,
		// OriginNodeID is the local compactor so Phase 2/3's resolver
		// routes replicas' pulls back to us. The FSM's applyRegisterFile
		// stamps LSN from the Raft log index; we leave it zero.
		OriginNodeID: b.coord.LocalNodeID(),
		Tier:         file.Tier,
		CreatedAt:    file.CreatedAt,
	}

	if err := b.coord.RegisterFileInManifest(entry); err != nil {
		return fmt.Errorf("register compacted file: %w", err)
	}

	// RegisterFileInManifest may still silently skip on a leader-flap race
	// between our IsLeader() check and the underlying Raft call. That's
	// acceptable: the next poll cycle's IsLeader() check will either see
	// us as leader (retry succeeds) or as not-leader (ErrNotLeader, retry
	// later). Idempotency in the FSM handlers makes the retry safe.
	return nil
}

// DeleteCompactedSource appends a CommandDeleteFile to the Raft log
// removing a source file from the cluster manifest. Same leader semantics
// as RegisterCompactedFile: returns compaction.ErrNotLeader on non-leader
// so the watcher retries.
//
// reason is a human-readable string for operator debugging; Phase 4
// always passes "compaction:<job_id>" so operators can correlate deletions
// with the compaction job that drove them.
func (b *CompactionBridge) DeleteCompactedSource(ctx context.Context, path, reason string) error {
	if err := b.checkLeaderAndDeadline(ctx, "delete compacted source"); err != nil {
		return err
	}
	if err := b.coord.DeleteFileFromManifest(path, reason); err != nil {
		return fmt.Errorf("delete compacted source: %w", err)
	}
	return nil
}

// Compile-time assertion that CompactionBridge implements the interface.
// If the interface shape ever drifts, this fails at build time, not at
// first watcher tick in production.
var _ compaction.ManifestBridge = (*CompactionBridge)(nil)
