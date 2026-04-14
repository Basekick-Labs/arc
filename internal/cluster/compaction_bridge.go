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
//   2. A refactor of the Raft FSM FileEntry schema doesn't ripple into
//      the compaction package; it only touches this adapter.
//
// Phase 4 + leader forwarding: the bridge no longer has its own
// IsLeader() short-circuit. The underlying Coordinator.RegisterFileInManifest
// and DeleteFileFromManifest now forward to the current Raft leader over
// the peer protocol when the local node is not leader (see forward_apply.go).
// The bridge therefore just calls them and trusts the result.
//
// Earlier Phase 4 designs returned compaction.ErrNotLeader from the
// bridge so the watcher would keep the manifest on disk and retry later.
// That worked for the compactor=leader topology but broke whenever the
// compactor was permanently a non-leader (the actual production shape,
// since RoleWriter typically owns Raft bootstrap). Forwarding fixes both
// topologies and also closes the Phase 1 silent-skip blind spot.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/compaction"
)

// bridgeCoordinator is the narrow interface the CompactionBridge needs from
// the full Coordinator. The bridge no longer touches IsLeader() — leader
// resolution and forwarding live entirely inside RegisterFileInManifest /
// DeleteFileFromManifest after the Phase 4 leader-forwarding refactor.
//
// LocalNodeID is still required so the bridge can stamp OriginNodeID on
// the FileEntry, ensuring Phase 2/3's multi-peer resolver routes replica
// pulls back to the compactor that produced the output.
type bridgeCoordinator interface {
	LocalNodeID() string
	RegisterFileInManifest(file raft.FileEntry) error
	DeleteFileFromManifest(path, reason string) error
	BatchFileOpsInManifest(ops []raft.BatchFileOp) error
}

// CompactionBridge adapts a bridgeCoordinator to compaction.ManifestBridge.
type CompactionBridge struct {
	coord bridgeCoordinator
}

// NewCompactionBridge constructs a bridge bound to the given coordinator.
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

// RegisterCompactedFile appends a CommandRegisterFile to the Raft log via
// the underlying coordinator. On the leader the command is applied
// directly; on a non-leader the coordinator forwards over the peer
// protocol to the current leader (see forward_apply.go). Either way the
// caller observes a successful return only after Raft has committed.
//
// The bridge translates the cluster-agnostic compaction.CompactedFile
// shape into raft.FileEntry, stamping OriginNodeID with the local node ID
// so Phase 2/3 readers know to pull the bytes from this compactor.
func (b *CompactionBridge) RegisterCompactedFile(ctx context.Context, file compaction.CompactedFile) error {
	if err := checkContextDeadline(ctx, "register compacted file"); err != nil {
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
		// Map known leader-resolution errors to ErrNotLeader so the
		// watcher recognizes them as transient retry conditions and
		// keeps the completion manifest on disk for the next poll.
		if isTransientLeaderError(err) {
			return fmt.Errorf("register compacted file: %w", compaction.ErrNotLeader)
		}
		return fmt.Errorf("register compacted file: %w", err)
	}
	return nil
}

// DeleteCompactedSource appends a CommandDeleteFile to the Raft log via
// the underlying coordinator. Same forwarding semantics as
// RegisterCompactedFile: leader applies directly, non-leader forwards.
//
// reason is a human-readable string for operator debugging; Phase 4
// always passes "compaction:<job_id>" so operators can correlate deletions
// with the compaction job that drove them.
func (b *CompactionBridge) DeleteCompactedSource(ctx context.Context, path, reason string) error {
	if err := checkContextDeadline(ctx, "delete compacted source"); err != nil {
		return err
	}

	if err := b.coord.DeleteFileFromManifest(path, reason); err != nil {
		if isTransientLeaderError(err) {
			return fmt.Errorf("delete compacted source: %w", compaction.ErrNotLeader)
		}
		return fmt.Errorf("delete compacted source: %w", err)
	}
	return nil
}

// checkContextDeadline rejects calls whose context has already expired
// or been cancelled before we touch the coordinator. Catches both
// DeadlineExceeded and Canceled, so a stuck Raft Apply can't extend the
// caller's budget by even one round-trip.
func checkContextDeadline(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

// isTransientLeaderError reports whether an error from RegisterFileInManifest
// or DeleteFileFromManifest indicates a leader-resolution failure that the
// caller should retry. We surface these as compaction.ErrNotLeader so the
// watcher keeps the completion manifest on disk for the next poll cycle.
//
// The two known transient causes (defined in forward_apply.go):
//   - ErrNoLeaderKnown: Raft hasn't observed a leader yet (election in
//     progress, or freshly-joined node before first heartbeat).
//   - ErrLeaderUnreachable: leader ID is known but the registry doesn't
//     have a coordinator address for it (mid-flight registry update).
//
// All other errors (auth failure, apply rejection, marshal bug) bubble up
// as-is so operators see the actual cause in logs.
func isTransientLeaderError(err error) bool {
	return errors.Is(err, ErrNoLeaderKnown) || errors.Is(err, ErrLeaderUnreachable)
}

// BatchFileOps groups all register and delete operations for one compaction
// manifest into a single Raft log entry via the underlying coordinator.
// Reduces Raft traffic from O(N) applies to 1 per manifest.
func (b *CompactionBridge) BatchFileOps(ctx context.Context, registers []compaction.CompactedFile, deletes []compaction.DeleteSourceOp) error {
	if err := checkContextDeadline(ctx, "batch file ops"); err != nil {
		return err
	}

	ops := make([]raft.BatchFileOp, 0, len(registers)+len(deletes))

	for _, file := range registers {
		payload, err := json.Marshal(raft.RegisterFilePayload{File: raft.FileEntry{
			Path:          file.Path,
			SHA256:        file.SHA256,
			SizeBytes:     file.SizeBytes,
			Database:      file.Database,
			Measurement:   file.Measurement,
			PartitionTime: file.PartitionTime,
			OriginNodeID:  b.coord.LocalNodeID(),
			Tier:          file.Tier,
			CreatedAt:     file.CreatedAt,
		}})
		if err != nil {
			return fmt.Errorf("batch file ops: marshal register payload: %w", err)
		}
		ops = append(ops, raft.BatchFileOp{Type: raft.CommandRegisterFile, Payload: payload})
	}

	for _, del := range deletes {
		payload, err := json.Marshal(raft.DeleteFilePayload{Path: del.Path, Reason: del.Reason})
		if err != nil {
			return fmt.Errorf("batch file ops: marshal delete payload: %w", err)
		}
		ops = append(ops, raft.BatchFileOp{Type: raft.CommandDeleteFile, Payload: payload})
	}

	if len(ops) == 0 {
		return nil
	}

	if err := b.coord.BatchFileOpsInManifest(ops); err != nil {
		if isTransientLeaderError(err) {
			return fmt.Errorf("batch file ops: %w", compaction.ErrNotLeader)
		}
		return fmt.Errorf("batch file ops: %w", err)
	}
	return nil
}

// Compile-time assertion that CompactionBridge implements the interface.
// If the interface shape ever drifts, this fails at build time, not at
// first watcher tick in production.
var _ compaction.ManifestBridge = (*CompactionBridge)(nil)
