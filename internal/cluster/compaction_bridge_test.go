package cluster

// Unit tests for CompactionBridge. These drive the bridge's mapping logic
// (CompactedFile → raft.FileEntry, OriginNodeID stamping, error mapping)
// against a stub bridgeCoordinator. The bridge is now a thin adapter —
// leader resolution and forwarding live in the underlying coordinator
// (forward_apply.go), so these tests focus on the translation layer.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/compaction"
)

// stubBridgeCoordinator is a minimal implementation of bridgeCoordinator
// that records calls and returns scripted errors. The bridge no longer
// touches IsLeader() — the underlying coordinator forwards or applies
// internally — so the stub only needs the three methods on the post-Phase-4
// interface: LocalNodeID, RegisterFileInManifest, DeleteFileFromManifest.
type stubBridgeCoordinator struct {
	nodeID        string
	registerErr   error
	deleteErr     error
	registered    []raft.FileEntry
	deleted       []struct{ Path, Reason string }
	registerCalls int
	deleteCalls   int
}

func (s *stubBridgeCoordinator) LocalNodeID() string { return s.nodeID }
func (s *stubBridgeCoordinator) RegisterFileInManifest(file raft.FileEntry) error {
	s.registerCalls++
	s.registered = append(s.registered, file)
	return s.registerErr
}
func (s *stubBridgeCoordinator) DeleteFileFromManifest(path, reason string) error {
	s.deleteCalls++
	s.deleted = append(s.deleted, struct{ Path, Reason string }{path, reason})
	return s.deleteErr
}

// --- RegisterCompactedFile ---

// TestCompactionBridge_RegisterMapsNoLeaderKnownToErrNotLeader documents
// the post-Phase-4 contract: when the underlying coordinator can't resolve
// a leader (e.g. mid-election), the bridge maps the error to
// compaction.ErrNotLeader so the watcher recognizes it as a transient
// retry and keeps the manifest on disk.
func TestCompactionBridge_RegisterMapsNoLeaderKnownToErrNotLeader(t *testing.T) {
	stub := &stubBridgeCoordinator{
		nodeID:      "compactor-1",
		registerErr: ErrNoLeaderKnown,
	}
	bridge := NewCompactionBridge(stub)

	err := bridge.RegisterCompactedFile(context.Background(), compaction.CompactedFile{
		Path:      "db/cpu/compacted.parquet",
		SHA256:    "abc",
		SizeBytes: 100,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, compaction.ErrNotLeader) {
		t.Errorf("expected ErrNotLeader (transient leader-resolution failure), got %v", err)
	}
	// Coordinator was still called — the bridge no longer short-circuits
	// before calling the underlying RegisterFileInManifest.
	if stub.registerCalls != 1 {
		t.Errorf("registerCalls: got %d, want 1 (bridge must call coordinator and let it forward)", stub.registerCalls)
	}
}

// TestCompactionBridge_RegisterMapsLeaderUnreachableToErrNotLeader: same
// reasoning, different transient cause (leader ID known but registry
// doesn't have an address for it).
func TestCompactionBridge_RegisterMapsLeaderUnreachableToErrNotLeader(t *testing.T) {
	stub := &stubBridgeCoordinator{
		nodeID:      "compactor-1",
		registerErr: ErrLeaderUnreachable,
	}
	bridge := NewCompactionBridge(stub)

	err := bridge.RegisterCompactedFile(context.Background(), compaction.CompactedFile{Path: "x.parquet"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, compaction.ErrNotLeader) {
		t.Errorf("expected ErrNotLeader, got %v", err)
	}
}

// TestCompactionBridge_RegisterSuccessAsLeader verifies the happy path.
// The stub returns nil from RegisterFileInManifest, mimicking the leader
// successfully applying via Raft.
func TestCompactionBridge_RegisterSuccessAsLeader(t *testing.T) {
	stub := &stubBridgeCoordinator{nodeID: "compactor-1"}
	bridge := NewCompactionBridge(stub)

	file := compaction.CompactedFile{
		Path:          "db/cpu/2026/04/11/14/compacted.parquet",
		SHA256:        "deadbeef",
		SizeBytes:     2048,
		Database:      "db",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		Tier:          "hot",
		CreatedAt:     time.Date(2026, 4, 11, 14, 30, 0, 0, time.UTC),
	}
	if err := bridge.RegisterCompactedFile(context.Background(), file); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if stub.registerCalls != 1 {
		t.Fatalf("registerCalls: got %d, want 1", stub.registerCalls)
	}
	got := stub.registered[0]
	if got.Path != file.Path {
		t.Errorf("Path: got %q, want %q", got.Path, file.Path)
	}
	if got.SHA256 != file.SHA256 {
		t.Errorf("SHA256: got %q, want %q", got.SHA256, file.SHA256)
	}
	if got.SizeBytes != file.SizeBytes {
		t.Errorf("SizeBytes: got %d, want %d", got.SizeBytes, file.SizeBytes)
	}
	if got.OriginNodeID != "compactor-1" {
		t.Errorf("OriginNodeID: got %q, want compactor-1 (bridge must stamp local node ID)", got.OriginNodeID)
	}
	if !got.PartitionTime.Equal(file.PartitionTime) {
		t.Errorf("PartitionTime: got %v, want %v", got.PartitionTime, file.PartitionTime)
	}
	if got.Tier != file.Tier {
		t.Errorf("Tier: got %q, want %q", got.Tier, file.Tier)
	}
}

// TestCompactionBridge_RegisterUnderlyingErrorSurfaces: a non-transient
// error from the coordinator (e.g. raft.Apply timeout, marshal failure)
// must NOT be mapped to ErrNotLeader — operators need to see the real
// cause in logs.
func TestCompactionBridge_RegisterUnderlyingErrorSurfaces(t *testing.T) {
	stub := &stubBridgeCoordinator{
		nodeID:      "c1",
		registerErr: errors.New("raft timeout"),
	}
	bridge := NewCompactionBridge(stub)

	err := bridge.RegisterCompactedFile(context.Background(), compaction.CompactedFile{Path: "x.parquet"})
	if err == nil {
		t.Fatal("expected error from underlying coordinator, got nil")
	}
	if errors.Is(err, compaction.ErrNotLeader) {
		t.Errorf("raft timeout must NOT map to ErrNotLeader")
	}
}

// TestCompactionBridge_RegisterRespectsDeadline: an already-expired
// context fails fast before touching the coordinator. Prevents a stuck
// Raft Apply from extending the caller's deadline budget.
func TestCompactionBridge_RegisterRespectsDeadline(t *testing.T) {
	stub := &stubBridgeCoordinator{nodeID: "c1"}
	bridge := NewCompactionBridge(stub)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	defer cancel()
	err := bridge.RegisterCompactedFile(ctx, compaction.CompactedFile{Path: "x.parquet"})
	if err == nil {
		t.Fatal("expected deadline error, got nil")
	}
	if stub.registerCalls != 0 {
		t.Errorf("registerCalls: got %d, want 0 (expired ctx must short-circuit)", stub.registerCalls)
	}
}

// --- DeleteCompactedSource ---

// Symmetric mapping test for the delete path.
func TestCompactionBridge_DeleteMapsNoLeaderKnownToErrNotLeader(t *testing.T) {
	stub := &stubBridgeCoordinator{
		nodeID:    "c1",
		deleteErr: ErrNoLeaderKnown,
	}
	bridge := NewCompactionBridge(stub)

	err := bridge.DeleteCompactedSource(context.Background(), "src.parquet", "compaction:job-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, compaction.ErrNotLeader) {
		t.Errorf("expected ErrNotLeader, got %v", err)
	}
	if stub.deleteCalls != 1 {
		t.Errorf("deleteCalls: got %d, want 1 (bridge calls coordinator before mapping)", stub.deleteCalls)
	}
}

func TestCompactionBridge_DeleteSuccessAsLeader(t *testing.T) {
	stub := &stubBridgeCoordinator{nodeID: "c1"}
	bridge := NewCompactionBridge(stub)

	err := bridge.DeleteCompactedSource(context.Background(), "src.parquet", "compaction:job-42")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if stub.deleteCalls != 1 {
		t.Fatalf("deleteCalls: got %d, want 1", stub.deleteCalls)
	}
	if stub.deleted[0].Path != "src.parquet" {
		t.Errorf("Path: got %q, want src.parquet", stub.deleted[0].Path)
	}
	if stub.deleted[0].Reason != "compaction:job-42" {
		t.Errorf("Reason: got %q, want compaction:job-42", stub.deleted[0].Reason)
	}
}

func TestCompactionBridge_DeleteUnderlyingErrorSurfaces(t *testing.T) {
	stub := &stubBridgeCoordinator{
		nodeID:    "c1",
		deleteErr: errors.New("raft not started"),
	}
	bridge := NewCompactionBridge(stub)

	err := bridge.DeleteCompactedSource(context.Background(), "x.parquet", "test")
	if err == nil {
		t.Fatal("expected error from underlying coordinator, got nil")
	}
	if errors.Is(err, compaction.ErrNotLeader) {
		t.Errorf("raft-not-started must NOT map to ErrNotLeader")
	}
}

// TestCompactionBridge_NilCoordinatorPanicsAtConstruction documents the
// post-review contract: nil coordinator is a programming error, not a
// runtime condition. The constructor panics loudly so the bug is caught
// at startup, not on the first bridge call.
func TestCompactionBridge_NilCoordinatorPanicsAtConstruction(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewCompactionBridge(nil) should panic, got no panic")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value should be string, got %T: %v", r, r)
		}
		if !strings.Contains(msg, "must not be nil") {
			t.Errorf("panic message should mention nil contract, got: %s", msg)
		}
	}()
	NewCompactionBridge(nil)
}
