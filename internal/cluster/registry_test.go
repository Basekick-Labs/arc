package cluster

import (
	"testing"

	"github.com/rs/zerolog"
)

func newTestRegistry() *Registry {
	return NewRegistry(&RegistryConfig{
		Logger: zerolog.Nop(),
	})
}

func newTestRegistryWithLocal() (*Registry, *Node) {
	localNode := NewNode("local-1", "Local Node", RoleWriter, "test-cluster")
	localNode.UpdateState(StateHealthy)

	registry := NewRegistry(&RegistryConfig{
		LocalNode: localNode,
		Logger:    zerolog.Nop(),
	})

	return registry, localNode
}

func TestRegistryRegisterAndGet(t *testing.T) {
	registry := newTestRegistry()

	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")
	node.UpdateState(StateHealthy)

	registry.Register(node)

	got, exists := registry.Get("node-1")
	if !exists {
		t.Fatal("Node should exist after registration")
	}
	if got.ID != "node-1" {
		t.Errorf("Get().ID = %v, want node-1", got.ID)
	}

	// Non-existent node
	_, exists = registry.Get("non-existent")
	if exists {
		t.Error("Non-existent node should not exist")
	}
}

func TestRegistryUnregister(t *testing.T) {
	registry := newTestRegistry()

	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")
	registry.Register(node)

	// Verify node exists
	if _, exists := registry.Get("node-1"); !exists {
		t.Fatal("Node should exist after registration")
	}

	// Unregister
	registry.Unregister("node-1")

	// Verify node is gone
	if _, exists := registry.Get("node-1"); exists {
		t.Error("Node should not exist after unregistration")
	}

	// Unregistering non-existent node should not panic
	registry.Unregister("non-existent")
}

func TestRegistryGetAll(t *testing.T) {
	registry := newTestRegistry()

	// Empty registry
	if nodes := registry.GetAll(); len(nodes) != 0 {
		t.Errorf("Empty registry should return 0 nodes, got %d", len(nodes))
	}

	// Add nodes
	for i := 1; i <= 3; i++ {
		node := NewNode("node-"+string(rune('0'+i)), "Node", RoleWriter, "test-cluster")
		registry.Register(node)
	}

	if nodes := registry.GetAll(); len(nodes) != 3 {
		t.Errorf("GetAll() returned %d nodes, want 3", len(nodes))
	}
}

func TestRegistryGetByRole(t *testing.T) {
	registry := newTestRegistry()

	// Add nodes with different roles
	writer1 := NewNode("writer-1", "Writer 1", RoleWriter, "test-cluster")
	writer2 := NewNode("writer-2", "Writer 2", RoleWriter, "test-cluster")
	reader1 := NewNode("reader-1", "Reader 1", RoleReader, "test-cluster")
	compactor1 := NewNode("compactor-1", "Compactor 1", RoleCompactor, "test-cluster")

	registry.Register(writer1)
	registry.Register(writer2)
	registry.Register(reader1)
	registry.Register(compactor1)

	writers := registry.GetByRole(RoleWriter)
	if len(writers) != 2 {
		t.Errorf("GetByRole(Writer) returned %d nodes, want 2", len(writers))
	}

	readers := registry.GetByRole(RoleReader)
	if len(readers) != 1 {
		t.Errorf("GetByRole(Reader) returned %d nodes, want 1", len(readers))
	}

	compactors := registry.GetByRole(RoleCompactor)
	if len(compactors) != 1 {
		t.Errorf("GetByRole(Compactor) returned %d nodes, want 1", len(compactors))
	}

	standalone := registry.GetByRole(RoleStandalone)
	if len(standalone) != 0 {
		t.Errorf("GetByRole(Standalone) returned %d nodes, want 0", len(standalone))
	}
}

func TestRegistryGetByState(t *testing.T) {
	registry := newTestRegistry()

	healthy := NewNode("healthy-1", "Healthy", RoleWriter, "test-cluster")
	healthy.UpdateState(StateHealthy)

	unhealthy := NewNode("unhealthy-1", "Unhealthy", RoleWriter, "test-cluster")
	unhealthy.UpdateState(StateUnhealthy)

	registry.Register(healthy)
	registry.Register(unhealthy)

	healthyNodes := registry.GetByState(StateHealthy)
	if len(healthyNodes) != 1 {
		t.Errorf("GetByState(Healthy) returned %d nodes, want 1", len(healthyNodes))
	}

	unhealthyNodes := registry.GetByState(StateUnhealthy)
	if len(unhealthyNodes) != 1 {
		t.Errorf("GetByState(Unhealthy) returned %d nodes, want 1", len(unhealthyNodes))
	}
}

func TestRegistryGetHealthy(t *testing.T) {
	registry := newTestRegistry()

	healthy1 := NewNode("healthy-1", "Healthy 1", RoleWriter, "test-cluster")
	healthy1.UpdateState(StateHealthy)

	healthy2 := NewNode("healthy-2", "Healthy 2", RoleReader, "test-cluster")
	healthy2.UpdateState(StateHealthy)

	unhealthy := NewNode("unhealthy-1", "Unhealthy", RoleWriter, "test-cluster")
	unhealthy.UpdateState(StateUnhealthy)

	registry.Register(healthy1)
	registry.Register(healthy2)
	registry.Register(unhealthy)

	healthyNodes := registry.GetHealthy()
	if len(healthyNodes) != 2 {
		t.Errorf("GetHealthy() returned %d nodes, want 2", len(healthyNodes))
	}
}

func TestRegistryGetWritersReadersCompactors(t *testing.T) {
	registry := newTestRegistry()

	// Add healthy nodes
	writer := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)

	reader := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	reader.UpdateState(StateHealthy)

	compactor := NewNode("compactor-1", "Compactor", RoleCompactor, "test-cluster")
	compactor.UpdateState(StateHealthy)

	// Add unhealthy writer (should not be returned)
	unhealthyWriter := NewNode("writer-2", "Unhealthy Writer", RoleWriter, "test-cluster")
	unhealthyWriter.UpdateState(StateUnhealthy)

	registry.Register(writer)
	registry.Register(reader)
	registry.Register(compactor)
	registry.Register(unhealthyWriter)

	writers := registry.GetWriters()
	if len(writers) != 1 {
		t.Errorf("GetWriters() returned %d nodes, want 1", len(writers))
	}

	readers := registry.GetReaders()
	if len(readers) != 1 {
		t.Errorf("GetReaders() returned %d nodes, want 1", len(readers))
	}

	compactors := registry.GetCompactors()
	if len(compactors) != 1 {
		t.Errorf("GetCompactors() returned %d nodes, want 1", len(compactors))
	}
}

func TestRegistryLocal(t *testing.T) {
	registry, localNode := newTestRegistryWithLocal()

	got := registry.Local()
	if got == nil {
		t.Fatal("Local() returned nil")
	}
	if got.ID != localNode.ID {
		t.Errorf("Local().ID = %v, want %v", got.ID, localNode.ID)
	}
}

func TestRegistryCount(t *testing.T) {
	registry := newTestRegistry()

	if count := registry.Count(); count != 0 {
		t.Errorf("Empty registry Count() = %d, want 0", count)
	}

	for i := 1; i <= 5; i++ {
		node := NewNode("node-"+string(rune('0'+i)), "Node", RoleWriter, "test-cluster")
		registry.Register(node)
	}

	if count := registry.Count(); count != 5 {
		t.Errorf("Count() = %d, want 5", count)
	}
}

func TestRegistryCountByRole(t *testing.T) {
	registry := newTestRegistry()

	for i := 0; i < 3; i++ {
		node := NewNode("writer-"+string(rune('0'+i)), "Writer", RoleWriter, "test-cluster")
		registry.Register(node)
	}
	for i := 0; i < 2; i++ {
		node := NewNode("reader-"+string(rune('0'+i)), "Reader", RoleReader, "test-cluster")
		registry.Register(node)
	}

	if count := registry.CountByRole(RoleWriter); count != 3 {
		t.Errorf("CountByRole(Writer) = %d, want 3", count)
	}
	if count := registry.CountByRole(RoleReader); count != 2 {
		t.Errorf("CountByRole(Reader) = %d, want 2", count)
	}
	if count := registry.CountByRole(RoleCompactor); count != 0 {
		t.Errorf("CountByRole(Compactor) = %d, want 0", count)
	}
}

func TestRegistryCountHealthy(t *testing.T) {
	registry := newTestRegistry()

	healthy1 := NewNode("healthy-1", "Healthy 1", RoleWriter, "test-cluster")
	healthy1.UpdateState(StateHealthy)

	healthy2 := NewNode("healthy-2", "Healthy 2", RoleReader, "test-cluster")
	healthy2.UpdateState(StateHealthy)

	unhealthy := NewNode("unhealthy-1", "Unhealthy", RoleWriter, "test-cluster")
	unhealthy.UpdateState(StateUnhealthy)

	registry.Register(healthy1)
	registry.Register(healthy2)
	registry.Register(unhealthy)

	if count := registry.CountHealthy(); count != 2 {
		t.Errorf("CountHealthy() = %d, want 2", count)
	}
}

func TestRegistrySummary(t *testing.T) {
	registry := newTestRegistry()

	// Add various nodes
	writer := NewNode("writer-1", "Writer", RoleWriter, "test-cluster")
	writer.UpdateState(StateHealthy)

	reader := NewNode("reader-1", "Reader", RoleReader, "test-cluster")
	reader.UpdateState(StateHealthy)

	unhealthy := NewNode("unhealthy-1", "Unhealthy", RoleWriter, "test-cluster")
	unhealthy.UpdateState(StateUnhealthy)

	registry.Register(writer)
	registry.Register(reader)
	registry.Register(unhealthy)

	summary := registry.Summary()

	if summary["total"] != 3 {
		t.Errorf("Summary[total] = %d, want 3", summary["total"])
	}
	if summary["healthy"] != 2 {
		t.Errorf("Summary[healthy] = %d, want 2", summary["healthy"])
	}
	if summary["unhealthy"] != 1 {
		t.Errorf("Summary[unhealthy] = %d, want 1", summary["unhealthy"])
	}
	if summary["writers"] != 2 {
		t.Errorf("Summary[writers] = %d, want 2", summary["writers"])
	}
	if summary["readers"] != 1 {
		t.Errorf("Summary[readers] = %d, want 1", summary["readers"])
	}
}

func TestRegistryWithLocalNode(t *testing.T) {
	registry, localNode := newTestRegistryWithLocal()

	// Local node should be automatically registered
	if count := registry.Count(); count != 1 {
		t.Errorf("Registry with local node should have 1 node, got %d", count)
	}

	got, exists := registry.Get(localNode.ID)
	if !exists {
		t.Fatal("Local node should exist in registry")
	}
	if got.ID != localNode.ID {
		t.Errorf("Get(local).ID = %v, want %v", got.ID, localNode.ID)
	}
}

func TestRegistryGetAvailable(t *testing.T) {
	registry := newTestRegistry()

	healthy := NewNode("healthy-1", "Healthy", RoleWriter, "test-cluster")
	healthy.UpdateState(StateHealthy)

	joining := NewNode("joining-1", "Joining", RoleWriter, "test-cluster")
	joining.UpdateState(StateJoining)

	unhealthy := NewNode("unhealthy-1", "Unhealthy", RoleWriter, "test-cluster")
	unhealthy.UpdateState(StateUnhealthy)

	registry.Register(healthy)
	registry.Register(joining)
	registry.Register(unhealthy)

	available := registry.GetAvailable()
	if len(available) != 2 {
		t.Errorf("GetAvailable() returned %d nodes, want 2 (healthy + joining)", len(available))
	}
}

func TestRegistryMaxNodes(t *testing.T) {
	// Create registry with max 3 nodes
	registry := NewRegistry(&RegistryConfig{
		MaxNodes: 3,
		Logger:   zerolog.Nop(),
	})

	// Register 3 nodes - should succeed
	for i := 1; i <= 3; i++ {
		node := NewNode("node-"+string(rune('0'+i)), "Node", RoleWriter, "test-cluster")
		err := registry.Register(node)
		if err != nil {
			t.Fatalf("Register node %d should succeed, got error: %v", i, err)
		}
	}

	if count := registry.Count(); count != 3 {
		t.Errorf("After registering 3 nodes, Count() = %d, want 3", count)
	}

	// Registering 4th node should fail
	node4 := NewNode("node-4", "Node 4", RoleWriter, "test-cluster")
	err := registry.Register(node4)
	if err != ErrTooManyNodes {
		t.Errorf("Registering node 4 should return ErrTooManyNodes, got: %v", err)
	}

	// Count should still be 3
	if count := registry.Count(); count != 3 {
		t.Errorf("After rejected registration, Count() = %d, want 3", count)
	}

	// Updating existing node should succeed
	node1Updated := NewNode("node-1", "Node 1 Updated", RoleReader, "test-cluster")
	err = registry.Register(node1Updated)
	if err != nil {
		t.Errorf("Updating existing node should succeed, got error: %v", err)
	}

	// Count should still be 3
	if count := registry.Count(); count != 3 {
		t.Errorf("After update, Count() = %d, want 3", count)
	}
}

func TestRegistryCloneIsolation(t *testing.T) {
	registry := newTestRegistry()

	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")
	node.UpdateState(StateHealthy)
	registry.Register(node)

	// Get cloned node
	clone, exists := registry.Get("node-1")
	if !exists {
		t.Fatal("Node should exist")
	}

	// Modify clone state
	clone.UpdateState(StateUnhealthy)

	// Verify original is unchanged
	clone2, _ := registry.Get("node-1")
	if clone2.GetState() != StateHealthy {
		t.Error("Original node state should not be affected by clone modification")
	}
}

func TestProcessHealthCheckResult(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")
	node.UpdateState(StateHealthy)

	// Healthy check should return nil (no transition from healthy to healthy)
	transition := node.ProcessHealthCheckResult(true, 3, 6)
	if transition != nil {
		t.Error("Healthy check on healthy node should return nil transition")
	}

	// Three failed checks to trigger unhealthy transition
	for i := 0; i < 2; i++ {
		transition = node.ProcessHealthCheckResult(false, 3, 6)
		if transition != nil {
			t.Errorf("Failed check %d should not trigger transition yet", i+1)
		}
	}

	// Third failed check should transition to unhealthy
	transition = node.ProcessHealthCheckResult(false, 3, 6)
	if transition == nil {
		t.Fatal("Third failed check should trigger transition")
	}
	if transition.NewState != StateUnhealthy {
		t.Errorf("New state should be unhealthy, got: %v", transition.NewState)
	}
	if transition.OldState != StateHealthy {
		t.Errorf("Old state should be healthy, got: %v", transition.OldState)
	}

	// Healthy check should transition back
	transition = node.ProcessHealthCheckResult(true, 3, 6)
	if transition == nil {
		t.Fatal("Healthy check after unhealthy should trigger transition")
	}
	if transition.NewState != StateHealthy {
		t.Errorf("New state should be healthy, got: %v", transition.NewState)
	}
}
