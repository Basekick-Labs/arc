package cluster

import (
	"testing"
	"time"
)

func TestNewNode(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	if node.ID != "node-1" {
		t.Errorf("NewNode().ID = %v, want %v", node.ID, "node-1")
	}
	if node.Name != "Test Node" {
		t.Errorf("NewNode().Name = %v, want %v", node.Name, "Test Node")
	}
	if node.Role != RoleWriter {
		t.Errorf("NewNode().Role = %v, want %v", node.Role, RoleWriter)
	}
	if node.ClusterName != "test-cluster" {
		t.Errorf("NewNode().ClusterName = %v, want %v", node.ClusterName, "test-cluster")
	}
	if node.State != StateUnknown {
		t.Errorf("NewNode().State = %v, want %v", node.State, StateUnknown)
	}
	if node.StartedAt.IsZero() {
		t.Error("NewNode().StartedAt should not be zero")
	}
}

func TestNodeStateTransitions(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	// Initial state is Unknown
	if node.GetState() != StateUnknown {
		t.Errorf("Initial state = %v, want %v", node.GetState(), StateUnknown)
	}

	// Test state transitions
	states := []NodeState{StateJoining, StateHealthy, StateUnhealthy, StateDead, StateLeaving}
	for _, state := range states {
		node.UpdateState(state)
		if node.GetState() != state {
			t.Errorf("After UpdateState(%v), GetState() = %v", state, node.GetState())
		}
	}
}

func TestNodeIsHealthy(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	// Initially not healthy (Unknown state)
	if node.IsHealthy() {
		t.Error("Node with Unknown state should not be healthy")
	}

	// Healthy state
	node.UpdateState(StateHealthy)
	if !node.IsHealthy() {
		t.Error("Node with Healthy state should be healthy")
	}

	// Unhealthy state
	node.UpdateState(StateUnhealthy)
	if node.IsHealthy() {
		t.Error("Node with Unhealthy state should not be healthy")
	}
}

func TestNodeIsAvailable(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	tests := []struct {
		state    NodeState
		expected bool
	}{
		{StateUnknown, false},
		{StateJoining, true},  // Joining nodes are available
		{StateHealthy, true},
		{StateUnhealthy, false},
		{StateDead, false},
		{StateLeaving, false},
	}

	for _, tt := range tests {
		node.UpdateState(tt.state)
		if got := node.IsAvailable(); got != tt.expected {
			t.Errorf("Node with state %v: IsAvailable() = %v, want %v", tt.state, got, tt.expected)
		}
	}
}

func TestNodeRecordHeartbeat(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")
	node.UpdateState(StateHealthy)

	// Record failed checks first
	node.RecordFailedCheck()
	node.RecordFailedCheck()
	if node.GetFailedChecks() != 2 {
		t.Errorf("GetFailedChecks() = %v, want 2", node.GetFailedChecks())
	}

	// Record heartbeat should reset failed checks
	stats := NodeStats{CPUUsage: 50.0, MemoryUsage: 60.0}
	node.RecordHeartbeat(stats)

	if node.GetFailedChecks() != 0 {
		t.Errorf("After heartbeat, GetFailedChecks() = %v, want 0", node.GetFailedChecks())
	}

	if node.GetLastHeartbeat().IsZero() {
		t.Error("After heartbeat, GetLastHeartbeat() should not be zero")
	}

	gotStats := node.GetStats()
	if gotStats.CPUUsage != 50.0 {
		t.Errorf("GetStats().CPUUsage = %v, want 50.0", gotStats.CPUUsage)
	}
}

func TestNodeRecordFailedCheck(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	for i := 1; i <= 5; i++ {
		count := node.RecordFailedCheck()
		if count != i {
			t.Errorf("RecordFailedCheck() returned %v, want %v", count, i)
		}
		if node.GetFailedChecks() != i {
			t.Errorf("GetFailedChecks() = %v, want %v", node.GetFailedChecks(), i)
		}
	}
}

func TestNodeSetAddresses(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	node.SetAddresses(":9100", ":8000")

	if node.Address != ":9100" {
		t.Errorf("Address = %v, want :9100", node.Address)
	}
	if node.APIAddress != ":8000" {
		t.Errorf("APIAddress = %v, want :8000", node.APIAddress)
	}
}

func TestNodeSetVersion(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	node.SetVersion("1.2.3")

	if node.Version != "1.2.3" {
		t.Errorf("Version = %v, want 1.2.3", node.Version)
	}
}

func TestNodeMarkJoined(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	beforeJoin := time.Now()
	node.MarkJoined()
	afterJoin := time.Now()

	if node.GetState() != StateHealthy {
		t.Errorf("After MarkJoined(), state = %v, want %v", node.GetState(), StateHealthy)
	}

	if node.JoinedAt.Before(beforeJoin) || node.JoinedAt.After(afterJoin) {
		t.Error("JoinedAt is not within expected time range")
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")

	caps := node.GetCapabilities()
	expected := RoleWriter.GetCapabilities()

	if caps != expected {
		t.Errorf("GetCapabilities() = %+v, want %+v", caps, expected)
	}
}

func TestNodeClone(t *testing.T) {
	node := NewNode("node-1", "Test Node", RoleWriter, "test-cluster")
	node.SetVersion("1.0.0")
	node.SetAddresses(":9100", ":8000")
	node.UpdateState(StateHealthy)
	node.RecordHeartbeat(NodeStats{CPUUsage: 50.0})

	clone := node.Clone()

	// Verify clone has same values
	if clone.ID != node.ID {
		t.Errorf("Clone.ID = %v, want %v", clone.ID, node.ID)
	}
	if clone.Name != node.Name {
		t.Errorf("Clone.Name = %v, want %v", clone.Name, node.Name)
	}
	if clone.Role != node.Role {
		t.Errorf("Clone.Role = %v, want %v", clone.Role, node.Role)
	}
	if clone.State != node.State {
		t.Errorf("Clone.State = %v, want %v", clone.State, node.State)
	}
	if clone.Version != node.Version {
		t.Errorf("Clone.Version = %v, want %v", clone.Version, node.Version)
	}

	// Modify clone and verify original is unchanged
	clone.UpdateState(StateUnhealthy)
	if node.GetState() != StateHealthy {
		t.Error("Modifying clone should not affect original")
	}
}

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{StateUnknown, "unknown"},
		{StateHealthy, "healthy"},
		{StateUnhealthy, "unhealthy"},
		{StateDead, "dead"},
		{StateJoining, "joining"},
		{StateLeaving, "leaving"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState.String() = %v, want %v", got, tt.expected)
		}
	}
}
