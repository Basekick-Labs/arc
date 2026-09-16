package cluster

// A heartbeat from a node this one has no record of used to be discarded in
// silence and acknowledged anyway, so a peer that believed it was a member and
// was not had no way to find out and neither did an operator (#849).

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func TestWarnUnknownHeartbeat_CountsEveryOneAndWarnsOncePerNode(t *testing.T) {
	c := &Coordinator{logger: zerolog.Nop()}
	before := metrics.Get().Snapshot()["cluster_heartbeats_unknown_node_total"].(int64)

	for i := 0; i < 5; i++ {
		c.warnUnknownHeartbeat("ghost-1", "127.0.0.1:1")
	}
	after := metrics.Get().Snapshot()["cluster_heartbeats_unknown_node_total"].(int64)
	if got := after - before; got != 5 {
		t.Errorf("counter rose by %d; want 5, one per heartbeat", got)
	}

	c.unknownHeartbeatMu.Lock()
	seen := len(c.unknownHeartbeatSeen)
	c.unknownHeartbeatMu.Unlock()
	if seen != 1 {
		t.Errorf("tracked %d node ids after five heartbeats from one node; want 1", seen)
	}
}

// Node ids are derived from hostname and pid, so a peer restarting in a loop
// mints new ones. The map is fed by the network and must stay bounded.
func TestWarnUnknownHeartbeat_BoundsTheNodesItTracks(t *testing.T) {
	c := &Coordinator{logger: zerolog.Nop()}
	for i := 0; i < unknownHeartbeatWarnCap+50; i++ {
		c.warnUnknownHeartbeat(nodeIDFor(i), "127.0.0.1:1")
	}
	c.unknownHeartbeatMu.Lock()
	seen := len(c.unknownHeartbeatSeen)
	c.unknownHeartbeatMu.Unlock()
	if seen > unknownHeartbeatWarnCap {
		t.Errorf("tracked %d node ids; cap is %d", seen, unknownHeartbeatWarnCap)
	}
}

// A repeat inside the interval is not re-warned, but one after it is.
func TestWarnUnknownHeartbeat_WarnsAgainAfterTheInterval(t *testing.T) {
	c := &Coordinator{logger: zerolog.Nop()}
	c.warnUnknownHeartbeat("ghost-1", "127.0.0.1:1")

	c.unknownHeartbeatMu.Lock()
	first := c.unknownHeartbeatSeen["ghost-1"]
	// Age the record past the interval.
	c.unknownHeartbeatSeen["ghost-1"] = first.Add(-unknownHeartbeatWarnInterval - time.Second)
	c.unknownHeartbeatMu.Unlock()

	c.warnUnknownHeartbeat("ghost-1", "127.0.0.1:1")
	c.unknownHeartbeatMu.Lock()
	second := c.unknownHeartbeatSeen["ghost-1"]
	c.unknownHeartbeatMu.Unlock()
	if !second.After(first) {
		t.Error("a heartbeat after the interval did not refresh the warning")
	}
}

func nodeIDFor(i int) string {
	return "ghost-" + string(rune('a'+i%26)) + "-" + time.Duration(i).String()
}

// A known node neither counts nor warns: the handler only calls this helper
// when the registry rejected the heartbeat.
func TestRecordHeartbeat_ReportsWhetherTheNodeIsKnown(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	if !reg.RecordHeartbeat("writer-1", NodeStats{}) {
		t.Error("a heartbeat from the local node was reported as unknown")
	}
	if reg.RecordHeartbeat("ghost-1", NodeStats{}) {
		t.Error("a heartbeat from an unregistered node was reported as known")
	}
}

// End to end through the handler: an unknown node's heartbeat is still
// acknowledged, but it is now counted and announced rather than discarded in
// silence. A known node's heartbeat does neither.
func TestHandleHeartbeat_AnnouncesAnUnknownNode(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: "test-cluster"},
		registry:  reg,
		localNode: local,
		logger:    zerolog.Nop(),
	}
	count := func() int64 {
		return metrics.Get().Snapshot()["cluster_heartbeats_unknown_node_total"].(int64)
	}

	before := count()
	deliverHeartbeat(c, &protocol.Heartbeat{NodeID: "ghost-1", State: string(StateHealthy)})
	if got := count() - before; got != 1 {
		t.Errorf("an unknown node's heartbeat moved the counter by %d; want 1", got)
	}

	before = count()
	deliverHeartbeat(c, &protocol.Heartbeat{NodeID: "writer-1", State: string(StateHealthy)})
	if got := count() - before; got != 0 {
		t.Errorf("a known node's heartbeat moved the counter by %d; want 0", got)
	}
}
