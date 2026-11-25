package cluster

import (
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1914: the list a node advertises at join must pass the bound the member it
// contacts applies, or the node never joins. Local storage with replication,
// the Iceberg export and failover advertised 17 capabilities against a bound of
// 16, and such a cluster never formed. Every optional capability is switched
// on here, so the next one added to localCapabilities shows up against the
// bound in this test rather than on a live cluster.
func TestEveryOptionalCapabilityFitsTheJoinBound(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c.storage = b
	c.cfg.ReplicationEnabled = true
	c.staged.replicaReady.Store(true)
	c.SetRehomeInScope(true)
	c.SetIcebergOn(true)
	c.cfg.StreamTakeover = true
	c.SetWALCoverageActive(true)

	caps := c.localCapabilities()
	for _, want := range []string{raft.CapabilityRehomeInScope, CapabilityLocalReplica, CapabilityIcebergExport, raft.CapabilityStreamClaims} {
		if !hasCapability(caps, want) {
			t.Fatalf("%s not advertised with every option on: %v", want, caps)
		}
	}
	if err := security.CheckJoinCapabilities(caps); err != nil {
		t.Fatalf("%d capabilities against a bound of %d: %v", len(caps), security.MaxJoinCapabilities, err)
	}
	// Headroom, so that adding a capability is a decision about the bound
	// and not a silent step towards it.
	if len(caps) > security.MaxJoinCapabilities/2 {
		t.Fatalf("%d capabilities leave less than half of the bound of %d free", len(caps), security.MaxJoinCapabilities)
	}

	// As the contacted member receives it: both bound checks and the
	// capability MAC, through handleJoinRequest.
	resp := deliverJoin(t, c, capsJoin(osSecret, caps, true))
	if resp == nil || !resp.Success {
		t.Fatalf("a join advertising %d capabilities was refused: %+v", len(caps), resp)
	}
	waitFSM(t, func() bool {
		n, ok := c.raftNode.FSM().GetNode("reader-9")
		return ok && len(n.Capabilities) == len(caps)
	})
}
