package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// newSharedNoReplicationRig is a node configured the way the Helm chart runs
// a shared-object-store cluster: replication off, so no puller, through the
// same callback decision Start makes.
func newSharedNoReplicationRig(t *testing.T, backend storage.Backend) (*Coordinator, *fakeTierRecorder) {
	t.Helper()
	raftNode := startRaftNode(t, "shared-node", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: "shared", SharedSecret: "shared-secret", ReplicationEnabled: false},
		storage:   backend,
		raftNode:  raftNode,
		localNode: NewNode("shared-node", "shared-node", RoleWriter, "shared"),
		logger:    zerolog.Nop(),
		ctx:       ctx,
	}
	recorder := &fakeTierRecorder{}
	c.SetTierRecorder(recorder)
	c.mu.Lock()
	c.startManifestCallbacksLocked()
	c.mu.Unlock()
	return c, recorder
}

func registerAndDelete(t *testing.T, c *Coordinator, path, reason string) {
	t.Helper()
	body := []byte("payload of " + path)
	if err := c.raftNode.RegisterFile(makeFileEntry(path, body, "origin"), 5*time.Second); err != nil {
		t.Fatalf("register %s: %v", path, err)
	}
	if err := c.raftNode.DeleteFile(path, reason, 5*time.Second); err != nil {
		t.Fatalf("delete %s: %v", path, err)
	}
}

// #1354 live run: on a shared object store with replication off — the Helm
// chart's shared-storage setting — the manifest delete callback lived only in
// the puller, so a migration never reached the other nodes' tier metadata
// (#1349) and a DELETE rewrite's old file was never left out of their reads
// (#1348).
func TestSharedStoreWithoutReplication_ManifestDeletesReachThisNode(t *testing.T) {
	c, recorder := newSharedNoReplicationRig(t, newMemBackend())

	migrated := "testdb/cpu/2026/10/08/m_daily.parquet"
	restored := "testdb/cpu/2026/10/08/r_daily.parquet"
	rewritten := "testdb/cpu/2026/10/08/00/old.parquet"
	compacted := "testdb/cpu/2026/10/08/00/in.parquet"
	registerAndDelete(t, c, migrated, "tiering:migrated")
	registerAndDelete(t, c, restored, raft.UnlinkReasonSnapshotRemoved)
	registerAndDelete(t, c, rewritten, deleteRewriteReason)
	registerAndDelete(t, c, compacted, "compaction:job-1")

	reports := recorder.unlinkReports()
	if len(reports) != 2 || reports[0].path != migrated || reports[1].path != restored {
		t.Fatalf("tier reports = %+v, want the migration and the snapshot drop only", reports)
	}
	c.open.mu.Lock()
	_, noted := c.open.localDeleted[rewritten]
	_, compactionNoted := c.open.localDeleted[compacted]
	c.open.mu.Unlock()
	if !noted {
		t.Fatal("a DELETE rewrite's old file was not recorded for exclusion from reads")
	}
	if compactionNoted {
		t.Fatal("a compaction input on a shared store was recorded as a pending local delete")
	}

	// Registered again: no longer a pending delete here.
	if err := c.raftNode.RegisterFile(makeFileEntry(rewritten, []byte("again"), "origin"), 5*time.Second); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	c.open.mu.Lock()
	_, still := c.open.localDeleted[rewritten]
	c.open.mu.Unlock()
	if still {
		t.Fatal("a re-registered path stayed in the pending-delete set")
	}
}

// Local storage without replication shares nothing: no node reads another's
// disk, so nothing registers callbacks.
func TestLocalStoreWithoutReplication_RegistersNoManifestCallbacks(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	c, recorder := newSharedNoReplicationRig(t, local)
	registerAndDelete(t, c, "testdb/cpu/2026/10/08/m_daily.parquet", "tiering:migrated")
	if reports := recorder.unlinkReports(); len(reports) != 0 {
		t.Fatalf("tier reports = %+v, want none on local storage without replication", reports)
	}
}
