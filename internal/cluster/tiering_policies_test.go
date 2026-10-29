package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/tiering"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func enableClusterPolicies(t *testing.T, c *Coordinator) {
	t.Helper()
	capNodes(t, c, map[string][]string{"raft-1": cpList, "writer-1": cpList, "writer-2": cpList, "reader-1": cpList})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
}

func seedTiering(t *testing.T, c *Coordinator, entries ...raft.TieringPolicyEntry) {
	t.Helper()
	epoch, err := c.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindTiering, Leader: "raft-1", Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.proposeLeader(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindTiering, Epoch: epoch, Tiering: entries, Now: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
}

func newPolicyStore(t *testing.T) *tiering.PolicyStore {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s, err := tiering.NewPolicyStore(db, &config.TieredStorageConfig{DefaultHotMaxAgeDays: 7}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// #1324: until tiering is seeded the store keeps using its own table; once
// seeded every read and write goes through the cluster state.
func TestTieringPolicyStore_UsesTheClusterOnceSeeded(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	ctx := context.Background()
	store := newPolicyStore(t)
	if err := store.Set(ctx, &tiering.DatabasePolicy{Database: "local", HotOnly: true}); err != nil {
		t.Fatal(err)
	}
	store.SetClusterSource(NewTieringPolicySource(c))

	// Not enabled, then enabled but not seeded: the local table.
	enableClusterPolicies(t, c)
	if p, _ := store.Get(ctx, "local"); p == nil || !p.HotOnly {
		t.Fatalf("before the seed: %+v, want the local row", p)
	}

	seedTiering(t, c, raft.TieringPolicyEntry{Database: "seeded", HotOnly: true})
	if p, _ := store.Get(ctx, "local"); p != nil {
		t.Fatalf("after the seed the local-only row still answers: %+v", p)
	}
	if !store.IsHotOnly(ctx, "seeded") {
		t.Fatal("seeded policy not visible")
	}
	if err := store.Set(ctx, &tiering.DatabasePolicy{Database: "db1", HotMaxAgeDays: intPtr(3)}); err != nil {
		t.Fatal(err)
	}
	if e, ok := c.TieringPolicy("db1"); !ok || e.HotMaxAgeDays == nil || *e.HotMaxAgeDays != 3 {
		t.Fatalf("set did not reach the cluster: %+v %v", e, ok)
	}
	if eff := store.GetEffective(ctx, "db1"); eff.Source != "custom" || eff.HotMaxAgeDays != 3 {
		t.Fatalf("effective: %+v", eff)
	}
	list, _ := store.List(ctx)
	if len(list) != 2 {
		t.Fatalf("list = %+v, want seeded and db1", list)
	}
	if err := store.Delete(ctx, "db1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "db1"); !errors.Is(err, tiering.ErrPolicyNotFound) {
		t.Fatalf("delete of a missing policy: %v", err)
	}
	// Nothing was written to the local table after the seed.
	store.SetClusterSource(nil)
	if p, _ := store.Get(ctx, "db1"); p != nil {
		t.Fatalf("a cluster write landed in the local table: %+v", p)
	}
}

func intPtr(v int) *int { return &v }

// A peer may forward a policy definition (the admin check ran on the node
// that took the HTTP call), whatever its role; never a seed.
func TestForwardApply_PolicyCommands(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()
	enableClusterPolicies(t, c)
	seedTiering(t, c)
	set, _ := json.Marshal(raft.SetTieringPolicyPayload{Database: "db1", HotOnly: true, Now: time.Now().UTC()})
	if ack := forwardOnce(t, c, "reader-1", &raft.Command{Type: raft.CommandSetTieringPolicy, Payload: set}); ack.Status != "ok" {
		t.Fatalf("set from a reader: %+v", ack)
	}
	if _, ok := c.TieringPolicy("db1"); !ok {
		t.Fatal("forwarded set not applied")
	}
	del, _ := json.Marshal(raft.DeleteTieringPolicyPayload{Database: "missing"})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandDeleteTieringPolicy, Payload: del}); ack.Status != "error" || !raft.IsPolicyNotFound(errors.New(ack.Error)) {
		t.Fatalf("forwarded delete of a missing policy: %+v", ack)
	}
	seed, _ := json.Marshal(raft.SeedPoliciesPayload{Kind: raft.PolicyKindTiering, Now: time.Now().UTC()})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandSeedPolicies, Payload: seed}); ack.Status != "error" || ack.Code != protocol.ForwardCodeInvalidCommand {
		t.Fatalf("forwarded seed: %+v, want refused", ack)
	}
}

// Retention definitions are forwardable like tiering's; an admin's adoption
// choice never is: it activates deleting policies, so it is taken on the
// leader's own HTTP call only.
func TestForwardApply_RetentionCommands(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()
	capNodes(t, c, map[string][]string{"raft-1": cpRetList, "writer-1": cpRetList, "writer-2": cpRetList, "reader-1": cpRetList})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnableRetentionPolicies()
	waitFSM(t, c.fsmOrNil().RetentionPoliciesEnabled)
	epoch, err := c.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindRetention, Leader: "raft-1", Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.proposeLeader(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindRetention, Epoch: epoch, RetentionFloor: 1, Now: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	fwd := func(typ raft.CommandType, p any) *protocol.ForwardApplyAck {
		raw, _ := json.Marshal(p)
		return forwardOnce(t, c, "reader-1", &raft.Command{Type: typ, Payload: raw})
	}
	if ack := fwd(raft.CommandCreateRetentionPolicy, raft.CreateRetentionPolicyPayload{Name: "p", Database: "db", RetentionDays: 9, Now: time.Now().UTC()}); ack.Status != "ok" {
		t.Fatalf("create from a reader: %+v", ack)
	}
	if ack := fwd(raft.CommandUpdateRetentionPolicy, raft.UpdateRetentionPolicyPayload{ID: 1, RetentionDays: intPtr(10)}); ack.Status != "ok" {
		t.Fatalf("update from a reader: %+v", ack)
	}
	if p, _ := c.RetentionPolicy(1); p.RetentionDays != 10 {
		t.Fatalf("forwarded writes not applied: %+v", p)
	}
	if ack := fwd(raft.CommandDeleteRetentionPolicy, raft.DeleteRetentionPolicyPayload{ID: 1}); ack.Status != "ok" {
		t.Fatalf("delete from a reader: %+v", ack)
	}
	if ack := fwd(raft.CommandChoosePolicyAdoption, raft.ChoosePolicyAdoptionPayload{Kind: raft.PolicyKindRetention, Node: "writer-2", Hash: "h"}); ack.Status != "error" || ack.Code != protocol.ForwardCodeInvalidCommand {
		t.Fatalf("forwarded adoption choice: %+v, want refused", ack)
	}
}
