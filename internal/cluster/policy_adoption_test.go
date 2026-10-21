package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/rs/zerolog"
)

func TestMergeTieringPolicies(t *testing.T) {
	t1, t2 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	got := mergeTieringPolicies(map[string]TieringLocalRows{
		"a": {DefaultHotMaxAgeDays: 7, Policies: []raft.TieringPolicyEntry{{Database: "db1", HotMaxAgeDays: intPtr(7), CreatedAt: t2, UpdatedAt: t2}, {Database: "db3"}}},
		"b": {DefaultHotMaxAgeDays: 7, Policies: []raft.TieringPolicyEntry{{Database: "db1", HotOnly: true, HotMaxAgeDays: intPtr(3), CreatedAt: t1, UpdatedAt: t1}, {Database: "db2", HotMaxAgeDays: intPtr(30)}}},
		"c": {DefaultHotMaxAgeDays: 7, Policies: []raft.TieringPolicyEntry{{Database: "db1", HotMaxAgeDays: intPtr(14)}, {Database: "db3"}}},
	})
	if len(got) != 3 {
		t.Fatalf("merged: %+v", got)
	}
	db1, db2, db3 := got[0], got[1], got[2]
	if !db1.HotOnly || db1.HotMaxAgeDays == nil || *db1.HotMaxAgeDays != 14 || !db1.CreatedAt.Equal(t1) || !db1.UpdatedAt.Equal(t2) {
		t.Fatalf("db1 = %+v: hot_only from any node, the longest age, the earliest creation", db1)
	}
	if db2.HotOnly || *db2.HotMaxAgeDays != 30 {
		t.Fatalf("db2 = %+v", db2)
	}
	if db3.HotMaxAgeDays != nil {
		t.Fatalf("db3 = %+v: unset everywhere stays unset", db3)
	}
}

// An unset age means the node's own default: a node whose default is longer
// than another node's explicit age keeps its data hot longer, so the merge
// takes the default.
func TestMergeTieringPolicies_UnsetAgeIsTheNodesDefault(t *testing.T) {
	got := mergeTieringPolicies(map[string]TieringLocalRows{
		"a": {DefaultHotMaxAgeDays: 30, Policies: []raft.TieringPolicyEntry{{Database: "db"}}},
		"b": {DefaultHotMaxAgeDays: 7, Policies: []raft.TieringPolicyEntry{{Database: "db", HotMaxAgeDays: intPtr(7)}}},
	})
	if len(got) != 1 || got[0].HotMaxAgeDays == nil || *got[0].HotMaxAgeDays != 30 {
		t.Fatalf("merged = %+v, want 30 (node a's default), not b's 7", got)
	}
	// A node that answered "none" (tiering off) contributes nothing.
	seed, err := mergePolicyAnswers(raft.PolicyKindTiering, map[string]json.RawMessage{"x": json.RawMessage("null")})
	if err != nil || len(seed.Tiering) != 0 {
		t.Fatalf("empty answer: %+v %v", seed, err)
	}
}

// adoptRig: a leader and a non-voting follower, each a coordinator with its
// own tiering policy store, the follower answering collects on a listener.
type adoptRig struct {
	leader, follower           *Coordinator
	leaderStore, followerStore *tiering.PolicyStore
	ln                         net.Listener
}

func newAdoptRig(t *testing.T) *adoptRig {
	t.Helper()
	if testing.Short() {
		t.Skip("requires a real Raft cluster")
	}
	lr := startRaftNode(t, "L", allocFreePort(t), true)
	t.Cleanup(func() { _ = lr.Stop() })
	if err := lr.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	fAddr := allocFreePort(t)
	fr := startRaftNode(t, "F", fAddr, false)
	t.Cleanup(func() { _ = fr.Stop() })
	if err := lr.AddNonvoter("F", fAddr, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	for _, n := range []*raft.NodeInfo{
		{ID: "L", Name: "L", Role: string(RoleWriter), ClusterName: "test-cluster", Address: "127.0.0.1:1", State: string(StateHealthy)},
		{ID: "F", Name: "F", Role: string(RoleReader), ClusterName: "test-cluster", Address: ln.Addr().String(), State: string(StateHealthy)},
	} {
		if err := lr.AddNode(n, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := json.Marshal(raft.EnableClusterPoliciesPayload{Nodes: []string{"L", "F"}, Now: time.Now()})
	if err := lr.Apply(&raft.Command{Type: raft.CommandEnableClusterPolicies, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return fr.FSM().ClusterPoliciesEnabled() && fr.LeaderID() == "L" }, "follower never caught up")

	mk := func(id string, rn *raft.Node) *Coordinator {
		local := NewNode(id, id, RoleWriter, "test-cluster")
		return &Coordinator{
			cfg:        &config.ClusterConfig{SharedSecret: forwardFSMSecret, ClusterName: "test-cluster", NodeID: id},
			registry:   NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()}),
			localNode:  local,
			raftNode:   rn,
			nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
			logger:     zerolog.Nop(),
			ctx:        context.Background(),
		}
	}
	r := &adoptRig{leader: mk("L", lr), follower: mk("F", fr), leaderStore: newPolicyStore(t), followerStore: newPolicyStore(t), ln: ln}
	for _, p := range []struct {
		c *Coordinator
		s *tiering.PolicyStore
	}{{r.leader, r.leaderStore}, {r.follower, r.followerStore}} {
		p.s.SetClusterSource(NewTieringPolicySource(p.c))
		p.c.SetLocalPolicyLister(raft.PolicyKindTiering, TieringLocalLister(p.s, 7))
	}
	r.serve(ln)
	return r
}

// serve answers collects on ln as the follower.
func (r *adoptRig) serve(ln net.Listener) {
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
				if err != nil {
					return
				}
				if q, ok := msg.Payload.(*protocol.LocalPoliciesQuery); ok {
					r.follower.handleLocalPoliciesQuery(conn, q)
				}
			}(conn)
		}
	}()
}

// moveFollower points the leader at a new listener for the follower.
func (r *adoptRig) moveFollower(t *testing.T, ln net.Listener) {
	t.Helper()
	n := NewNode("F", "F", RoleReader, "test-cluster")
	n.Address = ln.Addr().String()
	if err := r.leader.registry.Register(n); err != nil {
		t.Fatal(err)
	}
}

// #1324: the leader collects every node's own tiering policies, merges them,
// and seeds them; from then on both nodes read the same policies.
func TestAdoptPolicies_SeedsFromEveryNode(t *testing.T) {
	r := newAdoptRig(t)
	ctx := context.Background()
	if err := r.leaderStore.Set(ctx, &tiering.DatabasePolicy{Database: "db1", HotMaxAgeDays: intPtr(7)}); err != nil {
		t.Fatal(err)
	}
	if err := r.followerStore.Set(ctx, &tiering.DatabasePolicy{Database: "db1", HotOnly: true}); err != nil {
		t.Fatal(err)
	}
	if err := r.followerStore.Set(ctx, &tiering.DatabasePolicy{Database: "db2", HotMaxAgeDays: intPtr(30)}); err != nil {
		t.Fatal(err)
	}
	r.leader.maybeSeedPolicies(ctx)
	if !r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatalf("not seeded; status %+v", r.leader.ClusterPoliciesStatus())
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.PoliciesSeeded(raft.PolicyKindTiering) }, "follower never saw the seed")
	for name, s := range map[string]*tiering.PolicyStore{"leader": r.leaderStore, "follower": r.followerStore} {
		if !s.IsHotOnly(ctx, "db1") {
			t.Errorf("%s: db1 not hot_only (the follower's pin was dropped)", name)
		}
		if eff := s.GetEffective(ctx, "db2"); eff.HotMaxAgeDays != 30 {
			t.Errorf("%s: db2 = %+v (a policy only the follower held was dropped)", name, eff)
		}
	}
	if st := r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindTiering]; !st.Seeded || st.Collecting || len(st.Missing) != 0 {
		t.Fatalf("status = %+v", st)
	}
}

// A node that does not answer keeps the kind local: no seed from a partial
// view, the silent node named, local writes allowed again, and a later
// collect seeds once it answers.
func TestAdoptPolicies_SilentNodeDoesNotSeed(t *testing.T) {
	r := newAdoptRig(t)
	ctx := context.Background()
	if err := r.followerStore.Set(ctx, &tiering.DatabasePolicy{Database: "pinned", HotOnly: true}); err != nil {
		t.Fatal(err)
	}
	_ = r.ln.Close()
	prev := policyAdoptRetry
	policyAdoptRetry = 0
	t.Cleanup(func() { policyAdoptRetry = prev })

	r.leader.maybeSeedPolicies(ctx)
	if r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatal("seeded without the follower's answer: its hot_only pin would be dropped")
	}
	st := r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindTiering]
	if st.Collecting || len(st.Missing) != 1 || st.Missing[0] != "F" || st.Attempts != 1 || st.LastError != "unreachable before the collect" {
		t.Fatalf("status = %+v, want F named, found unreachable before any collect opened", st)
	}
	waitFor(t, 10*time.Second, func() bool { return !r.follower.PolicyCollecting(raft.PolicyKindTiering) }, "follower still sees the collect")
	if err := r.followerStore.Set(ctx, &tiering.DatabasePolicy{Database: "later", HotMaxAgeDays: intPtr(5)}); err != nil {
		t.Fatalf("a local write after the collect closed: %v", err)
	}

	// The follower answers again, on a new address.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	r.serve(ln)
	r.moveFollower(t, ln)
	r.leader.maybeSeedPolicies(ctx)
	if !r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatalf("not seeded once the follower answered: %+v", r.leader.ClusterPoliciesStatus())
	}
	if !r.leaderStore.IsHotOnly(ctx, "pinned") || r.leaderStore.GetEffective(ctx, "later").HotMaxAgeDays != 5 {
		t.Fatal("the follower's policies, including one written while awaiting, were not adopted")
	}
}

// While a collect is open a local write is refused, so it cannot land after
// the leader read the table and be lost.
func TestAdoptPolicies_OpenCollectRefusesLocalWrites(t *testing.T) {
	r := newAdoptRig(t)
	ctx := context.Background()
	epoch, err := r.leader.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindTiering, Leader: "L", Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.follower.waitAppliedHere(waitCtx, epoch); err != nil {
		t.Fatal(err)
	}
	err = r.followerStore.Set(ctx, &tiering.DatabasePolicy{Database: "db1", HotOnly: true})
	if !errors.Is(err, tiering.ErrPoliciesBeingAdopted) {
		t.Fatalf("local set during a collect: %v", err)
	}
	if err := r.followerStore.Delete(ctx, "db1"); !errors.Is(err, tiering.ErrPoliciesBeingAdopted) {
		t.Fatalf("local delete during a collect: %v", err)
	}
	// The node answers this collect, and refuses one that is not open.
	if status, _ := r.follower.localPoliciesAnswer(ctx, raft.PolicyKindTiering, epoch); status != protocol.LocalPoliciesOK {
		t.Fatalf("answer to the open collect: %s", status)
	}
	if status, _ := r.follower.localPoliciesAnswer(ctx, raft.PolicyKindTiering, epoch-1); status == protocol.LocalPoliciesOK {
		t.Fatal("answered a collect that is not open")
	}
}

// A query from a node that is not the Raft leader, or with a bad MAC, is
// refused.
func TestLocalPoliciesQuery_Refusals(t *testing.T) {
	r := newAdoptRig(t)
	ask := func(from, secret string) *protocol.LocalPoliciesReply {
		t.Helper()
		nonce, _ := security.GenerateNonce()
		ts := time.Now().Unix()
		q := &protocol.LocalPoliciesQuery{NodeID: from, ClusterName: "test-cluster", Kind: raft.PolicyKindTiering, Epoch: 1, Nonce: nonce, Timestamp: ts,
			HMAC: security.ComputeLocalPoliciesQueryHMAC(secret, nonce, from, "test-cluster", raft.PolicyKindTiering, 1, ts)}
		server, client := net.Pipe()
		go func() { r.follower.handleLocalPoliciesQuery(server, q); server.Close() }()
		msg, err := protocol.ReceiveMessage(client, 5*time.Second)
		client.Close()
		if err != nil {
			t.Fatal(err)
		}
		return msg.Payload.(*protocol.LocalPoliciesReply)
	}
	if rep := ask("X", forwardFSMSecret); rep.Status != protocol.LocalPoliciesRefused {
		t.Fatalf("query from a non-leader: %s", rep.Status)
	}
	if rep := ask("L", "another-secret-of-32-bytes-long!!"); rep.Status != protocol.LocalPoliciesRefused {
		t.Fatalf("query with a bad MAC: %s", rep.Status)
	}
}

// A node that accepts the connection but never answers holds the collect
// only until the query times out; the collect then closes and names it.
func TestAdoptPolicies_HungNodeClosesTheCollect(t *testing.T) {
	r := newAdoptRig(t)
	ctx := context.Background()
	prev := policyQueryTimeout
	policyQueryTimeout = 500 * time.Millisecond
	t.Cleanup(func() { policyQueryTimeout = prev })
	_ = r.ln.Close()
	hung, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hung.Close() })
	go func() {
		for {
			conn, err := hung.Accept()
			if err != nil {
				return
			}
			go func() { time.Sleep(5 * time.Second); conn.Close() }()
		}
	}()
	r.moveFollower(t, hung)
	start := time.Now()
	r.leader.maybeSeedPolicies(ctx)
	if el := time.Since(start); el > 4*time.Second {
		t.Fatalf("the collect stayed open %v", el)
	}
	st := r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindTiering]
	if st.Seeded || st.Collecting || len(st.Missing) != 1 || st.LastError != "no answer to the collect" {
		t.Fatalf("status = %+v", st)
	}
}

// A node that has not registered its policies yet (still starting) cannot
// answer: the collect waits for it rather than dropping its policies.
func TestAdoptPolicies_NodeWithoutAListerHoldsTheCollect(t *testing.T) {
	r := newAdoptRig(t)
	ctx := context.Background()
	if err := r.followerStore.Set(ctx, &tiering.DatabasePolicy{Database: "pinned", HotOnly: true}); err != nil {
		t.Fatal(err)
	}
	r.follower.SetLocalPolicyLister(raft.PolicyKindTiering, nil)
	r.leader.maybeSeedPolicies(ctx)
	if r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatal("seeded while the follower could not read its policies: its pin would be dropped")
	}
	if st := r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindTiering]; len(st.Missing) != 1 || st.Missing[0] != "F" {
		t.Fatalf("status = %+v", st)
	}
	// A node that does not run tiering says so explicitly, and seeds.
	r.follower.SetLocalPolicyLister(raft.PolicyKindTiering, EmptyPolicyLister)
	prev := policyAdoptRetry
	policyAdoptRetry = 0
	t.Cleanup(func() { policyAdoptRetry = prev })
	r.leader.maybeSeedPolicies(ctx)
	if !r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatalf("not seeded with an explicit empty answer: %+v", r.leader.ClusterPoliciesStatus())
	}
}

// Once seeded, every node's own table follows the cluster's policies, so a
// backup of its database carries them.
func TestMirrorTieringPolicies(t *testing.T) {
	r := newAdoptRig(t)
	ctx := context.Background()
	if err := r.followerStore.Set(ctx, &tiering.DatabasePolicy{Database: "pinned", HotOnly: true}); err != nil {
		t.Fatal(err)
	}
	r.leader.MirrorTieringPolicies(r.leaderStore)
	r.follower.MirrorTieringPolicies(r.followerStore)
	r.leader.maybeSeedPolicies(ctx)
	if !r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatal("not seeded")
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.PoliciesSeeded(raft.PolicyKindTiering) }, "follower never saw the seed")
	// Written on the leader (this rig's leader does not take forwards).
	if err := r.leaderStore.Set(ctx, &tiering.DatabasePolicy{Database: "later", HotMaxAgeDays: intPtr(9)}); err != nil {
		t.Fatal(err)
	}
	inTable := func(s *tiering.PolicyStore) map[string]tiering.DatabasePolicy {
		rows, err := s.LocalPolicies(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]tiering.DatabasePolicy{}
		for _, p := range rows {
			out[p.Database] = p
		}
		return out
	}
	for name, s := range map[string]*tiering.PolicyStore{"leader": r.leaderStore, "follower": r.followerStore} {
		waitFor(t, 10*time.Second, func() bool {
			rows := inTable(s)
			later, ok := rows["later"]
			return len(rows) == 2 && rows["pinned"].HotOnly && ok && later.HotMaxAgeDays != nil && *later.HotMaxAgeDays == 9
		}, name+"'s table does not follow the cluster's policies")
	}
	if err := r.leaderStore.Delete(ctx, "later"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { _, ok := inTable(r.followerStore)["later"]; return !ok }, "a cluster delete left the row in the follower's table")
}

func TestNewTieringPolicySourceWithoutRaft(t *testing.T) {
	if NewTieringPolicySource(nil) != nil || NewTieringPolicySource(&Coordinator{}) != nil {
		t.Fatal("a node without Raft got a cluster policy source")
	}
}

func TestClassifyPolicyError(t *testing.T) {
	cases := []struct {
		err  error
		want error
	}{
		{fmt.Errorf("x: %w", raft.ErrPolicyNotFound), tiering.ErrPolicyNotFound},
		{&ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: "delete tiering policy for \"x\": policy not found"}, tiering.ErrPolicyNotFound},
		{fmt.Errorf("apply: %w", raft.ErrCommandRefused), tiering.ErrInvalidPolicy},
		{&ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: "set tiering policy: a database name of 1 to 256 bytes is required"}, tiering.ErrInvalidPolicy},
		{&ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: "failed to apply command: leadership lost"}, tiering.ErrClusterUnavailable},
		{ErrNoLeaderKnown, tiering.ErrClusterUnavailable},
	}
	for _, c := range cases {
		if got := classifyPolicyError(c.err); !errors.Is(got, c.want) {
			t.Errorf("%v -> %v, want %v", c.err, got, c.want)
		}
	}
	if classifyPolicyError(nil) != nil {
		t.Error("nil not nil")
	}
}

func TestAdoptBackoff(t *testing.T) {
	if adoptBackoff(1) != policyAdoptRetry || adoptBackoff(2) != 2*policyAdoptRetry || adoptBackoff(3) != 4*policyAdoptRetry {
		t.Fatalf("backoff %v %v %v", adoptBackoff(1), adoptBackoff(2), adoptBackoff(3))
	}
	if adoptBackoff(50) != policyAdoptRetryMax {
		t.Fatalf("uncapped: %v", adoptBackoff(50))
	}
}
