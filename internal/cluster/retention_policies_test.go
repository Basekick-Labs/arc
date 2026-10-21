package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func rrow(id int64, name string, days int, active bool, runs int64) RetentionLocalRow {
	return RetentionLocalRow{ID: id, Name: name, Database: "db", RetentionDays: days, IsActive: active, Executions: runs,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func rset(rows ...RetentionLocalRow) RetentionLocalRows { return RetentionLocalRows{Policies: rows} }

// The leader seeds on its own only when the nodes agree and every active
// policy has run: a policy that never deleted anything is never activated
// without an admin choosing it (D1).
func TestPlanRetentionSeedAutomatic(t *testing.T) {
	cases := []struct {
		name    string
		perNode map[string]RetentionLocalRows
		why     string // substring; "" seeds
		keep    bool
		floor   int64
		n       int
	}{
		{"every node empty", map[string]RetentionLocalRows{"a": rset(), "b": rset()}, "", true, 1, 0},
		{"one node, every active policy ran", map[string]RetentionLocalRows{
			"a": rset(rrow(3, "p1", 30, true, 4), rrow(5, "p2", 7, false, 0)), "b": rset()}, "", true, 6, 2},
		{"one node, an active policy that never ran", map[string]RetentionLocalRows{
			"a": rset(rrow(3, "p1", 30, true, 4), rrow(4, "p2", 7, true, 0)), "b": rset()}, `"p2" is active and has never run`, false, 0, 0},
		{"same policy on two nodes, ran on one", map[string]RetentionLocalRows{
			"a": rset(rrow(3, "p1", 30, true, 4)), "b": rset(rrow(1, "p1", 30, true, 0))}, "", false, 4, 1},
		{"same policy on two nodes, ran on neither", map[string]RetentionLocalRows{
			"a": rset(rrow(3, "p1", 30, true, 0)), "b": rset(rrow(1, "p1", 30, true, 0))}, "never run", false, 0, 0},
		{"two definitions of a name", map[string]RetentionLocalRows{
			"a": rset(rrow(3, "p1", 30, true, 4)), "b": rset(rrow(1, "p1", 90, true, 9))}, `different definitions of retention policy "p1"`, false, 0, 0},
		{"a row the cluster's rules refuse", map[string]RetentionLocalRows{
			"a": rset(RetentionLocalRow{ID: 1, Name: "old", Database: "db", RetentionDays: 3, BufferDays: 5, Executions: 2})}, "fix it on that node first", false, 0, 0},
		{"different policies on two nodes, both ran", map[string]RetentionLocalRows{
			"a": rset(rrow(1, "p1", 30, true, 1)), "b": rset(rrow(2, "p2", 30, true, 1))}, "", false, 3, 2},
		{"one source but another node uses the same ID", map[string]RetentionLocalRows{
			"a": rset(rrow(1, "p1", 30, true, 1)), "b": rset(rrow(1, "x", 30, false, 0))}, "", false, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed, why := planRetentionSeed(tc.perNode, nil)
			if tc.why != "" {
				if !strings.Contains(why, tc.why) {
					t.Fatalf("why = %q, want %q", why, tc.why)
				}
				return
			}
			if why != "" {
				t.Fatalf("refused: %s", why)
			}
			if seed.RetentionKeepIDs != tc.keep || seed.RetentionFloor != tc.floor || len(seed.Retention) != tc.n {
				t.Fatalf("seed keep=%v floor=%d n=%d, want %v %d %d: %+v", seed.RetentionKeepIDs, seed.RetentionFloor, len(seed.Retention), tc.keep, tc.floor, tc.n, seed.Retention)
			}
			if tc.keep {
				for _, e := range seed.Retention {
					if e.ID == 0 || e.ID != e.Sources[0].ID {
						t.Fatalf("kept seed entry without its ID: %+v", e)
					}
				}
			}
		})
	}
}

// Identical rows on two nodes are one policy built from both, so both nodes'
// histories move to it.
func TestPlanRetentionSeedSourcesEveryHolder(t *testing.T) {
	seed, why := planRetentionSeed(map[string]RetentionLocalRows{
		"a": rset(rrow(3, "p1", 30, true, 4)), "b": rset(rrow(1, "p1", 30, true, 0))}, nil)
	if why != "" || len(seed.Retention) != 1 {
		t.Fatalf("%s %+v", why, seed)
	}
	src := seed.Retention[0].Sources
	if len(src) != 2 || src[0] != (raft.RetentionSource{Node: "a", ID: 3}) || src[1] != (raft.RetentionSource{Node: "b", ID: 1}) {
		t.Fatalf("sources %+v", src)
	}
}

// An admin's choice seeds exactly the chosen node's rows, a policy that never
// ran included, and only if they still hash to what the admin saw.
func TestPlanRetentionSeedChoice(t *testing.T) {
	perNode := map[string]RetentionLocalRows{
		"a": rset(rrow(3, "p1", 30, true, 0), rrow(4, "p2", 7, true, 0)),
		"b": rset(rrow(1, "p1", 90, true, 9), rrow(9, "z", 9, false, 0)),
	}
	hash := RetentionRowsHash(perNode["a"].Policies)
	seed, why := planRetentionSeed(perNode, &raft.PolicyAdoptChoice{Kind: raft.PolicyKindRetention, Node: "a", Hash: hash})
	if why != "" {
		t.Fatalf("refused: %s", why)
	}
	if len(seed.Retention) != 2 || seed.Retention[0].Name != "p1" || seed.Retention[0].RetentionDays != 30 {
		t.Fatalf("seed %+v", seed.Retention)
	}
	// b's ID 9 is above a's: the floor clears every node's IDs, and a's IDs
	// (3, 4) are used by no other node, so they are kept.
	if seed.RetentionFloor != 10 || !seed.RetentionKeepIDs {
		t.Fatalf("floor %d keep %v", seed.RetentionFloor, seed.RetentionKeepIDs)
	}

	changed := map[string]RetentionLocalRows{"a": rset(rrow(3, "p1", 1, true, 0), rrow(4, "p2", 7, true, 0)), "b": perNode["b"]}
	if _, why := planRetentionSeed(changed, &raft.PolicyAdoptChoice{Node: "a", Hash: hash}); !strings.Contains(why, "changed since they were chosen") {
		t.Fatalf("a changed set adopted: %q", why)
	}
	if _, why := planRetentionSeed(map[string]RetentionLocalRows{"b": perNode["b"]}, &raft.PolicyAdoptChoice{Node: "a", Hash: hash}); !strings.Contains(why, "did not answer") {
		t.Fatalf("missing chosen node: %q", why)
	}
	// The hash ignores run counts and timestamps but not IDs or definitions.
	again := perNode["a"].Policies
	again[0].Executions, again[0].CreatedAt = 99, time.Now()
	if RetentionRowsHash(again) != hash {
		t.Fatal("hash changed with run count / timestamp")
	}
	again[0].ID = 30
	if RetentionRowsHash(again) == hash {
		t.Fatal("hash ignored the ID")
	}
}

// enableRetention turns retention's own flag on in the rig, and gives each
// node a lister answering rows[node]; seen records, per call, whether a
// collect of retention was open on that node when it was asked.
func (r *adoptRig) enableRetention(t *testing.T, rows map[string]*RetentionLocalRows) (seen *[]bool) {
	t.Helper()
	raw, _ := json.Marshal(raft.EnableRetentionPoliciesPayload{Nodes: []string{"L", "F"}, Now: time.Now()})
	if err := r.leader.raftNode.Apply(&raft.Command{Type: raft.CommandEnableRetentionPolicies, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.fsmOrNil().RetentionPoliciesEnabled() }, "follower never saw the retention flag")
	var mu sync.Mutex
	var calls []bool
	for id, c := range map[string]*Coordinator{"L": r.leader, "F": r.follower} {
		id, c := id, c
		c.SetLocalPolicyLister(raft.PolicyKindRetention, func(context.Context) (json.RawMessage, error) {
			mu.Lock()
			calls = append(calls, c.PolicyCollecting(raft.PolicyKindRetention))
			mu.Unlock()
			return json.Marshal(rows[id])
		})
	}
	return &calls
}

// A merge that would refuse (a policy that never ran) is found by a probe,
// so no collect, and no window of refused writes, opens for it at any retry.
func TestAdoptRetention_ProbeAwaitsWithoutACollect(t *testing.T) {
	r := newAdoptRig(t)
	seen := r.enableRetention(t, map[string]*RetentionLocalRows{
		"L": {Policies: []RetentionLocalRow{rrow(1, "p1", 30, true, 0)}},
		"F": {},
	})
	ctx := context.Background()
	r.leader.maybeSeedPolicies(ctx)
	if r.leader.RetentionPoliciesSeeded() {
		t.Fatal("seeded a policy that never ran")
	}
	st := r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindRetention]
	if st.Seeded || st.Collecting || !strings.Contains(st.LastError, "never run") || st.Attempts != 1 {
		t.Fatalf("status %+v", st)
	}
	if len(*seen) != 2 {
		t.Fatalf("%d lister calls, want one probe per node", len(*seen))
	}
	for _, open := range *seen {
		if open {
			t.Fatal("a collect was open while the merge could only refuse")
		}
	}
	// Tiering, enabled with it, still seeds on its own.
	if !r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatal("tiering did not seed")
	}
}

// Every active policy ran on some node: the leader seeds on its own, keeping
// the IDs of the only node that held policies.
func TestAdoptRetention_SeedsWhenProven(t *testing.T) {
	r := newAdoptRig(t)
	r.enableRetention(t, map[string]*RetentionLocalRows{
		"L": {Policies: []RetentionLocalRow{rrow(4, "p1", 30, true, 3), rrow(6, "off", 9, false, 0)}},
		"F": {},
	})
	r.leader.maybeSeedPolicies(context.Background())
	if !r.leader.RetentionPoliciesSeeded() {
		t.Fatalf("not seeded: %+v", r.leader.ClusterPoliciesStatus())
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.RetentionPoliciesSeeded() }, "follower never saw the seed")
	if p, ok := r.follower.RetentionPolicy(4); !ok || p.Name != "p1" || !p.IsActive {
		t.Fatalf("follower reads %+v %v", p, ok)
	}
	if st := r.follower.RetentionMirrorState(); st.Floor != 7 || !st.SeededIDs[4] || !st.SeededIDs[6] {
		t.Fatalf("mirror state %+v", st)
	}
}

// Nodes that disagree wait for an admin. The choice is taken on the leader
// only, seeds the chosen node's rows if they still hash to what the admin
// saw, and a stale hash ends the choice without seeding.
func TestAdoptRetention_AdminChoice(t *testing.T) {
	r := newAdoptRig(t)
	rows := map[string]*RetentionLocalRows{
		"L": {Policies: []RetentionLocalRow{rrow(1, "p1", 30, true, 5)}},
		"F": {Policies: []RetentionLocalRow{rrow(1, "p1", 90, true, 0), rrow(2, "p2", 7, true, 0)}},
	}
	r.enableRetention(t, rows)
	ctx := context.Background()
	r.leader.maybeSeedPolicies(ctx)
	if r.leader.RetentionPoliciesSeeded() {
		t.Fatal("seeded two definitions of p1")
	}
	cand, err := r.leader.RetentionCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f := cand.Nodes["F"]
	if f.Error != "" || len(f.Policies) != 2 || f.Hash != RetentionRowsHash(rows["F"].Policies) || cand.Awaiting == nil {
		t.Fatalf("candidates %+v", cand)
	}
	if _, err := r.follower.RetentionCandidates(ctx); !errors.Is(err, ErrNotLeaderForTopology) {
		t.Fatalf("candidates on the follower: %v", err)
	}
	if err := r.follower.ChooseRetentionAdoption("F", f.Hash, "admin"); !errors.Is(err, ErrNotLeaderForTopology) {
		t.Fatalf("choice on the follower: %v", err)
	}

	// The node's policies change after the admin looked: nothing is adopted.
	rows["F"].Policies[0].RetentionDays = 1
	if err := r.leader.ChooseRetentionAdoption("F", f.Hash, "admin"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, pending := r.leader.fsmOrNil().PolicyAdoptChoiceFor(raft.PolicyKindRetention)
		return !pending
	}, "the stale choice was never tried")
	if r.leader.RetentionPoliciesSeeded() {
		t.Fatal("adopted policies the admin never saw")
	}
	if st := r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindRetention]; !strings.Contains(st.LastError, "changed since they were chosen") {
		t.Fatalf("status %+v", st)
	}

	// Chosen again with the current hash: the follower's policies, the one
	// that never ran included, are adopted, by the admin's choice.
	if err := r.leader.ChooseRetentionAdoption("F", RetentionRowsHash(rows["F"].Policies), "admin"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return r.follower.RetentionPoliciesSeeded() }, "the choice never seeded")
	p1, _ := r.leader.RetentionPolicyByName("p1")
	p2, ok := r.leader.RetentionPolicyByName("p2")
	// L also used ID 1: the IDs are renumbered above every node's.
	if p1.RetentionDays != 1 || !ok || p1.ID < 3 || p2.ID < 3 {
		t.Fatalf("adopted %+v %+v", p1, p2)
	}
	if err := r.leader.ChooseRetentionAdoption("F", "x", "admin"); !errors.Is(err, ErrPolicyAdoptionNotNeeded) {
		t.Fatalf("choice after the seed: %v", err)
	}
}

// A collect left open by a leader that died mid-collect is replaced at once,
// not probed around: only a collect closes it, and until then every node
// refuses local retention writes.
func TestAdoptRetention_StaleCollectIsReplaced(t *testing.T) {
	r := newAdoptRig(t)
	r.enableRetention(t, map[string]*RetentionLocalRows{
		"L": {Policies: []RetentionLocalRow{rrow(1, "p1", 30, true, 0)}},
		"F": {},
	})
	if _, err := r.leader.proposeLeader(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindRetention, Leader: "gone", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	r.leader.maybeSeedPolicies(context.Background())
	if r.leader.RetentionPoliciesCollecting() {
		t.Fatal("the stale collect is still open: local writes stay refused")
	}
	if st := r.leader.ClusterPoliciesStatus().Kinds[raft.PolicyKindRetention]; !strings.Contains(st.LastError, "never run") {
		t.Fatalf("status %+v", st)
	}
}

// Before retention's own flag is on (a node still on a tiering-only build),
// the adoption loop leaves retention alone: no node is asked for its rows
// and nothing is recorded.
func TestAdoptRetention_NotBeforeItsFlag(t *testing.T) {
	r := newAdoptRig(t)
	var calls atomic.Int32
	for _, c := range []*Coordinator{r.leader, r.follower} {
		c.SetLocalPolicyLister(raft.PolicyKindRetention, func(context.Context) (json.RawMessage, error) {
			calls.Add(1)
			return json.RawMessage(`{"policies":[]}`), nil
		})
	}
	r.leader.maybeSeedPolicies(context.Background())
	if n := calls.Load(); n != 0 {
		t.Fatalf("%d retention listers called before the flag", n)
	}
	if _, ok := r.leader.fsmOrNil().PolicyAwaitingAdoption(raft.PolicyKindRetention); ok {
		t.Fatal("retention adoption recorded before its flag")
	}
	if !r.leader.PoliciesSeeded(raft.PolicyKindTiering) {
		t.Fatal("tiering did not seed")
	}
}

// Adoption passes run one at a time: an admin's nudge waits for the loop's
// pass instead of racing it.
func TestAdoptionPassesAreSerialized(t *testing.T) {
	r := newAdoptRig(t)
	r.leader.adoptMu.Lock()
	done := make(chan struct{})
	go func() { r.leader.maybeSeedPolicies(context.Background()); close(done) }()
	select {
	case <-done:
		r.leader.adoptMu.Unlock()
		t.Fatal("a second adoption pass ran while one held the lock")
	case <-time.After(300 * time.Millisecond):
	}
	r.leader.adoptMu.Unlock()
	<-done
	if err := r.leader.ChooseRetentionAdoption("nope", "h", "admin"); !errors.Is(err, ErrRetentionPoliciesNotEnabled) {
		t.Fatalf("choice before the retention flag: %v", err)
	}
	r.enableRetention(t, map[string]*RetentionLocalRows{"L": {}, "F": {Policies: []RetentionLocalRow{rrow(1, "p", 9, true, 0)}}})
	if err := r.leader.ChooseRetentionAdoption("nope", "h", "admin"); !errors.Is(err, ErrUnknownPolicyNode) {
		t.Fatalf("choice of an unknown node: %v", err)
	}
}

// A mirror that fails is tried again without waiting for a policy change.
func TestRunPolicyMirrorRetriesAfterAnError(t *testing.T) {
	prev := policyMirrorRetry
	policyMirrorRetry = 20 * time.Millisecond
	t.Cleanup(func() { policyMirrorRetry = prev })
	r := newAdoptRig(t)
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.follower.ctx = ctx
	r.follower.RunPolicyMirror("retention", func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("database is locked")
		}
		return nil
	})
	waitFor(t, 5*time.Second, func() bool { return calls.Load() >= 3 }, "the failed mirror was never retried")
}
