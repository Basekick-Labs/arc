package governance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// govFSM is a real cluster state with governance enabled, for the view.
type govFSM struct {
	t   testing.TB
	f   *raft.ClusterFSM
	idx uint64
}

func newGovFSM(t testing.TB) *govFSM {
	t.Helper()
	g := &govFSM{t: t, f: raft.NewClusterFSM(zerolog.Nop())}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g.ok(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: "w1", Role: "writer", State: "healthy"}})
	g.ok(raft.CommandEnableClusterPolicies, raft.EnableClusterPoliciesPayload{Nodes: []string{"w1"}, Now: now})
	g.ok(raft.CommandEnableGovernancePolicies, raft.EnableGovernancePoliciesPayload{Nodes: []string{"w1"}, Now: now})
	return g
}

func (g *govFSM) apply(typ raft.CommandType, p any) interface{} {
	raw, _ := json.Marshal(p)
	data, _ := json.Marshal(raft.Command{Type: typ, Payload: raw})
	g.idx++
	return g.f.Apply(&hraft.Log{Index: g.idx, Data: data})
}

func (g *govFSM) ok(typ raft.CommandType, p any) uint64 {
	g.t.Helper()
	if res := g.apply(typ, p); res != nil {
		g.t.Fatalf("command %d: %v", typ, res)
	}
	return g.idx
}

// token creates cluster token name; returns its ID and the fingerprint of its
// stored hash.
func (g *govFSM) token(name string) (int64, string) {
	g.t.Helper()
	hash := "hash-" + name
	id := int64(g.ok(raft.CommandCreateToken, raft.CreateTokenPayload{Token: raft.TokenEntry{Name: name, Permissions: "read",
		TokenHash: hash, TokenPrefix: "p-" + name, CreatedAtUnixNano: 1, Enabled: true}}))
	return id, auth.TokenFingerprint(hash)
}

func (g *govFSM) seed(entries ...raft.GovernancePolicyEntry) {
	g.t.Helper()
	ep := g.ok(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindGovernance, Leader: "w1"})
	g.ok(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindGovernance, Epoch: ep, GovernanceFloor: 1, Governance: entries})
}

func (g *govFSM) set(token int64, create bool, rows, perMin int) {
	g.t.Helper()
	g.ok(raft.CommandSetGovernancePolicy, raft.SetGovernancePolicyPayload{TokenID: token, Create: create, MaxRowsPerQuery: rows, RateLimitPerMinute: perMin})
}

// fsmSource is the cluster source over a govFSM, as the coordinator's.
type fsmSource struct {
	g          *govFSM
	collecting bool
	mu         sync.Mutex
}

func (s *fsmSource) Seeded() bool     { return s.g.f.PoliciesSeeded(raft.PolicyKindGovernance) }
func (s *fsmSource) Collecting() bool { return s.collecting }
func (s *fsmSource) Get(id int64) (*Policy, bool) {
	e, ok := s.g.f.GovernancePolicy(id)
	if !ok {
		return nil, false
	}
	return PolicyFromEntry(e), true
}
func (s *fsmSource) Set(_ context.Context, p *Policy, create bool, fp string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h, ok := s.g.f.GovernanceTokenHash(p.TokenID); !ok || auth.TokenFingerprint(h) != fp {
		return ErrTokenNotCluster
	}
	if res := s.g.apply(raft.CommandSetGovernancePolicy, raft.SetGovernancePolicyPayload{TokenID: p.TokenID, Create: create,
		RateLimitPerMinute: p.RateLimitPerMinute, MaxRowsPerQuery: p.MaxRowsPerQuery}); res != nil {
		return res.(error)
	}
	return nil
}
func (s *fsmSource) Delete(_ context.Context, id int64, fp string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res := s.g.apply(raft.CommandDeleteGovernancePolicy, raft.DeleteGovernancePolicyPayload{TokenID: id}); res != nil {
		return res.(error)
	}
	return nil
}

func openDB(t testing.TB) *sql.DB {
	t.Helper()
	metrics.Init(zerolog.Nop())
	db, err := sql.Open("sqlite3", "file:"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func clusterManager(t testing.TB, db *sql.DB, cfg *config.GovernanceConfig) *Manager {
	t.Helper()
	if cfg == nil {
		cfg = &config.GovernanceConfig{}
	}
	m, err := NewManager(&ManagerConfig{DB: db, Config: cfg, Logger: zerolog.Nop(), Cluster: true})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// wire is what the coordinator's UseGovernance does, over a govFSM.
func wire(m *Manager, g *govFSM) *fsmSource {
	src := &fsmSource{g: g}
	m.SetClusterSource(src)
	g.f.AddPoliciesChangedCallback(func() { m.SyncClusterView(g.f) })
	m.SyncClusterView(g.f)
	return src
}

var defaults10 = &config.GovernanceConfig{DefaultMaxRowsPerQuery: 10}

// Seeded, the cluster state is the policy: a write applied anywhere is
// enforced at once, a delete falls back to the defaults, and a token whose
// stored hash is not the cluster token's gets the defaults.
func TestClusterViewIsEnforced(t *testing.T) {
	g := newGovFSM(t)
	a, fpA := g.token("a")
	m := clusterManager(t, openDB(t), defaults10)
	wire(m, g)
	if r := m.AdmitToken(a, fpA); r.MaxRows != 10 {
		t.Fatalf("before the seed: max rows %d, want the defaults' 10", r.MaxRows)
	}
	g.seed()
	g.set(a, true, 500, 0)
	if r := m.AdmitToken(a, fpA); r.MaxRows != 500 {
		t.Fatalf("after a Set: max rows %d, want 500", r.MaxRows)
	}
	if r := m.AdmitToken(a, "another-token's-fingerprint"); r.MaxRows != 10 {
		t.Fatalf("another token at the same ID: max rows %d, want the defaults' 10", r.MaxRows)
	}
	if r := m.AdmitToken(a, ""); r.MaxRows != 10 {
		t.Fatalf("no fingerprint: max rows %d, want the defaults' 10", r.MaxRows)
	}
	if p, _ := m.GetPolicyFor(context.Background(), a, fpA); p == nil || p.MaxRowsPerQuery != 500 {
		t.Fatalf("GET %+v", p)
	}
	if p := m.GetCachedPolicyFor(a, "x"); p == nil || p.MaxRowsPerQuery != 10 {
		t.Fatalf("usage policy for a mismatched token %+v, want the defaults", p)
	}
	g.ok(raft.CommandDeleteGovernancePolicy, raft.DeleteGovernancePolicyPayload{TokenID: a})
	if r := m.AdmitToken(a, fpA); r.MaxRows != 10 {
		t.Fatalf("after a delete: max rows %d, want the defaults' 10", r.MaxRows)
	}
}

// Once the cluster owns the policies, this node's table is never read: a row
// written into it behind the cluster's back is neither enforced nor listed.
func TestClusterOwnedTableIsNeverRead(t *testing.T) {
	g := newGovFSM(t)
	a, fpA := g.token("a")
	db := openDB(t)
	m := clusterManager(t, db, nil)
	wire(m, g)
	g.seed(raft.GovernancePolicyEntry{TokenID: a, MaxRowsPerQuery: 7})
	if _, err := db.Exec(`INSERT INTO governance_policies (token_id, max_rows_per_query) VALUES (?, 999)`, a); err != nil {
		t.Fatal(err)
	}
	m.policiesMu.Lock()
	m.policies[a] = &Policy{TokenID: a, MaxRowsPerQuery: 999}
	m.policiesMu.Unlock()
	if r := m.AdmitToken(a, fpA); r.MaxRows != 7 {
		t.Fatalf("max rows %d, want the cluster's 7", r.MaxRows)
	}
	list, err := m.ListPolicies(context.Background())
	if err != nil || len(list) != 1 || list[0].MaxRowsPerQuery != 7 {
		t.Fatalf("list %+v %v", list, err)
	}
	// A token whose local policy the cluster did not adopt (or deleted
	// since) gets the defaults, not what this node loaded before it.
	b, fpB := g.token("b")
	m.policiesMu.Lock()
	m.policies[b] = &Policy{TokenID: b, MaxRowsPerQuery: 999}
	m.policiesMu.Unlock()
	if r := m.AdmitToken(b, fpB); r.MaxRows != 0 {
		t.Fatalf("max rows %d for a token without a cluster policy, want none", r.MaxRows)
	}
}

// A restart of a node whose table the cluster owns: until its cluster state
// holds the seeded kind, only the defaults apply (never the table, which may
// be stale), writes and lists answer ErrNotCaughtUp; once it catches up, the
// cluster's policies apply. A standalone node ignores the marker.
func TestOwnedNodeBootsOnDefaults(t *testing.T) {
	g := newGovFSM(t)
	a, fpA := g.token("a")
	g.seed(raft.GovernancePolicyEntry{TokenID: a, MaxRowsPerQuery: 7})
	db := openDB(t)
	first := clusterManager(t, db, defaults10)
	if err := first.MirrorClusterPolicies(context.Background(), g.f); err != nil {
		t.Fatal(err)
	}
	// The policy is deleted while this node is down: its table still has it.
	g.ok(raft.CommandDeleteGovernancePolicy, raft.DeleteGovernancePolicyPayload{TokenID: a})

	behind := newGovFSM(t) // a cluster state that has not replayed the seed yet
	m := clusterManager(t, db, defaults10)
	src := wire(m, behind)
	if r := m.AdmitToken(a, fpA); r.MaxRows != 10 {
		t.Fatalf("not caught up: max rows %d, want the defaults' 10, not the stale table's 7", r.MaxRows)
	}
	if _, err := m.ListPolicies(context.Background()); !errors.Is(err, ErrNotCaughtUp) {
		t.Fatalf("list: %v", err)
	}
	if _, err := m.GetPolicyFor(context.Background(), a, fpA); !errors.Is(err, ErrNotCaughtUp) {
		t.Fatalf("get: %v", err)
	}
	if _, err := m.CreatePolicyFor(context.Background(), &Policy{TokenID: a}, fpA); !errors.Is(err, ErrNotCaughtUp) {
		t.Fatalf("write: %v", err)
	}
	_ = src
	// The cluster state catches up (its seed, then a new policy).
	b, fpB := behind.token("b")
	behind.seed(raft.GovernancePolicyEntry{TokenID: b, MaxRowsPerQuery: 3})
	if r := m.AdmitToken(b, fpB); r.MaxRows != 3 {
		t.Fatalf("caught up: max rows %d, want 3", r.MaxRows)
	}

	solo, err := NewManager(&ManagerConfig{DB: db, Config: defaults10, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	if r := solo.Admit(a); r.MaxRows != 7 {
		t.Fatalf("standalone on the backup copy: max rows %d, want the table's 7", r.MaxRows)
	}
}

// A write once seeded goes to the cluster and answers the stored row (the
// cluster's ID); a create for a token that is not the cluster's is refused;
// a collect refuses local writes.
func TestClusterWrites(t *testing.T) {
	g := newGovFSM(t)
	a, fpA := g.token("a")
	m := clusterManager(t, openDB(t), nil)
	src := wire(m, g)
	src.collecting = true
	if _, err := m.CreatePolicyFor(context.Background(), &Policy{TokenID: a}, fpA); !errors.Is(err, ErrPoliciesBeingAdopted) {
		t.Fatalf("during a collect: %v", err)
	}
	src.collecting = false
	g.seed()
	p, err := m.CreatePolicyFor(context.Background(), &Policy{TokenID: a, MaxRowsPerQuery: 5}, fpA)
	if err != nil || p.ID != 1 || p.MaxRowsPerQuery != 5 {
		t.Fatalf("create %+v %v", p, err)
	}
	if _, err := m.CreatePolicyFor(context.Background(), &Policy{TokenID: a}, "not-a"); !errors.Is(err, ErrTokenNotCluster) {
		t.Fatalf("create for another token: %v", err)
	}
	if n, _ := countRows(m.db); n != 0 {
		t.Fatalf("a cluster write touched this node's table (%d rows)", n)
	}
	if err := m.DeletePolicyFor(context.Background(), a, fpA); err != nil {
		t.Fatal(err)
	}
	if r := m.AdmitToken(a, fpA); r.MaxRows != 0 {
		t.Fatalf("after the cluster delete: %+v", r)
	}
}

func countRows(db *sql.DB) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM governance_policies`).Scan(&n)
	return n, err
}

// The view is rebuilt on the FSM goroutine without any of the manager's
// locks: an apply completes while a write holds policyWriteMu and a query
// holds admitMu (the deadlock review 2 found in the plan's first design).
func TestViewRebuildTakesNoManagerLock(t *testing.T) {
	g := newGovFSM(t)
	a, fpA := g.token("a")
	m := clusterManager(t, openDB(t), nil)
	wire(m, g)
	g.seed()
	built := make(chan struct{}, 1)
	m.cluster.afterViewBuildForTest = func() { built <- struct{}{} }
	m.policyWriteMu.Lock()
	m.admitMu.Lock()
	done := make(chan struct{})
	go func() {
		g.set(a, true, 9, 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the apply waited on the manager's locks")
	}
	m.admitMu.Unlock()
	m.policyWriteMu.Unlock()
	<-built
	if r := m.AdmitToken(a, fpA); r.MaxRows != 9 {
		t.Fatalf("max rows %d, want 9", r.MaxRows)
	}
}

// A changed limit reaches the token's counters at its next query, without
// resetting what was counted.
func TestClusterLimitChangeKeepsTheCount(t *testing.T) {
	g := newGovFSM(t)
	a, fpA := g.token("a")
	m := clusterManager(t, openDB(t), nil)
	wire(m, g)
	g.seed(raft.GovernancePolicyEntry{TokenID: a, RateLimitPerMinute: 2})
	for i := 0; i < 2; i++ {
		if !m.AdmitToken(a, fpA).Allowed {
			t.Fatalf("query %d refused", i+1)
		}
	}
	if m.AdmitToken(a, fpA).Allowed {
		t.Fatal("third query admitted at 2 per minute")
	}
	g.set(a, false, 0, 3)
	if !m.AdmitToken(a, fpA).Allowed {
		t.Fatal("refused after the limit rose to 3")
	}
	if m.AdmitToken(a, fpA).Allowed {
		t.Fatal("admitted a fourth: the raise reset the count")
	}
}

// The mirror makes the table the cluster's and marks it; a pass with nothing
// to change writes nothing.
func TestMirrorWritesOnlyChanges(t *testing.T) {
	g := newGovFSM(t)
	a, _ := g.token("a")
	b, _ := g.token("b")
	db := openDB(t)
	m := clusterManager(t, db, nil)
	if _, err := db.Exec(`INSERT INTO governance_policies (token_id, max_rows_per_query) VALUES (99, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := m.MirrorClusterPolicies(context.Background(), g.f); err != nil {
		t.Fatal(err)
	}
	if n, _ := countRows(db); n != 1 {
		t.Fatalf("before the seed the mirror changed the table (%d rows)", n)
	}
	// This node's own policy for token a, adopted as it is: not dormant.
	if _, err := db.Exec(`INSERT INTO governance_policies (token_id, max_rows_per_query) VALUES (?, 1)`, a); err != nil {
		t.Fatal(err)
	}
	g.seed(raft.GovernancePolicyEntry{TokenID: a, MaxRowsPerQuery: 1}, raft.GovernancePolicyEntry{TokenID: b, MaxScanDurationSec: 4})
	var logged bytes.Buffer
	m.logger = zerolog.New(&logged)
	if err := m.MirrorClusterPolicies(context.Background(), g.f); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logged.String(), `"token_ids":[99]`) {
		t.Fatalf("the dormant policies' tokens are not logged: %s", logged.String())
	}
	rows, err := ReadLocalRows(context.Background(), db)
	if err != nil || len(rows.Policies) != 2 || rows.Policies[0].TokenID != a || rows.Policies[1].MaxScanDurationSec != 4 {
		t.Fatalf("mirrored %+v %v", rows, err)
	}
	if owned, _ := readOwnedMarker(db); !owned {
		t.Fatal("no owned marker")
	}
	// This node's own row the cluster did not adopt (token 99) is kept,
	// dormant, never enforced.
	var dormant, dormantTok int64
	db.QueryRow(`SELECT COUNT(*), COALESCE(MAX(token_id), 0) FROM governance_policies_dormant`).Scan(&dormant, &dormantTok)
	if dormant != 1 || dormantTok != 99 {
		t.Fatalf("dormant rows %d (token %d), want token 99's", dormant, dormantTok)
	}
	if _, err := db.Exec(`CREATE TABLE writes (n INTEGER); CREATE TRIGGER count_del AFTER DELETE ON governance_policies BEGIN INSERT INTO writes VALUES (1); END;
		CREATE TRIGGER count_ins AFTER INSERT ON governance_policies BEGIN INSERT INTO writes VALUES (1); END;`); err != nil {
		t.Fatal(err)
	}
	if err := m.MirrorClusterPolicies(context.Background(), g.f); err != nil {
		t.Fatal(err)
	}
	var writes int
	db.QueryRow(`SELECT COUNT(*) FROM writes`).Scan(&writes)
	if writes != 0 {
		t.Fatalf("an unchanged pass wrote %d rows", writes)
	}
	g.set(a, false, 2, 0)
	if err := m.MirrorClusterPolicies(context.Background(), g.f); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(`SELECT COUNT(*) FROM writes`).Scan(&writes)
	if writes == 0 {
		t.Fatal("a changed policy was not mirrored")
	}
	db.QueryRow(`SELECT COUNT(*) FROM governance_policies_dormant`).Scan(&dormant)
	if dormant != 1 {
		t.Fatalf("a later pass moved the cluster's own rows to dormant (%d rows)", dormant)
	}
}

// A node reports, per policy, whether its token is here, the cluster's, and
// its hash's fingerprint; with no token table, that it cannot tell.
func TestReadLocalRowsReportsTheToken(t *testing.T) {
	db := openDB(t)
	m := clusterManager(t, db, nil)
	for _, tok := range []int64{1, 2, 3} {
		if _, err := db.Exec(`INSERT INTO governance_policies (token_id) VALUES (?)`, tok); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := m.LocalPolicyRows(context.Background())
	if err != nil || len(rows.Policies) != 3 || rows.Policies[0].Token != TokenUnknown || rows.Defaults == nil {
		t.Fatalf("no token table: %+v %v", rows, err)
	}
	if _, err := db.Exec(`CREATE TABLE api_tokens (id INTEGER PRIMARY KEY, token_hash TEXT, raft_materialised INTEGER DEFAULT 0);
		INSERT INTO api_tokens VALUES (1, 'h1', 1), (2, 'h2', 0)`); err != nil {
		t.Fatal(err)
	}
	rows, err = ReadLocalRows(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	r := rows.Policies
	if r[0].Token != TokenPresent || !r[0].RaftMaterialised || r[0].TokenFP != auth.TokenFingerprint("h1") ||
		r[1].Token != TokenPresent || r[1].RaftMaterialised || r[2].Token != TokenAbsent || rows.MaxIssuedID != 3 {
		t.Fatalf("token reports %+v (max %d)", r, rows.MaxIssuedID)
	}
}

// The adoption's read of this node's policies takes the lock local writes
// hold: a write in progress is either in the answer or, once the collect is
// open, refused (this is the first half).
func TestLocalRowsWaitForAWriteInProgress(t *testing.T) {
	m := clusterManager(t, openDB(t), nil)
	inWrite := make(chan struct{})
	release := make(chan struct{})
	m.afterPolicyWrite = func(string) {
		close(inWrite)
		<-release
	}
	go m.CreatePolicy(context.Background(), &Policy{TokenID: 4, MaxRowsPerQuery: 1})
	<-inWrite
	got := make(chan LocalRows, 1)
	go func() {
		rows, _ := m.LocalPolicyRows(context.Background())
		got <- rows
	}()
	select {
	case <-got:
		t.Fatal("the adoption read this node's policies while a write was in progress")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if rows := <-got; len(rows.Policies) != 1 || rows.Policies[0].TokenID != 4 {
		t.Fatalf("the read after the write: %+v", rows)
	}
}

// Reading the cluster's policies adds no allocation to Admit: the seeded path
// allocates exactly what the standalone path does.
func TestSeededAdmitAddsNoAllocation(t *testing.T) {
	g := newGovFSM(t)
	a, fpA := g.token("a")
	m := clusterManager(t, openDB(t), nil)
	wire(m, g)
	g.seed(raft.GovernancePolicyEntry{TokenID: a, RateLimitPerMinute: 1 << 30, MaxQueriesPerDay: 1 << 30, MaxRowsPerQuery: 5})
	solo := setupTestManager(t, nil)
	solo.CreatePolicy(context.Background(), &Policy{TokenID: 1, RateLimitPerMinute: 1 << 30, MaxQueriesPerDay: 1 << 30, MaxRowsPerQuery: 5})
	seeded := testing.AllocsPerRun(200, func() { m.AdmitToken(a, fpA) })
	standalone := testing.AllocsPerRun(200, func() { solo.Admit(1) })
	if seeded != standalone {
		t.Fatalf("seeded Admit allocates %.0f, standalone %.0f", seeded, standalone)
	}
}
