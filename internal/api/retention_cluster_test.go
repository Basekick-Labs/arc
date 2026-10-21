package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// fsmSource is a RetentionClusterSource over a real cluster FSM, applying
// commands directly (a single-node cluster without Raft).
type fsmSource struct {
	t          *testing.T
	f          *raft.ClusterFSM
	idx        uint64
	self       string
	collecting bool
}

func newFSMSource(t *testing.T, self string) *fsmSource {
	s := &fsmSource{t: t, f: raft.NewClusterFSM(zerolog.Nop()), self: self}
	s.must(raft.CommandEnableClusterPolicies, raft.EnableClusterPoliciesPayload{})
	s.must(raft.CommandEnableRetentionPolicies, raft.EnableRetentionPoliciesPayload{})
	return s
}

func (s *fsmSource) apply(typ raft.CommandType, p any) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	data, err := json.Marshal(&raft.Command{Type: typ, Payload: raw})
	if err != nil {
		return err
	}
	s.idx++
	if res, ok := s.f.Apply(&hraft.Log{Index: s.idx, Data: data}).(error); ok {
		return res
	}
	return nil
}

func (s *fsmSource) must(typ raft.CommandType, p any) {
	s.t.Helper()
	if err := s.apply(typ, p); err != nil {
		s.t.Fatalf("command %d: %v", typ, err)
	}
}

func (s *fsmSource) seed(keep bool, floor int64, entries ...raft.RetentionPolicyEntry) {
	s.t.Helper()
	s.must(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindRetention, Leader: s.self})
	s.must(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindRetention, Epoch: s.idx,
		Retention: entries, RetentionKeepIDs: keep, RetentionFloor: floor, Now: time.Now().UTC()})
}

func (s *fsmSource) RetentionPoliciesSeeded() bool {
	return s.f.RetentionPoliciesEnabled() && s.f.PoliciesSeeded(raft.PolicyKindRetention)
}
func (s *fsmSource) RetentionPoliciesCollecting() bool { return s.collecting }
func (s *fsmSource) RetentionPolicy(id int64) (raft.RetentionPolicyEntry, bool) {
	return s.f.RetentionPolicy(id)
}
func (s *fsmSource) RetentionPolicyByName(n string) (raft.RetentionPolicyEntry, bool) {
	return s.f.RetentionPolicyByName(n)
}
func (s *fsmSource) RetentionPolicies() []raft.RetentionPolicyEntry { return s.f.RetentionPolicies() }
func (s *fsmSource) CreateRetentionPolicy(_ context.Context, p raft.CreateRetentionPolicyPayload) error {
	p.Now = time.Now().UTC()
	return s.apply(raft.CommandCreateRetentionPolicy, p)
}
func (s *fsmSource) UpdateRetentionPolicy(_ context.Context, p raft.UpdateRetentionPolicyPayload) error {
	p.Now = time.Now().UTC()
	return s.apply(raft.CommandUpdateRetentionPolicy, p)
}
func (s *fsmSource) DeleteRetentionPolicy(_ context.Context, id int64) error {
	return s.apply(raft.CommandDeleteRetentionPolicy, raft.DeleteRetentionPolicyPayload{ID: id})
}
func (s *fsmSource) RetentionMirrorState() raft.RetentionMirror { return s.f.RetentionMirrorState() }
func (s *fsmSource) LocalNodeID() string                        { return s.self }

// clusterRetentionRig: a retention handler on the shared auth database (one
// connection, foreign keys on: the production default), with its routes.
type clusterRetentionRig struct {
	h   *RetentionHandler
	src *fsmSource
	app *fiber.App
}

func newClusterRetentionRig(t *testing.T) *clusterRetentionRig {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "arc.db")
	am, err := auth.NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	h, err := NewRetentionHandler(nil, nil, &config.RetentionConfig{Enabled: true, DBPath: dbPath}, nil, am, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := newFSMSource(t, "n1")
	h.SetClusterSource(src)
	app := fiber.New()
	app.Get("/retention", h.handleList)
	app.Get("/retention/:id", h.handleGet)
	app.Post("/retention", h.handleCreate)
	app.Put("/retention/:id", h.handleUpdate)
	app.Delete("/retention/:id", h.handleDelete)
	return &clusterRetentionRig{h: h, src: src, app: app}
}

func (r *clusterRetentionRig) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, err := r.h.db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func (r *clusterRetentionRig) count(t *testing.T, q string, args ...any) int {
	t.Helper()
	var n int
	if err := r.h.db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func (r *clusterRetentionRig) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// localPolicy inserts one of this node's own rows, with runs executions.
func (r *clusterRetentionRig) localPolicy(t *testing.T, id int64, name string, runs int) {
	t.Helper()
	r.exec(t, `INSERT INTO retention_policies (id, name, database, retention_days, buffer_days, is_active) VALUES (?, ?, 'db', 30, 0, 1)`, id, name)
	for i := 0; i < runs; i++ {
		r.exec(t, `INSERT INTO retention_executions (policy_id, status) VALUES (?, 'completed')`, id)
	}
}

// While the cluster collects the nodes' policies, a local write is refused
// with a 503 instead of landing after the collect read the table.
func TestRetentionLocalWriteRefusedWhileCollecting(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.src.collecting = true
	code, body := r.do(t, "POST", "/retention", `{"name":"p","database":"db","retention_days":30}`)
	if code != fiber.StatusServiceUnavailable || !strings.Contains(body["error"].(string), "being adopted") {
		t.Fatalf("create while collecting: %d %v", code, body)
	}
	r.src.collecting = false
	if code, body := r.do(t, "POST", "/retention", `{"name":"p","database":"db","retention_days":30}`); code != fiber.StatusCreated {
		t.Fatalf("local create before the seed: %d %v", code, body)
	}
}

// Once adopted, the API reads and writes the cluster's policies: a create
// answers with the cluster's ID, a partial PUT merges, a rename onto another
// name is 400, a missing policy is 404, and this node's history is joined.
func TestRetentionAPIOnceAdopted(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.src.seed(false, 50)
	code, body := r.do(t, "POST", "/retention", `{"name":"p1","database":"db","measurement":"cpu","retention_days":30,"buffer_days":2}`)
	if code != fiber.StatusCreated || body["id"].(float64) != 50 || body["measurement"] != "cpu" || body["is_active"] != false {
		t.Fatalf("create: %d %v", code, body)
	}
	if ts, _ := body["created_at"].(string); !strings.HasSuffix(ts, "Z") || strings.Contains(ts, ".") {
		t.Fatalf("created_at %q is not the API's timestamp shape", ts)
	}
	if code, body := r.do(t, "POST", "/retention", `{"name":"p1","database":"db","retention_days":3}`); code != fiber.StatusBadRequest || !strings.Contains(body["error"].(string), "already exists") {
		t.Fatalf("duplicate name: %d %v", code, body)
	}
	r.do(t, "POST", "/retention", `{"name":"p2","database":"db","retention_days":9}`)
	if code, body := r.do(t, "PUT", "/retention/50", `{"is_active":true}`); code != fiber.StatusOK || body["is_active"] != true || body["measurement"] != "cpu" || body["buffer_days"].(float64) != 2 {
		t.Fatalf("partial update: %d %v", code, body)
	}
	if code, body := r.do(t, "PUT", "/retention/50", `{"measurement":null}`); code != fiber.StatusOK || body["measurement"] != nil {
		t.Fatalf("measurement null did not widen the policy to the database: %d %v", code, body)
	}
	if code, body := r.do(t, "PUT", "/retention/50", `{"buffer_days":40}`); code != fiber.StatusBadRequest || !strings.Contains(body["error"].(string), "greater than buffer_days") {
		t.Fatalf("buffer above stored retention: %d %v", code, body)
	}
	if code, body := r.do(t, "PUT", "/retention/50", `{"name":"p2"}`); code != fiber.StatusBadRequest || !strings.Contains(body["error"].(string), "already exists") {
		t.Fatalf("rename onto p2: %d %v", code, body)
	}
	if code, _ := r.do(t, "PUT", "/retention/7", `{"is_active":true}`); code != fiber.StatusNotFound {
		t.Fatalf("update of a missing policy: %d", code)
	}
	// The run history is this node's; the row it needs is mirrored first.
	if id := r.h.recordExecutionStart(50, time.Now()); id == 0 {
		t.Fatal("execution not recorded: the mirror row was missing for the foreign key")
	}
	code, list := r.doList(t)
	if code != fiber.StatusOK || len(list) != 2 || list[1]["id"].(float64) != 50 || list[1]["last_execution_status"] != "running" {
		t.Fatalf("list: %d %v", code, list)
	}
	if code, _ := r.do(t, "DELETE", "/retention/50", ``); code != fiber.StatusOK {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := r.do(t, "DELETE", "/retention/50", ``); code != fiber.StatusNotFound {
		t.Fatalf("second delete: %d", code)
	}
	if code, _ := r.do(t, "GET", "/retention/50", ``); code != fiber.StatusNotFound {
		t.Fatalf("get after delete: %d", code)
	}
	if err := r.h.MirrorClusterPolicies(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_executions WHERE policy_id = 50`); n != 0 {
		t.Fatalf("a deleted policy's history stayed: %d rows", n)
	}
}

func (r *clusterRetentionRig) doList(t *testing.T) (int, []map[string]any) {
	t.Helper()
	resp, err := r.app.Test(httptest.NewRequest("GET", "/retention", nil), -1)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// A run reads the cluster's policy, never this node's row: a mirror that has
// not caught up cannot change what a run deletes, and an ID this node's own
// table holds but the cluster does not is not a policy.
func TestRetentionRunReadsTheClusterPolicy(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.localPolicy(t, 3, "mine", 0) // never adopted
	r.src.seed(false, 10)
	if err := r.src.CreateRetentionPolicy(context.Background(), raft.CreateRetentionPolicyPayload{Name: "p", Database: "db", RetentionDays: 30, IsActive: true}); err != nil {
		t.Fatal(err)
	}
	if err := r.h.MirrorClusterPolicies(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.src.UpdateRetentionPolicy(context.Background(), raft.UpdateRetentionPolicyPayload{ID: 10, RetentionDays: intPtr(90)}); err != nil {
		t.Fatal(err)
	}
	// The mirror still holds 30 days.
	if n := r.count(t, `SELECT retention_days FROM retention_policies WHERE id = 10`); n != 30 {
		t.Fatalf("mirror already caught up (%d): the test needs it stale", n)
	}
	p, err := r.h.GetPolicy(10)
	if err != nil || p.RetentionDays != 90 {
		t.Fatalf("run reads %+v %v, want the cluster's 90 days", p, err)
	}
	if err := r.src.CreateRetentionPolicy(context.Background(), raft.CreateRetentionPolicyPayload{Name: "off", Database: "db", RetentionDays: 3}); err != nil {
		t.Fatal(err)
	}
	active, _ := r.h.GetActivePolicies()
	if len(active) != 1 || active[0].ID != 10 || active[0].RetentionDays != 90 {
		t.Fatalf("active %+v", active)
	}
	if _, err := r.h.GetPolicy(3); err == nil {
		t.Fatal("a row the cluster never adopted resolved as a policy")
	}
}

func intPtr(v int) *int { return &v }

// The first mirror after the seed: a row the seed kept by ID stays with its
// history; a row the seed renumbered hands its history to the new ID; a row
// the cluster did not adopt moves to the dormant tables with its history.
func TestRetentionMirrorAfterTheSeed(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.localPolicy(t, 1, "kept", 2)
	r.localPolicy(t, 2, "renumbered", 3)
	r.localPolicy(t, 3, "not-adopted", 1)
	// IDs 1 kept; "renumbered" was built from this node's row 2 (and another
	// node's) and is numbered 20 by the seed.
	r.src.must(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindRetention, Leader: "n1"})
	r.src.must(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindRetention, Epoch: r.src.idx, RetentionFloor: 20, Now: time.Now(),
		Retention: []raft.RetentionPolicyEntry{
			{Name: "kept", Database: "db", RetentionDays: 30, IsActive: true, Sources: []raft.RetentionSource{{Node: "n1", ID: 1}}},
			{Name: "renumbered", Database: "db", RetentionDays: 30, IsActive: true, Sources: []raft.RetentionSource{{Node: "n1", ID: 2}, {Node: "n2", ID: 7}}},
		}})
	if err := r.h.MirrorClusterPolicies(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Renumbered: kept -> 20, renumbered -> 21 (by name order).
	kept, _ := r.src.RetentionPolicyByName("kept")
	ren, _ := r.src.RetentionPolicyByName("renumbered")
	if n := r.count(t, `SELECT COUNT(*) FROM retention_executions WHERE policy_id = ?`, kept.ID); n != 2 {
		t.Fatalf("kept's history: %d rows at %d", n, kept.ID)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_executions WHERE policy_id = ?`, ren.ID); n != 3 {
		t.Fatalf("renumbered's history: %d rows at %d", n, ren.ID)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_policies WHERE id IN (1, 2, 3)`); n != 0 {
		t.Fatalf("%d rows from before the seed stayed live", n)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_policies_dormant WHERE policy_id = 3 AND name = 'not-adopted'`); n != 1 {
		t.Fatal("the row the cluster did not adopt is not dormant")
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_executions_dormant WHERE policy_id = 3`); n != 1 {
		t.Fatal("its history is not dormant")
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_policies`); n != 2 {
		t.Fatalf("%d live rows, want the cluster's 2", n)
	}
	// A second mirror changes nothing.
	if err := r.h.MirrorClusterPolicies(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_policies_dormant`); n != 1 {
		t.Fatalf("a second mirror moved more rows: %d dormant", n)
	}
}

// With the IDs kept, the source node's row stays as it is, history included,
// and a row another node held under a different ID is never renamed into it.
func TestRetentionMirrorKeepsIDs(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.localPolicy(t, 4, "p", 2)
	var created string
	if err := r.h.db.QueryRow(`SELECT created_at FROM retention_policies WHERE id = 4`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	ct, _ := time.Parse(time.RFC3339, created)
	r.src.seed(true, 5, raft.RetentionPolicyEntry{ID: 4, Name: "p", Database: "db", RetentionDays: 30, IsActive: true, CreatedAt: ct, UpdatedAt: ct,
		Sources: []raft.RetentionSource{{Node: "n1", ID: 4}}})
	if err := r.h.MirrorClusterPolicies(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_executions WHERE policy_id = 4`); n != 2 {
		t.Fatalf("kept policy's history: %d", n)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_policies_dormant`); n != 0 {
		t.Fatalf("%d dormant rows", n)
	}
}

// Two policies that swap names are rewritten without tripping the unique
// name, and a cluster delete of a seeded policy removes its history rather
// than keeping it dormant.
func TestRetentionMirrorNameSwapAndDelete(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.src.seed(false, 1)
	ctx := context.Background()
	for _, n := range []string{"a", "b"} {
		if err := r.src.CreateRetentionPolicy(ctx, raft.CreateRetentionPolicyPayload{Name: n, Database: "db", RetentionDays: 9}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.h.MirrorClusterPolicies(ctx); err != nil {
		t.Fatal(err)
	}
	r.exec(t, `INSERT INTO retention_executions (policy_id, status) VALUES (1, 'completed')`)
	for _, u := range []raft.UpdateRetentionPolicyPayload{{ID: 1, Name: strPtr("tmp")}, {ID: 2, Name: strPtr("a")}, {ID: 1, Name: strPtr("b")}} {
		if err := r.src.UpdateRetentionPolicy(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.h.MirrorClusterPolicies(ctx); err != nil {
		t.Fatalf("mirror of swapped names: %v", err)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_policies WHERE (id = 1 AND name = 'b') OR (id = 2 AND name = 'a')`); n != 2 {
		t.Fatal("names not swapped in the mirror")
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_executions WHERE policy_id = 1`); n != 1 {
		t.Fatal("a rewrite of the row dropped its history")
	}
	if err := r.src.DeleteRetentionPolicy(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.h.MirrorClusterPolicies(ctx); err != nil {
		t.Fatal(err)
	}
	if n := r.count(t, `SELECT COUNT(*) FROM retention_executions WHERE policy_id = 1`) + r.count(t, `SELECT COUNT(*) FROM retention_policies_dormant`); n != 0 {
		t.Fatalf("a deleted policy left %d rows", n)
	}
}

func strPtr(s string) *string { return &s }

// The lister answers this node's own rows with how often each ran.
func TestRetentionLocalPolicyLister(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.localPolicy(t, 1, "a", 3)
	r.exec(t, `INSERT INTO retention_policies (id, name, database, measurement, retention_days, buffer_days, is_active) VALUES (2, 'b', 'db', '', 9, NULL, 0)`)
	raw, err := r.h.LocalPolicyLister()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var rows cluster.RetentionLocalRows
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows.Policies) != 2 || rows.Policies[0].Executions != 3 || !rows.Policies[0].IsActive || rows.Policies[0].CreatedAt.IsZero() ||
		rows.Policies[1].Measurement != nil || rows.Policies[1].BufferDays != 0 || rows.Policies[1].Executions != 0 {
		t.Fatalf("rows %+v", rows.Policies)
	}
}

// The adopt endpoint takes the retention kind, a node and a hash only.
func TestAdoptPoliciesEndpointValidates(t *testing.T) {
	h := NewClusterHandler(&cluster.Coordinator{}, nil, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	for _, body := range []string{`{"kind":"tiering","node":"a","hash":"h"}`, `{"kind":"retention","hash":"h"}`, `{"kind":"retention","node":"a"}`, `nope`} {
		req := httptest.NewRequest("POST", "/api/v1/cluster/policies/adopt", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusBadRequest {
			t.Fatalf("%s: %d", body, resp.StatusCode)
		}
	}
}

// A retention database of its own (retention.db_path off the auth database)
// opens with the auth database's settings: the history's foreign key is
// enforced, which the mirror's rewrite depends on.
func TestRetentionSeparateDBEnforcesForeignKeys(t *testing.T) {
	dir := t.TempDir()
	am, err := auth.NewAuthManager(filepath.Join(dir, "auth.db"), time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer am.Close()
	h, err := NewRetentionHandler(nil, nil, &config.RetentionConfig{Enabled: true, DBPath: filepath.Join(dir, "retention.db")}, nil, am, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if !h.ownsDB {
		t.Fatal("the test needs a database of retention's own")
	}
	if _, err := h.db.Exec(`INSERT INTO retention_executions (policy_id, status) VALUES (42, 'completed')`); err == nil {
		t.Fatal("an execution of a policy that does not exist was stored: foreign keys are off")
	}
}

// The lister reads under the lock local writes take, so a write is either in
// the cluster's read of the table or refused by its collect.
func TestRetentionLocalPolicyListerTakesTheWriteLock(t *testing.T) {
	r := newClusterRetentionRig(t)
	r.h.localMu.Lock()
	done := make(chan struct{})
	go func() {
		_, _ = r.h.LocalPolicyLister()(context.Background())
		close(done)
	}()
	select {
	case <-done:
		r.h.localMu.Unlock()
		t.Fatal("the lister read the table while a local write held the lock")
	case <-time.After(200 * time.Millisecond):
	}
	r.h.localMu.Unlock()
	<-done
}

// A node with retention disabled still answers the cluster with the policies
// its database holds from when retention ran there; no table, or no file, is
// no policies.
func TestRetentionTableLister(t *testing.T) {
	ctx := context.Background()
	decode := func(raw json.RawMessage, err error) []cluster.RetentionLocalRow {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		var rows cluster.RetentionLocalRows
		if err := json.Unmarshal(raw, &rows); err != nil {
			t.Fatal(err)
		}
		return rows.Policies
	}
	r := newClusterRetentionRig(t)
	r.localPolicy(t, 7, "old", 2)
	shared := r.h.db
	dir := t.TempDir()
	am, err := auth.NewAuthManager(filepath.Join(dir, "arc.db"), time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer am.Close()
	if got := decode(RetentionTableLister(am, filepath.Join(dir, "arc.db"))(ctx)); len(got) != 0 {
		t.Fatalf("auth database without a retention table: %+v", got)
	}
	if got := decode(RetentionTableLister(nil, filepath.Join(dir, "missing.db"))(ctx)); len(got) != 0 {
		t.Fatalf("missing file: %+v", got)
	}
	var path string
	if err := shared.QueryRow(`SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	got := decode(RetentionTableLister(nil, path)(ctx))
	if len(got) != 1 || got[0].ID != 7 || got[0].Executions != 2 {
		t.Fatalf("rows of a disabled node: %+v", got)
	}
}
