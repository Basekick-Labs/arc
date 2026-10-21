package api

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/gofiber/fiber/v2"
)

type testCQClusterSource struct {
	state   raft.CQMirror
	entries []raft.CQPolicyEntry
}

type unseededTestCQSource struct{ testCQClusterSource }

func (*unseededTestCQSource) CQsSeeded() bool { return false }

func (s *testCQClusterSource) CQEnabled() bool                     { return true }
func (s *testCQClusterSource) CQsSeeded() bool                     { return true }
func (s *testCQClusterSource) CQCollecting() bool                  { return false }
func (s *testCQClusterSource) CQReadBarrier(context.Context) error { return nil }
func (s *testCQClusterSource) CQ(int64) (raft.CQPolicyEntry, bool) {
	return raft.CQPolicyEntry{}, false
}
func (s *testCQClusterSource) CQByName(string) (raft.CQPolicyEntry, bool) {
	return raft.CQPolicyEntry{}, false
}
func (s *testCQClusterSource) CQs() []raft.CQPolicyEntry                                { return s.entries }
func (s *testCQClusterSource) CreateCQ(context.Context, raft.CreateCQPayload) error     { return nil }
func (s *testCQClusterSource) UpdateCQ(context.Context, raft.UpdateCQPayload) error     { return nil }
func (s *testCQClusterSource) DeleteCQ(context.Context, raft.DeleteCQPayload) error     { return nil }
func (s *testCQClusterSource) CompleteCQ(context.Context, raft.CompleteCQPayload) error { return nil }
func (s *testCQClusterSource) CQMirrorState() raft.CQMirror                             { return s.state }
func (s *testCQClusterSource) PrimaryEpoch() uint64                                     { return 3 }
func (s *testCQClusterSource) LocalNodeID() string                                      { return "node-a" }

func TestMirrorClusterCQsMovesHistoryByAdoptedSourceAndDormantsExcludedRows(t *testing.T) {
	h := newSQLiteOnlyCQHandler(t, filepath.Join(t.TempDir(), "cq.db"))
	t.Cleanup(func() { _ = h.sqliteDB.Close() })
	_, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries (id,name,database,source_measurement,destination_measurement,query,interval,is_active) VALUES (1,'hourly','db','cpu','cpu_hourly','SELECT count(*) FROM db.cpu','1h',1),(2,'legacy','db','mem','mem_hourly','SELECT count(*) FROM db.mem','1h',1)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.sqliteDB.Exec(`INSERT INTO continuous_query_executions (query_id,execution_id,status,start_time,end_time,records_written) VALUES (1,'old-run','completed','2025-01-01T00:00:00Z','2025-01-01T01:00:00Z',12),(2,'legacy-run','completed','2025-01-01T00:00:00Z','2025-01-01T01:00:00Z',2)`)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	e := raft.CQPolicyEntry{ID: 10, Name: "hourly", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_hourly", Query: "SELECT count(*) FROM db.cpu", Interval: "1h", IsActive: true, Version: 1, CreatedAt: created, UpdatedAt: created, Sources: []raft.CQSource{{Node: "node-a", ID: 1}}}
	src := &testCQClusterSource{entries: []raft.CQPolicyEntry{e}, state: raft.CQMirror{Entries: []raft.CQPolicyEntry{e}, Epoch: 3}}
	h.SetClusterSource(src)
	if err := h.MirrorClusterCQs(context.Background()); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := h.sqliteDB.QueryRow(`SELECT name FROM continuous_queries WHERE id=10`).Scan(&name); err != nil || name != "hourly" {
		t.Fatalf("mirrored row name=%q err=%v", name, err)
	}
	var runID string
	if err := h.sqliteDB.QueryRow(`SELECT execution_id FROM continuous_query_executions WHERE query_id=10`).Scan(&runID); err != nil || runID != "old-run" {
		t.Fatalf("migrated execution=%q err=%v", runID, err)
	}
	var dormant int
	if err := h.sqliteDB.QueryRow(`SELECT count(*) FROM continuous_queries_dormant WHERE name='legacy'`).Scan(&dormant); err != nil || dormant != 1 {
		t.Fatalf("dormant legacy rows=%d err=%v", dormant, err)
	}
	if err := h.sqliteDB.QueryRow(`SELECT count(*) FROM continuous_query_executions_dormant WHERE execution_id='legacy-run'`).Scan(&dormant); err != nil || dormant != 1 {
		t.Fatalf("dormant legacy history=%d err=%v", dormant, err)
	}
	src.entries = nil
	src.state.Entries = nil
	if err := h.MirrorClusterCQs(context.Background()); err != nil {
		t.Fatal(err)
	}
	var activeRows int
	if err := h.sqliteDB.QueryRow(`SELECT count(*) FROM continuous_queries WHERE id=10`).Scan(&activeRows); err != nil || activeRows != 0 {
		t.Fatalf("deleted cluster CQ rows=%d err=%v", activeRows, err)
	}
	if err := h.sqliteDB.QueryRow(`SELECT count(*) FROM continuous_query_executions_dormant WHERE execution_id='old-run'`).Scan(&dormant); err != nil || dormant != 0 {
		t.Fatalf("deleted cluster history was retained as dormant: rows=%d err=%v", dormant, err)
	}
}

func TestMirrorClusterCQsDormantsSameNameButDifferentDefinition(t *testing.T) {
	h := newSQLiteOnlyCQHandler(t, filepath.Join(t.TempDir(), "cq.db"))
	t.Cleanup(func() { _ = h.sqliteDB.Close() })
	_, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries (id,name,database,source_measurement,destination_measurement,query,interval,is_active) VALUES (10,'hourly','db','cpu','cpu_hourly','SELECT 999','1h',1)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.sqliteDB.Exec(`INSERT INTO continuous_query_executions (query_id,execution_id,status,start_time,end_time,records_written) VALUES (10,'wrong-run','completed','2025-01-01T00:00:00Z','2025-01-01T01:00:00Z',99)`)
	if err != nil {
		t.Fatal(err)
	}
	e := raft.CQPolicyEntry{ID: 10, Name: "hourly", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_hourly", Query: "SELECT 1", Interval: "1h", IsActive: true, Version: 1, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	src := &testCQClusterSource{entries: []raft.CQPolicyEntry{e}, state: raft.CQMirror{Entries: []raft.CQPolicyEntry{e}, Epoch: 1}}
	h.SetClusterSource(src)
	if err := h.MirrorClusterCQs(context.Background()); err != nil {
		t.Fatal(err)
	}
	var query string
	if err := h.sqliteDB.QueryRow(`SELECT query FROM continuous_queries WHERE id=10`).Scan(&query); err != nil || query != "SELECT 1" {
		t.Fatalf("canonical query=%q err=%v", query, err)
	}
	var active, dormant int
	if err := h.sqliteDB.QueryRow(`SELECT count(*) FROM continuous_query_executions WHERE query_id=10 AND execution_id='wrong-run'`).Scan(&active); err != nil || active != 0 {
		t.Fatalf("wrong history remained active=%d err=%v", active, err)
	}
	if err := h.sqliteDB.QueryRow(`SELECT count(*) FROM continuous_query_executions_dormant WHERE execution_id='wrong-run'`).Scan(&dormant); err != nil || dormant != 1 {
		t.Fatalf("wrong history dormant=%d err=%v", dormant, err)
	}
}

func TestCQSchedulerCanStartFromLocalRowsWhileAPIWaitsForSeed(t *testing.T) {
	h := newSQLiteOnlyCQHandler(t, filepath.Join(t.TempDir(), "cq.db"))
	t.Cleanup(func() { _ = h.sqliteDB.Close() })
	_, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries (id,name,database,source_measurement,destination_measurement,query,interval,is_active) VALUES (1,'hourly','db','cpu','cpu_hourly','SELECT 1','1h',1)`)
	if err != nil {
		t.Fatal(err)
	}
	h.SetClusterSource(&unseededTestCQSource{})
	active, err := h.GetActiveCQs()
	if err != nil || len(active) != 1 {
		t.Fatalf("scheduler startup inventory=%d err=%v", len(active), err)
	}
	app := fiber.New()
	app.Get("/continuous-queries", h.handleList)
	status, _ := sendCQ(t, app, "GET", "/continuous-queries", "")
	if status != fiber.StatusServiceUnavailable {
		t.Fatalf("pre-seed API status=%d", status)
	}
}
