package iceberg

import (
	"context"
	"testing"
	"time"
)

type closedGate struct{}

func (closedGate) CanRun() bool { return false }

// A publish that covered ingest files records the table's last lag; one that
// covered none leaves it as it was (#1772).
func TestLastLagRecordedOnlyWithFiles(t *testing.T) {
	e := newPassRig(t, 1, 1, 1).exp
	ident := e.tableIdent("db", "cpu")
	key := tableLagKey(ident)
	e.lag.mu.Lock()
	if e.lag.pending == nil {
		e.lag.pending = map[string][]time.Time{}
	}
	e.lag.pending[key] = []time.Time{time.Now().Add(-90 * time.Second), time.Now().Add(-10 * time.Second)}
	e.lag.mu.Unlock()
	e.drainExportLag(ident)
	l, ok := e.lag.last[key]
	if !ok || l.database != "db" || l.measurement != "cpu" || l.lag < 89*time.Second {
		t.Fatalf("last lag after a publish = %+v %v", l, ok)
	}
	e.drainExportLag(ident) // a publish with no ingest file
	if l2 := e.lag.last[key]; l2.lag != l.lag || !l2.at.Equal(l.at) {
		t.Fatalf("a publish with no file changed the last lag: %+v -> %+v", l, l2)
	}
	e.forgetExportLag(ident)
	if _, ok := e.lag.last[key]; ok {
		t.Fatal("forgetExportLag kept the last lag")
	}
}

func TestLastLagForgottenWithItsNamespace(t *testing.T) {
	e := newPassRig(t, 1, 1, 1).exp
	ident := e.tableIdent("db", "cpu")
	e.noteLastLag(tableLagKey(ident), ident, time.Minute, time.Now())
	e.forgetExportLagNamespace(ident[:len(ident)-1])
	if len(e.lag.last) != 0 {
		t.Fatalf("last lag after its namespace was forgotten: %v", e.lag.last)
	}
}

// Old entries are not reported and are deleted; the rest come worst first,
// ties by name, at most n.
func TestLastLagTables(t *testing.T) {
	r := newPassRig(t, 1, 1, 1)
	e, s := r.exp, r.sched
	s.interval = time.Minute // window = 1 h
	now := time.Now()
	for _, c := range []struct {
		db, m string
		lag   time.Duration
		at    time.Time
	}{
		{"db", "old", time.Hour, now.Add(-2 * time.Hour)},
		{"db", "b", 30 * time.Second, now},
		{"db", "a", 30 * time.Second, now},
		{"db", "slow", 5 * time.Minute, now},
	} {
		ident := e.tableIdent(c.db, c.m)
		e.noteLastLag(tableLagKey(ident), ident, c.lag, c.at)
	}
	got := s.LastLagTables(2, now)
	if len(got) != 2 || got[0].Measurement != "slow" || got[1].Measurement != "a" {
		t.Fatalf("LastLagTables = %+v, want slow then a", got)
	}
	if _, ok := e.lag.last[tableLagKey(e.tableIdent("db", "old"))]; ok {
		t.Fatal("an expired entry was kept")
	}
	// A timer-only export every 2 h keeps its series for three passes.
	s.interval = 2 * time.Hour
	if w := s.lastLagWindow(); w != 6*time.Hour {
		t.Fatalf("window = %v, want 6h", w)
	}
}

// Pending per table comes from the ingest reports and from committed files
// not yet published; only tables past the floor count.
func TestPendingTables(t *testing.T) {
	r := newPassRig(t, 1, 1, 1)
	e, s := r.exp, r.sched
	s.ingestInterval = 30 * time.Second // floor 1 min
	now := time.Now()
	s.reqMu.Lock()
	s.ingestPending["db\x00cpu"] = ingestReports{first: now.Add(-10 * time.Minute), last: now}
	s.ingestPending["db\x00fresh"] = ingestReports{first: now.Add(-10 * time.Second), last: now}
	s.reqMu.Unlock()
	mem := e.tableIdent("db2", "mem")
	e.lag.mu.Lock()
	if e.lag.pending == nil {
		e.lag.pending = map[string][]time.Time{}
	}
	e.lag.pending[tableLagKey(mem)] = []time.Time{now.Add(-20 * time.Minute)}
	e.lag.pending[tableLagKey([]string{"notours", "x"})] = []time.Time{now.Add(-time.Hour)}
	e.lag.mu.Unlock()

	top, total := s.PendingTables(1, now)
	if total != 2 {
		t.Fatalf("total = %d, want 2 (fresh is under the floor, an unmappable ident is left out)", total)
	}
	if len(top) != 1 || top[0].Database != "db2" || top[0].Measurement != "mem" || top[0].Seconds < 1199 {
		t.Fatalf("top = %+v, want db2/mem at 20 min", top)
	}
}

// A dropped database's tables are not waiting.
func TestForgetDatabaseClearsPending(t *testing.T) {
	s := newPassRig(t, 1, 1, 1).sched
	s.reqMu.Lock()
	s.ingestPending["db\x00cpu"] = ingestReports{first: time.Now()}
	s.ingestPending["other\x00cpu"] = ingestReports{first: time.Now()}
	s.reqMu.Unlock()
	s.forgetDatabase("db")
	if _, ok := s.ingestPending["db\x00cpu"]; ok {
		t.Fatal("a dropped database's table is still pending")
	}
	if _, ok := s.ingestPending["other\x00cpu"]; !ok {
		t.Fatal("another database's table was forgotten")
	}
}

// A node that stops exporting stops reporting last lags: another node's
// publishes count now (#1772).
func TestLastLagClearedWhenTheNodeStopsExporting(t *testing.T) {
	r := newPassRig(t, 1, 1, 1)
	e, s := r.exp, r.sched
	ident := e.tableIdent("db", "cpu")
	e.noteLastLag(tableLagKey(ident), ident, time.Minute, time.Now())
	s.gate = closedGate{}
	s.runPass(context.Background())
	if len(e.lag.last) != 0 {
		t.Fatal("a gated node kept its last lags")
	}
	s.gate = nil
	e.noteLastLag(tableLagKey(ident), ident, time.Minute, time.Now())
	if _, err := e.PauseExportForRestore(context.Background(), "restore b1"); err != nil {
		t.Fatal(err)
	}
	s.pausedForRestore()
	if len(e.lag.last) != 0 {
		t.Fatal("a node paused for a restore kept its last lags")
	}
}
