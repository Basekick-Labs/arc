package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A continuous-query definition is a row that outlives the request that wrote
// it: validateCQQuery gates create and update, but the body re-executes on a
// schedule and nothing re-checked it in between. A row stored before that
// validator existed, or by an older build whose validator knew fewer cases,
// or written straight into the shared auth SQLite, reaches the engine
// unvalidated by the current rules.
//
// executeAggregation now validates before running. These definitions are the
// shapes a stale row can carry; each must be refused at execution.
func TestExecuteAggregation_RevalidatesStoredDefinition(t *testing.T) {
	h := &ContinuousQueryHandler{logger: zerolog.Nop()}
	ctx := context.Background()
	now := time.Now().UTC()

	refused := []struct{ name, query string }{
		{"path literal in table position", "SELECT count(*) AS c FROM './data/db2/secrets/d.parquet'"},
		{"relation keyword with a path literal", "SELECT c FROM (TABLE './data/db2/secrets/d.parquet')"},
		{"filesystem I/O function", "SELECT * FROM read_csv('/etc/passwd')"},
		{"session mutation", "ATTACH '/tmp/x.db' AS x"},
		{"multi-statement", "SELECT 1; DROP TABLE cpu"},
		{"extension install", "INSTALL httpfs"},
	}
	for _, tt := range refused {
		t.Run("refused/"+tt.name, func(t *testing.T) {
			cq := &ContinuousQuery{Name: "stale", Database: "db1", SourceMeasurement: "cpu"}
			_, err := h.executeAggregation(ctx, cq, tt.query, now, now)
			if err == nil {
				t.Fatalf("a stored definition that the validator rejects was executed: %s", tt.query)
			}
			// It must fail on validation, not incidentally on storage or DuckDB,
			// or this test would pass for the wrong reason.
			if !strings.Contains(err.Error(), "not valid SQL to execute") {
				t.Errorf("refused for the wrong reason: %v", err)
			}
			// And the reason must reach the operator, so it can be recorded as
			// a failed execution rather than lost.
			if !strings.Contains(err.Error(), "stale") {
				t.Errorf("error does not name the continuous query: %v", err)
			}
		})
	}
}

// The other half: the guard must not refuse ordinary definitions.
//
// Asserted against ValidateSQLRequest directly rather than through
// executeAggregation, because a definition that PASSES the guard carries on
// into storage-path resolution and the DuckDB handle — both nil on a bare
// handler, so there is nothing observable past the guard without wiring a
// whole backend. The refused cases above already prove the call site is wired
// and that its error names the query; this proves the predicate it delegates
// to accepts the shapes a real continuous query uses.
func TestExecuteAggregation_OrdinaryDefinitionsAreNotRefused(t *testing.T) {
	for _, q := range []string{
		"SELECT count(*) AS c FROM db1.cpu WHERE time >= '2026-01-01T00:00:00Z'",
		"SELECT host, avg(usage) AS a FROM db1.cpu GROUP BY host",
		"WITH t AS (SELECT * FROM db1.cpu) SELECT count(*) AS c FROM t",
		"SELECT count(*) AS c FROM db1.cpu WHERE msg = 'a/*b'",
		"SELECT time_bucket(INTERVAL '1 hour', time) AS b, avg(usage) AS a FROM db1.cpu GROUP BY b",
		"SELECT count(*) AS c FROM db1.cpu WHERE host IN ('a','b') AND usage > 0.5",
	} {
		if err := ValidateSQLRequest(q); err != nil {
			t.Errorf("the guard would refuse an ordinary continuous query: %s -> %v", q, err)
		}
	}
}
