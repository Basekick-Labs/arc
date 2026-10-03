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
			// And the reason must name the definition, since it is recorded
			// with the failed execution and read back through the API.
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

// The stored database name is re-checked for the same reason the stored query
// is: it becomes a storage path segment on both sides of a run, and the
// create/update boundary began applying a name rule to it only in #995, so any
// row stored before that — or by an older build, or straight into the shared
// metadata SQLite, or left with an empty database by a partial PUT, which the
// same change now refuses — can carry anything.
//
// Every fixture below carries a VALID query. ValidateSQLRequest runs first, so
// a malformed one would fail for #1002's reason and these assertions would
// pass without the guard under test ever running.
func TestExecuteAggregation_RevalidatesStoredDatabaseName(t *testing.T) {
	h := &ContinuousQueryHandler{logger: zerolog.Nop()}
	ctx := context.Background()
	now := time.Now().UTC()
	const validQuery = "SELECT count(*) AS c FROM db1.cpu"

	refused := []struct{ name, database string }{
		{"reserved anchor root", "_schema"},
		{"reserved compaction state", "_compaction_state"},
		{"any underscore-prefixed reserved root", "_internal"},
		{"dot-prefixed, hidden from every listing", ".hidden"},
		{"hive partition syntax", "host=hub01"},
		{"recursive glob", "**"},
		{"single glob", "db*"},
		{"leading digit", "1db"},
		{"over the 64-byte limit", strings.Repeat("a", 65)},
		{"empty", ""},
	}
	for _, tt := range refused {
		t.Run("refused/"+tt.name, func(t *testing.T) {
			cq := &ContinuousQuery{Name: "stale", Database: tt.database, SourceMeasurement: "cpu"}
			_, err := h.executeAggregation(ctx, cq, validQuery, now, now)
			if err == nil {
				t.Fatalf("a stored definition with database %q was executed", tt.database)
			}
			// It must fail on the name rule, not incidentally on storage or
			// DuckDB, or this test would pass for the wrong reason. "**" is the
			// case that proves the ordering: GetStoragePath would also refuse
			// it, with "has an unusable source". The substring also pins the
			// wording against an apostrophe, which the global log hook's
			// quoted-span masking would swallow the rest of the line on.
			if !strings.Contains(err.Error(), "has an invalid database name") {
				t.Errorf("refused for the wrong reason: %v", err)
			}
			// And the reason must name the definition, since it is recorded
			// with the failed execution and read back through the API.
			if !strings.Contains(err.Error(), "stale") {
				t.Errorf("error does not name the continuous query: %v", err)
			}
		})
	}
}

// The other half: the rule must not refuse a database name an ordinary
// continuous query carries.
//
// Asserted against the predicate directly, for the reason the SQL half above
// gives — a definition that PASSES carries on into storage-path resolution and
// the DuckDB handle, both nil on a bare handler. The refused cases prove the
// call site is wired and that its error names the query.
func TestExecuteAggregation_OrdinaryDatabaseNamesAreNotRefused(t *testing.T) {
	for _, db := range []string{
		"default",
		"alpha",
		"db_1",
		"A-b",
		"Metrics-Prod_01",
		strings.Repeat("a", 64),
	} {
		if !isValidDatabaseName(db) {
			t.Errorf("the rule would refuse an ordinary continuous-query database: %q", db)
		}
	}
}

// The stored measurement names are re-checked for the same reason the stored
// database is, and the same way: each against the rule its own boundary
// applies. Until #1011 source_measurement had no boundary rule at all and a
// partial PUT blanked destination_measurement, so a stored row can carry
// anything a build before that accepted.
//
// Every fixture carries a valid query, a valid database and a valid value for
// whichever measurement is not under test: the guards run in the order
// database, source, destination, so an invalid earlier field would make these
// assertions pass without the guard under test ever running.
func TestExecuteAggregation_RevalidatesStoredSourceMeasurement(t *testing.T) {
	h := &ContinuousQueryHandler{logger: zerolog.Nop()}
	ctx := context.Background()
	now := time.Now().UTC()
	const validQuery = "SELECT count(*) AS c FROM db1.cpu"

	refused := []struct{ name, source string }{
		{"two path components", "a/b"},
		{"parent directory", ".."},
		{"single glob", "cpu*"},
		{"character class", "cpu[1]"},
		{"dot-prefixed, hidden from every listing", ".hidden"},
		{"empty, which a partial PUT used to store", ""},
	}
	for _, tt := range refused {
		t.Run("refused/"+tt.name, func(t *testing.T) {
			cq := &ContinuousQuery{
				Name:                   "stale",
				Database:               "db1",
				SourceMeasurement:      tt.source,
				DestinationMeasurement: "cpu_1h",
			}
			_, err := h.executeAggregation(ctx, cq, validQuery, now, now)
			if err == nil {
				t.Fatalf("a stored definition with source measurement %q was executed", tt.source)
			}
			// It must fail on the name rule, not incidentally on storage or
			// DuckDB. "a/b" and "cpu*" are the cases that prove the ordering:
			// GetStoragePath would also refuse them, with "has an unusable
			// source".
			if !strings.Contains(err.Error(), "has an invalid source measurement name") {
				t.Errorf("refused for the wrong reason: %v", err)
			}
			// And the reason must name the definition, since it is recorded
			// with the failed execution and read back through the API.
			if !strings.Contains(err.Error(), "stale") {
				t.Errorf("error does not name the continuous query: %v", err)
			}
		})
	}
}

// A destination measurement is the one field whose stored value had
// consequences only visible after the run: an empty one made the write land on
// a key with an empty segment, which every caller recorded as a SUCCESSFUL
// execution while the rows died at flush; and "a/b" wrote a nine-segment key
// that reads back as part of measurement "a", is deleted by that measurement's
// retention policy, and is exported into that measurement's Iceberg table.
func TestExecuteAggregation_RevalidatesStoredDestinationMeasurement(t *testing.T) {
	h := &ContinuousQueryHandler{logger: zerolog.Nop()}
	ctx := context.Background()
	now := time.Now().UTC()
	const validQuery = "SELECT count(*) AS c FROM db1.cpu"

	refused := []struct{ name, destination string }{
		{"two path components", "a/b"},
		{"single glob", "cpu*"},
		{"dot-prefixed", ".hidden"},
		{"underscore-prefixed", "_internal"},
		{"leading digit", "1cpu"},
		{"over the 128-byte limit", strings.Repeat("a", 129)},
		{"empty, which a partial PUT used to store", ""},
	}
	for _, tt := range refused {
		t.Run("refused/"+tt.name, func(t *testing.T) {
			cq := &ContinuousQuery{
				Name:                   "stale",
				Database:               "db1",
				SourceMeasurement:      "cpu",
				DestinationMeasurement: tt.destination,
			}
			_, err := h.executeAggregation(ctx, cq, validQuery, now, now)
			if err == nil {
				t.Fatalf("a stored definition with destination measurement %q was executed", tt.destination)
			}
			if !strings.Contains(err.Error(), "has an invalid destination measurement name") {
				t.Errorf("refused for the wrong reason: %v", err)
			}
			if !strings.Contains(err.Error(), "stale") {
				t.Errorf("error does not name the continuous query: %v", err)
			}
		})
	}
}

// The other halves: neither rule may refuse a name an ordinary continuous query
// carries.
//
// Asserted against the predicates directly, for the reason the database half
// above gives — a definition that PASSES carries on into storage-path
// resolution and the DuckDB handle, both nil on a bare handler.
//
// The source list is the point of using the storage rule rather than the
// create-time one: these are shapes real storage roots hold, written by builds
// older than the measurement-name rule (v26.02.1), by edge-sync receive, by
// backup restore or by WAL replay, and Arc has no rename endpoint for them.
func TestExecuteAggregation_OrdinaryMeasurementNamesAreNotRefused(t *testing.T) {
	for _, source := range []string{
		"cpu",
		"cpu_1h",
		"CPU-load_2",
		"_internal",
		"7cpu",
		"cpu.v2",
		"host=hub01",
	} {
		if err := validateCQSourceMeasurement(source); err != nil {
			t.Errorf("the rule would refuse a readable measurement: %q -> %v", source, err)
		}
	}
	for _, destination := range []string{
		"cpu_1h",
		"rollup-5m",
		"CPU_Daily",
		strings.Repeat("a", 128),
	} {
		if !isValidMeasurementName(destination) {
			t.Errorf("the rule would refuse an ordinary continuous-query destination: %q", destination)
		}
	}
}
