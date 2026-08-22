package api

import (
	"strings"
	"testing"
)

// assembleMeasurementSQL mirrors exactly what queryMeasurement builds so these
// tests exercise the same string the handler validates.
func assembleMeasurementSQL(database, measurement, where string) string {
	sql := "SELECT * FROM " + database + "." + measurement
	if where != "" {
		sql += " WHERE " + where
	}
	return sql + " ORDER BY time DESC LIMIT 100 OFFSET 0"
}

// TestQueryMeasurementWhereRejectsCrossTenantReads covers GHSA-wmjj-g8xc-6hwr.
//
// GET /api/v1/query/:measurement guarded its user-controlled `where` param only
// with validateWhereClauseQuery — a substring blocklist that blocks neither
// SELECT nor any DuckDB I/O table function. The fragment is concatenated into
// `SELECT * FROM db.meas WHERE <where> ...` and executed on a DuckDB handle whose
// sandbox allowlists the entire storage root, while RBAC inspects only the path
// params. A token scoped to one measurement could therefore read any tenant's
// files through a scalar subquery.
//
// Each payload below passes validateWhereClauseQuery (asserted explicitly) and
// must now be rejected by the shared validator.
func TestQueryMeasurementWhereRejectsCrossTenantReads(t *testing.T) {
	payloads := []struct {
		name  string
		where string
	}{
		// Class 1: I/O table functions (the reported vector).
		{"parquet_scan subquery", `time > (SELECT max(value) FROM parquet_scan('/data/arc/secretdb/cpu/d.parquet'))`},
		{"glob enumeration", `time > (SELECT count(*) FROM glob('/data/arc/secretdb/cpu/d.parquet'))`},
		{"read_parquet subquery", `time > (SELECT max(value) FROM read_parquet('/data/arc/secretdb/cpu/d.parquet'))`},
		{"parquet_metadata oracle", `1=1 AND (SELECT count(*) FROM parquet_metadata('/data/arc/secretdb/cpu/d.parquet')) > 0`},
		{"read_csv non-parquet", `time > (SELECT max(c) FROM read_csv('/data/arc/arc.db'))`},
		{"read_text non-parquet", `time > (SELECT len(content) FROM read_text('/data/arc/arc.db'))`},
		{"iceberg_scan", `time > (SELECT max(v) FROM iceberg_scan('/data/arc/secretdb/tbl'))`},

		// Class 2: quoted-identifier spelling (GHSA-93cm round 2).
		{"quoted identifier fn", `time > (SELECT max(v) FROM "parquet_scan"('/data/arc/secretdb/cpu/d.parquet'))`},

		// Class 3: bare-string replacement scan, no function name (GHSA-w8x2).
		{"replacement scan subquery", `time > (SELECT max(v) FROM '/data/arc/secretdb/cpu/d.parquet')`},

		// Class 3b: non-standard string syntaxes for the same replacement scan.
		// These carry no ' or " at all in the $$ form, so they also evade the
		// fragment blocklist's quote-parity check.
		{"E-string replacement scan", `time > (SELECT max(v) FROM e'/data/arc/secretdb/cpu/d.parquet')`},
		{"E-string uppercase", `time > (SELECT max(v) FROM E'/data/arc/secretdb/cpu/d.parquet')`},
		{"dollar-quoted replacement scan", `time > (SELECT max(v) FROM $$/data/arc/secretdb/cpu/d.parquet$$)`},
		{"tagged dollar-quote", `time > (SELECT max(v) FROM $t$/data/arc/secretdb/cpu/d.parquet$t$)`},

		// Class 4: comma cross-join inside a subquery.
		{"comma cross-join", `time > (SELECT max(b.v) FROM cpu, '/data/arc/secretdb/x.parquet' b)`},
	}

	for _, p := range payloads {
		t.Run(p.name, func(t *testing.T) {
			// Precondition: the legacy fragment blocklist does NOT catch this.
			// If this ever starts failing, the payload stopped exercising the gap.
			if err := validateWhereClauseQuery(p.where); err != nil {
				t.Fatalf("payload no longer exercises the gap — fragment blocklist rejected it: %v", err)
			}

			sql := assembleMeasurementSQL("default", "cpu", p.where)
			if err := ValidateSQLRequest(sql); err == nil {
				t.Errorf("VULNERABLE: cross-tenant payload accepted\n  where: %s\n  sql:   %s", p.where, sql)
			}
		})
	}
}

// TestQueryMeasurementWhereAllowsLegitimateFilters guards against false
// positives. This endpoint is public and working; rejecting valid filters would
// break production users. Every fragment here must keep passing.
func TestQueryMeasurementWhereAllowsLegitimateFilters(t *testing.T) {
	valid := []struct {
		name  string
		where string
	}{
		{"empty where", ``},
		{"timestamp comparison", `time > '2026-01-01'`},
		{"conjunction", `host = 'server1' AND value > 10`},
		{"date_trunc call", `date_trunc('hour', time) > '2026-01-01'`},
		{"between", `time BETWEEN '2026-01-01' AND '2026-01-02'`},
		{"in list", `status IN ('active', 'idle')`},
		{"like wildcard", `host LIKE 'web-%'`},
		{"is not null", `value IS NOT NULL`},
		{"escaped quote", `msg = 'it''s fine'`},
		{"nested parens", `(value > 10 AND value < 100) OR host = 'a'`},
		{"scientific notation", `value > 1e5`},
		{"dollar in literal", `msg = 'cost$5'`},
		{"column named e", `e > 5`},
		{"quoted keyword column", `"load" > 1`},
		{"substring keyword columns", `created_at_ms > 0 AND payload_size < 10`},
	}

	for _, v := range valid {
		t.Run(v.name, func(t *testing.T) {
			sql := assembleMeasurementSQL("mydb", "cpu", v.where)
			if err := ValidateSQLRequest(sql); err != nil {
				t.Errorf("FALSE POSITIVE: legitimate filter rejected\n  where: %s\n  err:   %v", v.where, err)
			}
		})
	}
}

// TestValidateSQLRequestBlocksNonStandardStringLiterals covers the review
// Blocker: MaskStringLiterals recognised only '…' and "…", so DuckDB's E'…' and
// $tag$…$tag$ spellings of the same replacement scan bypassed the table-position
// check. This hole was live on POST /api/v1/query, /estimate, and /arrow — every
// endpoint that calls ValidateSQLRequest — not just the measurement endpoint.
//
// Verified against DuckDB v1.4.3: all three spellings execute a replacement scan
// and return the target file's contents.
func TestValidateSQLRequestBlocksNonStandardStringLiterals(t *testing.T) {
	blocked := []string{
		`SELECT * FROM '/data/arc/secretdb/cpu/d.parquet'`,
		`SELECT * FROM e'/data/arc/secretdb/cpu/d.parquet'`,
		`SELECT * FROM E'/data/arc/secretdb/cpu/d.parquet'`,
		`SELECT * FROM $$/data/arc/secretdb/cpu/d.parquet$$`,
		`SELECT * FROM $t$/data/arc/secretdb/cpu/d.parquet$t$`,
		`SELECT * FROM cpu, $$/data/arc/secretdb/d.parquet$$ b`,
		`SELECT * FROM cpu WHERE x > (SELECT max(v) FROM e'/data/arc/o/d.parquet')`,
	}
	for _, sql := range blocked {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("VULNERABLE: replacement scan accepted: %s", sql)
		}
	}

	allowed := []string{
		`SELECT * FROM cpu WHERE host = 'server1'`,
		`SELECT * FROM cpu WHERE price > 100`,
		`SELECT * FROM cpu WHERE name = 'a$b'`,
		`SELECT * FROM cpu WHERE msg LIKE '%$%'`,
		`SELECT e FROM cpu`,
		`SELECT * FROM cpu WHERE value > 1e5`,
		`SELECT * FROM cpu WHERE code = 'e'`,
		`SELECT * FROM cpu WHERE a = 'it''s'`,
	}
	for _, sql := range allowed {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("FALSE POSITIVE: %s -> %v", sql, err)
		}
	}
}

// TestDeleteWhereRejectsIOFunctions covers review finding H4. The DELETE
// blocklist blocks SELECT (killing the subquery vector) but omitted the I/O
// function family, so a scalar expression needing no subquery turned the
// affected-row count into a cross-tenant existence oracle.
func TestDeleteWhereRejectsIOFunctions(t *testing.T) {
	h := &DeleteHandler{}
	payloads := []string{
		`1=1 OR list_contains(glob('/data/arc/**'), 'x')`,
		`time > 0 AND len(read_text('/data/arc/arc.db')) > 0`,
		`time > 0 AND "glob"('/data/arc/**') IS NOT NULL`,
		`time > 0 AND parquet_metadata('/data/arc/o/d.parquet') IS NOT NULL`,
	}
	for _, where := range payloads {
		if _, err := h.validateWhereClause(where); err == nil {
			t.Errorf("VULNERABLE: DELETE accepted I/O function: %s", where)
		}
	}

	// Legitimate DELETE filters must still work.
	valid := []string{
		`time < '2026-01-01'`,
		`host = 'server1' AND value > 10`,
		`payload_size > 100`,
		`offset_ms < 5`,
	}
	for _, where := range valid {
		if _, err := h.validateWhereClause(where); err != nil {
			t.Errorf("FALSE POSITIVE: DELETE rejected valid filter %q: %v", where, err)
		}
	}
}

// TestValidateCQQueryRejectsUnsafeSQL covers review finding H3. Continuous
// queries stored a fully user-supplied SQL body with no validation at all and
// re-executed it on a schedule.
func TestValidateCQQueryRejectsUnsafeSQL(t *testing.T) {
	unsafe := []string{
		`SELECT * FROM parquet_scan('/data/arc/secretdb/cpu/d.parquet') WHERE time BETWEEN {start_time} AND {end_time}`,
		`SELECT * FROM '/data/arc/secretdb/cpu/d.parquet' WHERE time BETWEEN {start_time} AND {end_time}`,
		`SELECT * FROM e'/data/arc/secretdb/d.parquet' WHERE time BETWEEN {start_time} AND {end_time}`,
		`ATTACH '/tmp/evil.db' AS evil; SELECT 1 WHERE {start_time} < {end_time}`,
		`SELECT * FROM cpu WHERE time BETWEEN {start_time} AND {end_time}; DROP TABLE cpu`,
	}
	for _, q := range unsafe {
		if err := validateCQQuery(q); err == nil {
			t.Errorf("VULNERABLE: CQ accepted unsafe query: %s", q)
		}
	}

	safe := []string{
		`SELECT mean(value) AS value, host FROM cpu WHERE time BETWEEN {start_time} AND {end_time} GROUP BY host`,
		`SELECT count(*) AS c FROM cpu WHERE time >= {start_time} AND time < {end_time}`,
		`SELECT date_trunc('hour', time) AS time, sum(bytes) AS bytes FROM net WHERE time BETWEEN {start_time} AND {end_time} GROUP BY 1`,
	}
	for _, q := range safe {
		if err := validateCQQuery(q); err != nil {
			t.Errorf("FALSE POSITIVE: CQ rejected valid query %q: %v", q, err)
		}
	}

	// Empty is handled by the caller's required-field check, not here.
	if err := validateCQQuery("   "); err != nil {
		t.Errorf("blank query should defer to required-field check, got: %v", err)
	}
	_ = strings.TrimSpace
}
