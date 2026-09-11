//go:build duckdb_arrow

package api

import (
	"strings"
	"testing"
)

// Tests for GHSA-w6w2-x8xv-q8x2: DuckDB dynamic-SQL table functions let an
// authenticated read token smuggle a nested read_parquet or replacement scan
// past the I/O denylist and RBAC, reading across the tenant boundary. The
// validator must reject them before any DB call.

func TestDynamicSQLFunctionPattern_Family(t *testing.T) {
	// The complete set of string-executing table functions in the pinned
	// DuckDB build plus autoloaded extensions, enumerated from
	// duckdb_functions(). json_execute_serialized_sql is the json-extension
	// member and the reason this is not just {query, query_table}; a plan that
	// stopped at the first two would have shipped a working cross-tenant read.
	mustBlock := []string{"query", "query_table", "json_execute_serialized_sql"}

	for _, fn := range mustBlock {
		for _, sql := range []string{
			"SELECT * FROM " + fn + "('SELECT 1')",
			"SELECT * FROM cpu, " + fn + "('SELECT 1')",
			"SELECT * FROM " + strings.ToUpper(fn) + "  ('SELECT 1')", // case + spacing
			"SELECT * FROM \"" + fn + "\"('SELECT 1')",                // quoted name
			"SELECT * FROM main." + fn + "('SELECT 1')",               // schema-qualified
			"SELECT * FROM " + fn + "/**/('SELECT 1')",                // comment before paren
		} {
			if err := ValidateSQLRequest(sql); err == nil {
				t.Errorf("ValidateSQLRequest(%q): expected rejection for dynamic-SQL function %q, got nil", sql, fn)
			}
		}
	}
}

func TestDynamicSQLFunction_ExploitVariantsBlocked(t *testing.T) {
	// These are the shapes that actually exfiltrated cross-tenant rows live.
	exploits := []string{
		`SELECT * FROM query('SELECT * FROM read_parquet(''/data/tenant_b/x.parquet'')')`,
		`SELECT count(*) FROM query('SELECT * FROM read_parquet(''/data/*/secrets/**/*.parquet'')')`,
		`SELECT * FROM query_table(['/data/tenant_b/x.parquet'])`,
		`SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM read_parquet(''/data/tenant_b/x.parquet'')'))`,
		`SELECT * FROM query('SELECT * FROM query(''SELECT 1'')')`, // nested
	}
	for _, sql := range exploits {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("ValidateSQLRequest(%q): exploit not blocked", sql)
		}
	}
}

func TestDynamicSQLFunction_NoFalsePositives(t *testing.T) {
	// Identifiers named like the functions, with no call form, must pass.
	ok := []string{
		"SELECT query FROM cpu",
		"SELECT * FROM cpu AS query",
		"SELECT count(*) AS query FROM cpu",
		"SELECT query_table FROM cpu",
		"SELECT * FROM cpu WHERE query_table > 5",
	}
	for _, sql := range ok {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("ValidateSQLRequest(%q): false positive, got %v", sql, err)
		}
	}
}
