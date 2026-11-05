//go:build duckdb_arrow

package api

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// reviewedCoreTableFunction is a table function of the linked DuckDB that user
// SQL may call: why that is safe, and how many overloads it had when someone
// decided so. A new overload is a new signature (a path argument, say) and is
// reviewed again.
type reviewedCoreTableFunction struct {
	overloads int
	reason    string
}

// Reasons shared by many entries.
const (
	reviewNoInput = "generates values from its arguments; reads no file, catalog or engine state"
	reviewEngine  = "engine or catalog metadata that holds no tenant data (at most engine-wide counters such as memory use or connection count): the query engine's catalog is empty (asserted below) and Arc's data lives in Parquet files"
	// These take a catalog name as a string, which RBAC never sees. Safe only
	// while the query engine's catalog holds nothing (asserted below); a view,
	// table or ATTACH on it would make them a way around RBAC.
	reviewCatalogByName = "resolves a catalog name given as a string; safe only while the query engine's catalog is empty (asserted below)"
)

// reviewedCoreTableFunctions is every table function of the linked DuckDB that
// ValidateSQLRequest lets user SQL call. Anything else the engine defines must
// be refused, by name, in every spelling.
var reviewedCoreTableFunctions = map[string]reviewedCoreTableFunction{
	"check_peg_parser":             {1, "parses its argument and returns nothing; changes no state (unlike enable_/disable_peg_parser)"},
	"duckdb_approx_database_count": {1, reviewEngine},
	"duckdb_columns":               {1, reviewEngine},
	"duckdb_connection_count":      {1, reviewEngine},
	"duckdb_constraints":           {1, reviewEngine},
	"duckdb_coordinate_systems":    {1, reviewEngine},
	"duckdb_databases":             {1, reviewEngine},
	"duckdb_dependencies":          {1, reviewEngine},
	"duckdb_extensions":            {1, "lists extensions; the sandbox refuses the file system access it needs"},
	"duckdb_functions":             {1, reviewEngine},
	"duckdb_indexes":               {1, reviewEngine},
	"duckdb_keywords":              {1, reviewEngine},
	"duckdb_memory":                {1, reviewEngine},
	"duckdb_optimizers":            {1, reviewEngine},
	"duckdb_schemas":               {1, reviewEngine},
	"duckdb_secret_types":          {1, "lists secret TYPES, not secrets (duckdb_secrets is refused)"},
	"duckdb_sequences":             {1, reviewEngine},
	"duckdb_table_sample":          {1, reviewCatalogByName},
	"duckdb_tables":                {1, reviewEngine},
	"duckdb_types":                 {1, reviewEngine},
	"duckdb_views":                 {1, reviewEngine},
	"generate_series":              {5, reviewNoInput},
	"icu_calendar_names":           {1, reviewNoInput},
	"json_each":                    {4, "parses its JSON argument; reads no file"},
	"json_tree":                    {4, "parses its JSON argument; reads no file"},
	"pg_timezone_names":            {1, reviewNoInput},
	"pragma_collations":            {1, reviewEngine},
	"pragma_database_size":         {1, reviewEngine},
	"pragma_metadata_info":         {2, reviewCatalogByName},
	"pragma_platform":              {1, reviewNoInput},
	"pragma_show":                  {1, reviewCatalogByName},
	"pragma_storage_info":          {1, reviewCatalogByName},
	"pragma_table_info":            {1, reviewCatalogByName},
	"pragma_user_agent":            {1, reviewNoInput},
	"pragma_version":               {1, reviewNoInput},
	"range":                        {5, reviewNoInput},
	"repeat":                       {1, reviewNoInput},
	"repeat_row":                   {1, reviewNoInput},
	"seq_scan":                     {1, "internal scan operator with no arguments; scans nothing when called directly"},
	"summary":                      {1, "summarises the rows of its TABLE argument, which RBAC checks like any other query"},
	"test_all_types":               {1, reviewNoInput},
	"test_vector_types":            {1, reviewNoInput},
	"unnest":                       {1, reviewNoInput},
}

// reviewedCoreScalars are the scalar functions whose names suggest they run,
// parse or plan SQL, and that user SQL may still call.
var reviewedCoreScalars = map[string]string{
	"current_query":        "returns the caller's own query text",
	"current_query_id":     "returns the caller's own query id",
	"json_serialize_sql":   "parses its argument without binding it: no table or file is resolved",
	"json_deserialize_sql": "turns a parse tree back into SQL text; executes nothing",
}

// refusedBody reports whether SQL text (a macro body, a view definition) calls
// something the validators refuse. The validators see only the name a query
// calls, so a macro or view wrapping a refused function must itself be refused.
func refusedBody(body string) string {
	body = strings.ToLower(body)
	if m := ioTableFunctionPattern.FindStringSubmatch(body); m != nil {
		return m[1]
	}
	if m := dynamicSQLFunctionPattern.FindStringSubmatch(body); m != nil {
		return m[1]
	}
	return sideEffectFunction(body)
}

// TestCoreTableFunctionsReviewed enumerates every table function, table macro,
// pragma and internal view of the DuckDB Arc links, and the scalars whose
// names say they handle SQL, on an engine built the way Arc builds
// it, and fails on one that user SQL can call without anyone having decided
// that is safe. The hand-written denylists drifted behind DuckDB once already:
// parquet_full_metadata and read_duckdb read files under the storage root and
// enable_profiling wrote one, all three accepted (GHSA-r7vm-9hcg-xgf7).
//
// Extensions Arc can load are covered by TestSupportedExtensionFunctionsReviewed;
// duckdb_functions() lists only what is loaded, and the sandbox refuses autoload.
func TestCoreTableFunctionsReviewed(t *testing.T) {
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1}, zerolog.Nop())
	require.NoError(t, err)
	defer duck.Close()
	ctx := context.Background()

	// The catalog-by-name entries above are safe only because nothing lives
	// in the query engine's catalog (compaction's temp tables are created in
	// its own subprocess engine).
	var objects int
	require.NoError(t, duck.DB().QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM duckdb_tables()) +
		(SELECT count(*) FROM duckdb_views() WHERE NOT internal) +
		(SELECT count(*) FROM duckdb_databases() WHERE NOT internal AND database_name <> 'memory')`).Scan(&objects))
	require.Zero(t, objects, "the query engine's catalog is no longer empty: review every reviewCatalogByName entry, they take the name RBAC never sees")

	rows, err := duck.DB().QueryContext(ctx, `SELECT lower(function_name), function_type, count(*),
		string_agg(coalesce(macro_definition, ''), ' ') FROM duckdb_functions()
		WHERE function_type IN ('table', 'table_macro', 'pragma') GROUP BY ALL ORDER BY ALL`)
	require.NoError(t, err)
	defer rows.Close()

	seen := map[string]bool{}
	var problems []string
	tables := 0
	for rows.Next() {
		var name, kind, body string
		var overloads int
		require.NoError(t, rows.Scan(&name, &kind, &overloads, &body))

		if kind == "pragma" {
			// A pragma is not callable as `FROM name(...)`; PRAGMA and CALL
			// are the statements that run it.
			for _, q := range []string{"PRAGMA " + name, "CALL " + name + "()"} {
				if ValidateSQLRequest(q) == nil {
					problems = append(problems, "pragma "+name+" accepted: "+q)
				}
			}
			continue
		}
		tables++
		seen[name] = true

		if r, ok := reviewedCoreTableFunctions[name]; ok {
			// histogram_values passed review as a catalog reader; its body
			// is query_table(source), which reads a file path.
			if inner := refusedBody(body); inner != "" {
				problems = append(problems, name+" is reviewed, but its macro body calls the refused "+inner+"()")
			}
			if r.overloads != overloads {
				problems = append(problems, name+": reviewed with "+strconv.Itoa(r.overloads)+" overloads, DuckDB now has "+strconv.Itoa(overloads)+"; review the new signature")
			}
			continue
		}
		// Refused, and refused BECAUSE of this function: an unrelated rule
		// tripping on the probe would hide a gap.
		for _, q := range []string{
			"SELECT * FROM " + name + "('x')",
			`SELECT * FROM "` + name + `"('x')`,
			"SELECT * FROM main." + name + "('x')",
			"SELECT 1 FROM cpu WHERE EXISTS (FROM " + strings.ToUpper(name) + " ('x'))",
		} {
			if err := ValidateSQLRequest(q); err == nil || !strings.Contains(strings.ToLower(err.Error()), name) {
				problems = append(problems, name+" ("+kind+") is neither refused nor reviewed: "+q+" -> "+errString(err))
			}
		}
		where := "time > 0 AND EXISTS (FROM " + name + "('x'))"
		if _, err := (&DeleteHandler{}).validateWhereClause(where); err == nil || !strings.Contains(strings.ToLower(err.Error()), name) {
			problems = append(problems, name+" accepted in a DELETE predicate: "+where+" -> "+errString(err))
		}
	}
	require.NoError(t, rows.Err())
	require.Greater(t, tables, 50, "the catalog query found too few table functions")

	// A view is called by bare name; an internal view over a refused function
	// (pg_prepared_statements over duckdb_prepared_statements) must be
	// refused by name too.
	views, err := duck.DB().QueryContext(ctx, `SELECT lower(view_name), sql FROM duckdb_views() WHERE internal ORDER BY 1`)
	require.NoError(t, err)
	defer views.Close()
	nViews := 0
	for views.Next() {
		var name, def string
		require.NoError(t, views.Scan(&name, &def))
		nViews++
		if inner := refusedBody(def); inner != "" {
			if err := ValidateSQLRequest("SELECT * FROM " + name); err == nil {
				problems = append(problems, "internal view "+name+" calls the refused "+inner+"() and is accepted by name")
			}
		}
	}
	require.NoError(t, views.Err())
	require.Greater(t, nViews, 10, "the view query found too few internal views")

	// Scalars that run, plan or parse SQL. json_serialize_plan binds its
	// argument, which expanded a reader's glob across every database.
	scalars, err := duck.DB().QueryContext(ctx, `SELECT DISTINCT lower(function_name) FROM duckdb_functions()
		WHERE function_type IN ('scalar', 'macro', 'aggregate') AND regexp_matches(lower(function_name), '(sql|plan|query|exec|eval)') ORDER BY 1`)
	require.NoError(t, err)
	defer scalars.Close()
	nScalars := 0
	for scalars.Next() {
		var name string
		require.NoError(t, scalars.Scan(&name))
		nScalars++
		if _, ok := reviewedCoreScalars[name]; ok {
			seen[name] = true
			continue
		}
		if err := ValidateSQLRequest("SELECT " + name + "('x')"); err == nil || !strings.Contains(strings.ToLower(err.Error()), name) {
			problems = append(problems, "scalar "+name+" is neither refused nor reviewed -> "+errString(err))
		}
	}
	require.NoError(t, scalars.Err())
	require.Greater(t, nScalars, 3, "the scalar query found too few functions")
	for name := range reviewedCoreScalars {
		if !seen[name] {
			problems = append(problems, "reviewed scalar "+name+" does not exist in this DuckDB any more")
		}
	}

	for name := range reviewedCoreTableFunctions {
		if !seen[name] {
			problems = append(problems, "reviewed "+name+" does not exist in this DuckDB any more; remove it or review what replaced it")
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Errorf("DuckDB table functions nobody has decided about. Refuse each in ioTableFunctionPattern, dynamicSQLFunctionPattern or sideEffectFunctionPattern, or add it to reviewedCoreTableFunctions with the reason it is safe:\n  %s",
			strings.Join(problems, "\n  "))
	}
}

func errString(err error) string {
	if err == nil {
		return "<accepted>"
	}
	return err.Error()
}
