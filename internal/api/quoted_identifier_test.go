package api

// Regression tests for quoted-identifier resolution in the query transform.
//
// Double-quoted tokens are DuckDB IDENTIFIERS, but MaskStringLiterals used to
// mask them like string literals: `"rocket-01".telemetry` became
// `__STR_0__.telemetry`, the placeholder matched the table pattern, and the
// unmask spliced the QUOTES back inside the read_parquet glob —
// `<root>/"rocket-01"/telemetry/**` — a literal directory that never exists.
// Net effect: any quoted database or measurement returned zero rows, and a
// hyphenated name (every edge-sync spoke ID in the docs) had NO working
// syntax, because the unquoted form is a SQL parser error. Live-reproduced on
// a hub in the 2026-08-19 edge-sync audit.

import (
	"context"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/rs/zerolog"
)

func newQuotedIdentTestHandler() *QueryHandler {
	return &QueryHandler{
		storage: &mockLocalBackend{basePath: "./data"},
		pruner:  pruning.NewPartitionPruner(zerolog.Nop()),
		logger:  zerolog.Nop(),
	}
}

func TestConvertSQL_QuotedIdentifiers(t *testing.T) {
	h := newQuotedIdentTestHandler()

	tests := []struct {
		name             string
		inputSQL         string
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			// The audit's exact failing shape: hyphenated spoke database.
			name:          "quoted hyphenated database, bare measurement",
			inputSQL:      `SELECT count(*) FROM "rocket-01".telemetry`,
			shouldContain: []string{"read_parquet('./data/rocket-01/telemetry/**/*.parquet'"},
			shouldNotContain: []string{
				`"rocket-01"`, // the quotes must never reach the path
				"__IDENT_",    // nor a leaked placeholder
			},
		},
		{
			name:          "quoted database and quoted measurement",
			inputSQL:      `SELECT * FROM "rocket-01"."engine-temp"`,
			shouldContain: []string{"read_parquet('./data/rocket-01/engine-temp/**/*.parquet'"},
			shouldNotContain: []string{
				`"rocket-01"`, `"engine-temp"`, "__IDENT_",
			},
		},
		{
			name:          "bare database, quoted measurement",
			inputSQL:      `SELECT * FROM telemetry."engine-temp"`,
			shouldContain: []string{"read_parquet('./data/telemetry/engine-temp/**/*.parquet'"},
		},
		{
			name:          "quoted measurement alone resolves under default",
			inputSQL:      `SELECT * FROM "engine-temp"`,
			shouldContain: []string{"read_parquet('./data/default/engine-temp/**/*.parquet'"},
		},
		{
			name:          "quoted db.meas in a JOIN",
			inputSQL:      `SELECT * FROM telemetry.cpu JOIN "rocket-01"."engine-temp" ON true`,
			shouldContain: []string{"read_parquet('./data/rocket-01/engine-temp/**/*.parquet'"},
		},
		{
			// Masking still protects string literals: a table-shaped string
			// value must never be rewritten.
			name:          "string literal containing a table reference is preserved",
			inputSQL:      `SELECT * FROM "rocket-01".telemetry WHERE msg = 'FROM mydb.cpu'`,
			shouldContain: []string{"'FROM mydb.cpu'", "read_parquet('./data/rocket-01/telemetry/**/*.parquet'"},
			shouldNotContain: []string{
				"read_parquet('./data/mydb/cpu/",
			},
		},
		{
			// An invalid quoted identifier must NEVER survive into the output
			// SQL: DuckDB executes a path-shaped quoted token in table
			// position as a replacement scan (deep-review Blocker on this
			// fix). The transform resolves it to an inert sentinel segment
			// instead — the raw token must be gone.
			name:          "traversal inside a quoted identifier resolves to the inert sentinel",
			inputSQL:      `SELECT * FROM "../../etc".telemetry`,
			shouldContain: []string{arcInvalidIdentifierSentinel},
			shouldNotContain: []string{
				`"../../etc"`,
				"__IDENT_",
			},
		},
		{
			// The reviewer's live-verified replacement-scan shape: a glob in
			// a quoted identifier. Both segments of the output path must be
			// sentinel-or-clean; the quoted glob must not survive.
			name:          "glob-shaped quoted identifier resolves to the inert sentinel",
			inputSQL:      `SELECT * FROM "db2/**/*.parquet"`,
			shouldContain: []string{arcInvalidIdentifierSentinel},
			shouldNotContain: []string{
				`"db2/**/*.parquet"`,
				"__IDENT_",
			},
		},
		{
			// A quoted CTE reference is the same virtual table as its
			// declaration; it must not be rewritten to a storage path.
			name:             "quoted CTE name is not rewritten",
			inputSQL:         `WITH "windowed-avg" AS (SELECT 1 AS x) SELECT * FROM "windowed-avg"`,
			shouldNotContain: []string{"read_parquet('./data/default/windowed-avg"},
		},
		{
			// Quoted column identifiers elsewhere in the query must be
			// restored untouched.
			name:          "quoted column identifier survives the round trip",
			inputSQL:      `SELECT "my col" FROM telemetry.cpu`,
			shouldContain: []string{`"my col"`, "read_parquet('./data/telemetry/cpu/**/*.parquet'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.convertSQLToStoragePaths(context.Background(), tt.inputSQL)
			for _, substr := range tt.shouldContain {
				if !strings.Contains(result, substr) {
					t.Errorf("missing %q\ninput:  %s\nresult: %s", substr, tt.inputSQL, result)
				}
			}
			for _, substr := range tt.shouldNotContain {
				if strings.Contains(result, substr) {
					t.Errorf("must not contain %q\ninput:  %s\nresult: %s", substr, tt.inputSQL, result)
				}
			}
		})
	}
}

// The header-database path (x-arc-database) must resolve quoted measurements
// the same way the dotted path does.
func TestConvertSQLWithDatabase_QuotedMeasurement(t *testing.T) {
	h := newQuotedIdentTestHandler()

	result := h.convertSQLToStoragePathsWithHeaderDB(context.Background(),
		`SELECT * FROM "engine-temp"`, "rocket-01")
	want := "read_parquet('./data/rocket-01/engine-temp/**/*.parquet'"
	if !strings.Contains(result, want) {
		t.Errorf("missing %q\nresult: %s", want, result)
	}
	if strings.Contains(result, "__IDENT_") {
		t.Errorf("leaked placeholder in: %s", result)
	}
}

// RBAC extraction must see the same names execution resolves — a quoted
// reference checked as placeholder text would be a grant that can never
// exist, and one not extracted at all would skip the permission check.
func TestExtractTableReferences_QuotedIdentifiers(t *testing.T) {
	sql := `SELECT * FROM "rocket-01"."engine-temp" JOIN "fleet-db".status ON true`
	masked, masks := sqlutil.MaskStringLiterals(sql, true)

	refs := extractTableReferences(masked, sqlutil.IdentifierNames(masks))
	got := make(map[string]bool, len(refs))
	for _, r := range refs {
		got[r.Database+"."+r.Measurement] = true
	}
	for _, want := range []string{"rocket-01.engine-temp", "fleet-db.status"} {
		if !got[want] {
			t.Errorf("missing extracted ref %q; got %v", want, got)
		}
	}
}

// With an x-arc-database header set, a quoted db.meas reference is cross-
// database syntax and must now be DETECTED (it was invisible when quoted
// tokens masked to string placeholders the scanner skipped... they matched
// as word-runs either way; this pins the intended behavior).
func TestHasCrossDatabaseSyntax_QuotedReference(t *testing.T) {
	if !hasCrossDatabaseSyntax(`SELECT * FROM "other-db".cpu`) {
		t.Error(`quoted "other-db".cpu not detected as cross-database syntax`)
	}
	if hasCrossDatabaseSyntax(`SELECT * FROM "engine-temp"`) {
		t.Error(`a lone quoted measurement wrongly detected as cross-database`)
	}
}

// ValidateSQLRequest must reject a path-shaped quoted identifier in table
// position — the double-quoted spelling of the GHSA-w8x2 replacement scan,
// including the comma cross-join position no table pattern reaches. Valid
// quoted tables and invalid quoted tokens OUTSIDE table position stay legal.
func TestValidateSQL_QuotedIdentifierReplacementScans(t *testing.T) {
	reject := []struct{ name, sql string }{
		{"direct FROM", `SELECT * FROM "db2/**/*.parquet"`},
		{"absolute path", `SELECT * FROM "/data/arc/db2/secrets/f.parquet"`},
		{"comma cross-join", `SELECT b.v FROM cpu, "db2/**/*.parquet" b`},
		{"JOIN position", `SELECT * FROM cpu JOIN "db2/x.parquet" b ON true`},
		{"subquery FROM", `SELECT * FROM (SELECT * FROM "db2/x.parquet")`},
		{"traversal", `SELECT * FROM "../../etc".telemetry`},
		{"backtick spelling", "SELECT * FROM `db2/**/*.parquet`"},
	}
	for _, tt := range reject {
		t.Run("reject "+tt.name, func(t *testing.T) {
			if err := ValidateSQLRequest(tt.sql); err == nil {
				t.Errorf("accepted: %s", tt.sql)
			}
		})
	}

	allow := []struct{ name, sql string }{
		{"valid quoted table", `SELECT * FROM "my-db".cpu`},
		{"valid quoted measurement", `SELECT * FROM "engine-temp"`},
		{"invalid quoted token as column", `SELECT "my col" FROM cpu`},
		{"path-shaped single-quoted VALUE", `SELECT * FROM cpu WHERE f = 'db2/**/*.parquet'`},
		{"quoted alias after table", `SELECT * FROM cpu "a b"`},
	}
	for _, tt := range allow {
		t.Run("allow "+tt.name, func(t *testing.T) {
			if err := ValidateSQLRequest(tt.sql); err != nil {
				t.Errorf("rejected legal SQL %q: %v", tt.sql, err)
			}
		})
	}
}
