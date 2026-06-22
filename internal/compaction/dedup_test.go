package compaction

import (
	"strings"
	"testing"
)

// buildCompactionSQL joins the statement(s) buildCompactionQuery returns into a
// single string for the string-assertion unit tests below. The dedup path
// returns two statements (CREATE TEMP TABLE + COPY); the standard path one.
func buildCompactionSQL(fileListSQL, orderByClause, outputFile string, tagColumns []string) string {
	return strings.Join(buildCompactionQuery(fileListSQL, orderByClause, outputFile, tagColumns), "\n;\n")
}

func TestBuildCompactionQuery_NoDedup(t *testing.T) {
	query := buildCompactionSQL("['a.parquet', 'b.parquet']", `ORDER BY "time"`, "/tmp/out.parquet", nil)

	if strings.Contains(query, "ROW_NUMBER") {
		t.Error("expected no ROW_NUMBER without dedup keys")
	}
	if !strings.Contains(query, "read_parquet") {
		t.Error("expected read_parquet in query")
	}
	if !strings.Contains(query, `ORDER BY "time"`) {
		t.Error("expected ORDER BY clause")
	}
}

func TestBuildCompactionQuery_WithDedup(t *testing.T) {
	query := buildCompactionSQL(
		"['a.parquet', 'b.parquet']",
		`ORDER BY "time"`,
		"/tmp/out.parquet",
		[]string{"host", "region"},
	)

	if !strings.Contains(query, "ROW_NUMBER") {
		t.Error("expected ROW_NUMBER with dedup keys")
	}
	if !strings.Contains(query, `PARTITION BY "host", "region", "time"`) {
		t.Error("expected PARTITION BY with tag columns and time")
	}
	// Dedup uses QUALIFY (not a SELECT *, ROW_NUMBER ... ) WHERE rn=1 subquery):
	// the subquery/star-expansion form mis-binds time under union_by_name. See
	// buildCompactionQuery for the full explanation.
	if !strings.Contains(query, "QUALIFY ROW_NUMBER") {
		t.Errorf("expected QUALIFY ROW_NUMBER dedup form, got: %s", query)
	}
	if strings.Contains(query, "__dedup_rn") {
		t.Error("expected no __dedup_rn subquery column (QUALIFY form has no helper column)")
	}
	// time must be normalized to TIMESTAMPTZ so mixed-type partitions reconcile.
	if !strings.Contains(query, "make_timestamptz") {
		t.Error("expected time normalized via make_timestamptz")
	}
	if !strings.Contains(query, `ORDER BY "time"`) {
		t.Error("expected outer ORDER BY clause")
	}
}

func TestBuildCompactionQuery_EmptyDedup(t *testing.T) {
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/out.parquet", []string{})

	if strings.Contains(query, "ROW_NUMBER") {
		t.Error("expected no ROW_NUMBER with empty dedup keys")
	}
}

func TestBuildCompactionQuery_SpecialCharsInPath(t *testing.T) {
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/it's out.parquet", []string{"host"})

	if !strings.Contains(query, "it''s") {
		t.Error("expected escaped single quote in output path")
	}
}

func TestBuildCompactionQuery_IdentifierEscaping(t *testing.T) {
	// Defense-in-depth: even if validation is bypassed, identifiers are properly escaped
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/out.parquet", []string{`host"injection`})

	// Double-quote inside identifier should be doubled
	if !strings.Contains(query, `"host""injection"`) {
		t.Errorf("expected escaped double-quote in identifier, got: %s", query)
	}
}

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"simple", "host", true},
		{"with_underscore", "host_name", true},
		{"with_hyphen", "host-name", true},
		{"with_numbers", "sensor123", true},
		{"mixed_case", "HostName", true},
		{"empty", "", false},
		{"with_double_quote", `host"`, false},
		{"with_space", "host name", false},
		{"with_semicolon", "host;DROP", false},
		{"with_paren", "host()", false},
		{"with_comma", "a,b", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("isValidIdentifier(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestReadTagColumnsValidation(t *testing.T) {
	// isValidIdentifier is called internally by readTagColumnsFromParquet
	// but we can't easily test that without a DuckDB instance.
	// Instead, test the validation logic directly.

	// Valid tags should pass
	for _, tag := range []string{"host", "region", "sensor_id", "host-name"} {
		if !isValidIdentifier(tag) {
			t.Errorf("expected %q to be valid", tag)
		}
	}

	// Injection attempts should fail
	for _, tag := range []string{`host" OR 1=1--`, `"; DROP TABLE`, `host,region`} {
		if isValidIdentifier(tag) {
			t.Errorf("expected %q to be invalid", tag)
		}
	}
}

// TestBuildCompactionQuery_DedupNonDedupBothNormalizeTime asserts both branches
// (with and without tag columns) normalize time to TIMESTAMPTZ so a VARCHAR-time
// file never wedges compaction. String-only (no DuckDB), so it lives here in the
// untagged test file rather than the duckdb_arrow-gated integration test.
func TestBuildCompactionQuery_DedupNonDedupBothNormalizeTime(t *testing.T) {
	withTags := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", []string{"host"})
	noTags := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", nil)
	for name, q := range map[string]string{"dedup": withTags, "non-dedup": noTags} {
		if !strings.Contains(q, "make_timestamptz") || !strings.Contains(q, "TIMESTAMPTZ") {
			t.Errorf("%s branch does not normalize time to TIMESTAMPTZ:\n%s", name, q)
		}
	}
}
