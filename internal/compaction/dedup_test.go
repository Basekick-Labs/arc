package compaction

import (
	"strings"
	"testing"
)

func TestBuildCompactionQuery_NoDedup(t *testing.T) {
	query := buildCompactionQuery("['a.parquet', 'b.parquet']", `ORDER BY "time"`, "/tmp/out.parquet", nil)

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
	query := buildCompactionQuery(
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
	if !strings.Contains(query, "EXCLUDE (__dedup_rn)") {
		t.Error("expected EXCLUDE __dedup_rn")
	}
	if !strings.Contains(query, "__dedup_rn = 1") {
		t.Error("expected WHERE __dedup_rn = 1")
	}
	if !strings.Contains(query, `ORDER BY "time"`) {
		t.Error("expected outer ORDER BY clause")
	}
}

func TestBuildCompactionQuery_EmptyDedup(t *testing.T) {
	query := buildCompactionQuery("['a.parquet']", "", "/tmp/out.parquet", []string{})

	if strings.Contains(query, "ROW_NUMBER") {
		t.Error("expected no ROW_NUMBER with empty dedup keys")
	}
}

func TestBuildCompactionQuery_SpecialCharsInPath(t *testing.T) {
	query := buildCompactionQuery("['a.parquet']", "", "/tmp/it's out.parquet", []string{"host"})

	if !strings.Contains(query, "it''s") {
		t.Error("expected escaped single quote in output path")
	}
}

func TestBuildCompactionQuery_IdentifierEscaping(t *testing.T) {
	// Defense-in-depth: even if validation is bypassed, identifiers are properly escaped
	query := buildCompactionQuery("['a.parquet']", "", "/tmp/out.parquet", []string{`host"injection`})

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
