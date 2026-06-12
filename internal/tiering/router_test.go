package tiering

import (
	"strings"
	"testing"
)

// TestBuildReadParquet_EscapesQuotes pins the escape contract for #307:
// every path interpolated into a DuckDB read_parquet() literal has single
// quotes doubled, so a quote-bearing path (e.g. from an unvalidated
// upstream database/measurement name) stays inside the string literal
// instead of terminating it.
func TestBuildReadParquet_EscapesQuotes(t *testing.T) {
	r := &Router{}

	tests := []struct {
		name  string
		paths []string
		want  string
	}{
		{
			name:  "empty",
			paths: nil,
			want:  "",
		},
		{
			name:  "single path no quotes",
			paths: []string{"mydb/cpu/2026/06/12/10/file.parquet"},
			want:  "read_parquet('mydb/cpu/2026/06/12/10/file.parquet')",
		},
		{
			name:  "single path with quote",
			paths: []string{"my'db/cpu/file.parquet"},
			want:  "read_parquet('my''db/cpu/file.parquet')",
		},
		{
			name:  "injection-shaped path stays inside the literal",
			paths: []string{"x') UNION SELECT * FROM read_parquet('/etc/secret') --"},
			want:  "read_parquet('x'') UNION SELECT * FROM read_parquet(''/etc/secret'') --')",
		},
		{
			name:  "multiple paths",
			paths: []string{"db/m/a.parquet", "db/m/b.parquet"},
			want:  "read_parquet(['db/m/a.parquet', 'db/m/b.parquet'], union_by_name=true)",
		},
		{
			name:  "multiple paths with quotes",
			paths: []string{"d'b/m/a.parquet", "db/m'/b.parquet"},
			want:  "read_parquet(['d''b/m/a.parquet', 'db/m''/b.parquet'], union_by_name=true)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := r.buildReadParquet(tt.paths); got != tt.want {
				t.Errorf("buildReadParquet(%v) = %q, want %q", tt.paths, got, tt.want)
			}
		})
	}
}

// TestBuildReadParquetExpr_EscapesQuotes verifies the public entry point
// routes through the escaped builder for both the single-tier and the
// multi-tier UNION ALL shapes.
func TestBuildReadParquetExpr_EscapesQuotes(t *testing.T) {
	r := &Router{}

	// Single tier: simple expression.
	single := r.BuildReadParquetExpr([]TieredPath{
		{Path: "my'db/cpu/a.parquet", Tier: TierHot},
	})
	if want := "read_parquet('my''db/cpu/a.parquet')"; single != want {
		t.Errorf("single-tier expr = %q, want %q", single, want)
	}

	// Two tiers: UNION ALL, both sides escaped.
	multi := r.BuildReadParquetExpr([]TieredPath{
		{Path: "d'b/m/hot.parquet", Tier: TierHot},
		{Path: "d'b/m/cold.parquet", Tier: TierCold},
	})
	for _, want := range []string{
		"read_parquet('d''b/m/hot.parquet')",
		"read_parquet('d''b/m/cold.parquet')",
		" UNION ALL ",
	} {
		if !strings.Contains(multi, want) {
			t.Errorf("multi-tier expr missing %q\ngot: %s", want, multi)
		}
	}
	// No raw (unescaped) quote-bearing path may survive.
	if strings.Contains(multi, "'d'b") {
		t.Errorf("multi-tier expr contains unescaped path: %s", multi)
	}
}
