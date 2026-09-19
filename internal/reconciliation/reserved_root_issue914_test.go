package reconciliation

import "testing"

func TestIsParquetCandidateSkipsSchemaAnchorsIssue914(t *testing.T) {
	cases := map[string]bool{
		"_schema/db/cpu.parquet":                   false,
		"_schema/spoke/child/cpu.parquet":          false,
		"db/m_schema_logs/2026/01/01/00/a.parquet": true,
		"db/cpu/2026/01/01/00/a.parquet":           true,
	}
	for p, want := range cases {
		if got := isParquetCandidate(p); got != want {
			t.Errorf("isParquetCandidate(%q) = %v, want %v", p, got, want)
		}
	}
}
