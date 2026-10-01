package edgesync

import "testing"

func TestIsSyncableFileSkipsReservedRootsIssue914(t *testing.T) {
	for p, want := range map[string]bool{
		"_schema/db/cpu.parquet":                false,
		"_compaction_state/x.parquet":           false,
		"db/cpu/2026/01/01/00/a.parquet":        true,
		"db/cpu/2026/01/01/00/.staging.parquet": false,
	} {
		if got := isSyncableFile(p); got != want {
			t.Errorf("isSyncableFile(%q) = %v, want %v", p, got, want)
		}
	}
}
