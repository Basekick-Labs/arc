package database

import "testing"

// A handler built without an engine (tests) reaches AllowedDirectories through
// the arcx hook; a nil receiver must answer "no list", not panic (#1016). arcx
// treats an empty list as deny-all, so no list is the safe answer.
func TestAllowedDirectoriesNilEngine(t *testing.T) {
	var d *DuckDB
	if got := d.AllowedDirectories(); got != nil {
		t.Fatalf("nil engine: AllowedDirectories() = %v, want nil", got)
	}
	if got := (&DuckDB{}).AllowedDirectories(); got != nil {
		t.Fatalf("engine without config: AllowedDirectories() = %v, want nil", got)
	}
}
