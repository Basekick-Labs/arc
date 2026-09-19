package storage

import "testing"

func TestIsReservedRootDir(t *testing.T) {
	for name, want := range map[string]bool{"_schema": true, "_schema/": true, "_compaction_state": true, ".hidden": true, "db": false, "my_db": false, "": false} {
		if got := IsReservedRootDir(name); got != want {
			t.Errorf("IsReservedRootDir(%q) = %v, want %v", name, got, want)
		}
	}
}
