package compaction

import (
	"context"
	"testing"
)

// Field schema anchors (#914) live under _schema/ at the storage root; the
// compaction discovery must not treat that directory (or _compaction_state)
// as a database.
func TestListDatabasesSkipsReservedRootsIssue914(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	for _, k := range []string{
		"db1/cpu/2025/01/01/00/file1.parquet",
		"_schema/db1/cpu.parquet",
		"_compaction_state/hourly/db1/job.json",
	} {
		if err := backend.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	databases, err := manager.listDatabases(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(databases) != 1 || databases[0] != "db1" {
		t.Fatalf("databases=%v", databases)
	}
}
