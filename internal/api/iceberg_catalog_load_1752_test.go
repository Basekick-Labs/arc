package api

import (
	"context"
	"sync"
	"testing"
)

// #1752: a table load asks the export for the files reported before it, but
// only on a GET, and only for a table the caller may read.
func TestIcebergCatalogLoadWaitsForFreshFilesOnlyWhereAllowed(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{"mydb": {"cpu", "mem"}}}
	rbac := &fakeRBAC{grants: map[string]map[string]bool{"mydb": {"cpu": true}}}
	app, h, tok := catalogTestApp(t, src, rbac)
	var mu sync.Mutex
	var asked []string
	h.SetAwaitFresh(func(_ context.Context, database, measurement string) {
		mu.Lock()
		asked = append(asked, database+"."+measurement)
		mu.Unlock()
	})
	calls := func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), asked...) }

	if status, _ := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables/cpu", tok); status != 200 {
		t.Fatalf("load cpu: %d", status)
	}
	if got := calls(); len(got) != 1 || got[0] != "mydb.cpu" {
		t.Fatalf("asked %v after a GET of a readable table, want [mydb.cpu]", got)
	}
	if status, _ := catalogGet(t, app, "HEAD", "/iceberg/v1/namespaces/arc_mydb/tables/cpu", tok); status != 204 {
		t.Fatalf("HEAD cpu: %d", status)
	}
	if status, _ := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables/mem", tok); status != 404 {
		t.Fatalf("load mem: %d, want 404", status)
	}
	if got := calls(); len(got) != 1 {
		t.Fatalf("asked %v: a HEAD or a table the caller cannot read must not ask", got)
	}
}
