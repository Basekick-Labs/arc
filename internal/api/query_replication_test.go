package api

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

func TestReplicationSQLSnapshotFollowsHTTPRequestLifetime(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	h := &QueryHandler{storage: backend, logger: zerolog.Nop(), queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize)}
	view := replicaview.NewView()
	h.SetReplicationView(view, func(path string) string { return filepath.Join(root, path) })
	identity := "00000000000000010000000000000001"
	coverage, _ := replicaview.FromIdentities([]string{identity})
	shadow := replicaview.File{Path: ".replica/db/cpu/shadow.parquet", SHA256: "shadow", Metadata: replicaview.FileMetadata{Database: "db", Measurement: "cpu", Hour: 1, Coverage: coverage, Columns: []string{"time", "v"}, Segments: []replicaview.Segment{{Identity: identity, TotalRows: 2, Start: 0, End: 2}}}}
	if err := view.Publish(shadow); err != nil {
		t.Fatal(err)
	}
	var request fasthttp.RequestCtx
	before, _, err := h.getTransformedSQL(&request, "SELECT * FROM cpu", "db")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(before, "WITH ORDINALITY") {
		t.Fatalf("replica source missing: %s", before)
	}
	primary := replicaview.File{Path: "db/cpu/primary.parquet", SHA256: "primary", Metadata: replicaview.FileMetadata{Database: "db", Measurement: "cpu", Hour: 1, Coverage: coverage}}
	if err := view.Replace([]string{shadow.Path}, []replicaview.File{primary}); err != nil {
		t.Fatal(err)
	}
	if view.CanUnlink(shadow.Path) {
		t.Fatal("query files released at SQL transform return")
	}
	// Repeated references within a single request reuse exactly one snapshot.
	same, _, err := h.getTransformedSQL(&request, "SELECT * FROM cpu", "db")
	if err != nil || same != before {
		t.Fatalf("source changed inside one request: %s %v", same, err)
	}
	request.ResetUserValues()
	if !view.CanUnlink(shadow.Path) {
		t.Fatal("HTTP request reset did not release the snapshot")
	}
	var next fasthttp.RequestCtx
	defer next.ResetUserValues()
	after, parallel, _, err := h.getTransformedSQLForParallel(&next, "SELECT * FROM cpu", "db")
	if err != nil {
		t.Fatal(err)
	}
	if parallel != nil || strings.Contains(after, "shadow.parquet") || !strings.Contains(after, "primary.parquet") {
		t.Fatalf("next query reused obsolete/parallel sources: %s", after)
	}
}
