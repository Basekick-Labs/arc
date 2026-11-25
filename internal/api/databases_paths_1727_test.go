package api

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// A database delete that completes drops the export's tables itself; asking
// for a reconcile there would race the drop and could recreate a table
// (#1727). Only a delete that stops part-way reports its directory.
func TestCompletedDatabaseDeleteReportsNoPaths(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	defer os.RemoveAll(tmpDir)
	var reported []string
	handler.SetOnPathsChanged(func(keys []string) { reported = append(reported, keys...) })
	backend, _ := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	defer backend.Close()
	_ = backend.Write(context.Background(), "deletedb/m1/data1.parquet", []byte("data"))
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/deletedb?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if len(reported) != 0 {
		t.Fatalf("a completed delete reported %v; it must leave the tables to its own catalog drop", reported)
	}
	src, err := os.ReadFile("databases.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "if len(deleteErrors) > 0 && h.onPathsChanged != nil {") {
		t.Fatal("the partial-delete report is no longer guarded by deleteErrors")
	}
}
