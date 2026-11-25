package api

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/gofiber/fiber/v2"
)

// noticeRecordingCoordinator adds the database-drop notice to the delete fake.
type noticeRecordingCoordinator struct {
	*databaseDeleteCoordinatorFake
	notices []string
}

func (c *noticeRecordingCoordinator) ProposeIcebergDatabaseDropped(_ context.Context, db string) error {
	c.notices = append(c.notices, db)
	return nil
}

// #1731: a completed DELETE, and its 404 re-run, tell every node to drop its
// own Iceberg tables of the database; a partial delete does not.
func TestDatabaseDeleteSendsTheIcebergDropNotice(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	path := "db/cpu/local.parquet"
	if err := handler.storage.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := handler.storage.Write(ctx, path, []byte("hot")); err != nil {
		t.Fatal(err)
	}
	c := &noticeRecordingCoordinator{databaseDeleteCoordinatorFake: &databaseDeleteCoordinatorFake{
		primary: true, role: "writer", entries: []*raft.FileEntry{{Path: path, Database: "db"}},
	}}
	handler.SetCoordinator(c)
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK || len(c.notices) != 1 || c.notices[0] != "db" {
		t.Fatalf("status %d, notices %v", resp.StatusCode, c.notices)
	}
	// Repeating it answers 404 and sends the notice again.
	resp, err = app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusNotFound || len(c.notices) != 2 {
		t.Fatalf("re-run: status %d, notices %v", resp.StatusCode, c.notices)
	}
}

func TestPartialDatabaseDeleteSendsNoIcebergDropNotice(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	path := "db/cpu/local.parquet"
	if err := handler.storage.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := handler.storage.Write(ctx, path, []byte("hot")); err != nil {
		t.Fatal(err)
	}
	// The manifest step fails: nothing is deleted, the DELETE errors.
	c := &noticeRecordingCoordinator{databaseDeleteCoordinatorFake: &databaseDeleteCoordinatorFake{
		primary: true, role: "writer", entries: []*raft.FileEntry{{Path: path, Database: "db"}}, failOnBatch: 1,
	}}
	handler.SetCoordinator(c)
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == fiber.StatusOK || len(c.notices) != 0 {
		t.Fatalf("status %d, notices %v: a failed delete must not tell nodes to drop", resp.StatusCode, c.notices)
	}
}
