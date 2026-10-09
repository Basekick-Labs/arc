package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
)

type databaseDeleteCoordinatorFake struct {
	primary     bool
	role        string
	entries     []*raft.FileEntry
	batches     [][]string
	failOnBatch int
	events      *[]string
	backend     storage.Backend
	probePath   string
}

func (f *databaseDeleteCoordinatorFake) IsPrimaryWriter() bool { return f.primary }
func (f *databaseDeleteCoordinatorFake) Role() string          { return f.role }

func (f *databaseDeleteCoordinatorFake) GetFileManifestByDatabase(database string) []*raft.FileEntry {
	var out []*raft.FileEntry
	for _, entry := range f.entries {
		if entry != nil && entry.Database == database {
			out = append(out, entry)
		}
	}
	return out
}

func (f *databaseDeleteCoordinatorFake) BatchFileOpsInManifestContext(ctx context.Context, ops []raft.BatchFileOp) error {
	paths := make([]string, 0, len(ops))
	for _, op := range ops {
		if op.Type != raft.CommandDeleteFile {
			return fmt.Errorf("manifest op type = %v, want delete", op.Type)
		}
		var payload raft.DeleteFilePayload
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			return err
		}
		if payload.Reason != "database-delete" {
			return fmt.Errorf("delete reason = %q, want database-delete", payload.Reason)
		}
		paths = append(paths, payload.Path)
	}
	f.batches = append(f.batches, paths)
	if f.events != nil {
		*f.events = append(*f.events, "manifest")
	}
	if f.backend != nil && f.probePath != "" {
		if exists, err := f.backend.Exists(ctx, f.probePath); err != nil || !exists {
			return fmt.Errorf("local copy %q was removed before manifest update: exists=%t err=%v", f.probePath, exists, err)
		}
	}
	if f.failOnBatch > 0 && len(f.batches) == f.failOnBatch {
		return errors.New("manifest unavailable")
	}
	removed := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		removed[path] = struct{}{}
	}
	kept := f.entries[:0]
	for _, entry := range f.entries {
		if entry == nil {
			continue
		}
		if _, ok := removed[entry.Path]; !ok {
			kept = append(kept, entry)
		}
	}
	f.entries = kept
	return nil
}

type databaseDeleteRecordingBackend struct {
	storage.Backend
	events *[]string
}

func (b *databaseDeleteRecordingBackend) Delete(ctx context.Context, path string) error {
	*b.events = append(*b.events, "delete:"+path)
	return b.Backend.Delete(ctx, path)
}

func (b *databaseDeleteRecordingBackend) DeleteBatch(ctx context.Context, paths []string) error {
	*b.events = append(*b.events, "delete-batch")
	if batch, ok := b.Backend.(storage.BatchDeleter); ok {
		return batch.DeleteBatch(ctx, paths)
	}
	for _, path := range paths {
		if err := b.Backend.Delete(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

func TestDatabasesHandler_Delete1094RejectsNonPrimaryWriter(t *testing.T) {
	for _, role := range []string{"writer", "reader", "compactor"} {
		t.Run(role, func(t *testing.T) {
			handler, app, tmpDir := setupTestDatabasesHandler(t, true)
			t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
			coordinator := &databaseDeleteCoordinatorFake{role: role, primary: false}
			handler.SetCoordinator(coordinator)

			resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != fiber.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", resp.StatusCode)
			}
			if len(coordinator.batches) != 0 {
				t.Fatalf("manifest batches = %d, want none on rejected request", len(coordinator.batches))
			}
		})
	}
}

func TestDatabasesHandler_Delete1094ManifestFirstChunkedAndRetryable(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	backend := handler.storage
	ctx := context.Background()
	const localPath = "db/cpu/local.parquet"
	if err := backend.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, localPath, []byte("hot data")); err != nil {
		t.Fatal(err)
	}

	events := []string{}
	entries := make([]*raft.FileEntry, 0, databaseDeleteManifestChunkSize+1)
	for i := 0; i < databaseDeleteManifestChunkSize+1; i++ {
		path := fmt.Sprintf("db/cpu/remote_%04d.parquet", i)
		if i == 0 {
			path = localPath
		}
		entries = append(entries, &raft.FileEntry{Path: path, Database: "db"})
	}
	coordinator := &databaseDeleteCoordinatorFake{
		primary:     true,
		role:        "writer",
		entries:     entries,
		events:      &events,
		backend:     backend,
		probePath:   localPath,
		failOnBatch: 2,
	}
	handler.storage = &databaseDeleteRecordingBackend{Backend: backend, events: &events}
	handler.SetCoordinator(coordinator)

	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("first status = %d, want 500", resp.StatusCode)
	}
	_ = resp.Body.Close()
	if len(coordinator.batches) != 2 || len(coordinator.batches[0]) != databaseDeleteManifestChunkSize || len(coordinator.batches[1]) != 1 {
		t.Fatalf("manifest batch sizes = %v, want [%d 1]", batchSizes(coordinator.batches), databaseDeleteManifestChunkSize)
	}
	if len(events) != 2 || events[0] != "manifest" || events[1] != "manifest" {
		t.Fatalf("events after manifest failure = %v, want only the two manifest attempts", events)
	}
	if exists, err := backend.Exists(ctx, localPath); err != nil || !exists {
		t.Fatalf("local file after manifest failure: exists=%t err=%v, want preserved", exists, err)
	}
	if exists, err := backend.Exists(ctx, "db/.arc-database"); err != nil || !exists {
		t.Fatalf("database marker after manifest failure: exists=%t err=%v, want preserved", exists, err)
	}

	coordinator.failOnBatch = 0
	resp, err = app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("retry status = %d, want 200", resp.StatusCode)
	}
	if len(coordinator.entries) != 0 {
		t.Fatalf("remaining manifest entries = %d, want none", len(coordinator.entries))
	}
	firstDelete := -1
	for i, event := range events {
		if strings.HasPrefix(event, "delete") {
			firstDelete = i
			break
		}
	}
	if firstDelete < 0 || firstDelete < 3 {
		t.Fatalf("event order = %v, want all manifest batches before storage deletes", events)
	}
	if exists, err := backend.Exists(ctx, localPath); err != nil || exists {
		t.Fatalf("local file after retry: exists=%t err=%v, want deleted", exists, err)
	}
}

func TestDatabasesHandler_Delete1094StandaloneKeepsExistingBehavior(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	path := "db/cpu/local.parquet"
	if err := handler.storage.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := handler.storage.Write(ctx, path, []byte("hot data")); err != nil {
		t.Fatal(err)
	}
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if _, err := os.Stat(filepath.Join(tmpDir, filepath.FromSlash(path))); !os.IsNotExist(err) {
		t.Fatalf("local file remains after standalone delete: %v", err)
	}
}

func batchSizes(batches [][]string) []int {
	sizes := make([]int, len(batches))
	for i, batch := range batches {
		sizes[i] = len(batch)
	}
	return sizes
}
