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

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
)

type databaseDeleteCoordinatorFake struct {
	synced            int
	primary           bool
	role              string
	entries           []*raft.FileEntry
	batches           [][]string
	failOnBatch       int
	transientFailures int
	events            *[]string
	backend           storage.Backend
	probePath         string
}

func (f *databaseDeleteCoordinatorFake) IsPrimaryWriter() bool { return f.primary }
func (f *databaseDeleteCoordinatorFake) SyncManifest(context.Context) error {
	f.synced++
	return nil
}
func (f *databaseDeleteCoordinatorFake) Role() string { return f.role }

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
		if payload.Reason != databaseDeleteManifestReason {
			return fmt.Errorf("delete reason = %q, want %q", payload.Reason, databaseDeleteManifestReason)
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
	if f.transientFailures > 0 {
		f.transientFailures--
		return cluster.ErrNoLeaderKnown
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

func TestDatabasesHandler_DeleteRetriesTransientLeaderChangeWithoutTiering(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	backend := handler.storage
	ctx := context.Background()
	path := "db/cpu/local.parquet"
	if err := backend.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, path, []byte("hot data")); err != nil {
		t.Fatal(err)
	}
	coordinator := &databaseDeleteCoordinatorFake{
		primary: true, role: "writer", entries: []*raft.FileEntry{{Path: path, Database: "db"}},
		backend: backend, probePath: path, transientFailures: 1,
	}
	handler.SetCoordinator(coordinator, true)
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(coordinator.batches) != 2 || len(coordinator.entries) != 0 {
		t.Fatalf("manifest attempts=%d remaining=%d", len(coordinator.batches), len(coordinator.entries))
	}
	if exists, err := backend.Exists(ctx, path); err != nil || exists {
		t.Fatalf("file exists=%t err=%v after retry", exists, err)
	}
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
			handler.SetCoordinator(coordinator, true)

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
	handler.SetCoordinator(coordinator, true)

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

// An edge-sync hub registers a spoke's files with the spoke's own database
// name in the Database field but a namespaced path, {spoke}/{db}/... Deleting
// the hub's database of the same name must neither propose those paths (every
// node would unlink the spoke's files) nor fail on them: they are skipped, as
// the tiering manager's PrepareDatabaseDelete skips them.
func TestDatabasesHandler_Delete1094SkipsEdgeSyncSpokeEntries(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	if err := handler.storage.Write(ctx, "telemetry/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := handler.storage.Write(ctx, "telemetry/cpu/a.parquet", []byte("hot")); err != nil {
		t.Fatal(err)
	}
	coordinator := &databaseDeleteCoordinatorFake{
		primary: true,
		role:    "writer",
		entries: []*raft.FileEntry{
			{Path: "telemetry/cpu/a.parquet", Database: "telemetry"},
			{Path: "rocket-01/telemetry/cpu/b.parquet", Database: "telemetry"},
		},
	}
	handler.SetCoordinator(coordinator, true)

	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/telemetry?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var proposed []string
	for _, b := range coordinator.batches {
		proposed = append(proposed, b...)
	}
	if len(proposed) != 1 || proposed[0] != "telemetry/cpu/a.parquet" {
		t.Fatalf("proposed manifest deletes = %v, want only telemetry/cpu/a.parquet", proposed)
	}
}

// With tiering enabled, the tiering manager owns the manifest removal and the
// primary-writer gate (#1091); the coordinator path added for #1094 must not
// run as well, or every entry would be proposed for deletion twice.
func TestDatabasesHandler_Delete1094TieringManagerOwnsManifestRemoval(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	if err := handler.storage.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := handler.storage.Write(ctx, "db/cpu/a.parquet", []byte("hot")); err != nil {
		t.Fatal(err)
	}
	events := []string{}
	tm := &fakeDatabaseTieringManager{events: &events}
	handler.tieringManager = tm
	// A non-primary coordinator: with a tiering manager wired, its gate is
	// BeginDatabaseDelete's, not this one, so the request proceeds here.
	coordinator := &databaseDeleteCoordinatorFake{
		primary: false,
		role:    "reader",
		entries: []*raft.FileEntry{{Path: "db/cpu/a.parquet", Database: "db"}},
	}
	handler.SetCoordinator(coordinator, true)

	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(coordinator.batches) != 0 {
		t.Fatalf("coordinator proposed %d manifest batches though the tiering manager owns removal", len(coordinator.batches))
	}
	if len(events) == 0 || events[0] != "begin" {
		t.Fatalf("tiering events = %v, want the tiering manager's begin/prepare/cleanup", events)
	}
}

// A file this node holds but the manifest snapshot does not list under the
// database — an edge-sync hub indexes a spoke namespace's files by the spoke's
// own database name, or the file is not registered yet — is proposed too, as
// the tiering manager's PrepareDatabaseDelete does with its hot listing. The
// manifest is synced before the snapshot is read.
func TestDatabasesHandler_Delete1094ProposesLocalFilesMissingFromTheManifest(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	for _, p := range []string{"rocket-01/.arc-database", "rocket-01/telemetry/cpu/a.parquet"} {
		if err := handler.storage.Write(ctx, p, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	coordinator := &databaseDeleteCoordinatorFake{primary: true, role: "writer",
		entries: []*raft.FileEntry{{Path: "rocket-01/telemetry/cpu/a.parquet", Database: "telemetry"}}}
	handler.SetCoordinator(coordinator, true)

	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/rocket-01?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var proposed []string
	for _, b := range coordinator.batches {
		proposed = append(proposed, b...)
	}
	if len(proposed) != 1 || proposed[0] != "rocket-01/telemetry/cpu/a.parquet" {
		t.Fatalf("proposed = %v, want the locally listed file", proposed)
	}
	if coordinator.synced == 0 {
		t.Error("manifest not synced before reading the snapshot")
	}
}

// Without shared storage or replication the cluster gate is off, as the
// tiering manager's is: nodes share nothing and each may delete its own files.
func TestDatabasesHandler_Delete1094UngatedWithoutSharedStorageOrReplication(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	if err := handler.storage.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := handler.storage.Write(ctx, "db/cpu/a.parquet", []byte("hot")); err != nil {
		t.Fatal(err)
	}
	coordinator := &databaseDeleteCoordinatorFake{primary: false, role: "reader",
		entries: []*raft.FileEntry{{Path: "db/cpu/a.parquet", Database: "db"}}}
	handler.SetCoordinator(coordinator, false)

	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200 on an ungated cluster", resp.StatusCode)
	}
}
