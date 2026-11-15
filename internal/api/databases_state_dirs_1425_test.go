package api

// Follow-up to #1425 / #1427: deleting a database removed its own directory
// tree on local storage but left the empty directories of its Arc state
// beside it: _schema/<db>/ (anchors deleted, directory kept) and
// _compaction_state/<tier>/<db>/ (manifests deleted as jobs commit).

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/storage"
)

func mustExist(t *testing.T, root, rel string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
		t.Fatalf("%s should exist: %v", rel, err)
	}
}

func mustBeGone(t *testing.T, root, rel string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); !os.IsNotExist(err) {
		t.Errorf("%s is still on disk (stat err %v)", rel, err)
	}
}

func deleteDatabaseRequest(t *testing.T, app *fiber.App, name string, want int) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/"+name+"?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("DELETE %s: status %d, want %d: %s", name, resp.StatusCode, want, body)
	}
}

// commitManifest writes a compaction manifest for database and deletes it, as
// a committed job does, leaving its (now empty) directory behind.
func commitManifest(t *testing.T, mm *compaction.ManifestManager, tier, database string) {
	t.Helper()
	ctx := context.Background()
	p, err := mm.WriteManifest(ctx, &compaction.Manifest{
		OutputPath: database + "/cpu/2026/01/01/00/out.parquet", Database: database, Measurement: "cpu",
		PartitionPath: database + "/cpu/2026/01/01/00", Tier: tier, JobID: "job-" + tier, CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mm.DeleteManifest(ctx, p); err != nil {
		t.Fatal(err)
	}
}

// TestDatabasesHandler_DeleteRemovesStateDirectories1425 runs the real
// pieces: ingest writes the anchors through the field-schema registry, the
// compaction manifest manager writes and deletes a manifest, and the delete
// goes through the handler wired with the registry as cmd/arc/main.go wires it.
func TestDatabasesHandler_DeleteRemovesStateDirectories1425(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("dropdb", "cpu", jan1, map[string]interface{}{"v": []int64{1}})
	e.write("dropdb2", "cpu", jan1, map[string]interface{}{"v": []int64{1}})
	mm := compaction.NewManifestManager(e.backend, zerolog.Nop())
	commitManifest(t, mm, "hourly", "dropdb")
	commitManifest(t, mm, "hourly", "dropdb2")
	// A job still in flight for the database: its manifest must survive.
	live, err := mm.WriteManifest(context.Background(), &compaction.Manifest{
		OutputPath: "dropdb/cpu/2026/01/01/out.parquet", Database: "dropdb", Measurement: "cpu",
		PartitionPath: "dropdb/cpu/2026/01/01", Tier: "daily", JobID: "job-live", CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Preconditions, so the test cannot pass by never creating what it checks.
	mustExist(t, e.dataDir, fieldschema.AnchorKey("dropdb", "cpu"))
	mustExist(t, e.dataDir, fieldschema.AnchorKey("dropdb2", "cpu"))
	mustExist(t, e.dataDir, compaction.ManifestBasePath+"/hourly/dropdb")

	h := NewDatabasesHandler(e.backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
	h.SetFieldSchema(e.reg)
	app := fiber.New()
	h.RegisterRoutes(app)
	deleteDatabaseRequest(t, app, "dropdb", fiber.StatusOK)

	mustBeGone(t, e.dataDir, "dropdb")
	mustBeGone(t, e.dataDir, fieldschema.AnchorBasePath+"/dropdb")
	mustBeGone(t, e.dataDir, compaction.ManifestBasePath+"/hourly/dropdb")
	mustExist(t, e.dataDir, live)
	// Another database, whose name has the deleted one as a prefix, is untouched.
	mustExist(t, e.dataDir, fieldschema.AnchorKey("dropdb2", "cpu"))
	mustExist(t, e.dataDir, compaction.ManifestBasePath+"/hourly/dropdb2")
	// The shared tier directory and roots stay.
	mustExist(t, e.dataDir, compaction.ManifestBasePath+"/hourly")
	mustExist(t, e.dataDir, fieldschema.AnchorBasePath)
}

// TestDatabasesHandler_DeleteClearsStateDirectoryResidue1425 is a database an
// earlier version deleted: only empty state directories are left, the DELETE
// answers 404 and clears them. Nested spoke directories go deepest first; a
// sibling whose name extends the deleted one stays, even empty.
func TestDatabasesHandler_DeleteClearsStateDirectoryResidue1425(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, true)
	defer os.RemoveAll(tmpDir)
	for _, dir := range []string{
		"_schema/olddb",
		"_schema/olddb/child",
		"_compaction_state/hourly/olddb",
		"_compaction_state/daily/olddb/child",
		"_schema/olddb2",
		"_compaction_state/hourly/olddb2",
		"_compaction_state/hourly/olddb-x",
	} {
		if err := os.MkdirAll(filepath.Join(tmpDir, filepath.FromSlash(dir)), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	deleteDatabaseRequest(t, app, "olddb", fiber.StatusNotFound)

	for _, gone := range []string{"_schema/olddb", "_compaction_state/hourly/olddb", "_compaction_state/daily/olddb"} {
		mustBeGone(t, tmpDir, gone)
	}
	for _, kept := range []string{"_schema/olddb2", "_compaction_state/hourly/olddb2", "_compaction_state/hourly/olddb-x", "_compaction_state/daily"} {
		mustExist(t, tmpDir, kept)
	}
}

// TestRemoveDatabaseStateDirectoriesRecreateRace runs the removal against
// writers recreating the same database's anchor and manifest through a second
// backend instance, the way the compaction subprocess holds its own (with its
// own directory cache). Each write overlaps one removal pass, as one DELETE
// is; the write must succeed and its file must survive.
func TestRemoveDatabaseStateDirectoriesRecreateRace(t *testing.T) {
	tmpDir := t.TempDir()
	deleter, err := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer deleter.Close()
	writer, err := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ctx := context.Background()
	anchor := fieldschema.AnchorKey("db", "cpu")
	manifest := compaction.ManifestBasePath + "/hourly/db/job.json"

	for i := 0; i < 200; i++ {
		// Warm the writer's directory cache, then empty the directories.
		for _, k := range []string{anchor, manifest} {
			if err := writer.Write(ctx, k, []byte("x")); err != nil {
				t.Fatal(err)
			}
			if err := writer.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(3)
		go func() { defer wg.Done(); removeDatabaseStateDirectories(ctx, deleter, "db", zerolog.Nop()) }()
		for _, k := range []string{anchor, manifest} {
			go func(k string) {
				defer wg.Done()
				if err := writer.Write(ctx, k, []byte(fmt.Sprint(i))); err != nil {
					errs <- fmt.Errorf("write %s: %w", k, err)
				}
			}(k)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("iteration %d: %v", i, err)
		}
		for _, k := range []string{anchor, manifest} {
			if ok, err := writer.Exists(ctx, k); err != nil || !ok {
				t.Fatalf("iteration %d: %s lost (exists %v, err %v)", i, k, ok, err)
			}
		}
	}
}
