package api

// #1425 on a store that keeps empty prefixes: SeaweedFS keeps a deleted
// database's directory entries after its last object is gone, so a listing
// built from directories still finds the database while GET answers 404 for
// it. The local backend's own residue is removed by the delete; these tests
// stand in for the object store with a backend whose ListDirectories reports
// prefixes that hold nothing.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// ghostPrefixes adds empty prefixes to a backend's directory listing, as a
// store that keeps them does: "ghost" with a "cpu" measurement under it.
type ghostPrefixes struct {
	storage.DirectoryLister
}

func (g ghostPrefixes) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	dirs, err := g.DirectoryLister.ListDirectories(ctx, prefix)
	switch prefix {
	case "":
		dirs = append(dirs, "ghost")
	case "ghost/":
		dirs = append(dirs, "cpu")
	}
	return dirs, err
}

// ghostProbingBackend is a store with empty prefixes and a first-hit probe
// (S3, Azure, GCS). It counts the listings of a whole database.
type ghostProbingBackend struct {
	*storage.LocalBackend
	ghostPrefixes
	dbLists atomic.Int64
}

func (b *ghostProbingBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	return b.ghostPrefixes.ListDirectories(ctx, prefix)
}

func (b *ghostProbingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	if prefix != "" {
		b.dbLists.Add(1)
	}
	return b.LocalBackend.List(ctx, prefix)
}

// ghostListingBackend is the same store without the probe (a backend that
// does not implement storage.PrefixProber), so content is found by List.
type ghostListingBackend struct {
	storage.Backend
	ghostPrefixes
}

func (b ghostListingBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	return b.ghostPrefixes.ListDirectories(ctx, prefix)
}

func TestDatabasesHandler_EmptyPrefixesAreNotADatabase(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*storage.LocalBackend) (storage.Backend, *atomic.Int64)
	}{
		{"probing backend", func(l *storage.LocalBackend) (storage.Backend, *atomic.Int64) {
			b := &ghostProbingBackend{LocalBackend: l, ghostPrefixes: ghostPrefixes{l}}
			return b, &b.dbLists
		}},
		{"listing backend", func(l *storage.LocalBackend) (storage.Backend, *atomic.Int64) {
			return ghostListingBackend{Backend: l, ghostPrefixes: ghostPrefixes{l}}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()
			backend, dbLists := tc.build(local)
			if _, ok := backend.(storage.PrefixProber); ok != (dbLists != nil) {
				t.Fatalf("backend implements PrefixProber = %v, want %v", ok, dbLists != nil)
			}
			app := fiber.New()
			NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop()).RegisterRoutes(app)

			// A database written by ingest (no marker) and one created
			// empty (only the marker, which a local listing hides).
			if err := local.Write(ctx, "live/cpu/2026/01/01/00/a.parquet", []byte("data")); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("POST", "/api/v1/databases", bytes.NewReader([]byte(`{"name":"empty"}`)))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req, testRequestTimeoutMS)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != fiber.StatusCreated {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("create: status %d: %s", resp.StatusCode, body)
			}

			if got := listedDatabases(t, app); !equalStrings(got, []string{"empty", "live"}) {
				t.Errorf("databases listed = %v, want [empty live]: empty prefixes are not a database", got)
			}
			for path, want := range map[string]int{
				"/api/v1/databases/ghost":              fiber.StatusNotFound,
				"/api/v1/databases/ghost/measurements": fiber.StatusNotFound,
				"/api/v1/databases/live":               fiber.StatusOK,
				"/api/v1/databases/live/measurements":  fiber.StatusOK,
				"/api/v1/databases/empty":              fiber.StatusOK,
				"/api/v1/databases/empty/measurements": fiber.StatusOK,
			} {
				resp, err := app.Test(httptest.NewRequest("GET", path, nil), testRequestTimeoutMS)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != want {
					t.Errorf("GET %s: status %d, want %d: %s", path, resp.StatusCode, want, body)
				}
				if path == "/api/v1/databases/live/measurements" && resp.StatusCode == fiber.StatusOK {
					var m MeasurementListResponse
					if err := json.Unmarshal(body, &m); err != nil || m.Count != 1 || m.Measurements[0].Name != "cpu" {
						t.Errorf("GET %s = %s, want the cpu measurement", path, body)
					}
				}
			}

			// A database without a marker is found from its first file,
			// not by listing all of them.
			if dbLists != nil {
				dbLists.Store(0)
				resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/databases/live", nil), testRequestTimeoutMS)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != fiber.StatusOK {
					t.Fatalf("GET live: status %d", resp.StatusCode)
				}
				if n := dbLists.Load(); n != 0 {
					t.Errorf("GET of a database listed its files %d times, want 0 (the probe answers)", n)
				}
			}
		})
	}
}

// A database whose only content cannot be found without failing is listed,
// as before #1425: a failed check is not an absence.
func TestDatabasesHandler_ListKeepsADatabaseWhoseCheckFails(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	backend := failingGhostExists{ghostListingBackend{Backend: local, ghostPrefixes: ghostPrefixes{local}}}
	app := fiber.New()
	NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop()).RegisterRoutes(app)
	if got := listedDatabases(t, app); !equalStrings(got, []string{"ghost"}) {
		t.Errorf("databases listed = %v, want [ghost]", got)
	}
}

type failingGhostExists struct{ ghostListingBackend }

func (b failingGhostExists) Exists(ctx context.Context, path string) (bool, error) {
	if strings.HasPrefix(path, "ghost/") {
		return false, io.ErrUnexpectedEOF
	}
	return b.ghostListingBackend.Exists(ctx, path)
}

// checkRecordingBackend records the databases a listing checks one by one.
type checkRecordingBackend struct {
	*ghostProbingBackend
	mu      sync.Mutex
	checked map[string]bool
}

func (b *checkRecordingBackend) record(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.checked[strings.SplitN(path, "/", 2)[0]] = true
}

func (b *checkRecordingBackend) Exists(ctx context.Context, path string) (bool, error) {
	b.record(path)
	return b.ghostProbingBackend.Exists(ctx, path)
}

func (b *checkRecordingBackend) HasObjectsUnderPrefix(ctx context.Context, prefix string) (bool, error) {
	b.record(prefix)
	return b.ghostProbingBackend.HasObjectsUnderPrefix(ctx, prefix)
}

// coldTieringManager is fakeDatabaseTieringManager with real tier metadata.
type coldTieringManager struct {
	fakeDatabaseTieringManager
	metadata *tiering.MetadataStore
}

func (m *coldTieringManager) GetMetadata() *tiering.MetadataStore { return m.metadata }

// DatabaseHasTierRows answers from the metadata, as the tiering manager does.
func (m *coldTieringManager) DatabaseHasTierRows(ctx context.Context, db string) (bool, error) {
	dbs, err := m.metadata.GetAllDatabases(ctx)
	for _, d := range dbs {
		if d == db {
			return true, err
		}
	}
	return false, err
}

// The list costs nothing extra for a database with a file or a cold-tier
// row: only a database found as directories alone is checked.
func TestDatabasesHandler_ListChecksOnlyDatabasesWithNothingFound(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	local, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	backend := &checkRecordingBackend{
		ghostProbingBackend: &ghostProbingBackend{LocalBackend: local, ghostPrefixes: ghostPrefixes{local}},
		checked:             map[string]bool{},
	}
	if err := local.Write(ctx, "live/cpu/2026/01/01/00/a.parquet", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := local.Write(ctx, "empty/.arc-database", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	// A cold-only database whose hot directories are still there.
	if err := os.MkdirAll(filepath.Join(root, "colddb", "cpu"), 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	metadata, err := tiering.NewMetadataStore(db, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.RecordColdFile(ctx, &tiering.FileMetadata{
		Path: "colddb/cpu/2026/01/01/00/a.parquet", Database: "colddb", Measurement: "cpu",
		PartitionTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), SizeBytes: 4, CreatedAt: time.Now(),
	}, time.Now()); err != nil {
		t.Fatal(err)
	}

	h := NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
	h.SetTieringManager(&coldTieringManager{metadata: metadata})
	app := fiber.New()
	h.RegisterRoutes(app)

	if got := listedDatabases(t, app); !equalStrings(got, []string{"colddb", "empty", "live"}) {
		t.Errorf("databases listed = %v, want [colddb empty live]", got)
	}
	checked := make([]string, 0, len(backend.checked))
	for db := range backend.checked {
		checked = append(checked, db)
	}
	sort.Strings(checked)
	if !equalStrings(checked, []string{"empty", "ghost"}) {
		t.Errorf("databases checked one by one = %v, want [empty ghost]: one with a file or a cold row costs nothing", checked)
	}
}

// showDatabases runs SHOW DATABASES against the backend and returns the names.
func showDatabases(t *testing.T, backend storage.Backend) []string {
	t.Helper()
	dir := t.TempDir()
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dir, TempDirectory: dir}, zerolog.Nop())
	if err != nil {
		t.Skipf("DuckDB: %v", err)
	}
	t.Cleanup(func() { duck.Close() })
	app := fiber.New()
	NewQueryHandler(duck, backend, zerolog.Nop(), 30, 1000).RegisterRoutes(app)
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"sql":"SHOW DATABASES"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("SHOW DATABASES: status %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Data [][]any `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("SHOW DATABASES: %v: %s", err, body)
	}
	names := make([]string, 0, len(out.Data))
	for _, row := range out.Data {
		names = append(names, row[0].(string))
	}
	return names
}

// SHOW DATABASES reads directories too, so it showed a deleted database on a
// store that keeps empty prefixes; a failed check still shows the database.
func TestShowDatabases_EmptyPrefixesAreNotADatabase(t *testing.T) {
	ctx := context.Background()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if err := local.Write(ctx, "live/cpu/2026/01/01/00/a.parquet", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := local.Write(ctx, "empty/.arc-database", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	probing := &ghostProbingBackend{LocalBackend: local, ghostPrefixes: ghostPrefixes{local}}
	if got := showDatabases(t, probing); !equalStrings(got, []string{"empty", "live"}) {
		t.Errorf("SHOW DATABASES = %v, want [empty live]", got)
	}
	listing := ghostListingBackend{Backend: local, ghostPrefixes: ghostPrefixes{local}}
	if got := showDatabases(t, listing); !equalStrings(got, []string{"empty", "live"}) {
		t.Errorf("SHOW DATABASES without a probe = %v, want [empty live]", got)
	}
	if got := showDatabases(t, failingGhostExists{listing}); !equalStrings(got, []string{"empty", "ghost", "live"}) {
		t.Errorf("SHOW DATABASES with a failing check = %v, want [empty ghost live]", got)
	}
}

// getDatabaseStatus answers the status of a GET and its body.
func getDatabaseStatus(t *testing.T, app *fiber.App, path string) (int, string) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest("GET", path, nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// A cluster node can hold a database's directories but none of its files
// while the manifest lists them on peers (#1094): it is listed, and so are
// its measurements, by the list, GET, the measurements endpoint and SHOW
// DATABASES. A ghost beside it is not.
func TestDatabasesHandler_ManifestOnlyDatabaseIsListed(t *testing.T) {
	root := t.TempDir()
	local, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if err := os.MkdirAll(filepath.Join(root, "peerdb", "cpu"), 0o700); err != nil {
		t.Fatal(err)
	}
	backend := &ghostProbingBackend{LocalBackend: local, ghostPrefixes: ghostPrefixes{local}}
	entries := []*raft.FileEntry{{Path: "peerdb/cpu/2026/01/01/00/a.parquet", Database: "peerdb", Measurement: "cpu"}}

	h := NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
	h.SetCoordinator(&databaseDeleteCoordinatorFake{primary: true, entries: entries})
	app := fiber.New()
	h.RegisterRoutes(app)
	if got := listedDatabases(t, app); !equalStrings(got, []string{"peerdb"}) {
		t.Errorf("databases listed = %v, want [peerdb]", got)
	}
	for _, path := range []string{"/api/v1/databases/peerdb", "/api/v1/databases/peerdb/measurements"} {
		if code, body := getDatabaseStatus(t, app, path); code != fiber.StatusOK {
			t.Errorf("GET %s: status %d, want 200: %s", path, code, body)
		}
	}

	dir := t.TempDir()
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dir, TempDirectory: dir}, zerolog.Nop())
	if err != nil {
		t.Skipf("DuckDB: %v", err)
	}
	t.Cleanup(func() { duck.Close() })
	recording := &checkRecordingBackend{ghostProbingBackend: backend, checked: map[string]bool{}}
	qh := NewQueryHandler(duck, recording, zerolog.Nop(), 30, 1000)
	qh.coordinator = manifestReadiness{entries: entries}
	if got := qh.dropEmptyDatabaseDirectories(context.Background(), []string{"ghost", "peerdb", "_schema"}); !equalStrings(got, []string{"peerdb"}) {
		t.Errorf("SHOW DATABASES keeps %v, want [peerdb]", got)
	}
	if recording.checked["_schema"] {
		t.Error("SHOW DATABASES checked a hidden directory it drops anyway")
	}
}

// manifestReadiness is a query handler's cluster coordinator with a manifest.
type manifestReadiness struct{ entries []*raft.FileEntry }

func (manifestReadiness) ReplicationReady() bool                     { return true }
func (manifestReadiness) ReplicationCatchUpStatus() map[string]int64 { return nil }
func (m manifestReadiness) GetFileManifestByDatabase(db string) []*raft.FileEntry {
	var out []*raft.FileEntry
	for _, e := range m.entries {
		if e.Database == db {
			out = append(out, e)
		}
	}
	return out
}

// A database whose rows are all in the cold tier, with its hot directories
// left, still has its measurements listed.
func TestDatabasesHandler_ColdOnlyDatabaseMeasurementsAreListed(t *testing.T) {
	root := t.TempDir()
	local, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if err := os.MkdirAll(filepath.Join(root, "colddb", "cpu"), 0o700); err != nil {
		t.Fatal(err)
	}
	h := NewDatabasesHandler(local, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
	h.SetTieringManager(&fakeDatabaseTieringManager{tierRows: true})
	app := fiber.New()
	h.RegisterRoutes(app)
	if code, body := getDatabaseStatus(t, app, "/api/v1/databases/colddb/measurements"); code != fiber.StatusOK || !strings.Contains(body, `"cpu"`) {
		t.Errorf("GET colddb/measurements: status %d, want 200 listing cpu: %s", code, body)
	}
}
