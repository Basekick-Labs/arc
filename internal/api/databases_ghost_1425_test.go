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

	"github.com/basekick-labs/arc/internal/config"
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
