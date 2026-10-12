package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// storeNotFoundBackend answers every listing the way a real backend does when
// its bucket or container does not exist: an error carrying
// storage.ErrStoreNotFound. Most S3-compatible stores create the bucket on
// the first authenticated write, so a fresh deployment queried before that
// write is in this state (#945).
type storeNotFoundBackend struct{ storage.Backend }

func (storeNotFoundBackend) List(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("failed to list S3 objects: %w", storage.ErrStoreNotFound)
}

func (storeNotFoundBackend) ListDirectories(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("failed to list S3 directories: %w", storage.ErrStoreNotFound)
}

func (storeNotFoundBackend) Exists(context.Context, string) (bool, error) {
	return false, fmt.Errorf("failed to check S3 object existence: %w", storage.ErrStoreNotFound)
}

// deniedBackend is the control: a listing failure that is NOT a missing
// store must still reach the client as an error. Without this half, a helper
// that matched too broadly would turn an authorization failure into an empty
// database list.
type deniedBackend struct{ storage.Backend }

func (deniedBackend) List(context.Context, string) ([]string, error) {
	return nil, errors.New("api error AccessDenied: access denied")
}

func (deniedBackend) ListDirectories(context.Context, string) ([]string, error) {
	return nil, errors.New("api error AccessDenied: access denied")
}

func (deniedBackend) Exists(context.Context, string) (bool, error) {
	return false, errors.New("api error AccessDenied: access denied")
}

// The four read paths #945 names are the only ones that treat a missing store
// as "no data yet". Everything else — the query planner's tier pruning,
// retention, backup, reconciliation — keeps seeing the error, because for
// those an empty listing is a decision input and a missing bucket would
// become a silently wrong answer.
func TestListDatabasesAndMeasurementsTolerateAMissingStore(t *testing.T) {
	h := NewDatabasesHandler(storeNotFoundBackend{}, nil, nil, zerolog.Nop())
	ctx := context.Background()

	dbs, err := h.listDatabases(ctx)
	if err != nil {
		t.Fatalf("listDatabases on a store that does not exist yet: %v", err)
	}
	if len(dbs) != 0 {
		t.Fatalf("listDatabases = %v, want empty", dbs)
	}

	measurements, err := h.listMeasurements(ctx, "anydb")
	if err != nil {
		t.Fatalf("listMeasurements on a store that does not exist yet: %v", err)
	}
	if len(measurements) != 0 {
		t.Fatalf("listMeasurements = %v, want empty", measurements)
	}
}

func TestListDatabasesStillFailsOnOtherListingErrors(t *testing.T) {
	h := NewDatabasesHandler(deniedBackend{}, nil, nil, zerolog.Nop())

	if _, err := h.listDatabases(context.Background()); err == nil {
		t.Fatal("listDatabases returned nil error for an authorization failure")
	}
	if _, err := h.listMeasurements(context.Background(), "anydb"); err == nil {
		t.Fatal("listMeasurements returned nil error for an authorization failure")
	}
}

// The symptom #945 actually reported: GET /api/v1/databases answered 500 with
// NoSuchBucket on a fresh deployment. It must answer 200 with an empty list.
func TestGetDatabasesReturns200OnAFreshStore(t *testing.T) {
	h := NewDatabasesHandler(storeNotFoundBackend{}, nil, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/databases", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/databases on a fresh store = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if databases, ok := body["databases"].([]any); ok && len(databases) != 0 {
		t.Fatalf("databases = %v, want empty", databases)
	}
}

func TestGetDatabasesStillFailsOnOtherListingErrors(t *testing.T) {
	h := NewDatabasesHandler(deniedBackend{}, nil, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/databases", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("GET /api/v1/databases returned 200 for an authorization failure")
	}
}

// A database on a fresh store does not exist yet: 404, not 500. Both checks
// are an Exists of the marker, which reports a bucket not created yet as
// ErrStoreNotFound (#1242), plus a listing that already did (#945).
func TestGetDatabaseIsNotFoundOnAFreshStore(t *testing.T) {
	h := NewDatabasesHandler(storeNotFoundBackend{}, nil, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	for _, path := range []string{"/api/v1/databases/anydb", "/api/v1/databases/anydb/measurements"} {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil), testRequestTimeoutMS)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s on a fresh store = %d, want 404", path, resp.StatusCode)
		}
	}
	exists, err := h.databaseExists(context.Background(), "anydb")
	if err != nil || exists {
		t.Fatalf("databaseExists on a fresh store = %v, %v; want false, nil", exists, err)
	}
}

// markerDeniedBackend refuses only the marker check; its listings answer a
// missing store, so a marker error that was swallowed would read as "no such
// database" instead of failing.
type markerDeniedBackend struct{ storeNotFoundBackend }

func (markerDeniedBackend) Exists(context.Context, string) (bool, error) {
	return false, errors.New("api error AccessDenied: access denied")
}

// The control: an authorization failure on the marker check is still an error.
func TestGetDatabaseStillFailsOnOtherErrors(t *testing.T) {
	h := NewDatabasesHandler(markerDeniedBackend{}, nil, nil, zerolog.Nop())
	if _, err := h.databaseExists(context.Background(), "anydb"); err == nil {
		t.Fatal("databaseExists returned nil error for an authorization failure on the marker check")
	}
}
