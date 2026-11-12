package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/iceberg"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// spellingBackend refuses "Mydb" as another spelling of "mydb", as a volume that folds names
// does (#1509), on any volume.
type spellingBackend struct{ *storage.LocalBackend }

func (b spellingBackend) CheckNameSpelling(segments ...string) error {
	if len(segments) > 0 && segments[0] == "Mydb" {
		return &storage.NameSpellingError{What: "database", Requested: "Mydb", OnDisk: "mydb"}
	}
	return nil
}

// existsFailingBackend fails the marker lookup: the existence check itself fails.
type existsFailingBackend struct{ *storage.LocalBackend }

func (existsFailingBackend) Exists(context.Context, string) (bool, error) {
	return false, errors.New("disk on fire")
}

type policyRig struct {
	app     *fiber.App
	manager *tieringPolicyTestManager
	dbs     *DatabasesHandler
	backend *storage.LocalBackend
}

func newPolicyCheckRig(t *testing.T, wrap func(*storage.LocalBackend) storage.Backend) *policyRig {
	t.Helper()
	app, manager := newTieringPoliciesTestApp(t, 0, nil)
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { local.Close() })
	var backend storage.Backend = local
	if wrap != nil {
		backend = wrap(local)
	}
	dbs := NewDatabasesHandler(backend, nil, nil, zerolog.Nop())
	// newTieringPoliciesTestApp registered a handler without the check; register one with it
	// on a fresh app, sharing the manager.
	handler := &TieringPoliciesHandler{manager: manager, logger: zerolog.Nop()}
	handler.SetDatabaseCheck(dbs.CheckDatabase)
	app = fiber.New()
	app.Put("/api/v1/tiering/policies/:database", handler.SetPolicy)
	app.Delete("/api/v1/tiering/policies/:database", handler.DeletePolicy)
	return &policyRig{app: app, manager: manager, dbs: dbs, backend: local}
}

func (r *policyRig) do(t *testing.T, method, db string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, "/api/v1/tiering/policies/"+db, strings.NewReader(`{"hot_max_age_days":3}`))
	req.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	resp, err := r.app.Test(req, 5000)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func (r *policyRig) stored(t *testing.T, db string) bool {
	t.Helper()
	p, err := r.manager.policies.Get(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return p != nil
}

// A policy PUT for a database that does not exist answers 404 and stores nothing (#1626).
func TestPolicyPutUnknownDatabase404_Issue1626(t *testing.T) {
	r := newPolicyCheckRig(t, nil)
	status, body := r.do(t, http.MethodPut, "nope")
	if status != fiber.StatusNotFound || !strings.Contains(body, "Database 'nope' not found") {
		t.Fatalf("PUT unknown: %d %s, want 404 Database 'nope' not found", status, body)
	}
	if r.stored(t, "nope") {
		t.Fatal("a policy was stored for a database that does not exist")
	}
}

// A database that exists by its marker alone (POST /api/v1/databases), or by its cold-tier rows
// alone, takes a policy.
func TestPolicyPutExistingDatabase200_Issue1626(t *testing.T) {
	t.Run("marker only", func(t *testing.T) {
		r := newPolicyCheckRig(t, nil)
		if err := r.backend.Write(context.Background(), "made/.arc-database", []byte("{}")); err != nil {
			t.Fatal(err)
		}
		if status, body := r.do(t, http.MethodPut, "made"); status != fiber.StatusOK {
			t.Fatalf("PUT marker-only: %d %s, want 200", status, body)
		}
		if !r.stored(t, "made") {
			t.Fatal("policy not stored")
		}
	})
	t.Run("tier rows only", func(t *testing.T) {
		r := newPolicyCheckRig(t, nil)
		r.dbs.SetTieringManager(&fakeDatabaseTieringManager{tierRows: true})
		if status, body := r.do(t, http.MethodPut, "coldonly"); status != fiber.StatusOK {
			t.Fatalf("PUT cold-only: %d %s, want 200", status, body)
		}
	})
}

// The new refusals keep the database routes' statuses: a malformed name 400, a name that exists
// in another spelling 400 with the spelling text, a failed check 500, a reserved or warehouse
// directory 404.
func TestPolicyPutRefusals_Issue1626(t *testing.T) {
	t.Run("invalid name 400", func(t *testing.T) {
		r := newPolicyCheckRig(t, nil)
		status, body := r.do(t, http.MethodPut, ".hidden")
		if status != fiber.StatusBadRequest || !strings.Contains(body, `invalid database name \".hidden\"`) {
			t.Fatalf("PUT .hidden: %d %s, want 400 invalid database name", status, body)
		}
	})
	t.Run("spelling 400", func(t *testing.T) {
		r := newPolicyCheckRig(t, func(l *storage.LocalBackend) storage.Backend { return spellingBackend{l} })
		status, body := r.do(t, http.MethodPut, "Mydb")
		if status != fiber.StatusBadRequest || !strings.Contains(body, "does not distinguish names") {
			t.Fatalf("PUT Mydb: %d %s, want 400 with the spelling text", status, body)
		}
	})
	t.Run("check failure 500", func(t *testing.T) {
		r := newPolicyCheckRig(t, func(l *storage.LocalBackend) storage.Backend { return existsFailingBackend{l} })
		status, body := r.do(t, http.MethodPut, "db1")
		if status != fiber.StatusInternalServerError || !strings.Contains(body, "Failed to check database") {
			t.Fatalf("PUT with a failing check: %d %s, want 500", status, body)
		}
		if r.stored(t, "db1") {
			t.Fatal("stored despite the failed check")
		}
	})
	t.Run("reserved root 404", func(t *testing.T) {
		r := newPolicyCheckRig(t, nil)
		if err := r.backend.Write(context.Background(), "_schema/x/y.parquet", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if status, body := r.do(t, http.MethodPut, "_schema"); status != fiber.StatusNotFound {
			t.Fatalf("PUT _schema: %d %s, want 404", status, body)
		}
	})
	t.Run("warehouse dir 404", func(t *testing.T) {
		r := newPolicyCheckRig(t, nil)
		if err := r.backend.Write(context.Background(), "lv_wh/arc_db.db/t/metadata/v1.metadata.json", []byte("x")); err != nil {
			t.Fatal(err)
		}
		r.dbs.SetWarehouseLayout(warehouseAt("lv_wh"))
		if status, body := r.do(t, http.MethodPut, "lv_wh"); status != fiber.StatusNotFound {
			t.Fatalf("PUT lv_wh: %d %s, want 404", status, body)
		}
	})
}

// DELETE is not checked: it removes the policy a deleted database left behind.
func TestPolicyDeleteOrphan200_Issue1626(t *testing.T) {
	r := newPolicyCheckRig(t, nil)
	if err := r.backend.Write(context.Background(), "gone/.arc-database", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if status, body := r.do(t, http.MethodPut, "gone"); status != fiber.StatusOK {
		t.Fatalf("PUT: %d %s", status, body)
	}
	if err := r.backend.Delete(context.Background(), "gone/.arc-database"); err != nil {
		t.Fatal(err)
	}
	if status, body := r.do(t, http.MethodDelete, "gone"); status != fiber.StatusOK {
		t.Fatalf("DELETE orphan policy: %d %s, want 200", status, body)
	}
}

// warehouseAt is the layout of a warehouse at <root>/<rel>, through the real
// iceberg.NewWarehouseLayout, so these tests exercise the rule main.go wires.
func warehouseAt(rel string) WarehouseDirs {
	return iceberg.NewWarehouseLayout("s3://bucket/prefix", "s3://bucket/prefix/"+rel, "arc")
}
