package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type refusingDropper struct{ err error }

func (d *refusingDropper) OwnsMetadataKey(string) bool { return false }
func (d *refusingDropper) DropDatabase(context.Context, string) error {
	return d.err
}

// #1789: the Iceberg drop waits for an export of the database's tables and
// gives up when it does not end in time. The data is deleted either way, so
// the DELETE still succeeds, but its answer says the tables are still there
// and what to do; when the drop went through it says nothing of the kind.
func TestDatabaseDeleteReportsAnIcebergDropThatDidNotHappen(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"the drop was refused", errors.New("an Iceberg export of database \"mydb\" was still running and its tables were not dropped")},
		{"the drop went through", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			if err := backend.Write(context.Background(), "mydb/cpu/2026/01/01/00/a.parquet", []byte("x")); err != nil {
				t.Fatal(err)
			}
			h := NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
			h.SetIcebergDropper(&refusingDropper{err: tc.err})
			app := fiber.New()
			h.RegisterRoutes(app)
			resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/mydb?confirm=true", nil), testRequestTimeoutMS)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != fiber.StatusOK {
				t.Fatalf("DELETE: status %d, body %s", resp.StatusCode, body)
			}
			var got map[string]any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatal(err)
			}
			note, has := got["iceberg_cleanup"].(string)
			if tc.err == nil {
				if has {
					t.Fatalf("the answer reports pending Iceberg cleanup after a drop that went through: %q", note)
				}
				return
			}
			if !has || !strings.Contains(note, "repeat the DELETE") || !strings.Contains(note, "still running") {
				t.Fatalf("the answer does not say the Iceberg tables were left and what to do: %s", body)
			}
			if keys, _ := backend.List(context.Background(), "mydb/"); len(keys) != 0 {
				t.Fatalf("data left after the DELETE: %v", keys)
			}
		})
	}
}

// The DELETE repeated after a drop that did not happen: the data is gone,
// so the answer is 404 as for any database that does not exist, but the drop
// is asked for again, and if it is refused again the 404 says so too.
func TestDatabaseDeleteRepeatedAsksForTheIcebergDropAgain(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Write(context.Background(), "mydb/cpu/2026/01/01/00/a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	d := &countingDropper{err: errors.New("an Iceberg export of database \"mydb\" was still running and its tables were not dropped")}
	h := NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
	h.SetIcebergDropper(d)
	app := fiber.New()
	h.RegisterRoutes(app)
	del := func() (int, map[string]any) {
		t.Helper()
		resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/mydb?confirm=true", nil), testRequestTimeoutMS)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("body %s: %v", body, err)
		}
		return resp.StatusCode, got
	}
	if code, body := del(); code != fiber.StatusOK || body["iceberg_cleanup"] == nil {
		t.Fatalf("first DELETE: %d %v, want 200 with the cleanup pending", code, body)
	}
	// Repeated while the export is still running: refused again.
	code, body := del()
	if code != fiber.StatusNotFound {
		t.Fatalf("repeated DELETE: status %d, want 404", code)
	}
	if note, _ := body["iceberg_cleanup"].(string); !strings.Contains(note, "repeat the DELETE") {
		t.Fatalf("the 404 of a repeat whose drop was refused again does not say so: %v", body)
	}
	// Repeated once the export has ended: the drop runs.
	d.err = nil
	code, body = del()
	if code != fiber.StatusNotFound || body["iceberg_cleanup"] != nil {
		t.Fatalf("repeated DELETE once the drop can run: %d %v, want a plain 404", code, body)
	}
	if d.calls != 3 {
		t.Fatalf("the Iceberg drop was asked for %d times over three DELETEs, want 3", d.calls)
	}
}

type countingDropper struct {
	err   error
	calls int
}

func (d *countingDropper) OwnsMetadataKey(string) bool { return false }
func (d *countingDropper) DropDatabase(context.Context, string) error {
	d.calls++
	return d.err
}
