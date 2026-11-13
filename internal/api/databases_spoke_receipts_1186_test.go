package api

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A confirmed database delete marks the spoke's receipts removed before
// anything is removed, with or without tiering (#1186). Before, only the
// tiering path reached the receipts, and with the wrong mark.
func TestDatabasesHandler_DeleteMarksSpokeReceiptsFirst(t *testing.T) {
	for _, withTiering := range []bool{false, true} {
		t.Run(fmt.Sprintf("tiering=%v", withTiering), func(t *testing.T) {
			handler, app, tmpDir := setupTestDatabasesHandler(t, true)
			defer os.RemoveAll(tmpDir)
			ctx := context.Background()
			backend, err := storage.NewLocalBackend(tmpDir, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			path := "rocket01/telemetry/cpu/data.parquet"
			if err := backend.Write(ctx, path, []byte("data")); err != nil {
				t.Fatal(err)
			}
			var events []string
			handler.storage = &orderedDeleteBackend{Backend: backend, events: &events}
			if withTiering {
				handler.SetTieringManager(&fakeDatabaseTieringManager{events: &events})
			}
			var marked []string
			handler.SetOnSpokeNamespaceRemoved(func(_ context.Context, name string) error {
				events = append(events, "mark")
				marked = append(marked, name)
				return nil
			})

			resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/rocket01?confirm=true", nil), testRequestTimeoutMS)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != fiber.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d: %s", resp.StatusCode, body)
			}
			if len(marked) != 1 || marked[0] != "rocket01" {
				t.Fatalf("marked %v, want [rocket01]", marked)
			}
			for i, e := range events {
				if e == "mark" {
					break
				}
				if e == "prepare" || e == "hot-delete" || e == "cleanup" {
					t.Fatalf("events %v: %s ran before the receipts were marked (index %d)", events, e, i)
				}
			}
		})
	}
}

// A marking failure refuses the delete and removes nothing.
func TestDatabasesHandler_DeleteRefusedWhenReceiptMarkFails(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	defer os.RemoveAll(tmpDir)
	ctx := context.Background()
	backend, err := storage.NewLocalBackend(tmpDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	path := "rocket01/telemetry/cpu/data.parquet"
	if err := backend.Write(ctx, path, []byte("data")); err != nil {
		t.Fatal(err)
	}
	var events []string
	handler.storage = &orderedDeleteBackend{Backend: backend, events: &events}
	handler.SetTieringManager(&fakeDatabaseTieringManager{events: &events})
	handler.SetOnSpokeNamespaceRemoved(func(context.Context, string) error { return fmt.Errorf("database is locked") })

	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/rocket01?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("status %d, want 500", resp.StatusCode)
	}
	for _, e := range events {
		if e == "prepare" || e == "hot-delete" || e == "cleanup" {
			t.Fatalf("events %v: the delete went ahead after the mark failed", events)
		}
	}
	if exists, err := backend.Exists(ctx, path); err != nil || !exists {
		t.Fatalf("file exists=%v err=%v after a refused delete", exists, err)
	}
}
