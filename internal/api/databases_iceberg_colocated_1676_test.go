package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// colocatedDropper stands in for iceberg.Exporter: it claims a colocated
// table's metadata keys and records what is still on storage when the
// DELETE hands over to it.
type colocatedDropper struct {
	backend   storage.Backend
	sawAtDrop []string
	dropped   bool
}

func (d *colocatedDropper) OwnsMetadataKey(key string) bool {
	return strings.Contains(key, "/metadata/") && !strings.HasSuffix(key, ".parquet")
}

func (d *colocatedDropper) DropDatabase(ctx context.Context, database string) error {
	d.dropped = true
	keys, err := d.backend.List(ctx, database+"/")
	if err != nil {
		return err
	}
	d.sawAtDrop = keys
	for _, k := range keys {
		_ = d.backend.Delete(ctx, k)
	}
	return nil
}

// #1676: a database DELETE leaves a colocated Iceberg table's metadata for
// DropDatabase, which writes its drop markers before removing it (#1611),
// and still removes every data file.
func TestDatabaseDeleteLeavesColocatedIcebergMetadataToTheDropper(t *testing.T) {
	ctx := context.Background()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	for _, k := range []string{
		"mydb/cpu/2026/01/01/00/a.parquet",
		"mydb/cpu/metadata/v1.metadata.json",
		"mydb/cpu/metadata/version-hint.text",
	} {
		if err := backend.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	d := &colocatedDropper{backend: backend}
	h := NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
	h.SetIcebergDropper(d)
	app := fiber.New()
	h.RegisterRoutes(app)

	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/mydb?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("DELETE: status %d", resp.StatusCode)
	}
	if !d.dropped {
		t.Fatal("the Iceberg dropper was not called")
	}
	want := map[string]bool{"mydb/cpu/metadata/v1.metadata.json": true, "mydb/cpu/metadata/version-hint.text": true}
	if len(d.sawAtDrop) != len(want) {
		t.Fatalf("storage at DropDatabase = %v, want only the metadata (data deleted, metadata left)", d.sawAtDrop)
	}
	for _, k := range d.sawAtDrop {
		if !want[k] {
			t.Fatalf("storage at DropDatabase holds %q, want only the colocated metadata", k)
		}
	}
	if keys, _ := backend.List(ctx, "mydb/"); len(keys) != 0 {
		t.Fatalf("left after the DELETE: %v", keys)
	}
}
