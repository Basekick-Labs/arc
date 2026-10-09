package iceberg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// Dotted namespace components use iceberg-go v0.7.0's encoded catalog key. Edge-sync databases
// therefore keep the spoke/database boundary as two components, and legacy rows are migrated
// before the scheduler can create a duplicate table.

func TestNamespaceIdentifierPreservesSpokeComponents(t *testing.T) {
	for _, tc := range []struct {
		prefix, database string
		wantErr          bool
	}{
		{"arc", "mydb", false},
		{"arc", "rocket-01", false},
		{"arc", "rocket_01", false},
		// Dotted spoke IDs are addressable when they remain a separate namespace component.
		{"arc", "rocket.01/telemetry", false},
		{"arc", "rocket/telemetry", false},
		// A restored or externally populated dotted database remains addressable through the
		// encoded namespace form even though Arc's database-create API does not create these names.
		{"arc", "rocket.01", false},
		{"arc", "site.a.b", false},
		{"arc", "rocket/telemetry.archive", true},
		{"arc", "/telemetry", true},
		{"arc", "rocket/", true},
		{"arc", "rocket/db/extra", true},
		// A dotted prefix poisons every database; config load refuses it, this is the backstop.
		{"my.wh", "mydb", true},
	} {
		err := checkNamespaceAddressable(tc.prefix, tc.database)
		if (err != nil) != tc.wantErr {
			t.Errorf("checkNamespaceAddressable(%q, %q) error = %v, want error = %v",
				tc.prefix, tc.database, err, tc.wantErr)
		}
		if err != nil && strings.TrimSpace(err.Error()) == "" {
			t.Errorf("error for (%q, %q) is empty", tc.prefix, tc.database)
		}
	}

	ns, err := namespaceIdentifier("arc", "rocket.01/telemetry")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(ns, ","), "arc_rocket.01,telemetry"; got != want {
		t.Fatalf("namespaceIdentifier() = %q, want %q", got, want)
	}
}

// A dotted spoke is exported as a two-component Iceberg namespace, and its encoded warehouse
// directory is excluded from the Arc data walk rather than discovered as another database.
func TestDottedSpokeReconcileUsesAddressableNamespace(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(root, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	database := "rocket.01/telemetry"
	dataDir := filepath.Join(root, "rocket.01", "telemetry", "cpu", "2026", "07", "14", "15")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dataDir, "a.parquet")
	writeArcStyleParquet(t, f, 1_752_500_000_000_000, 2)
	sc, err := UnionSchema(ctx, []string{f})
	if err != nil {
		t.Fatal(err)
	}

	if err := exp.ReconcileMeasurement(ctx, database, "cpu", sc, []FileRef{refOf(t, f)}); err != nil {
		t.Fatalf("reconcile dotted spoke namespace: %v", err)
	}
	tbl, err := exp.EnsureTable(ctx, database, "cpu", sc)
	if err != nil {
		t.Fatalf("load dotted spoke table: %v", err)
	}
	if got, want := strings.Join(tbl.Identifier(), ","), "arc_rocket.01,telemetry,cpu"; got != want {
		t.Fatalf("table identifier = %q, want %q", got, want)
	}
	if tbl.CurrentSnapshot() == nil {
		t.Fatal("dotted spoke table has no current snapshot")
	}

	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	encodedWarehouse := `__iceberg_namespace_v1__:["arc_rocket.01","telemetry"].db`
	if !src.isWarehouseDir(encodedWarehouse) {
		t.Errorf("encoded warehouse directory %q was not recognized", encodedWarehouse)
	}

	// A local dot-free database on the same exporter still exports.
	okDir := filepath.Join(root, "mydb", "cpu", "2026", "07", "14", "15")
	if err := os.MkdirAll(okDir, 0o755); err != nil {
		t.Fatal(err)
	}
	g := filepath.Join(okDir, "a.parquet")
	writeArcStyleParquet(t, g, 1_752_500_000_000_000, 2)
	sc2, err := UnionSchema(ctx, []string{g})
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.ReconcileMeasurement(ctx, "mydb", "cpu", sc2, []FileRef{refOf(t, g)}); err != nil {
		t.Fatalf("a dot-free database must still export: %v", err)
	}
}
