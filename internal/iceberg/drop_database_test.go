package iceberg

// #639 item 3: DropDatabase removes catalog tables, the namespace, and the
// warehouse metadata directory for a dropped Arc database, idempotently.

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestDropDatabase_RemovesCatalogAndWarehouse(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), base, 50)
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})
	sched.runPass(ctx)

	nsDir := filepath.Join(root, "arc_mydb.db")
	if _, err := os.Stat(nsDir); err != nil {
		t.Fatalf("reconcile did not create the warehouse namespace dir: %v", err)
	}
	var tables int
	if err := db.QueryRow("SELECT count(*) FROM iceberg_tables").Scan(&tables); err != nil || tables != 1 {
		t.Fatalf("catalog tables = (%d, %v), want 1 after reconcile", tables, err)
	}

	if err := exp.DropDatabase(ctx, "mydb"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	if err := db.QueryRow("SELECT count(*) FROM iceberg_tables").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("catalog tables = (%d, %v), want 0 after drop", tables, err)
	}
	var namespaces int
	if err := db.QueryRow("SELECT count(*) FROM iceberg_namespace_properties").Scan(&namespaces); err == nil && namespaces != 0 {
		t.Fatalf("namespace properties = %d, want 0 after drop", namespaces)
	}
	entries, _ := os.ReadDir(nsDir)
	if len(entries) != 0 {
		if _, statErr := os.Stat(nsDir); statErr == nil {
			t.Fatalf("warehouse namespace dir still holds %d entries after drop", len(entries))
		}
	}

	// Idempotence: dropping again, and dropping a never-exported database.
	if err := exp.DropDatabase(ctx, "mydb"); err != nil {
		t.Fatalf("second DropDatabase: %v", err)
	}
	if err := exp.DropDatabase(ctx, "neverdb"); err != nil {
		t.Fatalf("never-exported DropDatabase: %v", err)
	}

	// Spoke pseudo-databases are refused.
	if err := exp.DropDatabase(ctx, "rocket-01/telemetry"); err == nil {
		t.Fatal("namespaced database name unexpectedly accepted")
	}
}
