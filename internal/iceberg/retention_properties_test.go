package iceberg

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"
)

func TestEnsureTableReconcilesRetentionProperties(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	newExporter := func(retain int) *Exporter {
		exp, err := NewExporter(db, nil, "file://"+dir+"/warehouse", "arc", retain, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewExporter: %v", err)
		}
		return exp
	}

	initial, err := newExporter(10).EnsureTable(ctx, "db", "measurement", ArcSchema{})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	txn := initial.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{
		table.MetadataDeleteAfterCommitEnabledKey: "false",
		table.MetadataPreviousVersionsMaxKey:      "10",
	}); err != nil {
		t.Fatalf("set stale retention properties: %v", err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit stale retention properties: %v", err)
	}

	exp := newExporter(1)
	updated, err := exp.EnsureTable(ctx, "db", "measurement", ArcSchema{})
	if err != nil {
		t.Fatalf("EnsureTable with changed retention: %v", err)
	}
	if got := updated.Properties()[table.MetadataDeleteAfterCommitEnabledKey]; got != "true" {
		t.Errorf("delete-after-commit property = %q, want true", got)
	}
	if got := updated.Properties()[table.MetadataPreviousVersionsMaxKey]; got != "1" {
		t.Errorf("previous-versions-max = %q, want 1", got)
	}

	location := updated.MetadataLocation()
	unchanged, err := exp.EnsureTable(ctx, "db", "measurement", ArcSchema{})
	if err != nil {
		t.Fatalf("steady-state EnsureTable: %v", err)
	}
	if got := unchanged.MetadataLocation(); got != location {
		t.Errorf("steady-state EnsureTable wrote metadata: location changed from %q to %q", location, got)
	}
}
