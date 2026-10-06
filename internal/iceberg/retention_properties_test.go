package iceberg

import (
	"context"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// TestEnsureTableReconcilesRetentionProperties is the #1093 contract: a change to
// iceberg.retain_snapshots must reach an EXISTING table's metadata-file retention
// properties, which are otherwise only written on the CreateTable path — and the
// reconcile must stay a no-op once they match, so a steady-state pass does not mint a
// metadata version per tick.
func TestEnsureTableReconcilesRetentionProperties(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	initial, err := newTestExporter(t, dir, 10).EnsureTable(ctx, "db", "measurement", ArcSchema{})
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

	exp := newTestExporter(t, dir, 1)
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
