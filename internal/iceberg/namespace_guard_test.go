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

// A dotted Iceberg namespace component is addressed by iceberg-go v0.7.0's SQL catalog as a
// JSON-encoded key rather than the plain dotted string, so a table Arc published under v0.6.0
// stops being found, a duplicate is created, the warehouse directory stops matching
// isWarehouseDir, and no version-hint.text is published. Arc refuses such a database instead.
//
// Reachable today only through an edge-sync spoke ID, which may contain a single dot
// (validateSpokeID rejects "/", "\\", ":" and "..", but not "."), or through a dotted
// iceberg.namespace_prefix — which config load now refuses outright.

func TestNamespaceGuard_RefusesDottedDatabase(t *testing.T) {
	for _, tc := range []struct {
		prefix, database string
		wantErr          bool
	}{
		{"arc", "mydb", false},
		{"arc", "rocket-01", false},
		{"arc", "rocket_01", false},
		// An edge-sync spoke ID may carry one dot.
		{"arc", "rocket.01", true},
		{"arc", "site.a.b", true},
		// sanitizeNamespaceDB turns a separator into a dot, so a slash is refused too — even
		// though validateSpokeID rejects slashes, the exporter must not depend on that.
		{"arc", "rocket/telemetry", true},
		// A dotted prefix poisons every database; config load refuses it, this is the backstop.
		{"my.wh", "mydb", true},
	} {
		err := checkNamespaceAddressable(tc.prefix, tc.database)
		if (err != nil) != tc.wantErr {
			t.Errorf("checkNamespaceAddressable(%q, %q) error = %v, want error = %v",
				tc.prefix, tc.database, err, tc.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "contains a dot") {
			t.Errorf("error for (%q, %q) does not explain the cause: %v", tc.prefix, tc.database, err)
		}
	}
}

// The guard must sit in front of table creation, not merely exist: a reconcile for a dotted
// database must fail rather than create an unreadable table and orphan the existing one.
func TestNamespaceGuard_ReconcileRefusesInsteadOfCreating(t *testing.T) {
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

	dataDir := filepath.Join(root, "rocket.01", "cpu", "2026", "07", "14", "15")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dataDir, "a.parquet")
	writeArcStyleParquet(t, f, 1_752_500_000_000_000, 2)
	sc, err := UnionSchema(ctx, []string{f})
	if err != nil {
		t.Fatal(err)
	}

	err = exp.ReconcileMeasurement(ctx, "rocket.01", "cpu", sc, []FileRef{refOf(t, f)})
	if err == nil {
		t.Fatal("reconcile accepted a dotted database: it would publish an unreadable table")
	}
	if !strings.Contains(err.Error(), "contains a dot") {
		t.Errorf("error does not name the cause: %v", err)
	}

	// Nothing was written: no catalog row and no warehouse directory for it.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM iceberg_tables`).Scan(&n); err == nil && n != 0 {
		t.Errorf("catalog holds %d table rows, want 0", n)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		// The warehouse-directory shape is nsPrefix + "_" … ".db" (see isWarehouseDir); the
		// encoded form is what v0.7.0 would produce for a dotted namespace. The SQLite catalog
		// file is also named *.db, so match the directory shape, not the suffix alone.
		if !e.IsDir() {
			continue
		}
		if strings.Contains(e.Name(), "__iceberg_namespace_v1__") ||
			(strings.HasPrefix(e.Name(), "arc_") && strings.HasSuffix(e.Name(), ".db")) {
			t.Errorf("a warehouse directory was created for a refused database: %s", e.Name())
		}
	}

	// A dot-free database on the same exporter still exports.
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
