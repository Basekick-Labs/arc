package iceberg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// layoutFor builds the layout an exporter on a local root reports for a configured warehouse,
// through NewExporter, as main.go does (after NormalizeLocalWarehouse when normalize is set).
func layoutFor(t *testing.T, root, warehouse, nsPrefix string, normalize bool) *WarehouseLayout {
	t.Helper()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if normalize {
		warehouse = NormalizeLocalWarehouse(warehouse)
	}
	exp, err := NewExporter(db, backend, warehouse, nsPrefix, 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return exp.WarehouseLayout()
}

type layoutWant struct {
	root            string // RootName()
	nestedDB, nestM string
	hidden          []string // IsWarehouseRootDir true
	shown           []string // IsWarehouseRootDir false
}

func checkLayout(t *testing.T, l *WarehouseLayout, w layoutWant) {
	t.Helper()
	if got := l.RootName(); got != w.root {
		t.Errorf("RootName() = %q, want %q", got, w.root)
	}
	if db, m := l.Nested(); db != w.nestedDB || m != w.nestM {
		t.Errorf("Nested() = %q/%q, want %q/%q", db, m, w.nestedDB, w.nestM)
	}
	for _, n := range w.hidden {
		if !l.IsWarehouseRootDir(n) {
			t.Errorf("IsWarehouseRootDir(%q) = false, want true", n)
		}
	}
	for _, n := range w.shown {
		if l.IsWarehouseRootDir(n) {
			t.Errorf("IsWarehouseRootDir(%q) = true, want false", n)
		}
	}
}

// The warehouse rule at the default root and at the subdirectory values an operator sets
// (#1626, #534, #1616), resolved through a real exporter on a local backend.
func TestWarehouseLayoutLocal_Issue1626(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "data")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		root      string
		warehouse string
		prefix    string
		normalize bool
		want      layoutWant
	}{
		{"default: the root itself", root, DefaultWarehouse(mustLocal(t, root)), "arc", false,
			layoutWant{hidden: []string{"arc_db.db"}, shown: []string{"db", "data", "lv_wh", "arc_db"}}},
		{"bare subdirectory", root, filepath.Join(root, "lv_wh"), "arc", true,
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh", "lv_wh/", "arc_db.db"}, shown: []string{"lv_wh-other", "lv_w", "db"}}},
		{"bare subdirectory, not normalized (pre-#1616 rows)", root, filepath.Join(root, "lv_wh"), "arc", false,
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}, shown: []string{"lv_wh-other"}}},
		{"subdirectory with a trailing slash", root, filepath.Join(root, "lv_wh") + "/", "arc", true,
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}, shown: []string{"lv_wh-other"}}},
		{"file:// subdirectory with a trailing slash", root, localFileURI(filepath.Join(root, "lv_wh")) + "/", "arc", false,
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}, shown: []string{"lv_wh-other"}}},
		{"sibling sharing the prefix: wh-other is configured, wh is a database", root, filepath.Join(root, "wh-other"), "arc", true,
			layoutWant{root: "wh-other", hidden: []string{"wh-other"}, shown: []string{"wh"}}},
		{"nested two levels", root, filepath.Join(root, "a", "b"), "arc", true,
			layoutWant{nestedDB: "a", nestM: "b", shown: []string{"a", "b"}}},
		{"outside the root, in a sibling whose name extends the root's", root, filepath.Join(base, "data2", "wh"), "arc", true,
			layoutWant{hidden: []string{"arc_db.db"}, shown: []string{"wh", "data2"}}},
		{"symlinked root, warehouse by its real path", link, filepath.Join(root, "lv_wh"), "arc", true,
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}}},
		{"namespace_prefix=acme", root, filepath.Join(root, "lv_wh"), "acme", true,
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh", "acme_db.db"}, shown: []string{"arc_db.db"}}},
		{"dotted spoke namespace (#1129)", root, DefaultWarehouse(mustLocal(t, root)), "arc", false,
			layoutWant{hidden: []string{`__iceberg_namespace_v1__:["arc_rocket.01","telemetry"].db`}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkLayout(t, layoutFor(t, tc.root, tc.warehouse, tc.prefix, tc.normalize), tc.want)
		})
	}
}

func mustLocal(t *testing.T, root string) *storage.LocalBackend {
	t.Helper()
	b, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Object-store warehouses, from the bases DefaultWarehouse yields for an S3 backend
// ("s3://bucket/prefix", or "s3://bucket" without a prefix).
func TestWarehouseLayoutObjectStore_Issue1626(t *testing.T) {
	for _, tc := range []struct {
		name, root, warehouse string
		want                  layoutWant
	}{
		{"s3 prefix, subdirectory", "s3://bucket/prefix", "s3://bucket/prefix/lv_wh",
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}, shown: []string{"lv_wh-other"}}},
		{"s3 no prefix, subdirectory", "s3://bucket", "s3://bucket/lv_wh",
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}}},
		{"s3 default: the root itself", "s3://bucket/prefix", "s3://bucket/prefix",
			layoutWant{hidden: []string{"arc_db.db"}, shown: []string{"prefix", "db"}}},
		{"s3 another bucket whose name extends ours", "s3://bucket", "s3://bucket2/wh",
			layoutWant{shown: []string{"wh", "bucket2"}}},
		{"s3 another prefix whose name extends ours", "s3://bucket/pre", "s3://bucket/prefix2/wh",
			layoutWant{shown: []string{"wh", "fix2"}}},
		{"s3 nested", "s3://bucket/prefix", "s3://bucket/prefix/a/b/c",
			layoutWant{nestedDB: "a", nestM: "b", shown: []string{"a"}}},
		{"s3 root and warehouse with trailing slashes", "s3://bucket/prefix/", "s3://bucket/prefix/lv_wh/",
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}}},
		{"s3 root with a trailing slash, warehouse the root", "s3://bucket/prefix/", "s3://bucket/prefix",
			layoutWant{shown: []string{"prefix", "s3:", "lv_wh"}}},
		// Future-proofing: config refuses Iceberg on an Azure primary today (checkIcebergBackend).
		{"azure prefix, subdirectory", "azure://container/p", "azure://container/p/lv_wh",
			layoutWant{root: "lv_wh", hidden: []string{"lv_wh"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkLayout(t, NewWarehouseLayout(tc.root, tc.warehouse, "arc"), tc.want)
		})
	}
}

func TestWarehouseLayoutNilAndMeasurement_Issue1626(t *testing.T) {
	var nilLayout *WarehouseLayout
	if nilLayout.IsWarehouseRootDir("arc_db.db") || nilLayout.IsWarehouseMeasurement("a", "b") || nilLayout.RootName() != "" {
		t.Fatal("a nil layout (Iceberg off) must hide nothing")
	}
	l := NewWarehouseLayout("s3://bucket/p", "s3://bucket/p/a/b", "arc")
	if !l.IsWarehouseMeasurement("a", "b") {
		t.Error("IsWarehouseMeasurement(a, b) = false for a warehouse at <root>/a/b")
	}
	if rel, ok := l.ContainsWarehouse("a"); !ok || rel != "a/b" {
		t.Errorf("ContainsWarehouse(a) = %q, %v; want a/b, true", rel, ok)
	}
	for _, db := range []string{"a-other", "b", "aa", ""} {
		if _, ok := l.ContainsWarehouse(db); ok {
			t.Errorf("ContainsWarehouse(%q) = true", db)
		}
	}
	if _, ok := NewWarehouseLayout("s3://bucket/p", "s3://bucket/p/lv_wh", "arc").ContainsWarehouse("lv_wh"); ok {
		t.Error("a one-segment warehouse is not nested: ContainsWarehouse must be false (the root-dir guard covers it)")
	}
	if _, ok := nilLayout.ContainsWarehouse("a"); ok {
		t.Error("nil layout: ContainsWarehouse true")
	}
	for _, pair := range [][2]string{{"a", "c"}, {"b", "b"}, {"a", "b-other"}} {
		if l.IsWarehouseMeasurement(pair[0], pair[1]) {
			t.Errorf("IsWarehouseMeasurement(%s, %s) = true", pair[0], pair[1])
		}
	}
}

// The export walk applies the listings' rule: a subdirectory warehouse is not walked as a
// database, a nested one not as a measurement; without SetWarehouseLayout only the namespace
// directories are skipped, as before.
func TestWalkSkipsWarehouseLayout_Issue1626(t *testing.T) {
	root := t.TempDir()
	backend := mustLocal(t, root)
	ctx := context.Background()
	for _, k := range []string{
		"mydb/cpu/2026/07/14/15/a.parquet",
		"lv_wh/arc_mydb.db/cpu/metadata/v1.metadata.json",
		"a/b/arc_x.db/t/metadata/v1.metadata.json",
		"a/mem/2026/07/14/15/a.parquet",
		"arc_mydb.db/cpu/metadata/v1.metadata.json",
	} {
		if err := backend.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	names := func(src *StorageWalkSource) map[string]bool {
		ms, err := src.Measurements(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, m := range ms {
			out[m.Database+"/"+m.Measurement] = true
		}
		return out
	}

	plain := names(NewStorageWalkSource(backend, "arc", zerolog.Nop()))
	if plain["arc_mydb.db/cpu"] || !plain["lv_wh/arc_mydb.db"] {
		t.Fatalf("without a layout: %v; want namespace dirs skipped and lv_wh walked (the pre-#1626 rule)", plain)
	}

	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	src.SetWarehouseLayout(NewWarehouseLayout(localFileURI(root), localFileURI(filepath.Join(root, "lv_wh")), "arc"))
	got := names(src)
	if got["lv_wh/arc_mydb.db"] || got["arc_mydb.db/cpu"] || !got["mydb/cpu"] {
		t.Fatalf("subdirectory warehouse: walked %v", got)
	}

	nested := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	nested.SetWarehouseLayout(NewWarehouseLayout(localFileURI(root), localFileURI(filepath.Join(root, "a", "b")), "arc"))
	got = names(nested)
	if got["a/b"] || !got["a/mem"] || !got["mydb/cpu"] {
		t.Fatalf("nested warehouse: walked %v; want a/mem kept and a/b skipped", got)
	}
}

// The listings' per-directory cost with Iceberg on (#1626): in memory, no allocation.
func BenchmarkIsWarehouseRootDir(b *testing.B) {
	l := NewWarehouseLayout("s3://bucket/prefix", "s3://bucket/prefix/lv_wh", "arc")
	names := []string{"production", "lv_wh", "arc_db.db", "lv_wh-other", "telemetry"}
	b.ReportAllocs()
	n := 0
	for i := 0; i < b.N; i++ {
		if l.IsWarehouseRootDir(names[i%len(names)]) {
			n++
		}
	}
	if n == 0 && b.N >= len(names) {
		b.Fatal("nothing matched")
	}
}
