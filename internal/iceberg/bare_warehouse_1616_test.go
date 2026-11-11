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

// #1616: iceberg.warehouse set to a bare path (no file://). iceberg-go writes
// metadata locations in the warehouse's own spelling, so a bare warehouse
// gives bare locations, which the exporter compared against the file://
// storage root and never matched: no version hint, no permission hardening,
// and a false "not under the storage root" warning.

func TestNormalizeLocalWarehouse(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	abs := filepath.Join(t.TempDir(), "wh")
	for _, tc := range []struct{ in, want string }{
		{abs, localFileURI(abs)},
		{"rel/wh", localFileURI(filepath.Join(cwd, "rel", "wh"))},
		// iceberg-go's url.JoinPath would clean the leading ../ away.
		{"../wh", localFileURI(filepath.Join(filepath.Dir(cwd), "wh"))},
		{"/data/my wh", "file:///data/my%20wh"},
		{"file:///data/wh", "file:///data/wh"},
		{"s3://bucket/prefix", "s3://bucket/prefix"},
		{"", ""},
	} {
		if got := NormalizeLocalWarehouse(tc.in); got != tc.want {
			t.Errorf("NormalizeLocalWarehouse(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestResolveFileURI_BarePathsAndSchemeSpellings(t *testing.T) {
	dir := t.TempDir()
	want := resolveFileURI(localFileURI(dir) + "/arc_db.db/cpu/metadata/00001-x.metadata.json")
	for _, in := range []string{
		dir + "/arc_db.db/cpu/metadata/00001-x.metadata.json",
		"FILE://" + filepath.ToSlash(dir) + "/arc_db.db/cpu/metadata/00001-x.metadata.json",
		"file:" + filepath.ToSlash(dir) + "/arc_db.db/cpu/metadata/00001-x.metadata.json",
	} {
		if got := resolveFileURI(in); got != want {
			t.Errorf("resolveFileURI(%q) = %q, want %q", in, got, want)
		}
	}
	// A bare path is literal: iceberg-go writes "my%20wh" to a directory of
	// that name, so it must not be decoded to "my wh".
	literal := filepath.Join(dir, "my%20wh", "x.json")
	if got := decodedURIPath(resolveFileURI(literal)); !strings.HasSuffix(got, "/my%20wh/x.json") {
		t.Errorf("a bare %q resolved to %q: percent-decoded", literal, got)
	}
	// Unchanged: other schemes, and an opaque file:wh (url.Parse leaves its
	// path empty, which would resolve to the working directory).
	for _, in := range []string{"s3://bucket/a/b", "file:wh/metadata/00001-x.metadata.json", "file:"} {
		if got := resolveFileURI(in); got != in {
			t.Errorf("resolveFileURI(%q) = %q, want it unchanged", in, got)
		}
	}
}

// bareWarehouseExporter is a deployment from before the fix: the exporter
// given the bare warehouse as configured, so its rows hold bare locations.
func bareWarehouseExporter(t *testing.T, warehouse func(root string) string) (string, *Exporter) {
	t.Helper()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 20)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, warehouse(root), "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Logger: zerolog.Nop()})
	sched.runPass(context.Background())
	return root, exp
}

func TestBareWarehouse_PublishesHardensAndDrops(t *testing.T) {
	for _, tc := range []struct {
		name string
		wh   func(root string) string
	}{
		{"bare path as configured (rows hold bare locations)", func(root string) string { return filepath.Join(root, "wh") }},
		{"bare path normalised at startup", func(root string) string { return NormalizeLocalWarehouse(filepath.Join(root, "wh")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, exp := bareWarehouseExporter(t, tc.wh)
			metaDir := filepath.Join(root, "wh", "arc_mydb.db", "cpu", "metadata")
			if _, err := os.Stat(filepath.Join(metaDir, "version-hint.text")); err != nil {
				t.Fatalf("no version hint in a bare warehouse inside the root: %v", err)
			}
			entries, err := os.ReadDir(metaDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				info, err := e.Info()
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm()&0o077 != 0 {
					t.Errorf("%s is %v: metadata permission hardening skipped", e.Name(), info.Mode().Perm())
				}
			}
			if err := exp.DropDatabase(context.Background(), "mydb"); err != nil {
				t.Fatalf("DropDatabase: %v", err)
			}
			if _, err := os.Stat(filepath.Join(metaDir, "version-hint.text")); !os.IsNotExist(err) {
				t.Fatalf("DropDatabase left the bare warehouse's metadata: %v", err)
			}
		})
	}
}

// A bare warehouse OUTSIDE the root (a sibling whose name starts with the
// root's) still publishes nothing: the backend cannot address it.
func TestBareWarehouse_SiblingOfTheRootPublishesNothing(t *testing.T) {
	var sibling string
	root, _ := bareWarehouseExporter(t, func(root string) string {
		sibling = root + "-wh"
		t.Cleanup(func() { os.RemoveAll(sibling) })
		return sibling
	})
	if _, err := os.Stat(filepath.Join(sibling, "arc_mydb.db", "cpu", "metadata", "version-hint.text")); err == nil {
		t.Fatal("version hint written outside the storage root")
	}
	if _, err := os.Stat(filepath.Join(root, "arc_mydb.db")); err == nil {
		t.Fatal("metadata leaked into the storage root")
	}
}

// Hardening chmods the metadata directory and two ancestors, so it must only
// touch locations inside the configured warehouse, whatever they resolve to.
func TestHardenLocalMetadataPerms_OnlyInsideTheWarehouse(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(filepath.Join(root, "data"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, NormalizeLocalWarehouse(filepath.Join(root, "data", "wh")), "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(root, "otherapp", "ns.db", "t", "metadata")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(other, "00001-x.metadata.json")
	if err := os.WriteFile(f, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	exp.hardenLocalMetadataPerms(f)
	if info, err := os.Stat(f); err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("a file outside the warehouse was chmodded: %v %v", info.Mode().Perm(), err)
	}
	if info, err := os.Stat(filepath.Join(root, "otherapp")); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("a directory outside the warehouse was chmodded: %v %v", info.Mode().Perm(), err)
	}
}
