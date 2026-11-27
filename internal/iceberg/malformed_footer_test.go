package iceberg

import (
	"context"
	"database/sql"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// panicOnBadFooters makes the footer reader panic, as the Parquet library
// does on some malformed footers, for local files named bad*.parquet and for
// every remote tail read.
func panicOnBadFooters(t *testing.T) {
	t.Helper()
	prev := newParquetReader
	newParquetReader = func(r parquet.ReaderAtSeeker, opts ...file.ReadOption) (*file.Reader, error) {
		if f, ok := r.(*os.File); !ok || strings.HasPrefix(filepath.Base(f.Name()), "bad") {
			panic("makeslice: cap out of range")
		}
		return prev(r, opts...)
	}
	t.Cleanup(func() { newParquetReader = prev })
}

func writeMalformedParquet(t *testing.T, path string) {
	t.Helper()
	writeArcStyleParquet(t, path, 1_700_000_000_000_000, 5)
}

// Every footer read returns the panic as the file's error: local, remote
// (the tail read) and the parallel union over several files, whose reads run
// on worker goroutines.
func TestMalformedFooterIsAnError(t *testing.T) {
	panicOnBadFooters(t)
	root := t.TempDir()
	bad := filepath.Join(root, "db/m/2023/11/14/22/bad.parquet")
	good := filepath.Join(root, "db/m/2023/11/14/22/good.parquet")
	writeMalformedParquet(t, bad)
	writeArcStyleParquet(t, good, 1_700_000_000_000_000, 10)

	if _, err := SchemaFromParquet(bad); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("SchemaFromParquet: %v, want the malformed-footer error", err)
	}
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SchemaFromBackend(context.Background(), backend, "db/m/2023/11/14/22/bad.parquet", st.Size()); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatal("SchemaFromBackend accepted the malformed footer")
	}
	if _, err := UnionSchema(context.Background(), []string{good, bad}); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("UnionSchema: %v, want the malformed-footer error", err)
	}
	if _, err := SchemaFromParquet(good); err != nil {
		t.Fatalf("the well-formed file: %v", err)
	}
}

// A measurement holding a malformed file fails its pass, every pass, and the
// others are reconciled: the process survives, as it must on every restart
// while the file stays in storage.
func TestSchedulerSurvivesAMalformedFile(t *testing.T) {
	panicOnBadFooters(t)
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 20)
	writeMalformedParquet(t, filepath.Join(root, "mydb/mem/2023/11/14/22/bad.parquet"))
	writeArcStyleParquet(t, filepath.Join(root, "mydb/disk/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 20)
	writeMalformedParquet(t, filepath.Join(root, "mydb/disk/2023/11/14/22/bad.parquet"))

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Logger: zerolog.Nop()})
	for pass := 0; pass < 2; pass++ {
		sched.runPass(ctx)
	}
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	if lt.CurrentSnapshot() == nil {
		t.Fatal("the well-formed measurement was not reconciled")
	}
}

// panickingSource panics while listing one measurement's files.
type panickingSource struct {
	*StorageWalkSource
	measurement string
}

func (p panickingSource) FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error) {
	if m.Measurement == p.measurement {
		panic("listing bug")
	}
	return p.StorageWalkSource.FilesAndLocal(ctx, m)
}

// A panic anywhere on the reconcile goroutine fails that measurement only.
func TestSchedulerRecoversAPanicInOneMeasurement(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	writeArcStyleParquet(t, filepath.Join(root, "mydb/boom/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 20)
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 20)
	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := panickingSource{StorageWalkSource: NewStorageWalkSource(backend, "arc", zerolog.Nop()), measurement: "boom"}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})
	sched.runPass(ctx)
	if _, err := sched.reconcileOneRecovered(ctx, Measurement{Database: "mydb", Measurement: "boom"}, "k"); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("reconcileOneRecovered: %v, want the panic as an error", err)
	}
	lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
	if err != nil {
		t.Fatal(err)
	}
	if lt.CurrentSnapshot() == nil {
		t.Fatal("the measurement after the panicking one was not reconciled")
	}
}

// A footer over maxFooterBytes is refused before the library sees it, locally
// and remotely.
func TestOversizedFooterIsRefused(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "db/m/2023/11/14/22/a.parquet")
	writeArcStyleParquet(t, path, 1_700_000_000_000_000, 5)
	prev := maxFooterBytes
	maxFooterBytes = 16
	t.Cleanup(func() { maxFooterBytes = prev })
	if _, err := SchemaFromParquet(path); err == nil || !strings.Contains(err.Error(), "over the") {
		t.Fatalf("SchemaFromParquet: %v, want the footer limit", err)
	}
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(path)
	if _, err := SchemaFromBackend(context.Background(), backend, "db/m/2023/11/14/22/a.parquet", st.Size()); err == nil || !strings.Contains(err.Error(), "over the") {
		t.Fatalf("SchemaFromBackend: %v, want the footer limit", err)
	}
	maxFooterBytes = prev
	if _, err := SchemaFromParquet(path); err != nil {
		t.Fatalf("at the default limit: %v", err)
	}
}
