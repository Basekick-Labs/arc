package iceberg

// #1744: a column whose type differs between files (or between files and the
// table) fails the measurement's export, and the logged error is sanitized,
// which masks the quoted column name. The scheduler logs the conflict's
// fields as structured values so the operator can see which column it is.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/rs/zerolog"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
)

// writeStringValueParquet writes an Arc-style file whose "value" column is a
// string, where writeArcStyleParquet writes a double.
func writeStringValueParquet(t *testing.T, path string, baseTS int64, n int) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < n; i++ {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(baseTS + int64(i)*1_000_000))
		b.Field(1).(*array.StringBuilder).Append("host-1")
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprint(i))
	}
	rec := b.NewRecord()
	defer rec.Release()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnionSchemaReportsTheConflictingColumnAndFiles(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.parquet")
	b := filepath.Join(dir, "b.parquet")
	writeArcStyleParquet(t, a, 1_700_000_000_000_000, 3)
	writeStringValueParquet(t, b, 1_700_000_100_000_000, 3)
	_, err := UnionSchema(context.Background(), []string{a, b})
	var c *SchemaConflictError
	if !errors.As(err, &c) {
		t.Fatalf("err = %v, want a *SchemaConflictError", err)
	}
	if c.Column != "value" || c.With != conflictWithFiles || c.FirstFile != a || c.File != b || c.First == c.Then {
		t.Fatalf("conflict = %+v", c)
	}
	if !strings.Contains(err.Error(), `column "value" has conflicting types across files`) {
		t.Fatalf("error text changed: %v", err)
	}
}

func TestMergeSchemasAndEvolveReportTheConflict(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.parquet")
	b := filepath.Join(dir, "b.parquet")
	writeArcStyleParquet(t, a, 1_700_000_000_000_000, 3)
	writeStringValueParquet(t, b, 1_700_000_100_000_000, 3)
	sa, err := SchemaFromParquet(a)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := SchemaFromParquet(b)
	if err != nil {
		t.Fatal(err)
	}
	_, err = MergeSchemas(sa, sb)
	var c *SchemaConflictError
	// MergeSchemas leaves With to its caller, which knows what base came from.
	if !errors.As(err, &c) || c.Column != "value" || c.With != "" {
		t.Fatalf("MergeSchemas: %v", err)
	}

	// evolveSchema: the table has value as double, new files say string.
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 3)
	r := newColocRig(t, root, "n.db", layoutWarehouse)
	r.pass(t)
	tbl := mustLoad(t, r.exp, "mydb", "cpu")
	_, err = r.exp.evolveSchema(context.Background(), tbl, sb)
	c = nil
	if !errors.As(err, &c) || c.Column != "value" || c.With != conflictWithTable {
		t.Fatalf("evolveSchema: %v", err)
	}
	if !strings.Contains(err.Error(), `column "value" type mismatch: Iceberg table has`) {
		t.Fatalf("error text changed: %v", err)
	}
}

// Through the scheduler, with the production error sanitizer installed: the
// "measurement failed" event names the column in a field while its error
// stays sanitized.
func TestSchedulerLogsTheConflictingColumn(t *testing.T) {
	prev := zerolog.ErrorMarshalFunc
	zerolog.ErrorMarshalFunc = func(err error) interface{} { return sqlutil.SanitizeErrText(err.Error()) }
	t.Cleanup(func() { zerolog.ErrorMarshalFunc = prev })

	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 3)
	writeStringValueParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/b.parquet"), 1_700_000_100_000_000, 3)
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, DefaultWarehouse(backend), "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	s := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Logger: logger})
	s.runPass(context.Background())

	var found map[string]any
	for _, line := range strings.Split(buf.String(), "\n") {
		var ev map[string]any
		if json.Unmarshal([]byte(line), &ev) == nil && ev["message"] == "Iceberg reconcile: measurement failed" {
			found = ev
		}
	}
	if found == nil {
		t.Fatalf("no failure event; log:\n%s", buf.String())
	}
	if found["column"] != "value" || found["conflict_with"] != "files" || found["type_first"] == found["type_then"] {
		t.Fatalf("event fields = %v", found)
	}
	for _, k := range []string{"file_first", "file_then"} {
		if f, _ := found[k].(string); !strings.HasPrefix(f, "mydb/cpu/2023/11/14/") {
			t.Fatalf("%s = %v, want the file's storage key", k, found[k])
		}
	}
	if e, _ := found["error"].(string); strings.Contains(e, `"value"`) || e == "" {
		t.Fatalf("error field %q: the sanitizer must still mask it", e)
	}
}

// The common real case: the table has the column as double, and a new file
// brings it as string. The conflict is found merging the table's schema with
// the new file's (prepareChunk), reported as a table conflict naming the file.
func TestSchedulerLogsATableConflictWithTheNewFile(t *testing.T) {
	prev := zerolog.ErrorMarshalFunc
	zerolog.ErrorMarshalFunc = func(err error) interface{} { return sqlutil.SanitizeErrText(err.Error()) }
	t.Cleanup(func() { zerolog.ErrorMarshalFunc = prev })

	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 3)
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, DefaultWarehouse(backend), "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	s := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Logger: zerolog.New(&buf)})
	s.runPass(context.Background()) // creates the table with value as double
	writeStringValueParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/b.parquet"), 1_700_000_100_000_000, 3)
	buf.Reset()
	s.runPass(context.Background())

	var found map[string]any
	for _, line := range strings.Split(buf.String(), "\n") {
		var ev map[string]any
		if json.Unmarshal([]byte(line), &ev) == nil && ev["message"] == "Iceberg reconcile: measurement failed" {
			found = ev
		}
	}
	if found == nil {
		t.Fatalf("no failure event; log:\n%s", buf.String())
	}
	if found["column"] != "value" || found["conflict_with"] != "table" || found["file_then"] != "mydb/cpu/2023/11/14/23/b.parquet" {
		t.Fatalf("event fields = %v", found)
	}
	if _, ok := found["file_first"]; ok {
		t.Fatalf("file_first = %v: the table side has no file", found["file_first"])
	}
}

// A column name is bounded and quoted like every logged column name.
func TestLogSchemaConflictBoundsTheName(t *testing.T) {
	var buf bytes.Buffer
	l := zerolog.New(&buf)
	long := strings.Repeat("x", 1000) + "\xff"
	logSchemaConflict(l.Info(), fmt.Errorf("wrapped: %w", &SchemaConflictError{Column: long, First: "a", Then: "b", With: conflictWithFiles})).Msg("m")
	var ev map[string]any
	if err := json.Unmarshal(buf.Bytes(), &ev); err != nil {
		t.Fatal(err)
	}
	if c, _ := ev["column"].(string); len(c) > 300 || c == long {
		t.Fatalf("column field not bounded: %d bytes", len(c))
	}
}
