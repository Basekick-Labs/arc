package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

const (
	day1754  = int64(1_700_000_000_000_000) // 2023-11-14 22:13:20 UTC, in microseconds
	usPerDay = int64(24 * 60 * 60 * 1_000_000)
)

// writeNoStatsParquet writes an Arc-shaped file with no statistics at all, as the ingest
// writer did with ingest.write_statistics = false before #1754. times are in microseconds; a
// nil entry of valid is a null time.
func writeNoStatsParquet(t *testing.T, path string, times []int64, valid []bool) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i, ts := range times {
		if valid != nil && !valid[i] {
			b.Field(0).(*array.TimestampBuilder).AppendNull()
		} else {
			b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts))
		}
		b.Field(1).(*array.StringBuilder).Append("h")
		b.Field(2).(*array.Float64Builder).Append(float64(i))
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
	w, err := pqarrow.NewFileWriter(schema, f,
		parquet.NewWriterProperties(parquet.WithCompression(0), parquet.WithStats(false)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
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

func exporter1754(t *testing.T, dir string) *Exporter {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, nil, "file://"+dir+"/wh", "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return exp
}

// liveDays is the day each of the table's entries is registered under; nil: none.
func liveDays(t *testing.T, exp *Exporter, sc ArcSchema) map[string]any {
	t.Helper()
	ctx := context.Background()
	tbl, err := exp.EnsureTable(ctx, "db", "cpu", sc)
	if err != nil {
		t.Fatal(err)
	}
	live, err := exp.tableLiveFiles(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	for p, df := range live {
		out[p] = df.Partition()[1000]
	}
	return out
}

// A file whose footer has no min/max for the time column used to be registered under a null
// day, which readers prune for any time filter. Its day now comes from its rows, and it is the
// day a file with statistics holding the same times gets.
func TestFileWithoutTimeStatisticsIsRegisteredUnderItsDay(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	withStats := filepath.Join(dir, "stats.parquet")
	noStats := filepath.Join(dir, "nostats.parquet")
	nextDay := filepath.Join(dir, "nostats-next-day.parquet")
	spans := filepath.Join(dir, "nostats-spans.parquet")
	nulls := filepath.Join(dir, "nostats-nulls.parquet")
	withNull := filepath.Join(dir, "nostats-one-null.parquet")
	writeArcStyleParquet(t, withStats, day1754, 10)
	writeNoStatsParquet(t, noStats, []int64{day1754 + 5, day1754, day1754 + 9}, nil)
	writeNoStatsParquet(t, nextDay, []int64{day1754 + usPerDay}, nil)
	writeNoStatsParquet(t, spans, []int64{day1754, day1754 + usPerDay}, nil)
	writeNoStatsParquet(t, nulls, []int64{0, 0}, []bool{false, false})
	writeNoStatsParquet(t, withNull, []int64{0, day1754 + 3}, []bool{false, true})

	exp := exporter1754(t, dir)
	sc, err := SchemaFromParquet(withStats)
	if err != nil {
		t.Fatal(err)
	}
	var current []FileRef
	for _, p := range []string{withStats, noStats, nextDay, spans, nulls, withNull} {
		current = append(current, refOf(t, p))
	}
	res, err := exp.reconcile(ctx, reconcileRequest{database: "db", measurement: "cpu", schema: sc, current: current})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.leftOut != 2 {
		t.Errorf("leftOut = %d, want 2 (the file spanning two days and the one with only null times)", res.leftOut)
	}
	if !res.settled {
		t.Error("a pass that left files out did not settle: it would be repeated on the short interval for good")
	}
	days := liveDays(t, exp, sc)
	want := days[fileURI(withStats)]
	if want == nil {
		t.Fatalf("the file with statistics has no day: %v", days)
	}
	for _, p := range []string{noStats, withNull} {
		got, ok := days[fileURI(p)]
		if !ok {
			t.Errorf("%s is not in the table", filepath.Base(p))
		} else if got != want {
			t.Errorf("%s is under day %v (%T), want %v (%T), the day of the file with statistics", filepath.Base(p), got, got, want, want)
		}
	}
	if got := days[fileURI(nextDay)]; got == nil || got == want {
		t.Errorf("the next day's file is under day %v, want the day after %v", got, want)
	}
	for _, p := range []string{spans, nulls} {
		if _, ok := days[fileURI(p)]; ok {
			t.Errorf("%s is in the table; it has no single day", filepath.Base(p))
		}
	}

	// The next pass has nothing to commit, finds the same two files and leaves them out again.
	res, err = exp.reconcile(ctx, reconcileRequest{database: "db", measurement: "cpu", schema: sc, current: current})
	if err != nil || res.commits != 0 || res.leftOut != 2 || !res.settled {
		t.Errorf("second pass: commits=%d leftOut=%d settled=%v err=%v, want 0, 2, true, nil", res.commits, res.leftOut, res.settled, err)
	}
}

// registerAsBefore1754 puts files in the table as an earlier version did: the DataFile
// iceberg-go builds from the footer, with whatever day it has or lacks.
func registerAsBefore1754(t *testing.T, exp *Exporter, sc ArcSchema, paths []string) {
	t.Helper()
	ctx := context.Background()
	tbl, err := exp.EnsureTable(ctx, "db", "cpu", sc)
	if err != nil {
		t.Fatal(err)
	}
	var adds []iceberg.DataFile
	for _, p := range paths {
		fio, err := exp.fileIOFor(ctx, tbl, fileURI(p))
		if err != nil {
			t.Fatal(err)
		}
		df, err := icetable.FileToDataFile(ctx, fio, fileURI(p), tbl.Schema(), tbl.Spec(), 0, tbl.Properties())
		if err != nil {
			t.Fatal(err)
		}
		adds = append(adds, df)
	}
	if _, err := exp.commitDiff(ctx, tbl, nil, adds, "db", "cpu", false); err != nil {
		t.Fatal(err)
	}
}

// An entry registered under a null day by an earlier version is re-registered under its day by
// the next pass, a bounded number per pass.
func TestEntryWithoutDayIsReRegistered(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	withStats := filepath.Join(dir, "stats.parquet")
	writeArcStyleParquet(t, withStats, day1754, 10)
	paths := []string{withStats}
	const stale = maxRedatedPerPass + 3
	for i := 0; i < stale; i++ {
		p := filepath.Join(dir, fmt.Sprintf("nostats-%03d.parquet", i))
		writeNoStatsParquet(t, p, []int64{day1754 + int64(i)}, nil)
		paths = append(paths, p)
	}
	exp := exporter1754(t, dir)
	sc, err := SchemaFromParquet(withStats)
	if err != nil {
		t.Fatal(err)
	}
	registerAsBefore1754(t, exp, sc, paths)
	without := func() int {
		n := 0
		for _, d := range liveDays(t, exp, sc) {
			if d == nil {
				n++
			}
		}
		return n
	}
	if got := without(); got != stale {
		t.Fatalf("the earlier version's table has %d entries without a day, want %d: the test no longer reproduces it", got, stale)
	}
	var current []FileRef
	for _, p := range paths {
		current = append(current, refOf(t, p))
	}

	res, err := exp.reconcile(ctx, reconcileRequest{database: "db", measurement: "cpu", schema: sc, current: current})
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if got := without(); got != 3 {
		t.Errorf("after one pass %d entries have no day, want 3 (a pass takes %d)", got, maxRedatedPerPass)
	}
	if !res.partial || res.settled || res.remaining != 3 {
		t.Errorf("first pass: partial=%v settled=%v remaining=%d, want true, false, 3: the scheduler would not come back for the rest", res.partial, res.settled, res.remaining)
	}

	res, err = exp.reconcile(ctx, reconcileRequest{database: "db", measurement: "cpu", schema: sc, current: current})
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	days := liveDays(t, exp, sc)
	if len(days) != len(paths) {
		t.Errorf("the table holds %d files, want %d: a repair dropped or doubled one", len(days), len(paths))
	}
	for p, d := range days {
		if d != days[fileURI(withStats)] {
			t.Errorf("%s is under day %v, want %v", filepath.Base(p), d, days[fileURI(withStats)])
		}
	}
	if res.partial || !res.settled || res.leftOut != 0 {
		t.Errorf("second pass: partial=%v settled=%v leftOut=%d, want false, true, 0", res.partial, res.settled, res.leftOut)
	}

	res, err = exp.reconcile(ctx, reconcileRequest{database: "db", measurement: "cpu", schema: sc, current: current})
	if err != nil || res.commits != 0 {
		t.Errorf("third pass: commits=%d err=%v, want a converged table", res.commits, err)
	}
}

// An entry without a day whose file has no single day leaves the table.
func TestEntryWithoutDayThatSpansDaysLeavesTheTable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	withStats := filepath.Join(dir, "stats.parquet")
	spans := filepath.Join(dir, "nostats-spans.parquet")
	writeArcStyleParquet(t, withStats, day1754, 10)
	writeNoStatsParquet(t, spans, []int64{day1754, day1754 + usPerDay}, nil)
	exp := exporter1754(t, dir)
	sc, err := SchemaFromParquet(withStats)
	if err != nil {
		t.Fatal(err)
	}
	registerAsBefore1754(t, exp, sc, []string{withStats, spans})
	res, err := exp.reconcile(ctx, reconcileRequest{database: "db", measurement: "cpu", schema: sc,
		current: []FileRef{refOf(t, withStats), refOf(t, spans)}})
	if err != nil {
		t.Fatal(err)
	}
	days := liveDays(t, exp, sc)
	if _, ok := days[fileURI(spans)]; ok || len(days) != 1 {
		t.Errorf("table = %v, want only the file with a day", days)
	}
	if res.leftOut != 1 || !res.settled {
		t.Errorf("leftOut=%d settled=%v, want 1, true", res.leftOut, res.settled)
	}
}

// columnMicrosRange: units other than microseconds, and times before 1970.
func TestColumnMicrosRange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		unit   arrow.TimeUnit
		values []int64
		lo, hi int64
	}{
		{"microseconds", arrow.Microsecond, []int64{7, -3, 5}, -3, 7},
		{"milliseconds", arrow.Millisecond, []int64{2, -1}, -1000, 2000},
		{"nanoseconds before 1970 round down", arrow.Nanosecond, []int64{-1, 1999}, -1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "f.parquet")
			schema := arrow.NewSchema([]arrow.Field{{Name: "time", Type: &arrow.TimestampType{Unit: tc.unit, TimeZone: "UTC"}, Nullable: true}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			defer b.Release()
			for _, v := range tc.values {
				b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(v))
			}
			rec := b.NewRecord()
			defer rec.Release()
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithStats(false)), pqarrow.DefaultWriterProps())
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Write(rec); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			fio := iceio.LocalFS{}
			lo, hi, found, err := columnMicrosRange(context.Background(), fio, path, "time")
			if err != nil || !found || lo != tc.lo || hi != tc.hi {
				t.Errorf("range = [%d, %d] found=%v err=%v, want [%d, %d]", lo, hi, found, err, tc.lo, tc.hi)
			}
			if _, _, found, err := columnMicrosRange(context.Background(), fio, path, "absent"); found || err != nil {
				t.Errorf("an absent column: found=%v err=%v, want false, nil", found, err)
			}
		})
	}
}

// The gauge is the files left out of the tables whose last pass settled, it survives a pass
// the scheduler skips because nothing changed, and it goes with the table.
func TestFilesLeftOutGauge(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	hour := filepath.Join(root, "mydb/cpu/2023/11/14/22")
	writeArcStyleParquet(t, filepath.Join(hour, "a.parquet"), day1754, 20)
	writeStraddlingParquet(t, filepath.Join(hour, "spans.parquet"), day1754)
	writeNoStatsParquet(t, filepath.Join(hour, "nulls.parquet"), []int64{0}, []bool{false})
	writeNoStatsParquet(t, filepath.Join(hour, "nostats.parquet"), []int64{day1754}, nil)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, filepath.Join(root, "wh"), "arc", 3, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	open := true
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()),
		Gate: gate1754(func() bool { return open }), Logger: zerolog.Nop()})
	gauge := func() int64 { return metrics.Get().Snapshot()["iceberg_files_left_out"].(int64) }
	metrics.Get().SetIcebergFilesLeftOut(0)
	t.Cleanup(func() { metrics.Get().SetIcebergFilesLeftOut(0) })

	sched.runPass(context.Background())
	if got := gauge(); got != 2 {
		t.Fatalf("after the first pass the gauge is %d, want 2 (spans.parquet and nulls.parquet)", got)
	}
	sched.runPass(context.Background()) // nothing changed: the pass is skipped
	if got := gauge(); got != 2 {
		t.Errorf("after a pass skipped as unchanged the gauge is %d, want 2", got)
	}
	open = false
	sched.runPass(context.Background())
	if got := gauge(); got != 0 {
		t.Errorf("on a node that does not export the gauge is %d, want 0", got)
	}
	open = true
	if err := os.Remove(filepath.Join(hour, "spans.parquet")); err != nil {
		t.Fatal(err)
	}
	sched.runPass(context.Background())
	if got := gauge(); got != 1 {
		t.Errorf("after one of the two files is removed the gauge is %d, want 1", got)
	}
	if err := os.RemoveAll(filepath.Join(root, "mydb")); err != nil {
		t.Fatal(err)
	}
	sched.runPass(context.Background())
	if got := gauge(); got != 0 {
		t.Errorf("after the measurement is gone the gauge is %d, want 0", got)
	}
}

type gate1754 func() bool

func (g gate1754) CanRun() bool { return g() }
