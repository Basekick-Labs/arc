// buildEngineSQL tests: real LocalBackend over a temp dir. Covers the
// router-specific path logic — local-only gate, absolute-path construction from
// backend-relative List results, glob-free array shape, and empty-measurement
// decline. Tagged (buildEngineSQL lives in the tagged router). No engine call —
// this tests SQL construction, not execution.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/arcxengine"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func newLocalDeps(t *testing.T, base string) Deps {
	t.Helper()
	be, err := storage.NewLocalBackend(base, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	return Deps{Storage: be, Logger: zerolog.Nop(), Mode: ModeShadow}
}

// touchParquet creates an empty file at base/rel so List finds it.
func touchParquet(t *testing.T, base, rel string) {
	t.Helper()
	full := filepath.Join(base, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestBuildEngineSQL_CountStar_AbsolutePaths(t *testing.T) {
	base := t.TempDir()
	// Two hourly files under db/measurement/Y/M/D/H.
	touchParquet(t, base, "prod/cpu/2026/02/03/10/a.parquet")
	touchParquet(t, base, "prod/cpu/2026/02/03/11/b.parquet")
	// A non-parquet file that must be filtered out.
	touchParquet(t, base, "prod/cpu/2026/02/03/11/ignore.txt")

	deps := newLocalDeps(t, base)
	d := Decision{
		Eligible: true,
		Shape:    ShapeCountStar,
		Ctx:      arcxengine.Context{Database: "prod", Measurement: "cpu", TimeColumn: "time"},
	}
	sql, ok := deps.buildEngineSQL(context.Background(), d)
	if !ok {
		t.Fatal("expected ok, got decline")
	}
	if !strings.HasPrefix(sql, "SELECT count(*) FROM read_parquet([") {
		t.Fatalf("unexpected sql prefix: %s", sql)
	}
	// Absolute paths (base is absolute via NewLocalBackend), both .parquet files,
	// no .txt, no glob metachar.
	for _, want := range []string{"a.parquet", "b.parquet"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("sql missing %s: %s", want, sql)
		}
	}
	if strings.Contains(sql, "ignore.txt") {
		t.Fatalf("sql should not contain non-parquet file: %s", sql)
	}
	// The PATH ARRAY must be glob-free (the engine declines glob metachars in
	// paths). Check only the array, not the whole SQL — count(*) has a legit `*`.
	arrStart := strings.Index(sql, "read_parquet([") + len("read_parquet(")
	arrEnd := strings.LastIndex(sql, "])") + 1
	pathArray := sql[arrStart:arrEnd]
	if strings.ContainsAny(pathArray, "*?{") {
		t.Fatalf("path array must be glob-free (engine declines globs): %s", pathArray)
	}
	if !strings.Contains(sql, base) {
		t.Fatalf("paths must be absolute (contain base %q): %s", base, sql)
	}
}

func TestBuildEngineSQL_Agg_Shape(t *testing.T) {
	base := t.TempDir()
	touchParquet(t, base, "prod/cpu/2026/02/03/a.parquet") // daily-compacted
	deps := newLocalDeps(t, base)
	d := Decision{
		Eligible: true,
		Shape:    ShapeDateTruncCent,
		Unit:     "day",
		Ctx:      arcxengine.Context{Database: "prod", Measurement: "cpu", TimeColumn: "time"},
	}
	sql, ok := deps.buildEngineSQL(context.Background(), d)
	if !ok {
		t.Fatal("expected ok, got decline")
	}
	if !strings.HasPrefix(sql, "SELECT date_trunc('day', time), count(*) FROM read_parquet([") {
		t.Fatalf("unexpected agg sql: %s", sql)
	}
	if !strings.HasSuffix(sql, "]) GROUP BY 1") {
		t.Fatalf("unexpected agg sql suffix: %s", sql)
	}
}

func TestBuildEngineSQL_EmptyMeasurementDeclines(t *testing.T) {
	base := t.TempDir() // no files for the measurement
	deps := newLocalDeps(t, base)
	d := Decision{
		Eligible: true,
		Shape:    ShapeCountStar,
		Ctx:      arcxengine.Context{Database: "prod", Measurement: "nope", TimeColumn: "time"},
	}
	if _, ok := deps.buildEngineSQL(context.Background(), d); ok {
		t.Fatal("expected decline for measurement with no parquet files")
	}
}

// TestBuildEngineSQL_Agg_Filtered asserts the PR-A time-range WHERE is emitted
// between read_parquet(...) and GROUP BY 1, with the whereText verbatim.
func TestBuildEngineSQL_Agg_Filtered(t *testing.T) {
	base := t.TempDir()
	touchParquet(t, base, "prod/cpu/2026/02/03/04/a.parquet") // hourly
	deps := newLocalDeps(t, base)
	where := "time >= '2026-02-03T00:00:00Z' AND time < '2026-02-04T00:00:00Z'"
	d := Decision{
		Eligible:  true,
		Shape:     ShapeDateTruncCent,
		Unit:      "hour",
		WhereText: where,
		Ctx:       arcxengine.Context{Database: "prod", Measurement: "cpu", TimeColumn: "time"},
	}
	sql, ok := deps.buildEngineSQL(context.Background(), d)
	if !ok {
		t.Fatal("expected ok, got decline")
	}
	if !strings.HasPrefix(sql, "SELECT date_trunc('hour', time), count(*) FROM read_parquet([") {
		t.Fatalf("unexpected prefix: %s", sql)
	}
	if !strings.Contains(sql, ") WHERE "+where+" GROUP BY 1") {
		t.Fatalf("WHERE not emitted between read_parquet and GROUP BY 1: %s", sql)
	}
	if !strings.HasSuffix(sql, " GROUP BY 1") {
		t.Fatalf("unexpected suffix: %s", sql)
	}
}
