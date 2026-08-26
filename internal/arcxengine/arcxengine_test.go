//go:build cgo && arcx_engine

package arcxengine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// A real Arc parquet fixture with a known row count (243 rows), matching the
// arcx-side FFI test and the differential harness.
// Fixture path: ARCX_TEST_FIXTURE, else the repo-relative default below. It was a
// hardcoded absolute developer path, which meant these tests SILENTLY SKIPPED on every
// other machine (and in CI) — so the FFI bridge appeared covered while it was not.
var fixture243 = func() string {
	if p := os.Getenv("ARCX_TEST_FIXTURE"); p != "" {
		return p
	}
	// MUST be absolute: the engine sandbox rejects any `..` component outright (it
	// refuses the traversal *intent*, before resolving), so a repo-relative path would
	// be declined rather than read.
	abs, _ := filepath.Abs(filepath.Join("..", "..", "data", "arc", "agent_memory",
		"agent_events", "2026", "02", "03", "agent_events_20260209_184547_daily.parquet"))
	return abs
}()

// fixtureCtx is a Context whose sandbox actually contains the fixture.
//
// `Context{}` means NO allowed directories, which the engine treats as deny-all
// (fail-closed by design — an empty allowlist must never mean "allow everything").
// These tests used to pass with an empty Context only because the single-quoted
// `read_parquet('<path>')` form DECLINED at parse before the sandbox was ever
// consulted. That form now SERVES (arcx #54), so the deny-all sandbox correctly
// rejects it and the tests must supply the root they actually read from.
//
// The two negative tests below deliberately keep `Context{}` — they assert a
// decline and a missing-file error, neither of which should depend on a sandbox.
func fixtureCtx(path string) Context {
	return Context{AllowedDirs: []string{filepath.Dir(path)}}
}

func TestBridgeCountStar(t *testing.T) {
	if _, err := os.Stat(fixture243); err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	if !Available() {
		t.Fatal("engine should be available in the tagged build")
	}

	rec, err := Query("SELECT count(*) FROM read_parquet('"+fixture243+"')", fixtureCtx(fixture243))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rec.Release()

	if rec.NumCols() != 1 || rec.NumRows() != 1 {
		t.Fatalf("expected 1x1 result, got %dx%d", rec.NumRows(), rec.NumCols())
	}
	col, ok := rec.Column(0).(*array.Int64)
	if !ok {
		t.Fatalf("expected Int64 count column, got %T", rec.Column(0))
	}
	if got := col.Value(0); got != 243 {
		t.Fatalf("count(*) = %d, want 243", got)
	}
	// Column name should match DuckDB's default alias.
	if name := rec.ColumnName(0); name != "count_star()" {
		t.Fatalf("column name = %q, want count_star()", name)
	}
}

func TestBridgeUnsupportedDeclines(t *testing.T) {
	// A shape the engine doesn't handle → ErrUnsupported (caller falls back).
	_, err := Query("SELECT sum(x) FROM read_parquet('/x.parquet')", Context{})
	var un ErrUnsupported
	if !errors.As(err, &un) {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
}

func TestBridgeExecutionError(t *testing.T) {
	// Supported shape over a missing file → a real error (not ErrUnsupported).
	_, err := Query("SELECT count(*) FROM read_parquet('/nonexistent.parquet')", Context{})
	if err == nil {
		t.Fatal("expected an error for a missing file")
	}
	var un ErrUnsupported
	if errors.As(err, &un) {
		t.Fatalf("missing file should be a real error, not ErrUnsupported: %v", err)
	}
}

func TestBridgeVersion(t *testing.T) {
	v := Version()
	if v == "" {
		t.Fatal("version should be non-empty in the tagged build")
	}
}

// Repeated calls must not crash — a crude smoke test for leaks / double-free /
// use-after-free across the FFI boundary (the process-fatal class).
func TestBridgeRepeatedCallsNoCrash(t *testing.T) {
	if _, err := os.Stat(fixture243); err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	for i := 0; i < 200; i++ {
		rec, err := Query("SELECT count(*) FROM read_parquet('"+fixture243+"')", fixtureCtx(fixture243))
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		rec.Release()
	}
}
