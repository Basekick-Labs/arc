// End-to-end smoke: drive Decide → buildEngineSQL → arcxengine.Query against REAL
// Arc parquet data, proving the router constructs SQL the engine actually
// executes, and that the hour-over-daily decline (F2) fires. Tagged; skips if the
// fixture data root isn't present (so it's a no-op in CI without data).

//go:build cgo && arcx_engine

package arcxrouter

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/arcxengine"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// dataRoot is Arc's local data dir. The fixture db=agent_memory,
// measurement=agent_events lives under data/arc/, with daily-compacted files
// (Y/M/D, no hour dir) — perfect for exercising the day accept AND the hour
// decline. Root the backend at data/arc so the List prefix is
// agent_memory/agent_events/.
const dataRoot = "/Users/nacho/dev/basekick-labs/arc/data"

func smokeDepsRootedAtArc(t *testing.T) (Deps, bool) {
	t.Helper()
	root := dataRoot + "/arc"
	if _, err := os.Stat(root + "/agent_memory/agent_events"); err != nil {
		return Deps{}, false
	}
	be, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	return Deps{Storage: be, Logger: zerolog.Nop(), Mode: ModeShadow}, true
}

func TestE2E_CountStar_ExecutesOnEngine(t *testing.T) {
	deps, ok := smokeDepsRootedAtArc(t)
	if !ok {
		t.Skip("fixture data not present")
	}
	d := Decision{
		Eligible: true,
		Shape:    ShapeCountStar,
		Ctx:      arcxengine.Context{Database: "agent_memory", Measurement: "agent_events", TimeColumn: "time"},
	}
	sql, ok := deps.buildEngineSQL(context.Background(), d)
	if !ok {
		t.Fatal("buildEngineSQL declined for real data")
	}
	rec, err := arcxengine.Query(sql, d.Ctx)
	if err != nil {
		t.Fatalf("engine failed to execute router-built count SQL: %v", err)
	}
	defer rec.Release()
	got, err := canonicalFromArcx(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.rows) != 1 || got.rows[0].count <= 0 {
		t.Fatalf("expected one positive count row, got %+v", got.rows)
	}
	t.Logf("router→engine count(*) = %d", got.rows[0].count)
}

func TestE2E_DayAgg_ExecutesOnEngine(t *testing.T) {
	deps, ok := smokeDepsRootedAtArc(t)
	if !ok {
		t.Skip("fixture data not present")
	}
	d := Decision{
		Eligible: true,
		Shape:    ShapeDateTruncCent,
		Unit:     "day",
		Ctx:      arcxengine.Context{Database: "agent_memory", Measurement: "agent_events", TimeColumn: "time"},
	}
	sql, ok := deps.buildEngineSQL(context.Background(), d)
	if !ok {
		t.Fatal("buildEngineSQL declined")
	}
	rec, err := arcxengine.Query(sql, d.Ctx)
	if err != nil {
		t.Fatalf("engine failed on day agg: %v", err)
	}
	defer rec.Release()
	got, err := canonicalFromArcx(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.rows) == 0 {
		t.Fatal("expected at least one day bucket")
	}
	t.Logf("router→engine day agg produced %d buckets", len(got.rows))
}

// The load-bearing correctness gate (F2): hour bucketing over daily-compacted
// files must DECLINE at the engine (Unsupported), which the router maps to a
// silent DuckDB fallback. This proves the router doesn't need a daily-file
// pre-filter — the engine is the authority.
func TestE2E_HourOverDaily_EngineDeclines(t *testing.T) {
	deps, ok := smokeDepsRootedAtArc(t)
	if !ok {
		t.Skip("fixture data not present")
	}
	d := Decision{
		Eligible: true,
		Shape:    ShapeDateTruncCent,
		Unit:     "hour",
		Ctx:      arcxengine.Context{Database: "agent_memory", Measurement: "agent_events", TimeColumn: "time"},
	}
	sql, ok := deps.buildEngineSQL(context.Background(), d)
	if !ok {
		t.Fatal("buildEngineSQL declined before engine")
	}
	_, err := arcxengine.Query(sql, d.Ctx)
	var unsupported arcxengine.ErrUnsupported
	if !errors.As(err, &unsupported) {
		t.Fatalf("expected ErrUnsupported (hour over daily files), got %v", err)
	}
}
