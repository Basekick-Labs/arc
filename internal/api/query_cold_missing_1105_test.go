package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/fieldschema"
)

// fakeColdGuard is a node whose cold tier is on but failed to start, with
// cold rows for the measurements in cold (any time) or none.
type fakeColdGuard struct {
	missing bool
	cold    map[string]bool
	err     error
	calls   int
	start   *time.Time
}

func (f *fakeColdGuard) ColdTierMissing() bool { return f.missing }

func (f *fakeColdGuard) HasColdFilesInRange(_ context.Context, _, measurement string, start, _ *time.Time) (bool, error) {
	f.calls++
	f.start = start
	return f.cold[measurement], f.err
}

// #1105: a node whose cold tier is configured on but failed to start (bad
// endpoint, no credentials) used to answer a read of cold-tier data with the
// hot rows alone, success:true. It now refuses with 503, on both query paths,
// and only for a measurement that has cold rows; everything else is served.
func TestReadNeedingAMissingColdTierIsRefused(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	at := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	e.write("sch", "archived", at, map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	e.write("sch", "fresh", at, map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})

	guard := &fakeColdGuard{missing: true, cold: map[string]bool{"archived": true}}
	e.h.coldGuard = guard

	for _, headerDB := range []string{"", "sch"} {
		sql := "SELECT count(*) FROM sch.archived"
		if headerDB != "" {
			sql = "SELECT count(*) FROM archived"
		}
		resp, code := e.queryDB(sql, headerDB)
		if code != 503 || resp.Success || !strings.Contains(resp.Error, "cold storage tier") {
			t.Fatalf("header %q: code %d resp %+v, want 503 naming the cold tier", headerDB, code, resp)
		}
	}

	if resp, code := e.query("SELECT count(*) FROM sch.fresh"); code != 200 || !resp.Success {
		t.Fatalf("a measurement with no cold rows must still be served: %d %+v", code, resp)
	}

	// The query's time range reaches the guard (start widened there, not here).
	guard.calls = 0
	e.query("SELECT count(*) FROM sch.fresh WHERE time >= '2026-01-01T09:00:00Z' AND time < '2026-01-01T11:00:00Z'")
	if guard.calls == 0 || guard.start == nil || !guard.start.Equal(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("guard calls %d start %v; want the query's start", guard.calls, guard.start)
	}

	// A metadata error refuses: it cannot prove the read needs no cold data.
	guard.err = errors.New("sqlite busy")
	if _, code := e.query("SELECT count(*) FROM sch.fresh"); code != 503 {
		t.Fatalf("metadata error: code %d, want 503", code)
	}

	// A healthy cold tier is never asked.
	guard.err, guard.missing, guard.calls = nil, false, 0
	if resp, code := e.query("SELECT count(*) FROM sch.archived"); code != 200 || !resp.Success || guard.calls != 0 {
		t.Fatalf("healthy cold tier: %d %+v calls %d", code, resp, guard.calls)
	}
}

func TestTransformErrorStatus(t *testing.T) {
	if transformErrorStatus(errColdTierUnavailable) != 503 {
		t.Fatal("cold tier unavailable must be 503")
	}
	if transformErrorStatus(errors.New("bad name")) != 400 {
		t.Fatal("other transform errors stay 400")
	}
}
