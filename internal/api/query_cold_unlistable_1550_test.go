package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/rs/zerolog"
)

// objectStoreCold is an S3 cold tier served by handler, with a count of the
// requests it received.
func objectStoreCold(t *testing.T, handler http.HandlerFunc) (storage.Backend, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	b, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: "cold", Region: "us-east-1", Endpoint: strings.TrimPrefix(srv.URL, "http://"),
		UseSSL: false, PathStyle: true, AccessKey: "test", SecretKey: "test",
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewS3Backend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, &requests
}

// deniedCold is a cold tier whose every listing is refused, as with expired
// or unresolvable credentials.
func deniedCold(t *testing.T) (storage.Backend, *atomic.Int64) {
	return objectStoreCold(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
	})
}

// emptyObjectStoreCold is a healthy cold tier that holds nothing.
func emptyObjectStoreCold(t *testing.T) (storage.Backend, *atomic.Int64) {
	return objectStoreCold(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>cold</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`))
	})
}

// coldReadOf builds the multi-tier read of sch.<meas> for sql, as the query
// path does, and reports whether the transform was marked volatile.
func coldReadOf(t *testing.T, e *fieldSchemaEnv, view fakeTierView, meas, sql string) (string, bool) {
	t.Helper()
	e.h.tierView = view
	ctx, vr := pruning.WithVolatileResult(context.Background())
	paths := map[tiering.Tier]string{
		tiering.TierHot:  e.h.getStoragePath(ctx, "sch", meas),
		tiering.TierCold: e.h.storagePathForBackend(ctx, view.cold, "sch", meas),
	}
	return e.h.buildMultiTierReadParquet(ctx, "sch", meas, sql, paths, "FROM"), vr.Volatile
}

// #1550: a cold tier that cannot be listed kept its range's paths
// unverified, and DuckDB failed every read of a measurement with any cold
// rows on them, a hot-only range included. Tier metadata now decides: a
// range with no cold rows is answered from the hot tier.
func TestHotOnlyReadSucceedsWhileColdTierCannotBeListed(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "mixed", time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC), map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	cold, requests := deniedCold(t)
	var asks [][2]*time.Time
	view := fakeTierView{
		tiers:     map[tiering.Tier]bool{tiering.TierHot: true, tiering.TierCold: true},
		cold:      cold,
		rangeAsks: &asks,
	}

	from, volatile := coldReadOf(t, e, view, "mixed",
		"SELECT count(*) FROM sch.mixed WHERE time >= '2026-01-01T09:00:00Z' AND time < '2026-01-01T11:00:00Z'")
	if requests.Load() == 0 {
		t.Fatal("the cold tier was never listed; the test does not exercise an unlistable cold tier")
	}
	if strings.Contains(from, "s3://") {
		t.Fatalf("read %s still names the cold tier, which holds no rows in range", from)
	}
	var n int64
	if err := e.duck.DB().QueryRow("SELECT count(*) " + from).Scan(&n); err != nil {
		t.Fatalf("count(*) %s: %v", from, err)
	}
	if n != 1 {
		t.Fatalf("count = %d, want the 1 hot row", n)
	}
	if !volatile {
		t.Error("a read that left out the cold tier must not be cached: cold rows can arrive while the tier is down")
	}
	// The query's own range is what metadata is asked about.
	if len(asks) != 1 || asks[0][0] == nil || !asks[0][0].Equal(time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)) ||
		asks[0][1] == nil || !asks[0][1].Equal(time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("metadata asks %v, want one with the query's start and end", asks)
	}

	// A start-only range asks with an open end, and is answered from hot.
	asks = nil
	from, _ = coldReadOf(t, e, view, "mixed", "SELECT count(*) FROM sch.mixed WHERE time >= '2026-01-01T09:00:00Z'")
	if strings.Contains(from, "s3://") {
		t.Fatalf("start-only read %s still names the cold tier", from)
	}
	if len(asks) != 1 || asks[0][0] == nil || asks[0][1] != nil {
		t.Fatalf("start-only metadata asks %v, want one with the start and an open end", asks)
	}
}

// A read that does need the cold tier keeps it while the tier cannot be
// listed, and so fails loudly instead of answering without cold rows: when
// metadata has cold rows in range, when metadata cannot be asked, and when
// the range has no start to bound cold rows by.
func TestReadNeedingAnUnlistableColdTierKeepsIt(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "mixed", time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC), map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	cold, _ := deniedCold(t)
	tiers := map[tiering.Tier]bool{tiering.TierHot: true, tiering.TierCold: true}
	bounded := "SELECT count(*) FROM sch.mixed WHERE time >= '2026-01-01T09:00:00Z' AND time < '2026-01-01T11:00:00Z'"

	for _, tc := range []struct {
		name string
		view fakeTierView
		sql  string
	}{
		{"cold rows in range", fakeTierView{tiers: tiers, cold: cold, coldInRange: true}, bounded},
		{"metadata error", fakeTierView{tiers: tiers, cold: cold, coldRangeErr: errors.New("sqlite busy")}, bounded},
		{"end-only range", fakeTierView{tiers: tiers, cold: cold}, "SELECT count(*) FROM sch.mixed WHERE time < '2026-01-01T11:00:00Z'"},
		{"no range", fakeTierView{tiers: tiers, cold: cold}, "SELECT count(*) FROM sch.mixed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if from, _ := coldReadOf(t, e, tc.view, "mixed", tc.sql); !strings.Contains(from, "s3://cold/sch/mixed/") {
				t.Fatalf("read %s left out the cold tier the query needs", from)
			}
		})
	}
}

// A cold tier that lists normally is pruned by its listing, as before:
// metadata is never asked.
func TestListableColdTierIsNotAskedAboutColdRows(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "mixed", time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC), map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	cold, requests := emptyObjectStoreCold(t)
	var asks [][2]*time.Time
	view := fakeTierView{
		tiers:       map[tiering.Tier]bool{tiering.TierHot: true, tiering.TierCold: true},
		cold:        cold,
		coldInRange: true,
		rangeAsks:   &asks,
	}

	from, _ := coldReadOf(t, e, view, "mixed",
		"SELECT count(*) FROM sch.mixed WHERE time >= '2026-01-01T09:00:00Z' AND time < '2026-01-01T11:00:00Z'")
	if requests.Load() == 0 {
		t.Fatal("the cold tier was never listed")
	}
	if strings.Contains(from, "s3://") {
		t.Fatalf("read %s names a cold tier its listing verified empty", from)
	}
	if len(asks) != 0 {
		t.Fatalf("metadata asked %d times about a cold tier that lists, want 0", len(asks))
	}
}
