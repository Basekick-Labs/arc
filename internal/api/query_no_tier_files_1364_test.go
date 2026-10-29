package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/rs/zerolog"
)

// fakeTierView is tiering's view of a measurement: tier rows that say where
// its files are, over a cold store that may hold none of them.
type fakeTierView struct {
	tiers map[tiering.Tier]bool
	cold  storage.Backend
}

func (f fakeTierView) TiersForMeasurement(context.Context, string, string) (map[tiering.Tier]bool, error) {
	return f.tiers, nil
}

func (f fakeTierView) GetBackendForTier(tier tiering.Tier) storage.Backend {
	if tier == tiering.TierCold {
		return f.cold
	}
	return nil
}

// emptyCold is an empty cold store under the DuckDB sandbox.
func emptyCold(t *testing.T, e *fieldSchemaEnv) storage.Backend {
	t.Helper()
	dir := filepath.Join(e.dataDir, "coldstore")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	b, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// read builds the multi-tier read of sch.<meas> as the query path does and
// runs `SELECT <proj> <read>`.
func multiTierRead(t *testing.T, e *fieldSchemaEnv, view fakeTierView, meas string) string {
	t.Helper()
	e.h.tierView = view
	paths := map[tiering.Tier]string{
		tiering.TierHot:  e.h.getStoragePath(context.Background(), "sch", meas),
		tiering.TierCold: e.h.storagePathForBackend(context.Background(), view.cold, "sch", meas),
	}
	return e.h.buildMultiTierReadParquet(context.Background(), "sch", meas, "SELECT count(*) FROM sch."+meas, paths, "FROM")
}

// #1364: tier rows say a measurement has files (hot and cold), but every one
// is gone. The read was "(SELECT * WHERE 1=0)", which DuckDB refuses to bind
// ("* expression without FROM clause"), so even count(*) failed. With the
// schema anchor it reads as zero rows with the measurement's columns.
func TestMultiTierReadOfAMeasurementWithNoFilesLeft(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	at := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	e.write("sch", "gone", at, map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	e.mustRows("SELECT count(*) FROM sch.gone") // builds the anchor
	if err := os.RemoveAll(filepath.Join(e.dataDir, "sch", "gone")); err != nil {
		t.Fatal(err)
	}

	from := multiTierRead(t, e, fakeTierView{tiers: map[tiering.Tier]bool{tiering.TierHot: true, tiering.TierCold: true}, cold: emptyCold(t, e)}, "gone")
	var n int64
	if err := e.duck.DB().QueryRow("SELECT count(*) " + from).Scan(&n); err != nil {
		t.Fatalf("count(*) %s: %v", from, err)
	}
	if n != 0 {
		t.Fatalf("count = %d, want 0", n)
	}
	var avg any
	if err := e.duck.DB().QueryRow("SELECT avg(v) " + from + " WHERE host = 'a'").Scan(&avg); err != nil {
		t.Fatalf("a query naming the measurement's columns does not bind: %v", err)
	}
}

// Without an anchor, the read is the empty-measurement one: "No files found",
// which the handler answers with an empty result.
func TestMultiTierReadWithNoFilesLeftAndNoAnchor(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, false)
	e.h.SetFieldSchema(nil)
	from := multiTierRead(t, e, fakeTierView{tiers: map[tiering.Tier]bool{tiering.TierHot: true}, cold: emptyCold(t, e)}, "never")
	_, err := e.duck.DB().Exec("SELECT count(*) " + from)
	if err == nil {
		t.Fatal("a read with no files read without error")
	}
	if kind := e.h.classifyNoFiles(err); kind != noFilesEmpty {
		t.Fatalf("classified %v (%v), want noFilesEmpty", kind, err)
	}
}

// #1364 review: a measurement whose rows are cold only, with cold now empty,
// must not start reading local files the tier rows left out (stale copies of
// deleted files): without an anchor its empty read matches nothing.
func TestMultiTierReadWithNoFilesLeftDoesNotReadLocalFilesTheRowsLeftOut(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, false)
	e.h.SetFieldSchema(nil)
	e.write("sch", "stale", time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC), map[string]interface{}{"v": []int64{1, 2}})
	from := multiTierRead(t, e, fakeTierView{tiers: map[tiering.Tier]bool{tiering.TierCold: true}, cold: emptyCold(t, e)}, "stale")
	if strings.Contains(from, "/sch/stale/") {
		t.Fatalf("read %s names the hot glob the tier rows left out", from)
	}
	_, err := e.duck.DB().Exec("SELECT count(*) " + from)
	if err == nil {
		t.Fatal("the read found files: the local copy the rows left out was read")
	}
	if kind := e.h.classifyNoFiles(err); kind != noFilesEmpty {
		t.Fatalf("classified %v (%v), want noFilesEmpty", kind, err)
	}
}
