package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// fakeDeleteColdTier is tiering as a DELETE of cold files sees it (#1350),
// over a real local backend standing in for the cold store.
type fakeDeleteColdTier struct {
	backend    storage.Backend
	configured bool
	rows       []tiering.FileMetadata
	retired    []string
	reverted   []string
}

func (f *fakeDeleteColdTier) ColdBackend() storage.Backend { return f.backend }
func (f *fakeDeleteColdTier) ColdConfigured() bool         { return f.configured }
func (f *fakeDeleteColdTier) ColdFilesForMeasurement(_ context.Context, db, meas string) ([]tiering.FileMetadata, error) {
	var out []tiering.FileMetadata
	for _, r := range f.rows {
		if r.Database == db && r.Measurement == meas {
			out = append(out, r)
		}
	}
	return out, nil
}
func (f *fakeDeleteColdTier) RetireColdRows(_ context.Context, paths []string) (int, error) {
	f.retired = append(f.retired, paths...)
	return len(paths), nil
}
func (f *fakeDeleteColdTier) RevertColdRowToHot(_ context.Context, path string) error {
	f.reverted = append(f.reverted, path)
	return nil
}

type coldDeleteRig struct {
	*leftoverRig
	coldRoot string
	cold     storage.Backend
	tier     *fakeDeleteColdTier
	app      *fiber.App
	removed  int
}

const coldDay = "db/cpu/2026/01/05/"

func newColdDeleteRig(t *testing.T) *coldDeleteRig {
	t.Helper()
	r := newLeftoverRig(t)
	h := NewDeleteHandler(r.db, r.backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	r.h = h
	// Under the DuckDB sandbox root, outside the hot "db/" prefix.
	coldRoot := filepath.Join(r.root, "coldstore")
	if err := os.MkdirAll(coldRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	cold, err := storage.NewLocalBackend(coldRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cold.Close() })
	tier := &fakeDeleteColdTier{backend: cold, configured: true}
	h.coldTier = tier
	rig := &coldDeleteRig{leftoverRig: r, coldRoot: coldRoot, cold: cold, tier: tier}
	h.SetOnFilesRemoved(func() { rig.removed++ })
	rig.app = fiber.New()
	h.RegisterRoutes(rig.app)
	return rig
}

// coldFile writes key into cold storage with one row per host, and its row.
func (r *coldDeleteRig) coldFile(key string, hosts ...string) {
	r.t.Helper()
	full := filepath.Join(r.coldRoot, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		r.t.Fatal(err)
	}
	vals := make([]string, len(hosts))
	for i, h := range hosts {
		vals[i] = fmt.Sprintf("('%s', %d)", h, i)
	}
	q := fmt.Sprintf("COPY (SELECT * FROM (VALUES %s) AS v(host, v)) TO '%s' (FORMAT PARQUET)", strings.Join(vals, ", "), full)
	if _, err := r.db.DB().Exec(q); err != nil {
		r.t.Fatal(err)
	}
	r.tier.rows = append(r.tier.rows, tiering.FileMetadata{Path: key, Database: "db", Measurement: "cpu", Tier: tiering.TierCold})
}

func (r *coldDeleteRig) coldExists(key string) bool {
	size, err := r.cold.StatFile(context.Background(), key)
	if err != nil {
		r.t.Fatal(err)
	}
	return size >= 0
}

func (r *coldDeleteRig) delete(body string) (int, DeleteResponse) {
	r.t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.app.Test(req, 20000)
	if err != nil {
		r.t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out DeleteResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		r.t.Fatalf("decode %s: %v", raw, err)
	}
	return resp.StatusCode, out
}

const (
	coldWhole   = coldDay + "cpu_20260105_daily.parquet"
	coldPartial = coldDay + "cpu_20260105_b1_daily.parquet"
)

// #1350: a DELETE whose rows are all in cold storage answered 200 with zero
// files, and queries kept returning them. A cold file whose rows all match is
// deleted now, its row retired and the caches dropped.
func TestDeleteRemovesAColdFileWhoseRowsAllMatch(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0", "h0")

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if code != 200 || !out.Success {
		t.Fatalf("got %d %+v, want 200", code, out)
	}
	if out.DeletedCount != 2 || !slices.Equal(out.ColdDeletedFiles, []string{filepath.Base(coldWhole)}) {
		t.Fatalf("deleted %d rows, cold_deleted_files %v; want 2 and the cold file", out.DeletedCount, out.ColdDeletedFiles)
	}
	if r.coldExists(coldWhole) {
		t.Fatal("the cold file whose rows all match is still there")
	}
	if !slices.Equal(r.tier.retired, []string{coldWhole}) {
		t.Fatalf("retired %v, want its row", r.tier.retired)
	}
	if r.removed == 0 {
		t.Fatal("the query caches were not dropped after a cold file was deleted")
	}
}

// A cold file with only some matching rows is left as it is and reported:
// 207, named in cold_files and failed_files, its rows outside deleted_count.
func TestDeleteReportsAColdFileWithSomeMatchingRows(t *testing.T) {
	r := newColdDeleteRig(t)
	r.parquet(leftoverHour+"cpu_20261007_205000_000000000.parquet", "h0", "h5")
	r.coldFile(coldPartial, "h0", "h1", "h0")

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if code != 207 || out.Success {
		t.Fatalf("got %d %+v, want 207", code, out)
	}
	if out.DeletedCount != 1 {
		t.Fatalf("deleted_count %d, want 1 (the hot row only)", out.DeletedCount)
	}
	if out.ColdRowsNotDeleted != 2 || !slices.Equal(out.ColdFiles, []string{filepath.Base(coldPartial)}) ||
		!slices.Contains(out.FailedFiles, filepath.Base(coldPartial)) {
		t.Fatalf("cold report %+v, want 2 rows left in the partial file", out)
	}
	if !strings.Contains(out.Error, "only some of their rows match") {
		t.Fatalf("error %q does not say why", out.Error)
	}
	if !r.coldExists(coldPartial) {
		t.Fatal("a cold file with rows that do not match was deleted")
	}
}

// A dry run reports both kinds and changes nothing; the partial rows are not
// promised in deleted_count.
func TestDeleteDryRunReportsColdFiles(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0")
	r.coldFile(coldPartial, "h0", "h1")

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","dry_run":true}`)
	if code != 200 || !out.DryRun {
		t.Fatalf("got %d %+v, want a 200 dry run", code, out)
	}
	if out.DeletedCount != 1 || out.ColdRowsNotDeleted != 1 || len(out.ColdDeletedFiles) != 1 || len(out.ColdFiles) != 1 {
		t.Fatalf("dry run %+v, want 1 row to delete from the whole file and 1 left in the partial one", out)
	}
	if !r.coldExists(coldWhole) || !r.coldExists(coldPartial) || len(r.tier.retired) != 0 {
		t.Fatal("a dry run changed cold storage")
	}
}

// A file both in hot storage and in cold (a move that has not released the
// hot copy) is the hot side's: its cold copy is never deleted, and it is not
// counted twice.
func TestDeleteLeavesTheColdCopyOfAFileStillInHotStorage(t *testing.T) {
	r := newColdDeleteRig(t)
	r.parquet(coldWhole, "h0")
	r.coldFile(coldWhole, "h0")

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if !r.coldExists(coldWhole) {
		t.Fatal("deleted the cold copy of a file still in hot storage")
	}
	if code != 207 || !strings.Contains(out.Error, "copy is still in hot storage") {
		t.Fatalf("got %d %q, want 207 naming the hot copy", code, out.Error)
	}
	if out.DeletedCount != 1 {
		t.Fatalf("deleted_count %d, want 1 (the hot copy's row, once)", out.DeletedCount)
	}
}

// Cold rows count against the limits: a DELETE that would delete too many
// rows touches nothing.
func TestDeleteColdRowsCountAgainstTheLimits(t *testing.T) {
	r := newColdDeleteRig(t)
	r.h.config.MaxRowsPerDelete = 2
	r.coldFile(coldWhole, "h0", "h0", "h0")

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if code != 400 || !strings.Contains(out.Error, "exceeding maximum") {
		t.Fatalf("got %d %q, want the row limit", code, out.Error)
	}
	if !r.coldExists(coldWhole) {
		t.Fatal("a DELETE over the row limit deleted a cold file")
	}
}

// Cold storage configured but not usable here: the DELETE says so (207)
// rather than answer 200 as if nothing matched there.
func TestDeleteSaysWhenColdStorageWasNotExamined(t *testing.T) {
	r := newColdDeleteRig(t)
	r.tier.backend = nil
	r.parquet(leftoverHour+"cpu_20261007_205000_000000000.parquet", "h0", "h5")

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if code != 207 || out.ColdNotExamined == "" || out.DeletedCount != 1 {
		t.Fatalf("got %d %+v, want 207 naming cold storage as not examined, the hot row deleted", code, out)
	}
}

// No tiering, or cold storage not configured: unchanged.
func TestDeleteWithoutColdStorageIsUnchanged(t *testing.T) {
	r := newColdDeleteRig(t)
	r.tier.configured = false
	r.coldFile(coldWhole, "h0")
	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if code != 200 || out.DeletedCount != 0 || r.coldExists(coldWhole) == false {
		t.Fatalf("got %d %+v, want the old answer with cold storage untouched", code, out)
	}
}

// A cold row whose object is gone is retired, or pointed back at its hot copy.
func TestDeleteRetiresColdRowsWhoseObjectIsGone(t *testing.T) {
	r := newColdDeleteRig(t)
	gone := coldDay + "cpu_20260105_gone_daily.parquet"
	back := coldDay + "cpu_20260105_back_daily.parquet"
	r.tier.rows = append(r.tier.rows,
		tiering.FileMetadata{Path: gone, Database: "db", Measurement: "cpu", Tier: tiering.TierCold},
		tiering.FileMetadata{Path: back, Database: "db", Measurement: "cpu", Tier: tiering.TierCold})
	r.parquet(back, "h9")

	r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if !slices.Equal(r.tier.retired, []string{gone}) || !slices.Equal(r.tier.reverted, []string{back}) {
		t.Fatalf("retired %v reverted %v, want the gone row retired and the one with a hot copy reverted", r.tier.retired, r.tier.reverted)
	}
}

// A cold file that cannot be read is reported, never taken as "no match".
func TestDeleteReportsAnUnreadableColdFile(t *testing.T) {
	r := newColdDeleteRig(t)
	bad := coldDay + "cpu_20260105_bad_daily.parquet"
	if err := r.cold.Write(context.Background(), bad, []byte("not parquet")); err != nil {
		t.Fatal(err)
	}
	r.tier.rows = append(r.tier.rows, tiering.FileMetadata{Path: bad, Database: "db", Measurement: "cpu", Tier: tiering.TierCold})

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if code != 207 || !slices.Contains(out.FailedFiles, filepath.Base(bad)) {
		t.Fatalf("got %d %+v, want 207 naming the unreadable cold file", code, out)
	}
}
