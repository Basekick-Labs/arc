package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
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
	// onRemoved runs inside the cache-drop hook: what it sees is what a
	// cache refilled right after the drop would see.
	onRemoved func()
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
	h.SetOnFilesRemoved(func() {
		rig.removed++
		if rig.onRemoved != nil {
			rig.onRemoved()
		}
	})
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
	stillThere := false
	r.onRemoved = func() { stillThere = stillThere || r.coldExists(coldWhole) }

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
	if stillThere {
		t.Fatal("the query caches were dropped before the cold file was deleted: a read in between caches the deleted object")
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
// hot copy) is the hot side's: the claim refuses the hot copy, the cold copy
// is never deleted, and the file is reported once.
func TestDeleteLeavesTheColdCopyOfAFileStillInHotStorage(t *testing.T) {
	r := newColdDeleteRig(t)
	r.parquet(coldWhole, "h0")
	r.coldFile(coldWhole, "h0")
	r.h.moveClaims = &fakeMoveClaims{moving: map[string]bool{coldWhole: true}}

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if !r.coldExists(coldWhole) || !r.exists(coldWhole) {
		t.Fatal("a copy of a file in both tiers was deleted")
	}
	if code != 207 || !strings.Contains(out.Error, "copy is still in hot storage") {
		t.Fatalf("got %d %q, want 207 naming the hot copy", code, out.Error)
	}
	if out.DeletedCount != 0 || out.ColdRowsNotDeleted != 0 || len(out.FailedFiles) != 1 || out.AffectedFiles != 1 {
		t.Fatalf("got %+v, want nothing deleted and the file reported once", out)
	}
}

// fakeColdDeleteCoordinator is a cluster as the cold pass sees it: a manifest
// entry for a path means the cluster still has it in hot storage.
type fakeColdDeleteCoordinator struct {
	entries map[string]raft.FileEntry
}

func (f *fakeColdDeleteCoordinator) BatchFileOpsInManifest([]raft.BatchFileOp) error { return nil }
func (f *fakeColdDeleteCoordinator) GetFileEntry(path string) (*raft.FileEntry, bool) {
	e, ok := f.entries[path]
	return &e, ok
}
func (f *fakeColdDeleteCoordinator) IsPrimaryWriter() bool { return true }
func (f *fakeColdDeleteCoordinator) Role() string          { return "writer" }
func (f *fakeColdDeleteCoordinator) LocalNodeID() string   { return "w1" }

// On a cluster, a cold object the manifest still lists in hot storage (on
// another node, which could pull it back) is never deleted.
func TestDeleteLeavesAColdFileTheManifestStillLists(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0")
	r.h.coordinator = &fakeColdDeleteCoordinator{entries: map[string]raft.FileEntry{coldWhole: {Path: coldWhole, Tier: "hot"}}}
	h := r.h
	c := h.scanCold(context.Background(), "db", "cpu", "host = 'h0'")
	if c == nil || len(c.hotCopy) != 1 || len(c.whole) != 0 {
		t.Fatalf("scan %+v, want the file held as the hot side's", c)
	}
	// A manifest entry recorded as cold (older manifests) is not a hot copy.
	r.h.coordinator = &fakeColdDeleteCoordinator{entries: map[string]raft.FileEntry{coldWhole: {Path: coldWhole, Tier: "cold"}}}
	if c := h.scanCold(context.Background(), "db", "cpu", "host = 'h0'"); c == nil || len(c.whole) != 1 {
		t.Fatalf("scan %+v, want a cold manifest entry not to count as a hot copy", c)
	}
}

// listFailingBackend is a cold store whose listing fails.
type listFailingBackend struct{ storage.Backend }

func (b listFailingBackend) ListObjects(context.Context, string) ([]storage.ObjectInfo, error) {
	return nil, errors.New("bucket unreachable")
}

// A cold listing that fails is reported, never taken as an empty cold tier.
func TestDeleteSaysWhenTheColdListingFails(t *testing.T) {
	r := newColdDeleteRig(t)
	r.tier.backend = listFailingBackend{r.cold}
	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if code != 207 || !strings.Contains(out.ColdNotExamined, "bucket unreachable") {
		t.Fatalf("got %d %+v, want 207 naming the listing failure", code, out)
	}
}

// A cold file older than a column the clause names matches nothing there: not
// a failure, or every DELETE naming a newer column would answer 207.
func TestDeleteOfANewerColumnSkipsOlderColdFiles(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0")
	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"region = 'eu'","confirm":true}`)
	if code != 200 || len(out.FailedFiles) != 0 || !r.coldExists(coldWhole) {
		t.Fatalf("got %d %+v, want 200 with the older cold file untouched", code, out)
	}
}

// #1350 review H1: a row the listing did not show is checked again before
// it is retired. A migration may have copied its object since.
func TestRunColdKeepsARowWhoseObjectAppearedSinceTheListing(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0") // the object is there now
	c := &coldPass{backend: r.cold, missingRows: []string{coldWhole}}
	r.h.runCold(context.Background(), c, "host = 'h0'")
	if len(r.tier.retired) != 0 || len(r.tier.reverted) != 0 {
		t.Fatalf("retired %v reverted %v, want the row of an object that exists left alone", r.tier.retired, r.tier.reverted)
	}
}

// A whole-match file that no longer matches whole when it is deleted (it
// changed between the count and the delete) is reported, not deleted.
func TestRunColdRecountsBeforeDeleting(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0", "h1")
	size, _ := r.cold.StatFile(context.Background(), coldWhole)
	c := &coldPass{backend: r.cold, whole: []coldFile{{path: coldWhole, queryPath: filepath.Join(r.coldRoot, coldWhole), size: size, total: 2}}}
	r.h.runCold(context.Background(), c, "host = 'h0'")
	if !r.coldExists(coldWhole) || len(c.partial) != 1 || len(c.deleted) != 0 {
		t.Fatalf("pass %+v, want the file kept and reported partial", c)
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
