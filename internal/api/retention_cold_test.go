package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// fakeColdTier is the tiering side of cold retention (#168) over a real local
// backend standing in for the cold store.
type fakeColdTier struct {
	backend  storage.Backend // nil: cold storage not usable on this node
	rows     []tiering.FileMetadata
	listErr  error
	retired  []string
	reverted []string
	notified int
}

func (f *fakeColdTier) ColdBackend() storage.Backend { return f.backend }
func (f *fakeColdTier) GetColdFilesForRetention(_ context.Context, database string, _ time.Time) ([]tiering.FileMetadata, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []tiering.FileMetadata
	for _, r := range f.rows {
		if r.Database == database {
			out = append(out, r)
		}
	}
	return out, nil
}
func (f *fakeColdTier) RetireColdRows(_ context.Context, paths []string) (int, error) {
	f.retired = append(f.retired, paths...)
	return len(paths), nil
}
func (f *fakeColdTier) RevertColdRowToHot(_ context.Context, path string) error {
	f.reverted = append(f.reverted, path)
	return nil
}
func (f *fakeColdTier) NotifyColdFilesRemoved(n int) { f.notified += n }

type coldRetentionRig struct {
	h       *RetentionHandler
	hotDir  string
	coldDir string
	tier    *fakeColdTier
}

func newColdRetentionRig(t *testing.T) *coldRetentionRig {
	t.Helper()
	h, tmpDir := setupTestRetentionHandler(t)
	// Under tmpDir: the DuckDB sandbox only reads there.
	coldDir := filepath.Join(tmpDir, "coldstore")
	if err := os.MkdirAll(coldDir, 0o700); err != nil {
		t.Fatal(err)
	}
	cold, err := storage.NewLocalBackend(coldDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	tier := &fakeColdTier{backend: cold}
	h.coldTier = tier
	return &coldRetentionRig{h: h, hotDir: tmpDir, coldDir: coldDir, tier: tier}
}

// parquet writes a 10-row file whose rows all carry ts.
func (r *coldRetentionRig) parquet(t *testing.T, root, key string, ts time.Time) string {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	q := `COPY (SELECT TIMESTAMP '` + ts.UTC().Format("2006-01-02 15:04:05") + `' AS time, 'x' AS message FROM range(10)) TO '` + p + `' (FORMAT PARQUET)`
	if _, err := r.h.duckdb.DB().Exec(q); err != nil {
		t.Fatalf("write parquet %s: %v", key, err)
	}
	return p
}

// cold seeds a cold object and its tier row.
func (r *coldRetentionRig) cold(t *testing.T, measurement, key string, ts time.Time) string {
	t.Helper()
	p := r.parquet(t, r.coldDir, key, ts)
	r.row(measurement, key)
	return p
}

func (r *coldRetentionRig) row(measurement, key string) {
	r.tier.rows = append(r.tier.rows, tiering.FileMetadata{
		Path: key, Database: "testdb", Measurement: measurement, Tier: tiering.TierCold,
		PartitionTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	})
}

func (r *coldRetentionRig) policy(t *testing.T, measurement string) int64 {
	t.Helper()
	var m any
	if measurement != "" {
		m = measurement
	}
	res, err := r.h.db.Exec(`INSERT INTO retention_policies (name, database, measurement, retention_days, buffer_days, is_active)
		VALUES (?, ?, ?, ?, ?, ?)`, "p", "testdb", m, 30, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (r *coldRetentionRig) lastStatus(t *testing.T) (string, string) {
	t.Helper()
	var status string
	var msg *string
	if err := r.h.db.QueryRow(`SELECT status, error_message FROM retention_executions ORDER BY id DESC LIMIT 1`).Scan(&status, &msg); err != nil {
		t.Fatalf("read execution record: %v", err)
	}
	if msg == nil {
		return status, ""
	}
	return status, *msg
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

var (
	expiredTS = time.Date(2020, 1, 1, 0, 30, 0, 0, time.UTC)
	liveTS    = time.Now().UTC()
)

// The issue's case: a measurement that exists only in the cold tier. Its
// expired file goes with its row; a file in an expired partition whose rows
// are still inside retention stays; hot retention runs as before.
func TestColdRetentionDeletesExpiredColdFilesAndKeepsTheRest(t *testing.T) {
	r := newColdRetentionRig(t)
	ctx := context.Background()
	expired := r.cold(t, "cpu", "testdb/cpu/2020/01/01/00/expired.parquet", expiredTS)
	straddle := r.cold(t, "cpu", "testdb/cpu/2020/01/01/01/straddle.parquet", liveTS)
	hot := r.parquet(t, r.hotDir, "testdb/logs/2020/01/01/00/hot.parquet", expiredTS)

	res, err := r.h.ExecutePolicy(ctx, r.policy(t, ""))
	if err != nil {
		t.Fatalf("ExecutePolicy: %v", err)
	}
	if exists(expired) || !exists(straddle) || exists(hot) {
		t.Fatalf("after retention: expired cold=%v straddle cold=%v expired hot=%v, want false true false", exists(expired), exists(straddle), exists(hot))
	}
	if !slices.Equal(r.tier.retired, []string{"testdb/cpu/2020/01/01/00/expired.parquet"}) {
		t.Errorf("retired rows = %v, want the expired file's only", r.tier.retired)
	}
	if r.tier.notified == 0 {
		t.Error("query caches were not dropped after a cold object was deleted")
	}
	if res.ColdFilesDeleted != 1 || res.ColdRowsDeleted != 10 || res.FilesDeleted != 2 || res.DeletedCount != 20 {
		t.Errorf("response = %+v, want 1 cold file / 10 cold rows inside totals of 2 files / 20 rows", res)
	}
	if !slices.Contains(res.AffectedMeasurements, "cpu") {
		t.Errorf("affected measurements = %v, want the cold-only cpu", res.AffectedMeasurements)
	}
	if status, _ := r.lastStatus(t); status != "completed" {
		t.Errorf("execution status = %q, want completed", status)
	}
}

// A cold row whose object is already gone (the cold sync never retires one)
// is retired, and the run goes on to every other file, cold and hot.
func TestColdRetentionRetiresRowsOfMissingObjectsWithoutAborting(t *testing.T) {
	r := newColdRetentionRig(t)
	r.row("aaa", "testdb/aaa/2020/01/01/00/gone.parquet")
	expired := r.cold(t, "bbb", "testdb/bbb/2020/01/01/00/expired.parquet", expiredTS)
	hot := r.parquet(t, r.hotDir, "testdb/zzz/2020/01/01/00/hot.parquet", expiredTS)

	res, err := r.h.ExecutePolicy(context.Background(), r.policy(t, ""))
	if err != nil {
		t.Fatalf("ExecutePolicy aborted on a cold row with no object: %v", err)
	}
	if exists(expired) || exists(hot) {
		t.Errorf("expired files left behind: cold=%v hot=%v", exists(expired), exists(hot))
	}
	slices.Sort(r.tier.retired)
	if !slices.Equal(r.tier.retired, []string{"testdb/aaa/2020/01/01/00/gone.parquet", "testdb/bbb/2020/01/01/00/expired.parquet"}) {
		t.Errorf("retired rows = %v, want the missing object's and the deleted one's", r.tier.retired)
	}
	if res.ColdFilesSkipped != 0 {
		t.Errorf("cold_files_skipped = %d, want 0: a missing object is cleaned up, not skipped", res.ColdFilesSkipped)
	}
}

// A stale cold row over a live hot copy points back at the hot file instead of
// being deleted: deleting it would unregister a file queries still read.
func TestColdRetentionRevertsAStaleRowOverAHotCopy(t *testing.T) {
	r := newColdRetentionRig(t)
	const key = "testdb/cpu/2020/01/01/00/both.parquet"
	r.row("cpu", key)
	hot := r.parquet(t, r.hotDir, key, liveTS) // inside retention: hot keeps it

	if _, err := r.h.ExecutePolicy(context.Background(), r.policy(t, "")); err != nil {
		t.Fatalf("ExecutePolicy: %v", err)
	}
	if !exists(hot) {
		t.Fatal("hot copy deleted")
	}
	if !slices.Equal(r.tier.reverted, []string{key}) || slices.Contains(r.tier.retired, key) {
		t.Errorf("reverted = %v, retired = %v; want the row reverted to hot, not retired", r.tier.reverted, r.tier.retired)
	}
}

// Tiering on with cold storage off (Arc's own Iceberg error tells operators to
// turn cold off): hot retention runs as on a node without a cold tier, and the
// cold rows it could not examine are reported, not silently passed over.
func TestColdRetentionWithColdStorageOffRunsHotAndReportsTheColdRows(t *testing.T) {
	r := newColdRetentionRig(t)
	r.row("cpu", "testdb/cpu/2020/01/01/00/a.parquet")
	r.row("cpu", "testdb/cpu/2020/01/01/01/b.parquet")
	r.tier.backend = nil
	hot := r.parquet(t, r.hotDir, "testdb/logs/2020/01/01/00/hot.parquet", expiredTS)

	res, err := r.h.ExecutePolicy(context.Background(), r.policy(t, ""))
	if err != nil {
		t.Fatalf("ExecutePolicy failed with cold storage off: %v", err)
	}
	if exists(hot) {
		t.Error("expired hot file left behind")
	}
	if res.ColdFilesSkipped != 2 || !strings.Contains(res.ColdSkippedReason, "not enabled") {
		t.Errorf("cold_files_skipped = %d, reason %q; want both rows reported as not examined", res.ColdFilesSkipped, res.ColdSkippedReason)
	}
	if slices.Contains(res.AffectedMeasurements, "cpu") {
		t.Errorf("affected measurements = %v; cold-only measurements are not processed without cold storage", res.AffectedMeasurements)
	}
	if status, msg := r.lastStatus(t); status != "completed_with_errors" || !strings.Contains(msg, "not enabled") {
		t.Errorf("execution = %q %q, want completed_with_errors naming cold storage", status, msg)
	}
}

// An unreadable cold file and a quarantined row are counted and left alone;
// neither stops the files after them.
func TestColdRetentionCountsUnreadableAndQuarantinedFiles(t *testing.T) {
	r := newColdRetentionRig(t)
	const bad = "testdb/cpu/2020/01/01/00/bad.parquet"
	if err := os.MkdirAll(filepath.Join(r.coldDir, "testdb/cpu/2020/01/01/00"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.coldDir, filepath.FromSlash(bad)), []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.row("cpu", bad)
	q := r.cold(t, "cpu", "testdb/cpu/2020/01/01/01/quarantined.parquet", expiredTS)
	now := time.Now()
	r.tier.rows[len(r.tier.rows)-1].QuarantinedAt = &now
	expired := r.cold(t, "cpu", "testdb/cpu/2020/01/01/02/expired.parquet", expiredTS)

	res, err := r.h.ExecutePolicy(context.Background(), r.policy(t, ""))
	if err != nil {
		t.Fatalf("ExecutePolicy aborted on an unreadable cold file: %v", err)
	}
	if exists(expired) || !exists(q) {
		t.Errorf("expired deleted=%v quarantined kept=%v, want both true", !exists(expired), exists(q))
	}
	if res.ColdFilesSkipped != 2 || !strings.Contains(res.ColdSkippedReason, "quarantined") || !strings.Contains(res.ColdSkippedReason, "could not be checked, read or deleted") {
		t.Errorf("cold_files_skipped = %d, reason %q", res.ColdFilesSkipped, res.ColdSkippedReason)
	}
	if status, _ := r.lastStatus(t); status != "completed_with_errors" {
		t.Errorf("execution status = %q, want completed_with_errors", status)
	}
}

// A failed delete keeps the row only while the object may still be there.
func TestColdRetentionVerifiesAFailedDelete(t *testing.T) {
	for _, removeAnyway := range []bool{false, true} {
		t.Run("object_gone="+strconv.FormatBool(removeAnyway), func(t *testing.T) {
			r := newColdRetentionRig(t)
			const key = "testdb/cpu/2020/01/01/00/expired.parquet"
			r.cold(t, "cpu", key, expiredTS)
			// Delete fails, either leaving the object or after removing it (a
			// timed-out response to a delete that happened).
			r.h.coldDeleteHook = func(ctx context.Context, b storage.Backend, path string) error {
				if removeAnyway {
					if err := b.Delete(ctx, path); err != nil {
						return err
					}
				}
				return errors.New("simulated delete failure")
			}

			res, err := r.h.ExecutePolicy(context.Background(), r.policy(t, ""))
			if err != nil {
				t.Fatalf("ExecutePolicy: %v", err)
			}
			if removeAnyway {
				if !slices.Equal(r.tier.retired, []string{key}) || res.ColdFilesDeleted != 1 {
					t.Errorf("retired = %v, cold deleted = %d; a delete that removed the object must retire its row", r.tier.retired, res.ColdFilesDeleted)
				}
				return
			}
			if len(r.tier.retired) != 0 || res.ColdFilesSkipped != 1 || res.ColdFilesDeleted != 0 {
				t.Errorf("retired = %v, skipped = %d, deleted = %d; an object still there keeps its row and is reported", r.tier.retired, res.ColdFilesSkipped, res.ColdFilesDeleted)
			}
		})
	}
}

// A dry run reports what a real run would delete and changes nothing.
func TestColdRetentionDryRunChangesNothing(t *testing.T) {
	r := newColdRetentionRig(t)
	expired := r.cold(t, "cpu", "testdb/cpu/2020/01/01/00/expired.parquet", expiredTS)
	r.row("cpu", "testdb/cpu/2020/01/01/01/gone.parquet")
	// A stale cold row over a hot copy: a real run reverts it, a dry run not.
	r.row("cpu", "testdb/cpu/2020/01/01/02/both.parquet")
	r.parquet(t, r.hotDir, "testdb/cpu/2020/01/01/02/both.parquet", liveTS)
	id := r.policy(t, "")

	app := fiber.New()
	r.h.RegisterRoutes(app)
	req := httptest.NewRequest("POST", "/api/v1/retention/"+strconv.FormatInt(id, 10)+"/execute", strings.NewReader(`{"dry_run":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var res ExecuteRetentionResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	if res.ColdFilesDeleted != 1 || res.ColdRowsDeleted != 10 {
		t.Errorf("dry run response = %+v, want 1 cold file / 10 rows reported", res)
	}
	if !exists(expired) || len(r.tier.retired) != 0 || len(r.tier.reverted) != 0 || r.tier.notified != 0 {
		t.Errorf("dry run changed state: object=%v retired=%v reverted=%v notified=%d", exists(expired), r.tier.retired, r.tier.reverted, r.tier.notified)
	}
}

// A failed candidate query skips the cold tier for the run, reported, and
// hot retention still runs.
func TestColdRetentionDiscoveryErrorDoesNotStopHot(t *testing.T) {
	r := newColdRetentionRig(t)
	r.tier.listErr = errors.New("database is locked")
	hot := r.parquet(t, r.hotDir, "testdb/logs/2020/01/01/00/hot.parquet", expiredTS)

	res, err := r.h.ExecutePolicy(context.Background(), r.policy(t, ""))
	if err != nil {
		t.Fatalf("ExecutePolicy: %v", err)
	}
	if exists(hot) {
		t.Error("expired hot file left behind")
	}
	if !strings.Contains(res.ColdSkippedReason, "cold tier not processed: database is locked") {
		t.Errorf("cold_skipped_reason = %q", res.ColdSkippedReason)
	}
	if status, _ := r.lastStatus(t); status != "completed_with_errors" {
		t.Errorf("execution status = %q, want completed_with_errors", status)
	}
}

// A policy for one measurement leaves the others' cold files alone.
func TestColdRetentionRespectsThePolicyMeasurement(t *testing.T) {
	r := newColdRetentionRig(t)
	cpu := r.cold(t, "cpu", "testdb/cpu/2020/01/01/00/expired.parquet", expiredTS)
	mem := r.cold(t, "mem", "testdb/mem/2020/01/01/00/expired.parquet", expiredTS)

	if _, err := r.h.ExecutePolicy(context.Background(), r.policy(t, "cpu")); err != nil {
		t.Fatalf("ExecutePolicy: %v", err)
	}
	if exists(cpu) || !exists(mem) {
		t.Errorf("cpu deleted=%v mem kept=%v, want both true", !exists(cpu), exists(mem))
	}
}
