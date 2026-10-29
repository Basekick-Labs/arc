package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

// A DELETE reads only the files its time range can touch, and counts them in
// bounded chunks (delete_window.go).

type timedRow struct {
	at   string // UTC, 'YYYY-MM-DD HH:MM:SS'
	host string
}

// windowRig is the cold rig (hot and cold storage, the handler on a router)
// with time pruning on, as the default configuration has it.
func windowRig(t *testing.T) *coldDeleteRig {
	t.Helper()
	r := newColdDeleteRig(t)
	r.h.config.TimePruning = true
	return r
}

// timed writes key under root with a time column, as ingest does.
func timed(t *testing.T, r *coldDeleteRig, root, key string, rows ...timedRow) {
	t.Helper()
	full := filepath.Join(root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	vals := make([]string, len(rows))
	for i, row := range rows {
		vals[i] = fmt.Sprintf("(TIMESTAMP '%s', '%s', %d)", row.at, row.host, i)
	}
	q := fmt.Sprintf("COPY (SELECT * FROM (VALUES %s) AS v(time, host, v)) TO '%s' (FORMAT PARQUET)", strings.Join(vals, ", "), full)
	if _, err := r.db.DB().Exec(q); err != nil {
		t.Fatal(err)
	}
}

// corrupt writes a file no read can open: a DELETE that reads it fails.
func corrupt(t *testing.T, root, key string) {
	t.Helper()
	full := filepath.Join(root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("not a parquet file"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (r *coldDeleteRig) rows(key string) int {
	r.t.Helper()
	return countParquet(r.t, r.db, filepath.Join(r.root, key), "true")
}

const windowWhere = `time >= '2026-03-10 12:40:00' AND time < '2026-03-10 12:50:00'`

func windowBody(where string) string {
	return fmt.Sprintf(`{"database":"db","measurement":"cpu","where":%q,"confirm":true}`, where)
}

// Only the window's partitions are read: files in hours and days outside it
// are never opened (each is unreadable here, so reading one fails the
// DELETE), and the rows in the window are deleted, in an hour directory and
// in a daily compaction output.
func TestDeleteReadsOnlyThePartitionsOfItsTimeRange(t *testing.T) {
	r := windowRig(t)
	in := "db/cpu/2026/03/10/12/cpu_20260310_120000_000000001.parquet"
	daily := "db/cpu/2026/03/10/cpu_20260310_daily.parquet"
	timed(t, r, r.root, in, timedRow{"2026-03-10 12:30:00", "a"}, timedRow{"2026-03-10 12:45:00", "b"})
	timed(t, r, r.root, daily, timedRow{"2026-03-10 12:41:00", "c"}, timedRow{"2026-03-10 18:00:00", "d"})
	corrupt(t, r.root, "db/cpu/2026/03/10/15/cpu_20260310_150000_000000001.parquet") // past the end + pad
	corrupt(t, r.root, "db/cpu/2026/03/10/10/cpu_20260310_100000_000000001.parquet") // before the start - pad
	corrupt(t, r.root, "db/cpu/2026/03/09/cpu_20260309_daily.parquet")               // the day before
	code, out := r.delete(windowBody(windowWhere))
	if code != 200 || out.DeletedCount != 2 {
		t.Fatalf("got %d %+v, want 200 with the two rows in the window deleted", code, out)
	}
	if n := countParquet(t, r.db, filepath.Join(r.root, "db/cpu/2026/03/10/12/*.parquet"), "true"); n != 1 {
		t.Fatalf("hour directory holds %d rows, want 1", n)
	}

	// Pruning off reads every file again, the unreadable ones included.
	r.h.config.TimePruning = false
	if code, out = r.delete(windowBody(windowWhere)); code != 500 {
		t.Fatalf("pruning off: got %d %+v, want 500 from an unreadable file", code, out)
	}
}

// A file the count cannot read stops the DELETE before anything is
// rewritten. Before, it was skipped: its matching rows stayed, and the DELETE
// answered success.
func TestDeleteStopsOnAFileItCannotCount(t *testing.T) {
	r := windowRig(t)
	good := "db/cpu/2026/03/10/12/cpu_20260310_120000_000000001.parquet"
	bad := "db/cpu/2026/03/10/12/cpu_20260310_120500_000000001.parquet"
	timed(t, r, r.root, good, timedRow{"2026-03-10 12:45:00", "a"})
	corrupt(t, r.root, bad)
	code, out := r.delete(windowBody(windowWhere))
	if code != 500 || out.DeletedCount != 0 || !strings.Contains(out.Error, filepath.Base(bad)) {
		t.Fatalf("got %d %+v, want 500 naming the unreadable file", code, out)
	}
	if n := r.rows(good); n != 1 {
		t.Fatalf("the readable file holds %d rows, want it untouched (1)", n)
	}
}

// A start-only WHERE reaches a file stamped in the future: the open end is
// not the query path's assumed now + 1 day.
func TestDeleteWithOnlyAStartReachesLaterPartitions(t *testing.T) {
	r := windowRig(t)
	later := "db/cpu/2099/01/01/00/cpu_20990101_000000_000000001.parquet"
	timed(t, r, r.root, later, timedRow{"2099-01-01 00:10:00", "a"})
	corrupt(t, r.root, "db/cpu/2026/03/10/10/cpu_20260310_100000_000000001.parquet")
	code, out := r.delete(windowBody(`time >= '2026-03-10 12:00:00'`))
	if code != 200 || out.DeletedCount != 1 {
		t.Fatalf("got %d %+v, want the future row deleted and the earlier file not read", code, out)
	}
}

// An end-only WHERE reaches the oldest partition.
func TestDeleteWithOnlyAnEndReachesEarlierPartitions(t *testing.T) {
	r := windowRig(t)
	// Before the query path's 1970 floor (and its pad): the open start is not
	// the floor.
	old := "db/cpu/1969/12/31/21/cpu_19691231_210000_000000001.parquet"
	timed(t, r, r.root, old, timedRow{"1969-12-31 21:10:00", "a"})
	corrupt(t, r.root, "db/cpu/2026/03/10/15/cpu_20260310_150000_000000001.parquet")
	code, out := r.delete(windowBody(`time < '2026-03-10 12:00:00'`))
	if code != 200 || out.DeletedCount != 1 {
		t.Fatalf("got %d %+v, want the old row deleted and the later file not read", code, out)
	}
}

// A WHERE the extractor cannot prove a range for reads every file.
func TestDeleteWithoutAProvableRangeReadsEveryFile(t *testing.T) {
	r := windowRig(t)
	timed(t, r, r.root, "db/cpu/2026/03/10/12/cpu_20260310_120000_000000001.parquet", timedRow{"2026-03-10 12:45:00", "a"})
	corrupt(t, r.root, "db/cpu/2026/03/10/15/cpu_20260310_150000_000000001.parquet")
	if code, out := r.delete(windowBody(`(` + windowWhere + `) OR host = 'zzz'`)); code != 500 {
		t.Fatalf("got %d %+v, want every file read (500 from the unreadable one)", code, out)
	}
}

// The count runs in chunks of deleteScanChunk files.
func TestDeleteCountsInBoundedChunks(t *testing.T) {
	r := windowRig(t)
	old := deleteScanChunk
	deleteScanChunk = 2
	t.Cleanup(func() { deleteScanChunk = old; onDeleteCountQuery = nil })
	for i := range 5 {
		timed(t, r, r.root, fmt.Sprintf("db/cpu/2026/03/10/12/cpu_20260310_12000%d_000000001.parquet", i), timedRow{"2026-03-10 12:45:00", "a"})
	}
	var reads []int
	onDeleteCountQuery = func(n int) { reads = append(reads, n) }
	code, out := r.delete(windowBody(windowWhere))
	if code != 200 || out.DeletedCount != 5 {
		t.Fatalf("got %d %+v, want 5 rows deleted", code, out)
	}
	if fmt.Sprint(reads) != "[2 2 1]" {
		t.Fatalf("counting queries read %v files, want [2 2 1]", reads)
	}
}

// A chunk whose files cannot bind the WHERE on their own is counted with a
// chunk that can (the schema donor), as one query over every file read them
// before: here the files with code as a number in a LIKE that only the
// widened (text) union type binds. Read alone they would have been "no
// match", their rows left and the DELETE answering 200. The rewrite then
// reads each alone (as on main), so those files are reported as failed.
func TestDeleteCountsAFileThatCannotBindAloneWithADonor(t *testing.T) {
	r := windowRig(t)
	old := deleteScanChunk
	deleteScanChunk = 2
	t.Cleanup(func() { deleteScanChunk = old })
	write := func(key, code string) {
		full := filepath.Join(r.root, key)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		q := fmt.Sprintf("COPY (SELECT TIMESTAMP '2026-03-10 12:45:00' AS time, %s AS code) TO '%s' (FORMAT PARQUET)", code, full)
		if _, err := r.db.DB().Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	write("db/cpu/2026/03/10/12/cpu_a1.parquet", "42::BIGINT")
	write("db/cpu/2026/03/10/12/cpu_a2.parquet", "44::BIGINT")
	write("db/cpu/2026/03/10/12/cpu_b.parquet", "'43'")
	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"` + windowWhere + ` AND code LIKE '4%'","dry_run":true}`)
	if code != 200 || out.DeletedCount != 3 {
		t.Fatalf("dry run: got %d %+v, want all 3 rows counted", code, out)
	}
}

// A column newer than every file in the range matches nothing there: no
// donor, and not a failure (as for cold files since #1363).
func TestDeleteOfAColumnNoFileInRangeHasMatchesNothing(t *testing.T) {
	r := windowRig(t)
	timed(t, r, r.root, "db/cpu/2026/03/10/12/cpu_20260310_120000_000000001.parquet", timedRow{"2026-03-10 12:45:00", "a"})
	code, out := r.delete(windowBody(windowWhere + ` AND region = 'eu'`))
	if code != 200 || out.DeletedCount != 0 {
		t.Fatalf("got %d %+v, want 200 with nothing deleted", code, out)
	}
}

// A file listed and gone by the time it is counted (a compaction replaced
// it) is errScanFileChanged, which the handler answers 503 retry, not 500.
func TestDeleteCountOfAFileGoneSinceTheListingIsRetryable(t *testing.T) {
	r := windowRig(t)
	here := "db/cpu/2026/03/10/12/cpu_here.parquet"
	gone := "db/cpu/2026/03/10/12/cpu_gone.parquet"
	timed(t, r, r.root, here, timedRow{"2026-03-10 12:45:00", "a"})
	files := []fileInfo{
		{queryPath: filepath.Join(r.root, here), relativePath: here},
		{queryPath: filepath.Join(r.root, gone), relativePath: gone},
	}
	_, err := r.h.countMatchingRowsInFiles(context.Background(), files, windowWhere)
	if !errors.Is(err, errScanFileChanged) {
		t.Fatalf("got %v, want errScanFileChanged", err)
	}
}

// Cold: only the window's objects are counted, and a cold row for an object
// outside the window, which is there, is not taken for one whose object is
// gone.
func TestDeleteColdReadsOnlyTheWindowAndKeepsOtherRows(t *testing.T) {
	r := windowRig(t)
	in := "db/cpu/2026/03/10/12/cpu_cold_in.parquet"
	out := "db/cpu/2025/01/01/00/cpu_cold_out.parquet"
	timed(t, r, r.coldRoot, in, timedRow{"2026-03-10 12:45:00", "a"})
	r.tier.rows = append(r.tier.rows, tiering.FileMetadata{Path: in, Database: "db", Measurement: "cpu", Tier: tiering.TierCold})
	corrupt(t, r.coldRoot, out)
	r.tier.rows = append(r.tier.rows, tiering.FileMetadata{Path: out, Database: "db", Measurement: "cpu", Tier: tiering.TierCold})
	code, resp := r.delete(windowBody(windowWhere))
	if code != 200 || len(resp.ColdDeletedFiles) != 1 || r.coldExists(in) {
		t.Fatalf("got %d %+v, want the in-window cold file deleted", code, resp)
	}
	if !r.coldExists(out) || len(r.tier.retired) != 1 || r.tier.retired[0] != in {
		t.Fatalf("retired %v, want only the deleted file's row", r.tier.retired)
	}
	// The row of the object outside the window is not taken for a missing
	// one (runCold would stat it again, at a cost per row).
	if c := r.h.scanCold(context.Background(), "db", "cpu", windowWhere, r.h.windowFor("db", "cpu", windowWhere, time.Now())); slices.Contains(c.missingRows, out) {
		t.Fatalf("missing rows %v, want not %s", c.missingRows, out)
	}
}

// With compaction commit on, a manifest file outside the window that is not
// on this node is not waited for.
func TestDeleteDoesNotWaitForManifestFilesOutsideItsRange(t *testing.T) {
	shortDeleteWait(t)
	far := "db/cpu/2026/10/11/15/cpu_far.parquet"
	_, h, root := casCluster(t, casA, far)
	h.config.TimePruning = true
	if err := os.Remove(filepath.Join(root, far)); err != nil {
		t.Fatal(err)
	}
	// casA has no time column: the clause binds no file and matches nothing,
	// which is all this needs: whether the scan waits.
	body := windowBody(`time >= '2026-10-11 10:00:00' AND time < '2026-10-11 10:30:00' AND host = 'h1'`)
	if code, out, raw := sendDelete(t, h, body); code != 200 || len(out.UnreachableFiles) != 0 {
		t.Fatalf("got %d %s, want 200 without waiting for %s", code, raw, far)
	}
	h.config.TimePruning = false
	if code, _, raw := sendDelete(t, h, body); code != 503 {
		t.Fatalf("pruning off: got %d %s, want 503 waiting for %s", code, raw, far)
	}
}

// A recorded DELETE pins now(): the window is read at the pinned instant,
// from the WHERE as written (the pinned literal is a form the extractor does
// not read).
func TestDeleteWindowReadsNowAtThePinnedInstant(t *testing.T) {
	h := &DeleteHandler{config: &config.DeleteConfig{TimePruning: true}}
	pinned := time.Date(2026, 3, 10, 12, 30, 0, 0, time.UTC)
	o := scanOpts{windowWhere: `time >= now() - INTERVAL 1 HOUR`, windowNow: pinned}
	w := h.windowFor("db", "cpu", o.whereForWindow("ignored"), o.nowForWindow())
	if w == nil || !w.hasStart || w.hasEnd || !w.start.Equal(pinned.Add(-time.Hour-deleteWindowPad)) {
		t.Fatalf("window %+v, want start at the pinned now - 1h - pad, no end", w)
	}
	pinnedWhere, err := pinDeleteTime(o.windowWhere, pinned)
	if err != nil {
		t.Fatal(err)
	}
	if h.windowFor("db", "cpu", pinnedWhere, pinned) != nil {
		t.Fatal("the pinned form now reads a window: windowWhere may no longer be needed")
	}
}

func TestDeleteWindowKeeps(t *testing.T) {
	h := &DeleteHandler{config: &config.DeleteConfig{TimePruning: true}}
	w := h.windowFor("db", "cpu", windowWhere, time.Now())
	cases := map[string]bool{
		"db/cpu/2026/03/10/12/f.parquet":       true,
		"db/cpu/2026/03/10/11/f.parquet":       true, // inside the pad
		"db/cpu/2026/03/10/13/f.parquet":       true, // inside the pad
		"db/cpu/2026/03/10/10/f.parquet":       false,
		"db/cpu/2026/03/10/14/f.parquet":       false,
		"db/cpu/2026/03/10/f.parquet":          true, // daily
		"db/cpu/2026/03/09/f.parquet":          false,
		"db/cpu/2026/03/11/f.parquet":          false,
		"db/cpu/2026/02/30/12/f.parquet":       true, // not a date: kept
		"db/cpu/2026/3/10/12/f.parquet":        true, // not the layout: kept
		"db/cpu/f.parquet":                     true,
		"db/cpu/2026/03/10/12/x/f.parquet":     true,
		"spoke/db/cpu/2026/03/10/15/f.parquet": true, // another prefix
		"db/cpu2/2026/03/10/15/f.parquet":      true,
	}
	for p, want := range cases {
		if got := w.keeps(p); got != want {
			t.Errorf("keeps(%s) = %v, want %v", p, got, want)
		}
	}
	if (*deleteWindow)(nil).keeps("db/cpu/2026/03/10/10/f.parquet") != true {
		t.Error("a nil window keeps everything")
	}
	for _, where := range []string{`host = 'a'`, windowWhere + ` OR host = 'a'`, `time = '2026-03-10 12:45:00'`} {
		if h.windowFor("db", "cpu", where, time.Now()) != nil {
			t.Errorf("windowFor(%s) read a window", where)
		}
	}
	off := &DeleteHandler{config: &config.DeleteConfig{TimePruning: false}}
	if off.windowFor("db", "cpu", windowWhere, time.Now()) != nil {
		t.Error("pruning off read a window")
	}
}

// The trade delete.time_pruning makes: a file Arc 25.12 or earlier placed by
// the hour it was flushed holds rows of other hours, which a pruned DELETE of
// those hours does not read. Off, every file is read again.
func TestDeleteTimePruningOffReachesAFilePlacedByFlushHour(t *testing.T) {
	r := windowRig(t)
	// Rows of 12:45 flushed, by an old Arc, into the 15:00 directory.
	late := "db/cpu/2026/03/10/15/cpu_20260310_150000_000000001.parquet"
	timed(t, r, r.root, late, timedRow{"2026-03-10 12:45:00", "a"})
	if code, out := r.delete(windowBody(windowWhere)); code != 200 || out.DeletedCount != 0 {
		t.Fatalf("pruning on: got %d %+v, want the misplaced row not read", code, out)
	}
	r.h.config.TimePruning = false
	if code, out := r.delete(windowBody(windowWhere)); code != 200 || out.DeletedCount != 1 {
		t.Fatalf("pruning off: got %d %+v, want the misplaced row deleted", code, out)
	}
}

// The scan after a compaction replaced a file under the DELETE finds the
// output unreadable: 207 naming it, not "retry to finish" (a retry fails the
// same way).
func TestDeleteRescanReportsAnUnreadableFile(t *testing.T) {
	raceUnreadableOutput = true
	t.Cleanup(func() { raceUnreadableOutput = false })
	code, raw, _, _ := raceDelete(t, "host = 'h1'", nil)
	if code != 207 || !strings.Contains(raw, "cpu_o.parquet") {
		t.Fatalf("got %d %s, want 207 naming the unreadable output", code, raw)
	}
}

// countInChunks, against a counter whose binding is scripted.
func TestCountInChunks(t *testing.T) {
	old := deleteScanChunk
	deleteScanChunk = 2
	t.Cleanup(func() { deleteScanChunk = old })
	binder := &duckdb.Error{Type: duckdb.ErrorTypeBinder, Msg: "Binder Error: no column"}
	io := errors.New("IO Error: gone")
	name := func(i int) string { return fmt.Sprint("f", i) }
	// bindsAlone[i]: the file binds on its own; any set holding a binder
	// file and no donor (a file in donors) fails to bind.
	run := func(n int, bindsAlone map[int]bool, donors map[int]bool, broken map[int]bool, failed *[]int) (map[int]int, []int, error) {
		count := func(_ context.Context, idx []int) (map[int]int, error) {
			hasDonor, needs := false, false
			for _, i := range idx {
				if broken[i] {
					return nil, io
				}
				hasDonor = hasDonor || donors[i]
				needs = needs || !bindsAlone[i]
			}
			if needs && !hasDonor {
				return nil, binder
			}
			out := map[int]int{}
			for _, i := range idx {
				out[i] = 1
			}
			return out, nil
		}
		fail := func(i int, err error) error {
			*failed = append(*failed, i)
			return fmt.Errorf("failed %d: %w", i, err)
		}
		return countInChunks(context.Background(), n, name, count, fail)
	}

	// Files 0,1 cannot bind alone; 2,3 bind and carry the column: 0,1 are
	// counted with them.
	var failed []int
	got, donor, err := run(4, map[int]bool{2: true, 3: true}, map[int]bool{2: true, 3: true}, nil, &failed)
	if err != nil || len(got) != 4 || fmt.Sprint(donor) != "[2 3]" {
		t.Fatalf("donor: %v %v %v", got, donor, err)
	}
	// No file can be a donor: nothing matches, no error.
	got, donor, err = run(3, nil, nil, nil, &failed)
	if err != nil || len(got) != 0 || donor != nil {
		t.Fatalf("no donor: %v %v %v", got, donor, err)
	}
	// A file that binds alone but cannot lend its columns (bindsAlone, not
	// a donor) to the others: the others fail with the donor too.
	_, _, err = run(3, map[int]bool{2: true}, nil, nil, &failed)
	var be *whereBindError
	if !errors.As(err, &be) {
		t.Fatalf("unbindable even with a donor: %v, want whereBindError", err)
	}
	// The donor breaks after it bound: the donor is reported, not the file
	// counted with it.
	failed = nil
	brokenLater := map[int]bool{}
	calls := 0
	count := func(_ context.Context, idx []int) (map[int]int, error) {
		calls++
		for _, i := range idx {
			if brokenLater[i] {
				return nil, io
			}
		}
		if len(idx) == 2 && idx[0] == 0 { // the first chunk, 0 and 1, does not bind
			return nil, binder
		}
		out := map[int]int{}
		for _, i := range idx {
			out[i] = 1
		}
		if idx[0] == 2 { // the second chunk bound: it is the donor, and goes
			brokenLater[2] = true
		}
		return out, nil
	}
	fail := func(i int, err error) error { failed = append(failed, i); return err }
	if _, _, err = countInChunks(context.Background(), 4, name, count, fail); err == nil || fmt.Sprint(failed) != "[2]" {
		t.Fatalf("donor gone: %v failed %v, want the donor (2) reported", err, failed)
	}
	// A cancelled context ends the count with its own error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := func(context.Context, []int) (map[int]int, error) {
		return nil, errors.New("INTERRUPT Error: Interrupted!")
	}
	if _, _, err = countInChunks(ctx, 2, name, cancelled, fail); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v, want context.Canceled", err)
	}
}

// failSecondCount makes the second scan of a DELETE fail with err.
func failSecondCount(t *testing.T, err error) { failCount(t, 2, err) }

// failCount makes the nth scan of a DELETE fail with err.
func failCount(t *testing.T, nth int, err error) {
	t.Helper()
	calls := 0
	countFiles = func(h *DeleteHandler, ctx context.Context, files []fileInfo, where string) ([]affectedFile, error) {
		calls++
		if calls == nth {
			return nil, err
		}
		return h.countMatchingRowsInFiles(ctx, files, where)
	}
	t.Cleanup(func() { countFiles = (*DeleteHandler).countMatchingRowsInFiles })
}

// A WHERE that cannot be applied to a file on the scan after a compaction
// replaced one: 207 naming it, as for an unreadable file (a retry fails the
// same way).
func TestDeleteRescanReportsAFileTheWhereCannotBind(t *testing.T) {
	failSecondCount(t, &whereBindError{path: "db/cpu/2026/10/11/10/cpu_o.parquet", err: errors.New("Binder Error")})
	code, raw, _, _ := raceDelete(t, "host = 'h1'", nil)
	if code != 207 || !strings.Contains(raw, "cpu_o.parquet") {
		t.Fatalf("got %d %s, want 207 naming the file", code, raw)
	}
}

// The same on a recorded DELETE's second scan: 400, not 500.
func TestDeleteRecordedSecondScanAnswersABindError400(t *testing.T) {
	failSecondCount(t, &whereBindError{path: "db/cpu/f.parquet", err: errors.New("Binder Error")})
	root, backend, db := recordsDuckDB(t)
	writeRecordsParquet(t, db, root, "db/cpu/2026/10/11/10/cpu_a.parquet")
	events := &[]string{}
	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	h.SetCoordinator(&recordingCoordinator{active: true, events: events})
	h.SetFlusher(&recordingFlusher{events: events})
	if code, _, raw := sendDelete(t, h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`); code != 400 || !strings.Contains(raw, "recorded, not run") {
		t.Fatalf("got %d %s, want 400", code, raw)
	}
}

// The donor a cold recount reads with leaves out files this run deleted.
func TestColdLiveDonorLeavesOutDeletedFiles(t *testing.T) {
	c := &coldPass{donor: []coldFile{{path: "a"}, {path: "b"}}, deleted: []string{"a"}}
	if d := c.liveDonor(); len(d) != 1 || d[0].path != "b" {
		t.Fatalf("live donor %v, want [b]", d)
	}
}

// On the first scan a WHERE no file can be read with answers 400: nothing ran.
func TestDeleteFirstScanAnswersABindError400(t *testing.T) {
	r := windowRig(t)
	timed(t, r, r.root, "db/cpu/2026/03/10/12/cpu_a.parquet", timedRow{"2026-03-10 12:45:00", "a"})
	failCount(t, 1, &whereBindError{path: "db/cpu/2026/03/10/12/cpu_a.parquet", err: errors.New("Binder Error")})
	if code, out := r.delete(windowBody(windowWhere)); code != 400 || !strings.Contains(out.Error, "cpu_a.parquet") {
		t.Fatalf("got %d %+v, want 400 naming the file", code, out)
	}
}
