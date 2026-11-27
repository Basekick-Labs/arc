//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

func utcDB1825(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := pinTimeZoneUTC(db); err != nil {
		t.Fatal(err)
	}
	return db
}

func ts1825(s string) time.Time {
	v, err := time.Parse("2006-01-02 15:04:05.999999", s)
	if err != nil {
		panic(err)
	}
	return v.UTC()
}

// The range a job compares with its partition is the file's times as the
// compaction COPY would write them, whatever type the file stores them in.
func TestFileTimeRange1825(t *testing.T) {
	ctx := context.Background()
	db := utcDB1825(t)
	for _, tc := range []struct {
		name   string
		sel    string // a SELECT producing the file's rows
		lo, hi string // "" : no usable time
	}{
		{"timestamptz", `SELECT * FROM (VALUES (TIMESTAMPTZ '2026-04-11 14:10:00Z', 1), (TIMESTAMPTZ '2026-04-11 14:00:00Z', 2), (TIMESTAMPTZ '2026-04-11 14:59:59.999999Z', 3)) v(time, x)`,
			"2026-04-11 14:00:00", "2026-04-11 14:59:59.999999"},
		{"iso strings, naive and with a zone", `SELECT * FROM (VALUES ('2026-04-11 14:10:00', 1), ('2026-04-11T16:00:00+02:00', 2)) v(time, x)`,
			"2026-04-11 14:00:00", "2026-04-11 14:10:00"},
		{"epoch microseconds as a string", `SELECT * FROM (VALUES ('1775916000000000', 1), ('1775916600000000', 2)) v(time, x)`,
			"2026-04-11 14:00:00", "2026-04-11 14:10:00"},
		{"before 1970", `SELECT * FROM (VALUES (TIMESTAMPTZ '1969-12-31 23:30:00Z', 1)) v(time, x)`,
			"1969-12-31 23:30:00", "1969-12-31 23:30:00"},
		{"one null among values", `SELECT * FROM (VALUES (TIMESTAMPTZ '2026-04-11 14:10:00Z', 1), (NULL, 2)) v(time, x)`,
			"2026-04-11 14:10:00", "2026-04-11 14:10:00"},
		{"only nulls", `SELECT CAST(NULL AS TIMESTAMPTZ) AS time, 1 AS x`, "", ""},
		{"no time column", `SELECT 1 AS x, 'h' AS host`, "", ""},
		{"a string that is not a time", `SELECT 'garbage' AS time, 1 AS x`, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A directory named like a hive partition of the column: the read
			// must not take "time" from the path.
			path := filepath.Join(t.TempDir(), "time=2031", "it's a file.parquet")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			q := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET)`, tc.sel, escapeSQLPath(filepath.ToSlash(path)))
			if _, err := db.ExecContext(ctx, q); err != nil {
				t.Fatal(err)
			}
			lo, hi, found, err := fileTimeRange(ctx, db, path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.lo == "" {
				if found {
					t.Fatalf("found a range %v to %v, want none", lo, hi)
				}
				return
			}
			if !found || !lo.Equal(ts1825(tc.lo)) || !hi.Equal(ts1825(tc.hi)) {
				t.Fatalf("range = %v to %v (found %v), want %s to %s", lo, hi, found, tc.lo, tc.hi)
			}
		})
	}
}

// A file that cannot be read is an error, never a pass.
func TestFileTimeRangeUnreadableFileIsAnError1825(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.parquet")
	if err := os.WriteFile(path, []byte("PAR1 this is not a parquet file PAR1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, found, err := fileTimeRange(context.Background(), utcDB1825(t), path); err == nil {
		t.Fatalf("an unreadable file passed (found=%v); a file let through unread could be the misfiled one", found)
	}
}

// The window is the hour for the hourly tier and the UTC day for the daily
// tier, half-open.
func TestMisfiledInputsWindow1825(t *testing.T) {
	ctx := context.Background()
	db := utcDB1825(t)
	dir := t.TempDir()
	write := func(name string, times ...string) string {
		p := filepath.Join(dir, name)
		vals := ""
		for i, s := range times {
			if i > 0 {
				vals += ", "
			}
			vals += fmt.Sprintf("(TIMESTAMPTZ '%sZ', %d)", s, i)
		}
		q := fmt.Sprintf(`COPY (SELECT * FROM (VALUES %s) v(time, x)) TO '%s' (FORMAT PARQUET)`, vals, escapeSQLPath(filepath.ToSlash(p)))
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
		return p
	}
	files := map[string]string{
		"first-and-last":   write("a.parquet", "2026-04-11 14:00:00", "2026-04-11 14:59:59.999999"),
		"before-the-hour":  write("b.parquet", "2026-04-11 13:59:59.999999", "2026-04-11 14:30:00"),
		"the-next-hour":    write("c.parquet", "2026-04-11 14:30:00", "2026-04-11 15:00:00"),
		"the-next-day":     write("d.parquet", "2026-04-12 00:00:00"),
		"the-day-before":   write("e.parquet", "2026-04-10 23:59:59.999999"),
		"last-of-the-day":  write("f.parquet", "2026-04-11 23:59:59.999999"),
		"first-of-the-day": write("g.parquet", "2026-04-11 00:00:00"),
	}
	var names, paths []string
	for n, p := range files {
		names, paths = append(names, n), append(paths, p)
	}
	for _, tc := range []struct {
		tier       string
		at         string
		misfiled   map[string]bool // name -> OutsideDay
		checkOff   bool
		noPartTime bool
	}{
		{tier: "hourly", at: "2026-04-11 14:00:00", misfiled: map[string]bool{
			"before-the-hour": false, "the-next-hour": false, "the-next-day": true, "the-day-before": true,
			"last-of-the-day": false, "first-of-the-day": false}},
		{tier: "daily", at: "2026-04-11 00:00:00", misfiled: map[string]bool{"the-next-day": true, "the-day-before": true}},
		{tier: "hourly", at: "2026-04-11 14:00:00", checkOff: true},
		{tier: "hourly", noPartTime: true},
		{tier: "monthly", at: "2026-04-01 00:00:00"},
	} {
		j := &Job{Tier: tc.tier, checkInputWindow: !tc.checkOff}
		if !tc.noPartTime {
			j.PartitionTime = ts1825(tc.at)
		}
		got, err := j.misfiledInputs(ctx, db, paths, names)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, m := range got {
			outside, want := tc.misfiled[m.Path]
			if !want {
				t.Errorf("%s at %s: %s flagged (%v to %v), it is inside the partition", tc.tier, tc.at, m.Path, m.MinTime, m.MaxTime)
			} else if outside != m.OutsideDay {
				t.Errorf("%s at %s: %s OutsideDay = %v, want %v", tc.tier, tc.at, m.Path, m.OutsideDay, outside)
			}
			seen[m.Path] = true
		}
		for n := range tc.misfiled {
			if !seen[n] {
				t.Errorf("%s at %s: %s not flagged", tc.tier, tc.at, n)
			}
		}
	}
}

// writeInput1825 stores a one-row Arc-shaped file under key.
func writeInput1825(t *testing.T, db *sql.DB, backend storage.Backend, key, at string, value int) {
	t.Helper()
	ctx := context.Background()
	fixture := filepath.Join(t.TempDir(), "f.parquet")
	writeFixtureParquet(t, ctx, db, fixture, fmt.Sprintf("TIMESTAMPTZ '%sZ' AS time, 'h' AS host, %d.0 AS value", at, value))
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, key, data); err != nil {
		t.Fatal(err)
	}
}

// A job with an input holding rows of another day compacts nothing: no
// output, no input removed, no manifest, and it reports the file. With the
// check off it merges the file, as before.
func TestJobRunRefusesMisfiledInput1825(t *testing.T) {
	for _, tc := range []struct {
		name       string
		check      bool
		staged     bool
		strayAt    string
		outsideDay bool
	}{
		{"another day", true, false, "2026-04-12 05:00:00", true},
		{"another hour of the same day", true, false, "2026-04-11 09:00:00", false},
		{"another day, staged job", true, true, "2026-04-12 05:00:00", true},
		{"check off", false, false, "2026-04-12 05:00:00", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := utcDB1825(t)
			backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			const partition = "testdb/cpu/2026/04/11/14"
			keys := []string{partition + "/a.parquet", partition + "/b.parquet", partition + "/stray.parquet"}
			writeInput1825(t, db, backend, keys[0], "2026-04-11 14:00:01", 1)
			writeInput1825(t, db, backend, keys[1], "2026-04-11 14:00:02", 2)
			writeInput1825(t, db, backend, keys[2], tc.strayAt, 3)
			cfg := &JobConfig{
				Measurement: "cpu", PartitionPath: partition, Files: keys,
				StorageBackend: backend, Database: "testdb", Tier: "hourly",
				TempDirectory: t.TempDir(), Logger: zerolog.Nop(), DB: db,
				ManifestManager: NewManifestManager(backend, zerolog.Nop()), JobID: "misfiled-1825",
				PartitionTime: ts1825("2026-04-11 14:00:00"), CheckInputWindow: tc.check,
			}
			if tc.staged {
				cfg.Staged = true
				cfg.CompletionDir = t.TempDir()
			}
			job := NewJob(cfg)
			if err := job.Run(ctx); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if !tc.check {
				if job.FilesCompacted != 3 || len(job.MisfiledInputs()) != 0 {
					t.Fatalf("with the check off: compacted %d files, misfiled %v; want all 3 merged", job.FilesCompacted, job.MisfiledInputs())
				}
				return
			}
			got := job.MisfiledInputs()
			if len(got) != 1 || got[0].Path != keys[2] || got[0].OutsideDay != tc.outsideDay || !got[0].MinTime.Equal(ts1825(tc.strayAt)) {
				t.Fatalf("misfiled = %+v, want %s (outside the day: %v)", got, keys[2], tc.outsideDay)
			}
			if job.Status != JobStatusCompleted || job.FilesCompacted != 0 || job.OutputStorageKey != "" || len(job.CompactedInputKeys()) != 0 || job.BytesBefore != 0 {
				t.Fatalf("status %s, compacted %d, output %q, inputs %v, bytes %d: want a completed job that did nothing",
					job.Status, job.FilesCompacted, job.OutputStorageKey, job.CompactedInputKeys(), job.BytesBefore)
			}
			listed, err := backend.List(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			if len(listed) != 3 {
				t.Fatalf("storage holds %v, want exactly the three inputs: no output, no manifest, no staged file", listed)
			}
			if tc.staged {
				left, _ := os.ReadDir(cfg.CompletionDir)
				if len(left) != 0 {
					t.Fatalf("a completion record was left for a job that did nothing: %v", left[0].Name())
				}
			}
			if res := subprocessResult(job, nil); !res.Success || len(res.MisfiledInputs) != 1 || len(res.CompactedInputs) != 0 {
				t.Fatalf("subprocess result = %+v", res)
			}
		})
	}
}

// Every connection of the job's pool reads times in UTC, not only the one
// that ran the SET: the input check and the COPY may run on different ones.
func TestPinTimeZoneUTCCoversThePool1825(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SET GLOBAL TimeZone='Asia/Tokyo'"); err != nil { // stands for the host's zone
		t.Fatal(err)
	}
	if err := pinTimeZoneUTC(db); err != nil {
		t.Fatal(err)
	}
	var conns []*sql.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < 4; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		var zone string
		var micros int64
		if err := c.QueryRowContext(ctx, `SELECT current_setting('TimeZone'), epoch_us(TRY_CAST('2026-04-11 14:00:00' AS TIMESTAMPTZ))`).Scan(&zone, &micros); err != nil {
			t.Fatal(err)
		}
		if zone != "UTC" || micros != ts1825("2026-04-11 14:00:00").UnixMicro() {
			t.Fatalf("connection %d: zone %q, a naive string read as %v; want UTC on every connection", i, zone, time.UnixMicro(micros).UTC())
		}
	}
}

func TestMisfiledMemo1825(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_800_000_000, 0)
	m := newMisfiledMemo(dir, zerolog.Nop())
	inDay := MisfiledInput{Path: "db/cpu/2026/04/11/14/wrong-hour.parquet", MinTime: ts1825("2026-04-11 09:00:00"), MaxTime: ts1825("2026-04-11 09:00:00")}
	otherDay := MisfiledInput{Path: "db/cpu/2026/04/11/14/wrong-day.parquet", MinTime: ts1825("2026-04-12 05:00:00"), MaxTime: ts1825("2026-04-12 05:00:00"), OutsideDay: true}
	if n := m.record("hourly", "db/cpu/2026/04/11/14", []MisfiledInput{inDay, otherDay}, now); n != 3 {
		t.Fatalf("recorded %d entries, want 3: both for hourly, the other day's for daily too", n)
	}
	if n := m.record("hourly", "db/cpu/2026/04/11/14", []MisfiledInput{inDay}, now); n != 0 {
		t.Fatalf("a file already remembered was recorded again (%d)", n)
	}
	// The daily tier still takes a file that is only in the wrong hour: its
	// day-level file is read by every pruned query of the day.
	if m.has("daily", inDay.Path) || !m.has("daily", otherDay.Path) || !m.has("hourly", inDay.Path) {
		t.Fatal("daily must leave alone only the file whose rows leave the day")
	}
	files := []string{inDay.Path, "db/cpu/2026/04/11/14/ok.parquet", otherDay.Path}
	if kept, dropped := m.without("hourly", files); dropped != 2 || len(kept) != 1 || kept[0] != files[1] {
		t.Fatalf("hourly without = %v (%d dropped)", kept, dropped)
	}
	if kept, dropped := m.without("daily", files); dropped != 1 || len(kept) != 2 {
		t.Fatalf("daily without = %v (%d dropped)", kept, dropped)
	}
	if m.files() != 2 || len(m.list()) != 3 {
		t.Fatalf("files %d, entries %d; want 2 and 3", m.files(), len(m.list()))
	}

	reloaded := newMisfiledMemo(dir, zerolog.Nop())
	if len(reloaded.list()) != 3 || !reloaded.has("daily", otherDay.Path) || !reloaded.list()[0].FoundAt.Equal(now) {
		t.Fatalf("after a restart the memo holds %+v", reloaded.list())
	}
	if n := reloaded.forget("hourly", otherDay.Path); n != 1 || !reloaded.has("daily", otherDay.Path) {
		t.Fatalf("forget one tier of one path removed %d", n)
	}
	if n := reloaded.forget("", ""); n != 2 || reloaded.files() != 0 {
		t.Fatalf("forget all removed %d", n)
	}
	if again := newMisfiledMemo(dir, zerolog.Nop()); len(again.list()) != 0 {
		t.Fatalf("a cleared memo came back after a restart: %+v", again.list())
	}

	var none *misfiledMemo
	if none.has("hourly", "p") || none.files() != 0 || none.forgetPaths(files) != 0 || none.record("hourly", "p", []MisfiledInput{inDay}, now) != 0 {
		t.Fatal("nil memo")
	}
	if kept, dropped := none.without("hourly", files); dropped != 0 || len(kept) != 3 {
		t.Fatal("nil memo dropped files")
	}
}

// refusingSubprocess1825 stands for the subprocess: a job that holds stray
// refuses it and compacts nothing; any other job compacts its files.
func refusingSubprocess1825(t *testing.T, stray string, outsideDay bool) *[][]string {
	t.Helper()
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	var jobs [][]string
	runJobInSubprocess = func(_ context.Context, cfg *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		jobs = append(jobs, append([]string(nil), cfg.Files...))
		if !cfg.CheckInputWindow {
			t.Errorf("the job was not asked to check its inputs")
		}
		for _, f := range cfg.Files {
			if f == stray {
				return &SubprocessJobResult{Success: true, MisfiledInputs: []MisfiledInput{{Path: stray, OutsideDay: outsideDay}}}, nil
			}
		}
		return &SubprocessJobResult{Success: true, FilesCompacted: len(cfg.Files), CompactedInputs: cfg.Files, SourcesDeleted: true}, nil
	}
	return &jobs
}

// The next cycle compacts the partition's other files without the refused
// one, and a cycle after that has nothing to do; the file is not offered to a
// job again, also after a restart.
func TestRefusedFileIsLeftOutOfLaterJobs1825(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	part := "db/cpu/2026/01/01/03"
	stray := part + "/stray.parquet"
	files := []string{part + "/a.parquet", part + "/b.parquet", stray}
	for _, f := range files {
		if err := b.Write(ctx, f, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	left := append([]string(nil), files...)
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		c := candidateIssue915(part)
		c.Files, c.FileCount = append([]string(nil), left...), len(left)
		return []Candidate{c}, nil
	}}}
	jobs := refusingSubprocess1825(t, stray, true)
	before := metrics.Get().Snapshot()["compaction_inputs_misfiled"].(int64)

	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(*jobs) != 1 || len((*jobs)[0]) != 3 {
		t.Fatalf("cycle 1 ran %v, want one job of the three files", *jobs)
	}
	if got := m.MisfiledFiles(); len(got) != 2 || got[0].Path != stray {
		t.Fatalf("remembered %+v, want the stray file for hourly and daily", got)
	}
	snap := metrics.Get().Snapshot()
	if snap["compaction_inputs_misfiled"].(int64) != before+1 || snap["compaction_files_misfiled"].(int64) != 1 {
		t.Fatalf("counter %v (was %d), gauge %v; want +1 and 1", snap["compaction_inputs_misfiled"], before, snap["compaction_files_misfiled"])
	}

	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(*jobs) != 2 || len((*jobs)[1]) != 2 || (*jobs)[1][0] == stray || (*jobs)[1][1] == stray {
		t.Fatalf("cycle 2 ran %v, want a second job of the two other files", *jobs)
	}
	left = []string{part + "/out_compacted.parquet", stray} // what the hour holds now
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(*jobs) != 2 {
		t.Fatalf("cycle 3 ran another job (%v): one file besides the refused one is not a job", (*jobs)[2])
	}

	// A restart: the memo is read back.
	m.misfiled = newMisfiledMemo(m.TempDirectory, zerolog.Nop())
	left = append([]string(nil), files...)
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(*jobs) != 3 || len((*jobs)[2]) != 2 {
		t.Fatalf("after a restart the cycle ran %v, want a job without the refused file", (*jobs)[2:])
	}

	// Cleared: read again.
	if n := m.ClearMisfiled("", stray); n != 2 {
		t.Fatalf("cleared %d entries, want 2", n)
	}
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(*jobs) != 4 || len((*jobs)[3]) != 3 {
		t.Fatalf("after clearing, the cycle ran %v, want the three files offered again", (*jobs)[3:])
	}
}

// A settled day's job that refused a file is not remembered as "ran, do not
// run again while the files stay the same": the day's next job is the rest of
// those same files. And a settled day whose every file is refused takes no
// job at all.
func TestRefusedFileInASettledDay1825(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	part := "db/cpu/2026/01/01"
	stray := part + "/04/stray.parquet"
	files := []string{part + "/03/a.parquet", stray}
	for _, f := range files {
		if err := b.Write(ctx, f, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	left := append([]string(nil), files...)
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		c := candidateIssue915(part)
		c.Settled, c.Files, c.FileCount = true, append([]string(nil), left...), len(left)
		return []Candidate{c}, nil
	}}}
	jobs := refusingSubprocess1825(t, stray, true)

	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(*jobs) != 2 || len((*jobs)[1]) != 1 || (*jobs)[1][0] != files[0] {
		t.Fatalf("jobs = %v, want the refused one, then the day's other file alone on the next cycle", *jobs)
	}
	left = []string{stray}
	for i := 0; i < 3; i++ {
		_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	}
	if len(*jobs) != 2 {
		t.Fatalf("a day holding only the refused file ran %d more job(s)", len(*jobs)-2)
	}
}

// What is left of a candidate after the refused files are dropped must still
// be a job.
func TestFilterMisfiledKeepsTheFloors1825(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	tier := cycleTierIssue915{}
	part := "db/cpu/2026/01/01/03"
	m.misfiled.record("hourly", part, []MisfiledInput{{Path: part + "/s1.parquet"}, {Path: part + "/s2.parquet"}}, time.Now())
	c := candidateIssue915(part)
	c.Files = []string{part + "/a.parquet", part + "/b.parquet", part + "/s1.parquet"}
	if got, ok := m.filterMisfiled(c, tier); !ok || len(got.Files) != 2 || got.FileCount != 2 {
		t.Fatalf("two files left: %v %v", got.Files, ok)
	}
	c.Files = []string{part + "/a.parquet", part + "/s1.parquet", part + "/s2.parquet"}
	if _, ok := m.filterMisfiled(c, tier); ok {
		t.Fatal("one file left of a regular candidate ran as a job")
	}
	c.Settled = true
	if got, ok := m.filterMisfiled(c, tier); !ok || len(got.Files) != 1 {
		t.Fatalf("a settled day's one remaining hour-level file must run: %v %v", got.Files, ok)
	}
	c.Files = []string{part + "/s1.parquet", part + "/s2.parquet"}
	if _, ok := m.filterMisfiled(c, tier); ok {
		t.Fatal("a candidate of refused files only ran")
	}
	// A settled day left with its own daily file only: rewriting it alone
	// would produce the same file every cycle.
	c.Files = []string{"db/cpu/2026/01/01/cpu_x_daily.parquet", part + "/s1.parquet"}
	if _, ok := m.filterMisfiled(c, tier); ok {
		t.Fatal("a settled day left with only its daily file ran as a job")
	}
	c.Files = []string{part + "/a.parquet", part + "/b.parquet"}
	if got, ok := m.filterMisfiled(c, tier); !ok || len(got.Files) != 2 {
		t.Fatal("a candidate without refused files was changed")
	}
}

// A remembered file that is gone from storage is forgotten; one that is still
// there stays, whether or not its partition is a candidate.
func TestPruneMisfiled1825(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	part := "db/cpu/2026/01/01/03"
	kept, gone := part+"/kept.parquet", part+"/gone.parquet"
	if err := b.Write(ctx, kept, []byte("x")); err != nil {
		t.Fatal(err)
	}
	m.misfiled.record("hourly", part, []MisfiledInput{{Path: kept, OutsideDay: true}, {Path: gone, OutsideDay: true}}, time.Now())
	m.pruneMisfiled(ctx)
	got := m.MisfiledFiles()
	if len(got) != 2 || got[0].Path != kept || got[1].Path != kept {
		t.Fatalf("after the prune the memo holds %+v, want both entries of the file still in storage", got)
	}
	if g := metrics.Get().Snapshot()["compaction_files_misfiled"].(int64); g != 1 {
		t.Fatalf("gauge = %d, want 1", g)
	}
}

// Settled days whose files are all refused do not take the places of the
// cycle's settled days: a day behind more of them than a cycle collects still
// gets its job.
func TestRefusedSettledDaysTakeNoSlot1825(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	var cands []Candidate
	var refused []MisfiledInput
	for i := 0; i < maxSettledCandidatesCollected+10; i++ {
		part := fmt.Sprintf("db/cpu/2020/01/01/s%d", i)
		c := candidateIssue915(part)
		c.Settled, c.Files, c.FileCount = true, []string{part + "/stray.parquet"}, 1
		cands = append(cands, c)
		refused = append(refused, MisfiledInput{Path: c.Files[0]})
	}
	m.misfiled.record("hourly", "", refused, time.Now())
	real := candidateIssue915("db/cpu/2020/01/02/real")
	real.Settled, real.Files, real.FileCount = true, []string{"db/cpu/2020/01/02/real/a.parquet"}, 1
	cands = append(cands, real)
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return cands, nil }}}
	jobs := refusingSubprocess1825(t, "", false)
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if len(*jobs) != 1 || (*jobs)[0][0] != real.Files[0] {
		t.Fatalf("jobs = %d, want the one day that has a file to compact", len(*jobs))
	}
}

// A column named "Time" is the time column to DuckDB, and the COPY merges its
// rows: the check reads it too.
func TestFileTimeRangeAnyCaseOfTheColumnName1825(t *testing.T) {
	ctx := context.Background()
	db := utcDB1825(t)
	path := filepath.Join(t.TempDir(), "f.parquet")
	q := fmt.Sprintf(`COPY (SELECT TIMESTAMPTZ '2030-01-01 00:00:00Z' AS "Time", 1 AS x) TO '%s' (FORMAT PARQUET)`, escapeSQLPath(filepath.ToSlash(path)))
	if _, err := db.ExecContext(ctx, q); err != nil {
		t.Fatal(err)
	}
	lo, _, found, err := fileTimeRange(ctx, db, path)
	if err != nil || !found || !lo.Equal(ts1825("2030-01-01 00:00:00")) {
		t.Fatalf("range starts %v (found %v, err %v), want the file's 2030 row", lo, found, err)
	}
}

// A day is judged on the files a job could take. With eleven good hour files
// and one refused, under daily_min_files = 12, the day is a settled day and
// gets its daily file; judged with the refused file it was a regular
// candidate that the filter turned down on every cycle, for good.
func TestDayIsClassifiedWithoutRefusedFiles1825(t *testing.T) {
	now := time.Now().UTC()
	d := now.AddDate(0, 0, -30)
	day := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	written := day.Add(36 * time.Hour)
	var objects []string
	for h := 0; h < 12; h++ {
		objects = append(objects, hourFile("db", "cpu", day, h, written.Add(time.Duration(h)*time.Second)))
	}
	stray := objects[11]

	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	tier := settledTier(12, 7, 24)
	m.Tiers = []Tier{tier}
	m.misfiled.record("daily", "db/cpu", []MisfiledInput{{Path: stray, OutsideDay: true}}, now)

	// As NewManager wires it.
	tier.SetExclude(func(p string) bool { return m.misfiled.has("daily", p) })
	got := tier.FindCandidatesFromListing("db", "cpu", objects)
	if len(got) != 1 || !got[0].Settled || len(got[0].Files) != 11 {
		t.Fatalf("candidates = %+v, want one settled day of the eleven other files", got)
	}
	for _, f := range got[0].Files {
		if f == stray {
			t.Fatal("the refused file is in the candidate")
		}
	}
	if c, ok := m.filterMisfiled(got[0], tier); !ok || len(c.Files) != 11 {
		t.Fatalf("the manager's filter turned the day down: %v", ok)
	}
	// The listing is not changed for its other readers.
	if len(objects) != 12 || objects[11] != stray {
		t.Fatal("the tier changed the caller's listing")
	}
	// Without the exclusion: a regular candidate of twelve, which the filter refuses.
	tier.SetExclude(nil)
	got = tier.FindCandidatesFromListing("db", "cpu", objects)
	if len(got) != 1 || got[0].Settled || len(got[0].Files) != 12 {
		t.Fatalf("without the exclusion: %+v", got)
	}
}

// NewManager gives its tiers the exclusion.
func TestNewManagerWiresTheTierExclusion1825(t *testing.T) {
	backend := localBackend(t)
	now := time.Now().UTC()
	d := now.AddDate(0, 0, -30)
	day := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	a, b, stray := hourFile("db", "cpu", day, 1, day.Add(30*time.Hour)), hourFile("db", "cpu", day, 2, day.Add(31*time.Hour)), hourFile("db", "cpu", day, 3, day.Add(32*time.Hour))
	for _, on := range []bool{true, false} {
		tier := settledTier(12, 7, 24)
		m := NewManager(&ManagerConfig{StorageBackend: backend, LockManager: NewLockManager(), TempDirectory: t.TempDir(),
			Logger: zerolog.Nop(), Tiers: []Tier{tier}, MergeMisfiledInputs: !on})
		m.misfiled.record("daily", "p", []MisfiledInput{{Path: stray}}, now)
		got := tier.FindCandidatesFromListing("db", "cpu", []string{a, b, stray})
		want := 3
		if on {
			want = 2
		}
		if len(got) != 1 || len(got[0].Files) != want {
			t.Fatalf("check on=%v: candidate files = %+v, want %d", on, got, want)
		}
	}
}

// With compaction.refuse_misfiled_inputs = false the list is neither read nor
// used: files remembered by an earlier run are compacted like any other, the
// job is not asked to check, and the list on disk is left as it was.
func TestCheckOffIgnoresTheList1825(t *testing.T) {
	backend := localBackend(t)
	temp := t.TempDir()
	part := "db/cpu/2026/01/01/03"
	stray := part + "/stray.parquet"
	earlier := newMisfiledMemo(temp, zerolog.Nop())
	earlier.record("hourly", part, []MisfiledInput{{Path: stray, OutsideDay: true}}, time.Now())
	saved, err := os.ReadFile(filepath.Join(temp, misfiledFile))
	if err != nil {
		t.Fatal(err)
	}

	m := NewManager(&ManagerConfig{StorageBackend: backend, LockManager: NewLockManager(), TempDirectory: temp,
		Logger: zerolog.Nop(), MergeMisfiledInputs: true, MaxFilesPerBatch: 10})
	if len(m.MisfiledFiles()) != 0 || metrics.Get().Snapshot()["compaction_files_misfiled"].(int64) != 0 {
		t.Fatalf("with the check off the list reads %+v", m.MisfiledFiles())
	}
	files := []string{part + "/a.parquet", part + "/b.parquet", stray}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		c := candidateIssue915(part)
		c.Files, c.FileCount = files, len(files)
		return []Candidate{c}, nil
	}}}
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	var jobs []*SubprocessJobConfig
	runJobInSubprocess = func(_ context.Context, cfg *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		jobs = append(jobs, cfg)
		return &SubprocessJobResult{Success: true, FilesCompacted: len(cfg.Files), CompactedInputs: cfg.Files, SourcesDeleted: true}, nil
	}
	_, _ = m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"})
	if len(jobs) != 1 || len(jobs[0].Files) != 3 || jobs[0].CheckInputWindow {
		t.Fatalf("jobs = %+v, want one job of all three files, not asked to check", jobs)
	}
	if now, _ := os.ReadFile(filepath.Join(temp, misfiledFile)); string(now) != string(saved) {
		t.Fatal("the list on disk was changed with the check off")
	}
}

// A job that refused its inputs is no evidence that its partition fits in a
// job's memory: the too-wide strike the partition holds stays.
func TestRefusedJobDoesNotClearATooWideStrike1825(t *testing.T) {
	for _, refuse := range []bool{true, false} {
		m, b, cleanup := setupTestManager(t)
		ctx := context.Background()
		part := "db/cpu/2026/01/01/03"
		stray := part + "/stray.parquet"
		files := []string{part + "/a.parquet", part + "/b.parquet", stray}
		for _, f := range files {
			if err := b.Write(ctx, f, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		m.Tiers = []Tier{cycleTierIssue915{
			Tier: NewHourlyTier(&HourlyTierConfig{StorageBackend: b, MinAgeHours: 1, MinFiles: 2, Enabled: true, Logger: zerolog.Nop()}),
			find: func(context.Context, string, string) ([]Candidate, error) {
				c := candidateIssue915(part)
				c.Files, c.FileCount = files, len(files)
				return []Candidate{c}, nil
			}}}
		// One strike of a kind that needs two: the partition is still tried.
		m.tooWide.strike("hourly", part, files, "killed", m.TooWideSettings())
		target := ""
		if refuse {
			target = stray
		}
		refusingSubprocess1825(t, target, true)
		_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
		if kept := len(m.TooWidePartitions()) == 1; kept != refuse {
			t.Fatalf("job refused=%v: the strike is kept=%v", refuse, kept)
		}
		cleanup()
	}
}

// A staged job that refuses is remembered too: the staged path returns early
// in compactPartition, after the refusal is recorded.
func TestStagedJobRefusalIsRemembered1825(t *testing.T) {
	backend := localBackend(t)
	m := activeJobsRig(t, backend)
	ctx := context.Background()
	a := "db/cpu/2026/01/01/03/cpu_20260102_020000_000000001.parquet"
	stray := "db/cpu/2026/01/01/03/cpu_20260102_020000_000000002.parquet"
	for _, f := range []string{a, stray} {
		if err := backend.Write(ctx, f, []byte("12345")); err != nil {
			t.Fatal(err)
		}
	}
	m.SetStagedCommit(func() bool { return true },
		func(string) (int64, string, bool) { return 5, "sha", true },
		func(string) bool { return false })
	refusingSubprocess1825(t, stray, true)
	c := Candidate{Database: "db", Measurement: "cpu", PartitionPath: "cpu/2026/01/01/03", Tier: "hourly", BatchNumber: 1, Files: []string{a, stray}}
	if err := m.compactPartition(ctx, c, jobAttribution{}); err != nil {
		t.Fatal(err)
	}
	if !m.misfiled.has("hourly", stray) || !m.misfiled.has("daily", stray) {
		t.Fatalf("a staged job's refusal was not remembered: %+v", m.MisfiledFiles())
	}
}

// The list is checked against storage a window at a time, and every file gets
// its turn: files past the first window are reached on a later cycle.
func TestPruneMisfiledTakesEveryFileInTurn1825(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	const n = misfiledPrunePerCycle + 50
	var in []MisfiledInput
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("db/cpu/2026/01/01/03/f%04d.parquet", i)
		in = append(in, MisfiledInput{Path: p})
		if i < misfiledPrunePerCycle { // the first window's files are still in storage
			if err := b.Write(ctx, p, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
	}
	m.misfiled.record("hourly", "db/cpu/2026/01/01/03", in, time.Now())
	m.pruneMisfiled(ctx)
	if got := m.misfiled.files(); got != n {
		t.Fatalf("after the first window the list holds %d files, want all %d (its files exist)", got, n)
	}
	m.pruneMisfiled(ctx)
	if got := m.misfiled.files(); got != misfiledPrunePerCycle {
		t.Fatalf("after the second window the list holds %d files, want the %d still in storage", got, misfiledPrunePerCycle)
	}
	// Round again: one of the first window's files goes, and is forgotten.
	if err := b.Delete(ctx, in[3].Path); err != nil {
		t.Fatal(err)
	}
	m.pruneMisfiled(ctx)
	m.pruneMisfiled(ctx)
	if m.misfiled.has("hourly", in[3].Path) {
		t.Fatal("a file removed after its window was checked was never checked again")
	}
}

// A full list: the files of a job it has no room for are not remembered, and
// a settled day's job is then recorded like any job that ran, so it is not
// refused again on every cycle.
func TestFullListRecordsTheSettledJob1825(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	for i := 0; i < maxMisfiledEntries; i++ {
		m.misfiled.entries[misfiledKey("hourly", fmt.Sprintf("x/%d", i))] = MisfiledEntry{Tier: "hourly", Path: fmt.Sprintf("x/%d", i)}
	}
	part := "db/cpu/2026/01/01"
	stray := part + "/04/stray.parquet"
	files := []string{part + "/03/a.parquet", stray}
	for _, f := range files {
		if err := b.Write(ctx, f, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		c := candidateIssue915(part)
		c.Settled, c.Files, c.FileCount = true, files, len(files)
		return []Candidate{c}, nil
	}}}
	jobs := refusingSubprocess1825(t, stray, false)
	for i := 0; i < 3; i++ {
		_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	}
	if m.misfiled.has("hourly", stray) {
		t.Fatal("a full list took another file")
	}
	if len(*jobs) != 1 {
		t.Fatalf("the settled day's job ran %d times over 3 cycles, want 1", len(*jobs))
	}
}

// A large list is written once per cycle, not on every change.
func TestLargeListIsWrittenOnFlush1825(t *testing.T) {
	dir := t.TempDir()
	m := newMisfiledMemo(dir, zerolog.Nop())
	var in []MisfiledInput
	for i := 0; i < misfiledSaveEachChange+10; i++ {
		in = append(in, MisfiledInput{Path: fmt.Sprintf("p/%d", i)})
	}
	m.record("daily", "p", in, time.Now())
	if _, err := os.Stat(filepath.Join(dir, misfiledFile)); err == nil {
		t.Fatal("a list over the size written on each change was written at once")
	}
	m.flush()
	if got := len(newMisfiledMemo(dir, zerolog.Nop()).list()); got != len(in) {
		t.Fatalf("after flush a restart reads %d entries, want %d", got, len(in))
	}
}
