package pruning

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestParseFileTime(t *testing.T) {
	cases := []struct {
		name   string
		file   string
		wantOK bool
		want   time.Time
	}{
		{"ingest format", "cpu_20260828_200802_765188000.parquet", true,
			time.Date(2026, 8, 28, 20, 8, 2, 0, time.UTC)},
		{"underscored measurement", "my_weird_2x_measure_20260828_200802_765188000.parquet", true,
			time.Date(2026, 8, 28, 20, 8, 2, 0, time.UTC)},
		{"hourly compacted (real format: ..._{unixnano}_b{n}_compacted)", "cpu_20260828_200000_1787948669258000000_b0_compacted.parquet", false, time.Time{}},
		{"daily compacted", "cpu_20260828_000000_1787948669258000000_b0_daily.parquet", false, time.Time{}},
		{"too few tokens", "cpu_20260828_200802.parquet", false, time.Time{}},
		{"non-digit nanos", "cpu_20260828_200802_76518800x.parquet", false, time.Time{}},
		{"bad minute", "cpu_20260828_207102_765188000.parquet", false, time.Time{}},
		{"bad month", "cpu_20261328_200802_765188000.parquet", false, time.Time{}},
		{"garbage", "whatever.parquet", false, time.Time{}},
		{"full path is fine", "/data/apex/cpu/2026/08/28/20/cpu_20260828_200802_765188000.parquet", true,
			time.Date(2026, 8, 28, 20, 8, 2, 0, time.UTC)},
	}
	for _, c := range cases {
		got, ok := parseFileTime(c.file)
		if ok != c.wantOK {
			t.Errorf("%s: ok=%v want %v", c.name, ok, c.wantOK)
			continue
		}
		if ok && !got.Equal(c.want) {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// buildHourDir creates base/db/m/YYYY/MM/DD/HH for the given UTC hour and
// returns the glob pattern the generator would produce for it.
func buildHourDir(t *testing.T, base string, hour time.Time) (string, string) {
	t.Helper()
	dir := filepath.Join(base, "db", "m",
		hour.Format("2006"), hour.Format("01"), hour.Format("02"), hour.Format("15"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return dir, filepath.Join(dir, "*.parquet")
}

func mkfile(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func newTestPruner(now time.Time) *PartitionPruner {
	p := NewPartitionPruner(zerolog.Nop())
	p.SetFileTimePruning(true, 5*time.Minute)
	p.nowFn = func() time.Time { return now }
	return p
}

func fname(m string, ts time.Time) string {
	return fmt.Sprintf("%s_%s_%s_000000000.parquet", m, ts.Format("20060102"), ts.Format("150405"))
}

func TestApplyFileTimePruning(t *testing.T) {
	now := time.Date(2026, 8, 28, 20, 40, 0, 0, time.UTC)
	hour := now.Truncate(time.Hour) // 20:00
	base := t.TempDir()
	dir, glob := buildHourDir(t, base, hour)

	oldFile := mkfile(t, dir, fname("m", hour.Add(2*time.Minute)))    // 20:02 — out of range
	edgeFile := mkfile(t, dir, fname("m", now.Add(-6*time.Minute)))   // 20:34 — inside margin window
	freshFile := mkfile(t, dir, fname("m", now.Add(-30*time.Second))) // 20:39:30 — in range
	weirdFile := mkfile(t, dir, "handplaced.parquet")                 // unparseable — fail open
	anomFile := mkfile(t, dir, fname("m", hour.Add(-10*time.Minute))) // 19:50 in 20h dir — future-stamped anomaly, keep

	tr := &TimeRange{Start: now.Add(-10 * time.Minute), End: now} // 20:30..20:40, margin 5m → cutoff 20:25

	p := newTestPruner(now)
	ctx, vol := WithVolatileResult(context.Background())
	out, changed := p.applyFileTimePruning(ctx, []string{glob}, tr)
	if !changed || !vol.Volatile {
		t.Fatalf("expected expansion (changed=%v volatile=%v)", changed, vol.Volatile)
	}
	got := map[string]bool{}
	for _, f := range out {
		got[f] = true
	}
	if got[oldFile] {
		t.Error("out-of-range file was kept")
	}
	for _, want := range []string{edgeFile, freshFile, weirdFile, anomFile} {
		if !got[want] {
			t.Errorf("expected kept: %s", filepath.Base(want))
		}
	}

	// Not the current wall-clock hour → untouched.
	pPast := newTestPruner(now.Add(2 * time.Hour))
	out2, changed2 := pPast.applyFileTimePruning(context.Background(), []string{glob}, tr)
	if changed2 || len(out2) != 1 || out2[0] != glob {
		t.Error("non-current-hour boundary must not be expanded")
	}

	// Disabled → untouched.
	pOff := newTestPruner(now)
	pOff.SetFileTimePruning(false, 0)
	if _, changed := pOff.applyFileTimePruning(context.Background(), []string{glob}, tr); changed {
		t.Error("disabled pruning must be a no-op")
	}

	// All files in range → glob retained (no SQL bloat).
	trWide := &TimeRange{Start: hour.Add(-1 * time.Minute), End: now}
	// Start 19:59 → boundary hour 19 != current hour 20 → no-op by the
	// current-hour rule; use a Start inside the hour but before every file:
	trWide = &TimeRange{Start: hour.Add(1 * time.Minute), End: now} // 20:01, cutoff 19:56
	out3, changed3 := p.applyFileTimePruning(context.Background(), []string{glob}, trWide)
	if changed3 || out3[0] != glob {
		t.Error("all-kept case must retain the glob unchanged")
	}

	// Zero kept + single path → fail safe (originals returned).
	trFuture := &TimeRange{Start: now.Add(30 * time.Minute), End: now.Add(40 * time.Minute)}
	// boundary hour is still 20 (current); cutoff 21:05 → every parseable file
	// is out of range; weird+anomalous files are kept though, so drop them for
	// this case in a fresh dir.
	base2 := t.TempDir()
	dir2, glob2 := buildHourDir(t, base2, hour)
	mkfile(t, dir2, fname("m", hour.Add(2*time.Minute)))
	out4, changed4 := p.applyFileTimePruning(context.Background(), []string{glob2}, trFuture)
	if changed4 || len(out4) != 1 || out4[0] != glob2 {
		t.Error("zero-kept single-path case must fail safe to the original glob")
	}
	_ = dir2

	// Remote paths are never touched.
	remote := "s3://bucket/db/m/" + hour.Format("2006/01/02/15") + "/*.parquet"
	out5, changed5 := p.applyFileTimePruning(context.Background(), []string{remote}, tr)
	if changed5 || out5[0] != remote {
		t.Error("remote path must not be expanded")
	}
}

func TestApplyFileTimePruningKeptCap(t *testing.T) {
	now := time.Date(2026, 8, 28, 20, 40, 0, 0, time.UTC)
	hour := now.Truncate(time.Hour)
	base := t.TempDir()
	dir, glob := buildHourDir(t, base, hour)
	// More in-range files than the cap → glob retained. All timestamps land
	// inside the keep window (unique nanos tokens keep the names distinct).
	for i := 0; i < maxExpandedFiles+5; i++ {
		ts := now.Add(-time.Duration(i%300) * time.Second)
		mkfile(t, dir, fmt.Sprintf("m_%s_%s_%09d.parquet", ts.Format("20060102"), ts.Format("150405"), i))
	}
	// One out-of-range file so kept != all.
	mkfile(t, dir, fname("m", hour.Add(1*time.Second)))
	p := newTestPruner(now)
	tr := &TimeRange{Start: now.Add(-10 * time.Minute), End: now}
	out, changed := p.applyFileTimePruning(context.Background(), []string{glob}, tr)
	if changed || out[0] != glob {
		t.Error("kept-count above cap must retain the glob")
	}
}

func TestExtractTimeRangeUnquotedInterval(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	// DuckDB-native unquoted form — previously extracted nothing, silently
	// disabling all pruning for clients emitting this syntax.
	for _, q := range []string{
		"SELECT * FROM t WHERE time > now() - INTERVAL 300 SECOND ORDER BY time",
		"SELECT * FROM t WHERE time >= NOW() - INTERVAL 5 MINUTE",
		"SELECT * FROM t WHERE time > now() - INTERVAL '300 seconds'",
	} {
		tr := p.ExtractTimeRange(q)
		if tr == nil {
			t.Errorf("no time range extracted from: %s", q)
			continue
		}
		if d := time.Since(tr.Start); d < 4*time.Minute || d > 6*time.Minute {
			t.Errorf("unexpected Start (%v ago) for: %s", d, q)
		}
	}
}
