package compaction

import (
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestExtractNewestFileTimeCompactionFormats(t *testing.T) {
	stamp := time.Date(2026, 10, 10, 12, 34, 56, 0, time.UTC)
	day := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		file string
		want time.Time
	}{
		{"raw", "cpu_load_20261010_123456_123456789.parquet", stamp},
		{"hourly legacy", "cpu_load_20261010_123456_compacted.parquet", stamp},
		{"hourly nanos", "cpu_load_20261010_123456_123456789_compacted.parquet", stamp},
		{"daily legacy", "cpu_load_20261010_123456_daily.parquet", stamp},
		{"daily nanos", "cpu_load_20261010_123456_123456789_daily.parquet", stamp},
		{"daily date", "cpu_load_20261010_daily.parquet", day},
		{"daily date simple measurement", "cpu_20261010_daily.parquet", day},
		{"legacy opaque nanos", "cpu_load_20261010_123456_b123_daily.parquet", stamp},
		{"legacy numeric measurement opaque nanos", "cpu_20250101_20261010_123456_b123_daily.parquet", stamp},
		{"daily numeric measurement", "cpu_20250101_20261010_daily.parquet", day},
		{"legacy nanos resembling date", "foo_20260101_120000_20260102_daily.parquet", time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"batch zero", "cpu_load_20261010_123456_123456789_b0_compacted.parquet", stamp},
		{"batch one", "cpu_load_20261010_123456_123456789_b1_compacted.parquet", stamp},
		{"batch daily", "cpu_load_20261010_123456_123456789_b12_daily.parquet", stamp},
		{"large batch", "cpu_load_20261010_123456_123456789_b999999999999999999999999_compacted.parquet", stamp},
		{"batch-like measurement", "cpu_b1_20261010_123456_123456789_b2_compacted.parquet", stamp},
		{"rewrite batch", "cpu_load_20261010_123456_123456789_b1_compacted_rewrite_1791635696000000000.parquet", stamp},
		{"rewrite daily", "cpu_load_20261010_daily_rewrite_1791635696000000000.parquet", day},
		{"rewrite raw", "cpu_load_20261010_123456_123456789_rewrite_1791635696000000000.parquet", stamp},
		{"invalid date", "cpu_20260230_123456_123456789_b1_compacted.parquet", time.Time{}},
		{"invalid hour", "cpu_20261010_253456_123456789_b1_compacted.parquet", time.Time{}},
		{"short hour", "cpu_20261010_1234_123456789_b1_compacted.parquet", time.Time{}},
		{"invalid nanos", "cpu_20261010_123456_bad_b1_compacted.parquet", time.Time{}},
		{"empty batch", "cpu_20261010_123456_123456789_b_compacted.parquet", time.Time{}},
		{"negative batch", "cpu_20261010_123456_123456789_b-1_compacted.parquet", time.Time{}},
		{"nondecimal batch", "cpu_20261010_123456_123456789_b1x_compacted.parquet", time.Time{}},
		{"nonASCII batch", "cpu_20261010_123456_123456789_b١_compacted.parquet", time.Time{}},
		{"missing measurement", "_20261010_123456_123456789_b1_compacted.parquet", time.Time{}},
		{"invalid date batched daily", "cpu_20260230_123456_20261010_b1_daily.parquet", time.Time{}},
		{"invalid daily date", "cpu_20260230_daily.parquet", time.Time{}},
		{"date-only hourly unsupported", "cpu_20261010_compacted.parquet", time.Time{}},
		{"batch raw unsupported", "cpu_20261010_123456_123456789_b1.parquet", time.Time{}},
		{"invalid name", "invalid.parquet", time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractNewestFileTime([]string{"db/cpu/2026/10/10/12/" + tc.file})
			if !got.Equal(tc.want) {
				t.Fatalf("timestamp = %v, want %v", got, tc.want)
			}
		})
	}
	t.Run("mixed order", func(t *testing.T) {
		files := []string{"invalid.parquet", "cpu_20261009_123456_123_b1_compacted.parquet", "cpu_20261010_123456_456_b2_daily.parquet"}
		for i := 0; i < len(files); i++ {
			if got := extractNewestFileTime(files); !got.Equal(stamp) {
				t.Fatalf("newest = %v, want %v", got, stamp)
			}
			files = append(files[1:], files[0])
		}
	})
}

// These tests isolate the age gate from MinFiles and wall-clock cutoffs.
func TestCompactionBatchedFileAgeGate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	cutoff := now.Add(-24 * time.Hour)
	for _, tier := range []string{"hourly", "daily"} {
		for _, tc := range []struct {
			name          string
			fileTime      time.Time
			partitionDays int
			want          int
		}{
			{"recent", cutoff.Add(time.Hour), 2, 0},
			{"old", cutoff.Add(-time.Hour), 2, 1},
			{"equal cutoff", cutoff, 2, 1},
			{"historical recent", cutoff.Add(time.Hour), 30, 0},
		} {
			t.Run(tier+"/"+tc.name, func(t *testing.T) {
				partition := now.AddDate(0, 0, -tc.partitionDays)
				path := fmt.Sprintf("db/cpu_load/%s/03/cpu_load_%s_123456789_b1_compacted.parquet", partition.Format("2006/01/02"), tc.fileTime.Format("20060102_150405"))
				var got []Candidate
				want := tc.want
				if tier == "hourly" {
					x := NewHourlyTier(&HourlyTierConfig{Enabled: true, Logger: zerolog.Nop()})
					got = x.groupHourPartitions("db", "cpu_load", []string{path}, cutoff)
				} else {
					x := NewDailyTier(&DailyTierConfig{Enabled: true, SkipFileAgeCheckDays: 7, Logger: zerolog.Nop()})
					got = x.groupDayPartitions("db", "cpu_load", []string{path}, cutoff)
					if tc.partitionDays == 30 {
						want = 1 // Preserve the configured historical-data bypass.
					}
				}
				if len(got) != want {
					t.Fatalf("partitions = %d, want %d", len(got), want)
				}
			})
		}
	}
}

func TestCompactionBatchedFileCandidates(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tier := range []string{"hourly", "daily"} {
		for _, recent := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recent=%t", tier, recent), func(t *testing.T) {
				partition := now.AddDate(0, 0, -2)
				prefix := "db/cpu_load/" + partition.Format("2006/01/02") + "/03/"
				old := now.Add(-48 * time.Hour).Format("20060102_150405")
				newest := old
				if recent {
					newest = now.Format("20060102_150405")
				}
				var files []string
				var got []Candidate
				if tier == "hourly" {
					files = []string{prefix + "cpu_load_" + old + "_123.parquet", prefix + "cpu_load_" + old + "_456.parquet", prefix + "cpu_load_" + newest + "_789_b1_compacted.parquet"}
					x := NewHourlyTier(&HourlyTierConfig{Enabled: true, MinAgeHours: 1, MinFiles: 2, Logger: zerolog.Nop()})
					got = x.FindCandidatesFromListing("db", "cpu_load", files)
				} else {
					files = []string{prefix + "cpu_load_" + old + "_123_b1_compacted.parquet", prefix + "cpu_load_" + newest + "_456_b2_compacted.parquet"}
					x := NewDailyTier(&DailyTierConfig{Enabled: true, MinAgeHours: 24, MinFiles: 2, SkipFileAgeCheckDays: 7, Logger: zerolog.Nop()})
					got = x.FindCandidatesFromListing("db", "cpu_load", files)
				}
				want := 1
				if recent {
					want = 0
				}
				if len(got) != want {
					t.Fatalf("candidates = %d, want %d", len(got), want)
				}
			})
		}
	}
}
