package storage

import (
	"reflect"
	"testing"
	"time"
)

// GetQueryPathRange generates UTC calendar-day paths: Arc partitions are UTC
// dates, so non-UTC inputs normalize to UTC rather than anchoring the walk to
// their own zone. The Nov 2 20:00 New York instant is Nov 3 01:00 UTC, so the
// 11/03 partition MUST be listed; a local-day walk stops at 11/02 and drops it.
func TestGetQueryPathRangeUsesUTCCalendarDays(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}

	backend := &S3Backend{bucket: "test-bucket", prefix: "tenant/"}
	start := time.Date(2026, time.October, 31, 12, 0, 0, 0, loc) // Oct 31 16:00 UTC
	end := time.Date(2026, time.November, 2, 20, 0, 0, 0, loc)   // Nov 3 01:00 UTC

	got := backend.GetQueryPathRange("metrics", "cpu", start, end)
	want := []string{
		"s3://test-bucket/tenant/metrics/cpu/2026/10/31/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/01/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/02/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/03/*/*.parquet",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("non-UTC inputs must yield UTC-day paths:\ngot:  %v\nwant: %v", got, want)
	}
}

// The DST fall-back weekend must not skip or duplicate a day (#321): AddDate
// in UTC steps calendar days regardless of any zone's transitions.
func TestGetQueryPathRangeCalendarDaysAcrossDST(t *testing.T) {
	backend := &S3Backend{bucket: "test-bucket", prefix: "tenant/"}
	start := time.Date(2026, time.October, 31, 12, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.November, 2, 12, 0, 0, 0, time.UTC)

	got := backend.GetQueryPathRange("metrics", "cpu", start, end)
	want := []string{
		"s3://test-bucket/tenant/metrics/cpu/2026/10/31/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/01/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/02/*/*.parquet",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected paths across the DST weekend:\ngot:  %v\nwant: %v", got, want)
	}
}

// Mixed-zone inputs normalize to one UTC day set; neither input's zone may
// influence the walk.
func TestGetQueryPathRangeMixedLocationsNormalize(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load start timezone: %v", err)
	}
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("load end timezone: %v", err)
	}

	backend := &S3Backend{bucket: "test-bucket", prefix: "tenant/"}
	start := time.Date(2026, time.October, 31, 12, 0, 0, 0, newYork) // Oct 31 16:00 UTC
	end := time.Date(2026, time.November, 3, 8, 0, 0, 0, tokyo)      // Nov 2 23:00 UTC

	got := backend.GetQueryPathRange("metrics", "cpu", start, end)
	want := []string{
		"s3://test-bucket/tenant/metrics/cpu/2026/10/31/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/01/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/02/*/*.parquet",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed-zone inputs must normalize to UTC days:\ngot:  %v\nwant: %v", got, want)
	}
}
