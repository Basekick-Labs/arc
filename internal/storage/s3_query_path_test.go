package storage

import (
	"reflect"
	"testing"
	"time"
)

func TestGetQueryPathRangeUsesCalendarDaysAcrossDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}

	backend := &S3Backend{bucket: "test-bucket", prefix: "tenant/"}
	start := time.Date(2026, time.October, 31, 12, 0, 0, 0, loc)
	end := time.Date(2026, time.November, 2, 12, 0, 0, 0, loc)

	got := backend.GetQueryPathRange("metrics", "cpu", start, end)
	want := expectedDSTPaths()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected paths across DST boundary:\ngot:  %v\nwant: %v", got, want)
	}
}

func TestGetQueryPathRangeUsesStartLocationForRange(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load start timezone: %v", err)
	}
	losAngeles, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("load end timezone: %v", err)
	}

	backend := &S3Backend{bucket: "test-bucket", prefix: "tenant/"}
	start := time.Date(2026, time.October, 31, 12, 0, 0, 0, newYork)
	end := time.Date(2026, time.November, 2, 12, 0, 0, 0, losAngeles)

	got := backend.GetQueryPathRange("metrics", "cpu", start, end)
	want := expectedDSTPaths()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected paths with mixed locations:\ngot:  %v\nwant: %v", got, want)
	}
}

func expectedDSTPaths() []string {
	return []string{
		"s3://test-bucket/tenant/metrics/cpu/2026/10/31/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/01/*/*.parquet",
		"s3://test-bucket/tenant/metrics/cpu/2026/11/02/*/*.parquet",
	}
}
