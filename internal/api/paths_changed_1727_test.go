package api

import (
	"os"
	"strings"
	"testing"
)

// #1727: DELETE and retention report the measurement's directory, as a
// storage prefix, after they changed files; never on a dry run or a no-op.

func TestRetentionReportsChangedMeasurements(t *testing.T) {
	var got []string
	h := &RetentionHandler{}
	h.SetOnPathsChanged(func(keys []string) { got = append(got, keys...) })
	h.measurementChanged(false, 3, "db", "cpu")
	h.measurementChanged(true, 3, "db", "mem") // dry run
	h.measurementChanged(false, 0, "db", "disk")
	if strings.Join(got, ",") != "db/cpu/" {
		t.Fatalf("reported %v, want db/cpu/ only", got)
	}
}

func TestDeleteReportsTheMeasurementDirectory(t *testing.T) {
	var got []string
	removed := 0
	h := &DeleteHandler{}
	h.SetOnFilesRemoved(func() { removed++ })
	h.SetOnPathsChanged(func(keys []string) { got = append(got, keys...) })
	h.filesRemovedIn("spokeA", "db") // a hub's spoke data: the directory, mapped by the scheduler
	if removed != 1 || strings.Join(got, ",") != "spokeA/db/" {
		t.Fatalf("caches cleared %d, reported %v", removed, got)
	}
}

// Every DELETE notification reports the measurement, not just the cache.
func TestDeleteNotificationsAllReportPaths(t *testing.T) {
	src, err := os.ReadFile("delete.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), "h.filesRemoved()"); n != 0 {
		t.Fatalf("delete.go still clears caches without reporting paths at %d site(s)", n)
	}
	if n := strings.Count(string(src), "h.filesRemovedIn(req.Database, req.Measurement)"); n < 5 {
		t.Fatalf("%d DELETE notification sites report paths, want at least 5", n)
	}
}
