package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// #1752: a REST catalog load pulls an export forward on a standalone node and
// on a cluster over local storage, never on a cluster over a shared bucket,
// and only with the key set.
func TestIcebergLoadIntervalPerTopology(t *testing.T) {
	for _, tc := range []struct {
		name            string
		seconds         int
		cluster, shared bool
		want            time.Duration
	}{
		{"off", 0, false, false, 0},
		{"standalone", 7, false, false, 7 * time.Second},
		{"standalone on a shared-looking store (no cluster)", 7, false, true, 7 * time.Second},
		{"cluster, local storage", 7, true, false, 7 * time.Second},
		{"cluster, shared bucket", 7, true, true, 0},
	} {
		if got := icebergLoadInterval(tc.seconds, tc.cluster, tc.shared); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The catalog gets the scheduler's wait only when the load interval is on, and
// version retention follows the shorter of the two spacings.
func TestIcebergLoadExportWiring(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, want := range []string{
		"LoadInterval: icebergLoadEvery,",
		"awaitFresh = icebergScheduler.AwaitFreshForRead",
		"if icebergLoadEvery > 0 && icebergLoadEvery < icebergCommitSpacing {",
		"exporter.SetVersionRetention(time.Duration(cfg.Iceberg.MinVersionAge)*time.Second, icebergCommitSpacing)",
	} {
		if !strings.Contains(src, want) {
			t.Errorf("main.go no longer has %q", want)
		}
	}
}
