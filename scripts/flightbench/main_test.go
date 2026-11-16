package main

import (
	"reflect"
	"testing"
)

func TestArmLabelsSeparateRepeats(t *testing.T) {
	got := armLabels([]string{"http", "flight", "http", "flight", "http"})
	want := []string{"http", "flight", "http#2", "flight#2", "http#3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("armLabels = %v, want %v", got, want)
	}
}

// Over 2n rounds every arm runs in every position.
func TestRoundOrderCoversEveryPosition(t *testing.T) {
	const n = 4
	seen := map[[2]int]bool{}
	for r := 0; r < 2*n; r++ {
		order := roundOrder(n, r)
		for pos, arm := range order {
			seen[[2]int{arm, pos}] = true
		}
	}
	if len(seen) != n*n {
		t.Fatalf("covered %d of %d (arm, position) pairs", len(seen), n*n)
	}
}

func TestParsePSTime(t *testing.T) {
	for in, want := range map[string]float64{"0:12.34": 12.34, "01:02:03": 3723, "1-00:00:01": 86401} {
		if got := parsePSTime(in); got < want-1e-9 || got > want+1e-9 {
			t.Errorf("parsePSTime(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestMedian(t *testing.T) {
	rs := []result{{p50: 1}, {p50: 3}, {p50: 2}, {p50: 10}}
	if got := median(rs, func(r result) float64 { return float64(r.p50) }); got != 2.5 {
		t.Fatalf("median = %v, want 2.5", got)
	}
}
