package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestIngestPublishDelayHistogram(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	// On a bound, just over it, in the last finite bucket, past every bound,
	// and negative (a clock step), which counts as zero.
	for _, d := range []time.Duration{
		100 * time.Millisecond,
		101 * time.Millisecond,
		5200 * time.Millisecond,
		299 * time.Second,
		2 * time.Hour,
		-3 * time.Second,
	} {
		m.ObserveIngestPublishDelay(d)
	}
	out := m.PrometheusFormat()
	for _, want := range []string{
		"# TYPE arc_ingest_publish_delay_seconds histogram\n",
		`arc_ingest_publish_delay_seconds_bucket{le="0.01"} 1` + "\n", // the negative one
		`arc_ingest_publish_delay_seconds_bucket{le="0.05"} 1` + "\n", // nothing between
		`arc_ingest_publish_delay_seconds_bucket{le="0.1"} 2` + "\n",  // a bound includes its value
		`arc_ingest_publish_delay_seconds_bucket{le="0.15"} 3` + "\n", // 101 ms is over 0.1
		`arc_ingest_publish_delay_seconds_bucket{le="5"} 3` + "\n",    // 5.2 s is over 5
		`arc_ingest_publish_delay_seconds_bucket{le="5.5"} 4` + "\n",  // and inside 5.5
		`arc_ingest_publish_delay_seconds_bucket{le="120"} 4` + "\n",
		`arc_ingest_publish_delay_seconds_bucket{le="300"} 5` + "\n",
		`arc_ingest_publish_delay_seconds_bucket{le="+Inf"} 6` + "\n",
		"arc_ingest_publish_delay_seconds_count 6\n",
		"# TYPE arc_ingest_publish_delay_max_seconds gauge\n",
		"arc_ingest_publish_delay_max_seconds 7200\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
	wantSum := 0.1 + 0.101 + 5.2 + 299 + 7200
	// The exposition truncates to six decimals, so compare the number.
	var gotSum float64
	if _, err := fmt.Sscanf(grepLines(out, "publish_delay_seconds_sum "), "arc_ingest_publish_delay_seconds_sum %g", &gotSum); err != nil ||
		gotSum < wantSum-0.001 || gotSum > wantSum+0.001 {
		t.Errorf("Prometheus sum = %q (%v), want %.3f", grepLines(out, "publish_delay_seconds_sum "), err, wantSum)
	}
	snap := m.Snapshot()
	if got := snap["ingest_publish_delay_count"]; got != int64(6) {
		t.Errorf("ingest_publish_delay_count = %v, want 6", got)
	}
	if got := snap["ingest_publish_delay_max_seconds"]; got != float64(7200) {
		t.Errorf("ingest_publish_delay_max_seconds = %v, want 7200", got)
	}
	if got, ok := snap["ingest_publish_delay_sum_seconds"].(float64); !ok || got < wantSum-0.001 || got > wantSum+0.001 {
		t.Errorf("ingest_publish_delay_sum_seconds = %v, want %.3f", snap["ingest_publish_delay_sum_seconds"], wantSum)
	}
}

// Every bound has a label and the labels are the bounds: a bucket added to
// one list and not the other would shift every count after it.
func TestIngestPublishDelayLabelsMatchBounds(t *testing.T) {
	if len(ingestPublishDelayLabels) != len(ingestPublishDelayBounds)+1 {
		t.Fatalf("%d labels for %d bounds", len(ingestPublishDelayLabels), len(ingestPublishDelayBounds))
	}
	for i := 1; i < len(ingestPublishDelayBounds); i++ {
		if ingestPublishDelayBounds[i] <= ingestPublishDelayBounds[i-1] {
			t.Fatalf("bounds not increasing at %d: %v then %v", i, ingestPublishDelayBounds[i-1], ingestPublishDelayBounds[i])
		}
	}
	m := &Metrics{startTime: time.Now()}
	for _, b := range ingestPublishDelayBounds {
		m.ObserveIngestPublishDelay(time.Duration(b * float64(time.Second)))
	}
	out := m.PrometheusFormat()
	for i, label := range ingestPublishDelayLabels[:len(ingestPublishDelayBounds)] {
		want := fmt.Sprintf(`arc_ingest_publish_delay_seconds_bucket{le=%q} %d`+"\n", label, i+1)
		if !strings.Contains(out, want) {
			t.Errorf("an observation equal to bound %s is not in its own bucket: missing %q", label, want)
		}
	}
}

func grepLines(s, sub string) string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, sub) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
