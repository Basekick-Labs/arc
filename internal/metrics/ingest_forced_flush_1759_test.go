package metrics

import (
	"strings"
	"testing"
	"time"
)

// #1759: arc_ingest_forced_flushes_total has a fixed label set, 2 sources x 7
// results (lost_race since #1942), all exposed from the start; out-of-range
// values are dropped.
func TestIngestForcedFlushMetric1759(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.IncIngestForcedFlush(ForcedFlushSourceWrite, ForcedFlushForced)
	m.IncIngestForcedFlush(ForcedFlushSourceWrite, ForcedFlushForced)
	m.IncIngestForcedFlush(ForcedFlushSourceRead, ForcedFlushOverBudget)
	m.IncIngestForcedFlush(-1, ForcedFlushForced)
	m.IncIngestForcedFlush(ForcedFlushSourceWrite, forcedFlushResults)
	m.IncIngestForcedFlush(forcedFlushSources, 0)
	out := m.PrometheusFormat()
	if !strings.Contains(out, "# TYPE arc_ingest_forced_flushes_total counter\n") {
		t.Fatal("no TYPE line")
	}
	series := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "arc_ingest_forced_flushes_total{") {
			series++
		}
	}
	if series != 14 {
		t.Fatalf("%d series, want 14 (fixed: source x result)", series)
	}
	for _, want := range []string{
		`arc_ingest_forced_flushes_total{source="write",result="forced"} 2` + "\n",
		`arc_ingest_forced_flushes_total{source="read",result="over_budget"} 1` + "\n",
		`arc_ingest_forced_flushes_total{source="read",result="forced"} 0` + "\n",
		`arc_ingest_forced_flushes_total{source="write",result="not_wanted"} 0` + "\n",
		`arc_ingest_forced_flushes_total{source="write",result="closing"} 0` + "\n",
		`arc_ingest_forced_flushes_total{source="read",result="lost_race"} 0` + "\n",
		`arc_ingest_forced_flushes_total{source="write",result="lost_race"} 0` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}

// #1942: the label array is sized by the result count, so a result added
// without its label would compile and expose result="". Every label is set
// and distinct.
func TestIngestForcedFlushLabelsSet1942(t *testing.T) {
	seen := map[string]bool{}
	for i, l := range forcedFlushResultLabels {
		if l == "" || seen[l] {
			t.Fatalf("result %d: label %q is empty or repeated", i, l)
		}
		seen[l] = true
	}
	if forcedFlushResultLabels[ForcedFlushLostRace] != "lost_race" {
		t.Fatalf("ForcedFlushLostRace = %q", forcedFlushResultLabels[ForcedFlushLostRace])
	}
}
