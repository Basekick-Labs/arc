package metrics

import (
	"strings"
	"testing"
)

// #220: a scheduled compaction tick lost to a held cycle claim is counted under
// its tier, and every tier is exported from zero so a rate() alert can see it.
func TestCompactionScheduledSkipsByTier(t *testing.T) {
	m := &Metrics{}
	out := m.PrometheusFormat()
	for _, tier := range CompactionScheduledSkipTiers {
		if want := `arc_compaction_scheduled_skips_total{tier="` + tier + `"} 0`; !strings.Contains(out, want) {
			t.Errorf("%s missing from /metrics", want)
		}
	}

	m.IncCompactionScheduledSkips("daily")
	m.IncCompactionScheduledSkips("daily")
	m.IncCompactionScheduledSkips("hourly")
	m.IncCompactionScheduledSkips("weekly")

	for tier, want := range map[string]int64{"hourly": 1, "daily": 2, "other": 1, "weekly": 1} {
		if got := m.CompactionScheduledSkipsFor(tier); got != want {
			t.Errorf("CompactionScheduledSkipsFor(%q) = %d, want %d", tier, got, want)
		}
	}
	out = m.PrometheusFormat()
	if want := `arc_compaction_scheduled_skips_total{tier="daily"} 2`; !strings.Contains(out, want) {
		t.Errorf("%s missing from /metrics", want)
	}
	if got := m.Snapshot()["compaction_scheduled_skips"]; got != int64(4) {
		t.Errorf("Snapshot compaction_scheduled_skips = %v, want 4", got)
	}
}
