package metrics

import (
	"strings"
	"testing"
)

// #1343: the counters a compaction subprocess moves come back to the parent.
func TestChildCountersRoundTrip(t *testing.T) {
	child := &Metrics{}
	child.IncStorageWrites()
	child.IncStorageWriteBytes(40)
	child.IncStorageInvalidPathQuarantined()
	counts := child.ChildCounters()
	if counts["storage_writes"] != 1 || counts["storage_write_bytes"] != 40 || counts["storage_invalid_path_quarantined"] != 1 {
		t.Fatalf("child counters %v", counts)
	}
	if _, ok := counts["storage_reads"]; ok {
		t.Error("a zero counter was sent")
	}
	parent := &Metrics{}
	parent.AddChildCounters(counts)
	parent.AddChildCounters(map[string]int64{"from_a_newer_child": 5, "storage_reads": -3})
	parent.AddChildCounters(nil)
	if parent.storageWritesTotal.Load() != 1 || parent.storageWriteBytesTotal.Load() != 40 || parent.storageReadsTotal.Load() != 0 {
		t.Fatalf("parent writes=%d bytes=%d reads=%d", parent.storageWritesTotal.Load(), parent.storageWriteBytesTotal.Load(), parent.storageReadsTotal.Load())
	}
}

// The job, file and byte counters are on /metrics.
func TestCompactionCountersOnPrometheus(t *testing.T) {
	out := (&Metrics{}).PrometheusFormat()
	for _, name := range []string{
		"arc_compaction_jobs_total", "arc_compaction_jobs_success_total", "arc_compaction_jobs_failed_total",
		"arc_compaction_jobs_interrupted_total", "arc_compaction_files_compacted_total",
		"arc_compaction_bytes_read_total", "arc_compaction_bytes_written_total",
	} {
		if !strings.Contains(out, "\n"+name+" ") {
			t.Errorf("%s missing from /metrics", name)
		}
	}
}
