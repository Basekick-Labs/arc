package metrics

import "sync/atomic"

// Sources and results of a forced flush (ingest.ArrowBuffer.ForceFlushKey):
// the labels of arc_ingest_forced_flushes_total. A fixed set, so the series
// count never grows with keys or callers.
const (
	ForcedFlushSourceRead  = iota // a fresh query (#1760)
	ForcedFlushSourceWrite        // a visible write (#1759)
	forcedFlushSources
)

const (
	ForcedFlushForced     = iota // handed to a flush worker
	ForcedFlushNotDue            // the key was detached within the interval
	ForcedFlushOverBudget        // no spare flush capacity: left to its natural flush
	ForcedFlushNothing           // nothing buffered, and no detach since the due check
	ForcedFlushNotWanted         // buffered, but none of it was the caller's
	ForcedFlushClosing           // the buffer is shutting down
	ForcedFlushLostRace          // another detach took the rows after the due check
	forcedFlushResults
)

var (
	forcedFlushSourceLabels = [forcedFlushSources]string{"read", "write"}
	forcedFlushResultLabels = [forcedFlushResults]string{"forced", "not_due", "over_budget", "nothing", "not_wanted", "closing", "lost_race"}
)

type ingestForcedFlush struct {
	counts [forcedFlushSources][forcedFlushResults]atomic.Int64
}

// IncIngestForcedFlush counts one ForceFlushKey call by source and result
// (ForcedFlushSource*, ForcedFlush*). Out-of-range values are dropped.
func (m *Metrics) IncIngestForcedFlush(source, result int) {
	if source >= 0 && source < forcedFlushSources && result >= 0 && result < forcedFlushResults {
		m.ingestForcedFlush.counts[source][result].Add(1)
	}
}

func (m *Metrics) appendIngestForcedFlush(b []byte) []byte {
	b = append(b, "# HELP arc_ingest_forced_flushes_total Requests to flush one measurement's buffer early, by source (write: x-arc-wait: visible; read: reserved for fresh queries) and result: forced, not_due (flushed within the interval), over_budget (no spare flush capacity: the rows wait for their age or size flush), nothing (nothing left to flush), not_wanted (nothing of the caller's left to flush), closing, lost_race (another flush took the rows first)\n"...)
	b = append(b, "# TYPE arc_ingest_forced_flushes_total counter\n"...)
	names := []string{"source", "result"}
	for s, source := range forcedFlushSourceLabels {
		for r, result := range forcedFlushResultLabels {
			b = appendMetricWithLabels(b, "arc_ingest_forced_flushes_total", names, []string{source, result}, float64(m.ingestForcedFlush.counts[s][r].Load()))
		}
	}
	return b
}
