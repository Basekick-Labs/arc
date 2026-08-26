// arcx decline-census counters. Untagged: the counters compile into every build
// (they are plain atomics with no arcx linkage, so stock Arc's binary gains no
// arcx symbols), but ONLY the arcx_engine-tagged census path increments them —
// in stock Arc they exist and stay zero, which the JSON snapshot makes obvious.
//
// The label set is CLOSED and enforced HERE as well as at the producer: an
// unknown reason string is never stored or emitted — it increments an
// `invalid` counter with no name attached. That is defense in depth against a
// future caller passing an input-derived string (the bug that turns a metric
// into both a data leak and a cardinality bomb at once), and it fails loudly
// on a dashboard instead of silently minting a new label.

package metrics

import "sync/atomic"

// arcxCensusReasons mirrors arcxrouter's declineReasonNames. APPEND-ONLY: the
// index is the storage slot, so reordering silently re-labels history. The two
// tables are pinned to each other by TestArcxCensusReasonSetMatchesRouter in
// the arcxrouter package (which can see both).
var arcxCensusReasons = [...]string{
	"eligible",
	"none_ineligible",
	"unlexable",
	"tz_setting",
	"collation",
	"null_order",
	"cte",
	"join",
	"set_op",
	"window",
	"having",
	"group_by",
	"distinct",
	"offset",
	"star_projection",
	"agg_fn",
	"fn_other",
	"unresolvable_measurement",
}

var (
	arcxCensusCounts  [len(arcxCensusReasons)]atomic.Int64
	arcxCensusInvalid atomic.Int64
)

// IncArcxShapeCensus records one classified query. `reason` must be a member of
// arcxCensusReasons; anything else is counted as invalid WITHOUT recording the
// string (closed-set boundary — see the file comment).
func (m *Metrics) IncArcxShapeCensus(reason string) {
	for i := range arcxCensusReasons {
		if arcxCensusReasons[i] == reason {
			arcxCensusCounts[i].Add(1)
			return
		}
	}
	arcxCensusInvalid.Add(1)
}

// ArcxCensusReasons returns a copy of the closed label set, in slot order.
// Exists so arcxrouter's tests can pin its reason table to this one — the two
// are unexported in their own packages, and silent divergence would re-label
// dashboard history (see the append-only note above).
func ArcxCensusReasons() []string {
	out := make([]string, len(arcxCensusReasons))
	copy(out, arcxCensusReasons[:])
	return out
}

// arcxCensusSnapshot returns every counter (zeros included, so the closed set
// is visible on the dashboard and a stock build reads as all-zero rather than
// absent). Consumed by Snapshot() for /api/v1/metrics.
func (m *Metrics) arcxCensusSnapshot() map[string]int64 {
	out := make(map[string]int64, len(arcxCensusReasons)+1)
	for i, name := range arcxCensusReasons {
		out[name] = arcxCensusCounts[i].Load()
	}
	out["invalid"] = arcxCensusInvalid.Load()
	return out
}
