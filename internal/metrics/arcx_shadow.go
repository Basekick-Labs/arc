// arcx shadow-outcome counters. Untagged for the same reason as the decline
// census: plain atomics with no arcx linkage, so a stock build gains no arcx
// symbols and reads as all-zero rather than absent.
//
// WHY these exist (2026-10-03). Shadow outcomes were log-only, and `skipped` was
// logged at DEBUG. At the default `info` level a shed sample was therefore
// invisible — indistinguishable from "it compared and matched". That is not a
// cosmetic gap: `shadowSlots` sheds samples under back-to-back load, so a
// deterministic shadow bug presented as an intermittent one, and the
// "did my fix work?" check was unreliable for the same reason the "does this
// reproduce?" check had been. Outcomes have to be COUNTABLE, not just loggable.
//
// The label set is CLOSED and enforced here, exactly as the census is: an unknown
// outcome increments `invalid` with no string attached, so no caller can turn this
// into a cardinality bomb or a data leak.

package metrics

import "sync/atomic"

// arcxShadowOutcomes is APPEND-ONLY: the index is the storage slot, so reordering
// silently re-labels history.
//
// Every shadow attempt lands in exactly ONE bucket, so the sum equals attempts.
// That property is what lets "no skips" be read as "every attempt really was
// compared" — the review found two paths (oracle failure, recovered panic) that
// incremented nothing, which silently broke it.
//
//	match         — compared, identical
//	mismatch      — compared, DIFFERENT: the alarm that means "arcx is wrong"
//	argdiff       — arg_max/arg_min tie divergence (DuckDB is nondeterministic here)
//	declined      — the engine declined an eligible shape (eligibility over-claimed)
//	error         — the SHADOW engine run failed
//	oracle_error  — the DuckDB oracle failed; says nothing about arcx
//	panic         — a shadow goroutine panicked and was recovered
//
// skipped_* — WE chose not to compare. Never evidence about arcx, and split by
// cause because they mean different things operationally:
//
//	skipped_shed  — no shadow slot (sampling under load). THIS is the one that
//	                made a deterministic bug look intermittent.
//	skipped_cap   — result exceeded ShadowMaxRows
//	skipped_host  — host-side assembly or render failure (our bug, not arcx's)
var arcxShadowOutcomes = [...]string{
	"match",
	"mismatch",
	"argdiff",
	"declined",
	"error",
	"oracle_error",
	"panic",
	"skipped_shed",
	"skipped_cap",
	"skipped_host",
}

// Exported label constants. The call sites live in a file behind the
// `arcx_engine` tag, which Arc's CI never compiles — so a typo'd literal would
// compile, land in `invalid`, and leave the alarm counter reading 0 forever.
// Using these makes that a build error instead.
const (
	ShadowMatch       = "match"
	ShadowMismatch    = "mismatch"
	ShadowArgDiff     = "argdiff"
	ShadowDeclined    = "declined"
	ShadowError       = "error"
	ShadowOracleError = "oracle_error"
	ShadowPanic       = "panic"
	ShadowSkippedShed = "skipped_shed"
	ShadowSkippedCap  = "skipped_cap"
	ShadowSkippedHost = "skipped_host"
)

var (
	arcxShadowCounts  [len(arcxShadowOutcomes)]atomic.Int64
	arcxShadowInvalid atomic.Int64
)

// IncArcxShadowOutcome records one shadow run's outcome. `outcome` must be a
// member of arcxShadowOutcomes; anything else counts as invalid WITHOUT recording
// the string.
func (m *Metrics) IncArcxShadowOutcome(outcome string) {
	for i := range arcxShadowOutcomes {
		if arcxShadowOutcomes[i] == outcome {
			arcxShadowCounts[i].Add(1)
			return
		}
	}
	arcxShadowInvalid.Add(1)
}

// ArcxShadowOutcomes returns a copy of the closed label set, in slot order.
func ArcxShadowOutcomes() []string {
	out := make([]string, len(arcxShadowOutcomes))
	copy(out, arcxShadowOutcomes[:])
	return out
}

// arcxShadowSnapshot returns every counter, zeros included.
func (m *Metrics) arcxShadowSnapshot() map[string]int64 {
	out := make(map[string]int64, len(arcxShadowOutcomes)+1)
	for i, name := range arcxShadowOutcomes {
		out[name] = arcxShadowCounts[i].Load()
	}
	out["invalid"] = arcxShadowInvalid.Load()
	return out
}
