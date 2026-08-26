// Decline census: classify WHY a query is not arcx-eligible, from the full token
// stream — not from the recognizer's bail point. The recognizer is a sequence
// matcher that stops at the first off-path token, so 16 of 18 structurally
// distinct decline shapes (GROUP BY, JOIN, CTE, window, DISTINCT, …) look
// identical to it; a census built on its rejection sites would report one
// giant "not recognized" bucket. This classifier scans everything the lexer
// produced and picks a label from a CLOSED, compile-time set.
//
// HARD CONSTRAINT (repo-owner decision): no census path may log, store, or
// return query text — not literals, not identifiers, not function names, not
// even redacted. The Arc repo is public; the code must not APPEAR to leak on
// inspection, so there is deliberately no code path here where user bytes can
// reach a return value. Every returned string is a constant from
// declineReasonNames; identifiers are counted, never named (`fn_other`, never
// `fn_other:<name>`), which also keeps the metric label set bounded.

//go:build cgo && arcx_engine

package arcxrouter

import "strings"

// declineReason indexes declineReasonNames. uint8 by value: measured to add
// zero allocations to the classify path.
type declineReason uint8

// Append-only: these indices feed the metrics census array, so reordering
// silently re-labels historical dashboards.
const (
	reasonEligible declineReason = iota
	reasonNoneIneligible
	reasonUnlexable
	reasonTzSetting
	reasonCollation
	reasonNullOrder
	reasonCTE
	reasonJoin
	reasonSetOp
	reasonWindow
	reasonHaving
	reasonGroupBy
	reasonDistinct
	reasonOffset
	reasonStarProjection
	reasonAggFn
	reasonFnOther
	reasonUnresolvableMeasurement
	numDeclineReasons
)

// The ONLY strings this file can emit. Compile-time constants; the closed set
// is what keeps user bytes out of logs/metrics and the label cardinality fixed.
var declineReasonNames = [numDeclineReasons]string{
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

func (r declineReason) String() string {
	if r >= numDeclineReasons {
		return "none_ineligible" // unreachable; belt-and-braces over a panic
	}
	return declineReasonNames[r]
}

// aggFns: one bucket, deliberately per-set not per-function. "Which aggregate?"
// is answerable offline once the census says aggregation is the top bucket, and
// collapsing removes any temptation to derive a label from the function name.
var aggFns = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"median": true, "stddev": true, "stddev_samp": true, "stddev_pop": true,
	"variance": true, "var_samp": true, "var_pop": true,
	"first": true, "last": true, "mode": true, "percentile_cont": true,
	"percentile_disc": true, "approx_quantile": true,
}

// knownFns: functions the router/engine already handle in SOME form — their
// presence is not itself the decline signal, so they don't claim `fn_other`.
var knownFns = map[string]bool{
	"length": true, "substr": true, "substring": true,
	"starts_with": true, "ends_with": true, "contains": true,
	"date_trunc": true, "time_bucket": true, "read_parquet": true,
}

// CensusClassify buckets one query for the decline census. Returns a label
// (always a member of declineReasonNames — never derived from input) and the
// token count for the DEBUG fingerprint. It never calls the engine.
//
// Runs in the tagged build only, on every query that passed validation + RBAC,
// BEFORE the parallel/single dispatch — placement is load-bearing: the parallel
// executor takes exactly the simple single-table population arcx targets, so a
// census taken at the arcx hook (which only the non-parallel branch reaches)
// would systematically under-count the shapes most worth building next.
func CensusClassify(sql, headerDB string) (string, int) {
	// Session-setting guards first, mirroring eligibleShape's order — these are
	// their own buckets because they are correctness gates, not missing features.
	low := strings.ToLower(sql)
	for _, t := range tzInjectionTokens {
		if strings.Contains(low, t) {
			return reasonTzSetting.String(), 0
		}
	}
	for _, t := range collationTokens {
		if strings.Contains(low, t) {
			return reasonCollation.String(), 0
		}
	}
	for _, t := range nullOrderTokens {
		if strings.Contains(low, t) {
			return reasonNullOrder.String(), 0
		}
	}

	if m, ok := eligibleShape(sql); ok {
		if _, _, ok := resolveMeasurementToken(m.measurement, headerDB); ok {
			return reasonEligible.String(), 0
		}
		return reasonUnresolvableMeasurement.String(), 0
	}

	toks, ok := tokenize(sql)
	if !ok {
		// Outside the lexer's narrow vocabulary (arithmetic operators, ||, …).
		// An honest bucket of its own — not silently folded into a guess.
		return reasonUnlexable.String(), 0
	}
	return classifyDecline(toks).String(), len(toks)
}

// classifyDecline picks the highest-signal structural feature present.
// Priority: multi-relation / compositional shapes first (they are whole engine
// phases), then clause-level features, then projection-level ones.
func classifyDecline(toks []token) declineReason {
	var hasJoin, hasSetOp, hasWindow, hasHaving, hasGroupBy bool
	var hasDistinct, hasOffset, hasStar, hasAggFn, hasFnOther bool

	if len(toks) > 0 && toks[0].kind == tokIdent && toks[0].lower == "with" {
		return reasonCTE
	}
	for i, t := range toks {
		if t.kind == tokPunct && t.punct == '*' {
			// `count(*)` is the agg's star, not a star projection.
			if i == 0 || toks[i-1].kind != tokPunct || toks[i-1].punct != '(' {
				hasStar = true
			}
			continue
		}
		if t.kind != tokIdent {
			continue
		}
		switch t.lower {
		case "join":
			hasJoin = true
		case "union", "intersect", "except":
			hasSetOp = true
		case "over":
			hasWindow = true
		case "having":
			hasHaving = true
		case "group":
			if i+1 < len(toks) && toks[i+1].kind == tokIdent && toks[i+1].lower == "by" {
				hasGroupBy = true
			}
		case "distinct":
			hasDistinct = true
		case "offset":
			hasOffset = true
		default:
			// ident immediately followed by `(` is a function call.
			if i+1 < len(toks) && toks[i+1].kind == tokPunct && toks[i+1].punct == '(' {
				switch {
				case aggFns[t.lower]:
					hasAggFn = true
				case knownFns[t.lower]:
					// already-handled function: not the signal
				default:
					hasFnOther = true
				}
			}
		}
	}

	switch {
	case hasSetOp:
		return reasonSetOp
	case hasJoin:
		return reasonJoin
	case hasWindow:
		return reasonWindow
	case hasHaving:
		return reasonHaving
	case hasGroupBy:
		return reasonGroupBy
	case hasDistinct:
		return reasonDistinct
	case hasAggFn:
		return reasonAggFn
	case hasOffset:
		return reasonOffset
	case hasStar:
		return reasonStarProjection
	case hasFnOther:
		return reasonFnOther
	}
	return reasonNoneIneligible
}
