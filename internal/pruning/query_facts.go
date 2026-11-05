package pruning

import (
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"
)

// timeRangeScans counts scanTimeBoundsAt calls: a test seam, read through
// TimeRangeScans, that pins how many times a query's SQL is scanned for its
// range, whichever path reads it.
var timeRangeScans atomic.Int64

// TimeRangeScans returns how many times any query's SQL has been scanned for
// its time range since the process started. Tests take the difference across
// one rewrite.
func TimeRangeScans() int64 { return timeRangeScans.Load() }

// queryFactsCtxKey keys the per-query memo WithQueryFacts installs.
type queryFactsCtxKey struct{}

// queryFacts memoises what the pruner reads from a query's SQL alone — its
// time range, whether that range is relative to now, and the SQL's hash for
// the partition-cache key — so a query is scanned once, not once per table
// reference (#1479).
//
// The rewriter calls the pruner once per table reference, always with the
// same SQL, and each call used to re-scan the whole query: quadratic in the
// query's length for a query made of many short references. A comma join
// (`FROM x, x, x, …`) took 5.6 s at the default 10 KB query limit.
//
// Every value here depends on the SQL only, never on the table, so sharing it
// across references cannot change a result. The time range is read at the
// first reference's now; a range only exists for a single-table query (see
// scanTimeBoundsAt), so in practice it is shared by one reference's calls
// (the cold-tier guard, the tier split and the partition pruning).
type queryFacts struct {
	mu        sync.Mutex
	sql       string
	have      bool
	timeRange *TimeRange
	relative  bool
	sqlHash   [sha256.Size]byte
	computed  int
}

// WithQueryFacts returns ctx with a per-query memo for the pruner's SQL-only
// work attached. Install it once per query, at the start of the rewrite, on a
// context that lives for that query only. A context that already carries one
// is returned unchanged. The memo is keyed by the SQL text, so a context
// reused for another query recomputes rather than returning stale facts.
func WithQueryFacts(ctx context.Context) context.Context {
	if _, ok := ctx.Value(queryFactsCtxKey{}).(*queryFacts); ok {
		return ctx
	}
	return context.WithValue(ctx, queryFactsCtxKey{}, &queryFacts{})
}

// QueryFactsComputed reports how many times the memo on ctx computed the
// facts, or -1 when ctx carries none. A test seam: one query must compute
// them once, however many tables it references.
func QueryFactsComputed(ctx context.Context) int {
	qf, ok := ctx.Value(queryFactsCtxKey{}).(*queryFacts)
	if !ok {
		return -1
	}
	qf.mu.Lock()
	defer qf.mu.Unlock()
	return qf.computed
}

// facts returns the SQL-only facts for sql, from ctx's memo when it holds
// them for this SQL, computing and storing them otherwise. Without a memo it
// computes them, as every call did before #1479. It does not depend on
// p.enabled: the cold-tier guard and the tier split read the range whether
// or not partition pruning is on.
//
// The memo compares the SQL with ==, which returns at once when both strings
// share their bytes; the rewriter passes the same string to every call.
func (p *PartitionPruner) facts(ctx context.Context, sql string) (tr *TimeRange, relative bool, hash [sha256.Size]byte) {
	qf, ok := ctx.Value(queryFactsCtxKey{}).(*queryFacts)
	if !ok {
		return p.ExtractTimeRange(sql), relativeTimeWord.MatchString(sql), sha256.Sum256([]byte(sql))
	}
	qf.mu.Lock()
	defer qf.mu.Unlock()
	if !qf.have || qf.sql != sql {
		qf.sql = sql
		qf.timeRange = p.ExtractTimeRange(sql)
		qf.relative = relativeTimeWord.MatchString(sql)
		qf.sqlHash = sha256.Sum256([]byte(sql))
		qf.have = true
		qf.computed++
	}
	return qf.timeRange, qf.relative, qf.sqlHash
}

// ExtractTimeRangeCtx is ExtractTimeRange through ctx's per-query memo (see
// WithQueryFacts): the query is scanned once however many tables it
// references.
func (p *PartitionPruner) ExtractTimeRangeCtx(ctx context.Context, sql string) *TimeRange {
	tr, _, _ := p.facts(ctx, sql)
	return tr
}
