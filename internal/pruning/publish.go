package pruning

import (
	"context"
	"hash/fnv"
	"strings"
	"sync/atomic"
	"time"
)

// A file that appears in a partition a query has already found empty.
//
// The caches of this package remember that a partition has no files (the glob
// cache, 30 s) and the list of partitions a query was pruned to (the partition
// cache, 60 s; the SQL transform cache above it bakes the same list into the
// rewritten SQL for 60 s). That is safe for the current hour, whose absence is
// never remembered (live_hours.go), and for a partition that never changes.
// It was wrong for a row stamped in any other hour: late data, or a row
// stamped ahead of the clock. Its file was written, and queries kept answering
// without that hour until the entries expired (#1757: 30 s for any query,
// 60 s for a repeated one, measured).
//
// The fix is a generation per measurement. FilePublished moves it. Every
// cached listing and pruned list carries the generation its measurement had
// BEFORE the listing started, and an entry whose generation has moved is a
// miss. Nothing is scanned or deleted when a file is published: it is one
// atomic add, so it is safe to call from a flush worker for every file, and
// it cannot fail or block the flush.
//
// Why "before the listing started": a listing that ran just before the file
// landed and is stored just after carries the old generation, and is never
// read. Stamping at store time would let it live its whole TTL.
//
// Generations are striped: measurements share a stripe by hash. A collision
// only makes an unrelated measurement list again once.

// publishStripes is the number of generation counters measurements hash onto.
const publishStripes = 256

// listScope is a measurement's generation as read before a listing. The zero
// value belongs to no measurement and never goes stale.
type listScope struct {
	stripe *atomic.Uint64
	gen    uint64
}

// current reports whether no file has been published for the measurement
// since the scope was taken.
func (s listScope) current() bool {
	return s.stripe == nil || s.stripe.Load() == s.gen
}

func publishStripe(database, measurement string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(database))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(measurement))
	return int(h.Sum32() % publishStripes)
}

// scopeFor reads the measurement's generation. Call it before listing.
func (p *PartitionPruner) scopeFor(database, measurement string) listScope {
	s := &p.published[publishStripe(database, measurement)]
	return listScope{stripe: s, gen: s.Load()}
}

type listScopeKey struct{}

// withListScope attaches the scope every listing made under ctx is stored with.
func withListScope(ctx context.Context, s listScope) context.Context {
	return context.WithValue(ctx, listScopeKey{}, s)
}

// scopeFrom returns the scope attached to ctx, or the zero scope.
func scopeFrom(ctx context.Context) listScope {
	s, _ := ctx.Value(listScopeKey{}).(listScope)
	return s
}

// splitStorageGlob reads the database and measurement out of an unpruned
// storage glob, "{root}/{database}/{measurement}/**/*.parquet". It accepts
// exactly what storagePathPattern accepts, without running a regular
// expression on every query.
func splitStorageGlob(glob string) (database, measurement string, ok bool) {
	rest, found := strings.CutSuffix(glob, "/**/*.parquet")
	if !found {
		return "", "", false
	}
	i := strings.LastIndexByte(rest, '/')
	if i < 0 || i == len(rest)-1 {
		return "", "", false
	}
	measurement = rest[i+1:]
	rest = rest[:i]
	j := strings.LastIndexByte(rest, '/')
	if j < 0 || j == len(rest)-1 {
		return "", "", false
	}
	return rest[j+1:], measurement, true
}

// FilePublished tells the pruner that a file for the measurement, in the hour
// partitionTime falls in, is now in storage. Queries stop using anything
// cached for that measurement before this call.
//
// A file in the current hour changes nothing and is skipped: the current
// hour's absence is never cached, and skipping it keeps the caches of a
// measurement that is being written to. "Current" is the hour of the clock
// alone, not the live window: the previous hour is live only for a few
// minutes, and an entry made after that could have left it out.
//
// It only adds to an atomic. Call it after the file is readable, never before.
func (p *PartitionPruner) FilePublished(database, measurement string, partitionTime time.Time) {
	if partitionTime.UTC().Truncate(time.Hour).Equal(p.nowFn().UTC().Truncate(time.Hour)) {
		return
	}
	p.published[publishStripe(database, measurement)].Add(1)
}

// PublishSnapshot is every measurement's generation at one moment.
type PublishSnapshot [publishStripes]uint64

// PublishSnapshot reads the generations. A caller that will cache something
// derived from listings takes it BEFORE the work that lists, when it does not
// yet know which measurements that work will read.
func (p *PartitionPruner) PublishSnapshot() *PublishSnapshot {
	var s PublishSnapshot
	for i := range p.published {
		s[i] = p.published[i].Load()
	}
	return &s
}

// PublishStamp is one measurement's generation out of a snapshot.
type PublishStamp struct {
	stripe uint16
	gen    uint64
}

// Stamp picks the measurement's generation out of a snapshot.
func (s *PublishSnapshot) Stamp(database, measurement string) PublishStamp {
	i := publishStripe(database, measurement)
	return PublishStamp{stripe: uint16(i), gen: s[i]}
}

// StampsCurrent reports whether no file has been published, since the
// snapshot the stamps came from, for any of their measurements.
func (p *PartitionPruner) StampsCurrent(stamps []PublishStamp) bool {
	for _, st := range stamps {
		if p.published[st.stripe].Load() != st.gen {
			return false
		}
	}
	return true
}
