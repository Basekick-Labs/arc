package pruning

import (
	"regexp"
	"strconv"
	"time"
)

// The current hour is "live": its directory appears at its first flush, at
// any moment, so its absence must never be remembered: not in the glob
// cache, not in a parent listing, and not in a pruned path list cached by the
// partition cache or the SQL transform cache. Before, a dashboard query run
// once before a new hour's first flush kept answering without that hour for a
// minute or more (the caches chain).
//
// The previous hour stays live for liveGrace after the boundary: rows stamped
// 13:59:58 flush at 14:00:03 (max_buffer_age), and a replicated reader pulls
// the file later still (covered when it pulls within liveGrace). Hours not started yet are not live: only data stamped
// in the future lands there, and their absence is cached as before. Every
// cached pruned list is keyed by the current hour (liveEpoch), so a list made
// before an hour started is never served after it.
//
// Day paths (compacted files of a whole day) are never live: compaction is
// their only writer, and it invalidates every query cache when it finishes.

// liveGrace is how long the previous hour stays live after the boundary.
const liveGrace = 5 * time.Minute

// partitionTimePattern matches the time segments of a partition path:
// .../YYYY/MM/DD/HH/*.parquet (an hour) or .../YYYY/MM/DD/*.parquet (a day of
// compacted files), with either separator.
var partitionTimePattern = regexp.MustCompile(`[/\\](\d{4})[/\\](\d{2})[/\\](\d{2})(?:[/\\](\d{2}))?[/\\]\*\.parquet$`)

// liveWindow is the span of hour starts that are live at now.
type liveWindow struct{ from, to time.Time }

func liveWindowAt(now time.Time) liveWindow {
	return liveWindow{from: now.Add(-liveGrace).Truncate(time.Hour), to: now.Truncate(time.Hour)}
}

func (p *PartitionPruner) liveWindow() liveWindow {
	return liveWindowAt(p.nowFn())
}

// liveEpoch is the start of the current hour, part of every cache key that
// holds a pruned path list.
func (p *PartitionPruner) liveEpoch() int64 {
	return p.nowFn().Truncate(time.Hour).Unix()
}

// LiveEpoch is the start of the current hour (Unix seconds).
func (p *PartitionPruner) LiveEpoch() int64 { return p.liveEpoch() }

// CacheEpoch keys a cached pruning of sql: the current hour and, for a query
// whose range is relative to now(), the hours that range starts and ends in.
// The current hour alone is not enough there: a relative end (time < now() -
// INTERVAL '10 minutes') crosses into a new hour partway through the current
// one, and an entry cached just before it did would leave that hour out for
// the rest of its TTL. A range of literals cannot move, so it costs no scan.
func (p *PartitionPruner) CacheEpoch(sql string) string {
	if !relativeTimeWord.MatchString(sql) {
		return strconv.FormatInt(p.liveEpoch(), 10)
	}
	return p.epochFor(p.ExtractTimeRange(sql))
}

// epochFor is CacheEpoch for an already extracted range.
func (p *PartitionPruner) epochFor(tr *TimeRange) string {
	if tr == nil {
		return strconv.FormatInt(p.liveEpoch(), 10)
	}
	return strconv.FormatInt(p.liveEpoch(), 10) + ":" +
		strconv.FormatInt(tr.Start.Truncate(time.Hour).Unix(), 10) + ":" +
		strconv.FormatInt(tr.End.Truncate(time.Hour).Unix(), 10)
}

// relativeTimeWord finds the forms a range can be relative to now with.
var relativeTimeWord = regexp.MustCompile(`(?i)\b(now\s*\(|current_timestamp)`)

// SetClock replaces the pruner's clock. Tests only.
func (p *PartitionPruner) SetClock(now func() time.Time) { p.nowFn = now }

// isLive reports whether a partition path can still gain its first files
// from ingest. A path it cannot read is treated as live (never remembered as
// absent).
func (w liveWindow) isLive(path string) bool {
	m := partitionTimePattern.FindStringSubmatch(path)
	if m == nil {
		return true
	}
	if m[4] == "" {
		return false // a day path: written by compaction only
	}
	t, err := time.ParseInLocation("2006/01/02/15", m[1]+"/"+m[2]+"/"+m[3]+"/"+m[4], time.UTC)
	if err != nil {
		return true
	}
	return !t.Before(w.from) && !t.After(w.to)
}

// droppedLive reports whether a live partition of generated is not in
// existing.
func (w liveWindow) droppedLive(generated, existing []string) bool {
	kept := make(map[string]struct{}, len(existing))
	for _, e := range existing {
		kept[e] = struct{}{}
	}
	for _, g := range generated {
		if _, ok := kept[g]; !ok && w.isLive(g) {
			return true
		}
	}
	return false
}
