package storage

import (
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// StripRewriteSuffix returns the logical parquet path represented by an
// immutable DELETE rewrite. A rewrite is named <logical>_rewrite_<unixnano>.parquet.
// Non-rewrite names are returned unchanged. Keeping this parser in storage
// gives compaction, tiering, pruning and DELETE one definition of the filename
// convention.
func StripRewriteSuffix(filename string) string {
	const marker = "_rewrite_"
	if !strings.HasSuffix(filename, ".parquet") {
		return filename
	}
	base := strings.TrimSuffix(filename, ".parquet")
	idx := strings.LastIndex(base, marker)
	if idx < 0 || idx+len(marker) == len(base) {
		return filename
	}
	for _, r := range base[idx+len(marker):] {
		if r < '0' || r > '9' {
			return filename
		}
	}
	return base[:idx] + ".parquet"
}

// RewriteFamily returns the family of the file at path: the path with any
// `_rewrite_<n>` suffix stripped. A row DELETE names each rewrite of a file
// after its origin (rewritePath), so every rewrite of one file, and the file
// itself, share a family; two members in storage at once means a superseded
// one was not removed.
func RewriteFamily(path string) string {
	return StripRewriteSuffix(path)
}

// RewriteFamilies groups paths by RewriteFamily, keeping only families with
// more than one member.
func RewriteFamilies(paths []string) map[string][]string {
	all := make(map[string][]string, len(paths))
	for _, p := range paths {
		f := RewriteFamily(p)
		all[f] = append(all[f], p)
	}
	for f, members := range all {
		if len(members) < 2 {
			delete(all, f)
		}
	}
	return all
}

// lastNameStamp is the last stamp NameTime issued, in Unix nanoseconds.
var lastNameStamp atomic.Int64

// NameTime returns the time to stamp a new file name with: now, or 1 ns past
// the last stamp this process issued when now is not later than it, so every
// name built from it is unique within the process (#1282). Flush workers run
// concurrently and the clock repeats a value between them (macOS resolves
// microseconds); two flushes of one measurement and hour then got one name and
// the second write replaced the first. Build every part of a name (the
// YYYYMMDD_HHMMSS and the nanos) from the one returned value. Two processes
// writing to one bucket are not kept apart.
func NameTime(now time.Time) time.Time {
	n := now.UnixNano()
	for {
		last := lastNameStamp.Load()
		next := n
		if next <= last {
			next = last + 1
		}
		if lastNameStamp.CompareAndSwap(last, next) {
			return time.Unix(0, next).UTC()
		}
	}
}

// IngestFileTime extracts the flush timestamp from an ingest Parquet file
// name: {measurement}_{YYYYMMDD}_{HHMMSS}_{nanos}.parquet, the time NameTime
// returned just before the file was written. Parsing is deliberately strict
// (exactly an 8-digit date and a 6-digit time that time.Parse accepts, an
// all-digit last token, and a measurement token present) so no other name
// shape (hourly and daily compacted outputs, hand-placed files) can parse to
// a wrong time; anything else fails.
//
// A DELETE rewrite (<name>_rewrite_<unixnano>.parquet) parses to the time of
// the file it rewrote, not of the rewrite: a caller that must tell them apart
// compares StripRewriteSuffix(name) with name.
func IngestFileTime(name string) (time.Time, bool) {
	base := strings.TrimSuffix(filepath.Base(StripRewriteSuffix(name)), ".parquet")
	toks := strings.Split(base, "_")
	if len(toks) < 4 {
		return time.Time{}, false
	}
	d, hms, nanos := toks[len(toks)-3], toks[len(toks)-2], toks[len(toks)-1]
	if len(d) != 8 || len(hms) != 6 || !allDigits(d) || !allDigits(hms) || !allDigits(nanos) {
		return time.Time{}, false
	}
	ts, err := time.Parse("20060102150405", d+hms)
	if err != nil {
		return time.Time{}, false
	}
	// The last token is the flush time's nanoseconds within its second
	// (%09d). Without it a file flushed at hh:mm:ss.9 read as hh:mm:ss.0.
	if len(nanos) == 9 {
		n, _ := strconv.Atoi(nanos)
		ts = ts.Add(time.Duration(n))
	}
	return ts, true
}

func allDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
