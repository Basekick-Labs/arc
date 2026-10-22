package storage

import (
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
