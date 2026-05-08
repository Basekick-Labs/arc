//go:build !linux

package api

import "github.com/rs/zerolog"

// readProcStatus is a no-op outside Linux. /proc/self/status doesn't exist
// on darwin or windows, so the memstats response simply omits the process
// field on those platforms.
func readProcStatus(_ zerolog.Logger) *processMemStats { return nil }

// readGlibcHeap is a no-op outside Linux. The [heap] pseudo-segment is a
// linux-kernel concept; macOS uses libmalloc with different bookkeeping.
func readGlibcHeap(_ zerolog.Logger) *glibcHeapStats { return nil }
