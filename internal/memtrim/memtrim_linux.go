//go:build linux && cgo

package memtrim

/*
#ifdef __GLIBC__
#include <malloc.h>
#else
// musl, uClibc, and other Linux libcs do not provide malloc_trim.
// Treat as a no-op so cgo builds on Alpine etc. still compile.
static int malloc_trim(size_t pad) { (void)pad; return 0; }
#endif
*/
import "C"

import (
	"time"

	"github.com/basekick-labs/arc/internal/throttle"
)

// trimDebounce throttles ReleaseToOS so concurrent retention/delete/compaction
// callers can't serialize the process under glibc's allocator lock.
var trimDebounce = throttle.New(30 * time.Second)

// ReleaseToOS asks glibc to return free heap pages to the OS via malloc_trim(0).
// debug.FreeOSMemory only releases Go-managed memory; CGo allocations from the
// DuckDB httpfs extension live in glibc arenas that need an explicit trim.
//
// Throttled to once per 30s across the whole process. Calls that arrive inside
// the window return false without invoking the C function.
// On non-glibc Linux libcs (musl, uClibc) the C call is a stub and always
// returns 0; outside Linux see memtrim_other.go.
func ReleaseToOS() bool {
	if !trimDebounce.TryAcquire() {
		return false
	}
	return C.malloc_trim(0) == 1
}
