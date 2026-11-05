//go:build linux

package license

import (
	"golang.org/x/sys/unix"
	"time"
)

// CLOCK_BOOTTIME includes suspend time. The Go monotonic clock can stop while
// the host sleeps; that must not give an expired trial extra time on resume.
func newLicenseTicker() func() time.Time {
	return func() time.Time {
		var value unix.Timespec
		if unix.ClockGettime(unix.CLOCK_BOOTTIME, &value) != nil {
			return time.Time{}
		}
		return time.Unix(value.Sec, value.Nsec)
	}
}
