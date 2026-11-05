//go:build !linux

package license

import "time"

// Non-Linux platforms use Go's process monotonic clock. Suspend-inclusive clock
// support on these platforms needs separate runtime acceptance before release.
func newLicenseTicker() func() time.Time { return time.Now }
