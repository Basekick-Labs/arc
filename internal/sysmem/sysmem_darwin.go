//go:build darwin

package sysmem

import "golang.org/x/sys/unix"

// limit returns the machine's physical memory. macOS has no cgroups, so there is
// no per-process limit to find.
//
// Darwin is a RELEASE target, not just a development convenience: release-build
// produces a darwin-arm64 artifact for the Homebrew tap. PR #967's elastic
// reserve read /proc/meminfo only and therefore could not be enabled on macOS at
// all; this file exists so that cannot happen again.
//
// x/sys/unix rather than stdlib syscall: syscall.SysctlUint64 does not exist on
// darwin, and plain syscall.Sysctl returns the raw bytes as a string AND strips a
// trailing NUL — which silently truncates any little-endian value whose high byte
// is zero. 36 GiB is 0x900000000, so its last byte is zero and the value would
// come back as 7 bytes.
func limit() (uint64, Source, bool) {
	v, err := unix.SysctlUint64("hw.memsize")
	if err != nil || v == 0 {
		return 0, SourceUnknown, false
	}
	return v, SourceSysctl, true
}
