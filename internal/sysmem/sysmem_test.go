package sysmem

import "testing"

const (
	oneGiB = uint64(1) << 30
	twoGiB = uint64(2) << 30
	// The cgroup v1 "unlimited" sentinel is (LONG_MAX / PAGE_SIZE) * PAGE_SIZE,
	// so it DIFFERS BY PAGE SIZE. Both of these are real values seen in the
	// wild, and linux/arm64 with 64K pages is a release target — which is why
	// nothing here compares against a constant.
	v1Unlimited4K  = uint64(9223372036854771712)
	v1Unlimited64K = uint64(9223372036854710272)
)

func TestParseCgroupValue(t *testing.T) {
	for _, tc := range []struct {
		name     string
		contents string
		want     uint64
		wantOK   bool
	}{
		{"plain value", "536870912\n", 536870912, true},
		{"no trailing newline", "536870912", 536870912, true},
		{"v2 unlimited", "max\n", 0, false},
		{"v2 unlimited with spaces", "  max  ", 0, false},
		{"empty", "", 0, false},
		{"whitespace only", "   \n", 0, false},
		{"zero is not a limit", "0\n", 0, false},
		{"malformed", "not-a-number\n", 0, false},
		// Parsed as a number here; it is effectiveLimit that recognises it as
		// unlimited, by comparing against physical memory.
		{"v1 sentinel 4K parses", "9223372036854771712\n", v1Unlimited4K, true},
		{"v1 sentinel 64K parses", "9223372036854710272\n", v1Unlimited64K, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseCgroupValue(tc.contents)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("parseCgroupValue(%q) = (%d, %v), want (%d, %v)", tc.contents, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// TestEffectiveLimit_UnlimitedSentinelIsPageSizeIndependent is the regression
// that matters most here.
//
// Reading the v1 sentinel as a literal limit gives ~9 exabytes, which passes
// every plausible sanity check and lands back on whatever cap sits above it —
// i.e. the original #1026 bug, surviving the fix. Comparing against physical
// memory catches it regardless of page size; comparing against a constant does
// not, and 4K and 64K hosts disagree.
func TestEffectiveLimit_UnlimitedSentinelIsPageSizeIndependent(t *testing.T) {
	for _, sentinel := range []uint64{v1Unlimited4K, v1Unlimited64K} {
		if _, ok := effectiveLimit(sentinel, 0, 16*oneGiB, true); ok {
			t.Fatalf("effectiveLimit treated the v1 unlimited sentinel %d as a real limit", sentinel)
		}
	}
}

func TestEffectiveLimit(t *testing.T) {
	type tc struct {
		name            string
		maxLimit        uint64
		high            uint64
		physical        uint64
		requirePhysical bool
		want            uint64
		wantOK          bool
	}
	for _, c := range []tc{
		{name: "plain limit below physical", maxLimit: twoGiB, physical: 16 * oneGiB, want: twoGiB, wantOK: true},
		{name: "no limit at all", physical: 16 * oneGiB},
		// A cgroup limit may legitimately exceed physical memory; using it
		// unclamped would reintroduce over-provisioning by another route.
		{name: "limit above physical is not a limit", maxLimit: 32 * oneGiB, physical: 16 * oneGiB},
		{name: "limit equal to physical is kept", maxLimit: 16 * oneGiB, physical: 16 * oneGiB, want: 16 * oneGiB, wantOK: true},
		// Kubernetes MemoryQoS sets memory.high below memory.max; the kernel
		// begins reclaiming there, so that is the effective ceiling.
		{name: "memory.high lower than max wins", maxLimit: twoGiB, high: oneGiB, physical: 16 * oneGiB, want: oneGiB, wantOK: true},
		{name: "memory.high above max is ignored", maxLimit: oneGiB, high: twoGiB, physical: 16 * oneGiB, want: oneGiB, wantOK: true},
		{name: "memory.high alone is not a limit", high: oneGiB, physical: 16 * oneGiB},

		// cgroup v2 needs no physical reading: unlimited there is the literal
		// "max", which parseCgroupValue rejects before reaching here.
		{name: "v2 with no physical reading still works", maxLimit: twoGiB, want: twoGiB, wantOK: true},

		// cgroup v1 DOES need one. Unlimited there is a huge sentinel, and the
		// only thing distinguishing it from a real limit is exceeding the
		// machine — so with no physical reading it must refuse rather than accept
		// a ~9 exabyte "limit" (#1026, through the path meant to prevent it).
		{name: "v1 with no physical reading must refuse", maxLimit: twoGiB, requirePhysical: true},
		{name: "v1 sentinel 4K with no physical reading must refuse", maxLimit: v1Unlimited4K, requirePhysical: true},
		{name: "v1 sentinel 64K with no physical reading must refuse", maxLimit: v1Unlimited64K, requirePhysical: true},
		{name: "v1 real limit with physical is kept", maxLimit: twoGiB, physical: 16 * oneGiB, requirePhysical: true, want: twoGiB, wantOK: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := effectiveLimit(c.maxLimit, c.high, c.physical, c.requirePhysical)
			if ok != c.wantOK || got != c.want {
				t.Fatalf("effectiveLimit(%d, %d, %d, %v) = (%d, %v), want (%d, %v)",
					c.maxLimit, c.high, c.physical, c.requirePhysical, got, ok, c.want, c.wantOK)
			}
		})
	}
}

func TestParseMemTotal(t *testing.T) {
	const meminfo = "MemTotal:       16318268 kB\nMemFree:         1234567 kB\n"
	got, ok := parseMemTotal(meminfo)
	if !ok || got != 16318268*1024 {
		t.Fatalf("parseMemTotal = (%d, %v), want (%d, true)", got, ok, 16318268*1024)
	}
	for _, bad := range []string{"", "MemFree: 123 kB\n", "MemTotal:\n", "MemTotal: nope kB\n", "MemTotal: 0 kB\n"} {
		if _, ok := parseMemTotal(bad); ok {
			t.Fatalf("parseMemTotal(%q) reported success", bad)
		}
	}
}

// TestParseSelfCgroupV2Path covers the --cgroupns=host case: the real root
// cgroup has no memory interface files, so the container's own limit has to be
// found through the path named in /proc/self/cgroup.
func TestParseSelfCgroupV2Path(t *testing.T) {
	for _, tc := range []struct {
		name     string
		contents string
		want     string
		wantOK   bool
	}{
		{"docker under cgroupns=host", "0::/docker/abc123\n", "/docker/abc123", true},
		{"kubernetes path", "0::/kubepods/burstable/podXYZ/container\n", "/kubepods/burstable/podXYZ/container", true},
		{"private namespace root is not useful", "0::/\n", "", false},
		{"v1-only output has no v2 line", "11:memory:/docker/abc\n4:cpu:/docker/abc\n", "", false},
		{"mixed, v2 line present", "11:memory:/docker/abc\n0::/docker/abc\n", "/docker/abc", true},
		{"empty", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseSelfCgroupV2Path(tc.contents)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("parseSelfCgroupV2Path(%q) = (%q, %v), want (%q, %v)", tc.contents, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
