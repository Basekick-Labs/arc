// Package sysmem reports how much memory this process may actually use.
//
// It exists for one narrow reason. DuckDB already performs its own cgroup-aware
// detection — duckdb::CGroups::GetMemoryLimit is in the linked library, and with
// no memory_limit set DuckDB defaults to 80% of the detected limit — so Arc does
// NOT need this to size the main database. What Arc does need it for is the
// compaction subprocess limit: those are separate processes in the SAME cgroup,
// and if each one falls back to DuckDB's own default they take 80% of the whole
// container each, so a main process plus two subprocesses budgets 240% of the
// box. Dividing a share requires a concrete number, and that is what this
// package supplies.
//
// Everything that parses or decides lives in this file, deliberately untagged,
// so the table tests run on every platform. Only the I/O is platform-split, in
// the sibling files, following internal/wal/datasync_{linux,other}.go.
package sysmem

import (
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// Source says where a limit came from, so a startup log can distinguish a real
// cgroup limit from a fallback to host memory. A typed set rather than a bare
// string: the producer and the log drift otherwise.
type Source string

const (
	SourceCgroupV2 Source = "cgroup_v2"
	SourceCgroupV1 Source = "cgroup_v1"
	SourceMemInfo  Source = "meminfo"
	SourceSysctl   Source = "hw.memsize"
	SourceUnknown  Source = "unknown"
)

// maxReadBytes bounds every file read. These paths can be FUSE-backed (lxcfs),
// and this runs inside config.Load() before anything else exists, so a hung
// read would hang startup. Any error means "not detected" rather than a failure.
const maxReadBytes = 64 << 10

// rootFS is the filesystem the Linux reader walks. Overridden in tests with an
// fstest.MapFS; unexported so no test-only surface escapes the package.
var rootFS fs.FS = os.DirFS("/")

// Limit reports the memory this process may use.
//
// ok is false when nothing could be determined, in which case callers must keep
// whatever behaviour they had rather than substituting a guess.
func Limit() (bytes uint64, source Source, ok bool) {
	return limit()
}

// readFileLimited reads a small file, bounded. Paths are rootFS-relative, i.e.
// without the leading slash.
func readFileLimited(fsys fs.FS, name string) (string, bool) {
	f, err := fsys.Open(strings.TrimPrefix(name, "/"))
	if err != nil {
		return "", false
	}
	defer f.Close()

	// io.ReadAll over a LimitReader, not a single Read: a short read accepted as
	// the whole file is not merely incomplete, it is actively harmful here. A
	// truncated "MemTotal: 167" parses to 171 kB of "physical memory", and
	// effectiveLimit treats physical as authoritative — so it would then DISCARD a
	// perfectly good 2 GiB cgroup limit for exceeding it.
	b, err := io.ReadAll(io.LimitReader(f, maxReadBytes))
	if err != nil || len(b) == 0 {
		return "", false
	}
	return string(b), true
}

// parseCgroupValue reads a cgroup memory file's single-value contents.
//
// "max" (cgroup v2's unlimited) returns ok == false. A v1 unlimited cgroup
// instead stores a huge sentinel, which this does NOT special-case: the caller
// treats any value above physical memory as unlimited. That is deliberate —
// the sentinel is (LONG_MAX / PAGE_SIZE) * PAGE_SIZE, so it differs between a
// 4K-page host (…771712) and a 64K-page one (…710272), and linux/arm64 with 64K
// pages is a release target. Comparing against physical memory is page-size
// independent; comparing against a constant is not.
func parseCgroupValue(contents string) (uint64, bool) {
	s := strings.TrimSpace(contents)
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 {
		return 0, false
	}
	return v, true
}

// parseMemTotal pulls MemTotal (in kB) out of /proc/meminfo contents.
func parseMemTotal(contents string) (uint64, bool) {
	for _, line := range strings.Split(contents, "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		// The unit is checked rather than assumed. /proc/meminfo has always used
		// kB, but silently multiplying an unexpected unit by 1024 would produce a
		// confidently wrong "physical memory" figure, and physical is what
		// effectiveLimit trusts over a real cgroup limit.
		if len(fields) < 3 || fields[2] != "kB" {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || kb == 0 {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// parseSelfCgroupV2Path extracts the cgroup v2 path from /proc/self/cgroup,
// whose v2 line is "0::/some/path".
//
// Needed because the root read fails under --cgroupns=host: the real root
// cgroup has no memory interface files, and the container's own limit lives at
// its path instead. Under a private namespace the root read is what works, and
// also rescues a process re-parented into /init.scope — so both are tried, root
// first.
func parseSelfCgroupV2Path(contents string) (string, bool) {
	for _, line := range strings.Split(contents, "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) == 3 && parts[0] == "0" && parts[1] == "" && parts[2] != "" && parts[2] != "/" {
			return parts[2], true
		}
	}
	return "", false
}

// effectiveLimit folds a candidate cgroup limit together with physical memory.
//
// Two rules, both load-bearing:
//   - a candidate above physical memory is not a limit at all. This is how the
//     cgroup v1 unlimited sentinel is handled, page-size independently.
//   - memory.high, where set, is where the kernel begins reclaiming under
//     Kubernetes MemoryQoS, so the effective ceiling is below memory.max.
//
// requirePhysical exists for cgroup v1. There, "unlimited" is a huge sentinel
// rather than a literal "max", and the ONLY thing that distinguishes it from a
// real limit is being larger than the machine. With no physical reading the
// clamp cannot fire, and the sentinel sails through as a ~9 exabyte limit — which
// is #1026 reborn, through the one code path whose job is to prevent it.
// Measured: that produces a per-subprocess limit of 2459565876494606882B, which
// DuckDB cheerfully accepts as 2184.5 PiB.
//
// cgroup v2 does not need it: unlimited there is the literal string "max", which
// parseCgroupValue already rejects before this is reached.
func effectiveLimit(maxLimit, highLimit, physical uint64, requirePhysical bool) (uint64, bool) {
	if requirePhysical && physical == 0 {
		return 0, false
	}
	// A max is REQUIRED. memory.high on its own is a throttle point inside an
	// otherwise unlimited cgroup, not a limit, and treating it as one would
	// invent a ceiling where the operator set none. The caller never reaches here
	// without a max — cgroupV2Limit returns early on "max" — so this is the
	// contract made explicit rather than a reachable branch.
	if maxLimit == 0 {
		return 0, false
	}
	candidate := maxLimit
	if highLimit > 0 && highLimit < candidate {
		candidate = highLimit
	}
	if physical > 0 && candidate > physical {
		return 0, false
	}
	return candidate, true
}
