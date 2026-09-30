package ingest

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"strings"
)

// availableMemoryBytes reports an OS/container memory headroom estimate for
// control-path reserve allocation. It is never called from the ordinary write
// path. Unknown limits are reported as unknown so reserve activation can fail
// closed instead of risking a large allocation with no preflight evidence.
func availableMemoryBytes() (uint64, bool) {
	if limit, ok := readMemoryNumber("/sys/fs/cgroup/memory.max"); ok && limit < math.MaxInt64 {
		if current, currentOK := readMemoryNumber("/sys/fs/cgroup/memory.current"); currentOK && limit >= current {
			return limit - current, true
		}
	}
	if limit, ok := readMemoryNumber("/sys/fs/cgroup/memory/memory.limit_in_bytes"); ok && limit < 1<<60 {
		if current, currentOK := readMemoryNumber("/sys/fs/cgroup/memory/memory.usage_in_bytes"); currentOK && limit >= current {
			return limit - current, true
		}
	}
	if available, ok := procMemAvailable(); ok {
		return available, true
	}
	return platformAvailableMemoryBytes()
}

func readMemoryNumber(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	value := strings.TrimSpace(string(b))
	if value == "" || value == "max" {
		return 0, false
	}
	n, err := strconv.ParseUint(value, 10, 64)
	return n, err == nil
}

func procMemAvailable() (uint64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if !strings.HasPrefix(s.Text(), "MemAvailable:") {
			continue
		}
		fields := strings.Fields(s.Text())
		if len(fields) < 2 {
			return 0, false
		}
		kib, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || kib > math.MaxUint64/1024 {
			return 0, false
		}
		return kib * 1024, true
	}
	return 0, false
}
