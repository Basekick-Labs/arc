//go:build linux

package sysmem

import "path"

// Paths are rootFS-relative (no leading slash) because rootFS is os.DirFS("/").
const (
	cgroupV2Max  = "sys/fs/cgroup/memory.max"
	cgroupV2High = "sys/fs/cgroup/memory.high"
	cgroupV1Max  = "sys/fs/cgroup/memory/memory.limit_in_bytes"
	procMemInfo  = "proc/meminfo"
	procSelfCG   = "proc/self/cgroup"
	cgroupRoot   = "sys/fs/cgroup"
)

// limit resolves the Linux memory limit: cgroup v2, then v1, then host memory.
//
// Physical memory is read first because it is the yardstick that decides whether
// a cgroup value is a real limit or an unlimited sentinel.
//
// Known limitation, chosen deliberately: this reads the namespace root and the
// process's own cgroup and takes the tighter of the two. A limit set on some
// INTERMEDIATE ancestor (--cgroup-parent, or a pod-level cgroup under
// cgroupns=host) is still invisible. Walking the whole chain and taking the
// minimum would be the fully robust form; these two cover every container layout
// Arc is deployed in.
func limit() (uint64, Source, bool) {
	physical, hasPhysical := hostMemory()

	// cgroup v2, root first. See parseSelfCgroupV2Path for why both are tried.
	if v, ok := cgroupV2Limit(physical); ok {
		return v, SourceCgroupV2, true
	}

	// cgroup v1.
	if contents, ok := readFileLimited(rootFS, cgroupV1Max); ok {
		if raw, ok := parseCgroupValue(contents); ok {
			if v, ok := effectiveLimit(raw, 0, physical, true); ok {
				return v, SourceCgroupV1, true
			}
		}
	}

	if hasPhysical {
		return physical, SourceMemInfo, true
	}
	return 0, SourceUnknown, false
}

// cgroupV2Limit reads memory.max (and memory.high) from the cgroup root, then
// from the path named in /proc/self/cgroup.
func cgroupV2Limit(physical uint64) (uint64, bool) {
	try := func(maxPath, highPath string) (uint64, bool) {
		contents, ok := readFileLimited(rootFS, maxPath)
		if !ok {
			return 0, false
		}
		maxLimit, haveMax := parseCgroupValue(contents)
		if !haveMax {
			// Explicitly "max": unlimited at this level, so do not let a
			// memory.high reading stand in for a limit that is not there.
			return 0, false
		}
		var highLimit uint64
		if hc, ok := readFileLimited(rootFS, highPath); ok {
			if h, ok := parseCgroupValue(hc); ok {
				highLimit = h
			}
		}
		return effectiveLimit(maxLimit, highLimit, physical, false)
	}

	rootLimit, haveRoot := try(cgroupV2Max, cgroupV2High)

	// Also try the process's own cgroup path. Both are needed, for opposite
	// reasons: under --cgroupns=host the namespace root has no memory interface
	// files at all and only this path has the limit, while under a private
	// namespace the root read is what covers a process re-parented into
	// /init.scope.
	var leafLimit uint64
	var haveLeaf bool
	if selfContents, ok := readFileLimited(rootFS, procSelfCG); ok {
		if rel, ok := parseSelfCgroupV2Path(selfContents); ok {
			leafLimit, haveLeaf = try(
				path.Join(cgroupRoot, rel, "memory.max"),
				path.Join(cgroupRoot, rel, "memory.high"),
			)
		}
	}

	// Take the TIGHTER of the two. Preferring the root unconditionally reports a
	// looser limit than the process actually has whenever a descendant cgroup is
	// more restrictive — e.g. root at 8 GiB with the process in a 1 GiB
	// /init.scope would have reported 8 GiB. The minimum is correct under both
	// namespace layouts.
	switch {
	case haveRoot && haveLeaf:
		if leafLimit < rootLimit {
			return leafLimit, true
		}
		return rootLimit, true
	case haveRoot:
		return rootLimit, true
	case haveLeaf:
		return leafLimit, true
	}
	return 0, false
}

// hostMemory reads MemTotal from /proc/meminfo.
func hostMemory() (uint64, bool) {
	contents, ok := readFileLimited(rootFS, procMemInfo)
	if !ok {
		return 0, false
	}
	return parseMemTotal(contents)
}
