package api

import (
	"fmt"
	"math"

	"github.com/basekick-labs/arc/internal/diskspace"
)

// rehomeDiskSpace is diskspace.Space; a variable so a test can fake a full
// volume.
var rehomeDiskSpace = diskspace.Space

// rehomeSpaceMargin is left free on a volume beyond what a job needs.
const rehomeSpaceMargin = 64 << 20

// roomForJob says why a job on a file of size bytes does not fit, or "". A
// job copies the file to scratch and splits it there (about twice its size
// side by side; an estimate: DuckDB may spill there too, and the new files
// need not be smaller than the old one), then writes the new files to the hot
// tier (its size again). When scratch and hot storage are one volume the
// needs add up. A volume whose free space cannot be read is not refused on:
// the job fails on a full disk as before.
func (h *RehomeHandler) roomForJob(size int64) string {
	if size <= 0 {
		return ""
	}
	type vol struct {
		dir          string
		total, avail uint64
		ok           bool
	}
	stat := func(dir string) vol {
		if dir == "" {
			return vol{}
		}
		total, avail, err := rehomeDiskSpace(dir)
		return vol{dir: dir, total: total, avail: avail, ok: err == nil}
	}
	check := func(v vol, need int64, what string) string {
		if !v.ok {
			return ""
		}
		free := int64(min(v.avail, math.MaxInt64/2))
		if free < need+rehomeSpaceMargin {
			return fmt.Sprintf("not enough free space for the job on %s (%s): it needs about %d MiB and %d MiB are free", what, v.dir, (need+rehomeSpaceMargin)>>20, free>>20)
		}
		return ""
	}
	scratch := stat(h.scratchRoot)
	hot := vol{}
	if b, ok := h.storage.(interface{ GetBasePath() string }); ok {
		hot = stat(b.GetBasePath())
	}
	// One volume: the same size and, read a moment apart, about the same
	// free space.
	if scratch.ok && hot.ok && scratch.total == hot.total && absDiff(scratch.avail, hot.avail) < 1<<20 {
		return check(scratch, 3*size, "the scratch and hot storage volume")
	}
	if why := check(scratch, 2*size, "the scratch volume"); why != "" {
		return why
	}
	return check(hot, size, "the hot storage volume")
}

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
