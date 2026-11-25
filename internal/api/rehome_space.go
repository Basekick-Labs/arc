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
// side by side), then moves the new files to the hot tier (its size again,
// when that is another volume). A volume whose free space cannot be read is
// not refused on: the job fails on a full disk as before.
func (h *RehomeHandler) roomForJob(size int64) string {
	if size <= 0 {
		return ""
	}
	check := func(dir string, need int64, what string) string {
		if dir == "" {
			return ""
		}
		_, avail, err := rehomeDiskSpace(dir)
		if err != nil {
			return ""
		}
		free := int64(min(avail, math.MaxInt64/2))
		if free < need+rehomeSpaceMargin {
			return fmt.Sprintf("not enough free space for the job on %s (%s): it needs about %d MiB and %d MiB are free", what, dir, (need+rehomeSpaceMargin)>>20, free>>20)
		}
		return ""
	}
	if why := check(h.scratchRoot, 2*size, "the scratch volume"); why != "" {
		return why
	}
	if b, ok := h.storage.(interface{ GetBasePath() string }); ok {
		return check(b.GetBasePath(), size, "the hot storage volume")
	}
	return ""
}
