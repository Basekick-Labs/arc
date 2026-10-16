package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// claimKeep returns the Keep function of one stored record for a claimed
// replay (#1207 S4b1c): per partition hour, the rows of the record no
// registered file of that hour holds, read from the manifest at call time.
func claimKeep(fsm *raft.ClusterFSM, inst uint64, key string, cseq uint64, chunk, chunks uint16) func(time.Time, int) ([][2]int, error) {
	return func(hour time.Time, total int) ([][2]int, error) {
		st, ranges, regTotal, err := fsm.CoveredRows(inst, key, cseq, chunk, chunks, hour)
		switch {
		case err != nil:
			return nil, err
		case st == raft.CoveredUntracked:
			return nil, errors.New("the instance is not tracked: nothing can arbitrate its rows")
		case st == raft.CoveredAll:
			return nil, nil
		}
		if len(ranges) > 0 && regTotal != total {
			return nil, fmt.Errorf("batch %d of %s is registered against %d rows, the stored record has %d", cseq, key, regTotal, total)
		}
		return complementRows(ranges, total), nil
	}
}

// complementRows returns the parts of [0, total) outside the sorted ranges.
func complementRows(ranges [][2]int, total int) [][2]int {
	var out [][2]int
	cur := 0
	for _, r := range ranges {
		if r[0] > cur {
			out = append(out, [2]int{cur, min(r[0], total)})
		}
		if r[1] > cur {
			cur = r[1]
		}
	}
	if cur < total {
		out = append(out, [2]int{cur, total})
	}
	return out
}
