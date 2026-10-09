package wal

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const recoveryRowPrefix = "arc:rows:v1:"

type recoveryRowRange struct{ start, end int }

func trackedRowIdentity(identity string) bool {
	if len(identity) != 32 {
		return false
	}
	_, err := hex.DecodeString(identity)
	return err == nil && strings.ToLower(identity) == identity
}

func recoveryRowIdentity(parent string, start, end int) string {
	return recoveryRowPrefix + parent + ":" + strconv.Itoa(start) + ":" + strconv.Itoa(end)
}

// ParseRecoveryRowIdentity recognizes a checkpoint for the half-open row range
// [start,end) of one tracked WAL entry. The parent is a writer-instance/sequence
// identity, never a legacy content hash: identical legacy payloads can represent
// distinct writes. The encoding is independent of the configured batch size.
func ParseRecoveryRowIdentity(identity string) (parent string, start, end int, ok bool) {
	if !strings.HasPrefix(identity, recoveryRowPrefix) {
		return
	}
	parts := strings.Split(strings.TrimPrefix(identity, recoveryRowPrefix), ":")
	if len(parts) != 3 || !trackedRowIdentity(parts[0]) {
		return
	}
	lo, err := strconv.Atoi(parts[1])
	if err != nil || lo < 0 {
		return
	}
	hi, err := strconv.Atoi(parts[2])
	if err != nil || hi <= lo || recoveryRowIdentity(parts[0], lo, hi) != identity {
		return
	}
	return parts[0], lo, hi, true
}

func recoveryRowCoverage(checkpoints map[string]struct{}) map[string][]recoveryRowRange {
	coverage := make(map[string][]recoveryRowRange)
	for checkpoint := range checkpoints {
		if parent, start, end, ok := ParseRecoveryRowIdentity(checkpoint); ok {
			coverage[parent] = append(coverage[parent], recoveryRowRange{start, end})
		}
	}
	for parent, ranges := range coverage {
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
		merged := ranges[:0]
		for _, current := range ranges {
			if len(merged) == 0 || current.start > merged[len(merged)-1].end {
				merged = append(merged, current)
			} else if current.end > merged[len(merged)-1].end {
				merged[len(merged)-1].end = current.end
			}
		}
		coverage[parent] = merged
	}
	return coverage
}

// uncoveredRecoveryRows subtracts durable coverage before applying the current
// batch bound, so changing that bound on restart cannot replay a durable prefix.
func uncoveredRecoveryRows(count int, covered []recoveryRowRange) ([]recoveryRowRange, error) {
	var missing []recoveryRowRange
	next := 0
	for _, r := range covered {
		if r.end > count {
			return nil, fmt.Errorf("WAL row checkpoint ends at %d beyond entry length %d", r.end, count)
		}
		if next < r.start {
			missing = append(missing, recoveryRowRange{next, r.start})
		}
		next = r.end
	}
	if next < count {
		missing = append(missing, recoveryRowRange{next, count})
	}
	return missing, nil
}
