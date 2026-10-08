package raft

import (
	"fmt"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/replicaview"
)

// ReplicationRetirement prevents received WAL entries from resurrecting data
// intentionally removed while a replica was offline. Sequences are compressed
// per originating lifetime and partition; identical payloads remain distinct.
type ReplicationRetirement struct {
	Database    string               `json:"database"`
	Measurement string               `json:"measurement"`
	Hour        int64                `json:"hour"`
	Coverage    replicaview.Coverage `json:"coverage"`
}

func retirementKey(database, measurement string, hour int64) string {
	return fmt.Sprintf("%s\x00%s\x00%d", database, measurement, hour)
}

func (f *ClusterFSM) retireReplicationLocked(entry *FileEntry, reason string) {
	// Compaction changes representation. Its successor must become locally
	// available before any streamed coverage can be withdrawn.
	if (reason == "compaction" || strings.HasPrefix(reason, "compaction:") || strings.HasPrefix(reason, "tiering:")) || len(entry.WALCoverage) == 0 {
		return
	}
	if f.replicationRetired == nil {
		f.replicationRetired = make(map[string]ReplicationRetirement)
	}
	for _, part := range entry.WALCoverage {
		key := retirementKey(entry.Database, entry.Measurement, part.Hour)
		previous := f.replicationRetired[key]
		f.replicationRetired[key] = ReplicationRetirement{entry.Database, entry.Measurement, part.Hour, replicaview.Union(previous.Coverage, part.Coverage)}
	}

}

func (f *ClusterFSM) replicationRetirementsLocked() []ReplicationRetirement {
	result := make([]ReplicationRetirement, 0, len(f.replicationRetired))
	for _, retirement := range f.replicationRetired {
		retirement.Coverage = append(replicaview.Coverage(nil), retirement.Coverage...)
		result = append(result, retirement)
	}
	sort.Slice(result, func(i, j int) bool {
		return retirementKey(result[i].Database, result[i].Measurement, result[i].Hour) < retirementKey(result[j].Database, result[j].Measurement, result[j].Hour)
	})
	return result
}

func (f *ClusterFSM) ReplicationRetirements() []ReplicationRetirement {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.replicationRetirementsLocked()
}

func cloneManifestFile(file *FileEntry) FileEntry {
	result := *file
	result.Replaces = append([]string(nil), file.Replaces...)
	result.WALCoverage = make([]replicaview.PartitionCoverage, len(file.WALCoverage))
	for i, part := range file.WALCoverage {
		result.WALCoverage[i] = replicaview.PartitionCoverage{Hour: part.Hour, Coverage: append(replicaview.Coverage(nil), part.Coverage...)}
	}
	return result
}
