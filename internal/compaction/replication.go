package compaction

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/replicaview"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// compactionReplicationMetadata preserves original entry coverage even when
// compaction deduplicates rows or combines multiple hourly partitions.
func compactionReplicationMetadata(paths, storageKeys []string) (*replicaview.FileMetadata, error) {
	var result *replicaview.FileMetadata
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		metadata, err := replicaview.ReadFileMetadata(f, info.Size())
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("read compaction replication metadata: %w", err)
		}
		if metadata == nil {
			continue
		}
		if metadata.IsReplica() {
			return nil, fmt.Errorf("replica materialization cannot be compacted into originating data")
		}
		if result == nil {
			result = &replicaview.FileMetadata{Database: metadata.Database, Measurement: metadata.Measurement}
		}
		if result.Database != metadata.Database || result.Measurement != metadata.Measurement {
			return nil, fmt.Errorf("compaction crosses replication measurement identities")
		}
		result.Partitions = append(result.Partitions, metadata.PartitionCoverages()...)
	}
	if result == nil {
		return nil, nil
	}
	var err error
	result.Partitions, err = replicaview.NormalizePartitions(result.Partitions)
	if err != nil {
		return nil, err
	}
	for _, part := range result.Partitions {
		result.Coverage = replicaview.Union(result.Coverage, part.Coverage)
	}
	result.Replaces = append([]string(nil), storageKeys...)
	sort.Strings(result.Replaces)
	return result, nil
}

func buildCompactionQuery(fileListSQL, orderByClause, outputFile string, tagColumns []string, dedupTime bool, metadata ...map[string]string) []string {
	statements := buildCompactionQueryBase(fileListSQL, orderByClause, outputFile, tagColumns, dedupTime)
	if len(metadata) == 0 || len(metadata[0]) == 0 {
		return statements
	}
	keys := make([]string, 0, len(metadata[0]))
	for key := range metadata[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, sqlutil.QuoteStringLiteral(key)+": "+sqlutil.QuoteStringLiteral(metadata[0][key]))
	}
	// Only the final statement is COPY; its final parenthesis closes the
	// options list. Data paths and user field names are never searched/replaced.
	last := len(statements) - 1
	closing := strings.LastIndex(statements[last], ")")
	statements[last] = statements[last][:closing] + ", KV_METADATA {" + strings.Join(pairs, ",") + "}" + statements[last][closing:]
	return statements
}
