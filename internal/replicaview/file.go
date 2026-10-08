package replicaview

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
)

const FileMetadataKey = "arc:replication_file_v1"
const ReplicaPrefix = ".replica/"

// Segment locates a complete entry's rows within one hourly replica file.
// End is exclusive. These are footer metadata, never user-visible columns.
type Segment struct {
	Identity  string `json:"identity"`
	TotalRows int64  `json:"total_rows"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

type FileMetadata struct {
	Partitions  []PartitionCoverage `json:"partitions,omitempty"`
	Replaces    []string            `json:"replaces,omitempty"`
	Database    string              `json:"database"`
	Measurement string              `json:"measurement"`
	Hour        int64               `json:"hour"`
	Coverage    Coverage            `json:"coverage,omitempty"`
	Columns     []string            `json:"columns,omitempty"`
	Segments    []Segment           `json:"segments,omitempty"`
}

func (m FileMetadata) IsReplica() bool          { return len(m.Segments) != 0 }
func (m FileMetadata) PartitionTime() time.Time { return time.Unix(m.Hour*3600, 0).UTC() }

func (m FileMetadata) Encode() string { data, _ := json.Marshal(m); return string(data) }

func DecodeFileMetadata(encoded string, rows int64) (FileMetadata, error) {
	var m FileMetadata
	if err := json.Unmarshal([]byte(encoded), &m); err != nil {
		return m, fmt.Errorf("decode replication file metadata: %w", err)
	}
	if m.Database == "" || m.Measurement == "" {
		return m, fmt.Errorf("replication metadata missing measurement identity")
	}
	var err error
	m.Partitions, err = NormalizePartitions(m.Partitions)
	if err != nil {
		return m, err
	}
	m.Coverage, err = Normalize(m.Coverage)
	if err != nil {
		return m, err
	}
	var end int64
	for _, segment := range m.Segments {
		if _, _, err := ParseIdentity(segment.Identity); err != nil {
			return m, err
		}
		if segment.TotalRows <= 0 || segment.TotalRows < segment.End-segment.Start || segment.Start != end || segment.End <= segment.Start || segment.End > rows {
			return m, fmt.Errorf("invalid replica row segment")
		}
		if !m.Coverage.Contains(segment.Identity) {
			return m, fmt.Errorf("replica segment missing coverage identity")
		}
		end = segment.End
	}
	if len(m.Segments) > 0 && end != rows {
		return m, fmt.Errorf("replica segments do not cover every row")
	}
	return m, nil
}

// ReadFileMetadata only reads the Parquet footer. A missing key means an
// ordinary legacy file; malformed metadata is an error, never empty coverage.
func ReadFileMetadata(source io.ReaderAt, size int64) (*FileMetadata, error) {
	reader, err := file.NewParquetReader(io.NewSectionReader(source, 0, size))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	for _, kv := range reader.MetaData().KeyValueMetadata() {
		if kv.GetKey() != FileMetadataKey {
			continue
		}
		metadata, err := DecodeFileMetadata(kv.GetValue(), reader.NumRows())
		if err != nil {
			return nil, err
		}
		return &metadata, nil
	}
	return nil, nil
}

// SliceSegments maps a stable hourly selection into the replica file's new
// row positions. Indices are in original row order (groupByHour's contract).
func SliceSegments(segments []Segment, indices []int) []Segment {
	var result []Segment
	segmentIndex := 0
	for output, input := range indices {
		for segmentIndex < len(segments) && int64(input) >= segments[segmentIndex].End {
			segmentIndex++
		}
		if segmentIndex == len(segments) || int64(input) < segments[segmentIndex].Start {
			panic("replica row outside its entry segment")
		}
		identity := segments[segmentIndex].Identity
		if len(result) > 0 && result[len(result)-1].Identity == identity {
			result[len(result)-1].End++
		} else {
			result = append(result, Segment{Identity: identity, TotalRows: segments[segmentIndex].TotalRows, Start: int64(output), End: int64(output + 1)})
		}
	}
	return result
}

func (m FileMetadata) PartitionCoverages() []PartitionCoverage {
	if len(m.Partitions) > 0 {
		return m.Partitions
	}
	if len(m.Coverage) > 0 {
		return []PartitionCoverage{{m.Hour, m.Coverage}}
	}
	return nil
}
