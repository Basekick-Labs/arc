// Package replicaview tracks originating WAL entries independently of physical
// flush boundaries. Identities name accepted writes, never payload contents.
package replicaview

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

const MetadataKey = "arc:wal_coverage_v1"

// Span describes inclusive sequences from one originating writer lifetime.
// uint64 values encode as decimal strings to preserve precision in consumers.
type Span struct {
	Instance uint64 `json:"instance,string"`
	First    uint64 `json:"first,string"`
	Last     uint64 `json:"last,string"`
}

type Coverage []Span

func ParseIdentity(identity string) (instance, sequence uint64, err error) {
	if len(identity) != 32 {
		return 0, 0, fmt.Errorf("invalid WAL identity length")
	}
	instance, err = strconv.ParseUint(identity[:16], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid WAL instance: %w", err)
	}
	sequence, err = strconv.ParseUint(identity[16:], 16, 64)
	if err != nil || sequence == 0 {
		return 0, 0, fmt.Errorf("invalid WAL sequence")
	}
	return instance, sequence, nil
}

func FromIdentities(identities []string) (Coverage, error) {
	spans := make(Coverage, 0, len(identities))
	for _, identity := range identities {
		instance, seq, err := ParseIdentity(identity)
		if err != nil {
			return nil, err
		}
		spans = append(spans, Span{instance, seq, seq})
	}
	return Normalize(spans)
}

// Normalize owns its returned slice and never changes input snapshots.
func Normalize(input Coverage) (Coverage, error) {
	spans := append(Coverage(nil), input...)
	for _, span := range spans {
		if span.First == 0 || span.Last < span.First {
			return nil, fmt.Errorf("invalid WAL coverage interval")
		}
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].Instance != spans[j].Instance {
			return spans[i].Instance < spans[j].Instance
		}
		if spans[i].First != spans[j].First {
			return spans[i].First < spans[j].First
		}
		return spans[i].Last < spans[j].Last
	})
	result := spans[:0]
	for _, span := range spans {
		if len(result) > 0 {
			prev := &result[len(result)-1]
			if prev.Instance == span.Instance && (span.First <= prev.Last || (prev.Last != math.MaxUint64 && span.First == prev.Last+1)) {
				if span.Last > prev.Last {
					prev.Last = span.Last
				}
				continue
			}
		}
		result = append(result, span)
	}
	return result, nil
}

func Union(sets ...Coverage) Coverage {
	var all Coverage
	for _, set := range sets {
		all = append(all, set...)
	}
	// Stored coverage is validated on input. This call only combines it.
	result, err := Normalize(all)
	if err != nil {
		panic("invalid internal WAL coverage: " + err.Error())
	}
	return result
}

func (c Coverage) Contains(identity string) bool {
	instance, seq, err := ParseIdentity(identity)
	if err != nil {
		return false
	}
	i := sort.Search(len(c), func(i int) bool { return c[i].Instance > instance || (c[i].Instance == instance && c[i].Last >= seq) })
	return i < len(c) && c[i].Instance == instance && c[i].First <= seq
}

func Decode(value string) (Coverage, error) {
	var spans Coverage
	if err := json.Unmarshal([]byte(value), &spans); err != nil {
		return nil, fmt.Errorf("decode WAL coverage: %w", err)
	}
	return Normalize(spans)
}

func (c Coverage) Encode() string {
	if len(c) == 0 {
		return "[]"
	}
	encoded, _ := json.Marshal(c)
	return string(encoded)
}

// PartitionCoverage keeps coverage partition-scoped when compaction combines
// several hourly files into a daily/monthly output.
type PartitionCoverage struct {
	Hour     int64    `json:"hour"`
	Coverage Coverage `json:"coverage"`
}

func NormalizePartitions(input []PartitionCoverage) ([]PartitionCoverage, error) {
	byHour := make(map[int64]Coverage)
	for _, part := range input {
		coverage, err := Normalize(part.Coverage)
		if err != nil {
			return nil, err
		}
		byHour[part.Hour] = Union(byHour[part.Hour], coverage)
	}
	result := make([]PartitionCoverage, 0, len(byHour))
	for hour, coverage := range byHour {
		result = append(result, PartitionCoverage{hour, coverage})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Hour < result[j].Hour })
	return result, nil
}

func (c Coverage) Covers(other Coverage) bool {
	for _, span := range other {
		i := sort.Search(len(c), func(i int) bool {
			return c[i].Instance > span.Instance || (c[i].Instance == span.Instance && c[i].Last >= span.First)
		})
		if i == len(c) || c[i].Instance != span.Instance || c[i].First > span.First || c[i].Last < span.Last {
			return false
		}
	}
	return true
}

// Intersects reports overlap between normalized identity ranges.
func (c Coverage) Intersects(other Coverage) bool {
	i, j := 0, 0
	for i < len(c) && j < len(other) {
		a, b := c[i], other[j]
		if a.Instance < b.Instance || (a.Instance == b.Instance && a.Last < b.First) {
			i++
			continue
		}
		if b.Instance < a.Instance || (a.Instance == b.Instance && b.Last < a.First) {
			j++
			continue
		}
		return true
	}
	return false
}
