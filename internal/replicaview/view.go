package replicaview

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type File struct {
	// ReadPath optionally names a pinned immutable version of Path.
	ReadPath  string
	Path      string
	SHA256    string
	SizeBytes int64
	Metadata  FileMetadata
}

type Selection struct {
	File File
	// Segments is nil for an entire canonical file, and contains only the
	// uncovered entry ranges for a replica file.
	Segments []Segment
}

type partition struct {
	database    string
	measurement string
	hour        int64
}

// View publishes immutable, verified files and takes one source snapshot for
// each query. Merely advertising a primary file must never publish it here.
type View struct {
	mu      sync.Mutex
	files   map[string]File
	refs    map[string]int
	retired map[partition]Coverage
}

func NewView() *View {
	return &View{files: make(map[string]File), refs: make(map[string]int), retired: make(map[partition]Coverage)}
}

func cloneFile(f File) File {
	f.Metadata.Coverage = append(Coverage(nil), f.Metadata.Coverage...)
	f.Metadata.Partitions, _ = NormalizePartitions(f.Metadata.Partitions)
	f.Metadata.Replaces = append([]string(nil), f.Metadata.Replaces...)
	f.Metadata.Segments = append([]Segment(nil), f.Metadata.Segments...)
	f.Metadata.Columns = append([]string(nil), f.Metadata.Columns...)
	return f
}

func partitionOf(m FileMetadata) partition { return partition{m.Database, m.Measurement, m.Hour} }

func (v *View) PublishReplicationFile(ctx context.Context, path string, metadata FileMetadata, size int64, hash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return v.Publish(File{Path: path, SHA256: hash, SizeBytes: size, Metadata: metadata})
}

func (v *View) Publish(file File) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.publishLocked(file)
}

func (v *View) publishLocked(file File) error {
	rows := int64(0)
	if segments := file.Metadata.Segments; len(segments) > 0 {
		rows = segments[len(segments)-1].End
	}
	normalized, err := DecodeFileMetadata(file.Metadata.Encode(), rows)
	if err != nil {
		return err
	}
	file.Metadata = normalized

	if file.Path == "" || file.SHA256 == "" {
		return fmt.Errorf("replication view requires an immutable verified file")
	}
	if previous, ok := v.files[file.Path]; ok && previous.SHA256 != file.SHA256 {
		return fmt.Errorf("replication view file was replaced in place: %s", file.Path)
	}
	v.files[file.Path] = cloneFile(file)
	return nil
}

// Replace switches all inputs for verified outputs under the same lock used
// by query snapshots. Callers retain input files until CanUnlink permits them.
// No output is made visible if any validation fails.
func (v *View) Replace(inputs []string, outputs []File) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	inputSet := make(map[string]bool, len(inputs))
	for _, path := range inputs {
		inputSet[path] = true
	}
	for _, output := range outputs {
		if output.Path == "" || output.SHA256 == "" || output.Metadata.IsReplica() {
			return fmt.Errorf("invalid canonical replacement")
		}
		if previous, ok := v.files[output.Path]; ok && previous.SHA256 != output.SHA256 {
			return fmt.Errorf("replacement requires a new immutable file path")
		}
		if inputSet[output.Path] {
			return fmt.Errorf("replacement reuses its input path")
		}
	}

	replacementCoverage := make(map[partition]Coverage)
	for _, output := range outputs {
		for _, part := range output.Metadata.PartitionCoverages() {
			key := partition{output.Metadata.Database, output.Metadata.Measurement, part.Hour}
			replacementCoverage[key] = Union(replacementCoverage[key], part.Coverage)
		}
	}
	for _, path := range inputs {
		if input, ok := v.files[path]; ok {
			for _, part := range input.Metadata.PartitionCoverages() {
				key := partition{input.Metadata.Database, input.Metadata.Measurement, part.Hour}
				if !replacementCoverage[key].Covers(part.Coverage) {
					return fmt.Errorf("replacement lacks input coverage for %s", path)
				}
			}
		}
	}
	for _, path := range inputs {
		delete(v.files, path)
	}
	for _, output := range outputs {
		v.files[output.Path] = cloneFile(output)
	}
	return nil
}

// Retire records an intentional delete, not a compaction replacement. The
// caller must commit the coverage to the durable cluster state first so this
// decision survives restart and late WAL replay.
func (v *View) Retire(database, measurement string, hour int64, coverage Coverage, paths []string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	key := partition{database, measurement, hour}
	v.retired[key] = Union(v.retired[key], coverage)
	for _, path := range paths {
		delete(v.files, path)
	}
}

// CanUnlink is true only after a file has left the current source set and all
// query snapshots that could open it have been released.
func (v *View) CanUnlink(path string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, visible := v.files[path]
	return !visible && v.refs[path] == 0
}

type Snapshot struct {
	Sources     []Selection
	view        *View
	leasedPaths []string
	once        sync.Once
}

// Close implements io.Closer so a streamed HTTP response can own the lease
// until its body is completely consumed or the connection is closed.
func (s *Snapshot) Close() error {
	s.once.Do(func() {
		s.view.mu.Lock()
		defer s.view.mu.Unlock()
		for _, path := range s.leasedPaths {
			s.view.refs[path]--
			if s.view.refs[path] == 0 {
				delete(s.view.refs, path)
			}
		}
	})
	return nil
}

func (v *View) Snapshot(database, measurement string) *Snapshot {
	v.mu.Lock()
	defer v.mu.Unlock()
	snapshot := &Snapshot{view: v}
	covered := make(map[partition]Coverage)
	for key, coverage := range v.retired {
		if key.database == database && key.measurement == measurement {
			covered[key] = coverage
		}
	}
	var replicas []File
	for _, file := range v.files {
		m := file.Metadata
		if m.Database != database || m.Measurement != measurement {
			continue
		}
		if m.IsReplica() {
			replicas = append(replicas, file)
			continue
		}
		snapshot.Sources = append(snapshot.Sources, Selection{File: cloneFile(file)})
		for _, part := range m.PartitionCoverages() {
			key := partition{m.Database, m.Measurement, part.Hour}
			covered[key] = Union(covered[key], part.Coverage)
		}
	}
	// Stable order makes retries with multiple materializations choose the same
	// complete entry per partition. Payload content is never a deduplication key.
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].Path < replicas[j].Path })
	for _, file := range replicas {
		key := partitionOf(file.Metadata)
		selection := Selection{File: cloneFile(file)}
		var identities []string
		for _, segment := range file.Metadata.Segments {
			if !covered[key].Contains(segment.Identity) {
				selection.Segments = append(selection.Segments, segment)
				identities = append(identities, segment.Identity)
			}
		}
		if len(selection.Segments) == 0 {
			continue
		}
		snapshot.Sources = append(snapshot.Sources, selection)
		coverage, err := FromIdentities(identities)
		if err != nil {
			panic("invalid published replica identity")
		}
		covered[key] = Union(covered[key], coverage)
	}
	sort.Slice(snapshot.Sources, func(i, j int) bool { return snapshot.Sources[i].File.Path < snapshot.Sources[j].File.Path })
	for _, source := range snapshot.Sources {
		v.refs[source.File.Path]++
		snapshot.leasedPaths = append(snapshot.leasedPaths, source.File.Path)
	}
	return snapshot
}

// HasEntry recognizes complete durable replica materialization across hours.
// The same entry can occur in multiple retry files, so each partition is counted
// only once. A partial multi-hour flush is never mistaken for complete recovery.
func (v *View) HasEntry(database, measurement, identity string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	rowsByHour := make(map[int64]int64)
	var expected int64
	for _, file := range v.files {
		metadata := file.Metadata
		if metadata.Database != database || metadata.Measurement != measurement {
			continue
		}
		var rows int64
		for _, segment := range metadata.Segments {
			if segment.Identity == identity {
				rows += segment.End - segment.Start
				if expected == 0 {
					expected = segment.TotalRows
				}
				if expected != segment.TotalRows {
					return false
				}
			}
		}
		if rows > rowsByHour[metadata.Hour] {
			rowsByHour[metadata.Hour] = rows
		}
	}
	var total int64
	for _, rows := range rowsByHour {
		total += rows
	}
	return expected > 0 && total == expected
}
