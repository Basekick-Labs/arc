package replicaview

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
)

var ErrManifestNotReady = errors.New("replica manifest has not been reconciled")

// Canonical is an immutable manifest version, not a claim that its bytes are
// available locally. The footer and checksum are checked before publication.
type Canonical struct {
	Path, SHA256, Database, Measurement string
	SizeBytes                           int64
	Hour                                int64
	Partitions                          []PartitionCoverage
	Replaces                            []string
}

// LocalStore owns verified pins and a query view. Construction recovers replica
// files before WAL replay, but queries remain closed until Reconcile receives
// one coherent manifest and retirement snapshot. No extra per-write journal is
// introduced; identity and row ranges live in the existing WAL and Parquet.
type LocalStore struct {
	mu          sync.Mutex
	view        *View
	pins        *PinnedFiles
	directory   string
	verified    map[string]File
	provisional map[string]File
	manifest    []Canonical
	retirements []Retirement
	ready       bool
	canCollect  bool
	garbage     map[string]File
	canonical   map[string]File
}

func OpenLocalStore(ctx context.Context, directory string) (*LocalStore, error) {
	pins, err := OpenPinnedFiles(directory)
	if err != nil {
		return nil, err
	}
	s := &LocalStore{view: NewView(), pins: pins, directory: directory, verified: make(map[string]File), provisional: make(map[string]File), garbage: make(map[string]File), canonical: make(map[string]File)}
	s.view.SetUnavailable(ErrManifestNotReady)
	if err := s.recoverFiles(ctx); err != nil {
		pins.Close()
		return nil, err
	}
	return s, nil
}
func (s *LocalStore) View() *View { return s.view }
func (s *LocalStore) Resolve(key string) string {
	return filepath.Join(s.directory, filepath.FromSlash(key))
}
func (s *LocalStore) Close() error                   { s.mu.Lock(); defer s.mu.Unlock(); return s.pins.Close() }
func (s *LocalStore) HasEntry(db, m, id string) bool { return s.view.HasEntry(db, m, id) }

func (s *LocalStore) recoverFiles(ctx context.Context) error {
	for _, prefix := range []string{strings.TrimSuffix(ReplicaPrefix, "/"), strings.TrimSuffix(PinsPrefix, "/")} {
		err := fs.WalkDir(s.pins.root.FS(), prefix, func(physical string, d fs.DirEntry, err error) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if d.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink in replica recovery tree: %s", physical)
			}
			if !strings.HasSuffix(physical, ".parquet") {
				return nil
			}
			key := physical
			if strings.HasPrefix(physical, PinsPrefix) {
				key = path.Dir(strings.TrimPrefix(physical, PinsPrefix))
			}
			f, err := s.pins.root.Open(physical)
			if err != nil {
				return err
			}
			info, err := f.Stat()
			if err != nil {
				f.Close()
				return err
			}
			metadata, err := ReadFileMetadata(f, info.Size())
			if err != nil {
				f.Close()
				return err
			}
			if metadata == nil {
				f.Close()
				if strings.HasPrefix(key, ReplicaPrefix) {
					return fmt.Errorf("replica file lacks provenance: %s", key)
				}
				return nil // Legacy pins are reloaded only from an explicit manifest entry.
			}
			digest := sha256.New()
			_, err = io.Copy(digest, f)
			f.Close()
			if err != nil {
				return err
			}
			hash := hex.EncodeToString(digest.Sum(nil))
			if strings.HasPrefix(physical, PinsPrefix) && path.Base(physical) != hash+".parquet" {
				return fmt.Errorf("corrupted replica pin: %s", key)
			}
			if metadata.IsReplica() {
				return s.publishReplicaLocked(ctx, key, *metadata, info.Size(), hash)
			}
			verified, err := s.pinFile(key, hash, info.Size(), metadata)
			if err != nil {
				return err
			}
			s.provisional[key] = verified
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *LocalStore) pinFile(key, hash string, size int64, expected *FileMetadata) (File, error) {
	if f, ok := s.verified[key]; ok && f.SHA256 == hash {
		if f.SizeBytes != size {
			return File{}, fmt.Errorf("file size changed for immutable version: %s", key)
		}
		return f, nil
	}
	pinned, err := s.pins.Pin(key, hash)
	if err != nil {
		return File{}, err
	}
	f, err := s.pins.root.Open(pinned)
	if err != nil {
		return File{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return File{}, err
	}
	if info.Size() != size {
		return File{}, fmt.Errorf("pinned file size differs from manifest: %s", key)
	}
	metadata, err := ReadFileMetadata(f, size)
	if err != nil {
		return File{}, err
	}
	if metadata == nil {
		if expected == nil {
			return File{}, fmt.Errorf("file lacks replication identity: %s", key)
		}
		if len(expected.PartitionCoverages()) != 0 {
			return File{}, fmt.Errorf("manifest coverage has no matching footer: %s", key)
		}
		metadata = expected // Legacy canonical file, explicitly named by the manifest.
	}
	result := File{Path: key, ReadPath: pinned, SHA256: hash, SizeBytes: size, Metadata: *metadata}
	s.verified[key] = cloneFile(result)
	return result, nil
}

func (s *LocalStore) publishReplicaLocked(ctx context.Context, key string, metadata FileMetadata, size int64, hash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !strings.HasPrefix(key, ReplicaPrefix) || !metadata.IsReplica() {
		return fmt.Errorf("invalid replica publication")
	}
	f, err := s.pinFile(key, hash, size, &metadata)
	if err != nil {
		return err
	}
	if f.Metadata.Encode() != metadata.Encode() {
		return fmt.Errorf("replica footer differs from published metadata: %s", key)
	}
	return s.view.Publish(f)
}

func (s *LocalStore) PublishReplicationFile(ctx context.Context, key string, metadata FileMetadata, size int64, hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if metadata.IsReplica() {
		return s.publishReplicaLocked(ctx, key, metadata, size, hash)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := s.pinFile(key, hash, size, &metadata)
	if err != nil {
		return err
	}
	if f.Metadata.Encode() != metadata.Encode() {
		return fmt.Errorf("canonical footer differs from published metadata: %s", key)
	}
	s.provisional[key] = f
	// A primary flush can be queried before its asynchronous registration. It
	// stays provisional until a manifest snapshot or durable coverage accounts
	// for it; absence alone never authorizes deleting the accepted write.
	if !s.ready {
		return s.view.Publish(f)
	}
	return s.reconcileLocked(ctx)
}

// Reconcile must receive files and retirements from the SAME FSM observation.
// Callers serialize observations; a late older snapshot must not roll this back.
func (s *LocalStore) Reconcile(ctx context.Context, manifest []Canonical, retirements []Retirement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canCollect = false
	next := make([]Canonical, len(manifest))
	for i, entry := range manifest {
		entry.Replaces = append([]string(nil), entry.Replaces...)
		var err error
		entry.Partitions, err = NormalizePartitions(entry.Partitions)
		if err != nil {
			return err
		}
		next[i] = entry
	}
	s.manifest = next
	s.retirements = make([]Retirement, len(retirements))
	for i, r := range retirements {
		r.Coverage = append(Coverage(nil), r.Coverage...)
		s.retirements[i] = r
	}
	s.ready = true
	return s.reconcileLocked(ctx)
}

func (s *LocalStore) reconcileLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var available []File
	obsolete := make(map[string]File)
	blocked := make(map[string]error)
	accounted := make(map[partition]Coverage)
	availableCoverage := make(map[partition]Coverage)
	for _, r := range s.retirements {
		k := partition{r.Database, r.Measurement, r.Hour}
		accounted[k] = Union(accounted[k], r.Coverage)
		availableCoverage[k] = accounted[k]
	}
	for _, entry := range s.manifest {
		for _, part := range entry.Partitions {
			k := partition{entry.Database, entry.Measurement, part.Hour}
			accounted[k] = Union(accounted[k], part.Coverage)
		}
		expected := FileMetadata{Database: entry.Database, Measurement: entry.Measurement, Hour: entry.Hour, Partitions: entry.Partitions, Replaces: entry.Replaces}
		f, err := s.pinFile(entry.Path, entry.SHA256, entry.SizeBytes, &expected)
		if err == nil {
			err = validateCanonical(f, entry)
		}
		if err != nil {
			// A fresh originating file can still be represented by streamed rows.
			// A missing rewrite cannot: its deleted/deduplicated rows differ. Refuse
			// the affected measurement until its replacement bytes are verified.
			if len(entry.Replaces) > 0 || !errors.Is(err, fs.ErrNotExist) {
				blocked[MeasurementKey(entry.Database, entry.Measurement)] = fmt.Errorf("canonical file unavailable: %s: %w", entry.Path, err)
			}
			continue
		}
		available = append(available, f)
		for _, part := range entry.Partitions {
			k := partition{entry.Database, entry.Measurement, part.Hour}
			availableCoverage[k] = Union(availableCoverage[k], part.Coverage)
		}
		delete(s.provisional, entry.Path)
	}
	for key, f := range s.provisional {
		allCovered := len(f.Metadata.PartitionCoverages()) > 0
		anyCovered := false
		for _, part := range f.Metadata.PartitionCoverages() {
			coverage := accounted[partition{f.Metadata.Database, f.Metadata.Measurement, part.Hour}]
			allCovered = allCovered && availableCoverage[partition{f.Metadata.Database, f.Metadata.Measurement, part.Hour}].Covers(part.Coverage)
			anyCovered = anyCovered || coverage.Intersects(part.Coverage)
		}
		if blocked[MeasurementKey(f.Metadata.Database, f.Metadata.Measurement)] != nil {
			continue
		}
		if allCovered {
			obsolete[key] = f
			delete(s.provisional, key)
			continue
		}
		if anyCovered {
			blocked[MeasurementKey(f.Metadata.Database, f.Metadata.Measurement)] = fmt.Errorf("origin file overlaps manifest coverage: %s", key)
			continue
		}
		available = append(available, f)
	}
	if err := s.view.SetCanonicalState(available, s.retirements, blocked); err != nil {
		s.view.SetUnavailable(err)
		return err
	}
	nextCanonical := make(map[string]File, len(available))
	for _, f := range available {
		nextCanonical[f.Path] = f
		delete(s.garbage, f.Path)
	}
	for key, f := range s.canonical {
		if _, retained := nextCanonical[key]; !retained {
			obsolete[key] = f
		}
	}
	for key, f := range obsolete {
		s.garbage[key] = f
	}
	s.canonical = nextCanonical
	s.canCollect = len(blocked) == 0
	return nil
}

// PublishCanonical is the pull worker hook. It verifies the exact requested
// version, then reconciles against the latest manifest observation. A file
// removed meanwhile cannot re-enter the view just because its bytes arrived.
func (s *LocalStore) PublishCanonical(ctx context.Context, entry Canonical) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	expected := FileMetadata{Database: entry.Database, Measurement: entry.Measurement, Hour: entry.Hour, Partitions: entry.Partitions, Replaces: entry.Replaces}
	f, err := s.pinFile(entry.Path, entry.SHA256, entry.SizeBytes, &expected)
	if err != nil {
		return err
	}
	if err := validateCanonical(f, entry); err != nil {
		return err
	}
	if !s.ready {
		return nil
	}
	return s.reconcileLocked(ctx)
}

// Collect withdraws covered replica files, then unlinks only versions no active
// query can still open. It runs after manifest reconciliation, never on a Raft
// callback or the ingestion admission path.
func (s *LocalStore) Collect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready || !s.canCollect || len(s.provisional) > 0 {
		return nil
	}
	for _, f := range s.view.PruneCoveredReplicas() {
		s.garbage[f.Path] = f
	}
	for key, f := range s.garbage {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !s.view.CanUnlink(key) {
			continue
		}
		// Canonical storage keys belong to compaction/retention/file pulling.
		// We own only their pins. Replica shadow data belongs to this store.
		if f.Metadata.IsReplica() {
			if err := s.pins.root.Remove(key); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
		if err := s.pins.Remove(key, f.SHA256); err != nil {
			return err
		}
		delete(s.verified, key)
		delete(s.garbage, key)
	}
	return nil
}

func validateCanonical(file File, entry Canonical) error {
	actual, err := NormalizePartitions(file.Metadata.PartitionCoverages())
	if err != nil {
		return err
	}
	expected, err := NormalizePartitions(entry.Partitions)
	if err != nil {
		return err
	}
	if file.Metadata.IsReplica() || file.Metadata.Database != entry.Database || file.Metadata.Measurement != entry.Measurement || !reflect.DeepEqual(actual, expected) || !sameReplacements(file.Metadata.Replaces, entry.Replaces) {
		return fmt.Errorf("verified footer disagrees with manifest: %s", entry.Path)
	}
	return nil
}

func sameReplacements(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// CoveredOriginHours is used only by originating WAL replay. Pins recovered
// before a checkpoint still prove that their complete hourly contribution is
// durable. An intentional retirement is equally authoritative after the leader
// barrier; replica materializations never count as primary publication proof.
func (s *LocalStore) CoveredOriginHours(database, measurement, identity string) (map[int64]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return nil, ErrManifestNotReady
	}
	if _, _, err := ParseIdentity(identity); err != nil {
		return nil, err
	}
	covered := make(map[int64]bool)
	for _, r := range s.retirements {
		if r.Database == database && r.Measurement == measurement && r.Coverage.Contains(identity) {
			covered[r.Hour] = true
		}
	}
	include := func(files map[string]File) {
		for _, f := range files {
			if f.Metadata.Database != database || f.Metadata.Measurement != measurement || f.Metadata.IsReplica() {
				continue
			}
			for _, part := range f.Metadata.PartitionCoverages() {
				if part.Coverage.Contains(identity) {
					covered[part.Hour] = true
				}
			}
		}
	}
	include(s.canonical)
	include(s.provisional)
	return covered, nil
}
