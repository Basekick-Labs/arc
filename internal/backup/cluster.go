package backup

// Cluster awareness for backup and restore (#1083).
//
// On a cluster node the Raft file manifest, not the storage listing, says
// which files are data. Compaction commits in two Raft phases on two watcher
// ticks (internal/compaction/watcher.go): the output is registered when it
// has been written, and the inputs are manifest-deleted on a later tick, once
// the subprocess reports them gone from its storage. Peers pull the output in
// between, but nothing waits for them. A listing taken in that window holds
// the output and the inputs it replaced, and in Pattern 1 compaction runs on
// the compactor, so the primary never holds the _compaction_state manifests
// the #930 restore logic needs. A restore that put both back would serve
// every row twice, on every node, forever. So a cluster backup copies what
// the manifest lists and nothing else, reads the manifest once more at the
// end of the run and drops what the cluster has since said is no longer data,
// says what it left out in each direction, and carries a sidecar from which a
// cluster restore registers each file it writes. The residual window is a
// phase 2 that has not landed by the end of the run: the inputs are still
// registered, and the backup holds them next to the output exactly as the
// cluster does at that moment.
//
// Everything here is behind two optional hooks the Manager holds as
// interfaces: ClusterManifest (the Raft manifest, read and write) and
// TierRecorder (this node's tier metadata). Both are nil on a standalone node
// without tiering, and every path that touches them checks for nil first, so
// OSS behaviour is unchanged. The types are this package's own, not
// raft.FileEntry: internal/backup must not import internal/cluster, and
// cmd/arc/main.go adapts the coordinator to these interfaces.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// ManifestFile is one data file as the cluster manifest describes it. It is
// the row shape of the backup sidecar (see sidecarName) and what the cluster
// hooks exchange. The adapter in cmd/arc/main.go maps it onto raft.FileEntry,
// adding the fields only the cluster knows (origin node, tier, LSN).
type ManifestFile struct {
	Path          string    `json:"path"`
	SHA256        string    `json:"sha256"`
	SizeBytes     int64     `json:"size_bytes"`
	Database      string    `json:"database"`
	Measurement   string    `json:"measurement"`
	PartitionTime time.Time `json:"partition_time"`
	CreatedAt     time.Time `json:"created_at"`
}

// ClusterManifest is the backup manager's view of the cluster's Raft file
// manifest. Nil on a standalone node, and nil on a cluster node without a
// Raft manifest (cluster.raft_data_dir unset): the wiring in cmd/arc/main.go
// only attaches it where a manifest exists, because an empty manifest and a
// missing one must never both read as "nothing is data". One interface for
// both directions on purpose: a backup needs the read side and a restore
// needs both, and a manager with the reader wired but not the writer (or the
// reverse) is a configuration cell nobody runs, so the wiring cannot express
// it.
type ClusterManifest interface {
	// Sync blocks until this node's manifest has applied everything the
	// leader had committed when the call was made. Called before EVERY
	// snapshot (ManifestFiles): the primary writer is routinely a Raft
	// follower and can trail the leader for seconds after a restart, and a
	// stale snapshot would call registered files unregistered.
	Sync(ctx context.Context) error
	// ManifestFiles returns every hot data-file entry in the manifest in one
	// call. CreateBackup snapshots the manifest twice per run (after the
	// listing, and once more at the end of the data copy) and never looks
	// paths up one at a time. The order is unspecified.
	ManifestFiles() []ManifestFile
	// LocalNodeID is the ID the adapter stamps as origin on every entry it
	// registers. The manager only needs its length, to size register batches
	// (registerOpOverhead).
	LocalNodeID() string
	// BatchRegister adds files to the manifest as one Raft entry. The caller
	// chunks by manifestBatchOps and manifestBatchBytes; an error means the
	// whole batch was refused and none of it is in the manifest. Every entry
	// carries a non-zero CreatedAt (the FSM refuses a zero one).
	BatchRegister(ctx context.Context, files []ManifestFile) error
	// BatchDelete removes paths from the manifest as one Raft entry, stamped
	// with reason. Same chunking and failure semantics as BatchRegister.
	BatchDelete(ctx context.Context, paths []string, reason string) error
	// PauseCompaction pauses compaction cluster-wide for reason and returns
	// once every node has acknowledged that it has no compaction batch in
	// flight and no phase-2 manifest commit pending (#1087). Every cluster
	// restore takes it before its first manifest read and releases it with
	// Resume after its last register; the pause stays in force on its own
	// only for a bounded TTL, refreshed while the handle lives, so a restore
	// that dies releases compaction without operator action. An error means
	// the restore must not start: another node holds a pause, a node did not
	// ack in time, or no leader could be reached.
	PauseCompaction(ctx context.Context, reason string) (CompactionPause, error)
}

// CompactionPause is a cluster-wide compaction pause the restore holds.
type CompactionPause interface {
	// Lost reports whether the pause stopped being this restore's while held
	// (another requester took over an expired pause, a refresh failed until
	// it expired, or it was resumed), with the reason. The restore checks it
	// between manifest batches and before declaring success, and fails when
	// it is lost, because a compaction job may have run meanwhile.
	Lost() (bool, error)
	// Resume releases the pause. Idempotent. On a lost pause the release is
	// still attempted, best effort (the record may still be this generation,
	// expired with no leader reachable, and clearing it is correct; a stale
	// generation is refused by the FSM), and nil is returned either way.
	Resume(ctx context.Context) error
}

// TierRecorder receives one report per data file a restore writes, so this
// node's tier metadata describes the file the way it would one the flush path
// wrote. The query layer routes reads from those rows: a measurement with no
// row loses partition pruning, and one with a cold row and no hot row loses
// its local hot glob altogether (internal/tiering/replicated.go). Nil when
// tiering is off. The implementation must not block; tiering queues.
type TierRecorder interface {
	RecordRestoredFile(path string, sizeBytes int64)
}

// SetClusterManifest wires the cluster manifest. A nil argument is ignored,
// so a caller can pass a nil interface unconditionally; a caller holding a
// typed nil pointer must still check for nil itself before calling, because
// an interface holding a typed nil is not == nil (#713).
func (m *Manager) SetClusterManifest(cm ClusterManifest) {
	if cm == nil {
		return
	}
	m.cluster = cm
	m.logger.Info().Msg("Cluster manifest wired: backups cross-check the listing against the manifest and restores register the files they write")
}

// SetTierRecorder wires this node's tier metadata. Nil is ignored, as above.
func (m *Manager) SetTierRecorder(r TierRecorder) {
	if r == nil {
		return
	}
	m.tierRecorder = r
}

// Manifest batch caps. They mirror registrarDrainBatch and
// registrarDrainChunkBytes in internal/cluster/file_registrar.go, which that
// package pins with TestCoordinatorFileRegistrar_DrainChunksFitTheForwardFrame:
// 1000 operations is the Raft log-entry cap every manifest batch in Arc uses,
// and 256 KiB of payload is what still fits the 1 MiB forward frame after the
// payload is base64-encoded three times on its way to the leader (about 2.4x)
// plus per-op JSON framing. The cap on bytes matters because in Pattern 1 the
// primary writer is routinely a Raft follower, so every batch is forwarded.
// cmd/arc pins the fit over the real adapter payloads with a maximum-length
// node ID and maximum-length paths.
const (
	ManifestBatchOps   = 1000
	ManifestBatchBytes = 256 << 10
)

// The caps in effect, as variables so tests can shrink them and drive the
// chunking with a handful of files; TestManifestChunks_CapsMirrorTheRegistrar
// pins them to the constants.
var (
	manifestBatchOps   = ManifestBatchOps
	manifestBatchBytes = ManifestBatchBytes
)

// Per-operation byte overhead added to a marshalled ManifestFile or path to
// estimate the raft payload the adapter builds from it.
//
// The register payload is {"file":<raft.FileEntry>} and the FileEntry carries
// three fields a ManifestFile lacks: exactly `{"file":` + `}` (9 bytes),
// `,"origin_node_id":""` (19) plus the node ID itself, `,"tier":"hot"` (13)
// and `,"lsn":0` (8), 49 bytes plus the node ID. registerOpBaseOverhead
// rounds the fixed part up and registerOpOverhead adds the real node ID
// length, so the estimate holds for any node ID rather than only for ones
// under some boundary. The delete payload is {"path":...,"reason":...} plus
// the op type. If raft.FileEntry grows a field, grow the base.
const (
	registerOpBaseOverhead   = 64
	manifestDeleteOpOverhead = 96
)

// registerOpOverhead is the per-op overhead for entries stamped with a node
// ID of nodeIDLen bytes.
func registerOpOverhead(nodeIDLen int) int {
	return registerOpBaseOverhead + nodeIDLen
}

// opBatcher applies both caps to a batch being assembled one operation at a
// time. fits reports whether an operation of sz bytes may join the current
// batch; a batch always accepts its first operation so an oversized single
// entry is sent alone rather than never.
type opBatcher struct {
	ops   int
	bytes int
}

func (b *opBatcher) fits(sz int) bool {
	return b.ops == 0 || (b.ops < manifestBatchOps && b.bytes+sz <= manifestBatchBytes)
}

func (b *opBatcher) add(sz int) {
	b.ops++
	b.bytes += sz
}

func (b *opBatcher) reset() {
	b.ops, b.bytes = 0, 0
}

// manifestChunks splits n operations, in order, into index ranges [lo, hi)
// that each respect both caps. opBytes returns the estimated payload size of
// operation i.
func manifestChunks(n int, opBytes func(i int) int) [][2]int {
	var out [][2]int
	var b opBatcher
	lo := 0
	for i := 0; i < n; i++ {
		sz := opBytes(i)
		if !b.fits(sz) {
			out = append(out, [2]int{lo, i})
			lo = i
			b.reset()
		}
		b.add(sz)
	}
	if lo < n {
		out = append(out, [2]int{lo, n})
	}
	return out
}

// registerOpBytes estimates the raft payload of registering f with the given
// per-op overhead (registerOpOverhead). A row that does not marshal is sized
// as a whole batch so it ships alone.
func registerOpBytes(f ManifestFile, overhead int) int {
	data, err := json.Marshal(f)
	if err != nil {
		return manifestBatchBytes
	}
	return len(data) + overhead
}

// RegisterOpBytes is the estimate the manager uses to fill a register batch
// for entries stamped with originNodeID, exported so cmd/arc can pin that a
// batch filled to ManifestBatchBytes by this estimate fits the forward frame
// once the adapter has built the real payloads.
func RegisterOpBytes(f ManifestFile, originNodeID string) int {
	return registerOpBytes(f, registerOpOverhead(len(originNodeID)))
}

// deleteOpBytes estimates the raft payload of deleting path.
func deleteOpBytes(path, reason string) int {
	return len(path) + len(reason) + manifestDeleteOpOverhead
}

// sidecarName is the file under <backupID>/ that lists every database data
// file the backup copied, with the facts a cluster restore registers from:
// the SHA-256 of the bytes the backup holds, the size, and the manifest
// entry's database, measurement, partition time and created_at. Written by
// every backup, clustered or not, so a standalone backup can be restored on a
// cluster node later. Not under data/, so the restore listing never sees it.
const sidecarName = "manifest-files.json"

const sidecarVersion = 1

// fileSidecar is the on-disk shape of sidecarName.
type fileSidecar struct {
	Version int `json:"version"`
	// FromClusterManifest records whether the entries' database, measurement,
	// partition time and created_at came from the cluster manifest (true) or
	// were derived from each path by a standalone backup (false). The SHA-256
	// and size always describe the bytes in the backup.
	FromClusterManifest bool           `json:"from_cluster_manifest"`
	Files               []ManifestFile `json:"files"`
}

func sidecarPath(backupID string) string {
	return backupID + "/" + sidecarName
}

// sidecarBuilder collects the sidecar rows of one backup run. byPath holds
// the cluster manifest's entries when one is wired (nil otherwise); the copy
// loop adds a row per database data file it copied, and the end-of-run
// re-check drops the rows of files the manifest no longer lists.
//
// The rows stay in memory for the run (about 200 bytes each): the re-check
// needs the copied set to compare against the second manifest snapshot. The
// JSON is not held as well; writeSidecar streams it to a temp file.
type sidecarBuilder struct {
	byPath  map[string]ManifestFile
	files   []ManifestFile
	dropped map[string]bool
	// shaMismatches counts copied files whose bytes hash differently from
	// what the manifest registered for the path. The sidecar carries the hash
	// of the bytes, because that is what a restore writes and what peers
	// verify a pull against; the mismatch itself is logged, since it means
	// this node serves bytes the manifest does not describe (a rewrite in
	// flight, or divergence).
	shaMismatches int
}

// add records one copied database data file. created_at falls back to now
// for a file the manifest does not describe (a standalone backup): the FSM
// refuses a zero created_at, and "the backup saw it then" is the most honest
// value a standalone node has.
func (b *sidecarBuilder) add(path, sha string, size int64, now time.Time) (mismatch bool) {
	path = filepath.ToSlash(path)
	row := ManifestFile{Path: path, SHA256: sha, SizeBytes: size}
	if e, ok := b.byPath[path]; ok {
		row.Database, row.Measurement = e.Database, e.Measurement
		row.PartitionTime, row.CreatedAt = e.PartitionTime, e.CreatedAt
		if e.SHA256 != "" && e.SHA256 != sha {
			b.shaMismatches++
			mismatch = true
		}
	} else {
		row.Database, row.Measurement = parseDBMeasurement(path)
		row.PartitionTime = partitionTimeFromPath(path)
	}
	if row.CreatedAt.IsZero() {
		row.CreatedAt = now.UTC()
	}
	b.files = append(b.files, row)
	return mismatch
}

// drop marks a copied row as no longer part of the backup (the cluster
// manifest stopped listing the file during the run). O(1); rows resolves the
// marks in one pass.
func (b *sidecarBuilder) drop(path string) {
	if b.dropped == nil {
		b.dropped = make(map[string]bool)
	}
	b.dropped[filepath.ToSlash(path)] = true
}

// rows returns the live rows, dropping the marked ones, and compacts the
// slice so later calls are cheap.
func (b *sidecarBuilder) rows() []ManifestFile {
	if len(b.dropped) == 0 {
		return b.files
	}
	kept := b.files[:0]
	for _, f := range b.files {
		if !b.dropped[f.Path] {
			kept = append(kept, f)
		}
	}
	b.files = kept
	b.dropped = nil
	return b.files
}

// writeSidecar stores the run's rows under <backupID>/manifest-files.json,
// streaming the JSON through a temp file rather than marshalling it in memory
// next to the rows: a million-file backup has a sidecar of a few hundred MB.
func (m *Manager) writeSidecar(ctx context.Context, backupID string, b *sidecarBuilder) error {
	tmp, err := createTempFile("arc-backup-sidecar-*.json")
	if err != nil {
		return fmt.Errorf("failed to create sidecar temp file: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	w := &trackingWriter{w: tmp}
	fail := func(err error) error {
		if w.err != nil {
			err = w.err
		}
		return fmt.Errorf("failed to write sidecar temp file: %w", err)
	}
	if _, err := fmt.Fprintf(w, `{"version":%d,"from_cluster_manifest":%t,"files":[`, sidecarVersion, b.byPath != nil); err != nil {
		return fail(err)
	}
	enc := json.NewEncoder(w)
	for i, f := range b.rows() {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return fail(err)
			}
		}
		if err := enc.Encode(f); err != nil { // Encode appends a newline, which JSON permits between values
			return fail(err)
		}
	}
	if _, err := io.WriteString(w, "]}"); err != nil {
		return fail(err)
	}

	info, err := tmp.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat sidecar temp file: %w", err)
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek sidecar temp file: %w", err)
	}
	if err := m.backupStorage.WriteReader(ctx, sidecarPath(backupID), tmp, info.Size()); err != nil {
		m.cleanupPartialWrite(ctx, m.backupStorage, sidecarPath(backupID))
		return fmt.Errorf("failed to write file sidecar: %w", err)
	}
	return nil
}

// readSidecar loads a backup's sidecar as a path-keyed map. ok is false,
// with a nil error, when the backup has none (it predates #1083).
func (m *Manager) readSidecar(ctx context.Context, backupID string) (entries map[string]ManifestFile, ok bool, err error) {
	p := sidecarPath(backupID)
	exists, err := m.backupStorage.Exists(ctx, p)
	if err != nil {
		return nil, false, fmt.Errorf("failed to check for the file sidecar: %w", err)
	}
	if !exists {
		return nil, false, nil
	}
	data, err := m.backupStorage.Read(ctx, p)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read the file sidecar: %w", err)
	}
	var sc fileSidecar
	if err := json.Unmarshal(data, &sc); err != nil {
		return nil, false, fmt.Errorf("failed to decode the file sidecar: %w", err)
	}
	entries = make(map[string]ManifestFile, len(sc.Files))
	for _, f := range sc.Files {
		entries[filepath.ToSlash(f.Path)] = f
	}
	return entries, true, nil
}

// isRegistrableDataFile reports whether a storage key is a database data
// file: a Parquet file under a database root. Those are the only keys the
// cluster manifest holds, the only ones a cluster backup cross-checks, and
// the only ones a cluster restore registers or reports to tiering. Parquet
// under a reserved root (the _schema anchors, anything under "_" or ".") is
// Arc's own state and is copied and restored as before, never registered:
// the reconciliation sweep excludes those roots on purpose
// (internal/reconciliation/walk.go), and registering them would make the
// sweep and the manifest disagree. Iceberg metadata is never Parquet.
func isRegistrableDataFile(p string) bool {
	p = filepath.ToSlash(p)
	if !strings.HasSuffix(p, ".parquet") {
		return false
	}
	first, rest, found := strings.Cut(p, "/")
	if !found || rest == "" {
		return false
	}
	return !storage.IsReservedRootDir(first)
}

// partitionTimeFromPath derives the hour partition from the storage layout
// {db}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{file}.parquet, in UTC. Zero when
// the path does not have that shape; a standalone backup then records a
// zero partition time, which the manifest accepts.
func partitionTimeFromPath(p string) time.Time {
	parts := strings.Split(filepath.ToSlash(p), "/")
	if len(parts) < 7 || len(parts[2]) != 4 || len(parts[3]) != 2 || len(parts[4]) != 2 || len(parts[5]) != 2 {
		return time.Time{}
	}
	y, err1 := strconv.Atoi(parts[2])
	mo, err2 := strconv.Atoi(parts[3])
	d, err3 := strconv.Atoi(parts[4])
	h, err4 := strconv.Atoi(parts[5])
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return time.Time{}
	}
	if mo < 1 || mo > 12 || d < 1 || d > 31 || h < 0 || h > 23 {
		return time.Time{}
	}
	t := time.Date(y, time.Month(mo), d, h, 0, 0, 0, time.UTC)
	// time.Date normalises an impossible day (02/30) into the next month;
	// refuse that rather than register a partition the path does not name.
	if t.Day() != d {
		return time.Time{}
	}
	return t
}
