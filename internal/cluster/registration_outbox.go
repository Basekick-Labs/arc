package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// RegistrationJournal is the outbox journal file name inside the Raft data
// directory. The name and the add-record format are the ones #1206 shipped,
// so a journal left by that version loads unchanged.
const RegistrationJournal = "pending-registrations.jsonl"

// journalRecordVersion is written on every record. #1207 extends records
// (flush group, WAL coverage); a reader treats a missing version as 1.
const journalRecordVersion = 1

const (
	journalOpAdd = "add"
	journalOpAck = "ack"
)

// OutboxManifest is the slice of the coordinator the outbox drain needs, so a
// test can drive it without Raft.
type OutboxManifest interface {
	WaitForLeader(timeout time.Duration) error
	SyncManifest(ctx context.Context) error
	GetFileEntry(path string) (*raft.FileEntry, bool)
	BatchFileOpsInManifestContext(ctx context.Context, ops []raft.BatchFileOp) error
	DeleteFileFromManifest(ctx context.Context, path, reason string) error
	// StreamClaimsEnabled reports whether this node's FSM has stream claims
	// on: registrations then carry their coverage (#1207).
	StreamClaimsEnabled() bool
}

// OutboxStorage reports a file's size, -1 when it does not exist.
type OutboxStorage interface {
	StatFile(ctx context.Context, path string) (int64, error)
}

// RegistrationOutbox is how a node that ingests announces every Parquet file it
// flushes to the cluster manifest (#1206, #1209).
//
// The registrar it replaces was fire-and-forget: a 4096-entry queue that
// dropped when full, one attempt per file, and nothing on disk. Since the flush
// checkpoints the WAL right after registering, a dropped or failed
// registration, or a crash before the apply, left a file only its writer could
// query, with nothing left to re-register it.
//
// RegisterFile appends an add record to a journal in the Raft data directory.
// The flush calls SyncRegistrations once, after its last RegisterFile and
// before it checkpoints the WAL, so the records are durable before the WAL can
// drop the rows they describe. One fsync covers every record appended before
// it, so concurrent flushes share it.
//
// A drain started by HandOff runs for the life of the process. It registers
// pending files in batches, retries every failure with backoff, and appends an
// ack record (fsynced) once a batch is confirmed. A file that is gone, has
// changed size or is already in the manifest is acknowledged without being
// registered; those checks cost a storage stat, so a file flushed by this
// process gets them only from its second attempt on.
//
// Before HandOff it only collects: startup WAL recovery, MQTT ingest and
// anything else flushed before the cluster is up land here and wait (#1206).
//
// Built only on a node that can ingest: on a reader or a compactor the WAL
// holds nothing but entries received from a writer before #888, and announcing
// their flushes would spread duplicate rows to every node.
type RegistrationOutbox struct {
	journalPath string
	clusterName string
	logger      zerolog.Logger

	// syncMu serialises fsyncs and journal rewrites. Lock order: syncMu, then
	// mu. Appends take only mu, so they never wait on a fsync.
	syncMu sync.Mutex

	mu          sync.Mutex
	state       outboxState
	nodeID      string
	journal     *os.File
	broken      bool      // the journal fd is not trusted; the next sync rewrites it from memory
	lastRewrite time.Time // last failed rewrite
	batchEnd    int       // queue index just past the last batch nextBatch returned
	written     uint64    // records appended so far (a generation counter)
	synced      uint64    // generation the last successful fsync or rewrite covers
	fileRecords int       // records in the journal file, adds and acks, for compaction
	queue       []*outboxEntry
	head        int
	byPath      map[string]*outboxEntry
	live        int // entries not yet acknowledged
	// recent remembers the last files this outbox confirmed in the manifest,
	// for ReplacePending: this node's FSM can apply a commit after the leader
	// confirmed it, so "not in my FSM and not pending" is not proof of absence.
	recent     map[string]recentConfirm
	recentRing []string
	recentNext int

	wake        chan struct{}
	cancel      context.CancelFunc
	done        chan struct{}
	manifestRef OutboxManifest // kept for the final pass in Stop
	storeRef    OutboxStorage

	// The registration hold (registration_hold.go). holdGen counts changes to
	// the held set; unsyncedAcks are acknowledged entries whose ack is not
	// durable yet.
	holdGen         uint64
	hold            coverageHold
	holdBuiltGen    uint64
	holdBuiltSynced uint64
	unsyncedAcks    []*outboxEntry

	registered atomic.Int64
	skipped    atomic.Int64
	dropped    atomic.Int64
	lastAlarm  atomic.Int64 // unix nanos of the last backlog alarm
}

type recentConfirm struct {
	rec  journalRecord
	at   time.Time
	slot int
}

type outboxState int

const (
	outboxCollecting outboxState = iota
	outboxRunning
	outboxStopped
	outboxAbandoned
)

type outboxEntry struct {
	rec   journalRecord
	added time.Time
	// fresh: flushed by this process and not attempted yet. The file was just
	// written, so the storage checks are skipped on that first attempt.
	fresh bool
	done  bool
	// inFlight: in the batch attempt is working on. ReplacePending waits it
	// out rather than race a commit.
	inFlight bool
	// inManifest: settled because the manifest has it (registered by this
	// outbox, or found there already), as opposed to skipped.
	inManifest bool
	// vanished: registered, then found gone from storage and taken back out
	// of the manifest. Its rows are in no registered file.
	vanished bool
	// committed: the manifest accepted it, but this node's FSM had not
	// applied it when the attempt ended. Not sent again; settled once this
	// node sees it (#1207).
	committed bool
	// refused: the manifest refused it for good — another writer claimed
	// its rows (#1207). Settled without registering; the file stays.
	refused bool
	// ackAt is the journal generation of its ack record; its batches are held
	// until the journal is synced past it (#1207 S4c).
	ackAt uint64
}

// journalRecord is one journal line. ClusterName pins it to the cluster: a data
// directory moved into another cluster must not announce its files there. The
// node ID is not recorded. Without cluster.node_id it is hostname-PID, which
// changes with every start on a VM, and the file is on this node's disk either
// way, so the origin is this process's node ID at hand-off.
type journalRecord struct {
	Version       int       `json:"v,omitempty"`
	Op            string    `json:"op,omitempty"` // "" (a #1206 record) means add
	ClusterName   string    `json:"cluster_name"`
	Database      string    `json:"database,omitempty"`
	Measurement   string    `json:"measurement,omitempty"`
	Path          string    `json:"path"`
	PartitionTime time.Time `json:"partition_time,omitempty"`
	SizeBytes     int64     `json:"size_bytes,omitempty"`
	SHA256        string    `json:"sha256,omitempty"`
	// FlushedAt keeps the backlog age honest across restarts.
	FlushedAt time.Time `json:"at,omitempty"`
	// Supersedes names a file this one replaced before either was registered
	// (a DELETE rewrite, #1210). The drain deletes it from the manifest in the
	// same batch that registers this file, in case an apply that reported
	// failure had committed it after all.
	Supersedes string `json:"supersedes,omitempty"`
	// Cov is the coverage of the flush group the file belongs to (#1207),
	// on a coverage-enabled writer; a record carrying it is version 2. Every
	// file of a group carries the whole group's coverage. Readers before
	// #1207 ignore the field.
	Cov *ingest.GroupCoverage `json:"cov,omitempty"`
}

const journalRecordVersionCoverage = 2

// maxJournalLine bounds one journal line. Far above any real record: a
// group's coverage folds whole batches into ranges.
const maxJournalLine = 64 << 20

// Tunables, variables so tests can shrink them.
var (
	outboxRetryMin       = 200 * time.Millisecond
	outboxRetryMax       = 5 * time.Second
	outboxLeaderWait     = time.Second // not cancellable; kept under outboxStopBudget
	outboxStopBudget     = registrarDrainTimeout
	outboxAttemptTimeout = forwardApplyTimeout
	outboxAlarmAge       = time.Minute
	// outboxFreshWindow: a file flushed this recently by this process skips the
	// storage checks on its first attempt. Age, not "never attempted": during
	// an outage only the head batch is attempted, and a file behind it can be
	// deleted long before its turn.
	outboxFreshWindow = 30 * time.Second
	// outboxStatParallelism bounds concurrent storage checks; on object
	// storage each one is a request.
	outboxStatParallelism = 32
	// outboxBrokenRetry spaces out rewrites of a journal that keeps failing,
	// so a full Raft volume does not cost every flush a rewrite.
	outboxBrokenRetry = 5 * time.Second
	// outboxRecentConfirmed bounds how many confirmed paths ReplacePending
	// can still recognise.
	outboxRecentConfirmed = 4096
	// outboxRecentWindow is how long a confirmation is trusted over this
	// node's FSM: long enough for a commit to apply locally, short enough that
	// a file compaction has since removed is not reported registered.
	outboxRecentWindow = time.Minute
	// outboxReplaceWait caps how long ReplacePending and Withdraw wait out an
	// attempt in flight, so a DELETE is never held for a whole slow batch.
	outboxReplaceWait = 30 * time.Second
	outboxAlarmEvery  = 5 * time.Minute
	// A journal is rewritten without its acks once it holds this many records
	// more than twice what is pending.
	outboxCompactSlack = 4096
	// fsyncFile is the fsync the journal uses; a test counts or fails it.
	fsyncFile = func(f *os.File) error { return f.Sync() }
)

// NewRegistrationOutbox opens the journal in dir and loads what a previous
// process left in it. Records stamped with another cluster are dropped with a
// warning.
func NewRegistrationOutbox(dir, clusterName string, logger zerolog.Logger) (*RegistrationOutbox, error) {
	// 0700, as Raft creates it: on a fresh node this runs first, and MkdirAll
	// does not tighten an existing directory, so a wider mode here would expose
	// the Raft snapshots written into it later.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	r := &RegistrationOutbox{
		journalPath: filepath.Join(dir, RegistrationJournal),
		clusterName: clusterName,
		logger:      logger.With().Str("component", "file-registrar").Logger(),
		byPath:      make(map[string]*outboxEntry),
		recent:      make(map[string]recentConfirm),
		recentRing:  make([]string, outboxRecentConfirmed),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	// A crash mid-rewrite leaves its temp file behind.
	if leftovers, _ := filepath.Glob(filepath.Join(dir, RegistrationJournal+".tmp-*")); len(leftovers) > 0 {
		for _, f := range leftovers {
			_ = os.Remove(f)
		}
	}
	loaded, foreign, err := readJournal(r.journalPath, clusterName)
	if err != nil {
		return nil, err
	}
	if foreign > 0 {
		r.logger.Warn().Int("records", foreign).Str("journal", r.journalPath).
			Msg("Pending file registrations written for another cluster were dropped from the journal")
	}
	now := time.Now()
	for _, rec := range loaded {
		added := now
		if !rec.FlushedAt.IsZero() {
			added = rec.FlushedAt
		}
		r.enqueueLocked(rec, added, false)
	}
	// Rewrite on load: drops acks, foreign records and a torn last line, and
	// leaves an open journal holding exactly what is pending.
	r.syncMu.Lock()
	r.mu.Lock()
	err = r.rewriteLocked()
	r.mu.Unlock()
	r.syncMu.Unlock()
	if err != nil {
		return nil, err
	}
	if r.live > 0 {
		r.logger.Info().Int("files", r.live).
			Msg("Loaded file registrations a previous start did not confirm; they are registered once the cluster is up")
	}
	if n := r.unknownHoldsLocked(); n > 0 {
		r.logger.Warn().Int("files", n).
			Msg("Pending file registrations with truncated coverage from an older release: their WAL entries are not held until they register")
	}
	r.publishMetrics()
	return r, nil
}

// readJournal returns the add records for this cluster that no ack follows,
// in order, and how many records belonged to another cluster. A line that does
// not parse is skipped, so a torn line never takes the records after it.
func readJournal(path, clusterName string) ([]journalRecord, int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open journal: %w", err)
	}
	defer f.Close()
	var adds []journalRecord
	index := make(map[string]int)
	foreign := 0
	// Not a bufio.Scanner: its token limit fails the whole read on one long
	// line, and a failed read makes the node fall back to the registrar that
	// keeps nothing across restarts. A line over maxJournalLine is skipped
	// like any other line that does not parse.
	br := bufio.NewReaderSize(f, 64<<10)
	var readErr error
	for {
		line, err := readJournalLine(br)
		if err != nil && err != io.EOF {
			readErr = err
			break
		}
		if len(line) == 0 && err == io.EOF {
			break
		}
		var rec journalRecord
		if line == nil || json.Unmarshal(line, &rec) != nil || rec.Path == "" {
			if err == io.EOF {
				break
			}
			continue
		}
		if rec.ClusterName != clusterName {
			foreign++
			continue
		}
		switch rec.Op {
		case "", journalOpAdd:
			if i, ok := index[rec.Path]; ok {
				adds[i] = rec // the latest record for a path wins, as in memory
				continue
			}
			index[rec.Path] = len(adds)
			adds = append(adds, rec)
		case journalOpAck:
			if i, ok := index[rec.Path]; ok {
				adds[i].Path = "" // acknowledged
				delete(index, rec.Path)
			}
		}
		if err == io.EOF {
			break
		}
	}
	if readErr != nil {
		// Unreadable past this point: refuse rather than rewrite a partial
		// set over the journal on load.
		return nil, 0, fmt.Errorf("read journal: %w", readErr)
	}
	out := adds[:0]
	for _, rec := range adds {
		if rec.Path != "" {
			out = append(out, rec)
		}
	}
	return out, foreign, nil
}

// readJournalLine reads one line. A line longer than maxJournalLine is
// consumed without being held in memory and comes back as a nil line with a
// nil error (or io.EOF at the end), for the caller to skip.
func readJournalLine(br *bufio.Reader) ([]byte, error) {
	var line []byte
	over := false
	for {
		chunk, err := br.ReadSlice('\n')
		if !over {
			if len(line)+len(chunk) > maxJournalLine {
				over, line = true, nil
			} else {
				line = append(line, chunk...)
			}
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if over {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		if line == nil {
			line = []byte{}
		}
		return line, err
	}
}

// RegisterFile implements ingest.FileRegistrar. It appends without fsyncing;
// SyncRegistrations makes the append durable.
func (r *RegistrationOutbox) RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string) {
	r.RegisterCoveredFile(database, measurement, path, partitionTime, sizeBytes, sha256, nil)
}

// RegisterCoveredFile implements ingest.CoverageFileRegistrar: RegisterFile
// with the coverage of the file's flush group (#1207), journaled with the
// record. The file's outcome is reported through cov.Settle.
func (r *RegistrationOutbox) RegisterCoveredFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string, cov *ingest.GroupCoverage) {
	rec := journalRecord{
		Version: journalRecordVersion, Op: journalOpAdd, ClusterName: r.clusterName,
		Database: database, Measurement: measurement, Path: path,
		PartitionTime: partitionTime, SizeBytes: sizeBytes, SHA256: sha256,
		FlushedAt: time.Now().UTC(), Cov: cov,
	}
	if cov != nil {
		rec.Version = journalRecordVersionCoverage
	}
	r.mu.Lock()
	switch r.state {
	case outboxAbandoned:
		r.mu.Unlock()
		cov.Settle(path, ingest.CoverageAbandoned)
		if n := r.dropped.Add(1); n&(n-1) == 0 {
			r.logger.Warn().Str("path", path).Int64("total_dropped", n).
				Msg("Cluster manifest unavailable in this start; file not announced to it")
		}
		return
	case outboxStopped:
		// After the shutdown drain: keep it for the next start, durably now,
		// since no flush sync follows.
		r.enqueueLocked(rec, time.Now(), true)
		r.appendLocked(rec)
		r.mu.Unlock()
		_ = r.SyncRegistrations()
		return
	}
	r.enqueueLocked(rec, time.Now(), true)
	r.appendLocked(rec)
	r.mu.Unlock()
	r.publishMetrics()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *RegistrationOutbox) enqueueLocked(rec journalRecord, added time.Time, fresh bool) {
	if e, ok := r.byPath[rec.Path]; ok && !e.done {
		// A second flush to one path is not expected. Keep the latest record
		// as a new entry, without touching one attempt may be reading. The
		// earlier file's rows were overwritten: report them lost.
		e.done = true
		r.live--
		if e.rec.Cov != nil {
			r.holdGen++
		}
		if e.rec.Cov != nil && e.rec.Cov != rec.Cov {
			go e.rec.Cov.Settle(e.rec.Path, ingest.CoverageLost)
		}
	}
	e := &outboxEntry{rec: rec, added: added, fresh: fresh}
	if rec.Cov != nil {
		r.holdGen++
	}
	r.byPath[rec.Path] = e
	r.queue = append(r.queue, e)
	r.live++
}

// appendLocked writes one record. On a write error it truncates back to where
// the record started, so no torn line is left, and marks the journal broken:
// the next sync rewrites it from memory.
func (r *RegistrationOutbox) appendLocked(rec journalRecord) {
	r.written++
	if r.broken || r.journal == nil {
		return
	}
	line, err := marshalRecord(rec)
	if err == nil {
		var off int64
		if off, err = r.journal.Seek(0, io.SeekEnd); err == nil {
			if _, err = r.journal.Write(append(line, '\n')); err != nil {
				_ = r.journal.Truncate(off)
			}
		}
	}
	if err != nil {
		r.markBrokenLocked(err)
		return
	}
	r.fileRecords++
}

// marshalRecord writes an ack with only the fields it needs: a zero
// partition time would otherwise be spelled out on every one.
func marshalRecord(rec journalRecord) ([]byte, error) {
	if rec.Op == journalOpAck {
		return json.Marshal(struct {
			Version     int    `json:"v"`
			Op          string `json:"op"`
			ClusterName string `json:"cluster_name"`
			Path        string `json:"path"`
		}{rec.Version, rec.Op, rec.ClusterName, rec.Path})
	}
	return json.Marshal(rec)
}

func (r *RegistrationOutbox) markBrokenLocked(err error) {
	metrics.Get().IncFileRegistrationJournalErrors()
	if !r.broken {
		r.logger.Error().Err(err).Str("journal", r.journalPath).
			Msg("File registration journal write failed; registrations are held in memory and the journal is rewritten on the next sync")
	}
	r.broken = true
}

// SyncRegistrations makes every record appended so far durable. The flush
// calls it before checkpointing the WAL. Concurrent callers share one fsync:
// whoever holds syncMu covers every append written before its fsync, and a
// caller arriving after finds its records already covered.
func (r *RegistrationOutbox) SyncRegistrations() error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	r.mu.Lock()
	target := r.written
	if r.synced >= target && !r.broken {
		r.mu.Unlock()
		return nil
	}
	if r.broken {
		if time.Since(r.lastRewrite) < outboxBrokenRetry {
			r.mu.Unlock()
			return errors.New("file registration journal unavailable; held in memory")
		}
		err := r.rewriteLocked()
		r.mu.Unlock()
		return err
	}
	if r.journal == nil { // abandoned: nothing is journaled any more
		r.mu.Unlock()
		return nil
	}
	f := r.journal
	r.mu.Unlock()

	err := fsyncFile(f) // appends continue on the same fd meanwhile

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.markBrokenLocked(err)
		return err
	}
	if target > r.synced {
		r.synced = target
	}
	r.pruneUnsyncedAcksLocked()
	return nil
}

// rewriteLocked replaces the journal with the pending adds and reopens the
// append handle on the new file: temp file, fsync, rename, fsync the
// directory. Callers hold syncMu and mu, so no fsync runs on the old fd and no
// append lands on it after the rename.
func (r *RegistrationOutbox) rewriteLocked() error {
	dir := filepath.Dir(r.journalPath)
	tmp, err := os.CreateTemp(dir, RegistrationJournal+".tmp-*")
	if err != nil {
		return r.rewriteFailedLocked(fmt.Errorf("create journal temp: %w", err))
	}
	defer os.Remove(tmp.Name())
	w := bufio.NewWriter(tmp)
	n := 0
	for _, e := range r.queue[r.head:] {
		if e == nil || e.done {
			continue
		}
		line, err := json.Marshal(e.rec)
		if err != nil {
			continue
		}
		_, _ = w.Write(append(line, '\n'))
		n++
	}
	if err := w.Flush(); err != nil {
		_ = tmp.Close()
		return r.rewriteFailedLocked(fmt.Errorf("write journal temp: %w", err))
	}
	if err := fsyncFile(tmp); err != nil {
		_ = tmp.Close()
		return r.rewriteFailedLocked(fmt.Errorf("sync journal temp: %w", err))
	}
	if err := tmp.Close(); err != nil {
		return r.rewriteFailedLocked(err)
	}
	if err := os.Rename(tmp.Name(), r.journalPath); err != nil {
		return r.rewriteFailedLocked(fmt.Errorf("replace journal: %w", err))
	}
	if err := syncDir(dir); err != nil {
		return r.rewriteFailedLocked(err)
	}
	f, err := os.OpenFile(r.journalPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return r.rewriteFailedLocked(fmt.Errorf("open journal: %w", err))
	}
	if r.journal != nil {
		_ = r.journal.Close()
	}
	r.journal = f
	r.broken = false
	r.fileRecords = n
	r.synced = r.written
	return nil
}

func (r *RegistrationOutbox) rewriteFailedLocked(err error) error {
	r.lastRewrite = time.Now() // spaces out the next attempt
	r.markBrokenLocked(err)
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open journal directory: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync journal directory: %w", err)
	}
	return nil
}

// Pending reports how many files wait for registration.
func (r *RegistrationOutbox) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.live
}

// IsPending reports whether path waits for registration. For cleanup paths
// that must not act on a file the manifest does not know yet (#1210).
func (r *RegistrationOutbox) IsPending(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byPath[path]
	return ok && !e.done
}

// Abandon is for a start whose cluster coordinator never came up. From here
// RegisterFile drops instead of journaling, so a node running without its
// cluster does not grow the journal without bound. The journal keeps what was
// already pending for the next start. Reports whether this call abandoned it:
// false after a hand-off, when the drain is running.
func (r *RegistrationOutbox) Abandon() bool {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != outboxCollecting {
		return false
	}
	r.state = outboxAbandoned
	// Nothing in this process will register these files. Their groups stop
	// holding WAL entries; the journal still carries them for the next start.
	for _, e := range r.byPath {
		if !e.done && e.rec.Cov != nil {
			go e.rec.Cov.Settle(e.rec.Path, ingest.CoverageAbandoned)
		}
	}
	if r.journal != nil {
		if err := fsyncFile(r.journal); err == nil {
			r.synced = r.written
		}
		_ = r.journal.Close()
		r.journal = nil
	}
	r.logger.Warn().Int("pending", r.live).Str("journal", r.journalPath).
		Msg("Cluster manifest unavailable in this start; pending file registrations stay in the journal for the next start")
	covered := 0
	for _, e := range r.byPath {
		if !e.done && e.rec.Cov != nil {
			covered++
		}
	}
	if covered > 0 {
		r.logger.Error().Int("files", covered).
			Msg("WAL entries of files waiting to register are kept until a start with the cluster registers them; the WAL grows with them")
	}
	return true
}

// HandOff starts the drain that registers pending files through manifest
// under nodeID, this node's cluster ID. It runs until Stop.
func (r *RegistrationOutbox) HandOff(manifest OutboxManifest, store OutboxStorage, nodeID string) {
	r.mu.Lock()
	if r.state != outboxCollecting {
		r.mu.Unlock()
		return
	}
	r.state = outboxRunning
	r.nodeID = nodeID
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	n := r.live
	r.mu.Unlock()
	if n > 0 {
		r.logger.Info().Int("files", n).Msg("Registering files flushed before the cluster was up")
	}
	go r.run(ctx, manifest, store)
}

// Stop ends the drain, then spends at most outboxStopBudget registering what
// is still pending, so the final flush lands in the manifest before Raft
// stops. What is left stays in the journal for the next start.
func (r *RegistrationOutbox) Stop() {
	r.mu.Lock()
	cancel, done, state := r.cancel, r.done, r.state
	if state == outboxRunning || state == outboxCollecting {
		r.state = outboxStopped
	}
	r.mu.Unlock()
	if state != outboxRunning {
		_ = r.SyncRegistrations()
		return
	}
	cancel()
	timer := time.NewTimer(outboxStopBudget)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		r.logger.Warn().Msg("File registration drain did not stop in time; unconfirmed files stay in the journal")
		return
	}
	r.mu.Lock()
	manifest, store := r.manifestRef, r.storeRef
	r.mu.Unlock()
	ctx, cancelFinal := context.WithTimeout(context.Background(), outboxStopBudget)
	defer cancelFinal()
	for manifest != nil && ctx.Err() == nil {
		progressed, err := r.attempt(ctx, manifest, store)
		if err != nil || !progressed {
			break
		}
	}
	_ = r.SyncRegistrations()
	if n := r.Pending(); n > 0 {
		r.logger.Warn().Int("files", n).
			Msg("File registrations not confirmed at shutdown stay in the journal and are registered on the next start")
	}
}

// Done is closed when the drain loop has exited, which happens only after
// HandOff and then Stop.
func (r *RegistrationOutbox) Done() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

func (r *RegistrationOutbox) run(ctx context.Context, manifest OutboxManifest, store OutboxStorage) {
	r.mu.Lock()
	r.manifestRef, r.storeRef = manifest, store
	done := r.done
	r.mu.Unlock()
	defer close(done)

	delay := outboxRetryMin
	failures := 0
	sleep := func(d time.Duration) bool {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}
	backoff := func(err error, msg string) bool {
		failures++
		if failures&(failures-1) == 0 {
			r.logger.Warn().Err(err).Int("attempts", failures).Int("pending", r.Pending()).Msg(msg)
		}
		ok := sleep(delay)
		if delay *= 2; delay > outboxRetryMax {
			delay = outboxRetryMax
		}
		return ok
	}

	// The "already in the manifest" check reads this node's FSM, which can
	// trail the leader right after a restart. Catch up once first, so a file a
	// previous start registered is not registered again.
	for {
		if ctx.Err() != nil {
			return
		}
		r.alarm()
		if err := manifest.WaitForLeader(outboxLeaderWait); err != nil {
			if !backoff(err, "No cluster leader yet; file registrations wait") {
				return
			}
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
		err := manifest.SyncManifest(sctx)
		cancel()
		if err == nil {
			break
		}
		if !backoff(err, "Could not catch up with the cluster manifest; file registrations wait") {
			return
		}
	}
	delay, failures = outboxRetryMin, 0

	for ctx.Err() == nil {
		progressed, err := r.attempt(ctx, manifest, store)
		r.alarm()
		switch {
		case err != nil:
			if !backoff(err, "Failed to register files in the cluster manifest; retrying") {
				return
			}
		case progressed:
			delay, failures = outboxRetryMin, 0
		case r.Pending() > 0:
			// Only files whose storage check failed are left: retry later.
			if !sleep(delay) {
				return
			}
			if delay *= 2; delay > outboxRetryMax {
				delay = outboxRetryMax
			}
		default:
			select {
			case <-ctx.Done():
				return
			case <-r.wake:
			}
		}
	}
}

// attempt registers one batch. progressed is true when any file was settled.
func (r *RegistrationOutbox) attempt(ctx context.Context, manifest OutboxManifest, store OutboxStorage) (progressed bool, err error) {
	batch := r.nextBatch()
	if len(batch) == 0 {
		return false, nil
	}
	defer func() {
		r.mu.Lock()
		for _, e := range batch {
			e.inFlight = false
		}
		r.mu.Unlock()
	}()
	var settled, deferred []*outboxEntry
	// A file flushed by this process within outboxFreshWindow was just
	// written: it skips the storage checks, which on object storage are a
	// request each. Anything older, or loaded from the journal, may have been
	// deleted or rewritten while it waited.
	var candidates, toCheck, waiting []*outboxEntry
	r.mu.Lock()
	checked := make(map[*outboxEntry]bool, len(batch))
	for _, e := range batch {
		checked[e] = !e.fresh || time.Since(e.added) >= outboxFreshWindow
	}
	r.mu.Unlock()
	for _, e := range batch {
		if _, ok := manifest.GetFileEntry(e.rec.Path); ok {
			r.markInManifest(e)
			settled = append(settled, e) // already announced
			continue
		}
		if e.committed {
			waiting = append(waiting, e) // committed; waiting for this FSM
			continue
		}
		candidates = append(candidates, e)
		if checked[e] {
			toCheck = append(toCheck, e)
		}
	}
	// A committed file this FSM still does not show after a successful
	// barrier was committed and then removed (compaction or a DELETE got to
	// it first): it was registered, so it settles as such.
	if len(waiting) > 0 {
		bctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
		berr := manifest.SyncManifest(bctx)
		cancel()
		for _, e := range waiting {
			if _, ok := manifest.GetFileEntry(e.rec.Path); ok || berr == nil {
				r.markInManifest(e)
				settled = append(settled, e)
			} else {
				deferred = append(deferred, e)
			}
		}
	}
	sizes, statErrs := r.statAll(ctx, store, toCheck)

	var ops []raft.BatchFileOp
	var sent []*outboxEntry
	opEntry := map[int]*outboxEntry{} // register op index -> entry
	bytes := 0
	for _, e := range candidates {
		if bytes >= registrarDrainChunkBytes {
			break
		}
		if checked[e] {
			if statErrs[e] != nil {
				deferred = append(deferred, e)
				continue
			}
			if size := sizes[e]; size < 0 || size != e.rec.SizeBytes {
				r.skipped.Add(1)
				r.logger.Info().Str("path", e.rec.Path).Int64("size", size).Int64("recorded_size", e.rec.SizeBytes).
					Msg("Pending file is gone or was rewritten; not registered")
				settled = append(settled, e)
				continue
			}
		}
		// Coverage goes with every covered file: an FSM without stream
		// takeover ignores it, one with it needs it from the first file.
		rp := raft.RegisterFilePayload{File: r.fileEntry(e.rec), Cov2: fileCoverage(e.rec.Cov), Supersedes: e.rec.Supersedes}
		payload, err := json.Marshal(rp)
		if err != nil || raft.ValidateManifestPath(e.rec.Path) != nil {
			r.skipped.Add(1)
			r.logger.Warn().Str("path", e.rec.Path).Msg("Pending file registration has a path the cluster manifest refuses; dropped")
			settled = append(settled, e)
			continue
		}
		opEntry[len(ops)] = e
		ops = append(ops, raft.BatchFileOp{Type: raft.CommandRegisterFile, Payload: payload})
		if e.rec.Supersedes != "" {
			if del, err := json.Marshal(raft.DeleteFilePayload{Path: e.rec.Supersedes, Reason: "delete-rewrite"}); err == nil {
				ops = append(ops, raft.BatchFileOp{Type: raft.CommandDeleteFile, Payload: del})
			}
		}
		sent = append(sent, e)
		bytes += len(payload)
	}
	r.requeue(deferred)

	var applyErr error
	if len(ops) > 0 {
		actx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
		applyErr = manifest.BatchFileOpsInManifestContext(actx, ops)
		cancel()
		if applyErr != nil && raft.IsStreamClaimConflict(applyErr) {
			// Terminal for the named ops; the rest go again next round.
			refused := r.refuseClaimed(ctx, manifest, applyErr, opEntry)
			settled = append(settled, refused...)
			if len(refused) > 0 {
				applyErr = nil
			} else {
				r.logger.Warn().Err(applyErr).Msg("File registrations refused for a stream claim, but the refused files could not be identified; retrying")
			}
			r.mu.Lock()
			for _, e := range sent {
				e.fresh = false
			}
			r.mu.Unlock()
		} else if applyErr != nil {
			r.mu.Lock()
			for _, e := range sent {
				e.fresh = false
			}
			r.mu.Unlock()
		} else {
			r.registered.Add(int64(len(sent)))
			for _, e := range sent {
				r.markInManifest(e)
			}
			// A checked file may have been removed between its check and the
			// commit; an entry no peer can pull keeps a reader query gate red.
			// Detached from ctx, so a Stop here cannot skip it.
			var recheck []*outboxEntry
			for _, e := range sent {
				if checked[e] {
					recheck = append(recheck, e)
				}
			}
			after, errs := r.statAll(context.Background(), store, recheck)
			for _, e := range recheck {
				if errs[e] == nil && after[e] < 0 {
					r.mu.Lock()
					e.vanished = true
					r.mu.Unlock()
					dctx, cancel := context.WithTimeout(context.Background(), outboxAttemptTimeout)
					if err := manifest.DeleteFileFromManifest(dctx, e.rec.Path, raft.DeleteReasonVanished); err != nil {
						r.logger.Warn().Err(err).Str("path", e.rec.Path).
							Msg("File vanished while being registered and its manifest entry could not be removed")
					}
					cancel()
				}
			}
			// A covered file settles as registered only once this node's
			// FSM has applied the registration: the floors that settle
			// advances are published with this node's applied index, and a
			// standby trusts every file below it to be in its own FSM
			// (#1207). A forwarded commit returns before this node applies
			// it. Uncovered files keep settling at once.
			if anyCovered(sent) {
				bctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
				berr := manifest.SyncManifest(bctx)
				cancel()
				if berr != nil {
					r.mu.Lock()
					for _, e := range sent {
						e.committed = true
					}
					r.mu.Unlock()
					r.requeue(sent)
					sent = nil
				}
			}
			settled = append(settled, sent...)
		}
	}
	if len(settled) > 0 {
		r.ack(settled)
	}
	return len(settled) > 0, applyErr
}

func anyCovered(entries []*outboxEntry) bool {
	for _, e := range entries {
		if e.rec.Cov != nil {
			return true
		}
	}
	return false
}

func (r *RegistrationOutbox) markInManifest(e *outboxEntry) {
	r.mu.Lock()
	e.inManifest = true
	r.mu.Unlock()
}

// ReplaceOutcome says what ReplacePending found for the old path.
type ReplaceOutcome int

const (
	// ReplaceNotPending: the old path is not waiting here; this outbox knows
	// nothing about it.
	ReplaceNotPending ReplaceOutcome = iota
	// ReplaceSwapped: the old path was pending and never registered. It is
	// withdrawn and the new path is pending in its place, durably.
	ReplaceSwapped
	// ReplaceRegistered: the old path reached the manifest while the caller
	// waited. Replace it there; Entry carries its fields, so the caller does
	// not depend on this node's FSM having applied the commit yet.
	ReplaceRegistered
)

// ReplacePending is for a rewrite of a file that may still be pending, such as
// a row DELETE (#1210). The rewrite must be announced and the old file must
// not be, or readers get an entry for a file that is gone and never see the
// rows that survived. It waits out an attempt in flight on the old path, so it
// never races a commit.
func (r *RegistrationOutbox) ReplacePending(ctx context.Context, oldPath, newPath string, sizeBytes int64, sha256 string) (ReplaceOutcome, raft.FileEntry, error) {
	e, outcome, entry, err := r.settleOrLock(ctx, oldPath)
	if e == nil {
		return outcome, entry, err
	}
	// r.mu held, e pending and not in flight.
	rec := e.rec
	rec.Path, rec.SizeBytes, rec.SHA256 = newPath, sizeBytes, sha256
	rec.Supersedes = oldPath
	added := e.added // keep the backlog age honest
	// The rewrite carries the group's coverage (rec.Cov) and settles for it;
	// the old path does not settle at all.
	r.withdrawLocked(e)
	r.enqueueLocked(rec, added, true)
	r.appendLocked(rec)
	r.advanceHeadLocked()
	r.mu.Unlock()
	if err := r.SyncRegistrations(); err != nil {
		// Held in memory and registered in this process; only a crash first
		// loses it.
		r.logger.Error().Err(err).Str("path", newPath).Str("replaces", oldPath).
			Msg("Failed to persist a replaced file registration; a crash before it registers leaves the rewrite unannounced")
	}
	r.publishMetrics()
	select {
	case r.wake <- struct{}{}:
	default:
	}
	return ReplaceSwapped, raft.FileEntry{}, nil
}

// Withdraw is for a DELETE that removes a file outright (#1210): a pending
// file must not be registered after it is gone. Withdrawn reports a pending
// file taken out; otherwise outcome says whether the file was confirmed in the
// manifest, so the caller deletes it there.
func (r *RegistrationOutbox) Withdraw(ctx context.Context, path string) (withdrawn bool, outcome ReplaceOutcome, err error) {
	e, outcome, _, err := r.settleOrLock(ctx, path)
	if e == nil {
		return false, outcome, err
	}
	r.withdrawLocked(e)
	r.advanceHeadLocked()
	cov := e.rec.Cov
	r.mu.Unlock()
	if err := r.SyncRegistrations(); err != nil {
		r.logger.Warn().Err(err).Str("path", path).Msg("Failed to persist a withdrawn file registration")
	}
	// The rows are gone on purpose (a DELETE): they settle.
	cov.Settle(path, ingest.CoverageWithdrawn)
	r.publishMetrics()
	return true, ReplaceSwapped, nil
}

// settleOrLock waits out an attempt in flight on path, up to
// outboxReplaceWait. If path is pending it returns its entry with r.mu HELD.
// Otherwise it returns nil and what is known: confirmed recently (with the
// entry to use), or not known here.
func (r *RegistrationOutbox) settleOrLock(ctx context.Context, path string) (*outboxEntry, ReplaceOutcome, raft.FileEntry, error) {
	deadline := time.Now().Add(outboxReplaceWait)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	r.mu.Lock()
	for {
		e, ok := r.byPath[path]
		if !ok {
			rc, confirmed := r.recent[path]
			r.mu.Unlock()
			if confirmed && time.Since(rc.at) < outboxRecentWindow {
				return nil, ReplaceRegistered, r.fileEntry(rc.rec), nil
			}
			return nil, ReplaceNotPending, raft.FileEntry{}, nil
		}
		if !e.inFlight {
			if r.state == outboxAbandoned {
				r.mu.Unlock()
				return nil, ReplaceNotPending, raft.FileEntry{}, nil
			}
			return e, 0, raft.FileEntry{}, nil
		}
		r.mu.Unlock()
		if time.Now().After(deadline) {
			return nil, ReplaceNotPending, raft.FileEntry{}, errors.New("file registration still in flight; retry the request")
		}
		select {
		case <-ctx.Done():
			return nil, ReplaceNotPending, raft.FileEntry{}, ctx.Err()
		case <-tick.C:
		}
		r.mu.Lock()
	}
}

// withdrawLocked acknowledges a pending entry without registering it.
func (r *RegistrationOutbox) withdrawLocked(e *outboxEntry) {
	e.done = true
	r.live--
	delete(r.byPath, e.rec.Path)
	r.appendLocked(journalRecord{Version: journalRecordVersion, Op: journalOpAck, ClusterName: r.clusterName, Path: e.rec.Path})
	r.noteAckLocked(e)
}

// HoldsRegistration reports a file that is pending here or was confirmed a
// moment ago (this node's FSM may not show it yet). For cleanup that must not
// treat it as an orphan (#1212).
func (r *RegistrationOutbox) HoldsRegistration(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.byPath[path]; ok && !e.done {
		return true
	}
	rc, ok := r.recent[path]
	return ok && time.Since(rc.at) < outboxRecentWindow
}

// statAll checks entries in storage with bounded parallelism, each under its
// own outboxAttemptTimeout.
func (r *RegistrationOutbox) statAll(ctx context.Context, store OutboxStorage, entries []*outboxEntry) (map[*outboxEntry]int64, map[*outboxEntry]error) {
	sizes := make(map[*outboxEntry]int64, len(entries))
	errs := make(map[*outboxEntry]error, len(entries))
	if len(entries) == 0 {
		return sizes, errs
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, outboxStatParallelism)
	for _, e := range entries {
		wg.Add(1)
		sem <- struct{}{}
		go func(e *outboxEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			sctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
			size, err := store.StatFile(sctx, e.rec.Path)
			cancel()
			mu.Lock()
			sizes[e], errs[e] = size, err
			mu.Unlock()
		}(e)
	}
	wg.Wait()
	return sizes, errs
}

// nextBatch returns up to registrarDrainBatch pending entries from the head of
// the queue, without copying the rest.
func (r *RegistrationOutbox) nextBatch() []*outboxEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*outboxEntry
	i := r.head
	for ; i < len(r.queue) && len(out) < registrarDrainBatch; i++ {
		if e := r.queue[i]; e != nil && !e.done {
			e.inFlight = true
			out = append(out, e)
		}
	}
	r.batchEnd = i
	return out
}

// requeue moves entries whose storage check failed to the tail, so a file that
// keeps failing never holds back the ones behind it.
func (r *RegistrationOutbox) requeue(entries []*outboxEntry) {
	if len(entries) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	move := make(map[*outboxEntry]bool, len(entries))
	for _, e := range entries {
		e.fresh = false
		move[e] = true
	}
	end := r.batchEnd // the entries came from the last batch
	if end > len(r.queue) {
		end = len(r.queue)
	}
	for i := r.head; i < end; i++ {
		if move[r.queue[i]] {
			r.queue[i] = nil
		}
	}
	r.queue = append(r.queue, entries...)
	r.advanceHeadLocked()
}

// ack marks entries done and records it durably: an ack lost in a crash would
// let the next start re-register a file that compaction, retention or a delete
// has since taken out of the manifest on purpose. Compacts the journal when it
// has grown well past what is pending.
func (r *RegistrationOutbox) ack(entries []*outboxEntry) {
	type settlement struct {
		cov     *ingest.GroupCoverage
		path    string
		outcome ingest.CoverageOutcome
	}
	var settle []settlement
	r.mu.Lock()
	for _, e := range entries {
		if e.done {
			continue
		}
		if e.rec.Cov != nil {
			outcome := ingest.CoverageLost
			switch {
			case e.refused:
				outcome = ingest.CoverageRefused
			case e.inManifest && !e.vanished:
				outcome = ingest.CoverageRegistered
			}
			settle = append(settle, settlement{e.rec.Cov, e.rec.Path, outcome})
		}
		e.done = true
		r.live--
		if cur, ok := r.byPath[e.rec.Path]; ok && cur == e {
			delete(r.byPath, e.rec.Path)
		}
		if e.inManifest {
			slot := r.recentNext
			if old := r.recentRing[slot]; old != "" {
				if rc, ok := r.recent[old]; ok && rc.slot == slot {
					delete(r.recent, old) // only if a newer confirmation did not reuse it
				}
			}
			r.recentRing[slot] = e.rec.Path
			r.recent[e.rec.Path] = recentConfirm{rec: e.rec, at: time.Now(), slot: slot}
			r.recentNext = (slot + 1) % len(r.recentRing)
		}
		r.appendLocked(journalRecord{
			Version: journalRecordVersion, Op: journalOpAck,
			ClusterName: r.clusterName, Path: e.rec.Path,
		})
		r.noteAckLocked(e)
	}
	r.advanceHeadLocked()
	r.mu.Unlock()
	if err := r.SyncRegistrations(); err != nil {
		r.logger.Warn().Err(err).Msg("Failed to persist file registration acknowledgements")
	}
	// After the ack is durable, and outside r.mu: a group that finishes
	// releases its WAL entries, and nothing below may call back in here.
	for _, st := range settle {
		st.cov.Settle(st.path, st.outcome)
	}
	r.compact()
	r.publishMetrics()
}

func (r *RegistrationOutbox) advanceHeadLocked() {
	for r.head < len(r.queue) && (r.queue[r.head] == nil || r.queue[r.head].done) {
		r.queue[r.head] = nil
		r.head++
	}
	if r.head > 1024 && r.head*2 > len(r.queue) {
		r.queue = append([]*outboxEntry(nil), r.queue[r.head:]...)
		r.head = 0
	}
}

// compact empties the journal in place when nothing is pending, or rewrites it
// when acks dominate. Truncating the live fd keeps the append handle valid.
func (r *RegistrationOutbox) compact() {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.journal == nil || r.broken {
		return
	}
	switch {
	case r.live == 0 && r.fileRecords > 0:
		// No fsync: the acks are durable already, a truncate lost in a crash
		// leaves a fully acknowledged journal, and the next sync covers it.
		if err := r.journal.Truncate(0); err != nil {
			r.markBrokenLocked(err)
			return
		}
		r.fileRecords = 0
	case r.fileRecords > 2*r.live+outboxCompactSlack && r.live <= outboxCompactSlack:
		// Only a small pending set is rewritten: during a catch-up the journal
		// empties on its own within minutes, and rewriting a large backlog
		// under mu would stall every flush.
		if err := r.rewriteLocked(); err != nil {
			r.logger.Warn().Err(err).Msg("Failed to compact the file registration journal")
		}
	}
}

// publishMetrics exports the backlog: how many files wait and since when.
func (r *RegistrationOutbox) publishMetrics() {
	r.mu.Lock()
	live := r.live
	var oldest time.Time
	for i := r.head; i < len(r.queue); i++ {
		if e := r.queue[i]; e != nil && !e.done {
			oldest = e.added
			break
		}
	}
	r.mu.Unlock()
	m := metrics.Get()
	m.SetFileRegistrationsPending(int64(live))
	if oldest.IsZero() {
		m.SetFileRegistrationOldestPending(0)
	} else {
		m.SetFileRegistrationOldestPending(oldest.UnixNano())
	}
	m.SetFileRegistrationsConfirmed(r.registered.Load())
}

// alarm logs at Error, at most every outboxAlarmEvery, while the oldest
// pending file has waited longer than outboxAlarmAge: only this writer can
// query such files until they are registered.
func (r *RegistrationOutbox) alarm() {
	r.mu.Lock()
	live := r.live
	var oldest time.Time
	for i := r.head; i < len(r.queue); i++ {
		if e := r.queue[i]; e != nil && !e.done {
			oldest = e.added
			break
		}
	}
	r.mu.Unlock()
	if live == 0 || time.Since(oldest) < outboxAlarmAge {
		return
	}
	now := time.Now().UnixNano()
	last := r.lastAlarm.Load()
	if last != 0 && time.Duration(now-last) < outboxAlarmEvery {
		return
	}
	if r.lastAlarm.CompareAndSwap(last, now) {
		r.logger.Error().Int("pending", live).Dur("oldest", time.Since(oldest)).
			Msg("File registrations are backing up; until they reach the cluster manifest only this writer can query those files")
	}
}

func (r *RegistrationOutbox) fileEntry(rec journalRecord) raft.FileEntry {
	return raft.FileEntry{
		Path:          rec.Path,
		SHA256:        rec.SHA256,
		SizeBytes:     rec.SizeBytes,
		Database:      rec.Database,
		Measurement:   rec.Measurement,
		PartitionTime: rec.PartitionTime,
		OriginNodeID:  r.nodeID,
		Tier:          "hot",
		CreatedAt:     time.Now().UTC(),
	}
}

// refuseClaimed marks the entries of the ops a claim conflict names as
// refused and returns them for settling. A refused rewrite's paired delete is
// still applied on its own: a DELETE whose earlier apply committed must not
// leave the original file registered.
func (r *RegistrationOutbox) refuseClaimed(ctx context.Context, manifest OutboxManifest, err error, opEntry map[int]*outboxEntry) []*outboxEntry {
	var out []*outboxEntry
	for _, op := range raft.StreamClaimConflictOps(err) {
		e := opEntry[op.Op]
		if e == nil {
			continue
		}
		if e.rec.Supersedes != "" {
			// Settle only once the paired delete commits; otherwise retry the
			// entry next round (it is refused again, and the delete retried).
			dctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
			derr := manifest.DeleteFileFromManifest(dctx, e.rec.Supersedes, "delete-rewrite")
			cancel()
			if derr != nil {
				r.logger.Warn().Err(derr).Str("path", e.rec.Supersedes).Msg("Could not remove the file a refused rewrite replaced; retrying")
				continue
			}
		}
		r.mu.Lock()
		e.refused = true
		r.mu.Unlock()
		out = append(out, e)
		r.logger.Warn().Str("path", e.rec.Path).Str("kind", op.Kind).
			Msg("File registration refused by the stream takeover rules; the file is kept and not registered")
	}
	return out
}

// fileCoverage is a file's exact coverage as the cluster manifest records
// it, per writer instance (#1207 S4b). Over maxFileCoverageBytes it is sent
// truncated: the manifest then refuses nothing on its account, and the
// instance stops being tracked for takeover.
func fileCoverage(cov *ingest.GroupCoverage) []raft.InstanceCoverage {
	if cov == nil {
		return nil
	}
	ranges, partial, exact := cov.FilePieces()
	by := map[uint64]*raft.InstanceCoverage{}
	get := func(inst uint64) *raft.InstanceCoverage {
		if c := by[inst]; c != nil {
			return c
		}
		c := &raft.InstanceCoverage{Instance: inst, Key: cov.Key, Truncated: !exact}
		by[inst] = c
		return c
	}
	for _, rg := range ranges {
		c := get(rg.Instance)
		c.Ranges = append(c.Ranges, [2]uint64{rg.Lo, rg.Hi})
	}
	for _, p := range partial {
		c := get(p.Instance)
		c.Partial = append(c.Partial, raft.CoveragePiece{CSeq: p.CSeq, Chunk: p.Chunk, Chunks: p.Chunks, RowLo: p.RowLo, RowHi: p.RowHi, RowTotal: p.RowTotal})
	}
	if !exact {
		// The journal truncated it: name the instances, list nothing.
		for _, b := range cov.Bounds {
			get(b.Instance)
		}
	}
	out := make([]raft.InstanceCoverage, 0, len(by))
	for _, c := range by {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Instance < out[j].Instance })
	if raw, err := json.Marshal(out); err == nil && len(raw) > maxFileCoverageBytes {
		for i := range out {
			out[i].Ranges, out[i].Partial, out[i].Truncated = nil, nil, true
		}
	}
	return out
}

// maxFileCoverageBytes bounds one registration's coverage, so a batch of
// registrations stays within the 1 MiB forward frame.
const maxFileCoverageBytes = 64 << 10
