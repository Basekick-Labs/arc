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
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// EarlyFileRegistrarJournal is the journal file name inside the Raft data
// directory.
const EarlyFileRegistrarJournal = "pending-registrations.jsonl"

// FileRegistrarTarget is what EarlyFileRegistrar hands files to once the
// cluster is up: the live registrar for everything flushed from then on.
type FileRegistrarTarget interface {
	RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string)
}

// EarlyManifest is the slice of the coordinator the pending drain needs, so a
// test can drive it without Raft.
type EarlyManifest interface {
	WaitForLeader(timeout time.Duration) error
	SyncManifest(ctx context.Context) error
	GetFileEntry(path string) (*raft.FileEntry, bool)
	BatchFileOpsInManifestContext(ctx context.Context, ops []raft.BatchFileOp) error
	DeleteFileFromManifest(ctx context.Context, path, reason string) error
}

// EarlyStorage reports a file's size, -1 when it does not exist.
type EarlyStorage interface {
	StatFile(ctx context.Context, path string) (int64, error)
}

// EarlyFileRegistrar registers the files a node flushes before the cluster
// coordinator is up (#1206).
//
// The ArrowBuffer flushes long before the live CoordinatorFileRegistrar
// exists: startup WAL recovery flushes every replayed row and then deletes the
// WAL files, MQTT ingest starts, and the coordinator start can wait on a
// leader. A file flushed in that window used to reach no manifest at all, so
// only the node that wrote it could query it.
//
// Until HandOff, RegisterFile appends to a journal in the Raft data directory
// and fsyncs it before returning. The recovery barrier calls it inside the
// flush it waits on, so the journal entry is durable before recovery deletes
// the WAL file the rows came from. A signal during startup kills the process
// without running shutdown hooks (they are installed later), so the journal,
// not an in-memory list, is what carries these files to the next start.
//
// HandOff switches RegisterFile to the live registrar and starts a drain that
// registers the pending files. The drain waits for a leader, retries every
// failure, re-checks each file on disk right before each attempt, and removes
// the manifest entry again if the file vanished while the batch was in flight.
// It rewrites the journal after each confirmed batch, so what it has not
// confirmed is still there for the next start.
//
// Built only on a node that can ingest: on a reader or a compactor the WAL
// holds nothing but entries received from a writer before #888, and
// registering their flushes would spread duplicate rows to every node.
type EarlyFileRegistrar struct {
	journalPath string
	nodeID      string
	clusterName string
	logger      zerolog.Logger

	// handedOff is the lock-free fast path: after the hand-off every flush
	// goes straight to live without touching mu, which the drain holds across
	// journal rewrites. Set under mu, after live, so a reader that sees it
	// true also sees live.
	handedOff atomic.Bool

	mu       sync.Mutex
	state    earlyState
	pending  []earlyRecord
	journal  *os.File
	live     FileRegistrarTarget
	syncWarn bool // a journal write failed: stop appending, keep in memory

	cancel context.CancelFunc
	done   chan struct{}

	registered atomic.Int64
	skipped    atomic.Int64
	dropped    atomic.Int64
}

type earlyState int

const (
	earlyCollecting earlyState = iota
	earlyHandedOff
	earlyAbandoned
)

// earlyRecord is one journal line. ClusterName pins it to the cluster: a data
// directory moved into another cluster must not announce its files there. The
// node ID is not recorded. Without cluster.node_id it is hostname-PID, which
// changes with every start on a VM, and the file is on this node's disk either
// way, so the origin is this process's node ID at hand-off.
type earlyRecord struct {
	ClusterName   string    `json:"cluster_name"`
	Database      string    `json:"database"`
	Measurement   string    `json:"measurement"`
	Path          string    `json:"path"`
	PartitionTime time.Time `json:"partition_time"`
	SizeBytes     int64     `json:"size_bytes"`
	SHA256        string    `json:"sha256"`
}

// Retry pacing for the pending drain. Nothing else will register these files,
// so the drain does not give up; it backs off instead.
var (
	earlyRetryMin       = 200 * time.Millisecond
	earlyRetryMax       = 5 * time.Second
	earlyLeaderWait     = time.Second // not cancellable; kept under earlyStopBudget
	earlyStopBudget     = registrarDrainTimeout
	earlyAttemptTimeout = forwardApplyTimeout
)

// NewEarlyFileRegistrar opens the journal in dir and loads what a previous
// process left in it. Records stamped with another cluster are dropped with a
// warning.
func NewEarlyFileRegistrar(dir, clusterName string, logger zerolog.Logger) (*EarlyFileRegistrar, error) {
	// 0700, as Raft creates it: on a fresh node this runs first, and MkdirAll
	// does not tighten an existing directory, so a wider mode here would expose
	// the Raft snapshots written into it later.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	r := &EarlyFileRegistrar{
		journalPath: filepath.Join(dir, EarlyFileRegistrarJournal),
		clusterName: clusterName,
		logger:      logger.With().Str("component", "early-file-registrar").Logger(),
	}
	loaded, foreign, err := readEarlyJournal(r.journalPath, clusterName)
	if err != nil {
		return nil, err
	}
	if foreign > 0 {
		r.logger.Warn().Int("records", foreign).Str("journal", r.journalPath).
			Msg("Pending file registrations written for another cluster were dropped from the journal")
	}
	r.pending = loaded
	// Rewrite rather than append: it drops foreign records and a torn last
	// line from a crash mid-write, and leaves a journal of exactly r.pending.
	if err := r.rewriteJournalLocked(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(r.journalPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	if err := syncDir(dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	r.journal = f
	if len(loaded) > 0 {
		r.logger.Info().Int("files", len(loaded)).
			Msg("Loaded file registrations a previous start did not confirm; they are registered once the cluster is up")
	}
	return r, nil
}

// readEarlyJournal returns the records for this cluster, and how many belonged
// to another. A line that does not parse is skipped rather than ending the
// read, so a torn line never takes the records after it along.
func readEarlyJournal(path, clusterName string) ([]earlyRecord, int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open journal: %w", err)
	}
	defer f.Close()
	var out []earlyRecord
	foreign := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var rec earlyRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue
		}
		if rec.ClusterName != clusterName {
			foreign++
			continue
		}
		out = append(out, rec)
	}
	return out, foreign, nil
}

// RegisterFile implements ingest.FileRegistrar.
func (r *EarlyFileRegistrar) RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string) {
	if r.handedOff.Load() {
		r.live.RegisterFile(database, measurement, path, partitionTime, sizeBytes, sha256)
		return
	}
	r.mu.Lock()
	switch r.state {
	case earlyHandedOff:
		live := r.live
		r.mu.Unlock()
		live.RegisterFile(database, measurement, path, partitionTime, sizeBytes, sha256)
		return
	case earlyAbandoned:
		r.mu.Unlock()
		if n := r.dropped.Add(1); n&(n-1) == 0 {
			r.logger.Warn().Str("path", path).Int64("total_dropped", n).
				Msg("Cluster manifest unavailable in this start; file not announced to it")
		}
		return
	}
	defer r.mu.Unlock()
	rec := earlyRecord{
		ClusterName: r.clusterName,
		Database:    database, Measurement: measurement, Path: path,
		PartitionTime: partitionTime, SizeBytes: sizeBytes, SHA256: sha256,
	}
	r.pending = append(r.pending, rec)
	if r.syncWarn {
		return
	}
	if err := r.appendJournalLocked(rec); err != nil {
		// Kept in memory, so a normal start still registers it; only a crash
		// before the hand-off loses it. No further appends: the journal is
		// not trusted to take them.
		r.syncWarn = true
		r.logger.Error().Err(err).Str("journal", r.journalPath).
			Msg("Failed to persist a pending file registration; this and later ones are held in memory only until the cluster is up")
	}
}

func (r *EarlyFileRegistrar) appendJournalLocked(rec earlyRecord) error {
	if r.journal == nil {
		return errors.New("journal is closed")
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	off, err := r.journal.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := r.journal.Write(append(line, '\n')); err != nil {
		_ = r.journal.Truncate(off) // do not leave a torn line behind
		return err
	}
	return r.journal.Sync()
}

// rewriteJournalLocked replaces the journal with r.pending: temp file, fsync,
// rename, fsync the directory. An empty list removes the journal.
func (r *EarlyFileRegistrar) rewriteJournalLocked() error {
	dir := filepath.Dir(r.journalPath)
	if len(r.pending) == 0 {
		if err := os.Remove(r.journalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove journal: %w", err)
		}
		return syncDir(dir)
	}
	tmp, err := os.CreateTemp(dir, EarlyFileRegistrarJournal+".tmp-*")
	if err != nil {
		return fmt.Errorf("create journal temp: %w", err)
	}
	defer os.Remove(tmp.Name())
	w := bufio.NewWriter(tmp)
	for _, rec := range r.pending {
		line, err := json.Marshal(rec)
		if err != nil {
			_ = tmp.Close()
			return err
		}
		_, _ = w.Write(append(line, '\n'))
	}
	if err := w.Flush(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write journal temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync journal temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), r.journalPath); err != nil {
		return fmt.Errorf("replace journal: %w", err)
	}
	return syncDir(dir)
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
func (r *EarlyFileRegistrar) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// Abandon is for a start whose cluster coordinator never came up. From here
// RegisterFile drops instead of journaling, so a node running without its
// cluster does not fsync on every flush and grow the journal without bound.
// The journal keeps what was already pending for the next start.
func (r *EarlyFileRegistrar) Abandon() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != earlyCollecting {
		return
	}
	r.state = earlyAbandoned
	if r.journal != nil {
		_ = r.journal.Close()
		r.journal = nil
	}
	r.logger.Warn().Int("pending", len(r.pending)).Str("journal", r.journalPath).
		Msg("Cluster manifest unavailable in this start; pending file registrations stay in the journal for the next start")
}

// HandOff routes every later RegisterFile to live and starts registering the
// pending files through manifest under nodeID, this node's cluster ID. Stop
// ends the drain.
func (r *EarlyFileRegistrar) HandOff(manifest EarlyManifest, store EarlyStorage, live FileRegistrarTarget, nodeID string) {
	r.mu.Lock()
	if r.state != earlyCollecting {
		r.mu.Unlock()
		return
	}
	r.state = earlyHandedOff
	r.nodeID = nodeID
	r.live = live
	r.handedOff.Store(true)
	if r.journal != nil {
		_ = r.journal.Close()
		r.journal = nil
	}
	n := len(r.pending)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	r.mu.Unlock()

	if n == 0 {
		close(r.done)
		r.mu.Lock()
		if err := r.rewriteJournalLocked(); err != nil {
			r.logger.Warn().Err(err).Msg("Failed to remove the empty pending-registration journal")
		}
		r.mu.Unlock()
		return
	}
	r.logger.Info().Int("files", n).Msg("Registering files flushed before the cluster was up")
	go r.drain(ctx, manifest, store)
}

// Stop ends the drain, waiting at most earlyStopBudget so an unreachable
// leader does not hold the shutdown. Unconfirmed files stay in the journal.
func (r *EarlyFileRegistrar) Stop() {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	if r.state == earlyCollecting {
		// Never handed off and the process is stopping: keep the journal as is.
		if r.journal != nil {
			_ = r.journal.Close()
			r.journal = nil
		}
		r.state = earlyAbandoned
	}
	r.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	timer := time.NewTimer(earlyStopBudget)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		r.logger.Warn().Msg("Pending file registration drain did not stop in time; unconfirmed files stay in the journal")
	}
}

// Done is closed when the drain has nothing left or was stopped.
func (r *EarlyFileRegistrar) Done() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done
}

func (r *EarlyFileRegistrar) drain(ctx context.Context, manifest EarlyManifest, store EarlyStorage) {
	defer close(r.done)
	delay := earlyRetryMin
	failures := 0
	wait := func() bool {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
		if delay *= 2; delay > earlyRetryMax {
			delay = earlyRetryMax
		}
		return true
	}
	fail := func(err error, msg string) {
		failures++
		if failures&(failures-1) == 0 {
			r.logger.Warn().Err(err).Int("attempts", failures).Int("pending", r.Pending()).Msg(msg)
		}
	}

	// The "already in the manifest" check reads this node's FSM, which can
	// trail the leader right after a restart. Catch up once first, so a file a
	// previous start registered is not registered again.
	synced := false
	for {
		if ctx.Err() != nil {
			return
		}
		if !synced {
			if err := manifest.WaitForLeader(earlyLeaderWait); err != nil {
				fail(err, "No cluster leader yet; pending file registrations wait")
				if !wait() {
					return
				}
				continue
			}
			sctx, cancel := context.WithTimeout(ctx, earlyAttemptTimeout)
			err := manifest.SyncManifest(sctx)
			cancel()
			if err != nil {
				fail(err, "Could not catch up with the cluster manifest; pending file registrations wait")
				if !wait() {
					return
				}
				continue
			}
			synced = true
		}
		r.mu.Lock()
		batch := append([]earlyRecord(nil), r.pending...)
		r.mu.Unlock()
		if len(batch) == 0 {
			r.logger.Info().
				Int64("registered", r.registered.Load()).
				Int64("skipped", r.skipped.Load()).
				Msg("Files flushed before the cluster was up are registered")
			return
		}
		if err := manifest.WaitForLeader(earlyLeaderWait); err != nil {
			fail(err, "No cluster leader yet; pending file registrations wait")
			if !wait() {
				return
			}
			continue
		}

		// Decide each file right before the attempt: compaction, retention or
		// a DELETE may have removed or rewritten it since it was flushed.
		var ops []raft.BatchFileOp
		var sent, settled []earlyRecord
		bytes := 0
		var statErr error
		for _, rec := range batch {
			if len(ops) >= registrarDrainBatch || bytes >= registrarDrainChunkBytes {
				break
			}
			if _, ok := manifest.GetFileEntry(rec.Path); ok {
				settled = append(settled, rec) // already announced
				continue
			}
			size, err := store.StatFile(ctx, rec.Path)
			if err != nil {
				// Left pending and retried next round; the files behind it
				// still go now, so one unreadable path cannot block the rest.
				statErr = err
				continue
			}
			if size < 0 || size != rec.SizeBytes {
				r.skipped.Add(1)
				r.logger.Info().Str("path", rec.Path).Int64("size", size).Int64("recorded_size", rec.SizeBytes).
					Msg("File flushed before the cluster was up is gone or was rewritten; not registered")
				settled = append(settled, rec)
				continue
			}
			payload, err := json.Marshal(raft.RegisterFilePayload{File: r.fileEntry(rec)})
			if err != nil || raft.ValidateManifestPath(rec.Path) != nil {
				r.skipped.Add(1)
				r.logger.Warn().Str("path", rec.Path).Msg("Pending file registration has a path the cluster manifest refuses; dropped")
				settled = append(settled, rec)
				continue
			}
			ops = append(ops, raft.BatchFileOp{Type: raft.CommandRegisterFile, Payload: payload})
			sent = append(sent, rec)
			bytes += len(payload)
		}

		if len(ops) > 0 {
			actx, cancel := context.WithTimeout(ctx, earlyAttemptTimeout)
			err := manifest.BatchFileOpsInManifestContext(actx, ops)
			cancel()
			if err != nil {
				fail(err, "Failed to register files flushed before the cluster was up; retrying")
				if !wait() {
					return
				}
				continue
			}
			r.registered.Add(int64(len(sent)))
			// A file removed between the stat and the commit would leave an
			// entry no peer can pull, which keeps a reader query gate red.
			// Detached from ctx: a Stop between the commit and this check must
			// not leave an entry for a file that is gone with nothing tracking it.
			for _, rec := range sent {
				sctx, scancel := context.WithTimeout(context.Background(), earlyAttemptTimeout)
				size, err := store.StatFile(sctx, rec.Path)
				scancel()
				if err == nil && size < 0 {
					dctx, cancel := context.WithTimeout(context.Background(), earlyAttemptTimeout)
					if err := manifest.DeleteFileFromManifest(dctx, rec.Path, "vanished-before-registration"); err != nil {
						r.logger.Warn().Err(err).Str("path", rec.Path).
							Msg("File vanished while being registered and its manifest entry could not be removed")
					}
					cancel()
				}
			}
			settled = append(settled, sent...)
		}
		if len(settled) > 0 {
			r.settle(settled)
			delay, failures = earlyRetryMin, 0
		}
		if statErr != nil {
			fail(statErr, "Could not check a pending file on disk; retrying")
			if !wait() {
				return
			}
		}
	}
}

// settle removes done records from the pending list and the journal.
func (r *EarlyFileRegistrar) settle(done []earlyRecord) {
	gone := make(map[string]int, len(done))
	for _, rec := range done {
		gone[rec.Path]++
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.pending[:0]
	for _, rec := range r.pending {
		if gone[rec.Path] > 0 {
			gone[rec.Path]--
			continue
		}
		kept = append(kept, rec)
	}
	r.pending = kept
	if err := r.rewriteJournalLocked(); err != nil {
		// The next start re-registers these; the drain skips any already in
		// the manifest.
		r.logger.Warn().Err(err).Msg("Failed to rewrite the pending-registration journal")
	}
}

func (r *EarlyFileRegistrar) fileEntry(rec earlyRecord) raft.FileEntry {
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
