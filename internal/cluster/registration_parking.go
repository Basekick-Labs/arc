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
	"syscall"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
)

// Parked registrations (#1207 S4b).
//
// A registration the cluster manifest refuses for good (stream takeover: its
// batches were claimed by another writer, or are already registered) is not
// deleted: some of its rows may exist nowhere else. It is PARKED:
//
//  1. a record is appended and fsynced to the refused journal;
//  2. the file moves out of every query path, to <storage>/_refused/<path>,
//     and its local tiering row is removed;
//  3. it is acked in the pending journal.
//
// A crash between the steps is completed on the next start: the refused
// record wins over an un-acked add of the same path, and a file still at its
// original path is moved. A parked file keeps its WAL entries held, except
// batches whose rows are all registered elsewhere, and is deleted once every
// one of its rows is in a registered file this node holds a copy of. Rows
// nobody else published stay parked: a later release re-flushes them from
// the WAL.

// RefusedRegistrationJournal is the refused journal, next to the pending one.
const RefusedRegistrationJournal = "refused-registrations.jsonl"

// refusedDir holds parked files under the storage root. Every reserved root
// directory ("_" prefix) is skipped by queries, compaction, the reconciler,
// tiering and Iceberg.
const refusedDir = "_refused"

const (
	parkOpPark      = "park"
	parkOpResolved  = "resolved"
	parkResolveTick = 30 * time.Second
	parkAlarmEvery  = 10 * time.Minute
)

// parkedRecord is one line of the refused journal.
type parkedRecord struct {
	Op       string        `json:"op"`
	Rec      journalRecord `json:"rec"`
	Kind     string        `json:"kind,omitempty"`
	Instance uint64        `json:"instance,omitempty"`
	Epoch    uint64        `json:"epoch,omitempty"`
	// Held is the parked file's path relative to the storage root; empty
	// when the file could not be moved (it is left where it was).
	Held string    `json:"held,omitempty"`
	At   time.Time `json:"at"`
}

type parkedEntry struct {
	rec    parkedRecord
	moved  bool // the file is at Held (not at its original path)
	parked time.Time
}

// TakeoverReader is what parking needs from the cluster manifest, beyond
// OutboxManifest. The coordinator implements it.
type TakeoverReader interface {
	CoverageCovered(inst uint64, key string, p raft.CoveragePiece, hour time.Time) bool
	UncoveredBatches(inst uint64, key string, lo, hi uint64, hour time.Time) raft.Intervals
	CoveringFilesOf(database, measurement string, hour time.Time, inst uint64, key string, set raft.Intervals) []raft.FileEntry
	TakeoverGeneration() uint64
}

// LocalParking is where parked files go. Base is the local storage root;
// DropTiering removes a file's local tiering row (nil when tiering is off).
type LocalParking struct {
	Base        string
	DropTiering func(ctx context.Context, path string) error
}

// SetLocalParking enables moving parked files out of the query path, and
// finishes moves a crash interrupted. Without it (shared storage) a parked
// file is left where it is and still answers IsPending.
func (r *RegistrationOutbox) SetLocalParking(p LocalParking) {
	r.mu.Lock()
	r.parking = &p
	var unfinished []*parkedEntry
	for _, e := range r.parked {
		if !e.moved {
			unfinished = append(unfinished, e)
		}
	}
	r.mu.Unlock()
	for _, e := range unfinished {
		if err := r.moveParked(e); err != nil {
			r.logger.Error().Err(err).Str("path", e.rec.Rec.Path).Msg("Could not finish parking a refused file; it stays where it is and is still treated as pending")
		}
	}
	r.logOrphanedParkedFiles()
	metrics.Get().SetWALParked(r.parkedStats())
}

// loadRefused reads the refused journal: the latest record per path, minus
// the resolved ones. A torn or unparsable line is skipped.
func loadRefused(path, clusterName string) (map[string]*parkedEntry, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]*parkedEntry{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open refused registration journal: %w", err)
	}
	defer f.Close()
	out := map[string]*parkedEntry{}
	rd := bufio.NewReaderSize(f, 1<<20)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 && len(line) <= maxJournalLine {
			var pr parkedRecord
			if json.Unmarshal(line, &pr) == nil && pr.Rec.Path != "" && pr.Rec.ClusterName == clusterName {
				switch pr.Op {
				case parkOpPark:
					out[pr.Rec.Path] = &parkedEntry{rec: pr, parked: pr.At}
				case parkOpResolved:
					delete(out, pr.Rec.Path)
				}
			}
		}
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read refused registration journal: %w", err)
		}
	}
}

// appendRefused appends one record and fsyncs it (and the directory when the
// journal was created).
func (r *RegistrationOutbox) appendRefused(pr parkedRecord) error {
	raw, err := json.Marshal(pr)
	if err != nil {
		return err
	}
	_, statErr := os.Stat(r.refusedPath)
	created := errors.Is(statErr, os.ErrNotExist)
	f, err := os.OpenFile(r.refusedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := fsyncFile(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if created {
		syncDir(filepath.Dir(r.refusedPath))
	}
	return nil
}

// compactRefused rewrites the refused journal with the parked entries only.
// Caller holds parkMu.
func (r *RegistrationOutbox) compactRefused() error {
	r.mu.Lock()
	recs := make([]parkedRecord, 0, len(r.parked))
	for _, e := range r.parked {
		recs = append(recs, e.rec)
	}
	r.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].Rec.Path < recs[j].Rec.Path })
	tmp := r.refusedPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, pr := range recs {
		raw, err := json.Marshal(pr)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(raw)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := fsyncFile(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.refusedPath); err != nil {
		return err
	}
	syncDir(filepath.Dir(r.refusedPath))
	return nil
}

// park parks e, refused as op says. It runs inside an attempt, while e is in
// flight, so a DELETE waits for it. On error nothing is acked: e stays
// pending and is refused again next round.
func (r *RegistrationOutbox) park(e *outboxEntry, op raft.RefusedOp) error {
	r.parkMu.Lock()
	defer r.parkMu.Unlock()
	pr := parkedRecord{Op: parkOpPark, Rec: e.rec, Kind: op.Kind, Instance: op.Instance, Epoch: op.Epoch, At: time.Now().UTC()}
	r.mu.Lock()
	local := r.parking != nil
	r.mu.Unlock()
	if local {
		pr.Held = filepath.ToSlash(filepath.Join(refusedDir, e.rec.Path))
	}
	if err := r.appendRefused(pr); err != nil {
		return fmt.Errorf("write refused registration journal: %w", err)
	}
	pe := &parkedEntry{rec: pr, parked: pr.At}
	r.mu.Lock()
	r.parked[e.rec.Path] = pe
	r.holdGen++
	r.mu.Unlock()
	if local {
		if err := r.moveParked(pe); err != nil {
			// Recorded: the next start finishes the move. Until then the file
			// stays at its path and answers IsPending.
			r.logger.Error().Err(err).Str("path", e.rec.Path).Msg("Could not move a refused file out of the query path; it is still queried on this node")
		}
	}
	metrics.Get().SetWALParked(r.parkedStats())
	r.logger.Warn().Str("path", e.rec.Path).Str("kind", op.Kind).Str("held", pr.Held).
		Msg("File registration refused by the stream takeover rules; the file is parked out of the query path and kept until its rows are registered elsewhere")
	return nil
}

// moveParked moves a parked file to its held path, completing a move a crash
// interrupted, and drops its local tiering row.
func (r *RegistrationOutbox) moveParked(e *parkedEntry) error {
	r.mu.Lock()
	var p *LocalParking
	if r.parking != nil {
		cp := *r.parking
		p = &cp
	}
	r.mu.Unlock()
	if p == nil || e.rec.Held == "" {
		return nil
	}
	src := filepath.Join(p.Base, filepath.FromSlash(e.rec.Rec.Path))
	dst := filepath.Join(p.Base, filepath.FromSlash(e.rec.Held))
	_, srcErr := os.Stat(src)
	dstInfo, dstErr := os.Stat(dst)
	switch {
	case srcErr == nil && dstErr == nil:
		// Both: a copy was interrupted after writing the held copy. Keep the
		// held one if it is complete.
		if dstInfo.Size() == e.rec.Rec.SizeBytes {
			if err := os.Remove(src); err != nil {
				return err
			}
			syncDir(filepath.Dir(src))
			break
		}
		if err := os.Remove(dst); err != nil {
			return err
		}
		fallthrough
	case srcErr == nil:
		if err := mkdirAllSynced(filepath.Dir(dst), p.Base); err != nil {
			return err
		}
		if err := moveFile(src, dst); err != nil {
			return err
		}
		syncDir(filepath.Dir(src))
		syncDir(filepath.Dir(dst))
	case dstErr == nil:
		// Already moved.
	default:
		// Removed after its rows were found registered elsewhere, before the
		// resolved record was written: drop the record.
		r.logger.Error().Str("path", e.rec.Rec.Path).Str("held", e.rec.Held).
			Msg("A parked file is at neither its path nor its parked path; its record is dropped")
		r.mu.Lock()
		delete(r.parked, e.rec.Rec.Path)
		r.holdGen++
		r.mu.Unlock()
		return nil
	}
	if p.DropTiering != nil {
		if err := p.DropTiering(context.Background(), e.rec.Rec.Path); err != nil {
			r.logger.Warn().Err(err).Str("path", e.rec.Rec.Path).Msg("Could not remove the tiering row of a parked file")
		}
	}
	r.mu.Lock()
	e.moved = true
	r.mu.Unlock()
	return nil
}

// moveFile renames, or copies, syncs and removes across filesystems.
func moveFile(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil || !errors.Is(err, syscall.EXDEV) {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := fsyncFile(out); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	syncDir(filepath.Dir(dst))
	return os.Remove(src)
}

// mkdirAllSynced creates dir and fsyncs every directory it created, and the
// parent of the first, up to stop.
func mkdirAllSynced(dir, stop string) error {
	var created []string
	for d := dir; d != stop && d != filepath.Dir(d); d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		}
		created = append(created, d)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, d := range created {
		syncDir(filepath.Dir(d))
	}
	return nil
}

// logOrphanedParkedFiles reports files under _refused with no record (the
// refused journal was lost, e.g. a fresh Raft data directory): they are not
// managed, and an operator must account for their rows.
func (r *RegistrationOutbox) logOrphanedParkedFiles() {
	r.mu.Lock()
	p := r.parking
	known := map[string]bool{}
	for _, e := range r.parked {
		known[e.rec.Held] = true
	}
	r.mu.Unlock()
	if p == nil {
		return
	}
	root := filepath.Join(p.Base, refusedDir)
	orphans := 0
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(p.Base, path)
		if rerr == nil && !known[filepath.ToSlash(rel)] {
			orphans++
		}
		return nil
	})
	if orphans > 0 {
		r.logger.Error().Int("files", orphans).Str("dir", root).
			Msg("Parked files with no refused-registration record: their rows are in no registered file this node knows of; account for them before removing them")
	}
}

// parkedHoldSpans returns the coverage a parked file still holds in the WAL:
// every batch, except those whose rows the cluster manifest shows covered.
// A claim alone never releases a batch: a claim can be released, or finish
// without it. A truncated coverage holds everything it names.
func parkedHoldSpans(rec journalRecord, tk TakeoverReader) []ingest.CoverageRange {
	if rec.Cov == nil {
		return nil
	}
	spans, _ := rec.Cov.HeldSpans()
	ranges, partial, exact := rec.Cov.FilePieces()
	if tk == nil || !exact {
		return spans
	}
	var out []ingest.CoverageRange
	for _, rg := range ranges {
		for _, iv := range tk.UncoveredBatches(rg.Instance, rec.Cov.Key, rg.Lo, rg.Hi, rec.PartitionTime) {
			out = append(out, ingest.CoverageRange{Instance: rg.Instance, Lo: iv[0], Hi: iv[1]})
		}
	}
	for _, p := range partial {
		if !tk.CoverageCovered(p.Instance, rec.Cov.Key, rawPiece(p), rec.PartitionTime) {
			out = append(out, ingest.CoverageRange{Instance: p.Instance, Lo: p.CSeq, Hi: p.CSeq})
		}
	}
	return out
}

func rawPiece(p ingest.CoveragePiece) raft.CoveragePiece {
	return raft.CoveragePiece{CSeq: p.CSeq, Chunk: p.Chunk, Chunks: p.Chunks, RowLo: p.RowLo, RowHi: p.RowHi, RowTotal: p.RowTotal}
}

// parkedStats is the parked file count and bytes.
func (r *RegistrationOutbox) parkedStats() (int64, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var bytes int64
	for _, e := range r.parked {
		bytes += e.rec.Rec.SizeBytes
	}
	return int64(len(r.parked)), bytes
}

// resolveParkedLoop checks parked files every parkResolveTick once the
// outbox runs against the cluster.
func (r *RegistrationOutbox) resolveParkedLoop(ctx context.Context, manifest OutboxManifest, store OutboxStorage) {
	tk, ok := manifest.(TakeoverReader)
	if !ok {
		return
	}
	t := time.NewTicker(parkResolveTick)
	defer t.Stop()
	lastAlarm := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r.resolveParked(ctx, manifest, tk, store)
		if n, bytes := r.parkedStats(); n > 0 && time.Since(lastAlarm) >= parkAlarmEvery {
			lastAlarm = time.Now()
			r.logger.Error().Int64("files", n).Int64("bytes", bytes).
				Msg("Parked refused files hold rows no registered file has yet; their WAL entries are kept, and the WAL grows until they are re-flushed")
		}
	}
}

// resolveParked deletes every parked file whose rows are all in registered
// files this node holds. Only after a manifest barrier in this process: a
// stale manifest must never decide a delete.
func (r *RegistrationOutbox) resolveParked(ctx context.Context, manifest OutboxManifest, tk TakeoverReader, store OutboxStorage) {
	r.mu.Lock()
	var cands []*parkedEntry
	for _, e := range r.parked {
		cands = append(cands, e)
	}
	p := r.parking
	r.mu.Unlock()
	if len(cands) == 0 {
		return
	}
	bctx, cancel := context.WithTimeout(ctx, outboxAttemptTimeout)
	err := manifest.SyncManifest(bctx)
	cancel()
	if err != nil {
		return
	}
	for _, e := range cands {
		r.mu.Lock()
		pending := false
		if pe, ok := r.byPath[e.rec.Rec.Path]; ok && !pe.done {
			pending = true // parked, not acked yet: its Refused settle comes first
		}
		moved := e.moved
		r.mu.Unlock()
		if pending {
			continue
		}
		if !moved && e.rec.Held != "" {
			// A move that failed when parking: retry it, so the file leaves
			// the query path before the next start.
			if err := r.moveParked(e); err != nil {
				continue
			}
		}
		if !r.parkedResolvable(ctx, e, tk, store) {
			continue
		}
		if err := r.resolve(e, p); err != nil {
			r.logger.Warn().Err(err).Str("path", e.rec.Rec.Path).Msg("Could not remove a resolved parked file; retrying")
		}
	}
}

// parkedResolvable: every row of the file is covered by registered files,
// and each covering file named in the manifest has a local copy here (a
// batch settled by the floor has no file to name). Only for a file that was
// moved: one left in place would stay in the query path.
func (r *RegistrationOutbox) parkedResolvable(ctx context.Context, e *parkedEntry, tk TakeoverReader, store OutboxStorage) bool {
	rec := e.rec.Rec
	r.mu.Lock()
	moved := e.moved
	r.mu.Unlock()
	if rec.Cov == nil || e.rec.Held == "" || !moved {
		return false
	}
	ranges, partial, exact := rec.Cov.FilePieces()
	if !exact {
		return false
	}
	sets := map[uint64]raft.Intervals{}
	for _, rg := range ranges {
		if len(tk.UncoveredBatches(rg.Instance, rec.Cov.Key, rg.Lo, rg.Hi, rec.PartitionTime)) > 0 {
			return false
		}
		sets[rg.Instance] = append(sets[rg.Instance], [2]uint64{rg.Lo, rg.Hi})
	}
	for _, p := range partial {
		if !tk.CoverageCovered(p.Instance, rec.Cov.Key, rawPiece(p), rec.PartitionTime) {
			return false
		}
		sets[p.Instance] = append(sets[p.Instance], [2]uint64{p.CSeq, p.CSeq})
	}
	checked := map[string]bool{}
	for inst, set := range sets {
		for _, f := range tk.CoveringFilesOf(rec.Database, rec.Measurement, rec.PartitionTime, inst, rec.Cov.Key, set) {
			if f.Path == rec.Path {
				return false // the parked file itself is registered
			}
			if checked[f.Path] {
				continue
			}
			size, err := store.StatFile(ctx, f.Path)
			if err != nil || size != f.SizeBytes {
				return false
			}
			checked[f.Path] = true
		}
	}
	return true
}

// resolve deletes a parked file whose rows are registered elsewhere.
func (r *RegistrationOutbox) resolve(e *parkedEntry, p *LocalParking) error {
	r.parkMu.Lock()
	if e.rec.Held != "" && p != nil {
		path := filepath.Join(p.Base, filepath.FromSlash(e.rec.Held))
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			r.parkMu.Unlock()
			return err
		}
		syncDir(filepath.Dir(path))
	}
	done := e.rec
	done.Op = parkOpResolved
	done.At = time.Now().UTC()
	if err := r.appendRefused(done); err != nil {
		r.parkMu.Unlock()
		return err
	}
	r.mu.Lock()
	delete(r.parked, e.rec.Rec.Path)
	r.holdGen++
	r.resolvedSinceCompact++
	compact := r.resolvedSinceCompact > len(r.parked)+16 || len(r.parked) == 0
	if compact {
		r.resolvedSinceCompact = 0
	}
	r.mu.Unlock()
	if compact {
		if err := r.compactRefused(); err != nil {
			r.logger.Warn().Err(err).Msg("Could not compact the refused registration journal")
		}
	}
	r.parkMu.Unlock()
	e.rec.Rec.Cov.Settle(e.rec.Rec.Path, ingest.CoverageResolved)
	metrics.Get().SetWALParked(r.parkedStats())
	r.logger.Info().Str("path", e.rec.Rec.Path).Msg("Parked file removed: every one of its rows is in a registered file this node holds")
	return nil
}

// IsParked reports whether path is a parked registration.
func (r *RegistrationOutbox) IsParked(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.parked[path]
	return ok
}

// SetSendGate makes the outbox wait while gate returns false before sending
// registrations (#1207 S4b: until this writer instance is announced).
func (r *RegistrationOutbox) SetSendGate(gate func() bool) {
	if gate == nil {
		r.sendGate.Store(nil)
		return
	}
	r.sendGate.Store(&gate)
}

// Instances returns the writer instances with covered batches in pending or
// parked registrations, sorted.
func (r *RegistrationOutbox) Instances() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[uint64]bool{}
	note := func(cov *ingest.GroupCoverage) {
		if cov == nil {
			return
		}
		spans, _ := cov.HeldSpans()
		for _, s := range spans {
			seen[s.Instance] = true
		}
		for _, b := range cov.Bounds {
			seen[b.Instance] = true
		}
	}
	for _, e := range r.byPath {
		if !e.done {
			note(e.rec.Cov)
		}
	}
	for _, e := range r.unsyncedAcks {
		note(e.rec.Cov)
	}
	for _, e := range r.parked {
		note(e.rec.Rec.Cov)
	}
	out := make([]uint64, 0, len(seen))
	for inst := range seen {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SetParkingTieringDrop installs the tiering row removal for parked files once
// tiering is up, and removes the rows of files parked before it was.
func (r *RegistrationOutbox) SetParkingTieringDrop(drop func(ctx context.Context, path string) error) {
	r.mu.Lock()
	if r.parking == nil {
		r.mu.Unlock()
		return
	}
	r.parking.DropTiering = drop
	var moved []string
	for path, e := range r.parked {
		if e.moved {
			moved = append(moved, path)
		}
	}
	r.mu.Unlock()
	for _, path := range moved {
		if err := drop(context.Background(), path); err != nil {
			r.logger.Warn().Err(err).Str("path", path).Msg("Could not remove the tiering row of a parked file")
		}
	}
}

// CoveredSent reports how many covered registrations this outbox has sent.
func (r *RegistrationOutbox) CoveredSent() int64 { return r.coveredSent.Load() }
