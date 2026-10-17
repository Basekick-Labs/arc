package cluster

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// Republishing parked batches from the WAL (#1207 S4b1c2b).
//
// A file of an earlier instance of this writer that the manifest refused
// (part of it was taken over by another writer, or registered already) is
// parked whole, with its WAL entries kept. The rows of it nobody registered
// are this writer's to publish: the parts no active claim holds, and once a
// claim finished, what its claimant could not publish. They are read back
// from the WAL and written again through the claimed-replay path in origin
// mode: their own identity, only the rows not registered for the parked
// file's hour. The manifest arbitrates: a copy that lost a race to a claim
// or another file is refused and parks in turn. Once every row of a parked
// file is registered, the resolution deletes it and the WAL lets go.
//
// The manifest cannot always tell a parked piece is covered: a whole batch
// republished chunk by chunk has no piece in an hour where a chunk has no
// rows, and a row range none in an hour where its rows are elsewhere. Having
// decoded the entries, the republisher can: it records (instance, key,
// batch, hour) once every row of the parked pieces in that hour is
// registered, and the resolution takes that as covered.
//
// The current instance is left out: its refused batches are settled by its
// own flush groups, and a claim of it ends this process.

const (
	// republishEvery: the claimant's loop runs a pass this often.
	republishEvery = 30 * time.Second
	// republishScanBytes bounds the WAL bytes one pass reads.
	republishScanBytes = 1 << 30
)

type republisher struct {
	c        *Coordinator
	outbox   republishOutbox
	replay   claimantReplayer
	walDir   string
	instance func() uint64 // this process's instance
	// current is the WAL file this process writes (only its own instance's
	// entries); nil reads every file.
	current func() string
	logger  zerolog.Logger

	summaries map[string]walSummary
	// bad are records that cannot be republished in an hour (zero: every
	// hour), logged once; missing are batches with no complete WAL copy
	// here, logged once. Neither is looked for again.
	bad     map[republishHour]bool
	missing map[republishBatch]bool

	mu sync.Mutex
	// covered are (instance, key, batch, hour) whose parked rows are all
	// registered (ParkedCovered).
	covered map[republishBatchHour]bool
}

type republishOutbox interface {
	Instances() []uint64
	InstanceFiles(inst uint64) []InstanceFile
}

// republishRec names one WAL record of an earlier instance.
type republishRec struct {
	inst  uint64
	key   string
	cseq  uint64
	chunk uint16
}

type republishHour struct {
	rec  republishRec
	hour int64
}

type republishBatch struct {
	inst uint64
	key  string
	cseq uint64
}

type republishBatchHour struct {
	republishBatch
	hour int64
}

// walSummary is which batches one WAL file holds, per instance and key.
type walSummary struct {
	size    int64
	modTime time.Time
	spans   map[uint64]map[string]raft.Intervals
}

// republishCand is part of one batch to republish in one hour: every chunk
// (chunk < 0) or one, every row (rowTotal 0) or a range.
type republishCand struct {
	hour     time.Time
	chunk    int
	rowLo    int
	rowHi    int
	rowTotal int
	// piece is the parked piece a partial candidate stands for, so settle
	// can ask the manifest again whether it is covered; nil for a whole batch.
	piece *raft.CoveragePiece
}

func (c republishCand) matches(chunk uint16) bool { return c.chunk < 0 || c.chunk == int(chunk) }

func newRepublisher(c *Coordinator, outbox republishOutbox, replay claimantReplayer, walDir string, instance func() uint64) *republisher {
	return &republisher{
		c: c, outbox: outbox, replay: replay, walDir: walDir, instance: instance,
		logger:    c.logger.With().Str("component", "stream-republish").Logger(),
		summaries: map[string]walSummary{},
		bad:       map[republishHour]bool{},
		missing:   map[republishBatch]bool{},
		covered:   map[republishBatchHour]bool{},
	}
}

// ParkedCovered reports whether every parked row of batch cseq of (inst,
// key) in hour is registered, as the republisher verified (the parking
// resolution's hook).
func (rp *republisher) ParkedCovered(inst uint64, key string, cseq uint64, hour time.Time) bool {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.covered[republishBatchHour{republishBatch{inst, key, cseq}, hour.UTC().Truncate(time.Hour).Unix()}]
}

// pass republishes what it can of every parked file.
func (rp *republisher) pass(ctx context.Context) {
	fsm := rp.c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() {
		return
	}
	cands := rp.candidates(ctx, fsm)
	if len(cands) == 0 {
		return
	}
	rp.republish(ctx, fsm, cands)
}

// candidates returns, per instance, key and batch, the parts of parked
// files to republish, read after a manifest barrier.
func (rp *republisher) candidates(ctx context.Context, fsm *raft.ClusterFSM) map[uint64]map[string]map[uint64][]republishCand {
	self := rp.instance()
	var insts []uint64
	for _, inst := range rp.outbox.Instances() {
		if inst != self && inst != 0 {
			insts = append(insts, inst)
		}
	}
	if len(insts) == 0 {
		rp.mu.Lock()
		clear(rp.covered) // nothing of an earlier instance is parked here
		rp.mu.Unlock()
		return nil
	}
	sctx, cancel := context.WithTimeout(ctx, claimantSyncBudget)
	err := rp.c.SyncManifest(sctx)
	cancel()
	if err != nil {
		return nil
	}
	states := fsm.TakeoverInstanceStates()
	claims := map[uint64]raft.StreamClaim{}
	for _, k := range fsm.StreamClaims() {
		claims[k.Instance] = k
	}
	out := map[uint64]map[string]map[uint64][]republishCand{}
	parked := map[republishBatchHour]bool{}
	defer func() {
		rp.mu.Lock()
		for bh := range rp.covered {
			if !parked[bh] {
				delete(rp.covered, bh)
			}
		}
		rp.mu.Unlock()
	}()
	for _, inst := range insts {
		st, known := states[inst]
		if !known || st.Untracked {
			continue // nothing arbitrates it: it stays parked
		}
		files := rp.outbox.InstanceFiles(inst)
		// Batches a file still on its way holds, per key and hour: it
		// registers or parks first.
		type kh struct {
			key  string
			hour int64
		}
		pending := map[kh]raft.Intervals{}
		for _, f := range files {
			if f.Parked {
				continue
			}
			h := kh{f.Key, f.PartitionTime.UTC().Truncate(time.Hour).Unix()}
			for _, r := range f.Ranges {
				pending[h] = pending[h].Union(raft.Intervals{r})
			}
			for _, p := range f.Partial {
				pending[h] = pending[h].Union(raft.Intervals{{p.CSeq, p.CSeq}})
			}
		}
		k, claimed := claims[inst]
		for _, f := range files {
			if !f.Parked || !f.Exact || f.Epoch != 0 {
				continue
			}
			hour := f.PartitionTime.UTC().Truncate(time.Hour)
			// Batches an unfinished claim holds are its claimant's.
			var skip raft.Intervals
			if claimed && !k.Released && !k.Done {
				skip = k.Set[f.Key]
			}
			skip = skip.Union(pending[kh{f.Key, hour.Unix()}])
			add := func(cseq uint64, c republishCand) {
				b := republishBatch{inst, f.Key, cseq}
				parked[republishBatchHour{b, hour.Unix()}] = true
				if skip.Contains(cseq) || rp.missing[b] || rp.isCovered(b, hour) {
					return
				}
				if out[inst] == nil {
					out[inst] = map[string]map[uint64][]republishCand{}
				}
				if out[inst][f.Key] == nil {
					out[inst][f.Key] = map[uint64][]republishCand{}
				}
				out[inst][f.Key][cseq] = append(out[inst][f.Key][cseq], c)
			}
			for _, r := range f.Ranges {
				for _, iv := range fsm.UncoveredBatches(inst, f.Key, r[0], r[1], hour) {
					for c := iv[0]; c <= iv[1]; c++ {
						add(c, republishCand{hour: hour, chunk: -1})
					}
				}
			}
			for _, p := range f.Partial {
				if fsm.CoverageCovered(inst, f.Key, rawPiece(p), hour) {
					continue
				}
				piece := rawPiece(p)
				c := republishCand{hour: hour, chunk: -1, rowLo: p.RowLo, rowHi: p.RowHi, rowTotal: p.RowTotal, piece: &piece}
				if p.Chunks > 0 {
					c.chunk = int(p.Chunk)
				}
				add(p.CSeq, c)
			}
		}
	}
	return out
}

func (rp *republisher) isCovered(b republishBatch, hour time.Time) bool {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.covered[republishBatchHour{b, hour.Unix()}]
}

// republish reads the candidates' WAL records, replays them, and records
// what it learned: batches with no complete WAL copy, and (batch, hour)
// whose parked rows are all registered.
func (rp *republisher) republish(ctx context.Context, fsm *raft.ClusterFSM, cands map[uint64]map[string]map[uint64][]republishCand) {
	rp.pruneSummaries()
	paths, err := filepath.Glob(filepath.Join(rp.walDir, "*.wal"))
	if err != nil {
		return
	}
	sort.Strings(paths)
	current := ""
	if rp.current != nil {
		current = rp.current()
	}
	want := func(c wal.Coverage) bool {
		for _, x := range cands[c.Instance][c.Key][c.CSeq] {
			if x.matches(c.Chunk) {
				return true
			}
		}
		return false
	}
	type ik struct {
		inst uint64
		key  string
	}
	found := map[republishBatch]*republishSeen{}
	batch := map[ik][]ingest.ClaimedRecord{}
	bytes := map[ik]int{}
	// outcomes per (record, hour); a record's hours with rows all appear.
	outcomes := map[republishRec]map[int64]ingest.ReplayOutcome{}
	flush := func(g ik) {
		recs := batch[g]
		delete(batch, g)
		delete(bytes, g)
		if len(recs) > 0 {
			rp.replayCall(ctx, g.inst, g.key, recs, outcomes)
		}
	}
	scanned, complete := int64(0), true
	for _, path := range paths {
		if ctx.Err() != nil {
			return
		}
		if path == current {
			continue
		}
		if scanned >= republishScanBytes {
			complete = false
			break
		}
		if !rp.fileMayHold(path, cands) {
			continue
		}
		if fi, err := os.Stat(path); err == nil {
			scanned += fi.Size()
		}
		err := republishScanFile(path, rp.logger, want, func(e *wal.Entry) error {
			c := e.Coverage
			b := republishBatch{c.Instance, c.Key, c.CSeq}
			sn := found[b]
			if sn == nil {
				sn = &republishSeen{chunks: map[uint16]bool{}, of: max(c.Chunks, 1)}
				found[b] = sn
			}
			if sn.chunks[c.Chunk] {
				return nil // a second copy of one entry
			}
			sn.chunks[c.Chunk] = true
			rec := republishRec{c.Instance, c.Key, c.CSeq, c.Chunk}
			g := ik{c.Instance, c.Key}
			size := republishEntryBytes(e)
			if len(batch[g]) >= replayCallRecords || (len(batch[g]) > 0 && bytes[g]+size > replayCallBytes) {
				flush(g)
			}
			batch[g] = append(batch[g], ingest.ClaimedRecord{
				CSeq: c.CSeq, Chunk: c.Chunk, Chunks: max(c.Chunks, 1), Entry: e, PayloadBytes: size,
				Keep: rp.keepFor(fsm, cands[c.Instance][c.Key][c.CSeq], c, rec),
			})
			bytes[g] += size
			return nil
		})
		if errors.Is(err, fs.ErrNotExist) {
			// Purged between the listing and the read, which the prompt WAL
			// purge makes routine (#676). Not a gap in the scan: a file
			// holding a parked batch nobody registered is kept by the
			// registration hold, so a file that can go held none of the
			// candidates.
			continue
		}
		if err != nil {
			complete = false
			rp.logger.Warn().Err(err).Str("file", filepath.Base(path)).Msg("Reading a WAL file to republish parked batches failed")
		}
	}
	for g := range batch {
		flush(g)
	}
	rp.settle(fsm, cands, found, outcomes, complete)
}

// candsUncovered reports whether any of a batch's candidates is still
// uncovered in the manifest.
func candsUncovered(fsm *raft.ClusterFSM, inst uint64, key string, cseq uint64, cs []republishCand) bool {
	if fsm == nil {
		return true
	}
	for _, x := range cs {
		if x.piece != nil {
			if !fsm.CoverageCovered(inst, key, *x.piece, x.hour) {
				return true
			}
			continue
		}
		if len(fsm.UncoveredBatches(inst, key, cseq, cseq, x.hour)) > 0 {
			return true
		}
	}
	return false
}

// republishSeen is the chunks of one batch the WAL scan found.
type republishSeen struct {
	chunks map[uint16]bool
	of     uint16
}

// has reports whether every chunk the candidates need was found.
func (sn *republishSeen) has(cs []republishCand) bool {
	if sn == nil {
		return false
	}
	for _, x := range cs {
		if x.chunk >= 0 {
			if !sn.chunks[uint16(x.chunk)] {
				return false
			}
			continue
		}
		for ch := uint16(0); ch < sn.of; ch++ {
			if !sn.chunks[ch] {
				return false
			}
		}
	}
	return true
}

// settle records batches with no complete WAL copy (only after a complete
// scan), and each (batch, hour) whose parked rows are all registered: every
// record the candidates need answered Covered or NothingLeft in that hour,
// or has no rows in it.
func (rp *republisher) settle(fsm *raft.ClusterFSM, cands map[uint64]map[string]map[uint64][]republishCand, found map[republishBatch]*republishSeen,
	outcomes map[republishRec]map[int64]ingest.ReplayOutcome, complete bool) {
	for inst, byKey := range cands {
		for key, byCSeq := range byKey {
			for cseq, cs := range byCSeq {
				b := republishBatch{inst, key, cseq}
				sn := found[b]
				if !sn.has(cs) {
					// Only for a batch still uncovered now: another writer can
					// register it during the pass, and its WAL file then goes
					// (#676 purges it at once), which is not a missing copy.
					if complete && !rp.missing[b] && candsUncovered(fsm, inst, key, cseq, cs) {
						rp.missing[b] = true
						rp.logger.Error().Str("instance", fmt.Sprintf("%016x", inst)).Str("key", key).Uint64("batch", cseq).
							Msg("A parked batch nobody registered has no complete WAL copy here; its rows stay parked")
					}
					continue
				}
				hours := map[int64]bool{}
				for _, x := range cs {
					hours[x.hour.Unix()] = true
				}
				for h := range hours {
					ok := true
					for ch := uint16(0); ch < sn.of && ok; ch++ {
						needed := false
						for _, x := range cs {
							needed = needed || (x.hour.Unix() == h && x.matches(ch))
						}
						if !needed {
							continue
						}
						byHour := outcomes[republishRec{inst, key, cseq, ch}]
						if o, whole := byHour[0]; whole && o != ingest.ReplayCovered && o != ingest.ReplayNothingLeft {
							ok = false
						} else if o, has := byHour[h]; has && o != ingest.ReplayCovered && o != ingest.ReplayNothingLeft {
							ok = false
						}
					}
					if ok {
						rp.mu.Lock()
						rp.covered[republishBatchHour{b, h}] = true
						rp.mu.Unlock()
					}
				}
			}
		}
	}
}

// keepFor is a record's Keep, skipping hours that cannot be republished.
func (rp *republisher) keepFor(fsm *raft.ClusterFSM, cs []republishCand, c *wal.Coverage, rec republishRec) func(time.Time, int) ([][2]int, error) {
	keep := republishKeep(fsm, cs, c)
	return func(hour time.Time, total int) ([][2]int, error) {
		h := hour.UTC().Truncate(time.Hour).Unix()
		if rp.bad[republishHour{rec, h}] || rp.bad[republishHour{rec, 0}] {
			return nil, errRepublishBad
		}
		return keep(hour, total)
	}
}

var errRepublishBad = errors.New("cannot be republished (reported earlier)")

// replayCall replays the records of one (instance, key), noting each
// record's outcome per hour (hour 0: the whole record).
func (rp *republisher) replayCall(ctx context.Context, inst uint64, key string, recs []ingest.ClaimedRecord, outcomes map[republishRec]map[int64]ingest.ReplayOutcome) {
	note := func(r ingest.ClaimedRecord, hour int64, o ingest.ReplayOutcome) {
		id := republishRec{inst, key, r.CSeq, r.Chunk}
		if outcomes[id] == nil {
			outcomes[id] = map[int64]ingest.ReplayOutcome{}
		}
		outcomes[id][hour] = o
	}
	db, m, ok := strings.Cut(key, "/")
	if !ok {
		return
	}
	res, err := rp.replay.ReplayClaimed(ctx, ingest.ClaimedReplay{Database: db, Measurement: m, Instance: inst, Origin: true, Records: recs})
	if err != nil {
		for _, r := range recs {
			note(r, 0, ingest.ReplayFailed)
		}
		if !errors.Is(err, ingest.ErrReplayClosed) {
			rp.logger.Warn().Err(err).Str("key", key).Msg("Republishing parked batches failed; retrying")
		}
		return
	}
	for _, o := range res {
		r := recs[o.Record]
		h := int64(0)
		if !o.Hour.IsZero() {
			h = o.Hour.UTC().Truncate(time.Hour).Unix()
		}
		note(r, h, o.Outcome)
		switch o.Outcome {
		case ingest.ReplayUnreplayable:
			id := republishHour{republishRec{inst, key, r.CSeq, r.Chunk}, h}
			if errors.Is(o.Err, errRepublishBad) || rp.bad[id] {
				continue
			}
			rp.bad[id] = true
			rp.logger.Error().Err(o.Err).Str("instance", fmt.Sprintf("%016x", inst)).Str("key", key).Uint64("batch", r.CSeq).
				Msg("A parked batch cannot be republished from the WAL; its rows stay parked")
		case ingest.ReplayFailed:
			rp.logger.Warn().Err(o.Err).Str("key", key).Msg("Republishing a parked batch failed; retrying")
		}
	}
}

// republishKeep returns the Keep of one WAL record: the rows of its
// candidates in each of their hours, minus the rows registered there.
func republishKeep(fsm *raft.ClusterFSM, cs []republishCand, c *wal.Coverage) func(time.Time, int) ([][2]int, error) {
	registered := claimKeep(fsm, c.Instance, c.Key, c.CSeq, c.Chunk, max(c.Chunks, 1))
	return func(hour time.Time, total int) ([][2]int, error) {
		var want [][2]int
		for _, x := range cs {
			if !x.hour.Equal(hour.UTC().Truncate(time.Hour)) || (x.chunk >= 0 && x.chunk != int(c.Chunk)) {
				continue
			}
			if x.rowTotal == 0 {
				want = append(want, [2]int{0, total})
				continue
			}
			if x.rowTotal != total {
				return nil, fmt.Errorf("batch %d of %s is parked against %d rows, the WAL entry has %d", c.CSeq, c.Key, x.rowTotal, total)
			}
			want = append(want, [2]int{x.rowLo, x.rowHi})
		}
		if len(want) == 0 {
			return nil, nil // not parked in this hour
		}
		free, err := registered(hour, total)
		if err != nil {
			return nil, err
		}
		return intersectRows(normalizeRows(want), free), nil
	}
}

// normalizeRows sorts and merges row ranges.
func normalizeRows(rs [][2]int) [][2]int {
	sort.Slice(rs, func(i, j int) bool { return rs[i][0] < rs[j][0] })
	var out [][2]int
	for _, r := range rs {
		if r[1] <= r[0] {
			continue
		}
		if n := len(out); n > 0 && r[0] <= out[n-1][1] {
			out[n-1][1] = max(out[n-1][1], r[1])
			continue
		}
		out = append(out, r)
	}
	return out
}

// intersectRows intersects two sorted, disjoint lists of row ranges.
func intersectRows(a, b [][2]int) [][2]int {
	var out [][2]int
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		lo, hi := max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
		if lo < hi {
			out = append(out, [2]int{lo, hi})
		}
		if a[i][1] < b[j][1] {
			i++
		} else {
			j++
		}
	}
	return out
}

// republishEntryBytes estimates a decoded entry's size for the call bound.
func republishEntryBytes(e *wal.Entry) int {
	n := 0
	if e.ColumnarData != nil {
		for _, col := range e.ColumnarData.Columns {
			n += 16 * len(col)
		}
	}
	n += 64 * len(e.Records)
	return max(n, 1)
}

// republishScanFile reads a WAL file's covered entries. A variable so a test
// can remove the file between the listing and the read, as the WAL purge does.
var republishScanFile = func(path string, logger zerolog.Logger, match func(wal.Coverage) bool, fn func(*wal.Entry) error) error {
	return wal.NewReader(path, logger).ScanCovered(match, fn)
}

// fileMayHold reports whether the WAL file at path holds a batch of the
// candidates, from a summary of its covered entries read without decoding
// them (kept while the file's size and time do not change).
func (rp *republisher) fileMayHold(path string, cands map[uint64]map[string]map[uint64][]republishCand) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	sum, ok := rp.summaries[path]
	if !ok || sum.size != fi.Size() || !sum.modTime.Equal(fi.ModTime()) {
		sum = walSummary{size: fi.Size(), modTime: fi.ModTime(), spans: map[uint64]map[string]raft.Intervals{}}
		err := republishScanFile(path, rp.logger, func(c wal.Coverage) bool {
			if sum.spans[c.Instance] == nil {
				sum.spans[c.Instance] = map[string]raft.Intervals{}
			}
			sum.spans[c.Instance][c.Key] = sum.spans[c.Instance][c.Key].Union(raft.Intervals{{c.CSeq, c.CSeq}})
			return false
		}, func(*wal.Entry) error { return nil })
		if errors.Is(err, fs.ErrNotExist) {
			return false // purged since the Stat above (#676); see republish
		}
		if err != nil {
			return true // read it in full: the scan reports the error
		}
		rp.summaries[path] = sum
	}
	for inst, byKey := range cands {
		for key, byCSeq := range byKey {
			held := sum.spans[inst][key]
			for cseq := range byCSeq {
				if held.Contains(cseq) {
					return true
				}
			}
		}
	}
	return false
}

// pruneSummaries forgets files no longer on disk.
func (rp *republisher) pruneSummaries() {
	for p := range rp.summaries {
		if _, err := os.Stat(p); err != nil {
			delete(rp.summaries, p)
		}
	}
}
