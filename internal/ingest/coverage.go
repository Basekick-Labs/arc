package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
)

// Coverage (#1207 S1).
//
// Every batch committed to a buffer on a coverage-enabled writer gets a
// coverage id (instance, buffer key, cseq). cseq is minted per key under the
// shard lock at the moment the batch enters shard.buffers, so the batches of a
// key commit in cseq order and a flush of the key's live batches covers one
// contiguous range. A batch that is refused before that point mints nothing
// and — through wal.PreparedAppend — writes nothing to the WAL either.
//
// The coverage travels with the batch into the WAL entry (0x04), through the
// flush, and into the registration outbox with every file the flush wrote.
// The outbox reports each file's outcome; when every file of a flush group has
// settled and the group's WAL checkpoint is written, the group's WAL entries
// leave the purge floor (purge on registration) and its cseqs settle in the
// registered-floor tracker. The floor of a key is the highest cseq at or below
// which every batch is in a registered file; a later stage streams it to
// standby writers, which trim their copy below it.

// CoveragePiece is one batch's coverage, or part of one when recovery replays
// a covered entry in chunks or row ranges.
type CoveragePiece struct {
	Instance uint64 `json:"i"`
	CSeq     uint64 `json:"c"`
	// Chunks is zero for a whole batch. A replayed WAL entry is one chunk of
	// a batch split into Chunks entries.
	Chunk  uint16 `json:"k,omitempty"`
	Chunks uint16 `json:"n,omitempty"`
	// RowTotal is zero when the piece holds all rows of its chunk; otherwise it
	// holds rows [RowLo, RowHi) of RowTotal (a recovery row-range split).
	RowLo    int `json:"rl,omitempty"`
	RowHi    int `json:"rh,omitempty"`
	RowTotal int `json:"rt,omitempty"`
	// replayable is true when every WAL entry of the batch was enqueued: a
	// failed flush leaves the batch for recovery to replay. A batch whose WAL
	// append was dropped is replayable by nothing. Not persisted.
	replayable bool
}

func (p CoveragePiece) whole() bool { return p.Chunks == 0 && p.RowTotal == 0 }

// CoverageRange is a run of whole batches [Lo, Hi] of one instance.
type CoverageRange struct {
	Instance uint64 `json:"i"`
	Lo       uint64 `json:"lo"`
	Hi       uint64 `json:"hi"`
}

// GroupCoverage is a flush group's coverage, as handed to the registrar with
// every file of the group. Whole batches are folded into ranges; partial
// pieces (replays) are listed.
type GroupCoverage struct {
	ID      string          `json:"id"`
	Key     string          `json:"key"`
	Files   int             `json:"files"`
	Ranges  []CoverageRange `json:"ranges,omitempty"`
	Partial []CoveragePiece `json:"partial,omitempty"`
	// RangesOmitted and PartialOmitted count what a journaled copy left out
	// (see MarshalJSON): a reader never takes it for a complete coverage.
	RangesOmitted  int `json:"ranges_omitted,omitempty"`
	PartialOmitted int `json:"partial_omitted,omitempty"`

	// onSettle receives each file's outcome. Nil on coverage loaded from a
	// journal: the group that would act on it belonged to an earlier process.
	onSettle func(path string, outcome CoverageOutcome)
}

// NewGroupCoverage builds a GroupCoverage whose Settle calls onSettle. The
// buffer builds its own; this is for registrar tests.
func NewGroupCoverage(id, key string, files int, onSettle func(path string, outcome CoverageOutcome)) *GroupCoverage {
	return &GroupCoverage{ID: id, Key: key, Files: files, onSettle: onSettle}
}

// Journal bounds: a GroupCoverage is journaled with every file of its group,
// and a reader before #1207 refuses a journal with a line over 1 MiB. Whole
// batches fold into ranges, so only a long recovery can come near these.
const (
	maxJournalRanges  = 4096
	maxJournalPartial = 2048
)

// MarshalJSON caps what is journaled. A capped record says how much it left
// out, so a reader never takes a truncated coverage for a complete one.
func (g GroupCoverage) MarshalJSON() ([]byte, error) {
	type plain GroupCoverage
	out := plain(g)
	if n := len(out.Ranges); n > maxJournalRanges {
		out.Ranges, out.RangesOmitted = out.Ranges[:maxJournalRanges], out.RangesOmitted+n-maxJournalRanges
	}
	if n := len(out.Partial); n > maxJournalPartial {
		out.Partial, out.PartialOmitted = out.Partial[:maxJournalPartial], out.PartialOmitted+n-maxJournalPartial
	}
	return json.Marshal(out)
}

// CoverageOutcome is how one file of a group left the registration outbox.
type CoverageOutcome int

const (
	// CoverageRegistered: the file is in the cluster manifest.
	CoverageRegistered CoverageOutcome = iota
	// CoverageWithdrawn: the file was deliberately removed (DELETE dropped
	// every row) — its rows are gone on purpose.
	CoverageWithdrawn
	// CoverageLost: the outbox settled the file without it being registered
	// (gone, size mismatch, invalid path). Its rows are in no registered file.
	CoverageLost
	// CoverageAbandoned: the outbox stopped without a cluster; nothing will
	// ever register the file. The WAL hold is released; the floor holds.
	CoverageAbandoned
)

func (o CoverageOutcome) String() string {
	switch o {
	case CoverageRegistered:
		return "registered"
	case CoverageWithdrawn:
		return "withdrawn"
	case CoverageLost:
		return "lost"
	default:
		return "abandoned"
	}
}

// Settle reports the outcome of one file of the group. The registrar calls it
// once per file it was given; a ReplacePending moves the coverage to the
// rewrite and settles only the rewrite. Safe on a nil receiver and to call
// concurrently.
func (g *GroupCoverage) Settle(path string, outcome CoverageOutcome) {
	if g == nil || g.onSettle == nil {
		return
	}
	g.onSettle(path, outcome)
}

// CoverageFileRegistrar is a FileRegistrar that persists coverage with each
// file and reports its outcome through GroupCoverage.Settle. The cluster
// registration outbox implements it.
type CoverageFileRegistrar interface {
	FileRegistrar
	RegisterCoveredFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string, cov *GroupCoverage)
}

// coveredWALWriter is the WAL a coverage-enabled buffer appends through.
type coveredWALWriter interface {
	PrepareCovered(database, key string, payload []byte) (*wal.PreparedAppend, error)
	TrackedInstance() uint64
	MarkFlushedRetained(hashes []string) error
	ReleaseRegistered(hashes []string)
}

// coverageState is installed by EnableCoverage.
type coverageState struct {
	wal      coveredWALWriter
	instance uint64
	tracker  *coverageTracker
	groupSeq atomic.Uint64
}

// EnableCoverage turns on coverage ids (#1207). Call before the buffer takes
// writes; the WAL must be a *wal.Writer (or another coveredWALWriter) and the
// file registrar a CoverageFileRegistrar, or coverage stays off and the
// returned error says why.
func (b *ArrowBuffer) EnableCoverage() error {
	cw, ok := b.wal.(coveredWALWriter)
	if !ok || b.wal == nil {
		return fmt.Errorf("coverage needs a tracked WAL")
	}
	ref := b.fileRegistrar.Load()
	if ref == nil {
		return fmt.Errorf("coverage needs a file registrar")
	}
	if _, ok := ref.FileRegistrar.(CoverageFileRegistrar); !ok {
		return fmt.Errorf("coverage needs a journaled registration outbox")
	}
	b.coverage.Store(&coverageState{wal: cw, instance: cw.TrackedInstance(), tracker: newCoverageTracker(cw.TrackedInstance())})
	b.logger.Info().Str("instance", fmt.Sprintf("%016x", cw.TrackedInstance())).Msg("WAL coverage enabled: ingest batches carry coverage ids and WAL entries are held until their files are registered")
	return nil
}

// DisableCoverage stops minting coverage. Called when the registrar is
// abandoned: nothing would register the files, so holding the WAL for them
// would only grow it.
func (b *ArrowBuffer) DisableCoverage() {
	if b.coverage.Swap(nil) != nil {
		b.logger.Warn().Msg("WAL coverage disabled: the registration outbox was abandoned")
	}
}

// RegisteredFloors returns the registered floor of every key this instance
// has minted coverage for: every batch with cseq at or below the floor is in a
// registered file.
func (b *ArrowBuffer) RegisteredFloors() map[string]uint64 {
	cs := b.coverage.Load()
	if cs == nil {
		return nil
	}
	return cs.tracker.floors()
}

// KeyRegistration is one buffer key's registration progress for this
// instance: every cseq at or below Floor, and in Settled, is in a registered
// file; the cseqs in Lost settled without one.
type KeyRegistration struct {
	Floor   uint64
	Settled [][2]uint64
	Lost    []uint64
}

// Bounds on what one progress report carries per key: settled intervals
// above a floor stop at the first lost batch, which is rare and alarmed.
const (
	maxReportedSettled = 1024
	maxReportedLost    = 1024
)

// RegistrationProgress reports this instance's per-key progress, for the
// origin stream to publish to standby writers (#1207). ok is false while
// coverage is off.
func (b *ArrowBuffer) RegistrationProgress() (instance uint64, keys map[string]KeyRegistration, ok bool) {
	cs := b.coverage.Load()
	if cs == nil {
		return 0, nil, false
	}
	return cs.instance, cs.tracker.progress(), true
}

// nextCSeqLocked mints the next coverage sequence for key. Caller holds
// shard.mu, and must commit the batch under the same acquisition.
func (b *ArrowBuffer) nextCSeqLocked(cs *coverageState, shard *bufferShard, key string) uint64 {
	if shard.cseqs == nil {
		shard.cseqs = make(map[string]uint64)
	}
	shard.cseqs[key]++
	cs.tracker.minted()
	return shard.cseqs[key]
}

// replayPiece builds the coverage piece of a replayed covered entry, or false
// when the context carries none (a 0x03 entry, or coverage off).
func replayPiece(ctx context.Context, key, walIdentity string) (CoveragePiece, bool) {
	rc, ok := wal.ReplayCoverageFrom(ctx)
	// The entry records the key it was minted under; recovery derives the key
	// it replays into from the payload. A mismatch would settle another key's
	// cseq, so such an entry replays without coverage.
	if !ok || rc.Coverage.Key != key {
		return CoveragePiece{}, false
	}
	p := CoveragePiece{Instance: rc.Coverage.Instance, CSeq: rc.Coverage.CSeq, replayable: true}
	if rc.Coverage.Chunks > 1 {
		p.Chunk, p.Chunks = rc.Coverage.Chunk, rc.Coverage.Chunks
	}
	if _, start, end, ok := wal.ParseRecoveryRowIdentity(walIdentity); ok && rc.RowTotal > 0 && end-start < rc.RowTotal {
		p.RowLo, p.RowHi, p.RowTotal = start, end, rc.RowTotal
	}
	if p.Chunks == 1 {
		p.Chunks = 0
	}
	return p, true
}

func collectCoverage(records []interface{}) []CoveragePiece {
	var pieces []CoveragePiece
	for _, record := range records {
		if batch, ok := record.(*TypedColumnBatch); ok {
			pieces = append(pieces, batch.Coverage...)
		}
	}
	return pieces
}

// flushGroup joins the two events that release a covered flush's WAL entries:
// the checkpoint is written, and every file has settled in the outbox. They
// happen in either order — a fresh file can be confirmed by the outbox's
// drain before the flush gets to its checkpoint.
type flushGroup struct {
	b      *ArrowBuffer
	cs     *coverageState
	key    string
	pieces []CoveragePiece
	tokens []string

	mu           sync.Mutex
	files        int
	announced    bool // coverage() was called: the flush registered files
	settled      map[string]struct{}
	checkpointed bool
	lost         bool
	finished     bool
}

// newFlushGroup builds the coverage of an extracted buffer. Nil when coverage
// is off or none of the batches carries any.
func (b *ArrowBuffer) newFlushGroup(key string, records []interface{}, tokens []string) *flushGroup {
	cs := b.coverage.Load()
	if cs == nil {
		return nil
	}
	pieces := collectCoverage(records)
	if len(pieces) == 0 {
		return nil
	}
	return &flushGroup{b: b, cs: cs, key: key, pieces: pieces, tokens: tokens, settled: make(map[string]struct{})}
}

// coverage is what the registrar receives for each of the group's files.
func (g *flushGroup) coverage(files int) *GroupCoverage {
	g.mu.Lock()
	g.files = files
	g.announced = true
	g.mu.Unlock()
	gc := &GroupCoverage{
		ID:       fmt.Sprintf("%016x-%d", g.cs.instance, g.cs.groupSeq.Add(1)),
		Key:      g.key,
		Files:    files,
		onSettle: g.settle,
	}
	gc.Ranges, gc.Partial = foldPieces(g.pieces)
	return gc
}

// foldPieces folds whole pieces into maximal ranges per instance and returns
// the partial pieces separately, both in a stable order.
func foldPieces(pieces []CoveragePiece) ([]CoverageRange, []CoveragePiece) {
	var whole, partial []CoveragePiece
	for _, p := range pieces {
		if p.whole() {
			whole = append(whole, p)
		} else {
			partial = append(partial, p)
		}
	}
	sort.Slice(whole, func(i, j int) bool {
		if whole[i].Instance != whole[j].Instance {
			return whole[i].Instance < whole[j].Instance
		}
		return whole[i].CSeq < whole[j].CSeq
	})
	var ranges []CoverageRange
	for _, p := range whole {
		if n := len(ranges); n > 0 && ranges[n-1].Instance == p.Instance && p.CSeq <= ranges[n-1].Hi+1 {
			if p.CSeq > ranges[n-1].Hi {
				ranges[n-1].Hi = p.CSeq
			}
			continue
		}
		ranges = append(ranges, CoverageRange{Instance: p.Instance, Lo: p.CSeq, Hi: p.CSeq})
	}
	sort.Slice(partial, func(i, j int) bool {
		a, b := partial[i], partial[j]
		if a.Instance != b.Instance {
			return a.Instance < b.Instance
		}
		if a.CSeq != b.CSeq {
			return a.CSeq < b.CSeq
		}
		if a.Chunk != b.Chunk {
			return a.Chunk < b.Chunk
		}
		return a.RowLo < b.RowLo
	})
	return ranges, partial
}

func (g *flushGroup) settle(path string, outcome CoverageOutcome) {
	g.mu.Lock()
	if _, dup := g.settled[path]; dup || g.finished {
		g.mu.Unlock()
		return
	}
	g.settled[path] = struct{}{}
	if outcome == CoverageLost || outcome == CoverageAbandoned {
		g.lost = true
	}
	g.mu.Unlock()
	if outcome == CoverageLost {
		metrics.Get().IncWALCoverageLost(1)
		g.b.logger.Error().Str("buffer_key", g.key).Str("path", path).
			Msg("A flushed file settled without being registered; its rows are in no registered file and the registered floor of this key stops below them")
	}
	g.maybeFinish()
}

// checkpointDone records that the group's WAL checkpoint was attempted. A
// failed checkpoint counts: the data is in storage, as markWALFlushed's
// release on failure already assumes.
func (g *flushGroup) checkpointDone() {
	g.mu.Lock()
	g.checkpointed = true
	g.mu.Unlock()
	g.maybeFinish()
}

func (g *flushGroup) maybeFinish() {
	g.mu.Lock()
	if g.finished || !g.checkpointed || (g.announced && len(g.settled) < g.files) {
		g.mu.Unlock()
		return
	}
	g.finished = true
	// A flush that succeeded without registering a file put the rows in no
	// registered file. Not reachable today; release rather than pin the WAL.
	lost := g.lost || !g.announced
	g.mu.Unlock()
	g.cs.wal.ReleaseRegistered(g.tokens)
	g.cs.tracker.settleKey(g.key, g.pieces, !lost)
}

// failed handles a flush that wrote and registered nothing. Pieces whose
// every WAL entry is in the WAL stay unsettled: recovery replays them and the
// replay registers them. A piece with no complete WAL copy is gone for good,
// and its cseq holds the floor.
func (g *flushGroup) failed() {
	if g == nil {
		return
	}
	var gone []CoveragePiece
	for _, p := range g.pieces {
		if !p.replayable {
			gone = append(gone, p)
		}
	}
	if len(gone) > 0 {
		metrics.Get().IncWALCoverageLost(int64(len(gone)))
		g.b.logger.Error().Str("buffer_key", g.key).Int("batches", len(gone)).
			Msg("Flush failed for batches with no complete WAL copy; those rows are lost and the registered floor of this key stops below them")
		g.cs.tracker.settleKey(g.key, gone, false)
	}
}

// coverageTracker computes the registered floor per key for one instance.
//
// Settled and lost cseqs above the floor are kept as interval sets: groups
// settle contiguous ranges, so a set stays as small as the number of groups
// in flight, and stays bounded after a lost batch stops a floor for good.
type coverageTracker struct {
	instance uint64

	mu   sync.Mutex
	keys map[string]*keyFloor

	mintedTotal atomic.Int64
	// doneTotal counts settled cseqs, at or above the floor: the lag gauge is
	// minted minus settled, so it keeps measuring work in flight even after a
	// lost batch has stopped a floor.
	doneTotal atomic.Int64
}

type keyFloor struct {
	floor   uint64
	done    intervalSet // whole cseqs settled above the floor
	lost    intervalSet // cseqs that will never settle
	partial map[uint64]*partialCSeq
}

type partialCSeq struct {
	chunks int
	rows   map[uint16]*rowSpans
}

type rowSpans struct {
	total int
	spans [][2]int // merged, sorted, half-open
}

// intervalSet is a sorted list of disjoint, non-adjacent closed intervals.
type intervalSet [][2]uint64

func (s intervalSet) contains(x uint64) bool {
	i := sort.Search(len(s), func(i int) bool { return s[i][1] >= x })
	return i < len(s) && s[i][0] <= x
}

// add inserts x and reports whether it was new.
func (s *intervalSet) add(x uint64) bool {
	if s.contains(x) {
		return false
	}
	v := *s
	i := sort.Search(len(v), func(i int) bool { return v[i][0] > x })
	joinLeft := i > 0 && v[i-1][1]+1 == x
	joinRight := i < len(v) && v[i][0] == x+1
	switch {
	case joinLeft && joinRight:
		v[i-1][1] = v[i][1]
		v = append(v[:i], v[i+1:]...)
	case joinLeft:
		v[i-1][1] = x
	case joinRight:
		v[i][0] = x
	default:
		v = append(v, [2]uint64{})
		copy(v[i+1:], v[i:])
		v[i] = [2]uint64{x, x}
	}
	*s = v
	return true
}

func newCoverageTracker(instance uint64) *coverageTracker {
	return &coverageTracker{instance: instance, keys: make(map[string]*keyFloor)}
}

func (t *coverageTracker) minted() {
	t.mintedTotal.Add(1)
	t.publish()
}

func (t *coverageTracker) publish() {
	metrics.Get().SetWALRegisteredFloorLag(t.mintedTotal.Load() - t.doneTotal.Load())
}

// settleKey records the pieces of one key's flush group. ok=false marks them
// lost: they never complete, so the floor of the key stops below them.
func (t *coverageTracker) settleKey(key string, pieces []CoveragePiece, ok bool) {
	t.mu.Lock()
	kf := t.keys[key]
	if kf == nil {
		kf = &keyFloor{partial: map[uint64]*partialCSeq{}}
		t.keys[key] = kf
	}
	var settled int64
	for _, p := range pieces {
		if p.Instance != t.instance || p.CSeq <= kf.floor || kf.done.contains(p.CSeq) || kf.lost.contains(p.CSeq) {
			continue // previous instances are tracked from S4 on
		}
		if !ok {
			kf.lost.add(p.CSeq)
			delete(kf.partial, p.CSeq)
			continue
		}
		if p.whole() || kf.addPartial(p) {
			delete(kf.partial, p.CSeq)
			if kf.done.add(p.CSeq) {
				settled++
			}
		}
	}
	// Advance through settled cseqs; a lost one is never in done, so the
	// floor stops below it.
	if len(kf.done) > 0 && kf.done[0][0] == kf.floor+1 {
		kf.floor = kf.done[0][1]
		kf.done = kf.done[1:]
	}
	t.doneTotal.Add(settled)
	t.mu.Unlock()
	t.publish()
}

// addPartial folds a chunk/row-range piece in and reports whether its cseq is
// now complete.
func (kf *keyFloor) addPartial(p CoveragePiece) bool {
	pc := kf.partial[p.CSeq]
	if pc == nil {
		chunks := int(p.Chunks)
		if chunks == 0 {
			chunks = 1
		}
		pc = &partialCSeq{chunks: chunks, rows: map[uint16]*rowSpans{}}
		kf.partial[p.CSeq] = pc
	}
	rs := pc.rows[p.Chunk]
	if rs == nil {
		rs = &rowSpans{}
		pc.rows[p.Chunk] = rs
	}
	if p.RowTotal == 0 {
		rs.total, rs.spans = 1, [][2]int{{0, 1}}
	} else {
		rs.total = p.RowTotal
		rs.add(p.RowLo, p.RowHi)
	}
	if len(pc.rows) < pc.chunks {
		return false
	}
	for _, r := range pc.rows {
		if len(r.spans) != 1 || r.spans[0][0] != 0 || r.spans[0][1] < r.total {
			return false
		}
	}
	return true
}

func (rs *rowSpans) add(lo, hi int) {
	rs.spans = append(rs.spans, [2]int{lo, hi})
	sort.Slice(rs.spans, func(i, j int) bool { return rs.spans[i][0] < rs.spans[j][0] })
	merged := rs.spans[:1]
	for _, s := range rs.spans[1:] {
		last := &merged[len(merged)-1]
		if s[0] <= last[1] {
			if s[1] > last[1] {
				last[1] = s[1]
			}
			continue
		}
		merged = append(merged, s)
	}
	rs.spans = merged
}

func (t *coverageTracker) progress() map[string]KeyRegistration {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]KeyRegistration, len(t.keys))
	for k, kf := range t.keys {
		r := KeyRegistration{Floor: kf.floor}
		for i, iv := range kf.done {
			if i == maxReportedSettled {
				break
			}
			r.Settled = append(r.Settled, iv)
		}
		for _, iv := range kf.lost {
			for c := iv[0]; c <= iv[1] && len(r.Lost) < maxReportedLost; c++ {
				r.Lost = append(r.Lost, c)
			}
		}
		out[k] = r
	}
	return out
}

func (t *coverageTracker) floors() map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]uint64, len(t.keys))
	for k, kf := range t.keys {
		out[k] = kf.floor
	}
	return out
}

// groupCoverage is the coverage handed to the registrar with each of a
// group's files, or nil when the flush is not covered.
func groupCoverage(g *flushGroup, files int) *GroupCoverage {
	if g == nil {
		return nil
	}
	return g.coverage(files)
}

// commitCoverageLocked mints the batch's coverage id and commits its prepared
// WAL entry with it, or attaches a replayed entry's coverage. Caller holds
// shard.mu and appends the batch under the same acquisition. Returns the WAL
// commit error for the caller to record after unlocking; the batch commits
// either way — the WAL is for durability, not correctness.
func (b *ArrowBuffer) commitCoverageLocked(cs *coverageState, covered bool, prep *wal.PreparedAppend, shard *bufferShard, key string, batch *TypedColumnBatch, replay []CoveragePiece) error {
	if replay != nil {
		batch.Coverage = replay
		return nil
	}
	if !covered {
		return nil
	}
	cseq := b.nextCSeqLocked(cs, shard, key)
	piece := CoveragePiece{Instance: cs.instance, CSeq: cseq}
	var err error
	if prep != nil {
		var tokens []string
		tokens, err = prep.Commit(cseq)
		batch.WALHashes = tokens
		piece.replayable = err == nil
	}
	batch.Coverage = []CoveragePiece{piece}
	return err
}
