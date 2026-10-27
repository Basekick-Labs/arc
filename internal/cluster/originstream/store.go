package originstream

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// Store is a standby writer's copy of the batches its primaries committed
// (#1207). Layout:
//
//	<root>/<origin node>/<instance hex>/seg-<n>.recv   records
//	<root>/<origin node>/<instance hex>/floors.json    last accepted floors
//
// A record is [len u32][crc u32][cseq u64][chunk u16][chunks u16][keyLen u16][key][payload],
// len and crc covering everything after the crc. Nothing here is ever
// applied to a buffer or the local WAL.
//
// Trimming: a segment is discarded only when every record in it is in a
// registered file (at or below its key's floor, or in a settled interval
// above it) and the standby holds the files — see TrimReadiness. A lost batch
// (settled with no file) is kept: the standby may hold the only copy.
//
// The cap: past it, an instance that its origin has since replaced (the
// primary restarted, so its own recovery took care of that instance's
// unflushed rows) is evicted oldest first; if that is not enough, nothing is
// stored and the batches are recorded as gaps until trimming frees room.
type Store struct {
	root     string
	maxBytes int64
	segBytes int64
	logger   zerolog.Logger

	// syncMu serializes whole Sync calls (the session's ticker, its final
	// sync, Close), so a Sync returns only once every append before it is
	// durable even while another one is still writing. Lock order: syncMu,
	// then mu; nothing fsyncs while holding mu (#1287): the receiving
	// session appends under mu, and a primary drops what a standby does not
	// read in time.
	syncMu sync.Mutex
	// fsync syncs a file; nil is (*os.File).Sync. Tests park it.
	fsync func(*os.File) error
	// removeAll removes a renamed-away path; nil is os.RemoveAll. Tests
	// observe it.
	removeAll func(string) error

	mu sync.Mutex
	// Durability work deferred to the next Sync, done without mu: segment
	// files rolled over (synced, then closed), directories with a new entry,
	// floors received (written, then applied), and renamed-away paths to
	// remove (also removed by Trim after it unlocks).
	retired       map[*segment]*os.File
	dirsToSync    map[string]bool
	pendingFloors map[*instanceDir]*Floors
	unlink        []string
	// forgotten: files closed because their segment was removed; an fsync
	// of one may fail closed, which is not an error. Reset per Sync.
	forgotten map[*os.File]bool
	trimSeq   int
	closed    bool

	instances   map[instanceID]*instanceDir
	current     map[string]uint64 // origin -> instance of its latest session
	bytes       int64
	capped      bool
	lastWarn    time.Time
	lastPin     time.Time
	lastPinCalc time.Time
	// evictable, when set (stream takeover on), decides which instances the
	// cap may evict; see SetEvictable.
	evictable   EvictPolicy
	evictActive func() bool
	// lowSpace: the filesystem holding the store is below its free-space
	// reserve; new batches are dropped (recorded as gaps) until it recovers
	// (#1266). Set by SetLowSpace.
	lowSpace bool
	// trimWanted asks the trim loop for a pass now: the store just crossed
	// half its cap (#1266).
	trimWanted chan struct{}
}

type instanceID struct {
	origin   string
	instance uint64
}

type instanceDir struct {
	id     instanceID
	dir    string
	segs   []*segment
	keys   map[string]*keyState
	floors *Floors
	cur    *os.File
	dirty  bool
	// rollNext: an fsync of cur failed; its pages may be lost, and a later
	// fsync would not say so. The next append starts a new segment.
	rollNext bool
	nextN    int
	created  time.Time
}

type segment struct {
	n     int
	path  string
	size  int64
	cseqs map[string]intervalSet // per key, cseqs with at least one record here
}

type keyState struct {
	last      uint64      // every cseq at or below is accounted for: received whole, or in gaps
	open      uint64      // cseq whose chunks are arriving; 0 = none
	nextChunk uint16      // next chunk expected for open
	gaps      intervalSet // cseqs this standby is missing, at least in part
}

// TrimKey is one (instance, key) of a primary's stream.
type TrimKey struct {
	Origin       string
	Instance     uint64
	Key          string
	AppliedIndex uint64
}

// TrimRequest asks whether the batches CSeqs of one key, all registered per
// the floors, may be dropped: whether the files holding their rows are on
// this node, given that the floors are complete as of AppliedIndex (#1266).
type TrimRequest struct {
	TrimKey
	CSeqs [][2]uint64
}

// TrimReadiness answers each request, in order. Called without the store
// lock, once per pass, with one request per (segment, key) to trim. The
// coordinator implements it.
type TrimReadiness func(ctx context.Context, reqs []TrimRequest) []bool

const (
	defaultSegmentBytes = 64 << 20
	recordFixed         = 4 + 4 + 8 + 2 + 2 + 2
	floorsFile          = "floors.json"
	// trimSuffix marks a segment or instance directory renamed away under
	// the lock, to be removed after it (#1287).
	trimSuffix     = ".trim"
	resumeFraction = 0.9
)

// OpenStore opens the store at root, rebuilding its index from disk. A
// record that does not read back cleanly ends its segment: the segment is
// truncated there and what followed is treated as missing.
func OpenStore(root string, maxBytes int64, logger zerolog.Logger) (*Store, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create standby store: %w", err)
	}
	s := &Store{root: root, maxBytes: maxBytes, segBytes: defaultSegmentBytes, logger: logger,
		instances: map[instanceID]*instanceDir{}, current: map[string]uint64{}, trimWanted: make(chan struct{}, 1),
		retired: map[*segment]*os.File{}, dirsToSync: map[string]bool{}, pendingFloors: map[*instanceDir]*Floors{},
		forgotten: map[*os.File]bool{}}
	metrics.Get().SetStandbyStoreMaxBytes(maxBytes)
	origins, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, o := range origins {
		if !o.IsDir() {
			continue
		}
		insts, err := os.ReadDir(filepath.Join(root, o.Name()))
		if err != nil {
			return nil, err
		}
		for _, in := range insts {
			if strings.HasSuffix(in.Name(), trimSuffix) {
				// Dropped before a restart; its removal did not finish.
				_ = os.RemoveAll(filepath.Join(root, o.Name(), in.Name()))
				continue
			}
			inst, err := strconv.ParseUint(in.Name(), 16, 64)
			if err != nil || !in.IsDir() {
				continue
			}
			d, err := s.loadInstance(instanceID{o.Name(), inst}, filepath.Join(root, o.Name(), in.Name()))
			if err != nil {
				return nil, err
			}
			s.instances[d.id] = d
		}
	}
	s.publish()
	return s, nil
}

func (s *Store) loadInstance(id instanceID, dir string) (*instanceDir, error) {
	d := &instanceDir{id: id, dir: dir, keys: map[string]*keyState{}, created: time.Now()}
	if info, err := os.Stat(dir); err == nil {
		d.created = info.ModTime()
	}
	if raw, err := os.ReadFile(filepath.Join(dir, floorsFile)); err == nil {
		var f Floors
		if json.Unmarshal(raw, &f) == nil {
			sortFloors(&f)
			d.floors = &f
			// Start each key from its floor: everything at or below it is
			// registered, and segments trimmed before the restart must not
			// read back as gaps.
			for key, kf := range f.Keys {
				d.keys[key] = &keyState{last: kf.Floor}
			}
		}
	}
	if stale, _ := filepath.Glob(filepath.Join(dir, "*"+trimSuffix)); len(stale) > 0 {
		// Trimmed or evicted before a restart; the removal did not finish.
		for _, p := range stale {
			_ = os.Remove(p)
		}
	}
	names, err := filepath.Glob(filepath.Join(dir, "seg-*.recv"))
	if err != nil {
		return nil, err
	}
	sort.Slice(names, func(i, j int) bool { return segNum(names[i]) < segNum(names[j]) })
	for _, name := range names {
		seg := &segment{n: segNum(name), path: name, cseqs: map[string]intervalSet{}}
		size, damaged, err := scanSegment(name, func(key string, cseq uint64, chunk, chunks uint16) {
			seg.cseqs[key] = seg.cseqs[key].with(cseq)
			d.observe(key, cseq, chunk, chunks, false)
		})
		if err != nil {
			return nil, err
		}
		if damaged {
			s.logger.Warn().Str("segment", name).Int64("kept_bytes", size).
				Msg("Standby store segment has a damaged record; truncated there and the following batches are treated as missing")
			if err := os.Truncate(name, size); err != nil {
				return nil, err
			}
			for _, k := range d.keys {
				k.addGap(k.last+1, k.last+1, false)
			}
		}
		seg.size = size
		s.bytes += size
		d.segs = append(d.segs, seg)
		if seg.n >= d.nextN {
			d.nextN = seg.n + 1
		}
	}
	// Gaps the floors already cover are not gaps (trimmed through a settled
	// interval before the restart).
	d.dropRegisteredGaps()
	return d, nil
}

func segNum(path string) int {
	b := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "seg-"), ".recv")
	n, _ := strconv.Atoi(b)
	return n
}

// scanSegment reads every whole, intact record and returns the size up to
// the last one, and whether something after it did not read back.
func scanSegment(path string, fn func(key string, cseq uint64, chunk, chunks uint16)) (int64, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return off, err != io.EOF, nil
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		if n < recordFixed-8 || n > maxFrame {
			return off, true, nil
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return off, true, nil
		}
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(hdr[4:]) {
			return off, true, nil
		}
		keyLen := int(binary.BigEndian.Uint16(body[12:]))
		if 14+keyLen > len(body) {
			return off, true, nil
		}
		fn(string(body[14:14+keyLen]), binary.BigEndian.Uint64(body[0:]), binary.BigEndian.Uint16(body[8:]), binary.BigEndian.Uint16(body[10:]))
		off += 8 + int64(n)
	}
}

// observe advances a key's gap state with one arriving record. The primary
// sends a key's batches in cseq order and each batch's chunks in order, so
// anything skipped never arrived. live is false while rebuilding from disk:
// those gaps were counted when they happened.
func (d *instanceDir) observe(key string, cseq uint64, chunk, chunks uint16, live bool) {
	k := d.keys[key]
	if k == nil {
		k = &keyState{}
		d.keys[key] = k
	}
	if cseq <= k.last {
		return
	}
	if k.open != 0 && cseq == k.open {
		if chunk != k.nextChunk {
			k.addGap(cseq, cseq, live)
		}
	} else {
		if k.open != 0 {
			k.addGap(k.open, k.open, live) // its last chunks never came
			k.last = k.open
		}
		if cseq > k.last+1 {
			k.addGap(k.last+1, cseq-1, live)
		}
		if chunk != 0 {
			k.addGap(cseq, cseq, live)
		}
		k.open = cseq
	}
	k.nextChunk = chunk + 1
	if k.nextChunk >= chunks {
		k.last, k.open = cseq, 0
	}
}

func (k *keyState) addGap(lo, hi uint64, count bool) {
	k.gaps = k.gaps.withRange(lo, hi)
	if count {
		metrics.Get().IncStandbyStreamGaps()
	}
}

// dropRegisteredGaps forgets gaps the floors say are registered.
func (d *instanceDir) dropRegisteredGaps() {
	if d.floors == nil {
		return
	}
	for key, k := range d.keys {
		kf, ok := d.floors.Keys[key]
		if !ok {
			continue
		}
		var keep intervalSet
		for _, g := range k.gaps.above(kf.Floor) {
			if !kf.registeredRange(g[0], g[1]) {
				keep = append(keep, g)
			}
		}
		k.gaps = keep
	}
}

func (s *Store) instance(id instanceID) (*instanceDir, error) {
	if d := s.instances[id]; d != nil {
		return d, nil
	}
	dir := filepath.Join(s.root, id.origin, fmt.Sprintf("%016x", id.instance))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s.dirsToSync[filepath.Dir(dir)] = true
	d := &instanceDir{id: id, dir: dir, keys: map[string]*keyState{}, created: time.Now()}
	s.instances[id] = d
	return d, nil
}

// SetCurrent records origin's live instance. Every other instance of that
// origin may be evicted under the cap: the origin restarted, and its own WAL
// recovery has (or will) register what that instance left unflushed.
func (s *Store) SetCurrent(origin string, instance uint64) {
	s.mu.Lock()
	s.current[origin] = instance
	s.mu.Unlock()
}

// Append stores one entry. Past the byte cap it first evicts replaced
// instances; if that is not enough it stores nothing and marks the batch
// missing. Storing resumes under 90 % of the cap.
func (s *Store) Append(origin string, e *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	d, err := s.instance(instanceID{origin, e.Instance})
	if err != nil {
		return err
	}
	if !validEntry(e) {
		return fmt.Errorf("standby store: entry for %q cannot be stored (key %d bytes, payload %d bytes)", e.Key, len(e.Key), len(e.Payload))
	}
	rec := encodeRecord(e)
	if s.lowSpace {
		// Below the free-space reserve: the volume is shared with the WAL and
		// the Parquet files, and the store is what gives way (#1266).
		s.dropLocked(d, e)
		return nil
	}
	if s.maxBytes > 0 && s.bytes < s.maxBytes/2 && s.bytes+int64(len(rec)) >= s.maxBytes/2 {
		select {
		case s.trimWanted <- struct{}{}:
		default:
		}
	}
	if s.maxBytes > 0 {
		if s.bytes+int64(len(rec)) > s.maxBytes {
			s.evictLocked(int64(resumeFraction * float64(s.maxBytes)))
		}
		s.recheckCappedLocked()
		if !s.capped && s.bytes+int64(len(rec)) > s.maxBytes {
			s.capped = true
			metrics.Get().SetStandbyStoreCapped(true)
			s.logger.Warn().Int64("bytes", s.bytes).Int64("max_bytes", s.maxBytes).
				Msg("Standby store reached cluster.standby_store_max_bytes; batches are not kept until it is trimmed (alert on arc_standby_store_dropped_total)")
		}
		if s.capped {
			if policy := s.policyLocked(); policy != nil {
				s.pinnedWarnLocked(policy)
			}
			s.dropLocked(d, e)
			return nil
		}
	}
	seg, err := s.currentSegment(d)
	if err != nil {
		return err
	}
	if _, err := d.cur.Write(rec); err != nil {
		// Cut the torn record off so the segment reads back clean, and
		// start the next append in a new segment.
		_ = d.cur.Truncate(seg.size)
		s.retireLocked(seg, d)
		if errors.Is(err, syscall.ENOSPC) {
			// A full disk is a cap the operator did not set: keep the
			// session, record the batch as missing.
			s.warnLocked(err, "Standby store disk is full; batches are not kept until it is trimmed")
			s.dropLocked(d, e)
			return nil
		}
		return fmt.Errorf("standby store write: %w", err)
	}
	seg.size += int64(len(rec))
	seg.cseqs[e.Key] = seg.cseqs[e.Key].with(e.CSeq)
	s.bytes += int64(len(rec))
	d.dirty = true
	d.observe(e.Key, e.CSeq, e.Chunk, e.Chunks, true)
	metrics.Get().IncStandbyStreamEntries()
	s.publishLocked()
	return nil
}

func (s *Store) dropLocked(d *instanceDir, e *Entry) {
	k := d.keys[e.Key]
	if k == nil {
		k = &keyState{}
		d.keys[e.Key] = k
	}
	if e.CSeq > k.last {
		if e.CSeq > k.last+1 {
			k.addGap(k.last+1, e.CSeq-1, false)
		}
		k.addGap(e.CSeq, e.CSeq, true)
		k.last, k.open = e.CSeq, 0
	}
	metrics.Get().IncStandbyStoreDropped()
	s.publishLocked()
}

func (s *Store) warnLocked(err error, msg string) {
	if time.Since(s.lastWarn) < 10*time.Second {
		return
	}
	s.lastWarn = time.Now()
	s.logger.Warn().Err(err).Msg(msg)
}

// evictLocked frees room under the cap until the store is at or under
// target. Without a policy: instances whose origin has a newer current
// instance, oldest first. With one (stream takeover): the instances it
// allows, then those it allows only under pressure, oldest first in each.
func (s *Store) evictLocked(target int64) {
	policy := s.policyLocked()
	if policy == nil {
		var victims []*instanceDir
		for id, d := range s.instances {
			if s.replacedLocked(id) && len(d.segs) > 0 {
				victims = append(victims, d)
			}
		}
		s.evictDirsLocked(victims, target)
		return
	}
	tiers := [2][]*instanceDir{}
	for id, d := range s.instances {
		if len(d.segs) == 0 {
			continue
		}
		switch policy(s.instanceInfoLocked(id, d)) {
		case EvictFirst:
			tiers[0] = append(tiers[0], d)
		case EvictUnderPressure:
			tiers[1] = append(tiers[1], d)
		}
	}
	s.evictDirsLocked(tiers[0], target)
	s.evictDirsLocked(tiers[1], target)
}

func (s *Store) replacedLocked(id instanceID) bool {
	cur, ok := s.current[id.origin]
	return ok && cur != id.instance
}

func (s *Store) evictDirsLocked(victims []*instanceDir, target int64) {
	sort.Slice(victims, func(i, j int) bool { return victims[i].created.Before(victims[j].created) })
	for _, d := range victims {
		if s.bytes <= target {
			return
		}
		s.forgetLocked(d, d.segs...)
		var freed int64
		for _, seg := range d.segs {
			if err := s.removeLocked(seg.path); err == nil {
				freed += seg.size
			}
		}
		d.segs = nil
		s.bytes -= freed
		metrics.Get().AddStandbyStoreEvictedBytes(freed)
		s.logger.Warn().Str("origin", d.id.origin).Str("instance", fmt.Sprintf("%016x", d.id.instance)).Int64("bytes", freed).
			Msg("Standby store evicted the batches of a primary instance to stay under its cap")
	}
}

// encodeRecord frames e for a segment. The caller has checked validEntry, so
// the size arithmetic stays far below overflow.
func encodeRecord(e *Entry) []byte {
	n := recordFixed - 8 + len(e.Key) + len(e.Payload)
	rec := make([]byte, 8+n)
	body := rec[8:]
	binary.BigEndian.PutUint64(body[0:], e.CSeq)
	binary.BigEndian.PutUint16(body[8:], e.Chunk)
	binary.BigEndian.PutUint16(body[10:], e.Chunks)
	binary.BigEndian.PutUint16(body[12:], uint16(len(e.Key)))
	copy(body[14:], e.Key)
	copy(body[14+len(e.Key):], e.Payload)
	binary.BigEndian.PutUint32(rec[0:], uint32(n))
	binary.BigEndian.PutUint32(rec[4:], crc32.ChecksumIEEE(body))
	return rec
}

func (s *Store) currentSegment(d *instanceDir) (*segment, error) {
	if d.cur != nil && !d.rollNext && len(d.segs) > 0 && d.segs[len(d.segs)-1].size < s.segBytes {
		return d.segs[len(d.segs)-1], nil
	}
	if d.cur != nil && len(d.segs) > 0 {
		s.retireLocked(d.segs[len(d.segs)-1], d)
	}
	d.rollNext = false
	seg := &segment{n: d.nextN, path: filepath.Join(d.dir, fmt.Sprintf("seg-%08d.recv", d.nextN)), cseqs: map[string]intervalSet{}}
	f, err := os.OpenFile(seg.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	s.dirsToSync[d.dir] = true
	d.nextN++
	d.cur = f
	d.segs = append(d.segs, seg)
	return seg, nil
}

func syncDir(dir string) {
	if f, err := os.Open(dir); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
}

// retireLocked hands d's open file, which holds seg, to the next Sync, which
// syncs and closes it. Always, even with nothing dirty: a Sync in flight may
// still owe it an fsync (it clears dirty before it syncs), and syncing a
// clean file is cheap.
func (s *Store) retireLocked(seg *segment, d *instanceDir) {
	s.retired[seg] = d.cur
	d.cur, d.dirty = nil, false
}

// forgetLocked closes, without syncing, the files of d's segments being
// removed: a retired one, and the open one if it is among them.
func (s *Store) forgetLocked(d *instanceDir, segs ...*segment) {
	for _, seg := range segs {
		if f, ok := s.retired[seg]; ok {
			_ = f.Close()
			s.forgotten[f] = true
			delete(s.retired, seg)
		}
		if d.cur != nil && len(d.segs) > 0 && d.segs[len(d.segs)-1] == seg {
			_ = d.cur.Close()
			s.forgotten[d.cur] = true
			d.cur, d.dirty = nil, false
		}
	}
}

// removeLocked renames path away (a metadata operation) so its name can be
// reused at once and a restart deletes it, and queues the removal for after
// the lock.
func (s *Store) removeLocked(path string) error {
	// Unique: a directory left by an earlier removal that failed must not
	// make this rename fail.
	s.trimSeq++
	gone := fmt.Sprintf("%s.%d%s", path, s.trimSeq, trimSuffix)
	if err := os.Rename(path, gone); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	s.unlink = append(s.unlink, gone)
	return nil
}

func (s *Store) drainUnlink() {
	s.mu.Lock()
	paths := s.unlink
	s.unlink = nil
	s.mu.Unlock()
	for _, p := range paths {
		remove := os.RemoveAll
		if s.removeAll != nil {
			remove = s.removeAll
		}
		if err := remove(p); err != nil {
			// Left for the next open, which removes it.
			s.logger.Warn().Err(err).Str("path", p).Msg("Standby store could not remove a trimmed file")
		}
	}
}

func (s *Store) syncFile(f *os.File) error {
	if s.fsync != nil {
		return s.fsync(f)
	}
	return f.Sync()
}

// Sync makes every append so far durable, writes and then applies the
// floors received, and removes what was renamed away. Called on a short
// ticker by the receiving session: the primary's own WAL is asynchronous, so
// a per-entry fsync here would buy nothing. It fsyncs without the store
// lock, so appends go on meanwhile (#1287).
func (s *Store) Sync() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	s.mu.Lock()
	s.forgotten = map[*os.File]bool{} // no file of an earlier Sync is in flight
	retired := s.retired
	s.retired = map[*segment]*os.File{}
	dirs := s.dirsToSync
	s.dirsToSync = map[string]bool{}
	type open struct {
		d *instanceDir
		f *os.File
	}
	var opens []open
	for _, d := range s.instances {
		if d.dirty && d.cur != nil {
			// Cleared first: an append from here on is for the next Sync.
			opens = append(opens, open{d, d.cur})
			d.dirty = false
		}
	}
	floors := s.pendingFloors
	s.pendingFloors = map[*instanceDir]*Floors{}
	s.mu.Unlock()

	var first error
	// A file closed meanwhile because its segment was trimmed, evicted or
	// dropped no longer needs its records; any other failure is one.
	failed := func(f *os.File, err error) bool {
		if err == nil {
			return false
		}
		if errors.Is(err, os.ErrClosed) {
			s.mu.Lock()
			gone := s.forgotten[f]
			s.mu.Unlock()
			if gone {
				return false
			}
		}
		if first == nil {
			first = err
		}
		return true
	}
	for _, f := range retired {
		failed(f, s.syncFile(f))
		_ = f.Close()
	}
	for dir := range dirs {
		syncDir(dir)
	}
	for _, o := range opens {
		if failed(o.f, s.syncFile(o.f)) {
			s.mu.Lock()
			if o.d.cur == o.f {
				o.d.rollNext = true
			}
			s.mu.Unlock()
		}
	}
	for d, f := range floors {
		if err := s.writeFloors(d, f); err != nil {
			s.mu.Lock()
			live := s.instances[d.id] == d // a dropped instance needs none
			if live && s.pendingFloors[d] == nil {
				s.pendingFloors[d] = f // written again next time
			}
			s.mu.Unlock()
			if live {
				s.logger.Warn().Err(err).Str("origin", d.id.origin).Msg("Standby store could not record the primary's floors; retrying (trimming waits for them)")
			}
		}
	}
	s.drainUnlink()
	return first
}

// writeFloors makes f durable for d, then applies it: Trim trims only
// against floors a restart reads back.
func (s *Store) writeFloors(d *instanceDir, f *Floors) error {
	s.mu.Lock()
	live := s.instances[d.id] == d
	s.mu.Unlock()
	if !live {
		return nil // dropped: not written into a directory made again since
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	tmp := filepath.Join(d.dir, floorsFile+".tmp")
	if err := s.writeFileSync(tmp, raw); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(d.dir, floorsFile)); err != nil {
		return err
	}
	syncDir(d.dir)
	sortFloors(f)
	s.mu.Lock()
	if s.instances[d.id] == d {
		d.floors = f
	}
	s.mu.Unlock()
	return nil
}

// AcceptFloors records the primary's floors for its instance. They are
// written by the next Sync and apply once durable; a newer set replaces one
// not written yet. Nothing here touches the disk but a new instance's
// directory: the receiving session calls it once a second (#1287).
func (s *Store) AcceptFloors(origin string, f *Floors) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.instance(instanceID{origin, f.Instance})
	if err != nil {
		return err
	}
	s.pendingFloors[d] = f
	return nil
}

// sortFloors sorts each key's settled intervals once, for registeredRange,
// which runs per segment and key on every trim pass.
func sortFloors(f *Floors) {
	for _, kf := range f.Keys {
		sort.Slice(kf.Settled, func(i, j int) bool { return kf.Settled[i][0] < kf.Settled[j][0] })
	}
}

func (s *Store) writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := s.syncFile(f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

type trimCandidate struct {
	d    *instanceDir
	seg  *segment
	reqs []int // indexes into the pass's requests
	// size when the candidate was chosen. Readiness runs without the lock and
	// the primary keeps streaming into the open segment: a segment that grew
	// meanwhile holds batches nobody vetted (not registered yet) and is kept
	// (#1266).
	size int64
}

// Trim discards every segment whose records are all registered and held.
// Three phases, so the store lock is never held across readiness: collect
// the segments the floors cover, ask readiness for their keys unlocked, then
// delete those still present whose keys are all ready.
func (s *Store) Trim(ctx context.Context, ready TrimReadiness) (int64, error) {
	defer s.drainUnlink() // after the lock below is released
	s.mu.Lock()
	var cands []trimCandidate
	var reqs []TrimRequest
	lost := 0
	for _, d := range s.instances {
		if d.floors == nil {
			continue
		}
		lost += d.lostRecords()
		for _, seg := range d.segs {
			keys, ok := d.covered(seg)
			if !ok {
				continue
			}
			c := trimCandidate{d: d, seg: seg, size: seg.size}
			for _, k := range keys {
				// Copied: the open segment's intervals grow under the lock.
				ivs := append([][2]uint64(nil), seg.cseqs[k.Key]...)
				c.reqs = append(c.reqs, len(reqs))
				reqs = append(reqs, TrimRequest{TrimKey: k, CSeqs: ivs})
			}
			cands = append(cands, c)
		}
	}
	s.mu.Unlock()

	var answers []bool
	blocked := 0
	if len(reqs) > 0 {
		answers = ready(ctx, reqs)
		if len(answers) != len(reqs) {
			answers = nil // a malformed answer trims nothing
		}
		// Blocked: measurements (per origin) with a registered batch that
		// may not go yet, whatever holds it: a file not here, this FSM
		// behind, a claim or an origin not heard from.
		type mk struct{ origin, key string }
		waiting := map[mk]bool{}
		for i, r := range reqs {
			if answers == nil || !answers[i] {
				waiting[mk{r.Origin, r.Key}] = true
			}
		}
		blocked = len(waiting)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var freed int64
	for _, c := range cands {
		allReady := answers != nil
		for _, i := range c.reqs {
			if !allReady || !answers[i] {
				allReady = false
				break
			}
		}
		if !allReady || !c.d.holds(c.seg) || c.seg.size != c.size {
			continue
		}
		s.forgetLocked(c.d, c.seg)
		if err := s.removeLocked(c.seg.path); err != nil {
			continue
		}
		c.d.remove(c.seg)
		freed += c.seg.size
		s.bytes -= c.seg.size
	}
	for _, d := range s.instances {
		d.dropRegisteredGaps()
	}
	if s.maxBytes > 0 {
		s.recheckCappedLocked()
	}
	metrics.Get().AddStandbyStoreTrimmedBytes(freed)
	metrics.Get().SetStandbyStoreTrimState(int64(lost), int64(blocked), s.replacedBytesLocked())
	s.publishLocked()
	return freed, nil
}

// covered reports whether every record of seg is registered per the floors,
// and the keys readiness must vouch for.
func (d *instanceDir) covered(seg *segment) ([]TrimKey, bool) {
	keys := make([]TrimKey, 0, len(seg.cseqs))
	for key, ivs := range seg.cseqs {
		kf, ok := d.floors.Keys[key]
		if !ok {
			return nil, false
		}
		for _, iv := range ivs {
			if !kf.registeredRange(iv[0], iv[1]) {
				return nil, false
			}
		}
		keys = append(keys, TrimKey{Origin: d.id.origin, Instance: d.id.instance, Key: key, AppliedIndex: d.floors.AppliedIndex})
	}
	return keys, true
}

func (d *instanceDir) holds(seg *segment) bool {
	for _, s := range d.segs {
		if s == seg {
			return true
		}
	}
	return false
}

func (d *instanceDir) remove(seg *segment) {
	out := make([]*segment, 0, len(d.segs))
	for _, s := range d.segs {
		if s != seg {
			out = append(out, s)
		}
	}
	d.segs = out
}

// lostRecords counts batches held here that the primary reported lost.
func (d *instanceDir) lostRecords() int {
	n := 0
	for key, kf := range d.floors.Keys {
		for _, c := range kf.Lost {
			for _, seg := range d.segs {
				if seg.cseqs[key].contains(c) {
					n++
					break
				}
			}
		}
	}
	return n
}

func (s *Store) replacedBytesLocked() int64 {
	var n int64
	for id, d := range s.instances {
		if cur, ok := s.current[id.origin]; ok && cur != id.instance {
			for _, seg := range d.segs {
				n += seg.size
			}
		}
	}
	return n
}

// registeredRange reports whether every cseq in [lo, hi] is at or below the
// floor or in a settled interval. Settled is sorted when the floors are
// accepted or loaded (sortFloors); unsorted, it can only answer false.
func (kf KeyFloor) registeredRange(lo, hi uint64) bool {
	cur := lo
	if cur <= kf.Floor {
		cur = kf.Floor + 1
	}
	for _, iv := range kf.Settled {
		if cur > hi {
			break
		}
		if iv[0] <= cur && cur <= iv[1] {
			cur = iv[1] + 1
		}
	}
	return cur > hi
}

// Close syncs and closes open segments.
func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true // a session still running past its stop appends nothing more
	s.mu.Unlock()
	err := s.Sync()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.instances {
		if d.cur != nil {
			_ = d.cur.Close()
			d.cur = nil
		}
	}
	return err
}

var errStoreClosed = errors.New("standby store is closed")

// Stats is a snapshot for metrics and tests.
type Stats struct {
	Bytes          int64
	Instances      int
	Segments       int
	IncompleteKeys int
	Capped         bool
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statsLocked()
}

func (s *Store) statsLocked() Stats {
	st := Stats{Bytes: s.bytes, Instances: len(s.instances), Capped: s.capped}
	for _, d := range s.instances {
		st.Segments += len(d.segs)
		for _, k := range d.keys {
			if len(k.gaps) > 0 {
				st.IncompleteKeys++
			}
		}
	}
	return st
}

func (s *Store) publish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishLocked()
}

// recheckCappedLocked leaves the capped state once the store is back under
// 90 % of its cap. Called on every Append and after Trim and eviction, so the
// state clears when the primary has gone quiet too (#1266).
func (s *Store) recheckCappedLocked() {
	if s.capped && float64(s.bytes) < resumeFraction*float64(s.maxBytes) {
		s.capped = false
		metrics.Get().SetStandbyStoreCapped(false)
		metrics.Get().SetStandbyStorePinned(0)
		s.logger.Info().Int64("bytes", s.bytes).Msg("Standby store back under its cap; storing batches again")
	}
}

// TrimWanted fires when the store crosses half its cap: the trim loop runs a
// pass then rather than at its next tick (#1266).
func (s *Store) TrimWanted() <-chan struct{} { return s.trimWanted }

// SetLowSpace reports whether the store's filesystem is below its free-space
// reserve. Entering it first evicts what the eviction policy allows (as under
// the cap's pressure), then new batches are dropped until it leaves (#1266).
func (s *Store) SetLowSpace(low bool, availBytes, reserveBytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if low == s.lowSpace {
		return
	}
	s.lowSpace = low
	metrics.Get().SetStandbyStoreLowSpace(low)
	if low {
		s.evictLocked(0)
		if s.maxBytes > 0 {
			s.recheckCappedLocked()
		}
		s.logger.Warn().Uint64("available_bytes", availBytes).Uint64("reserve_bytes", reserveBytes).
			Msg("The standby store's volume is below its free-space reserve; new batches are not kept until space returns (the WAL and Parquet files on the same volume come first)")
	} else {
		s.logger.Info().Uint64("available_bytes", availBytes).Msg("Standby store volume back above its free-space reserve; storing batches again")
	}
	s.publishLocked()
}

func (s *Store) publishLocked() {
	st := s.statsLocked()
	metrics.Get().SetStandbyStore(st.Bytes, int64(st.IncompleteKeys))
}

// intervalSet is a sorted list of disjoint, non-adjacent closed intervals.
type intervalSet [][2]uint64

func (v intervalSet) with(x uint64) intervalSet { return v.withRange(x, x) }

func (v intervalSet) contains(x uint64) bool {
	i := sort.Search(len(v), func(i int) bool { return v[i][1] >= x })
	return i < len(v) && v[i][0] <= x
}

// above returns the part of v strictly above floor.
func (v intervalSet) above(floor uint64) intervalSet {
	out := make(intervalSet, 0, len(v))
	for _, iv := range v {
		if iv[1] <= floor {
			continue
		}
		if iv[0] <= floor {
			iv[0] = floor + 1
		}
		out = append(out, iv)
	}
	return out
}

// withRange adds [lo, hi].
func (v intervalSet) withRange(lo, hi uint64) intervalSet {
	// Fast path: at or past the end, the common case for in-order cseqs.
	if n := len(v); n == 0 || v[n-1][1]+1 < lo {
		return append(v, [2]uint64{lo, hi})
	} else if v[n-1][0] <= lo {
		if hi > v[n-1][1] {
			v[n-1][1] = hi
		}
		return v
	}
	out := make(intervalSet, 0, len(v)+1)
	placed := false
	for _, iv := range v {
		switch {
		case iv[1]+1 < lo:
			out = append(out, iv)
		case hi+1 < iv[0]:
			if !placed {
				out = append(out, [2]uint64{lo, hi})
				placed = true
			}
			out = append(out, iv)
		default: // overlaps or touches: absorb
			if iv[0] < lo {
				lo = iv[0]
			}
			if iv[1] > hi {
				hi = iv[1]
			}
		}
	}
	if !placed {
		out = append(out, [2]uint64{lo, hi})
	}
	return out
}
