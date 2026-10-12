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

	mu          sync.Mutex
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
}

type instanceID struct {
	origin   string
	instance uint64
}

type instanceDir struct {
	id      instanceID
	dir     string
	segs    []*segment
	keys    map[string]*keyState
	floors  *Floors
	cur     *os.File
	dirty   bool
	nextN   int
	created time.Time
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

// TrimKey is one (instance, key) a trim pass needs a readiness answer for.
type TrimKey struct {
	Origin       string
	Key          string
	AppliedIndex uint64
}

// TrimReadiness reports, for each key, whether every file of its measurement
// is present on this node, given that the primary's floors are complete as
// of the primary's applied index. Called without the store lock, once per
// pass for every key that has segments to trim. The coordinator implements it.
type TrimReadiness func(ctx context.Context, keys []TrimKey) map[TrimKey]bool

const (
	defaultSegmentBytes = 64 << 20
	recordFixed         = 4 + 4 + 8 + 2 + 2 + 2
	floorsFile          = "floors.json"
	resumeFraction      = 0.9
)

// OpenStore opens the store at root, rebuilding its index from disk. A
// record that does not read back cleanly ends its segment: the segment is
// truncated there and what followed is treated as missing.
func OpenStore(root string, maxBytes int64, logger zerolog.Logger) (*Store, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create standby store: %w", err)
	}
	s := &Store{root: root, maxBytes: maxBytes, segBytes: defaultSegmentBytes, logger: logger,
		instances: map[instanceID]*instanceDir{}, current: map[string]uint64{}}
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
			d.floors = &f
			// Start each key from its floor: everything at or below it is
			// registered, and segments trimmed before the restart must not
			// read back as gaps.
			for key, kf := range f.Keys {
				d.keys[key] = &keyState{last: kf.Floor}
			}
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
	syncDir(filepath.Dir(dir))
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
	d, err := s.instance(instanceID{origin, e.Instance})
	if err != nil {
		return err
	}
	if !validEntry(e) {
		return fmt.Errorf("standby store: entry for %q cannot be stored (key %d bytes, payload %d bytes)", e.Key, len(e.Key), len(e.Payload))
	}
	rec := encodeRecord(e)
	if s.maxBytes > 0 {
		if s.bytes+int64(len(rec)) > s.maxBytes {
			s.evictLocked(int64(resumeFraction * float64(s.maxBytes)))
		}
		if s.capped && float64(s.bytes) < resumeFraction*float64(s.maxBytes) {
			s.capped = false
			metrics.Get().SetStandbyStorePinned(0)
			s.logger.Info().Int64("bytes", s.bytes).Msg("Standby store back under its cap; storing batches again")
		}
		if !s.capped && s.bytes+int64(len(rec)) > s.maxBytes {
			s.capped = true
			s.logger.Warn().Int64("bytes", s.bytes).Int64("max_bytes", s.maxBytes).
				Msg("Standby store reached cluster.standby_store_max_bytes; batches are not kept until it is trimmed")
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
		_ = d.cur.Close()
		d.cur = nil
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
		if d.cur != nil {
			_ = d.cur.Close()
			d.cur = nil
		}
		var freed int64
		for _, seg := range d.segs {
			if err := os.Remove(seg.path); err == nil || errors.Is(err, os.ErrNotExist) {
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
	if d.cur != nil && len(d.segs) > 0 && d.segs[len(d.segs)-1].size < s.segBytes {
		return d.segs[len(d.segs)-1], nil
	}
	if d.cur != nil {
		_ = d.cur.Sync()
		_ = d.cur.Close()
		d.cur = nil
	}
	seg := &segment{n: d.nextN, path: filepath.Join(d.dir, fmt.Sprintf("seg-%08d.recv", d.nextN)), cseqs: map[string]intervalSet{}}
	f, err := os.OpenFile(seg.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	syncDir(d.dir)
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

// Sync makes every append so far durable. Called on a short ticker by the
// receiving session: the primary's own WAL is asynchronous, so a per-entry
// fsync here would buy nothing.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for _, d := range s.instances {
		if !d.dirty || d.cur == nil {
			continue
		}
		if err := d.cur.Sync(); err != nil && first == nil {
			first = err
		}
		d.dirty = false
	}
	return first
}

// AcceptFloors records the primary's floors for its instance durably.
func (s *Store) AcceptFloors(origin string, f *Floors) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.instance(instanceID{origin, f.Instance})
	if err != nil {
		return err
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	tmp := filepath.Join(d.dir, floorsFile+".tmp")
	if err := writeFileSync(tmp, raw); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(d.dir, floorsFile)); err != nil {
		return err
	}
	syncDir(d.dir)
	d.floors = f
	return nil
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

type trimCandidate struct {
	d    *instanceDir
	seg  *segment
	keys []TrimKey
}

// Trim discards every segment whose records are all registered and held.
// Three phases, so the store lock is never held across readiness: collect
// the segments the floors cover, ask readiness for their keys unlocked, then
// delete those still present whose keys are all ready.
func (s *Store) Trim(ctx context.Context, ready TrimReadiness) (int64, error) {
	s.mu.Lock()
	var cands []trimCandidate
	need := map[TrimKey]bool{}
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
			cands = append(cands, trimCandidate{d: d, seg: seg, keys: keys})
			for _, k := range keys {
				need[k] = true
			}
		}
	}
	s.mu.Unlock()

	var answers map[TrimKey]bool
	blocked := 0
	if len(need) > 0 {
		keys := make([]TrimKey, 0, len(need))
		for k := range need {
			keys = append(keys, k)
		}
		answers = ready(ctx, keys)
		for _, k := range keys {
			if !answers[k] {
				blocked++
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var freed int64
	for _, c := range cands {
		allReady := true
		for _, k := range c.keys {
			if !answers[k] {
				allReady = false
				break
			}
		}
		if !allReady || !c.d.holds(c.seg) {
			continue
		}
		if c.d.cur != nil && c.d.segs[len(c.d.segs)-1] == c.seg {
			_ = c.d.cur.Close()
			c.d.cur = nil
		}
		if err := os.Remove(c.seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			continue
		}
		c.d.remove(c.seg)
		freed += c.seg.size
		s.bytes -= c.seg.size
	}
	for _, d := range s.instances {
		d.dropRegisteredGaps()
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
		keys = append(keys, TrimKey{Origin: d.id.origin, Key: key, AppliedIndex: d.floors.AppliedIndex})
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
// floor or in a settled interval.
func (kf KeyFloor) registeredRange(lo, hi uint64) bool {
	cur := lo
	if cur <= kf.Floor {
		cur = kf.Floor + 1
	}
	settled := append([][2]uint64(nil), kf.Settled...)
	sort.Slice(settled, func(i, j int) bool { return settled[i][0] < settled[j][0] })
	for _, iv := range settled {
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
