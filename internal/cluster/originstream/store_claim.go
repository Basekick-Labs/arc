package originstream

// What a standby's claimant reads from the store (#1207 S4b1c): the
// instances it holds, the batches of each it can publish, the records
// themselves, and which instances the cap may evict.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// InstanceInfo describes one stored instance.
type InstanceInfo struct {
	Origin   string
	Instance uint64
	Bytes    int64
	// Floors is false until the origin sent floors for the instance;
	// FloorsApplied is the origin's applied index when it did.
	Floors        bool
	FloorsApplied uint64
	// Replaced: the origin has since streamed from a newer instance.
	Replaced bool
}

// Eviction tiers an EvictPolicy assigns.
const (
	EvictNever = iota
	EvictFirst
	// EvictUnderPressure: only when nothing in EvictFirst frees enough,
	// before new batches are dropped.
	EvictUnderPressure
)

// EvictPolicy assigns an instance its eviction tier. Called with the store
// lock held, on the append path: it must be a cheap read.
type EvictPolicy func(InstanceInfo) int

// SetEvictable installs the eviction policy, consulted while active reports
// true (stream takeover on in the cluster). Otherwise, or with a nil policy,
// the cap evicts replaced instances, as without stream takeover.
func (s *Store) SetEvictable(p EvictPolicy, active func() bool) {
	s.mu.Lock()
	s.evictable, s.evictActive = p, active
	s.mu.Unlock()
}

func (s *Store) policyLocked() EvictPolicy {
	if s.evictable == nil || s.evictActive == nil || !s.evictActive() {
		return nil
	}
	return s.evictable
}

func (s *Store) instanceInfoLocked(id instanceID, d *instanceDir) InstanceInfo {
	in := InstanceInfo{Origin: id.origin, Instance: id.instance, Replaced: s.replacedLocked(id)}
	for _, seg := range d.segs {
		in.Bytes += seg.size
	}
	if d.floors != nil {
		in.Floors, in.FloorsApplied = true, d.floors.AppliedIndex
	}
	return in
}

// pinnedWarnLocked reports, while a batch is dropped for want of room, the
// bytes the policy keeps beyond the store's own rule: instances their origin
// replaced (which the store alone would evict) kept for a possible takeover.
// The current primary's own instance is never counted: the store keeps it
// either way.
func (s *Store) pinnedWarnLocked(policy EvictPolicy) {
	if time.Since(s.lastPinCalc) < 10*time.Second {
		return // every dropped append lands here: count at most this often
	}
	s.lastPinCalc = time.Now()
	var pinned int64
	for id, d := range s.instances {
		if in := s.instanceInfoLocked(id, d); len(d.segs) > 0 && in.Replaced && policy(in) == EvictNever {
			for _, seg := range d.segs {
				pinned += seg.size
			}
		}
	}
	metrics.Get().SetStandbyStorePinned(pinned)
	if pinned == 0 || time.Since(s.lastPin) < 10*time.Minute {
		return
	}
	s.lastPin = time.Now()
	s.logger.Error().Int64("pinned_bytes", pinned).Int64("bytes", s.bytes).Int64("max_bytes", s.maxBytes).
		Msg("Standby store is full of batches kept for a possible stream takeover; new batches are not kept. Raise cluster.standby_store_max_bytes or settle the instances holding it")
}

// Instances lists the stored instances, ordered by origin then instance.
func (s *Store) Instances() []InstanceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]InstanceInfo, 0, len(s.instances))
	for id, d := range s.instances {
		out = append(out, s.instanceInfoLocked(id, d))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Origin != out[j].Origin {
			return out[i].Origin < out[j].Origin
		}
		return out[i].Instance < out[j].Instance
	})
	return out
}

// Received returns, per key, the batches of an instance this store can
// publish: stored, received whole (every cseq at or below the key's last
// arrived complete and in order, unless it is a gap), and not registered by
// the origin per its floors (those are in a file in every hour).
func (s *Store) Received(origin string, instance uint64) map[string][][2]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.instances[instanceID{origin, instance}]
	if d == nil {
		return nil
	}
	out := map[string][][2]uint64{}
	for key, k := range d.keys {
		var have intervalSet
		for _, seg := range d.segs {
			for _, iv := range seg.cseqs[key] {
				have = have.withRange(iv[0], iv[1])
			}
		}
		have = have.clip(1, k.last).minus(k.gaps)
		if d.floors != nil {
			if kf, ok := d.floors.Keys[key]; ok {
				have = have.minus(kf.registered())
			}
		}
		if len(have) > 0 {
			out[key] = have
		}
	}
	return out
}

// Record is one stored record.
type Record struct {
	Key     string
	CSeq    uint64
	Chunk   uint16
	Chunks  uint16
	Payload []byte
}

// Cursor resumes an Iterate where the previous call stopped. The zero value
// starts at the first record.
type Cursor struct {
	seg  int
	off  int64
	last map[string][2]uint64 // per key, the (cseq, chunk) last yielded
}

// ErrNoInstance: the store holds no such instance (evicted or dropped).
var ErrNoInstance = errors.New("standby store: no such instance")

// Iterate yields the records of an instance in store order, from cursor,
// until fn returns false (that record is not taken: the returned cursor
// starts with it) or the records stored when the call began are exhausted.
// It yields every stored record, those of gaps and of a batch still arriving
// included: a caller intersects with Received. Each (key, cseq, chunk) is yielded once per cursor lineage: a
// retransmitted record is skipped. The store lock is held only to snapshot
// the segment list; segments are read without it, up to their size at the
// snapshot. A segment trimmed meanwhile is skipped (its batches were
// registered). damaged counts segments whose remaining records could not be
// read back.
func (s *Store) Iterate(origin string, instance uint64, from Cursor, fn func(Record) bool) (next Cursor, damaged int, err error) {
	type segSnap struct {
		n    int
		path string
		size int64
	}
	s.mu.Lock()
	d := s.instances[instanceID{origin, instance}]
	if d == nil {
		s.mu.Unlock()
		return from, 0, ErrNoInstance
	}
	snaps := make([]segSnap, 0, len(d.segs))
	for _, seg := range d.segs {
		snaps = append(snaps, segSnap{seg.n, seg.path, seg.size})
	}
	s.mu.Unlock()

	// The cursor's dedup state is copied: a caller retrying from a cursor it
	// kept must not see this call's progress.
	next = Cursor{seg: from.seg, off: from.off, last: make(map[string][2]uint64, len(from.last))}
	for k, v := range from.last {
		next.last[k] = v
	}
	for _, sn := range snaps {
		if sn.n < next.seg {
			continue
		}
		if sn.n > next.seg {
			next.seg, next.off = sn.n, 0
		}
		stop, bad, err := readSegment(sn.path, next.off, sn.size, func(r Record, end int64) bool {
			l, seen := next.last[r.Key]
			if seen && (r.CSeq < l[0] || (r.CSeq == l[0] && uint64(r.Chunk) <= l[1])) {
				next.off = end
				return true // already yielded
			}
			if !fn(r) {
				return false // not taken: the next call starts with it
			}
			next.off = end
			next.last[r.Key] = [2]uint64{r.CSeq, uint64(r.Chunk)}
			return true
		})
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // trimmed or evicted meanwhile
			}
			return next, damaged, err
		}
		if bad {
			damaged++
			s.logger.Warn().Str("segment", filepath.Base(sn.path)).Str("origin", origin).
				Msg("Standby store segment does not read back past a damaged record; its later batches cannot be replayed from here")
		}
		if stop {
			return next, damaged, nil
		}
	}
	return next, damaged, nil
}

// readSegment reads the records of path in [off, size), calling fn with each
// and the offset after it. stop: fn returned false. bad: a record in range did
// not read back.
func readSegment(path string, off, size int64, fn func(Record, int64) bool) (stop, bad bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return false, false, err
	}
	defer f.Close()
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return false, false, err
	}
	r := bufio.NewReaderSize(io.LimitReader(f, size-off), 1<<20)
	var hdr [8]byte
	for off < size {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return false, true, nil
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		if n < recordFixed-8 || n > maxFrame || off+8+int64(n) > size {
			return false, true, nil
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return false, true, nil
		}
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(hdr[4:]) {
			return false, true, nil
		}
		keyLen := int(binary.BigEndian.Uint16(body[12:]))
		if 14+keyLen > len(body) {
			return false, true, nil
		}
		off += 8 + int64(n)
		rec := Record{Key: string(body[14 : 14+keyLen]), CSeq: binary.BigEndian.Uint64(body[0:]),
			Chunk: binary.BigEndian.Uint16(body[8:]), Chunks: binary.BigEndian.Uint16(body[10:]), Payload: body[14+keyLen:]}
		if !fn(rec, off) {
			return true, false, nil
		}
	}
	return false, false, nil
}

// DropInstance removes an instance and its records: a claimant does so once
// its claim is finished and every file of it is held locally.
func (s *Store) DropInstance(origin string, instance uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := instanceID{origin, instance}
	d := s.instances[id]
	if d == nil {
		return nil
	}
	if d.cur != nil {
		_ = d.cur.Close()
		d.cur = nil
	}
	var freed int64
	for _, seg := range d.segs {
		freed += seg.size
	}
	if err := os.RemoveAll(d.dir); err != nil {
		return fmt.Errorf("standby store: drop instance: %w", err)
	}
	d.segs = nil // a trim pass holding this dir must not free its bytes again
	syncDir(filepath.Dir(d.dir))
	delete(s.instances, id)
	s.bytes -= freed
	s.publishLocked()
	return nil
}

// registered returns the cseqs the floors say are in a file in every hour.
func (kf KeyFloor) registered() intervalSet {
	var v intervalSet
	if kf.Floor > 0 {
		v = v.withRange(1, kf.Floor)
	}
	for _, iv := range kf.Settled {
		if iv[0] <= iv[1] {
			v = v.withRange(iv[0], iv[1])
		}
	}
	return v
}

// clip returns the part of v within [lo, hi].
func (v intervalSet) clip(lo, hi uint64) intervalSet {
	var out intervalSet
	for _, iv := range v {
		a, b := iv[0], iv[1]
		if b < lo || a > hi {
			continue
		}
		if a < lo {
			a = lo
		}
		if b > hi {
			b = hi
		}
		out = append(out, [2]uint64{a, b})
	}
	return out
}

// minus returns v without every interval of w.
func (v intervalSet) minus(w intervalSet) intervalSet {
	out := append(intervalSet(nil), v...)
	for _, cut := range w {
		var next intervalSet
		for _, iv := range out {
			if iv[1] < cut[0] || iv[0] > cut[1] {
				next = append(next, iv)
				continue
			}
			if iv[0] < cut[0] {
				next = append(next, [2]uint64{iv[0], cut[0] - 1})
			}
			if iv[1] > cut[1] {
				next = append(next, [2]uint64{cut[1] + 1, iv[1]})
			}
		}
		out = next
	}
	return out
}
