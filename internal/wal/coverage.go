package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync/atomic"
	"time"
)

// Coverage identifies the ingest batch a WAL entry belongs to (#1207).
//
// The ingest buffer mints CSeq per (writer instance, buffer key) under the
// shard lock at the moment the batch is committed, so the batches of one key
// commit in CSeq order and a flush of that key covers one contiguous range. A
// batch too large for one entry is split into Chunks entries that share the
// CSeq; Chunk is this entry's index.
type Coverage struct {
	Instance uint64
	Key      string
	CSeq     uint64
	Chunk    uint16
	Chunks   uint16
}

// Covered entry layout (marker 0x04):
//
//	[0x04][instance u64][walseq u64][logical payload][key][keyLen u16][chunk u16][chunks u16][cseq u64]
//
// The logical payload is what a 0x03 entry carries (an enveloped msgpack
// value), so everything that reads a tracked entry reads a covered one the
// same way once the header and trailer are stripped, and the identity token is
// the same "instance+walseq" form — checkpoints, the purge floor and recovery
// identities are unchanged.
//
// CSeq is last so the CRC of everything before it can be computed outside the
// shard lock: Commit only writes 8 bytes and folds them into the CRC.
const (
	walCoverageFixedTrailer = 2 + 2 + 2 + 8
	// MaxCoverageKeyLen bounds the buffer key a covered entry carries; a
	// longer key is written as a plain tracked entry, without coverage.
	MaxCoverageKeyLen = 1 << 12
)

// ErrCoverageTrailer reports a 0x04 entry whose trailer does not fit.
var ErrCoverageTrailer = errors.New("WAL covered entry has a malformed coverage trailer")

func coverageTrailerLen(key string) int { return len(key) + walCoverageFixedTrailer }

// parseCoveredPayload splits a 0x04 payload into its token, logical payload and
// coverage.
func parseCoveredPayload(payload []byte) (token string, logical []byte, cov *Coverage, err error) {
	if len(payload) < walTrackedHeaderSize+walCoverageFixedTrailer {
		return "", nil, nil, ErrCoverageTrailer
	}
	instance := binary.BigEndian.Uint64(payload[1:9])
	walseq := binary.BigEndian.Uint64(payload[9:17])
	end := len(payload)
	cseq := binary.BigEndian.Uint64(payload[end-8:])
	chunks := binary.BigEndian.Uint16(payload[end-10 : end-8])
	chunk := binary.BigEndian.Uint16(payload[end-12 : end-10])
	keyLen := int(binary.BigEndian.Uint16(payload[end-14 : end-12]))
	keyEnd := end - walCoverageFixedTrailer
	keyStart := keyEnd - keyLen
	if keyStart < walTrackedHeaderSize || chunks == 0 || chunk >= chunks {
		return "", nil, nil, ErrCoverageTrailer
	}
	cov = &Coverage{
		Instance: instance,
		Key:      string(payload[keyStart:keyEnd]),
		CSeq:     cseq,
		Chunk:    chunk,
		Chunks:   chunks,
	}
	return fmt.Sprintf("%016x%016x", instance, walseq), payload[walTrackedHeaderSize:keyStart], cov, nil
}

// PreparedAppend is a covered WAL append built outside the ingest shard lock
// and committed under it (#1207). Prepare allocates the WAL sequences — they
// are pending in the purge floor from that moment — and assembles every entry
// except the coverage sequence. Commit stamps the sequence, fires the
// replication hook and enqueues. Abandon releases the sequences: nothing was
// written, so a write refused to the client never reaches the WAL or a replica.
//
// Exactly one of Commit or Abandon must be called; Abandon after Commit is a
// no-op, so a deferred Abandon is the intended pattern.
type PreparedAppend struct {
	w       *Writer
	key     string
	entries []preparedEntry
	done    bool
}

type preparedEntry struct {
	data      []byte // full entry: header + payload; CRC and cseq filled at Commit
	seq       uint64
	token     string
	prefixCRC uint32 // CRC of the payload up to, not including, the cseq
	logical   []byte // for the replication hook
	ts        uint64
	chunk     uint16
}

// PrepareCovered prepares a covered append of payload (msgpack) for database
// under buffer key key. An oversized payload is split into chunks exactly as
// AppendRawWithMetaTracked splits it.
func (w *Writer) PrepareCovered(database, key string, payload []byte) (*PreparedAppend, error) {
	if len(database) > 255 {
		return nil, fmt.Errorf("database name too long: %d bytes", len(database))
	}
	if len(key) > MaxCoverageKeyLen {
		return nil, fmt.Errorf("coverage key too long: %d bytes", len(key))
	}
	dbBytes := []byte(database)
	overhead := 3 + len(dbBytes) + coverageTrailerLen(key)
	chunks := [][]byte{payload}
	if !trackedPayloadFits(len(payload), overhead) {
		var err error
		chunks, err = splitOversizedPayload(payload)
		if err != nil {
			return nil, oversizedPayloadError(err)
		}
	}
	if len(chunks) > 1<<16-1 {
		return nil, oversizedPayloadError(fmt.Errorf("%d chunks exceed the coverage chunk index", len(chunks)))
	}
	for _, chunk := range chunks {
		if !trackedPayloadFits(len(chunk), overhead) {
			return nil, oversizedPayloadError(fmt.Errorf("tracked size %d exceeds limit %d", len(chunk)+overhead+walTrackedHeaderSize, MaxWALPayloadSize))
		}
	}
	p := &PreparedAppend{w: w, key: key, entries: make([]preparedEntry, 0, len(chunks))}
	for i, chunk := range chunks {
		logical := envelopePayload(dbBytes, chunk)
		// Allocation and publication as pending are one step; see
		// appendTrackedEntry.
		w.pendingMu.Lock()
		seq := atomic.AddUint64(&w.trackedSequence, 1)
		w.pendingSeqs[seq] = struct{}{}
		w.pendingMu.Unlock()
		payloadLen := walTrackedHeaderSize + len(logical) + coverageTrailerLen(key)
		data := make([]byte, WALEntryHeaderSize+payloadLen)
		body := data[WALEntryHeaderSize:]
		body[0] = WALCoveredMarker
		binary.BigEndian.PutUint64(body[1:9], w.trackedInstance)
		binary.BigEndian.PutUint64(body[9:17], seq)
		off := walTrackedHeaderSize + copy(body[walTrackedHeaderSize:], logical)
		off += copy(body[off:], key)
		binary.BigEndian.PutUint16(body[off:], uint16(len(key)))
		binary.BigEndian.PutUint16(body[off+2:], uint16(i))
		binary.BigEndian.PutUint16(body[off+4:], uint16(len(chunks)))
		ts := uint64(time.Now().UnixMicro())
		binary.BigEndian.PutUint32(data[0:4], uint32(payloadLen))
		binary.BigEndian.PutUint64(data[4:12], ts)
		p.entries = append(p.entries, preparedEntry{
			data:      data,
			seq:       seq,
			token:     fmt.Sprintf("%016x%016x", w.trackedInstance, seq),
			prefixCRC: crc32.ChecksumIEEE(body[:len(body)-8]),
			logical:   body[walTrackedHeaderSize : walTrackedHeaderSize+len(logical)],
			ts:        ts,
			chunk:     uint16(i),
		})
	}
	return p, nil
}

// Instance is the writer instance the prepared entries belong to.
func (p *PreparedAppend) Instance() uint64 { return p.w.trackedInstance }

// Chunks is the number of WAL entries the batch was split into.
func (p *PreparedAppend) Chunks() int { return len(p.entries) }

// Commit stamps cseq, fires the replication hook and enqueues every entry in
// order. It is cheap — no allocation proportional to the payload — because it
// runs under the ingest shard lock.
//
// On a full queue it stops at the first dropped entry and returns
// ErrWALDropped with the tokens of the entries already enqueued: those are in
// the WAL and must be checkpointed by the flush like any other, while the
// dropped and unsent ones are released here. The batch itself is committed to
// the buffer either way; the WAL is for durability, not correctness.
func (p *PreparedAppend) Commit(cseq uint64) ([]string, error) {
	if p.done {
		return nil, errors.New("WAL prepared append already finished")
	}
	p.done = true
	var cs [8]byte
	binary.BigEndian.PutUint64(cs[:], cseq)
	tokens := make([]string, 0, len(p.entries))
	for i := range p.entries {
		e := &p.entries[i]
		copy(e.data[len(e.data)-8:], cs[:])
		binary.BigEndian.PutUint32(e.data[12:16], crc32.Update(e.prefixCRC, crc32.IEEETable, cs[:]))
		if p.w.replicationHook.Load() != nil {
			p.w.replicate(&ReplicationEntry{TimestampUS: e.ts, Payload: e.logical, Coverage: &Coverage{
				Instance: p.w.trackedInstance, Key: p.key, CSeq: cseq, Chunk: e.chunk, Chunks: uint16(len(p.entries)),
			}})
		}
		if err := p.w.tryEnqueueEntry(walEntry{data: e.data, seq: e.seq}); err != nil {
			p.release(i)
			return tokens, err
		}
		tokens = append(tokens, e.token)
	}
	return tokens, nil
}

// Abandon releases the sequences of a prepared append that was never
// committed. Safe to defer: a no-op after Commit.
func (p *PreparedAppend) Abandon() {
	if p == nil || p.done {
		return
	}
	p.done = true
	p.release(0)
}

func (p *PreparedAppend) release(from int) {
	p.w.pendingMu.Lock()
	for _, e := range p.entries[from:] {
		delete(p.w.pendingSeqs, e.seq)
	}
	p.w.pendingMu.Unlock()
}

// TrackedInstance is this writer's instance id: the prefix of every identity
// token it mints and the instance of every coverage id it writes.
func (w *Writer) TrackedInstance() uint64 { return w.trackedInstance }

// ReplayCoverage travels with a replayed covered entry from recovery to the
// ingest buffer, so the file it is flushed into registers with the batch's
// original coverage (#1207). RowTotal is the entry's row count for a
// row-format entry, which recovery may replay in row ranges; zero for a
// columnar entry, which is always replayed whole.
type ReplayCoverage struct {
	Coverage Coverage
	RowTotal int
}

type replayCoverageKey struct{}

// WithReplayCoverage attaches rc to the context recovery passes to its replay
// callbacks.
func WithReplayCoverage(ctx context.Context, rc ReplayCoverage) context.Context {
	return context.WithValue(ctx, replayCoverageKey{}, rc)
}

// ReplayCoverageFrom returns the coverage of the entry being replayed, if any.
func ReplayCoverageFrom(ctx context.Context) (ReplayCoverage, bool) {
	rc, ok := ctx.Value(replayCoverageKey{}).(ReplayCoverage)
	return rc, ok
}
