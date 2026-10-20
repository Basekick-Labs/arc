package ingest

import (
	"fmt"
	"sync/atomic"
)

// Arrow string arrays use signed 32-bit offsets. Keep each flush's string
// columns well below that limit so a buffer deferred by a full flush queue can
// be written as several files once the queue drains.
const maxParquetStringColumnBytes int64 = 1 << 30

// Flush split (#1207 S4b1c2b).
//
// The cluster manifest judges a file by every batch in it: one batch of a
// file another writer claimed refuses, and parks, the whole file. So a flush
// never puts in one file batches of different writer instances (WAL
// recovery replays an earlier instance's batches into the same buffer keys
// as live ones), nor batches claimed by another writer with batches nobody
// claimed: each class is flushed as its own group, and the rows nobody
// claimed register at once instead of waiting in a parked file. That also
// covers a batch whose WAL append was dropped, which no later republish
// could read back. Nor does it mix batches a DELETE record holds with
// batches it does not (#1207 d1): the record is applied to a file as a
// whole, before it registers.

// ClaimedBatchFunc reports whether the manifest would refuse a file of the
// origin holding batch cseq of (instance, key) as claimed by another writer.
type ClaimedBatchFunc func(instance uint64, key string, cseq uint64) bool

// SetClaimedBatch installs the claimed predicate (nil: by instance only).
func (b *ArrowBuffer) SetClaimedBatch(fn ClaimedBatchFunc) {
	if fn == nil {
		b.claimedBatch.Store(nil)
		return
	}
	b.claimedBatch.Store(&fn)
}

type claimedBatchPtr = atomic.Pointer[ClaimedBatchFunc]

// DeleteScopeFunc returns the DELETE records holding batch cseq of
// (instance, key) in their scope (#1207 d1). A file is rewritten with a
// record as a whole, so batches with different answers never share one.
type DeleteScopeFunc func(instance uint64, key string, cseq uint64) []uint64

// SetDeleteScope installs the DELETE scope predicate (nil: none).
func (b *ArrowBuffer) SetDeleteScope(fn DeleteScopeFunc) {
	if fn == nil {
		b.deleteScope.Store(nil)
		return
	}
	b.deleteScope.Store(&fn)
}

type deleteScopePtr = atomic.Pointer[DeleteScopeFunc]

// flushPart is one class of an extracted buffer.
type flushPart struct {
	records   []interface{}
	walHashes []string
	group     *flushGroup
	rows      int
}

type flushClass struct {
	covered  bool
	instance uint64
	claimed  bool
	scope    string // the DELETE records holding its batches
}

// splitFlushRecordsByStringBytes groups whole buffered batches so accumulated
// string columns stay below Arrow's 32-bit offset limit. It returns nil when
// one group suffices or there is only one batch. Batches stay whole because
// their WAL and coverage identities must settle together; this handles growth
// across buffered requests without changing a single request's semantics.
func (b *ArrowBuffer) splitFlushRecordsByStringBytes(key string, records []interface{}, maxBytes int64) []flushPart {
	if maxBytes <= 0 || len(records) < 2 {
		return nil
	}
	if estimate, complete := estimateFlushRecordsBytes(records); complete && estimate <= maxBytes {
		// The retained-size estimate is at least the encoded payload or the
		// typed string data, so no individual string column can exceed it.
		return nil
	}

	var grouped [][]interface{}
	current := make([]interface{}, 0, min(len(records), 64))
	currentBytes := map[string]int64{}
	for _, record := range records {
		sizes := flushRecordStringColumnBytes(record)
		if len(current) > 0 && exceedsStringColumnLimit(currentBytes, sizes, maxBytes) {
			grouped = append(grouped, current)
			current = make([]interface{}, 0, min(len(records), 64))
			currentBytes = map[string]int64{}
		}
		current = append(current, record)
		for column, size := range sizes {
			currentBytes[column] = saturatingAddInt64(currentBytes[column], size)
		}
	}
	if len(current) > 0 {
		grouped = append(grouped, current)
	}
	if len(grouped) < 2 {
		return nil
	}

	parts := make([]flushPart, 0, len(grouped))
	for _, records := range grouped {
		// Own each slice so callers can release the source buffer/task after
		// the parts have been constructed.
		partRecords := append([]interface{}(nil), records...)
		hashes := collectWALHashes(partRecords)
		rows := 0
		for _, record := range partRecords {
			rows += flushRecordRowCount(record)
		}
		parts = append(parts, flushPart{
			records:   partRecords,
			walHashes: hashes,
			group:     b.newFlushGroup(key, partRecords, hashes),
			rows:      rows,
		})
	}
	return parts
}

func estimateFlushRecordsBytes(records []interface{}) (int64, bool) {
	var total int64
	for _, record := range records {
		batch, ok := record.(*TypedColumnBatch)
		if !ok || batch == nil || batch.estimatedBytes <= 0 {
			return 0, false
		}
		total = saturatingAddInt64(total, batch.estimatedBytes)
	}
	return total, true
}

func exceedsStringColumnLimit(current, next map[string]int64, maxBytes int64) bool {
	for column, size := range next {
		if size > maxBytes || current[column] > maxBytes-size {
			return true
		}
	}
	return false
}

func flushRecordStringColumnBytes(record interface{}) map[string]int64 {
	var columns map[string]interface{}
	switch r := record.(type) {
	case *TypedColumnBatch:
		if r != nil {
			columns = r.Data
		}
	case map[string]interface{}:
		columns = r
	}
	if len(columns) == 0 {
		return nil
	}

	var sizes map[string]int64
	for name, column := range columns {
		var size int64
		switch values := column.(type) {
		case []string:
			for _, value := range values {
				size = saturatingAddInt64(size, int64(len(value)))
			}
		case []interface{}:
			for _, value := range values {
				if value, ok := value.(string); ok {
					size = saturatingAddInt64(size, int64(len(value)))
				}
			}
		default:
			continue
		}
		if size == 0 {
			continue
		}
		if sizes == nil {
			sizes = make(map[string]int64)
		}
		sizes[name] = size
	}
	return sizes
}

func flushRecordRowCount(record interface{}) int {
	var columns map[string]interface{}
	switch r := record.(type) {
	case *TypedColumnBatch:
		if r != nil {
			columns = r.Data
		}
	case map[string]interface{}:
		columns = r
	}
	if times, ok := columns["time"].([]int64); ok {
		return len(times)
	}
	if times, ok := columns["time"].([]interface{}); ok {
		return len(times)
	}
	return 0
}

func saturatingAddInt64(a, b int64) int64 {
	if b > 0 && a > int64(^uint64(0)>>1)-b {
		return int64(^uint64(0) >> 1)
	}
	return a + b
}

// splitForCoverage returns the classes of records, in order, each with its
// own group, or nil when they need no split (coverage off, or one class).
func (b *ArrowBuffer) splitForCoverage(key string, records []interface{}) []flushPart {
	if b.coverage.Load() == nil || len(records) < 2 {
		return nil
	}
	var claimed ClaimedBatchFunc
	if p := b.claimedBatch.Load(); p != nil {
		claimed = *p
	}
	var scope DeleteScopeFunc
	if p := b.deleteScope.Load(); p != nil {
		scope = *p
	}
	classOf := func(record interface{}) flushClass {
		batch, ok := record.(*TypedColumnBatch)
		if !ok || len(batch.Coverage) == 0 {
			return flushClass{}
		}
		c := flushClass{covered: true, instance: batch.Coverage[0].Instance}
		if claimed != nil {
			for _, p := range batch.Coverage {
				if claimed(p.Instance, key, p.CSeq) {
					c.claimed = true
					break
				}
			}
		}
		if scope != nil {
			// The pieces of one batch are one batch (chunks of it): the first
			// answers for all.
			p := batch.Coverage[0]
			c.scope = fmt.Sprint(scope(p.Instance, key, p.CSeq))
		}
		return c
	}
	var order []flushClass
	byClass := map[flushClass][]interface{}{}
	for _, r := range records {
		c := classOf(r)
		if _, seen := byClass[c]; !seen {
			order = append(order, c)
		}
		byClass[c] = append(byClass[c], r)
	}
	if len(order) < 2 {
		return nil
	}
	parts := make([]flushPart, 0, len(order))
	for _, c := range order {
		recs := byClass[c]
		hashes := collectWALHashes(recs)
		rows := 0
		for _, r := range recs {
			var cols map[string]interface{}
			switch rec := r.(type) {
			case *TypedColumnBatch:
				cols = rec.Data
			case map[string]interface{}:
				cols = rec
			}
			if t, ok := cols["time"].([]int64); ok {
				rows += len(t)
			}
		}
		parts = append(parts, flushPart{records: recs, walHashes: hashes, group: b.newFlushGroup(key, recs, hashes), rows: rows})
	}
	return parts
}
