package ingest

import "sync/atomic"

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
// could read back.

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
