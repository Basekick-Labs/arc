package ingest

import (
	"fmt"
	"sync/atomic"
)

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
