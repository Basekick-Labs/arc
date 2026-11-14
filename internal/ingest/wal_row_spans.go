package ingest

import (
	"encoding/hex"
	"strings"

	"github.com/basekick-labs/arc/internal/wal"
)

// Per-hour WAL row checkpoints (#1492).
//
// A flush that spans several hours writes one Parquet file per hour and, until
// now, one WAL checkpoint after the last file. A crash in between replayed
// every row of the flush: the hours already in storage were stored twice,
// permanently for a tagless measurement (#948). With the sample arc.toml
// (5M rows, 100 ms) a kill -9 under backfill duplicated a fifth of what had
// been acknowledged.
//
// walRowSpans names the WAL entry and row of every row of a merged batch, by
// position, so the flush can checkpoint the rows of each hour file as soon as
// that file is stored and registered: the row-range checkpoint recovery
// already writes for itself (wal.ParseRecoveryRowIdentity), now written by
// the live flush too.
//
// The map is only ever built by mergeBatches, which concatenates the records
// in order (row p of the merge is row p-lo of the record whose span holds p).
// mergeSortedByTime reorders rows and leaves it nil, so a merge that is not
// positional can never carry one. A record gets a span only when its rows are
// known to be exactly the rows of one WAL entry, in order:
//
//   - a *TypedColumnBatch with a WAL copy and exactly one identity. hasWALCopy
//     is required as well as the count: a chunked payload whose later chunk
//     failed admission keeps the tokens that did commit (wal.PreparedAppend.
//     Commit), so one token can name a prefix of the rows;
//   - that identity a tracked token (row r of the batch is row r of the entry),
//     or a recovery row range P:s:e of exactly the batch's length (row r is row
//     s+r of P).
//
// Everything else (no WAL copy, a content hash, a chunked payload, a row-format
// record) gets no span: those rows are replayed as before. A wrong range here
// would be silent data loss, so the rule is "exactly known or nothing".
type walRowSpans struct {
	spans []walRowSpan
	rows  int // rows laid out; the merged batch's time column has this many
}

type walRowSpan struct {
	lo, hi int    // rows [lo, hi) of the merged batch
	parent string // the WAL entry, or "" when these rows cannot be named
	base   int    // row lo of the merge is row base of parent
	copied bool   // the record has a WAL copy (hasWALCopy)
	// tokens are the record's WAL entries when its rows cannot be named one
	// by one but every one of its identities is a tracked token (a chunked
	// payload: one batch, several entries). Replay re-stores those entries
	// whole, so a file holding these rows can be named by them as a whole
	// need (#1697). nil otherwise.
	tokens []string
}

// maxRowCheckpointRanges bounds one file's checkpoint. A flush whose rows
// interleave hours (a multi-tag secondary sort over a long span) would name
// nearly every row on its own; past this the file is left to the parent
// checkpoint, as before.
const maxRowCheckpointRanges = 1024

// walRowSpansOf lays records out as mergeBatches concatenates them. nil when a
// record's row count is unknown (no time column), which mergeBatches rejects
// anyway.
func walRowSpansOf(records []interface{}) *walRowSpans {
	out := &walRowSpans{spans: make([]walRowSpan, 0, len(records))}
	for _, record := range records {
		var batch *TypedColumnBatch
		var cols map[string]interface{}
		switch r := record.(type) {
		case *TypedColumnBatch:
			batch, cols = r, r.Data
		case map[string]interface{}:
			cols = r
		default:
			return nil
		}
		times, ok := cols["time"].([]int64)
		if !ok {
			return nil
		}
		n := len(times)
		span := walRowSpan{lo: out.rows, hi: out.rows + n}
		if batch != nil {
			span.copied = batch.hasWALCopy
			if batch.hasWALCopy && len(batch.WALHashes) == 1 {
				span.parent, span.base = rowSpanParent(batch.WALHashes[0], n)
			}
			if batch.hasWALCopy && span.parent == "" && len(batch.WALHashes) > 0 {
				tokens := true
				for _, h := range batch.WALHashes {
					if !isTrackedIdentity(h) {
						tokens = false
						break
					}
				}
				if tokens {
					span.tokens = append([]string(nil), batch.WALHashes...)
				}
			}
		}
		out.spans = append(out.spans, span)
		out.rows += n
	}
	return out
}

// rowSpanParent reads the identity a batch of n rows carries: a tracked token
// (the rows are the entry), or a row range of exactly n rows. "" otherwise.
func rowSpanParent(identity string, n int) (parent string, base int) {
	if len(identity) == walTrackedIdentityHexLen {
		if _, err := hex.DecodeString(identity); err == nil && strings.ToLower(identity) == identity {
			return identity, 0
		}
		return "", 0
	}
	if p, start, end, ok := wal.ParseRecoveryRowIdentity(identity); ok && end-start == n {
		return p, start
	}
	return "", 0
}

// rangesIn returns the row-range checkpoint identities of the rows at indices,
// ascending positions of the merged batch: one identity per maximal run of
// consecutive positions inside one span. Rows with no span are left out (they
// have nothing to checkpoint). nil when the identities would exceed
// maxRowCheckpointRanges.
func (s *walRowSpans) rangesIn(indices []int) []string {
	return s.rangesInLimit(indices, maxRowCheckpointRanges)
}

// rangesInLimit is rangesIn with its own bound; limit <= 0 is unbounded.
func (s *walRowSpans) rangesInLimit(indices []int, limit int) []string {
	if s == nil || len(indices) == 0 {
		return nil
	}
	var out []string
	si := 0
	runStart, runEnd := -1, -1 // positions [runStart, runEnd) of the open run
	flush := func() {
		if runStart < 0 {
			return
		}
		sp := s.spans[si]
		out = append(out, wal.RecoveryRowIdentity(sp.parent, sp.base+runStart-sp.lo, sp.base+runEnd-sp.lo))
		runStart, runEnd = -1, -1
	}
	for _, p := range indices {
		for si < len(s.spans) && s.spans[si].hi <= p {
			flush()
			si++
		}
		if si == len(s.spans) {
			break
		}
		if s.spans[si].parent == "" {
			continue
		}
		if runStart >= 0 && p == runEnd {
			runEnd++
			continue
		}
		flush()
		if limit > 0 && len(out) >= limit {
			return nil
		}
		runStart, runEnd = p, p+1
	}
	flush()
	return out
}

// lostWithoutCopy counts the rows of records without a WAL copy that are not
// at a written position: the rows a failed flush loses, for its report. The
// rows of such a record that did reach a stored file are not lost.
func (s *walRowSpans) lostWithoutCopy(written []bool) int {
	if s == nil {
		return 0
	}
	lost := 0
	for _, sp := range s.spans {
		if sp.copied {
			continue
		}
		for p := sp.lo; p < sp.hi; p++ {
			if p >= len(written) || !written[p] {
				lost++
			}
		}
	}
	return lost
}

// storedWithoutCopy counts the rows of records without a WAL copy that are at
// a written position: stored rows the WAL does not hold, which Close's drain
// must not count among the rows it still holds (#1693).
func (s *walRowSpans) storedWithoutCopy(written []bool) int {
	if s == nil {
		return 0
	}
	n := 0
	for _, sp := range s.spans {
		if sp.copied {
			continue
		}
		for p := sp.lo; p < sp.hi && p < len(written); p++ {
			if written[p] {
				n++
			}
		}
	}
	return n
}

// deferredNeed names what must be stored elsewhere before a stored hour file
// that holds rows the row map cannot name may be removed (#1697): the row
// ranges of its named rows (unbounded, unlike a checkpoint) and the whole
// tokens of its unnamed ones. ok is false when the file cannot be named that
// way: a row with no WAL copy (the file is its only copy), or an unnamed row
// whose record carries no tracked tokens (a content hash, a recovery replay
// with no identity). indices are ascending positions of the merged batch.
func (s *walRowSpans) deferredNeed(indices []int) (ids []string, ok bool) {
	if s == nil || len(indices) == 0 {
		return nil, false
	}
	seen := map[string]struct{}{}
	named := make([]int, 0, len(indices))
	si := 0
	for _, p := range indices {
		for si < len(s.spans) && s.spans[si].hi <= p {
			si++
		}
		if si == len(s.spans) {
			return nil, false
		}
		sp := s.spans[si]
		if !sp.copied {
			return nil, false
		}
		if sp.parent != "" {
			named = append(named, p)
			continue
		}
		if len(sp.tokens) == 0 {
			return nil, false
		}
		for _, t := range sp.tokens {
			if _, dup := seen[t]; !dup {
				seen[t] = struct{}{}
				ids = append(ids, t)
			}
		}
	}
	return append(ids, s.rangesInLimit(named, 0)...), true
}
