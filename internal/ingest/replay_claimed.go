package ingest

// The replay half of a stream takeover (#1207 S4b1c). A standby writer that
// claimed a dead writer's batches publishes them from its standby store:
// decoded here, filtered to the rows not yet in a registered file per
// partition hour, and flushed straight to Parquet with coverage that names
// the claim. Nothing goes through the shared buffer, the WAL or the ingest
// gate: the rows are not this writer's, and must never merge with its own.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/wal"
)

// ClaimedRecord is one stored record of a claimed instance: one WAL entry,
// a whole batch or one chunk of it.
type ClaimedRecord struct {
	CSeq   uint64
	Chunk  uint16
	Chunks uint16 // as stored: 1 for an unsplit batch
	Entry  *wal.Entry
	// PayloadBytes is the stored payload size, for the call's byte bound.
	PayloadBytes int
	// Keep returns the rows [lo, hi) of this record still to publish in
	// hour (rows of other hours in a range are ignored), given the record's
	// row count. Empty: the hour is already registered. An error: the
	// registered rows cannot be subtracted (the hour is not replayed).
	Keep func(hour time.Time, total int) ([][2]int, error)
}

// ClaimedReplay is one call: records of one buffer key of one claimed
// instance, published under one claim.
type ClaimedReplay struct {
	Database, Measurement string
	Instance, Epoch       uint64
	Records               []ClaimedRecord
}

// ReplayOutcome is what became of one record's rows in one hour.
type ReplayOutcome int

const (
	// ReplayWritten: written to a file and handed to the registrar.
	ReplayWritten ReplayOutcome = iota + 1
	// ReplayCovered: Keep said the hour is registered already.
	ReplayCovered
	// ReplayNothingLeft: every row of the record in this hour is inside the
	// registered ranges (the index never shows such an hour as covered: its
	// pieces count rows of every hour).
	ReplayNothingLeft
	// ReplayUnreplayable: the record cannot be published (Err says why):
	// undecodable, another target, no time column, or registered rows that
	// cannot be subtracted. Retrying will not change it.
	ReplayUnreplayable
	// ReplayFailed: the flush failed (Err); retry.
	ReplayFailed
)

// HourOutcome reports one (record, hour). Hour is zero for an outcome of
// the whole record (unreplayable before its rows were bucketed).
type HourOutcome struct {
	Record  int
	Hour    time.Time
	Outcome ReplayOutcome
	Err     error
}

var (
	// ErrReplayCoverageOff: this buffer cannot publish claimed rows (coverage
	// off, or no coverage registrar): they would register uncovered and
	// nothing could arbitrate them.
	ErrReplayCoverageOff = errors.New("claimed replay needs WAL coverage and a coverage registrar")
	// ErrReplayClosed: the buffer is shutting down.
	ErrReplayClosed = errors.New("claimed replay: buffer is closing")
	// ErrReplayTooLarge: the call is over a bound; split it.
	ErrReplayTooLarge = errors.New("claimed replay: call too large, split it")
	// ErrReplayDuplicate: one record is in the call twice.
	ErrReplayDuplicate = errors.New("claimed replay: a record is listed twice")
)

const (
	maxReplayRecords = 256
	maxReplayBytes   = 64 << 20
	// Per flush group, so every file's coverage stays exact through the
	// journal (2048 partials) and the 64 KiB registration cap.
	maxReplayGroupPartials      = 1000
	maxReplayGroupCoverageBytes = 60 << 10
)

// replaySub is one single-hour slice of one record, with its piece.
type replaySub struct {
	record int
	hour   time.Time
	batch  *TypedColumnBatch
}

// ReplayClaimed publishes the rows of r not yet registered, one outcome per
// (record, hour). A non-nil error refuses the whole call before anything is
// written.
func (b *ArrowBuffer) ReplayClaimed(ctx context.Context, r ClaimedReplay) ([]HourOutcome, error) {
	cs := b.coverage.Load()
	ref := b.fileRegistrar.Load()
	if cs == nil || ref == nil || ref.FileRegistrar == nil {
		return nil, ErrReplayCoverageOff
	}
	if _, ok := ref.FileRegistrar.(CoverageFileRegistrar); !ok {
		return nil, ErrReplayCoverageOff
	}
	if b.closing.Load() {
		return nil, ErrReplayClosed
	}
	if r.Epoch == 0 || r.Instance == 0 || r.Database == "" || r.Measurement == "" {
		return nil, errors.New("claimed replay: instance, epoch and target are required")
	}
	if len(r.Records) > maxReplayRecords {
		return nil, ErrReplayTooLarge
	}
	bytes, seen := 0, map[[2]uint64]bool{}
	for i, rec := range r.Records {
		bytes += rec.PayloadBytes
		if i > 0 && bytes > maxReplayBytes {
			return nil, ErrReplayTooLarge
		}
		id := [2]uint64{rec.CSeq, uint64(rec.Chunk)}
		if seen[id] {
			return nil, ErrReplayDuplicate
		}
		seen[id] = true
	}
	key := r.Database + "/" + r.Measurement

	var out []HourOutcome
	var subs []replaySub
	for i, rec := range r.Records {
		recSubs, outcomes := b.claimedRecordSubs(i, rec, r)
		out = append(out, outcomes...)
		subs = append(subs, recSubs...)
	}

	// One flush group per column layout: batches of differing types do not
	// merge. Checked before anything is written.
	groups := map[string][]replaySub{}
	for _, s := range subs {
		sig := replaySignature(s.batch.Data)
		groups[sig] = append(groups[sig], s)
	}
	sigs := make([]string, 0, len(groups))
	for sig, g := range groups {
		if err := replayGroupFits(g); err != nil {
			return nil, err
		}
		sigs = append(sigs, sig)
	}
	sort.Strings(sigs)

	for _, sig := range sigs {
		g := groups[sig]
		err := b.flushClaimedGroup(ctx, cs, key, r, g)
		for _, s := range g {
			o := HourOutcome{Record: s.record, Hour: s.hour, Outcome: ReplayWritten}
			if err != nil {
				o.Outcome, o.Err = ReplayFailed, err
			}
			out = appendOutcome(out, o)
		}
	}
	return out, nil
}

// appendOutcome records o once per (record, hour): a record with several
// keep ranges in one hour has several slices.
func appendOutcome(out []HourOutcome, o HourOutcome) []HourOutcome {
	for i := range out {
		if out[i].Record == o.Record && out[i].Hour.Equal(o.Hour) {
			if o.Outcome == ReplayFailed {
				out[i] = o
			}
			return out
		}
	}
	return append(out, o)
}

// claimedRecordSubs decodes one record and cuts it into single-hour slices
// of the rows Keep leaves, each carrying its piece.
func (b *ArrowBuffer) claimedRecordSubs(i int, rec ClaimedRecord, r ClaimedReplay) ([]replaySub, []HourOutcome) {
	bad := func(err error) []HourOutcome {
		return []HourOutcome{{Record: i, Outcome: ReplayUnreplayable, Err: err}}
	}
	columns, err := claimedColumns(rec.Entry, r.Database, r.Measurement)
	if err != nil {
		return nil, bad(err)
	}
	total, err := normalizeReplayColumns(columns)
	if err != nil {
		return nil, bad(err)
	}
	typed, _, err := b.convertColumnsToTyped(r.Measurement, columns)
	if err != nil {
		return nil, bad(fmt.Errorf("convert columns: %w", err))
	}
	times, ok := typed.Data["time"].([]int64)
	if !ok || len(times) != total {
		return nil, bad(errors.New("no time column after conversion"))
	}
	byHour := map[int64][]int{}
	for row, t := range times {
		h := HourBucketID(t)
		byHour[h] = append(byHour[h], row)
	}
	hours := make([]int64, 0, len(byHour))
	for h := range byHour {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(a, b int) bool { return hours[a] < hours[b] })

	piece := CoveragePiece{Instance: r.Instance, CSeq: rec.CSeq, replayable: true, claimEpoch: r.Epoch}
	if rec.Chunks > 1 {
		piece.Chunk, piece.Chunks = rec.Chunk, rec.Chunks
	}
	var subs []replaySub
	var outcomes []HourOutcome
	for _, hid := range hours {
		hour := hourIDToTime(hid)
		keep, err := rec.Keep(hour, total)
		if err != nil {
			outcomes = append(outcomes, HourOutcome{Record: i, Hour: hour, Outcome: ReplayUnreplayable, Err: err})
			continue
		}
		if len(keep) == 0 {
			outcomes = append(outcomes, HourOutcome{Record: i, Hour: hour, Outcome: ReplayCovered})
			continue
		}
		whole := len(keep) == 1 && keep[0] == [2]int{0, total}
		rows := byHour[hid]
		wrote := false
		for _, kr := range keep {
			var idx []int
			for _, row := range rows {
				if row >= kr[0] && row < kr[1] {
					idx = append(idx, row)
				}
			}
			if len(idx) == 0 {
				continue
			}
			p := piece
			if !whole {
				p.RowLo, p.RowHi, p.RowTotal = kr[0], kr[1], total
			}
			sub := sliceTypedColumnBatchByIndices(typed, idx)
			sub.Coverage = []CoveragePiece{p}
			subs = append(subs, replaySub{record: i, hour: hour, batch: sub})
			wrote = true
		}
		if !wrote {
			outcomes = append(outcomes, HourOutcome{Record: i, Hour: hour, Outcome: ReplayNothingLeft})
		}
	}
	return subs, outcomes
}

// claimedColumns returns the columns of a decoded entry, refusing one that
// does not hold rows of database/measurement (the stream key names the
// target; a mismatch would publish rows the index cannot account for).
func claimedColumns(e *wal.Entry, database, measurement string) (map[string][]interface{}, error) {
	switch {
	case e == nil:
		return nil, errors.New("no entry")
	case e.DecodeErr != nil:
		return nil, fmt.Errorf("undecodable entry: %w", e.DecodeErr)
	case e.ColumnarData != nil:
		db := e.ColumnarData.Database
		if db == "" {
			db = "default"
		}
		if db != database || e.ColumnarData.Measurement != measurement {
			return nil, fmt.Errorf("entry holds %s/%s, not %s/%s", db, e.ColumnarData.Measurement, database, measurement)
		}
		cols := make(map[string][]interface{}, len(e.ColumnarData.Columns))
		for k, v := range e.ColumnarData.Columns {
			cols[k] = v
		}
		return cols, nil
	case len(e.Records) > 0:
		db, m, cols, err := WALRowsToColumns(e.Records)
		if err != nil {
			return nil, err
		}
		if db != database || m != measurement {
			return nil, fmt.Errorf("entry holds %s/%s, not %s/%s", db, m, database, measurement)
		}
		return cols, nil
	}
	return nil, errors.New("empty entry")
}

// normalizeReplayColumns applies what a live write applied before the WAL
// copy was taken: equal lengths, timestamps in microseconds, valid UTF-8.
// Unlike WAL recovery it never generates a time column: "now" would put the
// rows in another hour than the origin's copy, past the index.
func normalizeReplayColumns(columns map[string][]interface{}) (int, error) {
	n := -1
	for name, col := range columns {
		if n == -1 {
			n = len(col)
		} else if len(col) != n {
			return 0, fmt.Errorf("column length mismatch (expected %d, got %d for %q)", n, len(col), name)
		}
	}
	if n <= 0 {
		return 0, errors.New("empty columns")
	}
	if _, ok := columns["time"]; !ok {
		return 0, errors.New("no time column")
	}
	if err := normalizeTimestampColumns(columns); err != nil {
		return 0, fmt.Errorf("normalize timestamps: %w", err)
	}
	sanitizeColumnarStrings(columns)
	return n, nil
}

// replaySignature names every column with its Go type, underscore-prefixed
// ones included (getColumnSignature skips those, and mergeBatches would
// panic on a type mismatch in one of them).
func replaySignature(data map[string]interface{}) string {
	parts := make([]string, 0, len(data))
	for name, v := range data {
		parts = append(parts, fmt.Sprintf("%s:%T", name, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// replayGroupFits refuses a group whose coverage would not stay exact.
func replayGroupFits(g []replaySub) error {
	var pieces []CoveragePiece
	for _, s := range g {
		pieces = append(pieces, s.batch.Coverage...)
	}
	ranges, partial := foldPieces(pieces)
	if len(partial) > maxReplayGroupPartials {
		return ErrReplayTooLarge
	}
	raw, err := json.Marshal(struct {
		R []CoverageRange `json:"r"`
		P []CoveragePiece `json:"p"`
	}{ranges, partial})
	if err != nil || len(raw) > maxReplayGroupCoverageBytes {
		return ErrReplayTooLarge
	}
	return nil
}

// flushClaimedGroup writes one group's slices as files of their hours.
// cs is the coverage state ReplayClaimed checked at entry. (The registrar is
// set once at startup; registerFileInTiering reads it again.)
func (b *ArrowBuffer) flushClaimedGroup(ctx context.Context, cs *coverageState, key string, r ClaimedReplay, g []replaySub) error {
	records := make([]interface{}, len(g))
	rows := 0
	for i, s := range g {
		records[i] = s.batch
		if t, ok := s.batch.Data["time"].([]int64); ok {
			rows += len(t)
		}
	}
	group := b.newFlushGroupWith(cs, key, records, nil)
	if group == nil {
		return ErrReplayCoverageOff
	}
	taskID := b.beginAsyncFlushTask()
	defer b.completeAsyncFlushTask(taskID)
	merged, err := b.mergeBatches(records)
	if err != nil {
		group.failed()
		return fmt.Errorf("merge claimed batches: %w", err)
	}
	if err := b.flushPartitionedData(ctx, key, r.Database, r.Measurement, merged, rows, flushTypeSync, time.Now().UTC(), group); err != nil {
		group.failed()
		return err
	}
	b.completeFlush(nil, group)
	return nil
}
