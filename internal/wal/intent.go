package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"strings"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
)

// Flush intents (#1646).
//
// A flush stores its file and then writes the WAL checkpoint that stops
// recovery replaying the file's entries. A crash between the two left a stored
// file and un-checkpointed entries, and recovery replayed them into a second
// file: every row of the flush stored twice, for good on a tagless
// measurement.
//
// Before the storage write, the flush now appends an intent naming the file
// (path and size) and the identities its checkpoint will name. When recovery
// meets an entry that is not checkpointed, it looks for an intent naming it;
// if the intent's file is in storage at that size, the rows are already
// stored, so it writes the checkpoint the crash prevented and skips the
// replay (RecoveryOptions.AdoptIntent decides; this package only carries the
// intent).
//
// The intent forces no sync of its own (the flush's checkpoint, written after
// it, syncs it), and the flush waits for it at most flushIntentAdmission for
// a slot in the WAL queue: a crash before it is durable, or an intent that
// found no slot, is the duplicate there always was, never a loss. It is a checkpoint frame
// holding one string, arc:intent:v1:<msgpack>, so a binary without this
// change reads it as a checkpoint identity that matches no entry.

const flushIntentPrefix = "arc:intent:v1:"

// MaxFlushIntentBytes bounds one intent. A flush naming more is written
// without one (the duplicate risk it always had); a frame past the payload
// limit would read as damage.
const MaxFlushIntentBytes = 1 << 20

// ErrFlushIntentTooLarge: the intent would exceed MaxFlushIntentBytes.
var ErrFlushIntentTooLarge = errors.New("flush intent too large")

// FlushIntent names a file a flush is about to store and the WAL identities
// (tracked tokens or row ranges) whose rows the file holds: exactly what the
// file's checkpoint will name.
type FlushIntent struct {
	Path string   `msgpack:"p"`
	Size int64    `msgpack:"s"`
	IDs  []string `msgpack:"i"`
}

// EncodeFlushIntent encodes an intent as a checkpoint identity.
func EncodeFlushIntent(in FlushIntent) (string, error) {
	b, err := msgpack.Marshal(in)
	if err != nil {
		return "", err
	}
	if len(b)+len(flushIntentPrefix) > MaxFlushIntentBytes {
		return "", ErrFlushIntentTooLarge
	}
	return flushIntentPrefix + string(b), nil
}

// ParseFlushIntent decodes a checkpoint identity written by EncodeFlushIntent.
func ParseFlushIntent(s string) (FlushIntent, bool) {
	if !strings.HasPrefix(s, flushIntentPrefix) {
		return FlushIntent{}, false
	}
	var in FlushIntent
	if err := msgpack.Unmarshal([]byte(s[len(flushIntentPrefix):]), &in); err != nil || in.Path == "" || in.Size <= 0 || len(in.IDs) == 0 {
		return FlushIntent{}, false
	}
	return in, true
}

// flushIntentAdmission bounds the flush's wait for a WAL queue slot. Under
// durable ack the queue is full most of the time and slots are handed to
// waiting senders, so taking only a free slot would drop nearly every intent
// (see markFlushed); a short wait gets one without holding the flush up.
const flushIntentAdmission = 100 * time.Millisecond

// RecordFlushIntent appends an intent, waiting at most flushIntentAdmission
// for room in the WAL queue and never for a sync.
func (w *Writer) RecordFlushIntent(in FlushIntent) error {
	s, err := EncodeFlushIntent(in)
	if err != nil {
		return err
	}
	payload, err := msgpack.Marshal([]string{s})
	if err != nil {
		return err
	}
	frame := append([]byte{WALCheckpointMarker}, payload...)
	data := make([]byte, WALEntryHeaderSize+len(frame))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(frame)))
	binary.BigEndian.PutUint64(data[4:12], uint64(time.Now().UnixMicro()))
	binary.BigEndian.PutUint32(data[12:16], crc32.ChecksumIEEE(frame))
	copy(data[WALEntryHeaderSize:], frame)
	if w.closedFlag.Load() {
		return errors.New("WAL writer is closed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), flushIntentAdmission)
	defer cancel()
	return w.enqueueEntryBlocking(ctx, walEntry{data: data, flushIntent: true})
}

// intentState is one intent seen by a recovery pass.
type intentState struct {
	FlushIntent
	tried bool
}

// intentAdopter indexes the intents among the checkpoint identities of the
// pass and returns the function the replay loop calls for an entry that is
// not checkpointed. It adopts every intent naming that entry (wholly or by
// row range) that is still entirely un-checkpointed, when AdoptIntent says
// its file is stored and CheckpointRecovered makes the intent's identities
// durable; it then adds them to flushed and rowCoverage, so the loop skips
// the rows they name. All or nothing per intent: an intent some of whose
// identities are already checkpointed (a replay stored them) is left to
// replay, so no row is skipped on the strength of a file that may not be the
// only copy.
func (r *Recovery) intentAdopter(ctx context.Context, opts *RecoveryOptions, flushed map[string]struct{}, rowCoverage map[string][]recoveryRowRange, stats *RecoveryStats) func(id string) {
	if opts.AdoptIntent == nil || opts.CheckpointRecovered == nil {
		return func(string) {}
	}
	byParent := make(map[string][]*intentState)
	for id := range flushed {
		in, ok := ParseFlushIntent(id)
		if !ok {
			continue
		}
		st := &intentState{FlushIntent: in}
		seen := make(map[string]bool, len(in.IDs))
		for _, iid := range in.IDs {
			parent := iid
			if p, _, _, isRange := ParseRecoveryRowIdentity(iid); isRange {
				parent = p
			} else if !trackedRowIdentity(iid) {
				parent = "" // not an identity replay can meet
			}
			if parent != "" && !seen[parent] {
				seen[parent] = true
				byParent[parent] = append(byParent[parent], st)
			}
		}
	}
	if len(byParent) == 0 {
		return func(string) {}
	}
	untouched := func(in FlushIntent) bool {
		for _, iid := range in.IDs {
			if _, done := flushed[iid]; done {
				return false
			}
			if p, s, e, isRange := ParseRecoveryRowIdentity(iid); isRange {
				if _, done := flushed[p]; done {
					return false
				}
				for _, c := range rowCoverage[p] {
					if c.start < e && s < c.end {
						return false
					}
				}
			} else if len(rowCoverage[iid]) > 0 {
				return false
			} else if !trackedRowIdentity(iid) {
				return false
			}
		}
		return true
	}
	return func(id string) {
		for _, st := range byParent[id] {
			if st.tried {
				continue
			}
			st.tried = true
			if !untouched(st.FlushIntent) || !opts.AdoptIntent(ctx, st.FlushIntent) {
				continue
			}
			if err := opts.CheckpointRecovered(ctx, st.IDs); err != nil {
				r.logger.Warn().Err(err).Str("storage_path", st.Path).
					Msg("Found the file of an interrupted flush but could not checkpoint its WAL entries; replaying them instead")
				continue
			}
			touched := make(map[string]bool)
			for _, iid := range st.IDs {
				flushed[iid] = struct{}{}
				if p, s, e, isRange := ParseRecoveryRowIdentity(iid); isRange {
					rowCoverage[p] = append(rowCoverage[p], recoveryRowRange{s, e})
					touched[p] = true
				}
			}
			for p := range touched {
				rowCoverage[p] = mergeRowRanges(rowCoverage[p])
			}
			stats.AdoptedFiles++
			r.logger.Info().Str("storage_path", st.Path).Int("entries", len(st.IDs)).
				Msg("A flush stored this file before a crash took its WAL checkpoint; checkpointed its entries instead of replaying them")
		}
	}
}

// mergeRowRanges sorts and merges overlapping or touching ranges.
func mergeRowRanges(ranges []recoveryRowRange) []recoveryRowRange {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
	merged := ranges[:0]
	for _, current := range ranges {
		if len(merged) == 0 || current.start > merged[len(merged)-1].end {
			merged = append(merged, current)
		} else if current.end > merged[len(merged)-1].end {
			merged[len(merged)-1].end = current.end
		}
	}
	return merged
}
