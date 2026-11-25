package compaction

import (
	"sync"
	"time"
)

// settledRetryAfter is how long a settled day whose job ran and left its
// files in place is left alone while those files stay the same (#1774).
const settledRetryAfter = 7 * 24 * time.Hour

// settledMemo remembers settled-day candidates (#1774) whose job ran and made
// no progress: a lone file the job cannot read (bad magic, a key no backend
// can address), files without a time column, or any other failure that is
// not the cycle's own cancellation. Such files stayed out of a day's daily
// file before #1774 too, but nothing selected them again; a settled day is
// selected every cycle, so without this it would fail, or be downloaded and
// skipped, every cycle for good and take a slot of maxSettledCandidatesPerCycle.
//
// Keyed by tier and partition, holding every file a job of the partition ran
// over: a candidate split into batches records each batch it ran (#1775 made
// settled candidates of any size). The day is skipped while every file it
// holds is one a job already ran over; a new file landing in the day makes it
// a new attempt. In memory only, and retried after settledRetryAfter, so a
// failure that was transient is not skipped for good.
type settledMemo struct {
	mu      sync.Mutex
	entries map[string]*settledMemoEntry
	now     func() time.Time
}

type settledMemoEntry struct {
	files map[string]struct{}
	at    time.Time // first record: the retry clock is not reset by later batches
}

func newSettledMemo() *settledMemo {
	return &settledMemo{entries: map[string]*settledMemoEntry{}, now: time.Now}
}

// record notes that a job ran over these files of the partition. A nil memo
// (a Manager not built by NewManager) records nothing.
func (s *settledMemo) record(tier, partition string, files []string) {
	if s == nil {
		return
	}
	key := tier + "|" + partition
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok || s.now().Sub(e.at) >= settledRetryAfter {
		e = &settledMemoEntry{files: map[string]struct{}{}, at: s.now()}
		s.entries[key] = e
	}
	for _, f := range files {
		e.files[f] = struct{}{}
	}
}

// skip reports whether every file of a settled candidate was already run
// over within settledRetryAfter. Any other candidate drops the entry.
func (s *settledMemo) skip(tier, partition string, files []string) bool {
	if s == nil {
		return false
	}
	key := tier + "|" + partition
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	if !ok {
		return false
	}
	covered := len(files) > 0 && s.now().Sub(e.at) < settledRetryAfter
	for _, f := range files {
		if _, ran := e.files[f]; !ran {
			covered = false
			break
		}
	}
	if !covered {
		delete(s.entries, key)
	}
	return covered
}
