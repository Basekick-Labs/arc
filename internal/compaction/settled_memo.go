package compaction

import (
	"sort"
	"strings"
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
// Keyed by tier and partition, by the exact file set: a file landing in or
// leaving the day makes it a new attempt. In memory only, and retried after
// settledRetryAfter, so a failure that was transient is not skipped for good.
type settledMemo struct {
	mu      sync.Mutex
	entries map[string]settledMemoEntry
	now     func() time.Time
}

type settledMemoEntry struct {
	files string
	at    time.Time
}

func newSettledMemo() *settledMemo {
	return &settledMemo{entries: map[string]settledMemoEntry{}, now: time.Now}
}

func settledFileSet(files []string) string {
	sorted := append([]string(nil), files...)
	sort.Strings(sorted)
	return strings.Join(sorted, "\n")
}

// record notes that a job ran over exactly these files of the partition.
// A nil memo (a Manager not built by NewManager) records nothing.
func (s *settledMemo) record(tier, partition string, files []string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[tier+"|"+partition] = settledMemoEntry{files: settledFileSet(files), at: s.now()}
}

// skip reports whether a settled candidate with these files was already run
// within settledRetryAfter. A different file set drops the entry.
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
	if e.files != settledFileSet(files) || s.now().Sub(e.at) >= settledRetryAfter {
		delete(s.entries, key)
		return false
	}
	return true
}
