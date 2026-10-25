package compaction

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// A partition too wide to compact in the memory a compaction job has (a table
// of thousands of columns) fails at every batch size: the splitter halves the
// batch down to its smallest and the last attempt fails like the first. Before
// this memo every cycle repeated that ladder, downloading the files again each
// time, for good. Now the partition is remembered and left alone while its
// files and the settings that size a job stay the same.
//
// The memo decides from outcomes, not from an estimate of the memory a batch
// needs: no estimate calibrated (2026-10-25: a 30-file batch of 551 columns
// fits that a model skipped; 701 columns fits at 491k rows and runs out of
// memory at 3.93M), and RSS runs at 1.3-2x memory_limit.

// tooWideFile is the memo's file under compaction.temp_directory. A top-level
// file: CleanupOrphanedTempDirs removes directories only.
const tooWideFile = "too_wide.json"

// errUnsplittable reports a batch that failed with a recoverable error (out
// of memory, killed, crashed) when the splitter could not halve it again. It
// wraps the attempt's error.
type errUnsplittable struct {
	reason string
	err    error
}

func (e *errUnsplittable) Error() string {
	return fmt.Sprintf("compaction failed at the smallest batch the splitter can make (%s): %v", e.reason, e.err)
}

func (e *errUnsplittable) Unwrap() error { return e.err }

// errSlowAttempt reports an attempt the cycle deadline cut off after it had
// run for at least half the cycle budget: a partition that spills instead of
// failing never reaches the smallest batch. It wraps the context error, so
// the cycle still counts the batch interrupted.
type errSlowAttempt struct {
	ran time.Duration
	err error
}

func (e *errSlowAttempt) Error() string {
	return fmt.Sprintf("compaction attempt cut off by the cycle deadline after %s: %v", e.ran.Round(time.Second), e.err)
}

func (e *errSlowAttempt) Unwrap() error { return e.err }

// memoryEvidence are the classifier reasons that point at the job's memory.
// "unknown" is not among them: a full disk, object-store throttling or an
// expired credential falls there, and must not leave a healthy partition
// uncompacted.
var memoryEvidence = map[string]bool{"memory_error": true, "oom_killed": true, "killed": true, "segfault": true, "aborted": true}

// tooWideReason is the memo's reason for a failure, "" when the failure does
// not count toward it.
func tooWideReason(err error) string {
	var u *errUnsplittable
	if errors.As(err, &u) {
		if memoryEvidence[u.reason] {
			return u.reason
		}
		return ""
	}
	var s *errSlowAttempt
	if errors.As(err, &s) {
		return "timeout"
	}
	return ""
}

// strikesNeeded is how many cycles in a row must fail to mark a partition
// (one strike per partition per cycle). DuckDB's own out-of-memory error is
// that job's limit: one is enough, and it holds until the files or the job
// sizing change. A kill may have hit another job running alongside (the
// kernel picks the largest process), a crash can be transient, and a cycle
// deadline depends on what else the cycle ran: two, and the mark expires
// after tooWideRetryAfter so the partition is tried again once a day.
func strikesNeeded(reason string) int {
	if reason == "memory_error" {
		return 1
	}
	return 2
}

// tooWideRetryAfter is how long a mark on weaker evidence than DuckDB's
// out-of-memory error holds.
const tooWideRetryAfter = 24 * time.Hour

// TooWideEntry is one memoized partition.
type TooWideEntry struct {
	Tier      string `json:"tier"`
	Partition string `json:"partition"`
	// Files is every file the partition held when it failed; the memo holds
	// while the partition has no file outside it other than this tier's
	// own outputs (a sibling batch that succeeded).
	Files   []string `json:"files"`
	Reason  string   `json:"reason"`
	Strikes int      `json:"strikes"`
	// Settings are the job sizing in force when it failed; a change (more
	// memory per job, fewer files per batch) retries the partition.
	Settings     string    `json:"settings"`
	FirstFailure time.Time `json:"first_failure"`
	LastFailure  time.Time `json:"last_failure"`
}

// Memoized reports whether the entry has enough strikes to skip, under any
// settings and with any files.
func (e TooWideEntry) Memoized() bool { return e.Strikes >= strikesNeeded(e.Reason) }

// Skips reports whether a cycle running with settings at now leaves the
// partition alone (files aside).
func (e TooWideEntry) Skips(settings string, now time.Time) bool {
	if !e.Memoized() || e.Settings != settings {
		return false
	}
	return e.Reason == "memory_error" || now.Sub(e.LastFailure) < tooWideRetryAfter
}

// tooWideMemo is safe to use as nil (a Manager not built by NewManager): it
// then remembers nothing.
type tooWideMemo struct {
	mu      sync.Mutex
	path    string // "" keeps it in memory only
	entries map[string]*TooWideEntry
	logger  zerolog.Logger
}

func tooWideKey(tier, partition string) string { return tier + "\x00" + partition }

// newTooWideMemo loads the memo from dir; an unreadable file starts empty.
func newTooWideMemo(dir string, logger zerolog.Logger) *tooWideMemo {
	m := &tooWideMemo{entries: map[string]*TooWideEntry{}, logger: logger}
	if dir == "" {
		return m
	}
	m.path = filepath.Join(dir, tooWideFile)
	data, err := os.ReadFile(m.path)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn().Err(err).Str("file", m.path).Msg("Could not read the list of partitions too wide to compact; every partition is tried again")
		}
		return m
	}
	var list []TooWideEntry
	if err := json.Unmarshal(data, &list); err != nil {
		logger.Warn().Err(err).Str("file", m.path).Msg("Could not parse the list of partitions too wide to compact; every partition is tried again")
		return m
	}
	for i := range list {
		e := list[i]
		m.entries[tooWideKey(e.Tier, e.Partition)] = &e
	}
	return m
}

// skip reports whether a candidate is memoized: enough strikes under the same
// settings, and no file the memo has not seen except the tier's own outputs.
func (m *tooWideMemo) skip(tier Tier, partition string, files []string, settings string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[tooWideKey(tier.GetTierName(), partition)]
	if !ok || !e.Skips(settings, time.Now()) {
		return false
	}
	known := make(map[string]struct{}, len(e.Files))
	for _, f := range e.Files {
		known[f] = struct{}{}
	}
	for _, f := range files {
		if _, seen := known[f]; !seen && !tier.IsCompactedFile(f) {
			return false
		}
	}
	return true
}

// strike records a failure that counts; it reports whether the partition is
// memoized now and was not before.
func (m *tooWideMemo) strike(tier, partition string, files []string, reason, settings string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tooWideKey(tier, partition)
	e, ok := m.entries[key]
	now := time.Now().UTC()
	if !ok || e.Settings != settings {
		e = &TooWideEntry{Tier: tier, Partition: partition, Settings: settings, FirstFailure: now}
		m.entries[key] = e
	}
	was := e.Memoized()
	set := make(map[string]struct{}, len(e.Files)+len(files))
	for _, f := range e.Files {
		set[f] = struct{}{}
	}
	for _, f := range files {
		set[f] = struct{}{}
	}
	e.Files = e.Files[:0]
	for f := range set {
		e.Files = append(e.Files, f)
	}
	sort.Strings(e.Files)
	e.Reason = reason
	e.Strikes++
	e.LastFailure = now
	m.saveLocked()
	return e.Memoized() && !was
}

// clear forgets a partition (it compacted).
func (m *tooWideMemo) clear(tier, partition string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tooWideKey(tier, partition)
	if _, ok := m.entries[key]; !ok {
		return
	}
	delete(m.entries, key)
	m.saveLocked()
}

// clearAll forgets every partition, or those of one tier and partition path
// when given; it returns how many it removed.
func (m *tooWideMemo) clearAll(tier, partition string) int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for key, e := range m.entries {
		if (tier == "" || e.Tier == tier) && (partition == "" || e.Partition == partition) {
			delete(m.entries, key)
			n++
		}
	}
	if n > 0 {
		m.saveLocked()
	}
	return n
}

// prune forgets the entries of the tiers a full-scope cycle covered whose
// partition it did not discover (files gone: retention, a database delete).
func (m *tooWideMemo) prune(tiers map[string]bool, seen map[string]bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for key, e := range m.entries {
		if tiers[e.Tier] && !seen[key] {
			delete(m.entries, key)
			n++
		}
	}
	if n > 0 {
		m.saveLocked()
	}
}

// list returns the entries, sorted.
func (m *tooWideMemo) list() []TooWideEntry {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listLocked()
}

func (m *tooWideMemo) listLocked() []TooWideEntry {
	out := make([]TooWideEntry, 0, len(m.entries))
	for _, e := range m.entries {
		c := *e
		c.Files = append([]string(nil), e.Files...)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tier != out[j].Tier {
			return out[i].Tier < out[j].Tier
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// memoized counts the entries a cycle with settings would skip.
func (m *tooWideMemo) memoized(settings string) int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	now := time.Now()
	for _, e := range m.entries {
		if e.Skips(settings, now) {
			n++
		}
	}
	return n
}

// saveLocked writes the memo atomically. A failure is logged: the memo still
// holds in memory until the process stops.
func (m *tooWideMemo) saveLocked() {
	if m.path == "" {
		return
	}
	data, err := json.Marshal(m.listLocked())
	if err == nil {
		err = writeFileAtomic(m.path, data)
	}
	if err != nil {
		m.logger.Warn().Err(err).Str("file", m.path).Msg("Could not save the list of partitions too wide to compact; it holds until restart")
	}
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(name, 0o600); err != nil {
		return err
	}
	return os.Rename(name, path)
}
