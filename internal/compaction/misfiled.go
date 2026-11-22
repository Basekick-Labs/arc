package compaction

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// Misfiled inputs (#1825).
//
// A partition is a directory, and a compaction job merges every file of it.
// Nothing ties a file's rows to the directory it sits in: ingest files each
// row under the UTC hour of its time, so for files Arc writes the two agree,
// but a file written by an older version (times before 1970 before 26.06.2,
// ingestion-time directories before 26.02.1), or restored or synced from such
// a store, or placed by hand, can hold rows of another hour or day. Merged
// with its neighbours it becomes part of an output that spans two days: the
// Iceberg export cannot give such a file a day and leaves it out (#1754), so
// rows that were in the table leave it.
//
// The job therefore reads each input's time range before it merges anything.
// If any input has a row outside the partition, the job does nothing: it
// compacts no subset of its own choosing, so every rule about how many files
// a job needs, and which, stays with the parent that formed the candidate.
// The parent remembers the file (misfiledMemo) and forms later candidates
// without it.

// MisfiledInput is an input a job refused: it holds a row outside the
// partition the file sits in.
type MisfiledInput struct {
	Path string `json:"path"` // storage key
	// MinTime and MaxTime are the file's smallest and largest time, as the
	// compaction COPY would write them.
	MinTime time.Time `json:"min_time"`
	MaxTime time.Time `json:"max_time"`
	// OutsideDay: a row is outside the UTC day of the directory too, so the
	// daily tier would refuse the file as well.
	OutsideDay bool `json:"outside_day,omitempty"`
}

// errMisfiledInputs is compactFiles' report that the job has inputs it must
// not merge. Run ends the job without compacting anything.
type errMisfiledInputs struct{ files []MisfiledInput }

func (e *errMisfiledInputs) Error() string {
	return fmt.Sprintf("%d input file(s) hold rows outside the partition", len(e.files))
}

const (
	tierNameHourly = "hourly"
	tierNameDaily  = "daily"
)

// partitionWindow is the half-open range of times a partition's files may
// hold: the hour for the hourly tier, the UTC day for the daily tier. ok is
// false when the job does not know its partition's time or tier.
func partitionWindow(tier string, partitionTime time.Time) (start, end time.Time, ok bool) {
	if partitionTime.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	start = partitionTime.UTC()
	switch tier {
	case tierNameHourly:
		return start, start.Add(time.Hour), true
	case tierNameDaily:
		return start, start.Add(24 * time.Hour), true
	}
	return time.Time{}, time.Time{}, false
}

// fileTimeRangeQuery reads the smallest and largest time of one file, in
// microseconds since the epoch, through the normalization the compaction COPY
// applies (timeNormalizeReplace): what is compared with the partition is what
// the output would hold. Footer statistics are not used: they are absent from
// files written without them, and are not times at all for a "time" column an
// old writer stored as a string. quotedPath is a quoted SQL string literal.
func fileTimeRangeQuery(quotedPath string) string {
	normalized := strings.TrimSuffix(timeNormalizeReplace, ` AS "time"`)
	return fmt.Sprintf(`SELECT min(epoch_us(t)), max(epoch_us(t)) FROM (SELECT %s AS t FROM %s)`,
		normalized, sqlutil.ReadParquet(quotedPath))
}

// fileTimeRange returns a file's time range. found is false when the file has
// no "time" column or no usable time value: such a file holds no row that can
// be outside a partition.
func fileTimeRange(ctx context.Context, db *sql.DB, localPath string) (lo, hi time.Time, found bool, err error) {
	quoted := sqlutil.QuoteStringLiteral(localPath)
	cols, err := parquetFilesColumns(ctx, db, quoted)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	// Any case: DuckDB binds "time" to a column named "Time" too, and the
	// compaction COPY would merge its rows.
	if _, ok := cols["time"]; !ok {
		return time.Time{}, time.Time{}, false, nil
	}
	var minUS, maxUS sql.NullInt64
	if err := db.QueryRowContext(ctx, fileTimeRangeQuery(quoted)).Scan(&minUS, &maxUS); err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("read the time range of %s: %w", filepath.Base(localPath), err)
	}
	if !minUS.Valid || !maxUS.Valid {
		return time.Time{}, time.Time{}, false, nil
	}
	return time.UnixMicro(minUS.Int64).UTC(), time.UnixMicro(maxUS.Int64).UTC(), true, nil
}

// misfiledInputs returns the inputs that hold a row outside the job's
// partition. A read that fails is an error, not a pass: a file let through
// unread could be the one that must not be merged.
func (j *Job) misfiledInputs(ctx context.Context, db *sql.DB, localPaths, storageKeys []string) ([]MisfiledInput, error) {
	if !j.checkInputWindow {
		return nil, nil
	}
	start, end, ok := partitionWindow(j.Tier, j.PartitionTime)
	if !ok {
		return nil, nil
	}
	dayStart := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)
	var out []MisfiledInput
	for i, p := range localPaths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lo, hi, found, err := fileTimeRange(ctx, db, p)
		if err != nil {
			return nil, err
		}
		if !found || (!lo.Before(start) && hi.Before(end)) {
			continue
		}
		out = append(out, MisfiledInput{
			Path: storageKeys[i], MinTime: lo, MaxTime: hi,
			OutsideDay: lo.Before(dayStart) || !hi.Before(dayEnd),
		})
	}
	return out, nil
}

// MisfiledEntry is one file a compaction tier leaves alone because a job
// found a row in it outside the partition it sits in.
type MisfiledEntry struct {
	Tier      string    `json:"tier"`
	Path      string    `json:"path"`
	Partition string    `json:"partition"`
	MinTime   time.Time `json:"min_time"`
	MaxTime   time.Time `json:"max_time"`
	FoundAt   time.Time `json:"found_at"`
}

const misfiledFile = "misfiled.json"

// maxMisfiledEntries bounds the memo. Past it, further files are not
// remembered: their jobs refuse them again each cycle, which costs a download
// and a read and loses nothing.
const maxMisfiledEntries = 100_000

// misfiledMemo is the files compaction leaves alone, kept on local disk
// beside the too-wide memo so a restart does not download and read them all
// again. It is this node's: another node that takes over compaction finds
// them again, one refused job per partition.
type misfiledMemo struct {
	mu      sync.Mutex
	path    string // "" keeps the memo in memory only
	entries map[string]MisfiledEntry
	// dirty: a change to a large list not written yet (saveLocked).
	dirty  bool
	logger zerolog.Logger
}

// misfiledSaveEachChange is the list size up to which every change is
// written at once. A larger list is written once per cycle (flush): the
// whole file is rewritten each time, under the lock every candidate filter
// takes. Entries not yet written when the process stops are found again.
const misfiledSaveEachChange = 2000

func misfiledKey(tier, path string) string { return tier + "\x00" + path }

func newMisfiledMemo(dir string, logger zerolog.Logger) *misfiledMemo {
	m := &misfiledMemo{entries: map[string]MisfiledEntry{}, logger: logger}
	if dir == "" {
		return m
	}
	m.path = filepath.Join(dir, misfiledFile)
	data, err := os.ReadFile(m.path)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn().Err(err).Str("path", m.path).Msg("Could not read the misfiled-inputs memo; compaction finds those files again")
		}
		return m
	}
	var entries []MisfiledEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		logger.Warn().Err(err).Str("path", m.path).Msg("The misfiled-inputs memo is not readable; compaction finds those files again")
		return m
	}
	for _, e := range entries {
		m.entries[misfiledKey(e.Tier, e.Path)] = e
	}
	return m
}

// record remembers files a job of tier refused, in one write. A file whose
// rows leave the directory's day is remembered for the daily tier too, which
// would refuse it for the same reason after downloading the whole day. It
// returns how many entries are new.
func (m *misfiledMemo) record(tier, partition string, files []MisfiledInput, now time.Time) int {
	if m == nil || len(files) == 0 {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	added := 0
	add := func(t string, f MisfiledInput) {
		key := misfiledKey(t, f.Path)
		if _, ok := m.entries[key]; ok || len(m.entries) >= maxMisfiledEntries {
			return
		}
		m.entries[key] = MisfiledEntry{Tier: t, Path: f.Path, Partition: partition, MinTime: f.MinTime, MaxTime: f.MaxTime, FoundAt: now.UTC()}
		added++
	}
	for _, f := range files {
		add(tier, f)
		if tier == tierNameHourly && f.OutsideDay {
			add(tierNameDaily, f)
		}
	}
	if added > 0 {
		m.saveLocked()
	}
	return added
}

// has reports whether tier leaves path alone.
func (m *misfiledMemo) has(tier, path string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return false
	}
	_, ok := m.entries[misfiledKey(tier, path)]
	return ok
}

// without returns files less those tier leaves alone, and how many it dropped.
func (m *misfiledMemo) without(tier string, files []string) ([]string, int) {
	if m == nil {
		return files, 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) == 0 {
		return files, 0
	}
	kept := make([]string, 0, len(files))
	for _, f := range files {
		if _, ok := m.entries[misfiledKey(tier, f)]; !ok {
			kept = append(kept, f)
		}
	}
	return kept, len(files) - len(kept)
}

// list returns the entries, by path then tier.
func (m *misfiledMemo) list() []MisfiledEntry {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.listLocked()
}

func (m *misfiledMemo) listLocked() []MisfiledEntry {
	out := make([]MisfiledEntry, 0, len(m.entries))
	for _, e := range m.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Path != out[b].Path {
			return out[a].Path < out[b].Path
		}
		return out[a].Tier < out[b].Tier
	})
	return out
}

// files is how many distinct files the memo holds.
func (m *misfiledMemo) files() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{}, len(m.entries))
	for _, e := range m.entries {
		seen[e.Path] = struct{}{}
	}
	return len(seen)
}

// forgetPaths drops every entry of the given files and returns how many it
// dropped.
func (m *misfiledMemo) forgetPaths(paths []string) int {
	if m == nil || len(paths) == 0 {
		return 0
	}
	drop := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		drop[p] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, e := range m.entries {
		if _, ok := drop[e.Path]; ok {
			delete(m.entries, k)
			n++
		}
	}
	if n > 0 {
		m.saveLocked()
	}
	return n
}

// flush writes a change saveLocked put off.
func (m *misfiledMemo) flush() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dirty {
		m.writeLocked()
	}
}

// forget drops the entries of path (every path when it is empty), for tier
// (every tier when it is empty), and returns how many it dropped.
func (m *misfiledMemo) forget(tier, path string) int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, e := range m.entries {
		if (path == "" || e.Path == path) && (tier == "" || e.Tier == tier) {
			delete(m.entries, k)
			n++
		}
	}
	if n > 0 {
		m.saveLocked()
	}
	return n
}

// paths returns the distinct files the memo holds.
func (m *misfiledMemo) paths() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{}, len(m.entries))
	var out []string
	for _, e := range m.entries {
		if _, ok := seen[e.Path]; !ok {
			seen[e.Path] = struct{}{}
			out = append(out, e.Path)
		}
	}
	sort.Strings(out)
	return out
}

func (m *misfiledMemo) saveLocked() {
	if len(m.entries) > misfiledSaveEachChange {
		m.dirty = true
		return
	}
	m.writeLocked()
}

func (m *misfiledMemo) writeLocked() {
	m.dirty = false
	if m.path == "" {
		return
	}
	data, err := json.Marshal(m.listLocked())
	if err == nil {
		err = writeFileAtomic(m.path, data)
	}
	if err != nil {
		m.logger.Warn().Err(err).Str("path", m.path).Msg("Could not save the misfiled-inputs memo; after a restart compaction finds those files again")
	}
}
