package rehome

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// A job swaps one misfiled file, the source, for the files its rows are split
// into, the outputs. Its record is a journal file in the storage root, and
// the registry is what the rest of the process asks about jobs in flight:
// which files reads must leave out, and which files nothing else may touch.
//
// States, and what a read of the measurement sees in each:
//
//	preparing  in memory only. The source is being split in a scratch
//	           directory. Reads: the source. Nothing left out.
//	staged     journaled. The outputs are durable under StagedRoot, which no
//	           read globs. Reads: the source. Nothing left out.
//	committed  journaled. The outputs are being moved to their final keys.
//	           Reads leave out EVERY output key, there yet or not: the source.
//	latched    journaled. Every output is at its final key. Reads leave out
//	           the source: the outputs. The source is removed after the reads
//	           from before the latch have left (internal/readgate).
//
// The exclusions name every output from the moment the job commits, not only
// the ones in place: a query's exclusions are fixed when its SQL is built and
// its files when its statement is bound, later, so a query built when two of
// five outputs were in place would also read the three that land before it
// binds, beside the source.
//
// The source and the outputs' final keys are owned by the job from preparing
// until it ends: compaction, DELETE, retention and a database delete leave an
// owned file alone (Owns, Claim).

// Reserved roots in the storage root. Any top-level name starting with "_"
// is skipped by every walker that asks storage.IsReservedRootDir.
const (
	// JournalRoot holds one record per job: _rehome/<id>.json.
	JournalRoot = "_rehome"
	// StagedRoot holds a job's outputs before they are moved into place:
	// _rehome_staged/<id>/<n>.parquet.
	StagedRoot = "_rehome_staged"
)

// State is where a job is.
type State string

const (
	StatePreparing State = "preparing"
	StateStaged    State = "staged"
	StateCommitted State = "committed"
	StateLatched   State = "latched"
)

// FileRef is a file a job reads or writes.
type FileRef struct {
	// Key is the file's storage key: the source's, or an output's final key.
	Key string `json:"key"`
	// Staged is an output's key under StagedRoot; empty for the source.
	Staged string `json:"staged,omitempty"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	// Start is the start of the output's hour or day; Rows its row count.
	Start     time.Time `json:"start,omitempty"`
	Rows      int64     `json:"rows,omitempty"`
	Remainder bool      `json:"remainder,omitempty"`
	// Tier is where the source is: "" for the hot tier, TierCold for the
	// cold one. Set when the job starts and never changed; outputs are
	// always written to the hot tier. ColdStore identifies the cold store
	// the source was in then, so a job taken up against another bucket or
	// prefix does not take "not there" for "removed".
	Tier      string `json:"tier,omitempty"`
	ColdStore string `json:"cold_store,omitempty"`
}

// TierCold is FileRef.Tier for a source in cold storage.
const TierCold = "cold"

// Cold reports a source that is in cold storage.
func (f FileRef) Cold() bool { return f.Tier == TierCold }

// Job is one swap.
type Job struct {
	ID          string    `json:"id"`
	Database    string    `json:"database"`
	Measurement string    `json:"measurement"`
	Source      FileRef   `json:"source"`
	Outputs     []FileRef `json:"outputs"`
	State       State     `json:"state"`
	// Cluster: a job the cluster drives (#1860), recorded on this node so
	// its reads leave the right files out whatever its Raft state after a
	// restart. Recover leaves it alone; this node's part of the job ends it
	// (at removed or reverted), or SweepLocal once the job is gone.
	Cluster   bool      `json:"cluster,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Prefix is the measurement's key prefix, as read exclusions are asked for.
func (j *Job) Prefix() string { return j.Database + "/" + j.Measurement + "/" }

func (j *Job) clone() *Job {
	c := *j
	c.Outputs = append([]FileRef(nil), j.Outputs...)
	return &c
}

func journalKey(id string) string { return JournalRoot + "/" + id + ".json" }

// StagedDir is the directory a job's outputs are staged in.
func StagedDir(id string) string { return StagedRoot + "/" + id }

// validID: a job id is hex, as the handler makes it, so a record's own name
// cannot point anywhere else.
func validID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// validate checks a record read from storage: it sits under its own id, is in
// a journaled state, and names only data files of one measurement, with its
// staged files under its own staged directory.
func (j *Job) validate(id string) error {
	if j.ID != id || !validID(id) {
		return fmt.Errorf("the record's id %q is not its file's", j.ID)
	}
	switch j.State {
	case StateStaged, StateCommitted, StateLatched:
	default:
		return fmt.Errorf("state %q is not one a record is written in", j.State)
	}
	src, err := ParseKey(j.Source.Key)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if src.Database != j.Database || src.Measurement != j.Measurement {
		return fmt.Errorf("source %s is not of %s/%s", j.Source.Key, j.Database, j.Measurement)
	}
	if len(j.Outputs) == 0 {
		return errors.New("no outputs")
	}
	seen := map[string]bool{j.Source.Key: true}
	for _, o := range j.Outputs {
		out, err := ParseKey(o.Key)
		if err != nil {
			return fmt.Errorf("output: %w", err)
		}
		if out.Database != j.Database || out.Measurement != j.Measurement {
			return fmt.Errorf("output %s is not of %s/%s", o.Key, j.Database, j.Measurement)
		}
		if seen[o.Key] {
			return fmt.Errorf("%s is named twice", o.Key)
		}
		seen[o.Key] = true
		if !strings.HasPrefix(o.Staged, StagedDir(id)+"/") || strings.Contains(o.Staged, "..") {
			return fmt.Errorf("staged file %q is not under the job's staged directory", o.Staged)
		}
		// A size of -1 is what a missing file stats as: a record with one
		// would pass "the output is in place at its size".
		if o.Size <= 0 || o.SHA256 == "" {
			return fmt.Errorf("output %s has no size or hash", o.Key)
		}
	}
	if j.Source.Size <= 0 || j.Source.SHA256 == "" {
		return errors.New("the source has no size or hash")
	}
	if j.Source.Tier != "" && j.Source.Tier != TierCold {
		return fmt.Errorf("the source's tier is %q", j.Source.Tier)
	}
	if j.Source.Cold() && j.Source.ColdStore == "" {
		return errors.New("the source is in cold storage and the record does not say which store")
	}
	return nil
}

// Registry is the jobs in flight on this node.
type Registry struct {
	store storage.Backend

	mu     sync.Mutex
	jobs   map[string]*Job
	owned  map[string]string // storage key -> job id
	claims map[string]int    // storage key -> holders (Claim)
	blocks map[string]int    // key prefix -> holders (Block)

	// gen moves when a job starts leaving files out of reads (committed):
	// it is part of the query transform cache's key.
	gen atomic.Uint64
	// seeded: the journal was read. Until then every path is owned and a
	// strict read fails: an unread journal may hold a job whose files
	// compaction would merge or retention delete.
	seeded atomic.Bool
}

// NewRegistry creates a registry over the storage root. Call Seed before
// anything lists data files.
func NewRegistry(store storage.Backend) *Registry {
	return &Registry{store: store, jobs: map[string]*Job{}, owned: map[string]string{}, claims: map[string]int{}, blocks: map[string]int{}}
}

// Seed reads every record under JournalRoot. A record that cannot be read or
// is not valid is an error naming it: the caller must not go on.
func (r *Registry) Seed(ctx context.Context) error {
	if r == nil {
		return nil
	}
	keys, err := r.store.List(ctx, JournalRoot+"/")
	if err != nil {
		return fmt.Errorf("list %s/: %w", JournalRoot, err)
	}
	jobs := map[string]*Job{}
	for _, k := range keys {
		name := strings.TrimPrefix(k, JournalRoot+"/")
		id, ok := strings.CutSuffix(name, ".json")
		if !ok || strings.Contains(name, "/") {
			continue // not a record: a writer's temporary file
		}
		data, err := r.store.Read(ctx, k)
		if err != nil {
			if storage.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("read %s: %w", k, err)
		}
		var j Job
		if err := json.Unmarshal(data, &j); err != nil {
			return fmt.Errorf("%s is not a re-home job record (%v); move the file away to start, after checking by hand that the files it names are each read once", k, err)
		}
		if err := j.validate(id); err != nil {
			return fmt.Errorf("%s is not a valid re-home job record (%v); move the file away to start, after checking by hand that the files it names are each read once", k, err)
		}
		jobs[id] = &j
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs = jobs
	r.owned = map[string]string{}
	for id, j := range jobs {
		r.owned[j.Source.Key] = id
		for _, o := range j.Outputs {
			r.owned[o.Key] = id
		}
		if j.State == StateCommitted || j.State == StateLatched {
			r.gen.Add(1)
		}
	}
	r.seeded.Store(true)
	return nil
}

// ErrOwned: a file of the request is owned by a job in flight.
var ErrOwned = errors.New("a re-home job in flight owns this file")

// Begin registers a job that is about to split its source: the source is
// owned from now. It fails if the source is owned or claimed, or the
// registry is not seeded.
func (r *Registry) Begin(j *Job) error {
	if !r.seeded.Load() {
		return errors.New("the re-home journal has not been read")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.jobs[j.ID]; ok {
		return fmt.Errorf("job %s exists", j.ID)
	}
	if _, ok := r.owned[j.Source.Key]; ok {
		return fmt.Errorf("%s: %w", j.Source.Key, ErrOwned)
	}
	if r.claims[j.Source.Key] > 0 || r.blockedLocked(j.Source.Key) {
		return fmt.Errorf("%s is being deleted, rewritten or backed up by another operation", j.Source.Key)
	}
	j.State = StatePreparing
	r.jobs[j.ID] = j.clone()
	r.owned[j.Source.Key] = j.ID
	return nil
}

// SetOutputs adds a preparing job's outputs: their final keys are owned from
// now. It fails if one is owned or claimed.
func (r *Registry) SetOutputs(id string, outputs []FileRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok || j.State != StatePreparing {
		return fmt.Errorf("job %s is not preparing", id)
	}
	for _, o := range outputs {
		if owner, ok := r.owned[o.Key]; ok && owner != id {
			return fmt.Errorf("%s: %w", o.Key, ErrOwned)
		}
		if r.claims[o.Key] > 0 || r.blockedLocked(o.Key) {
			return fmt.Errorf("%s is being deleted, rewritten or backed up by another operation", o.Key)
		}
	}
	j.Outputs = append([]FileRef(nil), outputs...)
	for _, o := range outputs {
		r.owned[o.Key] = id
	}
	return nil
}

// SetSource records a preparing job's source size and hash, read while it
// was copied.
func (r *Registry) SetSource(id string, src FileRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok || j.State != StatePreparing || j.Source.Key != src.Key {
		return fmt.Errorf("job %s is not preparing %s", id, src.Key)
	}
	j.Source = src
	return nil
}

// next is the one state a job may move to from each.
var next = map[State]State{StatePreparing: StateStaged, StateStaged: StateCommitted, StateCommitted: StateLatched}

// Advance journals the job in its next state, then makes that state the one
// reads and owners see. The journal is written first: a crash between the
// two leaves a record recovery acts on, never a state only memory knew.
func (r *Registry) Advance(ctx context.Context, id string, to State) error {
	r.mu.Lock()
	j, ok := r.jobs[id]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("no job %s", id)
	}
	if next[j.State] != to {
		r.mu.Unlock()
		return fmt.Errorf("job %s is %s; it cannot become %s", id, j.State, to)
	}
	rec := j.clone()
	r.mu.Unlock()

	rec.State, rec.UpdatedAt = to, time.Now().UTC()
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := r.store.Write(ctx, journalKey(id), data); err != nil {
		return fmt.Errorf("write the job's record: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if j, ok := r.jobs[id]; ok {
		j.State, j.UpdatedAt = rec.State, rec.UpdatedAt
		if to == StateCommitted {
			// Reads of the measurement start leaving files out: a cached
			// transform from before must not be served.
			r.gen.Add(1)
		}
	}
	return nil
}

// Mirror records a job the cluster drives (#1860) on this node, durably, in
// state committed (reads leave every output out) or latched (they leave the
// source out), and then makes it the state reads see. The record is written
// first, as Advance does. It may be called again with the same or the next
// state; a job of this node's own (not Cluster) under the id is an error.
func (r *Registry) Mirror(ctx context.Context, j *Job) error {
	if !r.seeded.Load() {
		return errors.New("the re-home journal has not been read")
	}
	if j.State != StateCommitted && j.State != StateLatched {
		return fmt.Errorf("a cluster job is recorded committed or latched, not %s", j.State)
	}
	rec := j.clone()
	rec.Cluster = true
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	rec.UpdatedAt = time.Now().UTC()
	if err := rec.validate(rec.ID); err != nil {
		return err
	}
	r.mu.Lock()
	if cur, ok := r.jobs[rec.ID]; ok {
		if !cur.Cluster {
			r.mu.Unlock()
			return fmt.Errorf("job %s is this node's own", rec.ID)
		}
		if cur.State == StateLatched && rec.State == StateCommitted {
			r.mu.Unlock()
			return fmt.Errorf("job %s is latched here; it cannot go back to committed", rec.ID)
		}
		rec.CreatedAt = cur.CreatedAt
	}
	for _, k := range append([]string{rec.Source.Key}, outputKeys(rec)...) {
		if owner, ok := r.owned[k]; ok && owner != rec.ID {
			r.mu.Unlock()
			return fmt.Errorf("%s: %w", k, ErrOwned)
		}
		// A backup or restore here, or a retention delete about to act:
		// the step waits for it (it is retried).
		if r.claims[k] > 0 || r.blockedLocked(k) {
			r.mu.Unlock()
			return fmt.Errorf("%s is being deleted, backed up or restored by another operation", k)
		}
	}
	r.mu.Unlock()

	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := r.store.Write(ctx, journalKey(rec.ID), data); err != nil {
		return fmt.Errorf("write the job's record: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	prev, had := r.jobs[rec.ID]
	r.jobs[rec.ID] = rec
	r.owned[rec.Source.Key] = rec.ID
	for _, k := range outputKeys(rec) {
		r.owned[k] = rec.ID
	}
	if !had || prev.State != rec.State {
		r.gen.Add(1)
	}
	return nil
}

func outputKeys(j *Job) []string {
	out := make([]string, len(j.Outputs))
	for i, o := range j.Outputs {
		out[i] = o.Key
	}
	return out
}

// HasCluster reports a cluster job this node records (Mirror).
func (r *Registry) HasCluster(id string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	return ok && j.Cluster
}

// End removes the job's record and then the job. The record goes first: a
// crash between the two is a job with no record, which a restart forgets.
func (r *Registry) End(ctx context.Context, id string) error {
	if err := r.store.Delete(ctx, journalKey(id)); err != nil && !storage.IsNotFound(err) {
		return fmt.Errorf("remove the job's record: %w", err)
	}
	r.Drop(id)
	return nil
}

// Drop forgets a job that has no record (it never staged, or its record is
// gone).
func (r *Registry) Drop(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.jobs[id]; !ok {
		return
	}
	delete(r.jobs, id)
	for k, owner := range r.owned {
		if owner == id {
			delete(r.owned, k)
		}
	}
}

// Get returns a copy of a job.
func (r *Registry) Get(id string) (*Job, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return nil, false
	}
	return j.clone(), true
}

// Jobs returns copies of every job, by creation time.
func (r *Registry) Jobs() []*Job {
	r.mu.Lock()
	out := make([]*Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, j.clone())
	}
	r.mu.Unlock()
	sort.Slice(out, func(a, b int) bool {
		if !out[a].CreatedAt.Equal(out[b].CreatedAt) {
			return out[a].CreatedAt.Before(out[b].CreatedAt)
		}
		return out[a].ID < out[b].ID
	})
	return out
}

// Active reports whether any job is in flight.
func (r *Registry) Active() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.jobs) > 0
}

// Owns reports whether a job in flight owns the file. Before the journal is
// read it is true for every file.
func (r *Registry) Owns(key string) bool {
	if r == nil {
		return false
	}
	if !r.seeded.Load() {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.owned[key]
	return ok
}

// Claim is taken by an operation that is about to delete a file outside any
// job (retention), across its check and its act: while
// held, no job can begin on the file or take it as an output key. ok is false
// when a job owns the file: the operation must leave it alone.
func (r *Registry) Claim(key string) (release func(), ok bool) {
	if r == nil {
		return func() {}, true
	}
	if !r.seeded.Load() {
		return func() {}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, owned := r.owned[key]; owned {
		return func() {}, false
	}
	r.claims[key]++
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if r.claims[key]--; r.claims[key] <= 0 {
				delete(r.claims, key)
			}
			r.mu.Unlock()
		})
	}, true
}

// Block is taken by an operation that acts on everything under a key prefix
// outside any job: a database delete ("db/"), a backup or a restore (""). ok
// is false when a job in flight owns a file under the prefix: the operation
// must not start. While the block is held no job can begin on, or write to,
// a file under the prefix. Each side registers first and then looks for the
// other, so one of the two always sees the other.
func (r *Registry) Block(prefix string) (release func(), ok bool) {
	if r == nil {
		return func() {}, true
	}
	if !r.seeded.Load() {
		return func() {}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.owned {
		if strings.HasPrefix(k, prefix) {
			return func() {}, false
		}
	}
	r.blocks[prefix]++
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if r.blocks[prefix]--; r.blocks[prefix] <= 0 {
				delete(r.blocks, prefix)
			}
			r.mu.Unlock()
		})
	}, true
}

func (r *Registry) blockedLocked(key string) bool {
	for p := range r.blocks {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Open reports whether a job is leaving files out of reads of the
// measurement at prefix: from committed until it ends. While it does, a
// query's transform is not cached.
func (r *Registry) Open(prefix string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, j := range r.jobs {
		if j.Prefix() == prefix && (j.State == StateCommitted || j.State == StateLatched) {
			return true
		}
	}
	return false
}

// ExcludedFor returns the files reads of the measurement at prefix must leave
// out: every output of a committed job, the source of a latched one.
func (r *Registry) ExcludedFor(_ context.Context, prefix string) []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, j := range r.jobs {
		if j.Prefix() != prefix {
			continue
		}
		switch j.State {
		case StateCommitted:
			for _, o := range j.Outputs {
				out = append(out, o.Key)
			}
		case StateLatched:
			out = append(out, j.Source.Key)
		}
	}
	sort.Strings(out)
	return out
}

// ExcludedTiered is ExcludedFor with, in the same answer, which of the keys
// are files in cold storage: a latched job's source that is on the cold
// tier. A read names such a file by its cold address. One call, one lock:
// asked in two, a job that latched in between would have a read leave out
// both sides, or neither.
func (r *Registry) ExcludedTiered(_ context.Context, prefix string) (keys, coldKeys []string) {
	if r == nil {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, j := range r.jobs {
		if j.Prefix() != prefix {
			continue
		}
		switch j.State {
		case StateCommitted:
			for _, o := range j.Outputs {
				keys = append(keys, o.Key)
			}
		case StateLatched:
			keys = append(keys, j.Source.Key)
			if j.Source.Cold() {
				coldKeys = append(coldKeys, j.Source.Key)
			}
		}
	}
	sort.Strings(keys)
	return keys, coldKeys
}

// Replaced returns, for the jobs under prefix whose reads already use the new
// files (latched), the key of the file each is replacing with the keys of the
// files that hold its rows now. The Iceberg export commits such a file's
// removal together with those files.
func (r *Registry) Replaced(prefix string) map[string][]string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out map[string][]string
	for _, j := range r.jobs {
		if j.Prefix() != prefix || j.State != StateLatched {
			continue
		}
		if out == nil {
			out = map[string][]string{}
		}
		keys := make([]string, len(j.Outputs))
		for i, o := range j.Outputs {
			keys[i] = o.Key
		}
		out[j.Source.Key] = keys
	}
	return out
}

// ExcludedForStrict is ExcludedFor for a reader that must not go on without
// the journal: the Iceberg export.
func (r *Registry) ExcludedForStrict(ctx context.Context, prefix string) ([]string, error) {
	if r != nil && !r.seeded.Load() {
		return nil, errors.New("the re-home journal has not been read")
	}
	return r.ExcludedFor(ctx, prefix), nil
}

// Generation moves when a job starts leaving files out of reads.
func (r *Registry) Generation() uint64 {
	if r == nil {
		return 0
	}
	return r.gen.Load()
}
