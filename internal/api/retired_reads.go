package api

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// RetiredReads (standalone) leaves a file a row DELETE replaced out of reads
// while it is still in storage next to its replacement: the window between
// the rewrite's publish and the old file's removal, and, when that removal
// failed, until the drainer removes it. It is an OpenCompactions source
// (CombineOpenCompactions).
//
// An entry is excluded once its replacement is in storage (the replacement
// holds every row the DELETE kept), and dropped once the old file is gone.
// At start it is seeded from the retired journal, so a restart does not
// reopen the window.
type RetiredReads struct {
	store   storage.Backend
	mu      sync.Mutex
	entries map[string]*retiredRead
	gen     atomic.Uint64
	onSeen  atomic.Pointer[func()]
}

type retiredRead struct {
	replacedBy    string
	seen          bool
	goneCheckedAt time.Time
}

// NewRetiredReads returns an empty source over store.
func NewRetiredReads(store storage.Backend) *RetiredReads {
	return &RetiredReads{store: store, entries: map[string]*retiredRead{}}
}

// SetOnSeen sets what runs the first time an entry's replacement is seen
// (drop DuckDB's object-store listing cache when it is on).
func (r *RetiredReads) SetOnSeen(fn func()) {
	if r != nil && fn != nil {
		r.onSeen.Store(&fn)
	}
}

// Seed adds the journal's records whose replacement is in storage.
func (r *RetiredReads) Seed(ctx context.Context) error {
	keys, err := r.store.List(ctx, storage.DeleteRetiredRoot+"/")
	if err != nil {
		return err
	}
	for _, k := range keys {
		data, err := r.store.Read(ctx, k)
		if err != nil {
			continue
		}
		old, ok := storage.ParseRetired(k, data)
		if !ok {
			continue
		}
		var rec retiredRecord
		if json := parseRetiredRecord(data, &rec); !json || rec.ReplacedBy == "" {
			continue
		}
		if n, err := r.store.StatFile(ctx, rec.ReplacedBy); err == nil && n >= 0 {
			r.add(old, rec.ReplacedBy)
		}
	}
	return nil
}

func (r *RetiredReads) add(old, replacedBy string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.entries[old] = &retiredRead{replacedBy: replacedBy}
	r.mu.Unlock()
	r.gen.Add(1)
}

func (r *RetiredReads) remove(old string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.entries, old)
	r.mu.Unlock()
}

// ExcludedFor returns the replaced files under prefix whose replacement is
// in storage.
func (r *RetiredReads) ExcludedFor(ctx context.Context, prefix string) []string {
	type cand struct {
		old string
		e   retiredRead
	}
	r.mu.Lock()
	var cands []cand
	for old, e := range r.entries {
		if strings.HasPrefix(old, prefix) {
			cands = append(cands, cand{old, *e})
		}
	}
	r.mu.Unlock()
	var out []string
	for _, c := range cands {
		e := c.e
		if time.Since(e.goneCheckedAt) >= goneCheckEvery {
			e.goneCheckedAt = time.Now()
			if n, err := r.store.StatFile(ctx, c.old); err == nil && n < 0 {
				r.remove(c.old)
				continue
			}
		}
		first := false
		if !e.seen {
			if n, err := r.store.StatFile(ctx, e.replacedBy); err == nil && n >= 0 {
				e.seen, first = true, true
			}
		}
		r.mu.Lock()
		if cur := r.entries[c.old]; cur != nil && cur.replacedBy == e.replacedBy {
			*cur = e
		}
		r.mu.Unlock()
		if first {
			if fn := r.onSeen.Load(); fn != nil {
				(*fn)()
			}
		}
		if e.seen {
			out = append(out, c.old)
		}
	}
	return out
}

// Open reports whether reads under prefix must not be cached.
func (r *RetiredReads) Open(prefix string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for old := range r.entries {
		if strings.HasPrefix(old, prefix) {
			return true
		}
	}
	return false
}

// Generation moves whenever an entry is added.
func (r *RetiredReads) Generation() uint64 { return r.gen.Load() }

// goneCheckEvery bounds how often an entry's "old file already gone" stat
// runs.
const goneCheckEvery = 2 * time.Second
