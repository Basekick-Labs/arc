package fieldschema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// Options configures a Registry.
type Options struct {
	// Enabled turns the feature on. Off, Resolve never returns a path and
	// Ensure never writes, so the query SQL is byte-identical to a build
	// without this package.
	Enabled bool
	// Bootstrap lets a measurement that has data but no stored anchor get
	// one built in the background from a bounded sample of its files, the
	// first time it is queried. Off, only ingest and POST rebuild create
	// anchors.
	Bootstrap bool
	// BootstrapMaxFiles caps how many file footers one bootstrap reads.
	// Newest days are sampled first; compacted files are preferred within a
	// day because they carry the union of their inputs' columns.
	BootstrapMaxFiles int
	// LocalDir is the directory the materialized anchors DuckDB reads are
	// written to. It must be inside DuckDB's allowed_directories. Empty
	// disables Resolve (queries run as today) but not Ensure.
	LocalDir string
	// RefreshTTL bounds how stale this process's view of a stored anchor can
	// be on the query side; another node's ingest becomes visible within it.
	RefreshTTL time.Duration
	// VerifyTTL bounds how long an ingest node trusts its cached view before
	// re-reading the stored anchor, which repairs a column lost to a
	// concurrent writer on shared storage.
	VerifyTTL time.Duration
	// NegativeTTL is how long "no stored anchor" is remembered.
	NegativeTTL time.Duration
}

func (o Options) withDefaults() Options {
	if o.BootstrapMaxFiles <= 0 {
		o.BootstrapMaxFiles = 500
	}
	if o.RefreshTTL <= 0 {
		o.RefreshTTL = time.Minute
	}
	if o.VerifyTTL <= 0 {
		o.VerifyTTL = 5 * time.Minute
	}
	if o.NegativeTTL <= 0 {
		o.NegativeTTL = 30 * time.Second
	}
	return o
}

// Field is one registered column, for the schema API.
type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// describeFunc runs DESCRIBE over a read_parquet of the given DuckDB paths
// and returns (name, type) pairs. Overridable in tests.
type describeFunc func(ctx context.Context, paths []string) ([][2]string, error)

// Registry holds every measurement's registered field schema this process
// knows about, backed by the stored anchors under _schema/.
type Registry struct {
	backend  storage.Backend
	opts     Options
	logger   zerolog.Logger
	describe describeFunc
	now      func() time.Time

	mu      sync.Mutex
	entries map[string]*entry

	queue   chan *entry
	stop    chan struct{}
	workers sync.WaitGroup
	once    sync.Once
}

type entry struct {
	mu          sync.Mutex
	database    string
	measurement string

	schema     *arrow.Schema // registered fields; nil when none are known
	loaded     bool          // stored anchor consulted at least once
	loadedAt   time.Time     // last read of the stored anchor
	absent     bool          // stored anchor was absent at loadedAt
	verifiedAt time.Time     // last time Ensure re-read the stored anchor

	localPath        string
	localFingerprint string

	bootstrapping     bool
	bootstrapFailures int
	nextBootstrap     time.Time
	refreshing        bool      // background re-read of the stored anchor in flight
	retryAfter        time.Time // backend error: do not re-issue the lookup before this
}

// maxEntries bounds the registry's memory against clients naming measurements
// that do not exist; beyond it, entries with no schema are evicted first.
const maxEntries = 10000

// New creates a registry. db may be nil, which disables bootstrap.
func New(backend storage.Backend, db *sql.DB, opts Options, logger zerolog.Logger) *Registry {
	r := &Registry{
		backend: backend,
		opts:    opts.withDefaults(),
		logger:  logger.With().Str("component", "fieldschema").Logger(),
		now:     time.Now,
		entries: make(map[string]*entry),
		queue:   make(chan *entry, 256),
		stop:    make(chan struct{}),
	}
	if db != nil {
		r.describe = duckDBDescribe(db)
	}
	return r
}

// Start launches the background bootstrap worker. One bootstrap runs at a
// time per process so a fleet of measurements without anchors cannot flood
// the object store with footer reads on upgrade day.
func (r *Registry) Start(ctx context.Context) {
	r.once.Do(func() {
		r.workers.Add(1)
		go func() {
			defer r.workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-r.stop:
					return
				case e := <-r.queue:
					r.runBootstrap(ctx, e)
				}
			}
		}()
	})
}

// Stop ends the bootstrap worker and waits for a running bootstrap.
func (r *Registry) Stop() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	r.workers.Wait()
}

// Enabled reports whether anchors are in use.
func (r *Registry) Enabled() bool { return r != nil && r.opts.Enabled }

func key(database, measurement string) string { return database + "/" + measurement }

func (r *Registry) entry(database, measurement string) *entry {
	k := key(database, measurement)
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[k]
	if !ok {
		if len(r.entries) >= maxEntries {
			r.evictLocked()
		}
		e = &entry{database: database, measurement: measurement}
		r.entries[k] = e
	}
	return e
}

// evictLocked drops up to a tenth of the entries, schema-less ones first.
// Caller holds r.mu. Dropping an entry only costs a re-read of its stored
// anchor on the next use.
func (r *Registry) evictLocked() {
	budget := maxEntries / 10
	for _, pass := range []bool{true, false} {
		for k, e := range r.entries {
			if budget == 0 {
				return
			}
			if pass && (e.schema != nil || e.bootstrapping) {
				continue
			}
			delete(r.entries, k)
			budget--
		}
	}
}

// loadStored reads the stored anchor into e (union-merged with what e
// already knows, narrowest type winning). Caller holds e.mu. A read failure
// is returned and leaves e untouched, so a transient outage never makes a
// known schema disappear.
// loadStored reads the stored anchor into e. The stored anchor is the
// source of truth: its column order and types are the base, and whatever
// this process had cached is folded in after it (so a column lost to a
// concurrent writer is still known locally and put back by publishLocked).
// Basing on the stored copy is what makes every node converge: two nodes
// that cached different orders both end up rewriting to the same stored
// order and then stop rewriting. An absent anchor forgets the cached view,
// because absence is how a deleted database looks. A backend error leaves
// the entry as it was and parks the next lookup for NegativeTTL.
// Caller holds e.mu.
func (r *Registry) loadStored(ctx context.Context, e *entry) error {
	k := AnchorKey(e.database, e.measurement)
	exists, err := r.backend.Exists(ctx, k)
	if err != nil {
		e.retryAfter = r.now().Add(r.opts.NegativeTTL)
		return fmt.Errorf("fieldschema: check %s: %w", k, err)
	}
	if !exists {
		e.loaded = true
		e.loadedAt = r.now()
		e.absent = true
		e.schema = nil
		return nil
	}
	data, err := r.backend.Read(ctx, k)
	if err != nil {
		e.retryAfter = r.now().Add(r.opts.NegativeTTL)
		return fmt.Errorf("fieldschema: read %s: %w", k, err)
	}
	e.loaded = true
	e.loadedAt = r.now()
	stored, err := DecodeAnchor(data)
	if err != nil {
		// An undecodable anchor names nothing; treat it as absent so ingest
		// rewrites it on the next schema change and bootstrap can rebuild it.
		r.logger.Warn().Err(err).Str("anchor", k).Msg("Stored field schema anchor cannot be decoded; ignoring it")
		e.absent = true
		e.schema = nil
		return nil
	}
	e.absent = false
	merged, _, conflicts := Merge(stored, e.schema)
	r.logConflicts(e, conflicts)
	e.schema = merged
	return nil
}

func (r *Registry) logConflicts(e *entry, conflicts []Conflict) {
	for _, c := range conflicts {
		r.logger.Warn().
			Str("database", e.database).
			Str("measurement", e.measurement).
			Str("field", c.Field).
			Str("registered_type", DuckDBTypeName(c.Stored)).
			Str("incoming_type", DuckDBTypeName(c.Incoming)).
			Msg("Field type conflict: the registered type is kept; DuckDB still promotes per query where files disagree")
	}
}

// Ensure registers the schema of a file that was just written for the
// measurement. It is the ingest hook and must never fail a flush: errors
// are logged and returned for tests only. The fast path (schema already
// covered, stored anchor verified recently) does no I/O.
func (r *Registry) Ensure(ctx context.Context, database, measurement string, schema *arrow.Schema, tagColumns []string) error {
	if !r.Enabled() {
		return nil
	}
	incoming := Normalize(schema, tagColumns)
	if incoming == nil || incoming.NumFields() == 0 {
		return nil
	}
	e := r.entry(database, measurement)
	e.mu.Lock()
	defer e.mu.Unlock()
	now := r.now()
	if !e.loaded {
		if err := r.loadStored(ctx, e); err != nil {
			r.logger.Warn().Err(err).Msg("Field schema anchor unavailable; will retry on the next flush")
			return err
		}
		e.verifiedAt = now
	}
	if e.schema != nil && !e.absent && Covers(e.schema, incoming) && now.Sub(e.verifiedAt) < r.opts.VerifyTTL {
		return nil
	}
	return r.publishLocked(ctx, e, incoming)
}

// publishLocked re-reads the stored anchor, folds the cached view and the
// incoming schema into it (stored order and types first), and writes it
// back when that changes it. The re-read is what repairs a column lost to a
// concurrent writer on shared storage: whichever node next passes through
// here puts it back. When the stored anchor is absent the cached view is NOT
// resurrected: absence means the database was deleted, and the new anchor
// starts from the incoming schema alone. Caller holds e.mu.
func (r *Registry) publishLocked(ctx context.Context, e *entry, incoming *arrow.Schema) error {
	cached := e.schema
	e.schema = nil
	if err := r.loadStored(ctx, e); err != nil {
		e.schema = cached
		r.logger.Warn().Err(err).Msg("Field schema anchor unavailable; will retry on the next flush")
		return err
	}
	stored := e.schema
	base := stored
	var c1 []Conflict
	if stored != nil {
		base, _, c1 = Merge(stored, cached)
	}
	merged, _, c2 := Merge(base, incoming)
	r.logConflicts(e, append(c1, c2...))
	e.schema = merged
	e.verifiedAt = r.now()
	if stored != nil && Fingerprint(stored) == Fingerprint(merged) {
		return nil
	}
	data, err := EncodeAnchor(merged)
	if err != nil {
		return err
	}
	k := AnchorKey(e.database, e.measurement)
	if err := r.backend.Write(ctx, k, data); err != nil {
		r.logger.Warn().Err(err).Str("anchor", k).Msg("Failed to write field schema anchor; will retry on the next flush")
		e.verifiedAt = time.Time{} // force the slow path next time
		return fmt.Errorf("fieldschema: write %s: %w", k, err)
	}
	e.absent = false
	r.logger.Info().
		Str("database", e.database).
		Str("measurement", e.measurement).
		Int("fields", merged.NumFields()).
		Msg("Field schema anchor updated")
	return nil
}

// Resolve returns the local anchor path DuckDB should list first for the
// measurement, or ok=false when there is none (no fields registered yet,
// feature off, or no local directory). It never blocks on a bootstrap: a
// missing anchor queues one and this query runs as today.
func (r *Registry) Resolve(ctx context.Context, database, measurement string) (string, bool) {
	if !r.Enabled() || r.opts.LocalDir == "" {
		return "", false
	}
	e := r.entry(database, measurement)
	e.mu.Lock()
	defer e.mu.Unlock()
	now := r.now()
	ttl := r.opts.RefreshTTL
	if e.absent {
		ttl = r.opts.NegativeTTL
	}
	stale := !e.loaded || now.Sub(e.loadedAt) >= ttl
	switch {
	case stale && e.schema != nil:
		// A known schema stays in service while the stored anchor is
		// re-read in the background: a query must never wait on the object
		// store for metadata it already has.
		if !e.refreshing {
			e.refreshing = true
			go r.refresh(e)
		}
	case stale && now.Before(e.retryAfter):
		// The backend failed recently; do not hammer it per query.
	case stale:
		if err := r.loadStored(ctx, e); err != nil {
			r.logger.Debug().Err(err).Msg("Field schema anchor lookup failed; query runs without it")
			return "", false
		}
	}
	if e.schema == nil {
		r.maybeBootstrapLocked(e)
		return "", false
	}
	path, err := r.materializeLocked(e)
	if err != nil {
		r.logger.Warn().Err(err).Msg("Failed to materialize field schema anchor; query runs without it")
		return "", false
	}
	return path, true
}

// refresh re-reads the stored anchor for an entry whose cached schema is
// past RefreshTTL. Runs off the query path.
func (r *Registry) refresh(e *entry) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.refreshing = false
	if err := r.loadStored(ctx, e); err != nil {
		r.logger.Debug().Err(err).Msg("Field schema anchor refresh failed; keeping the cached view")
	}
}

// IsLocalAnchorPath reports whether a DuckDB path is one of this process's
// materialized anchors, for the query path's error handling.
func (r *Registry) IsLocalAnchorPath(p string) bool {
	if r == nil || r.opts.LocalDir == "" {
		return false
	}
	prefix := filepath.ToSlash(filepath.Join(r.opts.LocalDir, "schema")) + "/"
	return strings.HasPrefix(filepath.ToSlash(p), prefix)
}

// materializeLocked writes e.schema as a local anchor when the local copy is
// missing or stale, and returns its slash-form path. Caller holds e.mu.
func (r *Registry) materializeLocked(e *entry) (string, error) {
	fp := Fingerprint(e.schema)
	if e.localPath != "" && e.localFingerprint == fp {
		if _, err := os.Stat(e.localPath); err == nil {
			return e.localPath, nil
		}
	}
	dir := filepath.Join(r.opts.LocalDir, "schema")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(key(e.database, e.measurement)))
	final := filepath.Join(dir, hex.EncodeToString(sum[:16])+".parquet")
	data, err := EncodeAnchor(e.schema)
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(dir, ".anchor-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return "", err
	}
	if err := os.Rename(tmpName, final); err != nil {
		os.Remove(tmpName)
		return "", err
	}
	e.localPath = filepath.ToSlash(final)
	e.localFingerprint = fp
	return e.localPath, nil
}

// Fields returns the registered fields of a measurement. ok is false when
// no anchor exists.
func (r *Registry) Fields(ctx context.Context, database, measurement string) ([]Field, bool, error) {
	if !r.Enabled() {
		return nil, false, nil
	}
	e := r.entry(database, measurement)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.loaded || r.now().Sub(e.loadedAt) >= r.opts.RefreshTTL {
		if err := r.loadStored(ctx, e); err != nil && e.schema == nil {
			return nil, false, err
		}
	}
	if e.schema == nil {
		return nil, false, nil
	}
	out := make([]Field, 0, e.schema.NumFields())
	for _, f := range e.schema.Fields() {
		out = append(out, Field{Name: f.Name, Type: DuckDBTypeName(f.Type)})
	}
	return out, true, nil
}

// DeleteDatabase removes every stored anchor of a database and forgets them
// locally. Called when the database itself is deleted.
func (r *Registry) DeleteDatabase(ctx context.Context, database string) error {
	if r == nil {
		return nil
	}
	prefix := AnchorPrefixForDatabase(database)
	keys, err := r.backend.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("fieldschema: list %s: %w", prefix, err)
	}
	var errs error
	for _, k := range keys {
		if !isAnchorPath(k) {
			continue
		}
		if err := r.backend.Delete(ctx, k); err != nil {
			errs = errors.Join(errs, fmt.Errorf("fieldschema: delete %s: %w", k, err))
		}
	}
	var dropped []*entry
	r.mu.Lock()
	for k, e := range r.entries {
		if e.database == database {
			dropped = append(dropped, e)
			delete(r.entries, k)
		}
	}
	r.mu.Unlock()
	for _, e := range dropped {
		e.mu.Lock()
		if e.localPath != "" {
			os.Remove(e.localPath)
		}
		e.schema = nil
		e.mu.Unlock()
	}
	return errs
}

// RebuildStatus is the outcome of Rebuild.
type RebuildStatus int

const (
	RebuildQueued RebuildStatus = iota
	RebuildAlreadyRunning
	RebuildQueueFull
	RebuildUnavailable // no query engine handle, or registry disabled
)

// Rebuild forces a bootstrap of the measurement from its files, merging into
// whatever is registered (never dropping a field).
func (r *Registry) Rebuild(database, measurement string) RebuildStatus {
	if !r.Enabled() || r.describe == nil {
		return RebuildUnavailable
	}
	e := r.entry(database, measurement)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.bootstrapping {
		return RebuildAlreadyRunning
	}
	e.bootstrapFailures = 0
	e.nextBootstrap = time.Time{}
	if !r.enqueueLocked(e) {
		return RebuildQueueFull
	}
	return RebuildQueued
}

// maybeBootstrapLocked queues a bootstrap for a measurement with no known
// schema, subject to backoff. Caller holds e.mu.
func (r *Registry) maybeBootstrapLocked(e *entry) {
	if !r.opts.Bootstrap || r.describe == nil || e.bootstrapping || r.now().Before(e.nextBootstrap) {
		return
	}
	r.enqueueLocked(e)
}

func (r *Registry) enqueueLocked(e *entry) bool {
	e.bootstrapping = true
	select {
	case r.queue <- e:
		return true
	default:
		e.bootstrapping = false
		e.nextBootstrap = r.now().Add(r.opts.NegativeTTL)
		return false
	}
}

// bootstrapBackoff grows 1m, 2m, 4m ... up to 1h.
func bootstrapBackoff(failures int) time.Duration {
	d := time.Minute
	for i := 1; i < failures && d < time.Hour; i++ {
		d *= 2
	}
	if d > time.Hour {
		d = time.Hour
	}
	return d
}

// runBootstrap builds an anchor from a bounded sample of the measurement's
// files. The anchor is additive, so a partial sample is safe: any column it
// misses binds wherever files carry it (as today) and ingest adds it on the
// next flush that writes it.
func (r *Registry) runBootstrap(parent context.Context, e *entry) {
	e.mu.Lock()
	database, measurement := e.database, e.measurement
	e.mu.Unlock()

	// One bound for the listing and the footer reads together.
	ctx, cancel := context.WithTimeout(parent, 10*time.Minute)
	defer cancel()

	// park records an outcome that produced nothing to register (no files,
	// or no field with a mappable type) and defers the next attempt so a
	// query loop cannot relist and re-DESCRIBE the measurement per query.
	park := func(reason string) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.bootstrapping = false
		e.nextBootstrap = r.now().Add(10 * time.Minute)
		r.logger.Debug().Str("database", database).Str("measurement", measurement).Str("reason", reason).Msg("Field schema bootstrap found nothing to register")
	}

	finish := func(err error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.bootstrapping = false
		if err != nil {
			e.bootstrapFailures++
			e.nextBootstrap = r.now().Add(bootstrapBackoff(e.bootstrapFailures))
			r.logger.Warn().Err(err).
				Str("database", database).
				Str("measurement", measurement).
				Int("failures", e.bootstrapFailures).
				Time("next_attempt", e.nextBootstrap).
				Msg("Field schema bootstrap failed")
			return
		}
		e.bootstrapFailures = 0
	}

	paths, err := r.sampleFiles(ctx, database, measurement)
	if err != nil {
		finish(err)
		return
	}
	if len(paths) == 0 {
		park("no files")
		return
	}
	uris := make([]string, 0, len(paths))
	for _, p := range paths {
		u, err := storage.ObjectURI(r.backend, p)
		if err != nil {
			continue
		}
		if err := storage.ValidateGlobSafe(u); err != nil {
			continue
		}
		uris = append(uris, u)
	}
	if len(uris) == 0 {
		park("no addressable files")
		return
	}
	rows, err := r.describe(ctx, uris)
	if err != nil {
		finish(err)
		return
	}
	fields := make([]arrow.Field, 0, len(rows))
	for _, row := range rows {
		t, ok := ArrowTypeFromDuckDB(row[1])
		if !ok {
			r.logger.Warn().
				Str("database", database).
				Str("measurement", measurement).
				Str("field", row[0]).
				Str("duckdb_type", row[1]).
				Msg("Field type has no anchor mapping; it binds only where files carry it")
			continue
		}
		fields = append(fields, arrow.Field{Name: row[0], Type: t, Nullable: true})
	}
	if len(fields) == 0 {
		park("no field with a mappable type")
		return
	}
	incoming := Normalize(arrow.NewSchema(fields, nil), nil)
	e.mu.Lock()
	err = r.publishLocked(ctx, e, incoming)
	e.mu.Unlock()
	if err == nil {
		r.logger.Info().
			Str("database", database).
			Str("measurement", measurement).
			Int("sampled_files", len(uris)).
			Int("fields", len(fields)).
			Msg("Field schema anchor bootstrapped from existing files")
	}
	finish(err)
}

// sampleFiles picks at most BootstrapMaxFiles keys under the measurement,
// newest days first. Within a day it prefers daily then hourly compaction
// outputs (each carries the union of its inputs' columns) and otherwise the
// newest raw file of each hour.
func (r *Registry) sampleFiles(ctx context.Context, database, measurement string) ([]string, error) {
	prefix := database + "/" + measurement + "/"
	keys, err := r.backend.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("fieldschema: list %s: %w", prefix, err)
	}
	type dayPick struct {
		daily     []string
		compacted []string
		rawByHour map[string]string
	}
	days := make(map[string]*dayPick)
	for _, k := range keys {
		if !strings.HasSuffix(k, ".parquet") {
			continue
		}
		rest := strings.TrimPrefix(k, prefix)
		segs := strings.Split(rest, "/")
		base := segs[len(segs)-1]
		if strings.HasPrefix(base, ".") {
			continue
		}
		day := rest
		hour := rest
		if len(segs) >= 4 {
			day = strings.Join(segs[:3], "/")
			hour = strings.Join(segs[:len(segs)-1], "/")
		}
		d := days[day]
		if d == nil {
			d = &dayPick{rawByHour: make(map[string]string)}
			days[day] = d
		}
		switch {
		case strings.Contains(base, "_daily"):
			d.daily = append(d.daily, k)
		case strings.Contains(base, "_compacted"):
			d.compacted = append(d.compacted, k)
		default:
			if cur, ok := d.rawByHour[hour]; !ok || base > filepath.Base(cur) {
				d.rawByHour[hour] = k
			}
		}
	}
	dayKeys := make([]string, 0, len(days))
	for d := range days {
		dayKeys = append(dayKeys, d)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dayKeys)))
	var out []string
	for _, dk := range dayKeys {
		d := days[dk]
		var pick []string
		switch {
		case len(d.daily) > 0:
			pick = d.daily
		case len(d.compacted) > 0:
			pick = d.compacted
		}
		// Raw files of hours not yet compacted always add potential columns.
		hours := make([]string, 0, len(d.rawByHour))
		for h := range d.rawByHour {
			hours = append(hours, h)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(hours)))
		for _, h := range hours {
			pick = append(pick, d.rawByHour[h])
		}
		for _, p := range pick {
			if len(out) >= r.opts.BootstrapMaxFiles {
				return out, nil
			}
			out = append(out, p)
		}
	}
	return out, nil
}

// duckDBDescribe returns a describeFunc over the query engine.
func duckDBDescribe(db *sql.DB) describeFunc {
	return func(ctx context.Context, paths []string) ([][2]string, error) {
		var sb strings.Builder
		sb.WriteString("DESCRIBE SELECT * FROM read_parquet([")
		for i, p := range paths {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("'")
			sb.WriteString(strings.ReplaceAll(p, "'", "''"))
			sb.WriteString("'")
		}
		sb.WriteString("], union_by_name=true)")
		rows, err := db.QueryContext(ctx, sb.String())
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		var out [][2]string
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, err
			}
			name, _ := vals[0].(string)
			typ, _ := vals[1].(string)
			if name == "" {
				continue
			}
			out = append(out, [2]string{name, typ})
		}
		return out, rows.Err()
	}
}
