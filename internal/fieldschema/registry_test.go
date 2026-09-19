package fieldschema

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

func newTestRegistry(t *testing.T, opts Options) (*Registry, storage.Backend, *time.Time) {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	opts.Enabled = true
	if opts.LocalDir == "" {
		opts.LocalDir = t.TempDir()
	}
	r := New(backend, nil, opts, zerolog.Nop())
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return now }
	return r, backend, &now
}

func TestEnsureWritesOnceAndMergesNewColumn(t *testing.T) {
	r, b, now := newTestRegistry(t, Options{})
	ctx := context.Background()
	jan := fields("time", tTsTZ, "source", tStr, "stable", tInt64)
	if err := r.Ensure(ctx, "db", "multiday", jan, []string{"source"}); err != nil {
		t.Fatal(err)
	}
	data, err := b.Read(ctx, AnchorKey("db", "multiday"))
	if err != nil {
		t.Fatalf("anchor not written: %v", err)
	}
	s1, _ := DecodeAnchor(data)
	if s1.NumFields() != 3 {
		t.Fatalf("anchor fields %v", s1)
	}
	// Same schema again: fast path, no storage I/O at all.
	counting := &countingBackend{Backend: b}
	r.backend = counting
	*now = now.Add(time.Second)
	if err := r.Ensure(ctx, "db", "multiday", jan, []string{"source"}); err != nil {
		t.Fatal(err)
	}
	if counting.writes != 0 || counting.reads != 0 || counting.exists != 0 {
		t.Fatalf("fast path did I/O: writes=%d reads=%d exists=%d", counting.writes, counting.reads, counting.exists)
	}
	r.backend = b
	// March adds weeks_later: merged and appended last.
	mar := fields("weeks_later", tInt64, "time", tTsTZ, "source", tStr, "stable", tInt64)
	if err := r.Ensure(ctx, "db", "multiday", mar, []string{"source"}); err != nil {
		t.Fatal(err)
	}
	data, _ = b.Read(ctx, AnchorKey("db", "multiday"))
	s2, _ := DecodeAnchor(data)
	if s2.NumFields() != 4 || s2.Field(3).Name != "weeks_later" || s2.Field(0).Name != "time" || s2.Field(1).Name != "source" {
		t.Fatalf("merged anchor %v", s2)
	}
	fs, ok, err := r.Fields(ctx, "db", "multiday")
	if err != nil || !ok || len(fs) != 4 || fs[3].Type != "BIGINT" {
		t.Fatalf("Fields=%v ok=%v err=%v", fs, ok, err)
	}
}

func TestResolveMaterializesLocalAnchorAndRefreshes(t *testing.T) {
	r, b, now := newTestRegistry(t, Options{RefreshTTL: time.Minute})
	ctx := context.Background()
	if _, ok := r.Resolve(ctx, "db", "cpu"); ok {
		t.Fatal("no anchor yet, Resolve must decline")
	}
	// Another node writes the anchor directly to storage.
	other := New(b, nil, Options{Enabled: true}, zerolog.Nop())
	if err := other.Ensure(ctx, "db", "cpu", fields("time", tTsTZ, "usage", tFloat), nil); err != nil {
		t.Fatal(err)
	}
	// Negative result is cached for NegativeTTL.
	if _, ok := r.Resolve(ctx, "db", "cpu"); ok {
		t.Fatal("negative cache should still hide the anchor")
	}
	*now = now.Add(31 * time.Second)
	path, ok := r.Resolve(ctx, "db", "cpu")
	if !ok {
		t.Fatal("anchor should be visible after the negative TTL")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("local anchor missing: %v", err)
	}
	s, err := DecodeAnchor(mustRead(t, path))
	if err != nil || s.NumFields() != 2 {
		t.Fatalf("local anchor %v %v", s, err)
	}
	// The other node adds a column; visible here after RefreshTTL.
	if err := other.Ensure(ctx, "db", "cpu", fields("time", tTsTZ, "usage", tFloat, "iowait", tFloat), nil); err != nil {
		t.Fatal(err)
	}
	path2, _ := r.Resolve(ctx, "db", "cpu")
	if s, _ := DecodeAnchor(mustRead(t, path2)); s.NumFields() != 2 {
		t.Fatal("refresh happened before RefreshTTL")
	}
	// Past RefreshTTL the known schema stays in service and the stored
	// anchor is re-read in the background; the new column appears shortly.
	*now = now.Add(2 * time.Minute)
	var path3 string
	deadline := time.Now().Add(5 * time.Second)
	for {
		path3, _ = r.Resolve(ctx, "db", "cpu")
		if s, _ := DecodeAnchor(mustRead(t, path3)); s.NumFields() == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("new column not picked up after RefreshTTL")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A deleted local file is re-materialized instead of handed to DuckDB.
	os.Remove(path3)
	path4, ok := r.Resolve(ctx, "db", "cpu")
	if !ok || path4 != path3 {
		t.Fatalf("path4=%s ok=%v", path4, ok)
	}
	if _, err := os.Stat(path4); err != nil {
		t.Fatal("local anchor not re-materialized")
	}
	// Disabled registry: SQL unchanged.
	off := New(b, nil, Options{Enabled: false, LocalDir: t.TempDir()}, zerolog.Nop())
	if _, ok := off.Resolve(ctx, "db", "cpu"); ok {
		t.Fatal("disabled registry must not resolve")
	}
}

func TestEnsureRepairsLostUpdateAfterVerifyTTL(t *testing.T) {
	r, b, now := newTestRegistry(t, Options{VerifyTTL: 5 * time.Minute})
	ctx := context.Background()
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "x", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	// A concurrent writer overwrites the anchor without x (lost update).
	other := New(b, nil, Options{Enabled: true}, zerolog.Nop())
	data, _ := EncodeAnchor(fields("time", tTsTZ, "y", tInt64))
	if err := b.Write(ctx, AnchorKey("db", "m"), data); err != nil {
		t.Fatal(err)
	}
	_ = other
	// Fast path: cached view still says x is registered, nothing happens.
	*now = now.Add(time.Minute)
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "x", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	if s, _ := DecodeAnchor(mustReadKey(t, b, AnchorKey("db", "m"))); s.NumFields() != 2 || s.Field(1).Name != "y" {
		t.Fatalf("expected the lost update to still be in place, got %v", s)
	}
	// After VerifyTTL the slow path re-reads, unions, and writes x back.
	*now = now.Add(5 * time.Minute)
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "x", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	s, _ := DecodeAnchor(mustReadKey(t, b, AnchorKey("db", "m")))
	if s.NumFields() != 3 || len(s.FieldIndices("x")) == 0 || len(s.FieldIndices("y")) == 0 {
		t.Fatalf("lost column not repaired: %v", s)
	}
}

func TestEnsureNarrowsTypeConflict(t *testing.T) {
	r, b, _ := newTestRegistry(t, Options{})
	ctx := context.Background()
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "status", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	// A malformed batch wrote status as a string: the anchor must NOT widen.
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "status", tStr), nil); err != nil {
		t.Fatal(err)
	}
	s, _ := DecodeAnchor(mustReadKey(t, b, AnchorKey("db", "m")))
	if !arrow.TypeEqual(s.Field(1).Type, tInt64) {
		t.Fatalf("anchor widened to %s", s.Field(1).Type)
	}
	// A batch with a narrower type narrows the anchor.
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "status", tBool), nil); err != nil {
		t.Fatal(err)
	}
	s, _ = DecodeAnchor(mustReadKey(t, b, AnchorKey("db", "m")))
	if !arrow.TypeEqual(s.Field(1).Type, tBool) {
		t.Fatalf("anchor not narrowed: %s", s.Field(1).Type)
	}
}

func TestBootstrapSamplesNewestDaysAndBacksOff(t *testing.T) {
	r, b, now := newTestRegistry(t, Options{Bootstrap: true, BootstrapMaxFiles: 3})
	ctx := context.Background()
	for _, k := range []string{
		"db/m/2026/01/01/00/raw_a.parquet", "db/m/2026/01/01/00/raw_b.parquet", "db/m/2026/01/01/01/raw_c.parquet",
		"db/m/2026/01/02/00/x_compacted.parquet", "db/m/2026/01/02/03/raw_d.parquet",
		"db/m/2026/03/01/00/y_daily.parquet", "db/m/2026/03/01/05/raw_e.parquet",
	} {
		if err := b.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := r.sampleFiles(ctx, "db", "m")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"db/m/2026/03/01/00/y_daily.parquet", "db/m/2026/03/01/05/raw_e.parquet", "db/m/2026/01/02/00/x_compacted.parquet"}
	if len(paths) != len(want) {
		t.Fatalf("sample=%v", paths)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("sample=%v want %v", paths, want)
		}
	}

	var mu sync.Mutex
	var seen [][]string
	fail := true
	r.describe = func(_ context.Context, uris []string) ([][2]string, error) {
		mu.Lock()
		seen = append(seen, uris)
		mu.Unlock()
		if fail {
			return nil, errors.New("simulated DESCRIBE failure")
		}
		return [][2]string{{"time", "TIMESTAMP WITH TIME ZONE"}, {"host", "VARCHAR"}, {"value", "DOUBLE"}, {"weird", "STRUCT(a INTEGER)"}}, nil
	}
	if _, ok := r.Resolve(ctx, "db", "m"); ok {
		t.Fatal("no anchor yet")
	}
	e := r.entry("db", "m")
	r.runBootstrap(ctx, <-r.queue)
	e.mu.Lock()
	failures, next := e.bootstrapFailures, e.nextBootstrap
	e.mu.Unlock()
	if failures != 1 || !next.After(*now) {
		t.Fatalf("failures=%d next=%v now=%v", failures, next, *now)
	}
	// Within backoff: Resolve must not queue again.
	*now = now.Add(31 * time.Second)
	if _, ok := r.Resolve(ctx, "db", "m"); ok || len(r.queue) != 0 {
		t.Fatalf("bootstrap re-queued inside backoff (queue=%d)", len(r.queue))
	}
	*now = now.Add(2 * time.Minute)
	fail = false
	if _, ok := r.Resolve(ctx, "db", "m"); ok || len(r.queue) != 1 {
		t.Fatalf("bootstrap not queued after backoff (queue=%d)", len(r.queue))
	}
	r.runBootstrap(ctx, <-r.queue)
	*now = now.Add(31 * time.Second)
	path, ok := r.Resolve(ctx, "db", "m")
	if !ok {
		t.Fatal("anchor not available after bootstrap")
	}
	s, _ := DecodeAnchor(mustRead(t, path))
	if s.NumFields() != 3 || s.Field(0).Name != "time" {
		t.Fatalf("bootstrapped anchor %v (unmapped STRUCT must be skipped)", s)
	}
	if len(seen) != 2 || len(seen[1]) != 3 {
		t.Fatalf("describe calls %v", seen)
	}
	// Rebuild coalesces while queued.
	if r.Rebuild("db", "m") != RebuildQueued || r.Rebuild("db", "m") != RebuildAlreadyRunning {
		t.Fatal("rebuild must queue once and coalesce")
	}
	<-r.queue
	// A bootstrap that maps no field parks the entry instead of retrying
	// per query.
	r.describe = func(context.Context, []string) ([][2]string, error) {
		return [][2]string{{"time", "TIMESTAMP_NS"}}, nil
	}
	e2 := r.entry("db", "parked")
	if err := b.Write(ctx, "db/parked/2026/01/01/00/a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	r.Resolve(ctx, "db", "parked")
	r.runBootstrap(ctx, <-r.queue)
	*now = now.Add(31 * time.Second)
	if _, ok := r.Resolve(ctx, "db", "parked"); ok || len(r.queue) != 0 {
		t.Fatalf("unmappable bootstrap must park, queue=%d", len(r.queue))
	}
	_ = e2
}

type countingBackend struct {
	storage.Backend
	writes, reads, exists int
}

func (c *countingBackend) Write(ctx context.Context, p string, d []byte) error {
	c.writes++
	return c.Backend.Write(ctx, p, d)
}
func (c *countingBackend) Read(ctx context.Context, p string) ([]byte, error) {
	c.reads++
	return c.Backend.Read(ctx, p)
}
func (c *countingBackend) Exists(ctx context.Context, p string) (bool, error) {
	c.exists++
	return c.Backend.Exists(ctx, p)
}

// Two nodes that first saw different column orders must converge on one
// stored order and then stop rewriting: the stored anchor is the base on
// every re-read.
func TestConcurrentWritersConvergeOnStoredOrder(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	mk := func() *Registry {
		r := New(backend, nil, Options{Enabled: true, LocalDir: t.TempDir(), VerifyTTL: time.Minute}, zerolog.Nop())
		r.now = func() time.Time { return now }
		return r
	}
	a, b := mk(), mk()
	// A writes [time,a]; B merges to [time,a,b]; then B's PUT is modelled as
	// having landed last with a view that never saw a (the race window
	// between B's re-read and its write): stored is [time,b], A caches
	// [time,a], B caches [time,a,b].
	if err := a.Ensure(ctx, "db", "m", fields("time", tTsTZ, "a", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Ensure(ctx, "db", "m", fields("time", tTsTZ, "b", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	raced, _ := EncodeAnchor(fields("time", tTsTZ, "b", tInt64))
	if err := backend.Write(ctx, AnchorKey("db", "m"), raced); err != nil {
		t.Fatal(err)
	}
	counting := &countingBackend{Backend: backend}
	a.backend, b.backend = counting, counting
	for round := 0; round < 4; round++ {
		now = now.Add(2 * time.Minute)
		if err := a.Ensure(ctx, "db", "m", fields("time", tTsTZ, "a", tInt64), nil); err != nil {
			t.Fatal(err)
		}
		if err := b.Ensure(ctx, "db", "m", fields("time", tTsTZ, "b", tInt64), nil); err != nil {
			t.Fatal(err)
		}
	}
	// Exactly one repair write (A puts a back), then silence.
	if counting.writes != 1 {
		t.Fatalf("writers did not converge: %d writes over 4 rounds", counting.writes)
	}
	s, _ := DecodeAnchor(mustReadKey(t, backend, AnchorKey("db", "m")))
	if s.NumFields() != 3 || s.Field(1).Name != "b" || s.Field(2).Name != "a" {
		t.Fatalf("stored order must win: %v", s)
	}
	pa, _ := a.Resolve(ctx, "db", "m")
	pb, _ := b.Resolve(ctx, "db", "m")
	sa, _ := DecodeAnchor(mustRead(t, pa))
	sb, _ := DecodeAnchor(mustRead(t, pb))
	if Fingerprint(sa) != Fingerprint(sb) || Fingerprint(sa) != Fingerprint(s) {
		t.Fatalf("nodes materialize different anchors:\n%v\n%v", sa, sb)
	}
}

// A database deleted on another node: the cached schema is forgotten once
// the stored anchor is seen absent, and ingest starts the new anchor from
// the incoming schema alone.
func TestRemoteDeleteForgetsCachedSchema(t *testing.T) {
	r, b, now := newTestRegistry(t, Options{RefreshTTL: time.Minute, VerifyTTL: time.Minute})
	ctx := context.Background()
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "old", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Resolve(ctx, "db", "m"); !ok {
		t.Fatal("anchor expected")
	}
	if err := b.Delete(ctx, AnchorKey("db", "m")); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Minute)
	r.Resolve(ctx, "db", "m") // schedules a background refresh
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := r.Resolve(ctx, "db", "m"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deleted anchor still resolves")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := r.Ensure(ctx, "db", "m", fields("time", tTsTZ, "fresh", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	s, _ := DecodeAnchor(mustReadKey(t, b, AnchorKey("db", "m")))
	if s.NumFields() != 2 || s.Field(1).Name != "fresh" {
		t.Fatalf("old columns resurrected after a remote delete: %v", s)
	}
}

func TestDeleteDatabaseRemovesAnchors(t *testing.T) {
	r, b, _ := newTestRegistry(t, Options{})
	ctx := context.Background()
	for _, m := range []string{"a", "b"} {
		if err := r.Ensure(ctx, "db", m, fields("time", tTsTZ, "v", tInt64), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Ensure(ctx, "other", "a", fields("time", tTsTZ, "v", tInt64), nil); err != nil {
		t.Fatal(err)
	}
	path, _ := r.Resolve(ctx, "db", "a")
	if err := r.DeleteDatabase(ctx, "db"); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"a", "b"} {
		if ok, _ := b.Exists(ctx, AnchorKey("db", m)); ok {
			t.Fatalf("anchor db/%s survived", m)
		}
	}
	if ok, _ := b.Exists(ctx, AnchorKey("other", "a")); !ok {
		t.Fatal("unrelated database's anchor deleted")
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("local anchor not removed")
	}
	if _, ok := r.Resolve(ctx, "db", "a"); ok {
		t.Fatal("deleted anchor still resolves")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustReadKey(t *testing.T, b storage.Backend, key string) []byte {
	t.Helper()
	data, err := b.Read(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
