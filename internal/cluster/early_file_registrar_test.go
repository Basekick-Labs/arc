package cluster

// #1206: files flushed before the cluster coordinator is up — startup WAL
// recovery above all, which deletes the WAL files after flushing — reached no
// manifest, so only the node that wrote them could query them.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

func init() {
	earlyRetryMin = time.Millisecond
	earlyRetryMax = 5 * time.Millisecond
	earlyLeaderWait = time.Millisecond
}

type fakeEarlyManifest struct {
	mu          sync.Mutex
	leaderErrs  int                  // WaitForLeader fails this many times
	batchErrs   []error              // returned by successive batch calls, then nil
	beforeBatch func(paths []string) // runs inside a batch call, before it commits
	registered  map[string]raft.FileEntry
	deleted     []string
	batches     [][]string
	existing    map[string]bool
}

func newFakeEarlyManifest() *fakeEarlyManifest {
	return &fakeEarlyManifest{registered: map[string]raft.FileEntry{}, existing: map[string]bool{}}
}

func (m *fakeEarlyManifest) WaitForLeader(time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leaderErrs > 0 {
		m.leaderErrs--
		return ErrNoLeaderKnown
	}
	return nil
}

func (m *fakeEarlyManifest) SyncManifest(context.Context) error { return nil }

func (m *fakeEarlyManifest) GetFileEntry(path string) (*raft.FileEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.existing[path] {
		return &raft.FileEntry{Path: path}, true
	}
	if e, ok := m.registered[path]; ok {
		return &e, true
	}
	return nil, false
}

func (m *fakeEarlyManifest) BatchFileOpsInManifestContext(_ context.Context, ops []raft.BatchFileOp) error {
	var entries []raft.FileEntry
	var paths []string
	for _, op := range ops {
		var p raft.RegisterFilePayload
		if err := json.Unmarshal(op.Payload, &p); err != nil {
			return err
		}
		entries = append(entries, p.File)
		paths = append(paths, p.File.Path)
	}
	m.mu.Lock()
	m.batches = append(m.batches, paths)
	hook := m.beforeBatch
	var err error
	if len(m.batchErrs) > 0 {
		err, m.batchErrs = m.batchErrs[0], m.batchErrs[1:]
	}
	m.mu.Unlock()
	if hook != nil {
		hook(paths)
	}
	if err != nil {
		return err
	}
	m.mu.Lock()
	for _, e := range entries {
		m.registered[e.Path] = e
	}
	m.mu.Unlock()
	return nil
}

func (m *fakeEarlyManifest) DeleteFileFromManifest(_ context.Context, path, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.registered, path)
	m.deleted = append(m.deleted, path)
	return nil
}

type fakeEarlyStorage struct {
	mu     sync.Mutex
	sizes  map[string]int64
	broken map[string]bool // StatFile fails for these, every time
}

func (s *fakeEarlyStorage) StatFile(_ context.Context, path string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken[path] {
		return -1, errors.New("access denied")
	}
	if size, ok := s.sizes[path]; ok {
		return size, nil
	}
	return -1, nil
}

func (s *fakeEarlyStorage) remove(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sizes, path)
}

type recordingLive struct {
	mu    sync.Mutex
	paths []string
}

func (l *recordingLive) RegisterFile(_, _, path string, _ time.Time, _ int64, _ string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.paths = append(l.paths, path)
}

func (l *recordingLive) has(path string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.paths {
		if p == path {
			return true
		}
	}
	return false
}

func newTestEarly(t *testing.T, dir string) *EarlyFileRegistrar {
	t.Helper()
	r, err := NewEarlyFileRegistrar(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEarlyFileRegistrar: %v", err)
	}
	t.Cleanup(r.Stop)
	return r
}

func waitDrained(t *testing.T, r *EarlyFileRegistrar) {
	t.Helper()
	select {
	case <-r.Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("drain did not finish; %d pending", r.Pending())
	}
}

func journalExists(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, EarlyFileRegistrarJournal))
	return err == nil
}

const (
	earlyPathA = "db/cpu/2026/10/11/10/cpu_a.parquet"
	earlyPathB = "db/cpu/2026/10/11/10/cpu_b.parquet"
)

func registerEarly(r *EarlyFileRegistrar, path string, size int64) {
	r.RegisterFile("db", "cpu", path, time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC), size, "abc")
}

// The issue itself: a file flushed before the hand-off reaches the manifest
// after it, under this node as origin.
func TestEarlyRegistrarRegistersFilesFlushedBeforeHandOff(t *testing.T) {
	dir := t.TempDir()
	r := newTestEarly(t, dir)
	registerEarly(r, earlyPathA, 10)
	registerEarly(r, earlyPathB, 20)

	m := newFakeEarlyManifest()
	store := &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10, earlyPathB: 20}}
	r.HandOff(m, store, &recordingLive{}, "writer-1")
	waitDrained(t, r)

	for _, p := range []string{earlyPathA, earlyPathB} {
		e, ok := m.registered[p]
		if !ok {
			t.Fatalf("%s was not registered", p)
		}
		if e.OriginNodeID != "writer-1" || e.Tier != "hot" || e.Database != "db" || e.Measurement != "cpu" {
			t.Fatalf("%s registered as %+v", p, e)
		}
	}
	if journalExists(dir) {
		t.Fatal("journal left behind after every pending file was registered")
	}
}

// A start killed before the hand-off (a signal during startup runs no
// shutdown hook) must not lose the files: recovery has already deleted the WAL.
func TestEarlyRegistrarJournalSurvivesACrash(t *testing.T) {
	dir := t.TempDir()
	first, err := NewEarlyFileRegistrar(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	registerEarly(first, earlyPathA, 10)
	// No Stop, no HandOff: the process is gone.

	second := newTestEarly(t, dir)
	if got := second.Pending(); got != 1 {
		t.Fatalf("restart loaded %d pending files, want 1", got)
	}
	m := newFakeEarlyManifest()
	second.HandOff(m, &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10}}, &recordingLive{}, "writer-1")
	waitDrained(t, second)
	if _, ok := m.registered[earlyPathA]; !ok {
		t.Fatal("file journaled by the crashed start was not registered after restart")
	}
}

// Nothing else will register these files, so no failure ends the drain: not
// a missing leader, not a refused batch.
func TestEarlyRegistrarRetriesUntilRegistered(t *testing.T) {
	r := newTestEarly(t, t.TempDir())
	registerEarly(r, earlyPathA, 10)

	m := newFakeEarlyManifest()
	m.leaderErrs = 3
	m.batchErrs = []error{errors.New("no quorum"), ErrLeaderUnreachable}
	r.HandOff(m, &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10}}, &recordingLive{}, "writer-1")
	waitDrained(t, r)
	if _, ok := m.registered[earlyPathA]; !ok {
		t.Fatal("file not registered after transient failures")
	}
}

// Compaction, retention or a DELETE can remove or rewrite a file before the
// cluster is up. Registering it would give readers an entry nothing serves.
func TestEarlyRegistrarSkipsFilesGoneOrRewritten(t *testing.T) {
	dir := t.TempDir()
	r := newTestEarly(t, dir)
	registerEarly(r, earlyPathA, 10) // gone
	registerEarly(r, earlyPathB, 20) // rewritten to another size

	m := newFakeEarlyManifest()
	r.HandOff(m, &fakeEarlyStorage{sizes: map[string]int64{earlyPathB: 99}}, &recordingLive{}, "writer-1")
	waitDrained(t, r)
	if len(m.registered) != 0 {
		t.Fatalf("registered %v; a missing or rewritten file must not be", m.registered)
	}
	if journalExists(dir) {
		t.Fatal("skipped files left in the journal")
	}
}

// The disk check must be fresh for every attempt. A batch that failed once is
// retried later, and the file can be deleted in between.
func TestEarlyRegistrarRechecksTheFileBeforeEachAttempt(t *testing.T) {
	r := newTestEarly(t, t.TempDir())
	registerEarly(r, earlyPathA, 10)
	registerEarly(r, earlyPathB, 20)

	store := &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10, earlyPathB: 20}}
	m := newFakeEarlyManifest()
	m.batchErrs = []error{errors.New("no quorum")}
	m.beforeBatch = func([]string) {
		// After the first (failing) attempt has stat'ed both files.
		store.remove(earlyPathA)
	}
	r.HandOff(m, store, &recordingLive{}, "writer-1")
	waitDrained(t, r)

	// Checked on the attempt itself, not the end state: the post-commit check
	// would take a stale entry out again, but readers could see it until then.
	if len(m.batches) < 2 {
		t.Fatalf("want a failed attempt and a retry, got %v", m.batches)
	}
	for _, p := range m.batches[len(m.batches)-1] {
		if p == earlyPathA {
			t.Fatal("the retry sent a file deleted after the first attempt: the stat was not repeated")
		}
	}
	if _, ok := m.registered[earlyPathB]; !ok {
		t.Fatal("the surviving file was not registered")
	}
}

// A file deleted while its batch commits leaves an entry no peer can pull,
// which keeps a reader query gate red. It must be taken out again.
func TestEarlyRegistrarRemovesTheEntryOfAFileThatVanishedDuringCommit(t *testing.T) {
	r := newTestEarly(t, t.TempDir())
	registerEarly(r, earlyPathA, 10)

	store := &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10}}
	m := newFakeEarlyManifest()
	m.beforeBatch = func([]string) { store.remove(earlyPathA) }
	r.HandOff(m, store, &recordingLive{}, "writer-1")
	waitDrained(t, r)

	if _, ok := m.registered[earlyPathA]; ok || len(m.deleted) != 1 {
		t.Fatalf("vanished file left in the manifest (deleted=%v)", m.deleted)
	}
}

// Already in the manifest (a previous start registered it and then crashed
// before rewriting the journal): no second registration, which would reset
// the entry's tier and creation time.
func TestEarlyRegistrarDoesNotReRegisterAKnownFile(t *testing.T) {
	r := newTestEarly(t, t.TempDir())
	registerEarly(r, earlyPathA, 10)

	m := newFakeEarlyManifest()
	m.existing[earlyPathA] = true
	r.HandOff(m, &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10}}, &recordingLive{}, "writer-1")
	waitDrained(t, r)
	if len(m.batches) != 0 {
		t.Fatalf("re-registered a file already in the manifest: %v", m.batches)
	}
}

// After the hand-off every file goes to the live registrar, not the journal.
func TestEarlyRegistrarForwardsAfterHandOff(t *testing.T) {
	dir := t.TempDir()
	r := newTestEarly(t, dir)
	live := &recordingLive{}
	r.HandOff(newFakeEarlyManifest(), &fakeEarlyStorage{}, live, "writer-1")
	registerEarly(r, earlyPathA, 10)
	if !live.has(earlyPathA) {
		t.Fatal("RegisterFile after the hand-off did not reach the live registrar")
	}
	if r.Pending() != 0 || journalExists(dir) {
		t.Fatal("RegisterFile after the hand-off was journaled")
	}
}

// Files registered concurrently with the hand-off are either drained or
// forwarded; none is stranded in a list nobody reads.
func TestEarlyRegistrarHandOffStrandsNothing(t *testing.T) {
	for i := 0; i < 20; i++ {
		r := newTestEarly(t, t.TempDir())
		m := newFakeEarlyManifest()
		store := &fakeEarlyStorage{sizes: map[string]int64{}}
		live := &recordingLive{}
		const n = 50
		paths := make([]string, n)
		for j := range paths {
			paths[j] = filepath.ToSlash(filepath.Join("db/cpu/2026/10/11/10", "f"+string(rune('a'+j%26))+string(rune('a'+j/26))+".parquet"))
			store.sizes[paths[j]] = 1
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, p := range paths {
				registerEarly(r, p, 1)
			}
		}()
		r.HandOff(m, store, live, "writer-1")
		wg.Wait()
		waitDrained(t, r)
		for _, p := range paths {
			if _, ok := m.registered[p]; !ok && !live.has(p) {
				t.Fatalf("iteration %d: %s neither drained nor forwarded", i, p)
			}
		}
	}
}

// A start whose cluster never came up stops journaling (no fsync per flush,
// no unbounded journal) and keeps what was pending for the next start.
func TestEarlyRegistrarAbandonKeepsThePendingAndStopsJournaling(t *testing.T) {
	dir := t.TempDir()
	r := newTestEarly(t, dir)
	registerEarly(r, earlyPathA, 10)
	r.Abandon()
	registerEarly(r, earlyPathB, 20)

	again := newTestEarly(t, dir)
	if got := again.Pending(); got != 1 {
		t.Fatalf("next start loaded %d pending files, want only the one journaled before Abandon", got)
	}
}

// A journal from another cluster is not announced in this one.
func TestEarlyRegistrarDropsAnotherClustersJournal(t *testing.T) {
	dir := t.TempDir()
	other, err := NewEarlyFileRegistrar(dir, "other-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	registerEarly(other, earlyPathA, 10)

	r := newTestEarly(t, dir)
	if got := r.Pending(); got != 0 {
		t.Fatalf("loaded %d records written for another cluster", got)
	}
}

// Shutdown must not wait on a leader that never comes.
func TestEarlyRegistrarStopIsBounded(t *testing.T) {
	dir := t.TempDir()
	r, err := NewEarlyFileRegistrar(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	registerEarly(r, earlyPathA, 10)
	m := newFakeEarlyManifest()
	m.leaderErrs = 1 << 30
	r.HandOff(m, &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10}}, &recordingLive{}, "writer-1")

	start := time.Now()
	r.Stop()
	if took := time.Since(start); took > earlyStopBudget+time.Second {
		t.Fatalf("Stop took %v", took)
	}
	if !journalExists(dir) {
		t.Fatal("unconfirmed file dropped from the journal at shutdown")
	}
}

// A file whose stat keeps failing (S3 403, EACCES) stays pending, but must not
// hold back the files behind it.
func TestEarlyRegistrarAnUnreadableFileDoesNotBlockTheRest(t *testing.T) {
	r := newTestEarly(t, t.TempDir())
	registerEarly(r, earlyPathA, 10) // first, and never readable
	registerEarly(r, earlyPathB, 20)

	m := newFakeEarlyManifest()
	store := &fakeEarlyStorage{sizes: map[string]int64{earlyPathA: 10, earlyPathB: 20}, broken: map[string]bool{earlyPathA: true}}
	r.HandOff(m, store, &recordingLive{}, "writer-1")

	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		_, ok := m.registered[earlyPathB]
		m.mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a file behind an unreadable one was never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := r.Pending(); got != 1 {
		t.Fatalf("pending = %d, want the unreadable file kept for a later round", got)
	}
}

// A torn line in the middle of the journal must not take the records after it.
func TestEarlyRegistrarJournalSurvivesATornLine(t *testing.T) {
	dir := t.TempDir()
	good := func(path string) string {
		b, _ := json.Marshal(earlyRecord{ClusterName: "test-cluster", Database: "db", Measurement: "cpu", Path: path, SizeBytes: 1})
		return string(b)
	}
	body := good(earlyPathA) + "\n" + `{"cluster_name":"test-clu` + "\n" + good(earlyPathB) + "\n"
	if err := os.WriteFile(filepath.Join(dir, EarlyFileRegistrarJournal), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	r := newTestEarly(t, dir)
	if got := r.Pending(); got != 2 {
		t.Fatalf("loaded %d records around a torn line, want 2", got)
	}
}

// Created before Raft on a fresh node, the directory must be as private as
// Raft makes it: Raft snapshots land in it later.
func TestEarlyRegistrarCreatesAPrivateDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "raft")
	newTestEarly(t, dir)
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o700 {
		t.Fatalf("raft data directory created with %o, want 700", mode)
	}
}
