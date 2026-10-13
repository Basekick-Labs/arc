package cluster

// #1206: files flushed before the cluster coordinator is up reached no
// manifest. #1209: after that, the registrar dropped on a full queue, gave up
// on a failed apply and kept nothing on disk. The outbox journals every
// registration and retries until the manifest confirms it.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

func init() {
	outboxRetryMin = time.Millisecond
	outboxRetryMax = 5 * time.Millisecond
	outboxLeaderWait = time.Millisecond
}

type fakeOutboxManifest struct {
	mu          sync.Mutex
	leaderErrs  int
	leaderGate  chan struct{}
	batchErrs   []error
	beforeBatch func(paths []string)
	registered  map[string]raft.FileEntry
	deleted     []string
	batches     [][]string
	existing    map[string]bool
	claims      bool                    // StreamClaimsEnabled
	coverage    []raft.InstanceCoverage // coverage carried by registrations
	supersedes  []string                // Supersedes carried by registrations
	refuse      func(path string) bool  // a claim refuses this registration
	pending     map[string]map[string]raft.PendingRegistrationPayload
	refuseKind  string            // the refusal's kind (default claimed)
	deleteSeen  map[string]uint64 // DeleteSeen carried by registrations
}

func newFakeOutboxManifest() *fakeOutboxManifest {
	return &fakeOutboxManifest{registered: map[string]raft.FileEntry{}, existing: map[string]bool{}, pending: map[string]map[string]raft.PendingRegistrationPayload{}, deleteSeen: map[string]uint64{}}
}

func (m *fakeOutboxManifest) WaitForLeader(time.Duration) error {
	m.mu.Lock()
	gate := m.leaderGate
	if m.leaderErrs > 0 {
		m.leaderErrs--
		m.mu.Unlock()
		return ErrNoLeaderKnown
	}
	m.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return nil
}

func (m *fakeOutboxManifest) SyncManifest(context.Context) error { return nil }

func (m *fakeOutboxManifest) StreamClaimsEnabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.claims
}

func (m *fakeOutboxManifest) GetFileEntry(path string) (*raft.FileEntry, bool) {
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

func (m *fakeOutboxManifest) BatchFileOpsInManifestContext(_ context.Context, ops []raft.BatchFileOp) error {
	var entries []raft.FileEntry
	var paths, deletes []string
	var refused []raft.RefusedOp
	m.mu.Lock()
	refuse := m.refuse
	m.mu.Unlock()
	var pendingOps []struct {
		set bool
		p   raft.PendingRegistrationPayload
	}
	for i, op := range ops {
		if op.Type == raft.CommandDeleteFile {
			var d raft.DeleteFilePayload
			if err := json.Unmarshal(op.Payload, &d); err != nil {
				return err
			}
			deletes = append(deletes, d.Path)
			continue
		}
		if op.Type == raft.CommandSetPendingRegistration || op.Type == raft.CommandClearPendingRegistration {
			var p raft.PendingRegistrationPayload
			if err := json.Unmarshal(op.Payload, &p); err != nil {
				return err
			}
			pendingOps = append(pendingOps, struct {
				set bool
				p   raft.PendingRegistrationPayload
			}{op.Type == raft.CommandSetPendingRegistration, p})
			continue
		}
		if op.Type != raft.CommandRegisterFile {
			return fmt.Errorf("unsupported test batch operation %d", op.Type)
		}
		var p raft.RegisterFilePayload
		if err := json.Unmarshal(op.Payload, &p); err != nil {
			return err
		}
		entries = append(entries, p.File)
		paths = append(paths, p.File.Path)
		m.mu.Lock()
		kind := m.refuseKind
		if m.deleteSeen == nil {
			m.deleteSeen = map[string]uint64{}
		}
		m.deleteSeen[p.File.Path] = p.DeleteSeen
		m.mu.Unlock()
		if kind == "" {
			kind = raft.RefusedClaimed
		}
		if refuse != nil && refuse(p.File.Path) {
			refused = append(refused, raft.RefusedOp{Op: i, Kind: kind})
		}
		m.mu.Lock()
		m.coverage = append(m.coverage, p.Cov2...)
		if p.Supersedes != "" {
			m.supersedes = append(m.supersedes, p.Supersedes)
		}
		m.mu.Unlock()
	}
	m.mu.Lock()
	if len(paths) > 0 {
		m.batches = append(m.batches, paths)
	}
	hook := m.beforeBatch
	if len(paths) == 0 {
		hook = nil
	}
	var err error
	if len(paths) > 0 && len(m.batchErrs) > 0 {
		err, m.batchErrs = m.batchErrs[0], m.batchErrs[1:]
	}
	m.mu.Unlock()
	if hook != nil {
		hook(paths)
	}
	if err != nil {
		return err
	}
	if len(refused) > 0 {
		// As a forwarded refusal arrives: text only.
		return errors.New("leader rejected (claim_conflict): " + (&raft.StreamClaimConflictError{Ops: refused}).Error())
	}
	m.mu.Lock()
	for _, e := range entries {
		m.registered[e.Path] = e
	}
	for _, p := range deletes {
		delete(m.registered, p)
	}
	for _, op := range pendingOps {
		byID := m.pending[op.p.Path]
		if op.set {
			if byID == nil {
				byID = make(map[string]raft.PendingRegistrationPayload)
				m.pending[op.p.Path] = byID
			}
			byID[op.p.OwnerID+"\x00"+op.p.RegistrationID] = op.p
		} else if existing, ok := byID[op.p.OwnerID+"\x00"+op.p.RegistrationID]; ok && existing.OwnerID == op.p.OwnerID {
			delete(byID, op.p.OwnerID+"\x00"+op.p.RegistrationID)
			if len(byID) == 0 {
				delete(m.pending, op.p.Path)
			}
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *fakeOutboxManifest) DeleteFileFromManifest(_ context.Context, path, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.registered, path)
	m.deleted = append(m.deleted, path)
	return nil
}

func (m *fakeOutboxManifest) isRegistered(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.registered[path]
	return ok
}

func (m *fakeOutboxManifest) batchLog() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]string(nil), m.batches...)
}

type fakeOutboxStorage struct {
	mu     sync.Mutex
	sizes  map[string]int64
	broken map[string]bool
	stats  atomic.Int64
}

func (s *fakeOutboxStorage) StatFile(_ context.Context, path string) (int64, error) {
	s.stats.Add(1)
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

func (s *fakeOutboxStorage) remove(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sizes, path)
}

func newTestOutbox(t *testing.T, dir string) *RegistrationOutbox {
	t.Helper()
	r, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRegistrationOutbox: %v", err)
	}
	t.Cleanup(r.Stop)
	return r
}

// crash drops an outbox without Stop, as a SIGKILL would, and closes its fd so
// a test can reopen the directory. Unsynced appends stay visible (page cache),
// so restart tests cannot catch a missing fsync; the fsync-snapshot test does.
func crash(r *RegistrationOutbox) {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done // the drain must not outlive the test that crashed it
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.journal != nil {
		_ = r.journal.Close()
		r.journal = nil
	}
	r.state = outboxAbandoned
}

func waitOutbox(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func journalSize(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, RegistrationJournal))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

const (
	outPathA = "db/cpu/2026/10/11/10/cpu_a.parquet"
	outPathB = "db/cpu/2026/10/11/10/cpu_b.parquet"
	outPathC = "db/cpu/2026/10/11/10/cpu_c.parquet"
)

func register(r *RegistrationOutbox, path string, size int64) {
	r.RegisterFile("db", "cpu", path, time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC), size, "abc")
}

// loadPending writes paths through one outbox and reopens the directory, so
// the entries come back as loaded ones: the storage checks apply to them.
func loadPending(t *testing.T, dir string, sizes map[string]int64) *RegistrationOutbox {
	t.Helper()
	first, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for p, size := range sizes {
		register(first, p, size)
	}
	if err := first.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	crash(first)
	return newTestOutbox(t, dir)
}

// #1206: a file flushed before the hand-off is registered after it, under this
// node as origin, and the journal ends empty.
func TestOutboxRegistersFilesFlushedBeforeHandOff(t *testing.T) {
	dir := t.TempDir()
	r := newTestOutbox(t, dir)
	register(r, outPathA, 10)
	register(r, outPathB, 20)

	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}, "writer-1")
	waitOutbox(t, "both registered and the journal emptied", func() bool { return r.Pending() == 0 && journalSize(t, dir) == 0 })

	for _, p := range []string{outPathA, outPathB} {
		e := m.registered[p]
		if e.OriginNodeID != "writer-1" || e.Tier != "hot" || e.Database != "db" || e.Measurement != "cpu" {
			t.Fatalf("%s registered as %+v", p, e)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) != 0 {
		t.Fatalf("pending registration intents remain after manifest registration: %v", m.pending)
	}
}

// #1209: steady state cannot drop. The old registrar's queue held 4096.
func TestOutboxDropsNothingInSteadyState(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	m := newFakeOutboxManifest()
	store := &fakeOutboxStorage{sizes: map[string]int64{}}
	r.HandOff(m, store, "writer-1")
	const n = 10000
	for i := 0; i < n; i++ {
		register(r, fmt.Sprintf("db/cpu/2026/10/11/10/f%05d.parquet", i), 1)
	}
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	waitOutbox(t, "all registered", func() bool { return r.Pending() == 0 })
	m.mu.Lock()
	got := len(m.registered)
	m.mu.Unlock()
	if got != n {
		t.Fatalf("registered %d of %d files", got, n)
	}
}

// A crash after the flush synced its registrations and before the apply must
// not lose the file: the WAL rows behind it may already be gone.
func TestOutboxJournalSurvivesACrash(t *testing.T) {
	dir := t.TempDir()
	r := loadPending(t, dir, map[string]int64{outPathA: 10})
	if got := r.Pending(); got != 1 {
		t.Fatalf("restart loaded %d pending files, want 1", got)
	}
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "registered after restart", func() bool { return m.isRegistered(outPathA) })
}

// Nothing else registers these files, so no failure class ends the retries.
// The old registrar dropped a file on any non-transient apply error.
func TestOutboxRetriesEveryFailure(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	register(r, outPathA, 10)
	m := newFakeOutboxManifest()
	m.leaderErrs = 3
	m.batchErrs = []error{errors.New("no quorum"), ErrLeaderUnreachable, errors.New("forward rejected")}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "registered after failures", func() bool { return m.isRegistered(outPathA) })
}

// A file flushed by this process was just written: its first attempt costs no
// storage request. On object storage each check is one.
func TestOutboxChecksNoStorageOnAFreshFilesFirstAttempt(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	m := newFakeOutboxManifest()
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}
	register(r, outPathA, 10)
	r.HandOff(m, store, "writer-1")
	waitOutbox(t, "seed registration", func() bool { return m.isRegistered(outPathA) })
	register(r, outPathB, 10)
	waitOutbox(t, "registered after the drain became idle", func() bool { return m.isRegistered(outPathB) })
	if n := store.stats.Load(); n != 0 {
		t.Fatalf("%d storage checks for a file registered on its first attempt", n)
	}
}

// Compaction, retention or a DELETE may remove or rewrite a file before it is
// registered. Registering it would give readers an entry nothing serves.
func TestOutboxSkipsFilesGoneOrRewritten(t *testing.T) {
	dir := t.TempDir()
	r := loadPending(t, dir, map[string]int64{outPathA: 10, outPathB: 20})
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathB: 99}}, "writer-1")
	waitOutbox(t, "both settled", func() bool { return r.Pending() == 0 })
	if len(m.registered) != 0 {
		t.Fatalf("registered %v; a missing or rewritten file must not be", m.registered)
	}
}

// The storage check runs before every retry: the file can be deleted between
// attempts.
func TestOutboxRechecksTheFileBeforeEachRetry(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	register(r, outPathA, 10)
	register(r, outPathB, 20)
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}
	m := newFakeOutboxManifest()
	m.batchErrs = []error{errors.New("no quorum")}
	m.beforeBatch = func([]string) { store.remove(outPathA) }
	r.HandOff(m, store, "writer-1")
	waitOutbox(t, "settled", func() bool { return r.Pending() == 0 })

	batches := m.batchLog()
	if len(batches) < 2 {
		t.Fatalf("want a failed attempt and a retry, got %v", batches)
	}
	for _, p := range batches[len(batches)-1] {
		if p == outPathA {
			t.Fatal("the retry sent a file deleted after the first attempt")
		}
	}
	if !m.isRegistered(outPathB) {
		t.Fatal("the surviving file was not registered")
	}
}

// A retried file deleted while its batch commits leaves an entry no peer can
// pull, which keeps a reader query gate red. It is taken out again.
func TestOutboxRemovesTheEntryOfAFileThatVanishedDuringCommit(t *testing.T) {
	r := loadPending(t, t.TempDir(), map[string]int64{outPathA: 10})
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}
	m := newFakeOutboxManifest()
	m.beforeBatch = func([]string) { store.remove(outPathA) }
	r.HandOff(m, store, "writer-1")
	waitOutbox(t, "settled", func() bool { return r.Pending() == 0 })
	if m.isRegistered(outPathA) {
		t.Fatalf("vanished file left in the manifest (deleted=%v)", m.deleted)
	}
}

// Already in the manifest: no second registration, which would reset the
// entry's tier and creation time.
func TestOutboxDoesNotReRegisterAKnownFile(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	register(r, outPathA, 10)
	m := newFakeOutboxManifest()
	m.existing[outPathA] = true
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "settled", func() bool { return r.Pending() == 0 })
	if b := m.batchLog(); len(b) != 0 {
		t.Fatalf("re-registered a file already in the manifest: %v", b)
	}
}

// A file whose check keeps failing (S3 403, EACCES) stays pending without
// holding back the files behind it.
func TestOutboxAnUnreadableFileDoesNotBlockTheRest(t *testing.T) {
	r := loadPending(t, t.TempDir(), map[string]int64{outPathA: 10, outPathB: 20})
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}, broken: map[string]bool{outPathA: true}}
	m := newFakeOutboxManifest()
	r.HandOff(m, store, "writer-1")
	waitOutbox(t, "the readable file registered", func() bool { return m.isRegistered(outPathB) })
	if got := r.Pending(); got != 1 {
		t.Fatalf("pending = %d, want the unreadable file kept", got)
	}
}

// B1: the journal is emptied by truncating the open handle, not by replacing
// the file. Replacing it under an open handle loses every later append.
func TestOutboxAnAppendAfterTheJournalEmptiedSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}, "writer-1")
	register(r, outPathA, 10)
	waitOutbox(t, "first file registered and journal emptied", func() bool { return r.Pending() == 0 && journalSize(t, dir) == 0 })

	m.mu.Lock()
	m.batchErrs = make([]error, 100000) // nothing registers from here
	for i := range m.batchErrs {
		m.batchErrs[i] = errors.New("no quorum")
	}
	m.mu.Unlock()
	register(r, outPathB, 20)
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	crash(r)

	again := newTestOutbox(t, dir)
	if !again.IsPending(outPathB) {
		t.Fatal("a file registered after the journal was emptied did not survive a restart")
	}
}

// B1, the rewrite path: compaction replaces the file and must reopen the
// handle, or later appends land on the unlinked inode.
func TestOutboxAnAppendAfterACompactionSurvivesARestart(t *testing.T) {
	old := outboxCompactSlack
	outboxCompactSlack = 2
	t.Cleanup(func() { outboxCompactSlack = old })

	dir := t.TempDir()
	// outPathA stays pending (loaded, and its check fails), so the journal is
	// rewritten rather than emptied once the acks pile up.
	r := loadPending(t, dir, map[string]int64{outPathA: 10})
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathB: 20}, broken: map[string]bool{outPathA: true}}
	m := newFakeOutboxManifest()
	r.HandOff(m, store, "writer-1")
	for i := 0; i < 20; i++ {
		register(r, fmt.Sprintf("db/cpu/2026/10/11/10/c%02d.parquet", i), 1)
	}
	waitOutbox(t, "all but the first registered", func() bool { return r.Pending() == 1 })

	m.mu.Lock()
	m.batchErrs = make([]error, 100000) // nothing registers from here
	for i := range m.batchErrs {
		m.batchErrs[i] = errors.New("no quorum")
	}
	m.mu.Unlock()
	register(r, outPathB, 20)
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	records := r.fileRecords
	r.mu.Unlock()
	if records > outboxCompactSlack+2 {
		t.Fatalf("journal holds %d records: no compaction happened, so this proves nothing", records)
	}
	crash(r)
	again := newTestOutbox(t, dir)
	if !again.IsPending(outPathB) {
		t.Fatal("a file registered after a journal compaction did not survive a restart")
	}
}

// B2: an ack is durable before the journal can be emptied. Otherwise a crash
// re-registers a file the manifest has since dropped on purpose (a compaction
// source whose storage delete lagged): duplicate rows on every reader.
func TestOutboxAcksAreDurable(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, RegistrationJournal)
	var mu sync.Mutex
	var syncedSnapshots []string
	prev := fsyncFile
	fsyncFile = func(f *os.File) error {
		if f.Name() == journal {
			b, _ := os.ReadFile(journal)
			mu.Lock()
			syncedSnapshots = append(syncedSnapshots, string(b))
			mu.Unlock()
		}
		return prev(f)
	}
	t.Cleanup(func() { fsyncFile = prev })

	r := newTestOutbox(t, dir)
	register(r, outPathA, 10)
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "registered", func() bool { return r.Pending() == 0 })

	mu.Lock()
	defer mu.Unlock()
	for _, snap := range syncedSnapshots {
		if strings.Contains(snap, `"op":"ack"`) && strings.Contains(snap, outPathA) {
			return
		}
	}
	t.Fatalf("no fsync covered the ack before the journal was emptied: %q", syncedSnapshots)
}

// One fsync per flush, however many files the flush registered.
func TestOutboxSharesOneFsyncAcrossAppends(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, RegistrationJournal)
	var syncs atomic.Int64
	prev := fsyncFile
	fsyncFile = func(f *os.File) error {
		if f.Name() == journal {
			syncs.Add(1)
		}
		return prev(f)
	}
	t.Cleanup(func() { fsyncFile = prev })

	r := newTestOutbox(t, dir)
	syncs.Store(0)
	for i := 0; i < 720; i++ { // a 720-hour backfill flush
		register(r, fmt.Sprintf("db/cpu/2026/10/11/10/h%03d.parquet", i), 1)
	}
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	if err := r.SyncRegistrations(); err != nil { // nothing new: no fsync
		t.Fatal(err)
	}
	if n := syncs.Load(); n != 1 {
		t.Fatalf("%d fsyncs for one flush of 720 files, want 1", n)
	}
}

// A failed fsync does not turn durability off for good: the next sync
// rewrites the journal from memory.
func TestOutboxRecoversFromAFailedFsync(t *testing.T) {
	dir := t.TempDir()
	journal := filepath.Join(dir, RegistrationJournal)
	var fail atomic.Bool
	prev := fsyncFile
	fsyncFile = func(f *os.File) error {
		if f.Name() == journal && fail.CompareAndSwap(true, false) {
			return errors.New("EIO")
		}
		return prev(f)
	}
	t.Cleanup(func() { fsyncFile = prev })

	r, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	register(r, outPathA, 10)
	fail.Store(true)
	if err := r.SyncRegistrations(); err == nil {
		t.Fatal("a failed fsync was reported as durable")
	}
	register(r, outPathB, 20)
	if err := r.SyncRegistrations(); err != nil {
		t.Fatalf("the sync after a failure did not recover: %v", err)
	}
	crash(r)
	again := newTestOutbox(t, dir)
	if !again.IsPending(outPathA) || !again.IsPending(outPathB) {
		t.Fatalf("after a failed fsync and a recovery, pending = %d, want both files", again.Pending())
	}
}

// A journal written by #1206 (add records with no op or version) loads.
func TestOutboxLoadsAJournalWithoutOps(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	for _, p := range []string{outPathA, outPathB} {
		b, _ := json.Marshal(map[string]any{"cluster_name": "test-cluster", "database": "db", "measurement": "cpu", "path": p, "size_bytes": 1})
		buf.Write(append(b, '\n'))
	}
	if err := os.WriteFile(filepath.Join(dir, RegistrationJournal), buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := newTestOutbox(t, dir).Pending(); got != 2 {
		t.Fatalf("loaded %d records from a #1206 journal, want 2", got)
	}
}

// Concurrent flushes during the hand-off: every file is registered.
func TestOutboxHandOffStrandsNothing(t *testing.T) {
	for i := 0; i < 10; i++ {
		r := newTestOutbox(t, t.TempDir())
		m := newFakeOutboxManifest()
		const n = 50
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < n; j++ {
				register(r, fmt.Sprintf("db/cpu/2026/10/11/10/s%02d.parquet", j), 1)
			}
		}()
		r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{}}, "writer-1")
		wg.Wait()
		if err := r.SyncRegistrations(); err != nil {
			t.Fatal(err)
		}
		waitOutbox(t, "all registered", func() bool { return r.Pending() == 0 })
		m.mu.Lock()
		got := len(m.registered)
		m.mu.Unlock()
		if got != n {
			t.Fatalf("iteration %d: registered %d of %d", i, got, n)
		}
	}
}

// A start whose cluster never came up stops journaling and keeps what was
// pending for the next start.
func TestOutboxAbandonKeepsThePendingAndStopsJournaling(t *testing.T) {
	dir := t.TempDir()
	r := newTestOutbox(t, dir)
	register(r, outPathA, 10)
	r.Abandon()
	register(r, outPathB, 20)
	again := newTestOutbox(t, dir)
	if !again.IsPending(outPathA) || again.IsPending(outPathB) {
		t.Fatalf("next start pending = %d, want only the file journaled before Abandon", again.Pending())
	}
}

// A journal from another cluster is not announced in this one.
func TestOutboxDropsAnotherClustersJournal(t *testing.T) {
	dir := t.TempDir()
	other, err := NewRegistrationOutbox(dir, "other-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	register(other, outPathA, 10)
	_ = other.SyncRegistrations()
	crash(other)
	if got := newTestOutbox(t, dir).Pending(); got != 0 {
		t.Fatalf("loaded %d records written for another cluster", got)
	}
}

// Shutdown does not wait on a leader that never comes, and keeps the file.
func TestOutboxStopIsBounded(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	register(r, outPathA, 10)
	m := newFakeOutboxManifest()
	m.leaderErrs = 1 << 30
	m.batchErrs = make([]error, 1000)
	for i := range m.batchErrs {
		m.batchErrs[i] = errors.New("no quorum")
	}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	start := time.Now()
	r.Stop()
	if took := time.Since(start); took > 2*outboxStopBudget+time.Second {
		t.Fatalf("Stop took %v", took)
	}
	crash(r)
	if !newTestOutbox(t, dir).IsPending(outPathA) {
		t.Fatal("unconfirmed file dropped at shutdown")
	}
}

// The final flush lands before Raft stops: Stop registers what is pending.
func TestOutboxStopRegistersTheFinalFlush(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{}}, "writer-1")
	register(r, outPathA, 10)
	r.Stop()
	if !m.isRegistered(outPathA) {
		t.Fatal("the final flush was not registered at shutdown")
	}
}

func TestOutboxTornLineDoesNotTakeTheRecordsAfterIt(t *testing.T) {
	dir := t.TempDir()
	good := func(path string) string {
		b, _ := json.Marshal(journalRecord{ClusterName: "test-cluster", Database: "db", Measurement: "cpu", Path: path, SizeBytes: 1})
		return string(b)
	}
	body := good(outPathA) + "\n" + `{"cluster_name":"test-clu` + "\n" + good(outPathB) + "\n"
	if err := os.WriteFile(filepath.Join(dir, RegistrationJournal), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := newTestOutbox(t, dir).Pending(); got != 2 {
		t.Fatalf("loaded %d records around a torn line, want 2", got)
	}
}

func TestOutboxCreatesAPrivateDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "raft")
	newTestOutbox(t, dir)
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o700 {
		t.Fatalf("raft data directory created with %o, want 700", mode)
	}
}

// M1: a file waiting behind a failing batch can be deleted long before its
// turn. Freshness is by age, so it is checked once it is no longer fresh.
func TestOutboxChecksAFileThatWaitedPastTheFreshWindow(t *testing.T) {
	old := outboxFreshWindow
	outboxFreshWindow = 20 * time.Millisecond
	t.Cleanup(func() { outboxFreshWindow = old })

	r := newTestOutbox(t, t.TempDir())
	store := &fakeOutboxStorage{sizes: map[string]int64{}}
	m := newFakeOutboxManifest()
	m.batchErrs = make([]error, 5)
	for i := range m.batchErrs {
		m.batchErrs[i] = errors.New("no quorum")
	}
	// More than one batch: the last file is never attempted while the
	// manifest fails.
	for i := 0; i < registrarDrainBatch+1; i++ {
		p := fmt.Sprintf("db/cpu/2026/10/11/10/w%05d.parquet", i)
		store.sizes[p] = 1
		register(r, p, 1)
	}
	tail := fmt.Sprintf("db/cpu/2026/10/11/10/w%05d.parquet", registrarDrainBatch)
	time.Sleep(50 * time.Millisecond)
	store.remove(tail) // deleted while it waited
	r.HandOff(m, store, "writer-1")
	waitOutbox(t, "settled", func() bool { return r.Pending() == 0 })
	if m.isRegistered(tail) {
		t.Fatal("registered a file deleted while it waited behind a failing batch")
	}
}

// #1210: a DELETE rewriting a pending file swaps it for the rewrite, durably.
func TestOutboxReplacePendingSwapsDurably(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	register(r, outPathA, 10)
	outcome, _, err := r.ReplacePending(context.Background(), outPathA, outPathB, 7, "new")
	if err != nil || outcome != ReplaceSwapped {
		t.Fatalf("outcome %v err %v, want swapped", outcome, err)
	}
	crash(r)
	again := newTestOutbox(t, dir)
	if again.IsPending(outPathA) || !again.IsPending(outPathB) {
		t.Fatal("after a restart the swap is lost: the original is pending or the rewrite is not")
	}
}

// A batch in flight carrying the original may commit it: ReplacePending waits
// for the outcome instead of racing the commit, and reports the registration.
func TestOutboxReplacePendingWaitsOutAnAttemptInFlight(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	register(r, outPathA, 10)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	m := newFakeOutboxManifest()
	m.beforeBatch = func([]string) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	<-entered

	type result struct {
		outcome ReplaceOutcome
		entry   raft.FileEntry
	}
	got := make(chan result, 1)
	go func() {
		o, e, _ := r.ReplacePending(context.Background(), outPathA, outPathB, 7, "new")
		got <- result{o, e}
	}()
	select {
	case res := <-got:
		t.Fatalf("ReplacePending returned %v while the original was being committed", res.outcome)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	res := <-got
	if res.outcome != ReplaceRegistered || res.entry.Path != outPathA || res.entry.Database != "db" {
		t.Fatalf("got %v %+v, want the original reported registered with its fields", res.outcome, res.entry)
	}
}

// Confirmed a moment ago and acknowledged: still recognised, because this
// node's FSM may not have applied the commit yet.
func TestOutboxReplacePendingRecognisesARecentlyConfirmedFile(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{}}, "writer-1")
	register(r, outPathA, 10)
	waitOutbox(t, "registered", func() bool { return r.Pending() == 0 })
	outcome, entry, err := r.ReplacePending(context.Background(), outPathA, outPathB, 7, "new")
	if err != nil || outcome != ReplaceRegistered || entry.Path != outPathA {
		t.Fatalf("outcome %v entry %+v err %v, want registered", outcome, entry, err)
	}
}

func TestOutboxReplacePendingIgnoresAnUnknownFile(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	outcome, _, err := r.ReplacePending(context.Background(), outPathA, outPathB, 7, "new")
	if err != nil || outcome != ReplaceNotPending {
		t.Fatalf("outcome %v err %v, want not pending", outcome, err)
	}
}

// M1: an apply can report failure after the leader committed it. A swap then
// must still take the old path out of the manifest: the rewrite's batch
// deletes it.
func TestOutboxSwappedRewriteDeletesTheOriginalInTheSameBatch(t *testing.T) {
	// Back off long enough that the swap lands between the failed attempt and
	// its retry, deterministically.
	oldMin, oldMax := outboxRetryMin, outboxRetryMax
	outboxRetryMin, outboxRetryMax = 500*time.Millisecond, 500*time.Millisecond
	t.Cleanup(func() { outboxRetryMin, outboxRetryMax = oldMin, oldMax })

	r := newTestOutbox(t, t.TempDir())
	register(r, outPathA, 10)
	m := newFakeOutboxManifest()
	m.batchErrs = []error{errors.New("timeout after commit")}
	m.beforeBatch = func(paths []string) {
		// First attempt: the leader commits, the caller sees an error.
		if len(paths) == 1 && paths[0] == outPathA {
			m.mu.Lock()
			m.registered[outPathA] = raft.FileEntry{Path: outPathA}
			m.beforeBatch = nil
			m.mu.Unlock()
		}
	}
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 7}}
	r.HandOff(m, store, "writer-1")
	waitOutbox(t, "first attempt failed", func() bool { return len(m.batchLog()) >= 1 })

	outcome, _, err := r.ReplacePending(context.Background(), outPathA, outPathB, 7, "new")
	if err != nil || outcome != ReplaceSwapped {
		t.Fatalf("outcome %v err %v, want swapped during the backoff", outcome, err)
	}
	waitOutbox(t, "rewrite registered", func() bool { return m.isRegistered(outPathB) })
	if m.isRegistered(outPathA) {
		t.Fatal("the original stays in the manifest after its rewrite was registered: a ghost entry")
	}
}

func TestOutboxWithdrawTakesAPendingFileOut(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	register(r, outPathA, 10)
	withdrawn, _, err := r.Withdraw(context.Background(), outPathA)
	if err != nil || !withdrawn {
		t.Fatalf("withdrawn=%v err=%v", withdrawn, err)
	}
	crash(r)
	if newTestOutbox(t, dir).IsPending(outPathA) {
		t.Fatal("a withdrawn file came back after a restart")
	}
}

// For cleanup: a file confirmed a moment ago still counts, because this
// node's FSM may not show it yet.
func TestOutboxHoldsRegistrationCoversRecentConfirmations(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	m := newFakeOutboxManifest()
	register(r, outPathA, 10)
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{}}, "writer-1")
	waitOutbox(t, "seed registration", func() bool { return m.isRegistered(outPathA) })
	register(r, outPathB, 10)
	if !r.HoldsRegistration(outPathB) {
		t.Fatal("a pending file is not held")
	}
	waitOutbox(t, "registered", func() bool { return m.isRegistered(outPathB) })
	if !r.HoldsRegistration(outPathB) {
		t.Fatal("a file confirmed a moment ago is not held")
	}
	if r.HoldsRegistration(outPathC) {
		t.Fatal("an unknown file is held")
	}
}

func TestOutboxSyncPublishesPendingRegistrationForOtherNodes(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	manifest := newFakeOutboxManifest()
	r.mu.Lock()
	r.nodeID = "writer-1"
	r.manifestRef = manifest
	r.mu.Unlock()
	r.RegisterFile("db", "measurement", "db/measurement/file.parquet", time.Now(), 12, "sum")
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	manifest.mu.Lock()
	defer manifest.mu.Unlock()
	if got := len(manifest.pending["db/measurement/file.parquet"]); got != 1 {
		t.Fatalf("published pending intent count = %d, want 1", got)
	}
}

func TestOutboxPublishesFlushBeforeDrainStarts(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	manifest := newFakeOutboxManifest()
	manifest.leaderGate = make(chan struct{})
	r.HandOff(manifest, &fakeOutboxStorage{sizes: map[string]int64{}}, "writer-1")
	defer func() {
		close(manifest.leaderGate)
		r.Stop()
	}()

	const path = "db/measurement/flush-before-drain.parquet"
	r.RegisterFile("db", "measurement", path, time.Now(), 12, "sum")
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	manifest.mu.Lock()
	defer manifest.mu.Unlock()
	if len(manifest.pending[path]) != 1 {
		t.Fatalf("flush returned without publishing its pending intent: %#v", manifest.pending[path])
	}
}

func TestOutboxClearUsesOwnerPersistedInJournal(t *testing.T) {
	dir := t.TempDir()
	r := newTestOutbox(t, dir)
	register(r, outPathA, 10)
	r.mu.Lock()
	entry := r.byPath[outPathA]
	entry.rec.OwnerID = "original-writer"
	r.appendLocked(entry.rec)
	r.mu.Unlock()
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	crash(r)

	restarted := newTestOutbox(t, dir)
	defer restarted.Stop()
	restarted.mu.Lock()
	restarted.nodeID = "new-process-node-id"
	entry = restarted.byPath[outPathA]
	restarted.mu.Unlock()
	clear, err := restarted.clearIntentOp(entry)
	if err != nil {
		t.Fatal(err)
	}
	var payload raft.PendingRegistrationPayload
	if err := json.Unmarshal(clear.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.OwnerID != "original-writer" {
		t.Fatalf("clear owner = %q, want original-writer", payload.OwnerID)
	}
}
