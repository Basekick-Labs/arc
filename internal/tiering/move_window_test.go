package tiering

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// hookCold runs onWrite after each cold copy lands, and can fail StatFile.
type hookCold struct {
	*coldLister
	onWrite func(path string)
	statErr error
}

func (c *hookCold) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if err := c.coldLister.WriteReader(ctx, path, r, size); err != nil {
		return err
	}
	if c.onWrite != nil {
		c.onWrite(path)
	}
	return nil
}

func (c *hookCold) StatFile(ctx context.Context, path string) (int64, error) {
	if c.statErr != nil {
		return -1, c.statErr
	}
	return c.coldLister.StatFile(ctx, path)
}

// hookHot runs onDelete before each hot delete.
type hookHot struct {
	*mockBackend
	onDelete func(path string)
}

func (h *hookHot) Delete(ctx context.Context, path string) error {
	if h.onDelete != nil {
		h.onDelete(path)
	}
	return h.mockBackend.Delete(ctx, path)
}

// moveRig is a primary-writer manager with hooked backends and rows for files.
func moveRig(t *testing.T, files ...string) (*Manager, *hookHot, *hookCold, *mockGate) {
	t.Helper()
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	t.Cleanup(cleanup)
	gate.primary.Store(true)
	hc := &hookCold{coldLister: cold}
	m.coldBackend = hc
	hh := &hookHot{mockBackend: hot}
	m.hotBackend = hh
	for _, p := range files {
		mustWrite(t, hot, p)
		recordHotRow(t, m, p, manifestPartition)
	}
	return m, hh, hc, gate
}

func journal(t *testing.T, b storage.Backend, path, replacedBy string) {
	t.Helper()
	data, _ := json.Marshal(map[string]string{"path": path, "replaced_by": replacedBy})
	if err := b.Write(context.Background(), storage.RetiredKey(path), data); err != nil {
		t.Fatal(err)
	}
}

func coldHas(t *testing.T, c *hookCold, path string) bool {
	t.Helper()
	ok, err := c.Exists(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestClaimForDelete_RefusesAMovingFileAndViceVersa(t *testing.T) {
	m, _, _, _ := moveRig(t, gateDailyA)
	ctx := context.Background()

	if !m.claims.claimMove(gateDailyA) {
		t.Fatal("claimMove refused a free file")
	}
	if _, err := m.ClaimForDelete(ctx, gateDailyA); !errors.Is(err, ErrFileMoving) {
		t.Fatalf("ClaimForDelete on a moving file = %v, want ErrFileMoving", err)
	}
	m.claims.releaseMove(gateDailyA)

	release, err := m.ClaimForDelete(ctx, gateDailyA)
	if err != nil {
		t.Fatalf("ClaimForDelete on a free file: %v", err)
	}
	if m.claims.claimMove(gateDailyA) {
		t.Fatal("claimMove took a file a DELETE holds")
	}
	release()
	if !m.claims.claimMove(gateDailyA) {
		t.Fatal("claimMove refused a file after the DELETE released it")
	}
}

// A cold copy next to a hot file is a move not finished (flipped and not
// released, or a flip that failed): a DELETE of the hot copy would leave the
// cold one holding the deleted rows.
func TestClaimForDelete_RefusesAFileWithAColdCopy(t *testing.T) {
	const hourly = "db1/cpu/2024/03/15/10/cpu_20240315_100000.parquet"
	m, _, cold, _ := moveRig(t, gateDailyA)
	ctx := context.Background()
	mustWrite(t, cold.mockBackend, gateDailyA)
	mustWrite(t, cold.mockBackend, hourly)

	if _, err := m.ClaimForDelete(ctx, gateDailyA); !errors.Is(err, ErrFileMoving) {
		t.Fatalf("ClaimForDelete with a cold copy = %v, want ErrFileMoving", err)
	}
	// Refused without holding the claim: the migrator can still finish it.
	if !m.claims.claimMove(gateDailyA) {
		t.Fatal("a refused ClaimForDelete kept its claim")
	}
	m.claims.releaseMove(gateDailyA)

	// Only a file the migrator could have copied is asked about.
	release, err := m.ClaimForDelete(ctx, hourly)
	if err != nil {
		t.Fatalf("ClaimForDelete on an hourly file = %v, want nil", err)
	}
	release()

	cold.statErr = errors.New("cold unreachable")
	if err := cold.mockBackend.Delete(ctx, gateDailyA); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ClaimForDelete(ctx, gateDailyA); !errors.Is(err, ErrColdCheckFailed) {
		t.Fatalf("ClaimForDelete with cold unreachable = %v, want ErrColdCheckFailed (fail closed)", err)
	}
}

func TestMigrateBatch_SkipsAFileADeleteHolds(t *testing.T) {
	m, _, cold, _ := moveRig(t, gateDailyA, gateDailyB)
	release, err := m.ClaimForDelete(context.Background(), gateDailyA)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	migrated, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if migrated != 1 || failed != 0 {
		t.Fatalf("MigrateBatch = (%d, %d), want (1, 0)", migrated, failed)
	}
	if coldHas(t, cold, gateDailyA) {
		t.Fatal("a file a DELETE holds was copied to cold")
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierHot {
		t.Fatalf("held file tier = %s, want hot", got)
	}
}

// The DELETE journal names the file after the copy started (the DELETE won
// the claim race before this migration claimed it): the cold copy holds the
// rows the DELETE removed, so it goes and the row stays hot.
func TestCopyAndFlip_DropsTheCopyOfAFileADeleteReplaced(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA)
	cold.onWrite = func(p string) { journal(t, hot.mockBackend, p, p+"_rewrite_1") }

	migrated, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA)})
	if migrated != 0 || failed != 1 {
		t.Fatalf("MigrateBatch = (%d, %d), want (0, 1)", migrated, failed)
	}
	if coldHas(t, cold, gateDailyA) {
		t.Fatal("the cold copy of a file a DELETE replaced was kept")
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierHot {
		t.Fatalf("tier = %s, want hot", got)
	}
}

// siblingManifest adds the rewrite-family lookup the cluster coordinator has.
type siblingManifest struct {
	*fakeManifest
	sibling map[string]bool
}

func (s *siblingManifest) HasRewriteSibling(path string) bool { return s.sibling[path] }

func TestCopyAndFlip_DropsTheCopyWhenTheManifestListsARewriteInstead(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA)
	fake := &siblingManifest{fakeManifest: &fakeManifest{hot: hot.mockBackend, entries: map[string]int64{gateDailyA: 7}}, sibling: map[string]bool{}}
	m.manifest = fake
	cold.onWrite = func(p string) {
		delete(fake.entries, p)
		fake.sibling[p] = true
	}

	if _, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA)}); failed != 1 {
		t.Fatalf("failed = %d, want 1", failed)
	}
	if coldHas(t, cold, gateDailyA) {
		t.Fatal("the cold copy of a file the manifest replaced was kept")
	}
}

// Unregistered with no registered sibling is not evidence: the copy stays.
func TestCopyAndFlip_KeepsTheCopyWithoutEvidence(t *testing.T) {
	m, hot, _, _ := moveRig(t, gateDailyA)
	m.manifest = &siblingManifest{fakeManifest: &fakeManifest{hot: hot.mockBackend, entries: map[string]int64{}}, sibling: map[string]bool{}}

	if migrated, _ := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA)}); migrated != 1 {
		t.Fatalf("migrated = %d, want 1", migrated)
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold", got)
	}
}

func TestFindCandidates_SkipsAFileTheJournalNames(t *testing.T) {
	m, hot, _, _ := moveRig(t, gateDailyA, gateDailyB)
	journal(t, hot.mockBackend, gateDailyA, gateDailyA+"_rewrite_1")

	cands, err := m.migrator.FindCandidates(context.Background(), TierHot, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 1 || cands[0].Path != gateDailyB {
		t.Fatalf("candidates = %+v, want only %s", cands, gateDailyB)
	}
}

func TestFindCandidates_SkipsAFileTheManifestReplaced(t *testing.T) {
	m, hot, _, _ := moveRig(t, gateDailyA, gateDailyB)
	m.manifest = &siblingManifest{
		fakeManifest: &fakeManifest{hot: hot.mockBackend, entries: map[string]int64{gateDailyB: 7}},
		sibling:      map[string]bool{gateDailyA: true, gateDailyB: true},
	}

	cands, err := m.migrator.FindCandidates(context.Background(), TierHot, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) != 1 || cands[0].Path != gateDailyB {
		t.Fatalf("candidates = %+v, want only %s (registered, sibling or not)", cands, gateDailyB)
	}
}

// The row stopped saying hot during the copy and the hot copy is gone:
// another node finished the move. The cold copy may be the only one left.
func TestCopyAndFlip_KeepsTheCopyWhenTheRowChanged(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA)
	cold.onWrite = func(p string) {
		if err := m.metadata.UpdateTier(context.Background(), p, TierCold); err != nil {
			t.Error(err)
		}
		_ = hot.mockBackend.Delete(context.Background(), p)
	}

	if migrated, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA)}); migrated != 0 || failed != 1 {
		t.Fatalf("MigrateBatch = (%d, %d), want (0, 1)", migrated, failed)
	}
	if !coldHas(t, cold, gateDailyA) {
		t.Fatal("the cold copy was deleted on a changed row: it can be the only copy")
	}
}

func TestCopyAndFlip_DemotedDuringTheCopyDoesNotFlip(t *testing.T) {
	m, hot, cold, gate := moveRig(t, gateDailyA, gateDailyB)
	m.migrator.maxConcurrent = 1
	cold.onWrite = func(string) { gate.primary.Store(false) }

	migrated, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if migrated != 0 || failed != 1 {
		t.Fatalf("MigrateBatch = (%d, %d), want (0, 1)", migrated, failed)
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierHot {
		t.Fatalf("tier = %s, want hot", got)
	}
	if ok, _ := hot.Exists(context.Background(), gateDailyA); !ok {
		t.Fatal("hot copy removed by a demoted node")
	}
	if coldHas(t, cold, gateDailyB) {
		t.Fatal("a demoted node started another copy")
	}
}

// A copy that finished flips even when the cycle is cancelled right after:
// otherwise the cold copy sits next to a hot row until the next cycle.
func TestCopyAndFlip_FlipsACompletedCopyAfterCancel(t *testing.T) {
	m, _, cold, _ := moveRig(t, gateDailyA)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cold.onWrite = func(string) { cancel() }

	_, _ = m.migrator.MigrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA)})
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold", got)
	}
}

// Hot copies are released as files flip, not after the whole batch: with one
// copy at a time, the third copy waits for the first file's hot delete.
func TestMigrateBatch_ReleasesHotCopiesWhileCopying(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA, gateDailyB, gateDailyC)
	m.migrator.maxConcurrent = 1
	aGone := make(chan struct{})
	var once sync.Once
	hot.onDelete = func(p string) {
		if p == gateDailyA {
			once.Do(func() { close(aGone) })
		}
	}
	waited := false
	cold.onWrite = func(p string) {
		if p != gateDailyC {
			return
		}
		select {
		case <-aGone:
		case <-time.After(5 * time.Second):
			waited = true
		}
	}

	if migrated, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB), candidateFor(gateDailyC)}); migrated != 3 || failed != 0 {
		t.Fatalf("MigrateBatch = (%d, %d), want (3, 0)", migrated, failed)
	}
	if waited {
		t.Fatal("the first file's hot copy was still there when the last copy finished: release waits for the batch")
	}
}

// orderManifest records the manifest call in a shared event log.
type orderManifest struct {
	*fakeManifest
	log   *[]string
	mu    *sync.Mutex
	fail  error
	after func()
}

func (o *orderManifest) DeleteFilesFromManifest(ctx context.Context, paths []string, reason string) error {
	o.mu.Lock()
	*o.log = append(*o.log, "manifest")
	o.mu.Unlock()
	if o.after != nil {
		o.after()
	}
	if o.fail != nil {
		return o.fail
	}
	return o.fakeManifest.DeleteFilesFromManifest(ctx, paths, reason)
}

func TestReleaseHotCopies_InvalidatesAroundTheDeletes(t *testing.T) {
	m, hot, _, _ := moveRig(t, gateDailyA)
	var mu sync.Mutex
	var events []string
	add := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }
	m.manifest = &orderManifest{fakeManifest: &fakeManifest{hot: hot.mockBackend, entries: map[string]int64{gateDailyA: 7}}, log: &events, mu: &mu}
	m.SetOnReadCachesStale(func() { add("invalidate") })
	hot.onDelete = func(string) { add("hot delete") }

	if migrated, _ := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA)}); migrated != 1 {
		t.Fatalf("migrated = %d, want 1", migrated)
	}
	want := []string{"invalidate", "manifest", "hot delete", "invalidate"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

// A manifest failure stops new copies; the ones in flight finish and flip.
func TestMigrateBatch_ManifestFailureStopsNewCopies(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA, gateDailyB, gateDailyC)
	m.migrator.maxConcurrent = 1
	var mu sync.Mutex
	var events []string
	failedOnce := make(chan struct{})
	var once sync.Once
	om := &orderManifest{fakeManifest: &fakeManifest{hot: hot.mockBackend, entries: map[string]int64{gateDailyA: 7, gateDailyB: 7, gateDailyC: 7}}, log: &events, mu: &mu, fail: errors.New("quorum lost")}
	m.manifest = om
	// Released once stop is set, not when the manifest call returns.
	prev := onReleaseStopped
	onReleaseStopped = func() { once.Do(func() { close(failedOnce) }) }
	t.Cleanup(func() { onReleaseStopped = prev })
	cold.onWrite = func(p string) {
		if p == gateDailyB {
			select {
			case <-failedOnce:
			case <-time.After(5 * time.Second):
				t.Error("the first release never ran")
			}
		}
	}

	_, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB), candidateFor(gateDailyC)})
	if failed != 1 {
		t.Fatalf("failed = %d, want 1 (the manifest failure)", failed)
	}
	if coldHas(t, cold, gateDailyC) {
		t.Fatal("a copy started after the manifest failure")
	}
	if got := fileMeta(t, m, gateDailyB).Tier; got != TierCold {
		t.Fatalf("in-flight file tier = %s, want cold", got)
	}
	if ok, _ := hot.Exists(context.Background(), gateDailyA); !ok {
		t.Fatal("a hot copy was deleted after its manifest delete failed")
	}
}

// A replica that records a pulled file's move to cold drops its read caches:
// a cold listing cached before the copy would hide the file.
func TestRecordUnlinkedFile_MigratedDropsTheReadCaches(t *testing.T) {
	cold := newMockBackend("s3")
	const path = "db1/cpu/2026/10/03/a_daily.parquet"
	cold.seedRaw(path, []byte("cold copy"))
	m := newTierEventManager(t, cold, true)
	var mu sync.Mutex
	calls := 0
	m.SetOnReadCachesStale(func() { mu.Lock(); calls++; mu.Unlock() })

	m.RecordReplicatedFile(path, 9)
	waitTierEvents(t, m, 1)
	mu.Lock()
	before := calls
	mu.Unlock()
	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	// The processed count moves per event, before the batch's end-of-batch
	// invalidation: poll for it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := calls - before
		mu.Unlock()
		if got >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a move recorded on this node did not drop its read caches")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// takeoverManifest fails every manifest delete and reports the files a
// compaction owns.
type takeoverManifest struct {
	*fakeManifest
	mu   sync.Mutex
	owns map[string]bool
}

func (t *takeoverManifest) DeleteFilesFromManifest(context.Context, []string, string) error {
	return errors.New("quorum lost")
}

func (t *takeoverManifest) CompactionOwns(path string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.owns[path]
}

// After a manifest failure the flipped files keep their hot copies, but one a
// compaction took over since its flip still loses its cold copy: the
// compaction's output holds its rows.
func TestStreamRelease_KeptFilesACompactionTookOverLoseTheirColdCopy(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA, gateDailyB)
	ctx := context.Background()
	for _, p := range []string{gateDailyA, gateDailyB} {
		mustWrite(t, cold.mockBackend, p)
		if err := m.metadata.UpdateTier(ctx, p, TierCold); err != nil {
			t.Fatal(err)
		}
	}
	tm := &takeoverManifest{fakeManifest: &fakeManifest{hot: hot.mockBackend, entries: map[string]int64{gateDailyA: 7, gateDailyB: 7}}, owns: map[string]bool{}}
	m.manifest = tm
	stopped := make(chan struct{})
	prev := onReleaseStopped
	onReleaseStopped = func() { close(stopped) }
	t.Cleanup(func() { onReleaseStopped = prev })

	ch := make(chan MigrationCandidate, 2)
	var stop atomic.Bool
	errc := make(chan error, 1)
	go func() { errc <- m.migrator.streamRelease(ctx, ch, &stop) }()
	ch <- candidateFor(gateDailyA)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the first release never failed")
	}
	tm.mu.Lock()
	tm.owns[gateDailyB] = true
	tm.mu.Unlock()
	ch <- candidateFor(gateDailyB)
	close(ch)
	if err := <-errc; err == nil {
		t.Fatal("streamRelease reported no failure")
	}
	if coldHas(t, cold, gateDailyB) {
		t.Fatal("a kept file a compaction took over kept its cold copy")
	}
	if got := fileMeta(t, m, gateDailyB).Tier; got != TierHot {
		t.Fatalf("taken-over file tier = %s, want hot", got)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Fatal("a hot copy was deleted after its manifest delete failed")
	}
}

// A manifest failure stops the cycle, not just its batch.
func TestMigrateTier_ManifestFailureStopsTheCycle(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA, gateDailyB, gateDailyC)
	m.migrator.batchSize = 1
	m.migrator.maxConcurrent = 1
	var mu sync.Mutex
	var events []string
	m.manifest = &orderManifest{fakeManifest: &fakeManifest{hot: hot.mockBackend, entries: map[string]int64{gateDailyA: 7, gateDailyB: 7, gateDailyC: 7}}, log: &events, mu: &mu, fail: errors.New("quorum lost")}

	m.migrator.MigrateTier(context.Background(), TierHot, TierCold)
	copied := 0
	for _, p := range []string{gateDailyA, gateDailyB, gateDailyC} {
		if coldHas(t, cold, p) {
			copied++
		}
	}
	if copied != 1 {
		t.Fatalf("copied %d files, want 1: the batches after the manifest failure ran", copied)
	}
}

// Standalone, a hot copy gone during the copy means a DELETE (or retention)
// removed its rows: the cold copy goes, nothing is flipped.
func TestCopyAndFlip_StandaloneDropsTheCopyOfAVanishedSource(t *testing.T) {
	m, hot, cold, _ := moveRig(t, gateDailyA, gateDailyB)
	m.clusterGate = nil
	cold.onWrite = func(p string) {
		if p == gateDailyA {
			_ = hot.mockBackend.Delete(context.Background(), p)
		}
	}

	migrated, failed := m.migrator.MigrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if migrated != 1 || failed != 1 {
		t.Fatalf("MigrateBatch = (%d, %d), want (1, 1)", migrated, failed)
	}
	if coldHas(t, cold, gateDailyA) {
		t.Fatal("the cold copy of a vanished source was kept")
	}
	if got := fileMeta(t, m, gateDailyB).Tier; got != TierCold {
		t.Fatalf("the other file's tier = %s, want cold", got)
	}
}

// A DELETE of data too young to migrate asks cold storage nothing.
func TestClaimForDelete_RecentFilesSkipTheColdCheck(t *testing.T) {
	m, _, cold, _ := moveRig(t)
	day := time.Now().UTC()
	recent := "db1/cpu/" + day.Format("2006/01/02") + "/cpu_" + day.Format("20060102") + "_daily.parquet"
	mustWrite(t, cold.mockBackend, recent)
	cold.statErr = errors.New("cold unreachable")
	release, err := m.ClaimForDelete(context.Background(), recent)
	if err != nil {
		t.Fatalf("ClaimForDelete on a recent file = %v, want nil", err)
	}
	release()
}
