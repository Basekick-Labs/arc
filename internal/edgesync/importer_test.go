package edgesync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type importRig struct {
	importer *Importer
	index    *BundleIndex
	registry *Registry
	hubStore storage.Backend
	secret   string

	// batches records the size of each manifest flush, so a test can prove
	// proposals are batched rather than one-per-file.
	batches []int
}

func newImportRig(t *testing.T, clustered bool) *importRig {
	t.Helper()

	dir := t.TempDir()
	hubStore, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { hubStore.Close() })

	db, err := sql.Open("sqlite3", filepath.Join(dir, "hub.db"))
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	hubIndex, err := NewHubIndex(db, zerolog.Nop())
	if err != nil {
		t.Fatalf("hub index: %v", err)
	}
	bundleIndex, err := NewBundleIndex(db, zerolog.Nop())
	if err != nil {
		t.Fatalf("bundle index: %v", err)
	}
	registry, err := NewRegistry(db, newTestCipher(t), zerolog.Nop())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	secret, err := registry.Register(context.Background(), testSpokeID, "test spoke")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	rig := &importRig{index: bundleIndex, registry: registry, hubStore: hubStore, secret: secret}

	collector := NewCollectingRegistrar()
	receiver, err := NewReceiver(ReceiverConfig{
		Backend:      hubStore,
		Index:        hubIndex,
		Logger:       zerolog.Nop(),
		RegisterFile: collector.Register,
	})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}

	var flush func(context.Context, []*ReceivedFile) error
	if clustered {
		flush = func(_ context.Context, files []*ReceivedFile) error {
			rig.batches = append(rig.batches, len(files))
			return nil
		}
	}

	imp, err := NewImporter(ImporterConfig{
		Receiver: receiver, Collector: collector, Index: bundleIndex,
		Registry: registry, HubID: testHubID, FlushManifest: flush, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("importer: %v", err)
	}
	rig.importer = imp
	return rig
}

// exportBundle builds a real bundle from a spoke, for the hub to import.
func exportBundle(t *testing.T, secret string, n int, hubID string) string {
	t.Helper()
	ctx := context.Background()

	storeDir := t.TempDir()
	backend, err := storage.NewLocalBackend(storeDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("spoke backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	w, err := NewBundleWriter(BundleWriterConfig{
		Backend: backend, SpokeID: testSpokeID, HubID: hubID,
		Secret: secret, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}

	entries := make([]*LedgerEntry, 0, n)
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("default/cpu/2026/08/07/%02d/f_%04d.parquet", i%24, i)
		content := fmt.Sprintf("payload %04d", i)
		if err := backend.Write(ctx, p, []byte(content)); err != nil {
			t.Fatalf("write: %v", err)
		}
		sum, _ := sha256Of(content)
		entries = append(entries, &LedgerEntry{
			HubID: hubID, Path: p, SHA256: sum, SizeBytes: int64(len(content)),
			Database: "default", Measurement: "cpu",
			PartitionTime: time.Date(2026, 8, 7, i%24, 0, 0, 0, time.UTC),
		})
	}

	res, err := w.Export(ctx, t.TempDir(), entries, time.Now())
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	return res.Dir
}

func TestImporter_RoundTripsAVerifiedBundle(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 3, testHubID)

	res, err := rig.importer.Import(ctx, dir)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.Committed != 3 {
		t.Errorf("committed = %d, want 3", res.Committed)
	}
	if len(res.Conflicts) != 0 {
		t.Errorf("conflicts = %v, want none", res.Conflicts)
	}

	// The files must be in hub storage, namespaced under the spoke.
	for i := 0; i < 3; i++ {
		p := NamespacedPath(testSpokeID, fmt.Sprintf("default/cpu/2026/08/07/%02d/f_%04d.parquet", i, i))
		if ok, err := rig.hubStore.Exists(ctx, p); err != nil || !ok {
			t.Errorf("%s is not in hub storage (err=%v)", p, err)
		}
	}
}

// Replay protection for an artifact that sits on a drive for weeks: durable
// state, not a timestamp window.
func TestImporter_RefusesAReimport(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 2, testHubID)

	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatalf("first import: %v", err)
	}
	_, err := rig.importer.Import(ctx, dir)
	if !errors.Is(err, ErrBundleAlreadyImported) {
		t.Fatalf("second import error = %v, want ErrBundleAlreadyImported", err)
	}
	// The message must say WHEN, so a duplicate drive is diagnosable.
	if !strings.Contains(err.Error(), "imported at") {
		t.Errorf("the refusal does not say when it was imported: %v", err)
	}
}

// A bundle for another hub validates fine under the same spoke's secret, so
// the MAC alone does not stop a scavenged drive importing anywhere.
func TestImporter_RefusesABundleForAnotherHub(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 2, "some-other-hub")

	_, err := rig.importer.Import(ctx, dir)
	if !errors.Is(err, ErrBundleInvalid) {
		t.Fatalf("error = %v, want ErrBundleInvalid", err)
	}
	if !strings.Contains(err.Error(), "addressed to hub") {
		t.Errorf("the refusal does not explain the mismatch: %v", err)
	}
}

// A tampered bundle must not commit a single byte.
func TestImporter_TamperedBundleCommitsNothing(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 3, testHubID)

	// Corrupt one file AFTER export.
	victim := filepath.Join(dir, dataDir, "default/cpu/2026/08/07/01/f_0001.parquet")
	if err := os.WriteFile(victim, []byte("tampered!!!"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := rig.importer.Import(ctx, dir); !errors.Is(err, ErrBundleInvalid) {
		t.Fatalf("error = %v, want ErrBundleInvalid", err)
	}

	// NOTHING committed — verification runs before any commit.
	for i := 0; i < 3; i++ {
		p := NamespacedPath(testSpokeID, fmt.Sprintf("default/cpu/2026/08/07/%02d/f_%04d.parquet", i, i))
		if ok, _ := rig.hubStore.Exists(ctx, p); ok {
			t.Errorf("%s was committed from a tampered bundle", p)
		}
	}
	// And no dedup row, so a corrected drive can still be imported.
	if seen, _ := rig.index.Seen(ctx, testSpokeID, "any"); seen != nil {
		t.Error("a refused bundle was recorded")
	}
}

// An unregistered or disabled spoke must not be able to write to the hub.
func TestImporter_RefusesUnknownAndDisabledSpokes(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 1, testHubID)

	if err := rig.registry.SetEnabled(ctx, testSpokeID, false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	_, err := rig.importer.Import(ctx, dir)
	if !errors.Is(err, ErrBundleInvalid) || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("a disabled spoke's bundle was not refused for being disabled: %v", err)
	}

	if err := rig.registry.Delete(ctx, testSpokeID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = rig.importer.Import(ctx, dir)
	if !errors.Is(err, ErrBundleInvalid) || !strings.Contains(err.Error(), "unknown spoke") {
		t.Errorf("an unregistered spoke's bundle was not refused: %v", err)
	}
}

// The Cluster Operations Checklist caps Raft batches at 1000. An import is a
// tight loop, unlike the online path where HTTP rate-limits one proposal per
// request — so a 2500-file bundle must be three proposals, not 2500.
func TestImporter_BatchesManifestProposals(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, true)

	const files = ImportBatchSize*2 + 500
	dir := exportBundle(t, rig.secret, files, testHubID)

	res, err := rig.importer.Import(ctx, dir)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.Committed != files {
		t.Fatalf("committed = %d, want %d", res.Committed, files)
	}

	if len(rig.batches) != 3 {
		t.Errorf("manifest flushes = %d (%v), want 3 for %d files", len(rig.batches), rig.batches, files)
	}
	for i, n := range rig.batches {
		if n > ImportBatchSize {
			t.Errorf("batch %d carried %d ops, over the %d cap", i, n, ImportBatchSize)
		}
	}
	total := 0
	for _, n := range rig.batches {
		total += n
	}
	if total != files {
		t.Errorf("batched %d ops for %d files: some were never registered", total, files)
	}
}

// A standalone hub has no manifest, so import must work with no flush hook.
func TestImporter_WorksWithoutAManifest(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 2, testHubID)

	if _, err := rig.importer.Import(ctx, dir); err != nil {
		t.Fatalf("standalone import failed: %v", err)
	}
}

// Re-importing a DIFFERENT bundle carrying files the hub already holds is the
// lost-drive-then-resent case: no bytes rewritten, no conflict raised.
func TestImporter_AlreadyPresentFilesAreNotConflicts(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)

	first := exportBundle(t, rig.secret, 2, testHubID)
	if _, err := rig.importer.Import(ctx, first); err != nil {
		t.Fatalf("first import: %v", err)
	}

	// A second bundle with identical content at identical paths.
	second := exportBundle(t, rig.secret, 2, testHubID)
	res, err := rig.importer.Import(ctx, second)
	if err != nil {
		t.Fatalf("second import: %v", err)
	}
	if res.AlreadyPresent != 2 {
		t.Errorf("already_present = %d, want 2", res.AlreadyPresent)
	}
	if res.Committed != 0 {
		t.Errorf("committed = %d, want 0: identical content must not be rewritten", res.Committed)
	}
	if len(res.Conflicts) != 0 {
		t.Errorf("identical content raised %d conflicts", len(res.Conflicts))
	}
}

// §6.1: the same path holding different content is reported, never overwritten.
func TestImporter_ConflictsAreReportedNotOverwritten(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)

	dir := exportBundle(t, rig.secret, 1, testHubID)
	const rel = "default/cpu/2026/08/07/00/f_0000.parquet"

	// The hub already holds different content at that path.
	if err := rig.hubStore.Write(ctx, NamespacedPath(testSpokeID, rel), []byte("the hub's version")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	res, err := rig.importer.Import(ctx, dir)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if len(res.Conflicts) != 1 {
		t.Fatalf("conflicts = %v, want one", res.Conflicts)
	}
	if res.Conflicts[0].Path != rel {
		t.Errorf("conflict path = %q, want %q", res.Conflicts[0].Path, rel)
	}

	// The hub's copy is untouched.
	var buf strings.Builder
	if err := rig.hubStore.ReadTo(ctx, NamespacedPath(testSpokeID, rel), &buf); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if buf.String() != "the hub's version" {
		t.Errorf("the hub's copy was overwritten: %q", buf.String())
	}
}

// A hostile manifest must not be able to exceed the hub's own limit.
func TestImporter_EnforcesItsOwnFileCap(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, false)
	dir := exportBundle(t, rig.secret, 5, testHubID)

	rig.importer.maxFiles = 2
	_, err := rig.importer.Import(ctx, dir)
	if !errors.Is(err, ErrBundleInvalid) || !strings.Contains(err.Error(), "limit") {
		t.Errorf("error = %v, want a refusal citing the hub's limit", err)
	}
}

// Already-present files also register (resolveExisting re-attempts it), so
// they enter the collector too. If the size check only runs on newly-committed
// files, a re-imported bundle accumulates past the cap and the final flush
// carries one oversized proposal — violating the rule this exists to satisfy.
func TestImporter_BatchCapHoldsForAlreadyPresentFiles(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, true)

	const files = ImportBatchSize + 200
	first := exportBundle(t, rig.secret, files, testHubID)
	if _, err := rig.importer.Import(ctx, first); err != nil {
		t.Fatalf("first import: %v", err)
	}

	// A DIFFERENT bundle carrying the same content: every file resolves to
	// already_present, and every one still registers.
	rig.batches = nil
	second := exportBundle(t, rig.secret, files, testHubID)
	res, err := rig.importer.Import(ctx, second)
	if err != nil {
		t.Fatalf("second import: %v", err)
	}
	if res.AlreadyPresent != files {
		t.Fatalf("already_present = %d, want %d", res.AlreadyPresent, files)
	}

	for i, n := range rig.batches {
		if n > ImportBatchSize {
			t.Errorf("batch %d carried %d ops, over the %d cap", i, n, ImportBatchSize)
		}
	}
	if len(rig.batches) == 0 {
		t.Error("already-present files produced no manifest batches at all")
	}
}

// The endpoint is a concurrent Fiber handler, and one collector and one
// staging area are shared across imports. Without serialization two imports
// collide in staging — both stage the same path, one promotes, the other fails
// "file not found" — and Reset truncates the other's buffered registrations,
// leaving committed files outside the manifest with no error.
// A concurrent import fails FAST with ErrImportInProgress instead of
// queueing silently behind a run that can legally take hours (#614) — and
// the refused bundle imports cleanly afterward, so nothing is lost.
func TestImporter_ConcurrentImportFailsFast(t *testing.T) {
	ctx := context.Background()
	rig := newImportRig(t, true)

	const each = 60
	a := exportBundle(t, rig.secret, each, testHubID)
	b := exportBundle(t, rig.secret, each, testHubID)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, dir := range []string{a, b} {
		wg.Add(1)
		go func(idx int, d string) {
			defer wg.Done()
			_, errs[idx] = rig.importer.Import(ctx, d)
		}(i, dir)
	}
	wg.Wait()

	var okCount, busyCount int
	refused := ""
	for i, err := range errs {
		switch {
		case err == nil:
			okCount++
		case errors.Is(err, ErrImportInProgress):
			busyCount++
			refused = []string{a, b}[i]
		default:
			t.Fatalf("import %d: unexpected error %v", i, err)
		}
	}
	// Timing-dependent: usually one wins and one is refused, but both may
	// succeed if the first releases the lock before the second tries. What
	// must NEVER happen is both refused, or any other error.
	if okCount == 0 {
		t.Fatalf("no import succeeded (ok=%d busy=%d)", okCount, busyCount)
	}

	// The refused drive imports cleanly on retry — refusal loses nothing.
	if busyCount == 1 {
		if _, err := rig.importer.Import(ctx, refused); err != nil {
			t.Fatalf("retry of the refused bundle failed: %v", err)
		}
	}

	total := 0
	for _, n := range rig.batches {
		total += n
	}
	if total != each*2 {
		t.Errorf("registered %d files across both imports, want %d", total, each*2)
	}
}
