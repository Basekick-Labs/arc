package compaction

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// clusterManifestFake is a ManifestBridge that keeps what the cluster
// manifest would: the set of registered paths. failDeletes makes a batch
// that carries deletes fail, as a lost quorum would.
type clusterManifestFake struct {
	mu          sync.Mutex
	files       map[string]bool
	failDeletes error
	reasons     []string
	// onDelete, when set, runs inside a batch that carries deletes, before
	// it is applied or refused: what the caller had done by then.
	onDelete func()
}

func (b *clusterManifestFake) RegisterCompactedFile(_ context.Context, f CompactedFile) error {
	return b.BatchFileOps(context.Background(), []CompactedFile{f}, nil)
}

func (b *clusterManifestFake) DeleteCompactedSource(_ context.Context, path, reason string) error {
	return b.BatchFileOps(context.Background(), nil, []DeleteSourceOp{{Path: path, Reason: reason}})
}

func (b *clusterManifestFake) BatchFileOps(_ context.Context, registers []CompactedFile, deletes []DeleteSourceOp) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(deletes) > 0 && b.onDelete != nil {
		b.onDelete()
	}
	if len(deletes) > 0 && b.failDeletes != nil {
		return b.failDeletes
	}
	if b.files == nil {
		b.files = map[string]bool{}
	}
	for _, r := range registers {
		b.files[r.Path] = true
	}
	for _, d := range deletes {
		delete(b.files, d.Path)
		b.reasons = append(b.reasons, d.Reason)
	}
	return nil
}

func (b *clusterManifestFake) registered() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for p := range b.files {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

const (
	retractJob    = "db_cpu_2026_01_02_03_retract"
	retractOutput = "db/cpu/2026/01/02/03/compacted.parquet"
	retractInputA = "db/cpu/2026/01/02/03/a.parquet"
	retractInputB = "db/cpu/2026/01/02/03/b.parquet"
	retractBody   = "full output" // 11 bytes
)

// retractRig is a cluster-mode manager (a completion directory), the real
// completion watcher over a fake cluster manifest, and local storage.
type retractRig struct {
	t       *testing.T
	backend storage.Backend
	bridge  *clusterManifestFake
	watcher *CompletionWatcher
	manager *Manager
	dir     string
	paused  bool
}

func newRetractRig(t *testing.T) *retractRig {
	t.Helper()
	r := &retractRig{t: t, backend: localBackend(t), bridge: &clusterManifestFake{}, dir: t.TempDir()}
	w, err := NewCompletionWatcher(CompletionWatcherConfig{
		Dir:          r.dir,
		Bridge:       r.bridge,
		PollInterval: time.Hour, // polled by hand
		ApplyTimeout: time.Second,
		PauseGate:    func() bool { return r.paused },
		Storage:      r.backend,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.watcher = w
	r.manager = NewManager(&ManagerConfig{
		StorageBackend: r.backend,
		LockManager:    NewLockManager(),
		TempDirectory:  t.TempDir(),
		CycleTimeout:   time.Minute,
		CompletionDir:  r.dir,
		Logger:         zerolog.Nop(),
	})
	r.manager.SetOutputRetractor(w.RetractOutput)
	return r
}

func (r *retractRig) write(path, body string) {
	r.t.Helper()
	if err := r.backend.Write(context.Background(), path, []byte(body)); err != nil {
		r.t.Fatal(err)
	}
}

// job puts a finished upload in place: output, inputs, the crash-recovery
// manifest and the output_written completion record, all agreeing on size.
func (r *retractRig) job() string {
	r.t.Helper()
	r.write(retractOutput, retractBody)
	r.write(retractInputA, "source a")
	r.write(retractInputB, "source b")
	manifestPath, err := r.manager.ManifestManager.WriteManifest(context.Background(), &Manifest{
		OutputPath:    retractOutput,
		OutputSize:    int64(len(retractBody)),
		InputFiles:    []string{retractInputA, retractInputB},
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: "db/cpu/2026/01/02/03",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         retractJob,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.record(CompletionStateOutputWritten)
	return manifestPath
}

func (r *retractRig) record(state CompletionState) {
	r.t.Helper()
	if err := writeCompletionManifest(r.dir, &CompletionManifest{
		JobID:       retractJob,
		Database:    "db",
		Measurement: "cpu",
		Tier:        "hourly",
		State:       state,
		Outputs: []CompactedOutput{{
			Path: retractOutput, SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
			SizeBytes: int64(len(retractBody)), Database: "db", Measurement: "cpu", Tier: "hourly",
			PartitionTime: time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC), CreatedAt: time.Now().UTC(),
		}},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}); err != nil {
		r.t.Fatal(err)
	}
}

func (r *retractRig) hasRecord() bool {
	_, err := os.Stat(filepath.Join(r.dir, retractJob+".json"))
	return err == nil
}

func (r *retractRig) recover() (int, error) {
	return r.manager.recoverOrphanedManifests(context.Background(), recoveryScope{})
}

// assertNoDanglingEntry is the property #1811 is about: the cluster manifest
// never lists a file that is not in storage.
func (r *retractRig) assertNoDanglingEntry() {
	r.t.Helper()
	for _, p := range r.bridge.registered() {
		ok, err := r.backend.Exists(context.Background(), p)
		if err != nil {
			r.t.Fatal(err)
		}
		if !ok {
			r.t.Errorf("the cluster manifest lists %s, which is not in storage", p)
		}
	}
}

// The output is registered, then its stored size no longer matches. Recovery
// discards it so the job is redone, and the cluster manifest must not keep
// its entry.
func TestRecoveryUnregistersAMismatchedOutputBeforeDeletingIt(t *testing.T) {
	r := newRetractRig(t)
	manifestPath := r.job()
	r.watcher.poll(context.Background())
	if got := r.bridge.registered(); len(got) != 1 || got[0] != retractOutput {
		t.Fatalf("precondition: registered = %v, want the output", got)
	}

	r.write(retractOutput, "short")
	recovered, err := r.recover()
	if err != nil || recovered != 1 {
		t.Fatalf("recovery = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, r.backend, false, retractOutput, manifestPath)
	mustExist(t, r.backend, true, retractInputA, retractInputB)
	if got := r.bridge.registered(); len(got) != 0 {
		t.Errorf("registered = %v, want nothing", got)
	}
	if r.hasRecord() {
		t.Error("the completion record is still there: a restarted watcher would register the output again")
	}

	// A watcher that starts now (its memory of what it registered is gone)
	// has nothing to re-register.
	fresh, err := NewCompletionWatcher(CompletionWatcherConfig{Dir: r.dir, Bridge: r.bridge, PollInterval: time.Hour, ApplyTimeout: time.Second, Storage: r.backend, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	fresh.poll(context.Background())
	r.assertNoDanglingEntry()
}

// The cluster manifest cannot be changed (no quorum): nothing is deleted
// from storage, and the recovery manifest stays for the next cycle.
func TestRecoveryDeletesNothingWhenTheOutputCannotBeUnregistered(t *testing.T) {
	r := newRetractRig(t)
	manifestPath := r.job()
	r.watcher.poll(context.Background())
	r.write(retractOutput, "short")
	r.bridge.failDeletes = errors.New("no quorum")
	// The record that would re-register the output goes BEFORE the cluster
	// manifest is asked: observed from inside the request.
	recordAtDelete := true
	r.bridge.onDelete = func() { recordAtDelete = r.hasRecord() }

	recovered, err := r.recover()
	if err == nil || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, error)", recovered, err)
	}
	mustExist(t, r.backend, true, retractOutput, retractInputA, retractInputB, manifestPath)
	r.assertNoDanglingEntry()
	if recordAtDelete {
		t.Error("the completion record was still there when the cluster manifest was asked to drop the output")
	}
	if r.hasRecord() {
		t.Error("the completion record is back after a failed un-register")
	}
	r.bridge.onDelete = nil

	// The cluster answers again: the same manifest is finished off.
	r.bridge.failDeletes = nil
	recovered, err = r.recover()
	if err != nil || recovered != 1 {
		t.Fatalf("recovery after the cluster recovered = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, r.backend, false, retractOutput, manifestPath)
	if got := r.bridge.registered(); len(got) != 0 {
		t.Errorf("registered = %v, want nothing", got)
	}
}

// Another node's watcher registered the output (shared storage, compactor
// failover): this node has no completion record for the job, and must still
// un-register before it deletes.
func TestRecoveryUnregistersAnOutputAnotherNodeRegistered(t *testing.T) {
	r := newRetractRig(t)
	manifestPath := r.job()
	if err := os.Remove(filepath.Join(r.dir, retractJob+".json")); err != nil {
		t.Fatal(err)
	}
	r.bridge.files = map[string]bool{retractOutput: true}
	r.write(retractOutput, "short")

	if recovered, err := r.recover(); err != nil || recovered != 1 {
		t.Fatalf("recovery = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, r.backend, false, retractOutput, manifestPath)
	if got := r.bridge.registered(); len(got) != 0 {
		t.Errorf("registered = %v, want nothing", got)
	}
}

// The output was registered and then went missing. Recovery gives the job
// up; the entry and the record that would re-register it go too.
func TestRecoveryUnregistersAnOutputThatWentMissing(t *testing.T) {
	r := newRetractRig(t)
	manifestPath := r.job()
	r.watcher.poll(context.Background())
	if err := r.backend.Delete(context.Background(), retractOutput); err != nil {
		t.Fatal(err)
	}

	if recovered, err := r.recover(); err != nil || recovered != 1 {
		t.Fatalf("recovery = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, r.backend, false, manifestPath)
	mustExist(t, r.backend, true, retractInputA, retractInputB)
	if got := r.bridge.registered(); len(got) != 0 || r.hasRecord() {
		t.Errorf("registered = %v, record present = %v; want neither", got, r.hasRecord())
	}
}

// A node with the completion directory set but no watcher (it fell back to
// standalone) has no cluster manifest to keep in step: a partial upload is
// discarded as it always was.
func TestRecoveryStillDiscardsWithoutAWatcher(t *testing.T) {
	r := newRetractRig(t)
	r.manager.SetOutputRetractor(nil)
	manifestPath := r.job()
	r.write(retractOutput, "short")

	if recovered, err := r.recover(); err != nil || recovered != 1 {
		t.Fatalf("recovery = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, r.backend, false, retractOutput, manifestPath)
	mustExist(t, r.backend, true, retractInputA, retractInputB)
}

// An output_written record whose output is not in storage as recorded is
// not registered; once the file is there, it is.
func TestWatcherDoesNotRegisterAnOutputThatIsNotInStorage(t *testing.T) {
	for _, c := range []struct{ name, body string }{
		{"absent", ""},
		{"another size", "short"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRetractRig(t)
			r.record(CompletionStateOutputWritten)
			if c.body != "" {
				r.write(retractOutput, c.body)
			}
			r.watcher.poll(context.Background())
			r.watcher.poll(context.Background())
			if got := r.bridge.registered(); len(got) != 0 {
				t.Fatalf("registered = %v, want nothing", got)
			}
			if !r.hasRecord() {
				t.Fatal("the record was removed")
			}

			r.write(retractOutput, retractBody)
			// Looked at again once the back-off has elapsed.
			for i := 0; i < unregisterableRecheckPolls; i++ {
				r.watcher.poll(context.Background())
			}
			if got := r.bridge.registered(); len(got) != 1 || got[0] != retractOutput {
				t.Errorf("registered = %v, want the output once it is in storage", got)
			}
		})
	}
}

func TestRetractOutputRefusals(t *testing.T) {
	t.Run("under the cluster-wide pause", func(t *testing.T) {
		r := newRetractRig(t)
		r.job()
		r.watcher.poll(context.Background())
		r.paused = true
		if err := r.watcher.RetractOutput(context.Background(), retractJob, retractOutput); err == nil {
			t.Fatal("RetractOutput succeeded under a pause")
		}
		if got := r.bridge.registered(); len(got) != 1 || !r.hasRecord() {
			t.Errorf("registered = %v, record present = %v; want both untouched", got, r.hasRecord())
		}
	})
	t.Run("a record at sources_deleted", func(t *testing.T) {
		r := newRetractRig(t)
		r.job()
		r.bridge.files = map[string]bool{retractOutput: true}
		r.record(CompletionStateSourcesDeleted)
		if err := r.watcher.RetractOutput(context.Background(), retractJob, retractOutput); err == nil {
			t.Fatal("RetractOutput succeeded for a job whose sources are recorded as deleted")
		}
		if got := r.bridge.registered(); len(got) != 1 || !r.hasRecord() {
			t.Errorf("registered = %v, record present = %v; want both untouched", got, r.hasRecord())
		}
	})
	t.Run("the reason does not read as a compaction that consumed inputs", func(t *testing.T) {
		r := newRetractRig(t)
		r.job()
		r.watcher.poll(context.Background())
		if err := r.watcher.RetractOutput(context.Background(), retractJob, retractOutput); err != nil {
			t.Fatal(err)
		}
		if len(r.bridge.reasons) != 1 || r.bridge.reasons[0] != "compaction-recovery:"+retractJob {
			t.Errorf("delete reasons = %v", r.bridge.reasons)
		}
	})
}

// The watcher polls while recovery un-registers: no register may land after
// the un-register it would undo. Run with -race.
func TestRetractOutputIsSerialisedWithTheWatcher(t *testing.T) {
	for i := 0; i < 50; i++ {
		r := newRetractRig(t)
		r.job()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); r.watcher.poll(context.Background()) }()
		go func() {
			defer wg.Done()
			if err := r.watcher.RetractOutput(context.Background(), retractJob, retractOutput); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
		r.watcher.poll(context.Background())
		if got := r.bridge.registered(); len(got) != 0 {
			t.Fatalf("iteration %d: registered = %v after the un-register", i, got)
		}
	}
}

// The output went missing on THIS node and the inputs are gone too: the job
// finished, and on per-node storage other nodes may hold good copies of the
// output. Un-registering would make them drop it. The recovery manifest goes,
// as it always did, and the cluster manifest is left alone.
func TestRecoveryDoesNotUnregisterAMissingOutputWhoseInputsAreGone(t *testing.T) {
	r := newRetractRig(t)
	manifestPath := r.job()
	r.watcher.poll(context.Background())
	for _, p := range []string{retractOutput, retractInputA, retractInputB} {
		if err := r.backend.Delete(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}

	if recovered, err := r.recover(); err != nil || recovered != 1 {
		t.Fatalf("recovery = (%d, %v), want (1, nil)", recovered, err)
	}
	mustExist(t, r.backend, false, manifestPath)
	if got := r.bridge.registered(); len(got) != 1 || got[0] != retractOutput {
		t.Errorf("registered = %v, want the output still listed", got)
	}
}

// An input whose key can never be addressed cannot be confirmed stored, now
// or on any later cycle: the missing-output branch must still finish, as it
// did before it had anything to check, without un-registering.
func TestRecoveryOfAMissingOutputFinishesWithAnUnaddressableInput(t *testing.T) {
	r := newRetractRig(t)
	const badInput = "db/cpu/2026/01/02/03/../../escape.parquet"
	if ok, err := r.backend.Exists(context.Background(), badInput); !errors.Is(err, storage.ErrInvalidPath) {
		t.Fatalf("precondition: Exists(%q) = %v, %v; want ErrInvalidPath", badInput, ok, err)
	}
	r.write(retractInputA, "source a")
	manifestPath, err := r.manager.ManifestManager.WriteManifest(context.Background(), &Manifest{
		OutputPath: retractOutput, OutputSize: int64(len(retractBody)),
		InputFiles: []string{retractInputA, badInput},
		Database:   "db", Measurement: "cpu", PartitionPath: "db/cpu/2026/01/02/03", Tier: "hourly",
		Status: ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: retractJob,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.bridge.files = map[string]bool{retractOutput: true}

	for pass := 1; pass <= 2; pass++ {
		if _, err := r.recover(); err != nil {
			t.Fatalf("pass %d: recovery: %v", pass, err)
		}
	}
	mustExist(t, r.backend, false, manifestPath)
	if got := r.bridge.registered(); len(got) != 1 {
		t.Errorf("registered = %v, want the output left listed", got)
	}
}

// The job is still running on another node: its output appears while this
// node, having found it missing, un-registers it. That is not an abandoned
// job, so the recovery manifest is kept for a later pass.
func TestRecoveryKeepsTheManifestWhenTheOutputAppears(t *testing.T) {
	r := newRetractRig(t)
	manifestPath := r.job()
	if err := r.backend.Delete(context.Background(), retractOutput); err != nil {
		t.Fatal(err)
	}
	r.bridge.onDelete = func() { r.write(retractOutput, retractBody) }

	recovered, err := r.recover()
	if !errors.Is(err, errOutputAppeared) || recovered != 0 {
		t.Fatalf("recovery = (%d, %v), want (0, errOutputAppeared)", recovered, err)
	}
	mustExist(t, r.backend, true, retractOutput, retractInputA, retractInputB, manifestPath)
}

// Once the sources are deleted the output is all there is: a sources_deleted
// record is applied in full, registers and deletes, whatever storage says
// about the output. The check before registering is for output_written only.
func TestWatcherAppliesSourcesDeletedWhateverStorageHolds(t *testing.T) {
	r := newRetractRig(t)
	r.bridge.files = map[string]bool{retractInputA: true, retractInputB: true}
	// The output is absent from storage.
	if err := writeCompletionManifest(r.dir, &CompletionManifest{
		JobID: retractJob, Database: "db", Measurement: "cpu", Tier: "hourly",
		State: CompletionStateSourcesDeleted,
		Outputs: []CompactedOutput{{
			Path: retractOutput, SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
			SizeBytes: int64(len(retractBody)), Database: "db", Measurement: "cpu", Tier: "hourly",
			PartitionTime: time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC), CreatedAt: time.Now().UTC(),
		}},
		DeletedSources: []string{retractInputA, retractInputB},
		CreatedAt:      time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	r.watcher.poll(context.Background())
	if got := r.bridge.registered(); len(got) != 1 || got[0] != retractOutput {
		t.Errorf("registered = %v, want the output and no sources", got)
	}
	if r.hasRecord() {
		t.Error("the sources_deleted record was not removed after being applied")
	}
}

// A record found wanting is not checked against storage on every poll.
func TestWatcherBacksOffOnAnOutputThatIsNotInStorage(t *testing.T) {
	r := newRetractRig(t)
	r.record(CompletionStateOutputWritten) // output absent
	stats := &countingStatBackend{Backend: r.backend}
	r.watcher.cfg.Storage = stats
	for i := 0; i < 10; i++ {
		r.watcher.poll(context.Background())
	}
	if n := stats.calls(); n != 1 {
		t.Errorf("storage was asked %d times over 10 polls, want 1", n)
	}
	for i := 0; i < unregisterableRecheckPolls; i++ {
		r.watcher.poll(context.Background())
	}
	if n := stats.calls(); n != 2 {
		t.Errorf("storage was asked %d times after the back-off elapsed, want 2", n)
	}
}

type countingStatBackend struct {
	storage.Backend
	mu sync.Mutex
	n  int
}

func (b *countingStatBackend) StatFile(ctx context.Context, path string) (int64, error) {
	b.mu.Lock()
	b.n++
	b.mu.Unlock()
	return b.Backend.StatFile(ctx, path)
}

func (b *countingStatBackend) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.n
}
