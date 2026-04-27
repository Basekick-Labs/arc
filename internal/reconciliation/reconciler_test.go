package reconciliation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// fakeCoordinator is an in-memory Coordinator implementing the minimal
// interface the reconciler needs. The manifest is a path→entry map under
// a mutex so tests can mutate it concurrently with Reconcile if a test
// case needs to simulate a race.
type fakeCoordinator struct {
	mu         sync.Mutex
	manifest   map[string]*raft.FileEntry
	batchErr   error // returned by BatchFileOpsInManifest when set
	batchCalls [][]raft.BatchFileOp
}

func newFakeCoordinator(entries ...*raft.FileEntry) *fakeCoordinator {
	m := make(map[string]*raft.FileEntry, len(entries))
	for _, e := range entries {
		m[e.Path] = e
	}
	return &fakeCoordinator{manifest: m}
}

func (f *fakeCoordinator) GetFileManifest() []*raft.FileEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*raft.FileEntry, 0, len(f.manifest))
	for _, e := range f.manifest {
		ec := *e
		out = append(out, &ec)
	}
	return out
}

func (f *fakeCoordinator) GetFileEntry(path string) (*raft.FileEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.manifest[path]
	if !ok {
		return nil, false
	}
	ec := *e
	return &ec, true
}

func (f *fakeCoordinator) BatchFileOpsInManifest(ops []raft.BatchFileOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.batchErr != nil {
		return f.batchErr
	}
	f.batchCalls = append(f.batchCalls, ops)
	for _, op := range ops {
		switch op.Type {
		case raft.CommandDeleteFile:
			var p raft.DeleteFilePayload
			if err := json.Unmarshal(op.Payload, &p); err != nil {
				return err
			}
			delete(f.manifest, p.Path)
		case raft.CommandRegisterFile:
			var p raft.RegisterFilePayload
			if err := json.Unmarshal(op.Payload, &p); err != nil {
				return err
			}
			pc := p.File
			f.manifest[p.File.Path] = &pc
		}
	}
	return nil
}

// fakeBackend is an in-memory storage.Backend. Only the methods the
// reconciler actually exercises are implemented meaningfully; unused
// ones return zero values.
type fakeBackend struct {
	mu    sync.Mutex
	files map[string]*fakeObject
}

type fakeObject struct {
	data         []byte
	lastModified time.Time
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{files: make(map[string]*fakeObject)}
}

func (b *fakeBackend) put(path string, mtime time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &fakeObject{data: []byte("test"), lastModified: mtime}
}

func (b *fakeBackend) Write(_ context.Context, path string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &fakeObject{data: data, lastModified: time.Now().UTC()}
	return nil
}

func (b *fakeBackend) WriteReader(_ context.Context, path string, r io.Reader, size int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &fakeObject{data: data, lastModified: time.Now().UTC()}
	return nil
}

func (b *fakeBackend) Read(_ context.Context, path string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	obj, ok := b.files[path]
	if !ok {
		return nil, errors.New("not found")
	}
	out := make([]byte, len(obj.data))
	copy(out, obj.data)
	return out, nil
}

func (b *fakeBackend) ReadTo(_ context.Context, path string, w io.Writer) error {
	data, err := b.Read(context.Background(), path)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (b *fakeBackend) ReadToAt(_ context.Context, path string, w io.Writer, offset int64) error {
	data, err := b.Read(context.Background(), path)
	if err != nil {
		return err
	}
	if offset < 0 || offset > int64(len(data)) {
		return errors.New("invalid offset")
	}
	_, err = w.Write(data[offset:])
	return err
}

func (b *fakeBackend) StatFile(_ context.Context, path string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	obj, ok := b.files[path]
	if !ok {
		return -1, nil
	}
	return int64(len(obj.data)), nil
}

func (b *fakeBackend) List(_ context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0)
	for p := range b.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	return out, nil
}

func (b *fakeBackend) Delete(_ context.Context, path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.files, path)
	return nil
}

func (b *fakeBackend) Exists(_ context.Context, path string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.files[path]
	return ok, nil
}

func (b *fakeBackend) Close() error       { return nil }
func (b *fakeBackend) Type() string       { return "fake" }
func (b *fakeBackend) ConfigJSON() string { return "{}" }

// ListObjects implements storage.ObjectLister so tests get a real mtime
// on each object, matching what the local/S3/Azure backends do in
// production.
func (b *fakeBackend) ListObjects(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]storage.ObjectInfo, 0)
	for p, obj := range b.files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, storage.ObjectInfo{
				Path:         p,
				Size:         int64(len(obj.data)),
				LastModified: obj.lastModified,
			})
		}
	}
	return out, nil
}

// DeleteBatch implements storage.BatchDeleter — exercises the
// reconciler's fast-path on production S3/Azure backends.
func (b *fakeBackend) DeleteBatch(_ context.Context, paths []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range paths {
		delete(b.files, p)
	}
	return nil
}

// ListDirectories implements storage.DirectoryLister — used by the
// reconciler's root walk to find files under databases/measurements
// that aren't in the manifest. Returns one segment deeper than prefix.
func (b *fakeBackend) ListDirectories(_ context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	dirs := make(map[string]struct{})
	for p := range b.files {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := strings.TrimPrefix(p, prefix)
		idx := strings.Index(rest, "/")
		if idx <= 0 {
			continue
		}
		dirs[rest[:idx]] = struct{}{}
	}
	out := make([]string, 0, len(dirs))
	for d := range dirs {
		out = append(out, d)
	}
	return out, nil
}

// Compile-time checks.
var _ storage.Backend = (*fakeBackend)(nil)
var _ storage.ObjectLister = (*fakeBackend)(nil)
var _ storage.BatchDeleter = (*fakeBackend)(nil)
var _ storage.DirectoryLister = (*fakeBackend)(nil)

// fakeGate is a static gate for tests.
type fakeGate struct {
	scan  bool
	sweep bool
	role  string
}

func (g *fakeGate) ShouldRunStorageScan() bool   { return g.scan }
func (g *fakeGate) ShouldRunManifestSweep() bool { return g.sweep }
func (g *fakeGate) Role() string                 { return g.role }

// ---- Lifecycle tests ----

func newReconciler(t *testing.T, cfg Config, coord Coordinator, store storage.Backend, gate Gate) *Reconciler {
	t.Helper()
	r, err := NewReconciler(cfg, coord, store, gate, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	return r
}

func TestNewReconciler_RejectsMissingDeps(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		coord   Coordinator
		storage storage.Backend
		wantErr string
	}{
		{
			name:    "missing storage",
			cfg:     Config{Enabled: true, BackendKind: BackendShared},
			coord:   newFakeCoordinator(),
			storage: nil,
			wantErr: "storage backend is required",
		},
		{
			name:    "missing backend kind",
			cfg:     Config{Enabled: true},
			coord:   newFakeCoordinator(),
			storage: newFakeBackend(),
			wantErr: "backend_kind must be set",
		},
		{
			name:    "missing coordinator for shared",
			cfg:     Config{Enabled: true, BackendKind: BackendShared},
			coord:   nil,
			storage: newFakeBackend(),
			wantErr: "coordinator is required",
		},
		{
			name:    "missing local node id for local",
			cfg:     Config{Enabled: true, BackendKind: BackendLocal},
			coord:   newFakeCoordinator(),
			storage: newFakeBackend(),
			wantErr: "local_node_id is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReconciler(tt.cfg, tt.coord, tt.storage, nil, nil, zerolog.Nop())
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestNewReconciler_StandaloneAcceptsNilCoord(t *testing.T) {
	cfg := Config{Enabled: true, BackendKind: BackendStandalone}
	r, err := NewReconciler(cfg, nil, newFakeBackend(), nil, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("standalone with nil coord should be accepted, got: %v", err)
	}
	if r == nil {
		t.Fatal("reconciler is nil")
	}
}

func TestReconcile_Disabled(t *testing.T) {
	r := newReconciler(t,
		Config{Enabled: false, BackendKind: BackendShared},
		newFakeCoordinator(), newFakeBackend(), &fakeGate{scan: true, sweep: true})
	_, err := r.Reconcile(context.Background(), false)
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got: %v", err)
	}
}

func TestReconcile_Gated(t *testing.T) {
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		newFakeCoordinator(), newFakeBackend(),
		&fakeGate{scan: false, sweep: false, role: "reader"})
	_, err := r.Reconcile(context.Background(), false)
	if !errors.Is(err, ErrGated) {
		t.Fatalf("expected ErrGated, got: %v", err)
	}
}

func TestReconcile_GateAllowsOneHalfStillRuns(t *testing.T) {
	// Local-storage mode: every node runs the storage scan but only one
	// is the leader for manifest writes. The gate returns true on at
	// least one half — Reconcile must NOT return ErrGated.
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "node-a"},
		newFakeCoordinator(), newFakeBackend(),
		&fakeGate{scan: true, sweep: false, role: "writer-follower"})
	run, err := r.Reconcile(context.Background(), true)
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if run == nil {
		t.Fatal("expected run, got nil")
	}
	if run.Aborted {
		t.Fatalf("expected non-aborted run, got: %+v", run)
	}
}

func TestReconcile_NoOverlap(t *testing.T) {
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		newFakeCoordinator(), newFakeBackend(),
		&fakeGate{scan: true, sweep: true})

	if !r.runState.CompareAndSwap(false, true) {
		t.Fatal("setup: runState should have been false")
	}
	_, err := r.Reconcile(context.Background(), true)
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("expected ErrAlreadyRunning, got: %v", err)
	}
	r.runState.Store(false)
}

func TestReconcile_ManifestTooLargeAborts(t *testing.T) {
	coord := newFakeCoordinator(
		fileEntry("db/m/2026/04/27/12/a.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/b.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/c.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/d.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/e.parquet", "node-a"),
	)
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared, MaxManifestSize: 3},
		coord, newFakeBackend(), &fakeGate{scan: true, sweep: true})

	run, err := r.Reconcile(context.Background(), true)
	if !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("expected ErrManifestTooLarge, got: %v", err)
	}
	if run == nil {
		t.Fatal("expected partial run, got nil")
	}
	if !run.Aborted {
		t.Fatal("expected run.Aborted=true")
	}
	if run.AbortReason != AbortManifestTooLarge {
		t.Fatalf("expected abort reason %q, got %q", AbortManifestTooLarge, run.AbortReason)
	}

	if r.LastRun() == nil || r.LastRun().ID != run.ID {
		t.Fatalf("expected LastRun to be the aborted run, got: %v", r.LastRun())
	}
}

func TestReconcile_RunHistoryRingBuffer(t *testing.T) {
	coord := newFakeCoordinator()
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		coord, newFakeBackend(), &fakeGate{scan: true, sweep: true})

	for i := 0; i < runHistoryCap+5; i++ {
		_, err := r.Reconcile(context.Background(), true)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	if got := len(r.RecentRuns()); got != runHistoryCap {
		t.Fatalf("expected ring buffer to cap at %d, got %d", runHistoryCap, got)
	}
}

func TestReconcile_FindRun(t *testing.T) {
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		newFakeCoordinator(), newFakeBackend(), &fakeGate{scan: true, sweep: true})
	run, err := r.Reconcile(context.Background(), true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, ok := r.FindRun(run.ID)
	if !ok || got == nil {
		t.Fatal("expected to find the run we just ran")
	}
	if _, ok := r.FindRun("does-not-exist"); ok {
		t.Fatal("expected FindRun to miss for unknown id")
	}
}

func TestReconcile_ContextCancelClassified(t *testing.T) {
	coord := newFakeCoordinator()
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		coord, newFakeBackend(), &fakeGate{scan: true, sweep: true})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling
	_, err := r.Reconcile(ctx, true)
	if err == nil {
		t.Fatal("expected error from canceled ctx")
	}
	last := r.LastRun()
	if last == nil || !last.Aborted {
		t.Fatalf("expected aborted last run, got: %+v", last)
	}
	if last.AbortReason != AbortCtxCanceled {
		t.Fatalf("expected abort reason %q, got %q", AbortCtxCanceled, last.AbortReason)
	}
}

func TestReconcile_DryRunReportsDiff(t *testing.T) {
	// Build a real diff: 2 manifest entries (one missing from storage),
	// plus an old orphan storage file and a young (grace-protected) one.
	now := time.Now().UTC()
	coord := newFakeCoordinator(
		fileEntry("db/m/2026/04/27/12/a.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/missing.parquet", "node-a"), // not in storage
	)
	store := newFakeBackend()
	store.put("db/m/2026/04/27/12/a.parquet", now.Add(-2*time.Hour))
	store.put("db/m/2026/04/27/12/orphan.parquet", now.Add(-48*time.Hour)) // old, eligible
	store.put("db/m/2026/04/27/12/young.parquet", now.Add(-1*time.Hour))   // grace skip

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		coord, store, &fakeGate{scan: true, sweep: true})

	run, err := r.Reconcile(context.Background(), true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.OrphanManifestCount != 1 {
		t.Errorf("expected 1 orphan-manifest, got %d (sample: %v)", run.OrphanManifestCount, run.OrphanManifestSample)
	}
	if run.OrphanStorageCount != 1 {
		t.Errorf("expected 1 orphan-storage, got %d (sample: %v)", run.OrphanStorageCount, run.OrphanStorageSample)
	}
	if run.SkippedGrace != 1 {
		t.Errorf("expected 1 grace skip, got %d", run.SkippedGrace)
	}
	if run.ManifestFileCount != 2 {
		t.Errorf("expected manifest count 2, got %d", run.ManifestFileCount)
	}
	// storage_file_count is the number of .parquet candidates returned
	// from the walk after filtering (3 files put — all parquet).
	if run.StorageFileCount != 3 {
		t.Errorf("expected storage file count 3, got %d", run.StorageFileCount)
	}
	// Dry run: both ManifestDeletes and StorageDeletes count the work
	// we *would* have done so operators can read the run report and
	// know what an act run would delete.
	if run.ManifestDeletes != 1 {
		t.Errorf("dry run should count would-be manifest deletes: got %d", run.ManifestDeletes)
	}
	if run.StorageDeletes != 1 {
		t.Errorf("dry run should count would-be storage deletes: got %d", run.StorageDeletes)
	}
	// And the fake coordinator's manifest is unchanged (no act).
	if len(coord.batchCalls) != 0 {
		t.Errorf("dry run should NOT call BatchFileOpsInManifest, got %d calls", len(coord.batchCalls))
	}
	// And the storage backend is unchanged (no act).
	if exists, _ := store.Exists(context.Background(), "db/m/2026/04/27/12/orphan.parquet"); !exists {
		t.Error("dry run should NOT delete from storage")
	}
}

func TestReconcile_ActModeAppliesManifestDeletes(t *testing.T) {
	now := time.Now().UTC()
	coord := newFakeCoordinator(
		fileEntry("db/m/2026/04/27/12/a.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/missing-1.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/missing-2.parquet", "node-a"),
	)
	store := newFakeBackend()
	store.put("db/m/2026/04/27/12/a.parquet", now.Add(-2*time.Hour))

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		coord, store, &fakeGate{scan: true, sweep: true})

	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.ManifestDeletes != 2 {
		t.Errorf("expected 2 manifest deletes, got %d", run.ManifestDeletes)
	}
	// Coordinator should have one batch call with two ops.
	if len(coord.batchCalls) != 1 || len(coord.batchCalls[0]) != 2 {
		t.Fatalf("expected 1 batch with 2 ops, got: %+v", coord.batchCalls)
	}
	// And the manifest is now consistent with storage.
	if _, ok := coord.manifest["db/m/2026/04/27/12/missing-1.parquet"]; ok {
		t.Error("missing-1.parquet should have been deleted from manifest")
	}
	if _, ok := coord.manifest["db/m/2026/04/27/12/missing-2.parquet"]; ok {
		t.Error("missing-2.parquet should have been deleted from manifest")
	}
}

func TestReconcile_ManifestSweepRecheckCatchesRace(t *testing.T) {
	// Simulate a race: the diff says path P is orphan-manifest, but
	// between snapshot and apply a writer registered the file. The
	// re-check must see storage.Exists(P)=true and skip the delete.
	now := time.Now().UTC()
	coord := newFakeCoordinator(
		fileEntry("db/m/raced.parquet", "node-a"),
	)
	store := newFakeBackend()
	// Storage starts empty (so the diff sees raced.parquet as orphan-manifest).
	// Plant the racing register: put the file in storage AFTER the diff
	// would compute. We do this by pre-putting it now — the fakeBackend
	// snapshot races itself, but the re-check walks Exists which sees
	// the file. To force the race we need to put the file AFTER walkStorage
	// but BEFORE sweepOrphanManifest runs. Easiest: put it before the run
	// but skip-walk by NOT putting it? No — then the diff wouldn't even
	// flag it. We need a different shape:
	//
	// The whole point: diff says orphan, re-check says exists. So put the
	// file in storage with a path the walk DOES NOT enumerate (different
	// prefix), then assert the re-check skips. The walk is per-prefix and
	// `db/m/raced.parquet` is in `db/m/` so we need a different layout.
	//
	// Simpler: register the file under a prefix the walk's derivePrefixes
	// doesn't see, then put it under the same path in storage. The walk
	// only enumerates derived prefixes, so the file is invisible to the
	// walk → flagged as orphan-manifest → re-check via Exists succeeds →
	// skip.
	//
	// Even simpler test: use BackendStandalone — nope, that bypasses
	// the manifest sweep entirely.
	//
	// Cleanest: use the orphan-manifest path's per-file Exists() re-check
	// directly with a fakeBackend that has the file but a manifest that
	// doesn't enumerate the prefix correctly. But our walk does enumerate
	// `db/m/`. So: put the file in storage under a path whose prefix isn't
	// covered by `db/m/` — e.g. `weird/path.parquet`, then register a
	// manifest entry for `weird/path.parquet` with Database="db",
	// Measurement="m". The walk will look under `db/m/` and not find it,
	// so the diff flags it. The re-check via Exists sees the file at
	// `weird/path.parquet` and skips the delete.
	store.put("weird/path.parquet", now.Add(-2*time.Hour))
	coord.manifest["weird/path.parquet"] = &raft.FileEntry{
		Path: "weird/path.parquet", Database: "db", Measurement: "m",
		OriginNodeID: "node-a", CreatedAt: now,
	}
	// Also include raced.parquet in storage so it's NOT actually orphan;
	// the walk will pick it up and the diff won't flag it.
	store.put("db/m/raced.parquet", now.Add(-2*time.Hour))

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		coord, store, &fakeGate{scan: true, sweep: true})

	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// One file (weird/path.parquet) is in the manifest but the walk
	// doesn't enumerate it → diff flags as orphan-manifest. The re-check
	// via Exists sees the file and skips. Net: 0 deletes, 1 SkippedRecheck.
	if run.OrphanManifestCount != 1 {
		t.Errorf("expected 1 orphan-manifest before re-check, got %d", run.OrphanManifestCount)
	}
	if run.SkippedRecheck != 1 {
		t.Errorf("expected 1 skipped-recheck, got %d", run.SkippedRecheck)
	}
	if run.ManifestDeletes != 0 {
		t.Errorf("re-check race: no deletes should be applied, got %d", run.ManifestDeletes)
	}
}

func TestReconcile_ManifestSweepAbortsOnRaftError(t *testing.T) {
	now := time.Now().UTC()
	coord := newFakeCoordinator(
		fileEntry("db/m/missing-1.parquet", "node-a"),
		fileEntry("db/m/missing-2.parquet", "node-a"),
	)
	coord.batchErr = errors.New("raft: quorum lost")
	store := newFakeBackend()
	_ = now

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared},
		coord, store, &fakeGate{scan: true, sweep: true})

	run, err := r.Reconcile(context.Background(), false)
	if err == nil {
		t.Fatal("expected an error from Reconcile")
	}
	if run == nil || !run.Aborted {
		t.Fatalf("expected aborted run, got: %+v", run)
	}
	if run.AbortReason != AbortRaftQuorumLoss {
		t.Errorf("expected abort reason %q, got %q", AbortRaftQuorumLoss, run.AbortReason)
	}
	if run.ManifestDeletes != 0 {
		t.Errorf("on raft failure no deletes should be counted, got %d", run.ManifestDeletes)
	}
}

func TestReconcile_BlastCapStopsManifestSweep(t *testing.T) {
	now := time.Now().UTC()
	// 5 orphan-manifest entries, cap at 3.
	coord := newFakeCoordinator(
		fileEntry("db/m/o1.parquet", "node-a"),
		fileEntry("db/m/o2.parquet", "node-a"),
		fileEntry("db/m/o3.parquet", "node-a"),
		fileEntry("db/m/o4.parquet", "node-a"),
		fileEntry("db/m/o5.parquet", "node-a"),
	)
	store := newFakeBackend()
	_ = now

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared, MaxDeletesPerRun: 3, BatchSize: 10},
		coord, store, &fakeGate{scan: true, sweep: true})

	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !run.CapHit {
		t.Error("expected CapHit=true")
	}
	if run.ManifestDeletes != 3 {
		t.Errorf("expected exactly 3 deletes (cap), got %d", run.ManifestDeletes)
	}
}

func TestReconcile_LocalModeFiltersForeignOriginFiles(t *testing.T) {
	// In local-storage mode, files owned by node-B should not appear in
	// node-A's manifest set after manifestToKeys() — otherwise node-A
	// would see them as orphan-manifest (storage walk on node-A's disk
	// won't find them).
	now := time.Now().UTC()
	coord := newFakeCoordinator(
		fileEntry("db/m/own.parquet", "node-a"),
		fileEntry("db/m/foreign.parquet", "node-b"),
	)
	store := newFakeBackend()
	store.put("db/m/own.parquet", now.Add(-2*time.Hour))
	// foreign.parquet lives on node-b's disk; node-a's storage doesn't see it.

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendLocal, LocalNodeID: "node-a"},
		coord, store, &fakeGate{scan: true, sweep: true})
	run, err := r.Reconcile(context.Background(), true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.OrphanManifestCount != 0 {
		t.Errorf("local mode should not flag foreign-origin files as orphan-manifest, got %d (sample: %v)", run.OrphanManifestCount, run.OrphanManifestSample)
	}
}

func TestReconcile_ActModeDeletesOrphanStorage(t *testing.T) {
	now := time.Now().UTC()
	coord := newFakeCoordinator(
		fileEntry("db/m/2026/04/27/12/a.parquet", "node-a"),
	)
	store := newFakeBackend()
	store.put("db/m/2026/04/27/12/a.parquet", now.Add(-2*time.Hour))
	store.put("db/m/2026/04/27/12/orphan-1.parquet", now.Add(-48*time.Hour))
	store.put("db/m/2026/04/27/12/orphan-2.parquet", now.Add(-48*time.Hour))
	// young.parquet should be skipped by grace.
	store.put("db/m/2026/04/27/12/young.parquet", now.Add(-30*time.Minute))

	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: true},
		coord, store, &fakeGate{scan: true, sweep: true})

	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.StorageDeletes != 2 {
		t.Errorf("expected 2 storage deletes, got %d (sample: %v)", run.StorageDeletes, run.OrphanStorageSample)
	}
	if run.SkippedGrace != 1 {
		t.Errorf("expected 1 grace skip, got %d", run.SkippedGrace)
	}
	// Storage now has only a.parquet and young.parquet.
	if exists, _ := store.Exists(context.Background(), "db/m/2026/04/27/12/orphan-1.parquet"); exists {
		t.Error("orphan-1.parquet should be deleted")
	}
	if exists, _ := store.Exists(context.Background(), "db/m/2026/04/27/12/orphan-2.parquet"); exists {
		t.Error("orphan-2.parquet should be deleted")
	}
	if exists, _ := store.Exists(context.Background(), "db/m/2026/04/27/12/young.parquet"); !exists {
		t.Error("young.parquet should NOT be deleted (grace window)")
	}
	if exists, _ := store.Exists(context.Background(), "db/m/2026/04/27/12/a.parquet"); !exists {
		t.Error("a.parquet should NOT be deleted (in manifest)")
	}
}

func TestReconcile_StorageSweepRecheckCatchesRace(t *testing.T) {
	now := time.Now().UTC()
	coord := newFakeCoordinator()
	store := newFakeBackend()
	store.put("db/m/raced.parquet", now.Add(-48*time.Hour))

	// Simulate the race: between the diff snapshot and the sweep, a writer
	// registers the file. Easiest way to plant this: pre-register so the
	// diff sees it as orphan-storage (manifest empty at snapshot time,
	// but our fake stores re-checks against the *current* manifest).
	//
	// Trick: build a Reconciler whose snapshotManifest sees an empty
	// manifest, but whose GetFileEntry sees the registered entry. We do
	// this by having the test pre-register AFTER snapshotManifest reads
	// but BEFORE sweepOrphanStorage runs. Without test hooks the simplest
	// approach is to register the file in the fake coordinator now (so
	// snapshot sees it) and watch the diff: with the file in BOTH manifest
	// and storage, the diff finds nothing to delete. So this test instead
	// asserts the simpler invariant: when GetFileEntry returns true mid-run,
	// SkippedRecheck is incremented and the file is NOT deleted.
	//
	// We can simulate this by registering the manifest entry AFTER the
	// diff is computed but the only sync point we have is between Reconcile
	// invocations. So: run twice. First run with empty manifest deletes the
	// file. To test the re-check, we use a stub: pre-register a manifest
	// entry with the same path BUT in storage have a file that's old. The
	// diff will find the file IS in the manifest → not orphan-storage at
	// all, and the sweep won't be invoked for it. Skip this test as written.
	//
	// Alternative shape that actually exercises the re-check: have a
	// custom Coordinator that returns empty from GetFileManifest but
	// returns a hit from GetFileEntry for the path. That mimics the race.
	coord.manifest["db/m/raced.parquet"] = &raft.FileEntry{
		Path: "db/m/raced.parquet", Database: "db", Measurement: "m",
		OriginNodeID: "node-a", CreatedAt: now,
	}
	// Now install a wrapper that hides the entry from GetFileManifest
	// (so the diff thinks it's orphan-storage) but exposes it via
	// GetFileEntry (so the re-check skips).
	wrapped := &racingCoordinator{inner: coord, hidePath: "db/m/raced.parquet"}

	r, err := NewReconciler(
		Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: true},
		wrapped, store, &fakeGate{scan: true, sweep: true}, nil, zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.OrphanStorageCount != 1 {
		t.Errorf("expected 1 orphan-storage candidate before recheck, got %d", run.OrphanStorageCount)
	}
	if run.SkippedRecheck != 1 {
		t.Errorf("expected 1 skipped-recheck, got %d", run.SkippedRecheck)
	}
	if run.StorageDeletes != 0 {
		t.Errorf("re-check race: no storage deletes, got %d", run.StorageDeletes)
	}
	if exists, _ := store.Exists(context.Background(), "db/m/raced.parquet"); !exists {
		t.Error("file should NOT have been deleted (concurrent register raced)")
	}
}

func TestReconcile_PreManifestOrphansRespectFlag(t *testing.T) {
	now := time.Now().UTC()
	coord := newFakeCoordinator()
	store := newFakeBackend()
	// 3-segment path — matches the OLD permissive check but NOT the
	// new tightened looksLikeManagedPath that requires the full 7-segment
	// db/m/yyyy/mm/dd/hh/file.parquet layout.
	store.put("foo/bar/baz.parquet", now.Add(-48*time.Hour))
	// Properly-laid-out file too.
	store.put("db/m/2026/04/27/12/orphan.parquet", now.Add(-48*time.Hour))

	// Flag OFF: foo/bar/baz should be left alone, orphan should be deleted.
	r := newReconciler(t,
		Config{Enabled: true, BackendKind: BackendShared, DeletePreManifestOrphans: false},
		coord, store, &fakeGate{scan: true, sweep: true})
	run, err := r.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.StorageDeletes != 1 {
		t.Errorf("expected 1 delete (the orphan, not foo/bar/baz), got %d", run.StorageDeletes)
	}
	if exists, _ := store.Exists(context.Background(), "foo/bar/baz.parquet"); !exists {
		t.Error("foo/bar/baz.parquet should NOT be deleted (DeletePreManifestOrphans=false rejects sub-7-segment paths)")
	}
}

func TestLooksLikeManagedPath(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"db/m/2026/04/27/12/file.parquet", true},          // canonical 7-segment
		{"db/m/2026/04/27/12/sub/file.parquet", true},      // 8 segments — fine
		{"db/m/file.parquet", false},                       // too short
		{"db/m/2026/04/27/file.parquet", false},            // 6 segments — too short
		{"db/m/2026/04/27/12/", false},                     // empty trailing segment
		{"../etc/passwd/2026/04/27/12/x.parquet", false},   // .. segment
		{"db/m/./27/04/12/x.parquet", false},               // . segment
		{"//m/2026/04/27/12/file.parquet", false},          // empty leading segment
	}
	for _, c := range cases {
		got := looksLikeManagedPath(c.path)
		if got != c.want {
			t.Errorf("looksLikeManagedPath(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestReconcile_WalkPartialFlagSurfacesPrefixErrors(t *testing.T) {
	// Wrap fakeBackend with an ObjectLister that returns an error for one
	// prefix to simulate a transient backend hiccup.
	now := time.Now().UTC()
	coord := newFakeCoordinator(
		fileEntry("db/m1/2026/04/27/12/a.parquet", "node-a"),
		fileEntry("db/m2/2026/04/27/12/b.parquet", "node-a"),
	)
	store := newFakeBackend()
	store.put("db/m1/2026/04/27/12/a.parquet", now.Add(-2*time.Hour))
	store.put("db/m2/2026/04/27/12/b.parquet", now.Add(-2*time.Hour))

	flaky := &flakyObjectLister{fakeBackend: store, badPrefix: "db/m1/"}
	r, err := NewReconciler(
		Config{Enabled: true, BackendKind: BackendShared},
		coord, flaky, &fakeGate{scan: true, sweep: true}, nil, zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	run, err := r.Reconcile(context.Background(), true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !run.WalkPartial {
		t.Error("expected run.WalkPartial=true after a prefix list failure")
	}
	if len(run.Errors) == 0 {
		t.Error("expected run.Errors to surface the prefix failure")
	}
}

// flakyObjectLister returns an error on a chosen prefix; the rest is
// delegated to the wrapped fakeBackend.
type flakyObjectLister struct {
	*fakeBackend
	badPrefix string
}

func (f *flakyObjectLister) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if prefix == f.badPrefix {
		return nil, errors.New("simulated backend hiccup")
	}
	return f.fakeBackend.ListObjects(ctx, prefix)
}

// racingCoordinator wraps a fakeCoordinator and hides a single path
// from GetFileManifest while keeping it visible via GetFileEntry. Used
// to simulate the diff-snapshot-vs-sweep-recheck race.
type racingCoordinator struct {
	inner    *fakeCoordinator
	hidePath string
}

func (c *racingCoordinator) GetFileManifest() []*raft.FileEntry {
	all := c.inner.GetFileManifest()
	out := make([]*raft.FileEntry, 0, len(all))
	for _, e := range all {
		if e.Path != c.hidePath {
			out = append(out, e)
		}
	}
	return out
}

func (c *racingCoordinator) GetFileEntry(path string) (*raft.FileEntry, bool) {
	return c.inner.GetFileEntry(path)
}

func (c *racingCoordinator) BatchFileOpsInManifest(ops []raft.BatchFileOp) error {
	return c.inner.BatchFileOpsInManifest(ops)
}

// ---- Helpers ----

func fileEntry(path, originID string) *raft.FileEntry {
	parts := strings.SplitN(path, "/", 3)
	db, meas := "", ""
	if len(parts) >= 2 {
		db, meas = parts[0], parts[1]
	}
	return &raft.FileEntry{
		Path:         path,
		SizeBytes:    1024,
		Database:     db,
		Measurement:  meas,
		OriginNodeID: originID,
		Tier:         "hot",
		CreatedAt:    time.Now().UTC().Add(-2 * time.Hour),
	}
}
