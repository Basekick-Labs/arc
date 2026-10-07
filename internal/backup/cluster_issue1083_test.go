package backup

// Tests for #1083: backup and restore on a cluster node. A fake cluster
// manifest and a fake tier recorder stand in for the coordinator and tiering;
// no Raft is involved. Every behaviour here is reached only through the two
// hooks, so the tests at the end confirm the standalone path is unchanged.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

const fakeNodeID = "arc-writer-0.arc-writer.arc.svc.cluster.local"

// fakeClusterManifest records every call. onRead runs before the n-th
// ManifestFiles call returns (1-based), so a test can change the world
// between the backup's two snapshots. order records every call kind;
// manifestOps filters it to the register/delete writes.
type fakeClusterManifest struct {
	mu               sync.Mutex
	entries          []ManifestFile
	onRead           func(call int)
	reads            int
	syncErr          error
	registers        [][]ManifestFile
	deletes          [][]string
	reasons          []string
	order            []string
	failRegisterCall int // 1-based call index to refuse; 0 never
	failDelete       bool
	// The cluster-wide compaction pause (#1087). pauseErr refuses the pause;
	// pauseReasons records what each pause was taken for; loseAfterRegisters
	// makes Lost report true once that many register calls have happened
	// (0 never). "pause" and "resume" land in order like every other call.
	pauseErr           error
	pauseReasons       []string
	loseAfterRegisters int
	pauses             []*fakeCompactionPause
}

// fakeCompactionPause is the handle the fake hands out.
type fakeCompactionPause struct {
	f       *fakeClusterManifest
	resumes int
}

func (f *fakeClusterManifest) PauseCompaction(_ context.Context, reason string) (CompactionPause, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.order = append(f.order, "pause")
	f.pauseReasons = append(f.pauseReasons, reason)
	if f.pauseErr != nil {
		return nil, f.pauseErr
	}
	p := &fakeCompactionPause{f: f}
	f.pauses = append(f.pauses, p)
	return p, nil
}

func (p *fakeCompactionPause) Lost() (bool, error) {
	p.f.mu.Lock()
	defer p.f.mu.Unlock()
	if p.f.loseAfterRegisters > 0 && len(p.f.registers) >= p.f.loseAfterRegisters {
		return true, errors.New("lost at 2026-10-06T10:00:00Z: generation 3 requested by other-node replaced this generation 2")
	}
	return false, nil
}

func (p *fakeCompactionPause) Resume(context.Context) error {
	p.f.mu.Lock()
	defer p.f.mu.Unlock()
	p.resumes++
	p.f.order = append(p.f.order, "resume")
	return nil
}

// pauseOps filters order to the pause/resume calls and the manifest writes,
// so a test can assert the pause brackets every write.
func (f *fakeClusterManifest) pauseOps() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, op := range f.order {
		if op == "register" || op == "delete" || op == "pause" || op == "resume" || op == "sync" || op == "read" {
			out = append(out, op)
		}
	}
	return out
}

func (f *fakeClusterManifest) Sync(context.Context) error {
	f.mu.Lock()
	f.order = append(f.order, "sync")
	f.mu.Unlock()
	return f.syncErr
}

func (f *fakeClusterManifest) LocalNodeID() string { return fakeNodeID }

func (f *fakeClusterManifest) ManifestFiles() []ManifestFile {
	f.mu.Lock()
	f.reads++
	call := f.reads
	hook := f.onRead
	f.order = append(f.order, "read")
	f.mu.Unlock()
	if hook != nil {
		hook(call)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ManifestFile, len(f.entries))
	copy(out, f.entries)
	return out
}

func (f *fakeClusterManifest) BatchRegister(_ context.Context, files []ManifestFile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	batch := make([]ManifestFile, len(files))
	copy(batch, files)
	f.registers = append(f.registers, batch)
	f.order = append(f.order, "register")
	if f.failRegisterCall == len(f.registers) {
		return errors.New("raft: manifest apply failed: no quorum")
	}
	return nil
}

func (f *fakeClusterManifest) BatchDelete(_ context.Context, paths []string, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	batch := make([]string, len(paths))
	copy(batch, paths)
	f.deletes = append(f.deletes, batch)
	f.reasons = append(f.reasons, reason)
	f.order = append(f.order, "delete")
	if f.failDelete {
		return errors.New("raft: manifest apply failed: no quorum")
	}
	return nil
}

func (f *fakeClusterManifest) add(e ManifestFile) {
	f.mu.Lock()
	f.entries = append(f.entries, e)
	f.mu.Unlock()
}

func (f *fakeClusterManifest) remove(paths ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gone := make(map[string]bool, len(paths))
	for _, p := range paths {
		gone[p] = true
	}
	kept := f.entries[:0]
	for _, e := range f.entries {
		if !gone[e.Path] {
			kept = append(kept, e)
		}
	}
	f.entries = kept
}

func (f *fakeClusterManifest) registeredPaths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, b := range f.registers {
		for _, e := range b {
			out = append(out, e.Path)
		}
	}
	return out
}

func (f *fakeClusterManifest) manifestOps() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, op := range f.order {
		if op == "register" || op == "delete" {
			out = append(out, op)
		}
	}
	return out
}

type tierReport struct {
	path string
	size int64
}

type fakeTierRecorder struct {
	mu      sync.Mutex
	reports []tierReport
}

func (r *fakeTierRecorder) RecordRestoredFile(path string, sizeBytes int64) {
	r.mu.Lock()
	r.reports = append(r.reports, tierReport{path: path, size: sizeBytes})
	r.mu.Unlock()
}

func shaOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

var fixedCreatedAt = time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)

// entryFor is the manifest entry the cluster would hold for a file with the
// given bytes, with a created_at that is recognisably not "now".
func entryFor(path string, data []byte) ManifestFile {
	db, meas := parseDBMeasurement(path)
	return ManifestFile{
		Path: path, SHA256: shaOf(data), SizeBytes: int64(len(data)),
		Database: db, Measurement: meas,
		PartitionTime: partitionTimeFromPath(path), CreatedAt: fixedCreatedAt,
	}
}

func mustWrite(t *testing.T, b storage.Backend, path string, data []byte) {
	t.Helper()
	if err := b.Write(context.Background(), path, data); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readSidecarFile(t *testing.T, backupDir, backupID string) fileSidecar {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(backupDir, backupID, sidecarName))
	if err != nil {
		t.Fatalf("sidecar: %v", err)
	}
	var sc fileSidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatalf("decode sidecar: %v", err)
	}
	return sc
}

func writeSidecarFile(t *testing.T, backupDir, backupID string, sc fileSidecar) {
	t.Helper()
	raw, err := json.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, backupID, sidecarName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func sidecarByPath(sc fileSidecar) map[string]ManifestFile {
	out := make(map[string]ManifestFile, len(sc.Files))
	for _, f := range sc.Files {
		out[f.Path] = f
	}
	return out
}

func newClusterManager(t *testing.T, data storage.Backend, backupDir string, cm ClusterManifest, tr TierRecorder) *Manager {
	t.Helper()
	m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	m.SetClusterManifest(cm)
	m.SetTierRecorder(tr)
	return m
}

func existsIn(t *testing.T, b storage.Backend, path string) bool {
	t.Helper()
	ok, err := b.Exists(context.Background(), path)
	if err != nil {
		t.Fatalf("exists %s: %v", path, err)
	}
	return ok
}

// The compaction window after phase 2 has landed on every node but this one
// still lists the inputs: the compacted output is listed and registered, the
// inputs it replaced are listed but no longer in the manifest. The inputs are
// not copied, are counted and named, and stay out of the inventory; the
// output, a registered file in another measurement and the _schema anchor are
// copied as before. The sidecar describes exactly the registered data files,
// with the hash of the bytes and the manifest entry's metadata.
func TestBackup_ClusterSkipsUnregisteredLocalFiles(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	const (
		partition = "db/cpu/2026/01/01/00"
		output    = partition + "/x_compacted.parquet"
		inputA    = partition + "/a.parquet"
		inputB    = partition + "/b.parquet"
		other     = "db/mem/2026/01/01/00/u00.parquet"
		anchor    = "_schema/db/cpu/anchor.parquet"
	)
	outBytes, otherBytes := []byte("OUT-compacted"), []byte("U-registered")
	mustWrite(t, data, inputA, []byte("IN-a"))
	mustWrite(t, data, inputB, []byte("IN-b"))
	mustWrite(t, data, output, outBytes)
	mustWrite(t, data, other, otherBytes)
	mustWrite(t, data, anchor, []byte("ANCHOR"))

	cm := &fakeClusterManifest{entries: []ManifestFile{entryFor(output, outBytes), entryFor(other, otherBytes)}}
	backupDir := t.TempDir()
	m := newClusterManager(t, data, backupDir, cm, nil)

	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	mf := res.Manifest
	if !mf.ClusterManifestChecked {
		t.Error("ClusterManifestChecked = false, want true")
	}
	if mf.UnregisteredSkipped != 2 || strings.Join(mf.UnregisteredSample, ",") != inputA+","+inputB {
		t.Errorf("unregistered = %d %v, want 2 [%s %s]", mf.UnregisteredSkipped, mf.UnregisteredSample, inputA, inputB)
	}
	if mf.ManifestOnlyFiles != 0 || mf.LeftManifestDuringRun != 0 {
		t.Errorf("manifest-only=%d left=%d, want 0, 0", mf.ManifestOnlyFiles, mf.LeftManifestDuringRun)
	}
	if mf.TotalFiles != 3 || mf.AuxiliaryFiles != 1 || mf.SkippedFiles != 0 {
		t.Errorf("total=%d auxiliary=%d skipped=%d, want 3, 1, 0", mf.TotalFiles, mf.AuxiliaryFiles, mf.SkippedFiles)
	}
	if len(mf.Databases) != 1 || mf.Databases[0].FileCount != 2 || len(mf.Databases[0].Measurements) != 2 {
		t.Errorf("inventory = %+v, want db with cpu(1) and mem(1)", mf.Databases)
	}
	if cm.reads != 2 {
		t.Errorf("ManifestFiles calls = %d, want 2 (snapshot after the listing, re-check at the end)", cm.reads)
	}

	backupStore := mustLocalBackend(t, backupDir, zerolog.Nop())
	for _, k := range []string{inputA, inputB} {
		if existsIn(t, backupStore, mf.BackupID+"/data/"+k) {
			t.Errorf("%s was copied; an unregistered file must not be", k)
		}
	}
	for _, k := range []string{output, other, anchor} {
		if !existsIn(t, backupStore, mf.BackupID+"/data/"+k) {
			t.Errorf("%s not in backup", k)
		}
	}

	sc := readSidecarFile(t, backupDir, mf.BackupID)
	if sc.Version != sidecarVersion || !sc.FromClusterManifest || len(sc.Files) != 2 {
		t.Fatalf("sidecar = version %d from_manifest %v %d files, want %d, true, 2", sc.Version, sc.FromClusterManifest, len(sc.Files), sidecarVersion)
	}
	rows := sidecarByPath(sc)
	want := entryFor(output, outBytes)
	if got := rows[output]; got != want {
		t.Errorf("sidecar[%s] = %+v, want %+v", output, got, want)
	}
	if _, ok := rows[anchor]; ok {
		t.Error("sidecar lists the _schema anchor; reserved roots are never registered")
	}

	p := m.GetProgress()
	if p.UnregisteredSkipped != 2 || len(p.UnregisteredSample) != 2 {
		t.Errorf("status unregistered = %d %v, want 2 and 2 names", p.UnregisteredSkipped, p.UnregisteredSample)
	}
	summaries, err := m.ListBackups(ctx)
	if err != nil || len(summaries) != 1 {
		t.Fatalf("ListBackups = %v, %v", summaries, err)
	}
	if summaries[0].UnregisteredSkipped != 2 || summaries[0].TotalFiles != 3 {
		t.Errorf("summary = %+v, want unregistered 2, total 3", summaries[0])
	}
}

// The two-phase compaction window as the cluster commits it (watcher.go):
// at the first snapshot the output is registered AND the inputs are still
// registered (phase 1 landed, phase 2 not yet), so the backup copies all
// three; phase 2 lands during the run; the end-of-run re-read finds the
// inputs gone from the manifest and takes them back out of the backup,
// counted as left_manifest_during_run. In the same run a manifest entry this
// node pulled after the listing is copied, and one it still lacks is counted.
func TestBackup_ClusterTwoPhaseCompactionWindow(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	const (
		partition = "db/cpu/2026/01/01/00"
		output    = partition + "/x_compacted.parquet"
		inputA    = partition + "/a.parquet"
		inputB    = partition + "/b.parquet"
		pulled    = "db/cpu/2026/01/01/01/pulled-after-listing.parquet"
		never     = "db/cpu/2026/01/01/02/never-arrives.parquet"
	)
	outBytes, aBytes, bBytes, pulledBytes := []byte("OUT-compacted"), []byte("IN-a"), []byte("IN-b"), []byte("PULLED")
	mustWrite(t, data, inputA, aBytes)
	mustWrite(t, data, inputB, bBytes)
	mustWrite(t, data, output, outBytes)
	cm := &fakeClusterManifest{entries: []ManifestFile{
		entryFor(inputA, aBytes), entryFor(inputB, bBytes), entryFor(output, outBytes),
		entryFor(pulled, pulledBytes), entryFor(never, []byte("N")),
	}}
	cm.onRead = func(call int) {
		if call == 2 {
			cm.remove(inputA, inputB)               // phase 2 landed
			mustWrite(t, data, pulled, pulledBytes) // replication delivered it
		}
	}
	backupDir := t.TempDir()
	m := newClusterManager(t, data, backupDir, cm, nil)

	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	mf := res.Manifest
	if mf.LeftManifestDuringRun != 2 || strings.Join(mf.LeftManifestSample, ",") != inputA+","+inputB {
		t.Errorf("left = %d %v, want 2 [%s %s]", mf.LeftManifestDuringRun, mf.LeftManifestSample, inputA, inputB)
	}
	if mf.ManifestOnlyFiles != 1 || strings.Join(mf.ManifestOnlySample, ",") != never {
		t.Errorf("manifest-only = %d %v, want 1 [%s]", mf.ManifestOnlyFiles, mf.ManifestOnlySample, never)
	}
	if mf.TotalFiles != 2 || mf.TotalSizeBytes != int64(len(outBytes)+len(pulledBytes)) || mf.UnregisteredSkipped != 0 {
		t.Errorf("total=%d bytes=%d unregistered=%d, want 2, %d, 0", mf.TotalFiles, mf.TotalSizeBytes, mf.UnregisteredSkipped, len(outBytes)+len(pulledBytes))
	}
	if len(mf.Databases) != 1 || mf.Databases[0].FileCount != 2 || len(mf.Databases[0].Measurements) != 1 || mf.Databases[0].Measurements[0].FileCount != 2 {
		t.Errorf("inventory = %+v, want db/cpu with 2 files", mf.Databases)
	}
	backupStore := mustLocalBackend(t, backupDir, zerolog.Nop())
	for _, k := range []string{inputA, inputB} {
		if existsIn(t, backupStore, mf.BackupID+"/data/"+k) {
			t.Errorf("%s is still in the backup after leaving the manifest", k)
		}
	}
	for _, k := range []string{output, pulled} {
		if !existsIn(t, backupStore, mf.BackupID+"/data/"+k) {
			t.Errorf("%s not in backup", k)
		}
	}
	rows := sidecarByPath(readSidecarFile(t, backupDir, mf.BackupID))
	if len(rows) != 2 {
		t.Errorf("sidecar rows = %v, want output and pulled only", rows)
	}
	if got, want := rows[pulled], entryFor(pulled, pulledBytes); got != want {
		t.Errorf("sidecar[%s] = %+v, want the manifest entry %+v", pulled, got, want)
	}
	p := m.GetProgress()
	if p.ProcessedFiles != 2 || p.TotalFiles != 2 || p.LeftManifestDuringRun != 2 || p.ManifestOnlyFiles != 1 {
		t.Errorf("progress processed=%d total=%d left=%d manifest-only=%d, want 2 2 2 1", p.ProcessedFiles, p.TotalFiles, p.LeftManifestDuringRun, p.ManifestOnlyFiles)
	}
	summaries, _ := m.ListBackups(ctx)
	if len(summaries) != 1 || summaries[0].LeftManifestDuringRun != 2 || summaries[0].ManifestOnlyFiles != 1 {
		t.Errorf("summary = %+v, want left 2, manifest-only 1", summaries)
	}
}

// Direction (a): a manifest entry this node does not hold at the listing. One
// never arrives and is counted; one arrives between the listing and the end
// of the run and is copied; one leaves the manifest before the re-check
// (retention) and is not a gap.
func TestBackup_ClusterManifestOnlyIsRecheckedAtTheEnd(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	const (
		held     = "db/cpu/2026/01/01/00/held.parquet"
		never    = "db/cpu/2026/01/01/00/never-arrives.parquet"
		arrives  = "db/cpu/2026/01/01/01/arrives-late.parquet"
		retained = "db/cpu/2026/01/01/02/retention-took-it.parquet"
	)
	heldBytes, lateBytes := []byte("HELD"), []byte("LATE")
	mustWrite(t, data, held, heldBytes)
	cm := &fakeClusterManifest{entries: []ManifestFile{
		entryFor(held, heldBytes), entryFor(never, []byte("N")), entryFor(arrives, lateBytes), entryFor(retained, []byte("R")),
	}}
	cm.onRead = func(call int) {
		if call == 2 {
			mustWrite(t, data, arrives, lateBytes) // pulled after the listing
			cm.remove(retained)                    // retention ran meanwhile
		}
	}
	backupDir := t.TempDir()
	m := newClusterManager(t, data, backupDir, cm, nil)

	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	mf := res.Manifest
	if mf.ManifestOnlyFiles != 1 || strings.Join(mf.ManifestOnlySample, ",") != never {
		t.Errorf("manifest-only = %d %v, want 1 [%s]", mf.ManifestOnlyFiles, mf.ManifestOnlySample, never)
	}
	if mf.TotalFiles != 2 || mf.UnregisteredSkipped != 0 || mf.LeftManifestDuringRun != 0 {
		t.Errorf("total=%d unregistered=%d left=%d, want 2, 0, 0", mf.TotalFiles, mf.UnregisteredSkipped, mf.LeftManifestDuringRun)
	}
	backupStore := mustLocalBackend(t, backupDir, zerolog.Nop())
	if !existsIn(t, backupStore, mf.BackupID+"/data/"+arrives) {
		t.Error("a registered file pulled after the listing was not copied at the re-check")
	}
	rows := sidecarByPath(readSidecarFile(t, backupDir, mf.BackupID))
	if got, want := rows[arrives], entryFor(arrives, lateBytes); got != want {
		t.Errorf("sidecar[%s] = %+v, want %+v", arrives, got, want)
	}
	if p := m.GetProgress(); p.ManifestOnlyFiles != 1 || p.ProcessedFiles != 2 {
		t.Errorf("status manifest_only_files=%d processed=%d, want 1, 2", p.ManifestOnlyFiles, p.ProcessedFiles)
	}
	summaries, _ := m.ListBackups(ctx)
	if len(summaries) != 1 || summaries[0].ManifestOnlyFiles != 1 {
		t.Errorf("summary = %+v, want manifest_only_files 1", summaries)
	}
}

// Direction (b), the transient half: a listed file whose registration had not
// reached this node at the first snapshot but has by the second is copied
// after all, inventoried, and described in the sidecar from its entry.
func TestBackup_ClusterLateRegistrationIsCopied(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	const (
		first = "db/cpu/2026/01/01/00/f1.parquet"
		late  = "db/cpu/2026/01/01/00/f2-late.parquet"
	)
	firstBytes, lateBytes := []byte("F1"), []byte("F2-late")
	mustWrite(t, data, first, firstBytes)
	mustWrite(t, data, late, lateBytes)
	cm := &fakeClusterManifest{entries: []ManifestFile{entryFor(first, firstBytes)}}
	cm.onRead = func(call int) {
		if call == 2 {
			cm.add(entryFor(late, lateBytes))
		}
	}
	backupDir := t.TempDir()
	m := newClusterManager(t, data, backupDir, cm, nil)

	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	mf := res.Manifest
	if mf.TotalFiles != 2 || mf.UnregisteredSkipped != 0 || mf.TotalSizeBytes != int64(len(firstBytes)+len(lateBytes)) {
		t.Errorf("total=%d unregistered=%d bytes=%d, want 2, 0, %d", mf.TotalFiles, mf.UnregisteredSkipped, mf.TotalSizeBytes, len(firstBytes)+len(lateBytes))
	}
	if len(mf.Databases) != 1 || mf.Databases[0].FileCount != 2 || mf.Databases[0].Measurements[0].FileCount != 2 {
		t.Errorf("inventory = %+v, want db/cpu with 2 files", mf.Databases)
	}
	backupStore := mustLocalBackend(t, backupDir, zerolog.Nop())
	if !existsIn(t, backupStore, mf.BackupID+"/data/"+late) {
		t.Error("late-registered file not copied")
	}
	rows := sidecarByPath(readSidecarFile(t, backupDir, mf.BackupID))
	if got, want := rows[late], entryFor(late, lateBytes); got != want {
		t.Errorf("sidecar[%s] = %+v, want the manifest entry %+v", late, got, want)
	}
	if p := m.GetProgress(); p.TotalFiles != 2 || p.ProcessedFiles != 2 {
		t.Errorf("progress total=%d processed=%d, want 2, 2", p.TotalFiles, p.ProcessedFiles)
	}
}

// An empty manifest while the node lists data files is a node with nothing to
// check against, not a node with no data: the backup is refused and nothing
// is written. A node that lists no data files (only an anchor) proceeds.
func TestBackup_ClusterEmptyManifestRefused(t *testing.T) {
	ctx := context.Background()
	t.Run("data files listed", func(t *testing.T) {
		data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		mustWrite(t, data, "db/cpu/2026/01/01/00/f.parquet", []byte("F"))
		mustWrite(t, data, "_compaction_state/hourly/db/job.json", []byte("{}"))
		backupDir := t.TempDir()
		m := newClusterManager(t, data, backupDir, &fakeClusterManifest{}, nil)
		_, err := m.CreateBackup(ctx, BackupOptions{})
		if err == nil || !strings.Contains(err.Error(), "cluster manifest is empty") {
			t.Fatalf("err = %v, want a refusal naming the empty manifest", err)
		}
		if p := m.GetProgress(); p.Status != "failed" {
			t.Errorf("status = %s, want failed", p.Status)
		}
		if entries, _ := os.ReadDir(backupDir); len(entries) != 0 {
			t.Errorf("backup dir holds %v after a refusal; nothing may be written", entries)
		}
	})
	t.Run("no data files listed", func(t *testing.T) {
		data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		mustWrite(t, data, "_schema/db/cpu/anchor.parquet", []byte("A"))
		m := newClusterManager(t, data, t.TempDir(), &fakeClusterManifest{}, nil)
		res, err := m.CreateBackup(ctx, BackupOptions{})
		if err != nil || res.Manifest.TotalFiles != 1 {
			t.Fatalf("err = %v total = %v, want an anchor-only backup", err, res)
		}
	})
}

// Every snapshot is preceded by a sync, on backup and on restore, and a sync
// that fails fails the run before anything is copied.
func TestBackup_ClusterSyncsBeforeEverySnapshot(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	const f = "db/cpu/2026/01/01/00/f.parquet"
	mustWrite(t, data, f, []byte("F"))
	t.Run("backup order", func(t *testing.T) {
		cm := &fakeClusterManifest{entries: []ManifestFile{entryFor(f, []byte("F"))}}
		m := newClusterManager(t, data, t.TempDir(), cm, nil)
		if _, err := m.CreateBackup(ctx, BackupOptions{}); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(cm.order, ","); got != "sync,read,sync,read" {
			t.Errorf("call order = %s, want sync,read,sync,read", got)
		}
	})
	t.Run("backup sync failure", func(t *testing.T) {
		cm := &fakeClusterManifest{entries: []ManifestFile{entryFor(f, []byte("F"))}, syncErr: errors.New("follower barrier: could not reach the leader")}
		backupDir := t.TempDir()
		m := newClusterManager(t, data, backupDir, cm, nil)
		_, err := m.CreateBackup(ctx, BackupOptions{})
		if err == nil || !strings.Contains(err.Error(), "could not sync the cluster manifest") {
			t.Fatalf("err = %v, want a sync failure", err)
		}
		if entries, _ := os.ReadDir(backupDir); len(entries) != 0 {
			t.Errorf("backup dir holds %v after a sync failure", entries)
		}
	})
	t.Run("restore order", func(t *testing.T) {
		backupDir, backupID, _, _ := seedClusterBackup(t, 2)
		cm := &fakeClusterManifest{}
		m := newClusterManager(t, mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupDir, cm, nil)
		if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true}); err != nil {
			t.Fatal(err)
		}
		// The pause (#1087) brackets every manifest access.
		if got := strings.Join(cm.order, ","); got != "pause,sync,read,register,resume" {
			t.Errorf("call order = %s, want pause,sync,read,register,resume", got)
		}
	})
	t.Run("restore sync failure", func(t *testing.T) {
		backupDir, backupID, keys, _ := seedClusterBackup(t, 2)
		cm := &fakeClusterManifest{syncErr: errors.New("raft not available")}
		dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		m := newClusterManager(t, dest, backupDir, cm, nil)
		_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true})
		if err == nil || !strings.Contains(err.Error(), "could not sync the cluster manifest") {
			t.Fatalf("err = %v, want a sync failure", err)
		}
		if existsIn(t, dest, keys[0]) {
			t.Error("a file was written after the sync failed")
		}
	})
}

// A skipped source read (compaction or retention removed the file between the
// listing and its copy) is reconciled against the end-of-run manifest: gone
// from the manifest means not missing data, and a replace-mode restore is not
// blocked by it; still in the manifest means the backup really is incomplete.
func TestBackup_ClusterSkipsReconciledAgainstTheManifest(t *testing.T) {
	ctx := context.Background()
	const ghost = "db/cpu/2026/09/17/00/compacted-away.parquet"
	seed := func(t *testing.T, ghostLeaves bool) (string, string, *fakeClusterManifest) {
		t.Helper()
		data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		cm := &fakeClusterManifest{}
		for i := 0; i < 20; i++ {
			k := fmt.Sprintf("db/cpu/2026/09/17/00/good-%02d.parquet", i)
			b := []byte(fmt.Sprintf("PAR1-good-%02d", i))
			mustWrite(t, data, k, b)
			cm.add(entryFor(k, b))
		}
		cm.add(entryFor(ghost, []byte("gone")))
		if ghostLeaves {
			cm.onRead = func(call int) {
				if call == 2 {
					cm.remove(ghost)
				}
			}
		}
		backupDir := t.TempDir()
		m := newClusterManager(t, skipInventory{Backend: data, extra: []string{ghost}}, backupDir, cm, nil)
		res, err := m.CreateBackup(ctx, BackupOptions{})
		if err != nil {
			t.Fatalf("CreateBackup: %v", err)
		}
		mf := res.Manifest
		if mf.SkippedFiles != 1 || strings.Join(mf.SkippedSample, ",") != ghost || mf.TotalFiles != 21 {
			t.Fatalf("skipped=%d sample=%v total=%d, want 1 [%s] 21", mf.SkippedFiles, mf.SkippedSample, mf.TotalFiles, ghost)
		}
		return backupDir, mf.BackupID, cm
	}
	replaceInto := func(t *testing.T, backupDir, backupID string) error {
		t.Helper()
		cm := &fakeClusterManifest{entries: []ManifestFile{entryFor("db/cpu/2026/09/17/00/stale.parquet", []byte("x"))}}
		m := newClusterManager(t, mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupDir, cm, nil)
		_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true, Mode: RestoreModeReplace})
		return err
	}
	t.Run("ghost left the manifest", func(t *testing.T) {
		backupDir, backupID, _ := seed(t, true)
		mf, err := (&Manager{backupStorage: mustLocalBackend(t, backupDir, zerolog.Nop())}).GetBackup(ctx, backupID)
		if err != nil {
			t.Fatal(err)
		}
		if mf.SkippedReconciled != 1 {
			t.Errorf("SkippedReconciled = %d, want 1", mf.SkippedReconciled)
		}
		if err := replaceInto(t, backupDir, backupID); err != nil {
			t.Errorf("replace with a reconciled skip: %v, want success", err)
		}
	})
	t.Run("ghost still in the manifest", func(t *testing.T) {
		backupDir, backupID, _ := seed(t, false)
		mf, err := (&Manager{backupStorage: mustLocalBackend(t, backupDir, zerolog.Nop())}).GetBackup(ctx, backupID)
		if err != nil {
			t.Fatal(err)
		}
		if mf.SkippedReconciled != 0 {
			t.Errorf("SkippedReconciled = %d, want 0", mf.SkippedReconciled)
		}
		if err := replaceInto(t, backupDir, backupID); err == nil || !strings.Contains(err.Error(), "incomplete") {
			t.Errorf("replace with an unreconciled skip: err = %v, want a refusal", err)
		}
	})
}

// Every backup writes the sidecar, clustered or not. Standalone rows carry
// the hash and size of the bytes, the database and measurement from the path,
// the hour partition from the path, and a created_at of the backup time.
func TestBackup_StandaloneWritesSidecarFromPaths(t *testing.T) {
	ctx := context.Background()
	data := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	const (
		f1     = "db/cpu/2026/09/13/07/f1.parquet"
		anchor = "_schema/db/cpu/anchor.parquet"
	)
	f1Bytes := bytes.Repeat([]byte("p"), 4096)
	mustWrite(t, data, f1, f1Bytes)
	mustWrite(t, data, anchor, []byte("A"))
	backupDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{DataStorage: data, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().Add(-time.Second)
	res, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if res.Manifest.ClusterManifestChecked {
		t.Error("ClusterManifestChecked = true without a cluster manifest")
	}
	sc := readSidecarFile(t, backupDir, res.Manifest.BackupID)
	if sc.FromClusterManifest || len(sc.Files) != 1 {
		t.Fatalf("sidecar = from_manifest %v, %d files; want false, 1 (the anchor is not a data file)", sc.FromClusterManifest, len(sc.Files))
	}
	row := sc.Files[0]
	if row.Path != f1 || row.SHA256 != shaOf(f1Bytes) || row.SizeBytes != int64(len(f1Bytes)) {
		t.Errorf("row = %+v, want path %s sha %s size %d", row, f1, shaOf(f1Bytes), len(f1Bytes))
	}
	if row.Database != "db" || row.Measurement != "cpu" {
		t.Errorf("row db/meas = %s/%s, want db/cpu", row.Database, row.Measurement)
	}
	if want := time.Date(2026, 9, 13, 7, 0, 0, 0, time.UTC); !row.PartitionTime.Equal(want) {
		t.Errorf("row partition = %v, want %v", row.PartitionTime, want)
	}
	if row.CreatedAt.Before(before) || row.CreatedAt.After(time.Now().Add(time.Second)) {
		t.Errorf("row created_at = %v, want about now", row.CreatedAt)
	}
}

func TestManifestChunks_CapsMirrorTheRegistrar(t *testing.T) {
	// internal/cluster/file_registrar.go: registrarDrainBatch and
	// registrarDrainChunkBytes. Change both together or not at all.
	if ManifestBatchOps != 1000 || ManifestBatchBytes != 256<<10 {
		t.Fatalf("caps = %d ops, %d bytes; want 1000 and %d", ManifestBatchOps, ManifestBatchBytes, 256<<10)
	}
	if manifestBatchOps != ManifestBatchOps || manifestBatchBytes != ManifestBatchBytes {
		t.Fatalf("effective caps = %d ops, %d bytes; want the constants", manifestBatchOps, manifestBatchBytes)
	}
	// The register overhead is the FileEntry wrapper (49 bytes) rounded up,
	// plus the real node ID; it must never be below the exact figure.
	if registerOpOverhead(0) < 49 || registerOpOverhead(253) != registerOpBaseOverhead+253 {
		t.Fatalf("registerOpOverhead = %d/%d, want at least 49 and base plus the node ID", registerOpOverhead(0), registerOpOverhead(253))
	}
}

// Each cap binds on its own: the count cap on synthetic sizes, the byte cap
// on long paths long before 1000 entries and on realistic entries at a few
// hundred. Every chunk respects both caps and the chunks cover the input in
// order.
func TestManifestChunks_CountAndBytesBindIndependently(t *testing.T) {
	overhead := registerOpOverhead(len(fakeNodeID))
	check := func(t *testing.T, files []ManifestFile) [][2]int {
		t.Helper()
		chunks := manifestChunks(len(files), func(i int) int { return registerOpBytes(files[i], overhead) })
		next := 0
		for _, c := range chunks {
			if c[0] != next || c[1] <= c[0] {
				t.Fatalf("chunk %v does not continue from %d", c, next)
			}
			next = c[1]
			if n := c[1] - c[0]; n > manifestBatchOps {
				t.Errorf("chunk %v has %d ops, cap %d", c, n, manifestBatchOps)
			}
			total := 0
			for i := c[0]; i < c[1]; i++ {
				total += registerOpBytes(files[i], overhead)
			}
			if total > manifestBatchBytes {
				t.Errorf("chunk %v carries %d payload bytes, cap %d", c, total, manifestBatchBytes)
			}
		}
		if next != len(files) {
			t.Fatalf("chunks cover %d of %d entries", next, len(files))
		}
		return chunks
	}

	t.Run("count binds", func(t *testing.T) {
		// A real ManifestFile is at least ~270 estimated bytes, so with the
		// real caps the byte cap binds first on every real entry (below);
		// the count cap is exercised with a synthetic size.
		chunks := manifestChunks(2500, func(int) int { return 1 })
		if fmt.Sprint(chunks) != "[[0 1000] [1000 2000] [2000 2500]]" {
			t.Errorf("chunks = %v, want [0,1000) [1000,2000) [2000,2500)", chunks)
		}
	})
	t.Run("bytes bind on realistic entries", func(t *testing.T) {
		files := make([]ManifestFile, 2500)
		for i := range files {
			files[i] = entryFor(fmt.Sprintf("db/cpu/2026/01/01/00/f%04d.parquet", i), []byte{byte(i)})
		}
		chunks := check(t, files)
		// Arc's own paths come to a few hundred entries per 256 KiB, which is
		// what the registrar's frame arithmetic assumes.
		if n := chunks[0][1]; n >= manifestBatchOps || n < 400 {
			t.Errorf("first chunk holds %d realistic entries, want a few hundred under the count cap", n)
		}
	})
	t.Run("bytes bind on long paths", func(t *testing.T) {
		long := "db/" + strings.Repeat("m", 400) + "/2026/01/01/00/" + strings.Repeat("f", 400)
		files := make([]ManifestFile, 600)
		for i := range files {
			files[i] = entryFor(fmt.Sprintf("%s%04d.parquet", long, i), []byte{byte(i)})
		}
		if registerOpBytes(files[0], overhead)*len(files) <= manifestBatchBytes {
			t.Fatal("fixture does not exceed the byte cap; the test proves nothing")
		}
		chunks := check(t, files)
		if len(chunks) < 2 {
			t.Fatalf("chunks = %v, want the byte cap to split 600 long entries", chunks)
		}
		for _, c := range chunks {
			if c[1]-c[0] >= manifestBatchOps {
				t.Errorf("chunk %v reached the count cap; bytes should bind first", c)
			}
		}
	})
	t.Run("an oversized single entry still ships alone", func(t *testing.T) {
		files := []ManifestFile{entryFor("db/m/2026/01/01/00/"+strings.Repeat("x", manifestBatchBytes)+".parquet", nil)}
		if chunks := manifestChunks(1, func(int) int { return registerOpBytes(files[0], overhead) }); len(chunks) != 1 {
			t.Errorf("chunks = %v, want one", chunks)
		}
	})
}

// seedClusterBackup takes a standalone backup of n data files plus one
// _schema anchor and returns what a cluster restore needs. The sidecar rows
// carry the manifest metadata only when the backup ran against a manifest;
// here they are path-derived, which is the standalone-backup-onto-cluster case.
func seedClusterBackup(t *testing.T, n int) (backupDir, backupID string, keys []string, contents map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	src := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	contents = make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("db/cpu/2026/09/13/00/f_%03d.parquet", i)
		b := bytes.Repeat([]byte{byte('a' + i%26)}, 16+i)
		mustWrite(t, src, k, b)
		keys = append(keys, k)
		contents[k] = b
	}
	mustWrite(t, src, "_schema/db/cpu/anchor.parquet", []byte("ANCHOR"))
	backupDir = t.TempDir()
	mgr, err := NewManager(&ManagerConfig{DataStorage: src, BackupPath: backupDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	res, err := mgr.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	return backupDir, res.Manifest.BackupID, keys, contents
}

func setBatchCaps(t *testing.T, ops, bytesCap int) {
	t.Helper()
	prevOps, prevBytes := manifestBatchOps, manifestBatchBytes
	manifestBatchOps, manifestBatchBytes = ops, bytesCap
	t.Cleanup(func() { manifestBatchOps, manifestBatchBytes = prevOps, prevBytes })
}

// A cluster restore registers every data file it writes, from the sidecar,
// in batches capped by count; the anchor is restored but neither registered
// nor reported to tiering; every data file is reported to tiering.
func TestRestore_ClusterRegistersFromSidecarInBatchesByCount(t *testing.T) {
	setBatchCaps(t, 4, 256<<10)
	backupDir, backupID, keys, contents := seedClusterBackup(t, 10)
	cm := &fakeClusterManifest{}
	tr := &fakeTierRecorder{}
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	m := newClusterManager(t, dest, backupDir, cm, tr)

	if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	p := m.GetProgress()
	if p.Status != "completed" || p.Mode != RestoreModeMerge || p.FilesRegistered != 10 || p.RegistrationFailed != 0 || p.SidecarMismatches != 0 {
		t.Fatalf("status=%s mode=%s registered=%d failed=%d mismatches=%d, want completed merge 10 0 0", p.Status, p.Mode, p.FilesRegistered, p.RegistrationFailed, p.SidecarMismatches)
	}
	sizes := make([]int, len(cm.registers))
	for i, b := range cm.registers {
		sizes[i] = len(b)
	}
	if fmt.Sprint(sizes) != "[4 4 2]" {
		t.Errorf("register batch sizes = %v, want [4 4 2]", sizes)
	}
	if got := cm.registeredPaths(); strings.Join(got, ",") != strings.Join(keys, ",") {
		t.Errorf("registered paths = %v, want %v in order", got, keys)
	}
	for _, b := range cm.registers {
		for _, e := range b {
			if e.SHA256 != shaOf(contents[e.Path]) || e.SizeBytes != int64(len(contents[e.Path])) || e.CreatedAt.IsZero() || e.Database != "db" || e.Measurement != "cpu" {
				t.Errorf("registered entry %+v does not match the sidecar/bytes", e)
			}
		}
	}
	if len(cm.deletes) != 0 {
		t.Errorf("merge mode issued %d manifest deletes", len(cm.deletes))
	}
	if !existsIn(t, dest, "_schema/db/cpu/anchor.parquet") {
		t.Error("the _schema anchor was not restored")
	}
	if len(tr.reports) != 10 {
		t.Fatalf("tier reports = %d, want 10 (the anchor is not reported)", len(tr.reports))
	}
	for _, r := range tr.reports {
		if r.size != int64(len(contents[r.path])) {
			t.Errorf("tier report %+v has the wrong size", r)
		}
	}
}

// The byte cap binds too: with it set to three rows, ten files register in
// batches of three.
func TestRestore_ClusterRegistersInBatchesByBytes(t *testing.T) {
	backupDir, backupID, _, _ := seedClusterBackup(t, 10)
	m0 := &Manager{backupStorage: mustLocalBackend(t, backupDir, zerolog.Nop()), logger: zerolog.Nop()}
	entries, ok, err := m0.readSidecar(context.Background(), m0.defaultDestination(), backupID)
	if err != nil || !ok {
		t.Fatalf("sidecar: ok=%v err=%v", ok, err)
	}
	// Rows differ by a few bytes of size; the largest times three fits, the
	// smallest times four does not.
	overhead := registerOpOverhead(len(fakeNodeID))
	largest, smallest := 0, int(^uint(0)>>1)
	for _, e := range entries {
		sz := registerOpBytes(e, overhead)
		if sz > largest {
			largest = sz
		}
		if sz < smallest {
			smallest = sz
		}
	}
	if smallest*4 <= largest*3 {
		t.Fatalf("fixture rows vary too much (min %d, max %d) for a clean cap", smallest, largest)
	}
	setBatchCaps(t, 1000, largest*3)

	cm := &fakeClusterManifest{}
	m := newClusterManager(t, mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupDir, cm, nil)
	if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	sizes := make([]int, len(cm.registers))
	for i, b := range cm.registers {
		sizes[i] = len(b)
	}
	if fmt.Sprint(sizes) != "[3 3 3 1]" {
		t.Errorf("register batch sizes = %v, want [3 3 3 1]", sizes)
	}
}

// A refused batch aborts the restore where it stands: the batch's files are
// written and reported as unregistered, the batch before it is registered,
// and nothing after it is written.
func TestRestore_ClusterRegistrationFailureAbortsAndReportsPaths(t *testing.T) {
	setBatchCaps(t, 4, 256<<10)
	backupDir, backupID, keys, _ := seedClusterBackup(t, 10)
	cm := &fakeClusterManifest{failRegisterCall: 2}
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	m := newClusterManager(t, dest, backupDir, cm, nil)

	_, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true})
	if err == nil || !strings.Contains(err.Error(), "refused to register 4") {
		t.Fatalf("err = %v, want an abort naming 4 unregistered files", err)
	}
	p := m.GetProgress()
	if p.Status != "failed" || p.FilesRegistered != 4 || p.RegistrationFailed != 4 {
		t.Errorf("status=%s registered=%d failed=%d, want failed 4 4", p.Status, p.FilesRegistered, p.RegistrationFailed)
	}
	if strings.Join(p.RegistrationFailedSample, ",") != strings.Join(keys[4:8], ",") {
		t.Errorf("registration_failed_sample = %v, want %v", p.RegistrationFailedSample, keys[4:8])
	}
	if len(cm.registers) != 2 {
		t.Errorf("register calls = %d, want 2 (no further batch after a refusal)", len(cm.registers))
	}
	for _, k := range keys[:8] {
		if !existsIn(t, dest, k) {
			t.Errorf("%s should be written (its batch was attempted)", k)
		}
	}
	for _, k := range keys[8:] {
		if existsIn(t, dest, k) {
			t.Errorf("%s was written after the abort", k)
		}
	}
}

// A backup object that does not match its sidecar row (a different hash at
// the same size, a different size, or no row at all) is detected before
// anything is written: the live copy stays, the file is counted and named,
// every other file is restored and registered, and the restore ends failed.
func TestRestore_ClusterSidecarMismatchSkipsBeforeWrite(t *testing.T) {
	ctx := context.Background()
	backupDir, backupID, keys, contents := seedClusterBackup(t, 4)
	dataDir := filepath.Join(backupDir, backupID, "data")
	// keys[0]: same size, different bytes. keys[1]: truncated. keys[2]: no row.
	corrupt := bytes.ToUpper(contents[keys[0]])
	if err := os.WriteFile(filepath.Join(dataDir, filepath.FromSlash(keys[0])), corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, filepath.FromSlash(keys[1])), contents[keys[1]][:4], 0o600); err != nil {
		t.Fatal(err)
	}
	sc := readSidecarFile(t, backupDir, backupID)
	kept := sc.Files[:0]
	for _, f := range sc.Files {
		if f.Path != keys[2] {
			kept = append(kept, f)
		}
	}
	sc.Files = kept
	writeSidecarFile(t, backupDir, backupID, sc)

	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	live := []byte("LIVE-GOOD-COPY")
	for _, k := range keys[:3] {
		mustWrite(t, dest, k, live)
	}
	cm := &fakeClusterManifest{}
	m := newClusterManager(t, dest, backupDir, cm, nil)

	_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true})
	if err == nil || !strings.Contains(err.Error(), "do not match its sidecar") {
		t.Fatalf("err = %v, want an incomplete restore naming the sidecar mismatches", err)
	}
	p := m.GetProgress()
	if p.Status != "failed" || p.SidecarMismatches != 3 || p.FilesRegistered != 1 || p.RegistrationFailed != 0 {
		t.Errorf("status=%s mismatches=%d registered=%d failed=%d, want failed 3 1 0", p.Status, p.SidecarMismatches, p.FilesRegistered, p.RegistrationFailed)
	}
	if strings.Join(p.SidecarMismatchSample, ",") != strings.Join(keys[:3], ",") {
		t.Errorf("sidecar_mismatch_sample = %v, want %v", p.SidecarMismatchSample, keys[:3])
	}
	for _, k := range keys[:3] {
		got, err := dest.Read(ctx, k)
		if err != nil || !bytes.Equal(got, live) {
			t.Errorf("%s: live copy = %q err %v, want untouched %q", k, got, err, live)
		}
	}
	if got, _ := dest.Read(ctx, keys[3]); !bytes.Equal(got, contents[keys[3]]) {
		t.Errorf("%s was not restored", keys[3])
	}
	if got := cm.registeredPaths(); strings.Join(got, ",") != keys[3] {
		t.Errorf("registered = %v, want only %s", got, keys[3])
	}
}

// A path the manifest already lists keeps the manifest's labels: database,
// measurement, partition time and created_at come from the existing entry,
// the hash and size from the sidecar.
func TestRestore_ClusterExistingEntryLabelsWin(t *testing.T) {
	backupDir, backupID, keys, contents := seedClusterBackup(t, 2)
	existing := ManifestFile{
		Path: keys[0], SHA256: shaOf([]byte("old")), SizeBytes: 3,
		Database: "spokedb", Measurement: "m2",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), CreatedAt: fixedCreatedAt,
	}
	cm := &fakeClusterManifest{entries: []ManifestFile{existing}}
	m := newClusterManager(t, mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupDir, cm, nil)
	if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if len(cm.registers) != 1 || len(cm.registers[0]) != 2 {
		t.Fatalf("registers = %v, want one batch of 2", cm.registers)
	}
	got := cm.registers[0][0]
	if got.Path != keys[0] || got.Database != "spokedb" || got.Measurement != "m2" || !got.PartitionTime.Equal(existing.PartitionTime) || !got.CreatedAt.Equal(fixedCreatedAt) {
		t.Errorf("registered %+v, want the existing entry labels", got)
	}
	if got.SHA256 != shaOf(contents[keys[0]]) || got.SizeBytes != int64(len(contents[keys[0]])) {
		t.Errorf("registered %+v, want the sidecar hash and size", got)
	}
	if other := cm.registers[0][1]; other.Database != "db" || other.Measurement != "cpu" {
		t.Errorf("unlisted path registered as %+v, want the sidecar labels db/cpu", other)
	}
}

// Replace mode: the current manifest entries of each restored database are
// removed before anything is written or registered, except the paths the
// restore itself writes (overwritten and re-registered instead), and entries
// of databases the backup does not hold are untouched. On a local backend the
// manager leaves the unlink to the delete workers; on a shared backend it
// removes the objects itself, after the manifest.
func TestRestore_ReplaceDeletesCurrentEntriesBeforeWriting(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%v", shared), func(t *testing.T) {
			backupDir, backupID, keys, _ := seedClusterBackup(t, 3)
			const (
				stale   = "db/cpu/2026/09/13/00/deleted-since-the-backup.parquet"
				stale2  = "db/mem/2026/09/13/00/other-measurement.parquet"
				foreign = "otherdb/x/2026/09/13/00/not-in-the-backup.parquet"
			)
			ctx := context.Background()
			local := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
			var dest storage.Backend = local
			if shared {
				dest = sharedTypeBackend{LocalBackend: local}
			}
			for _, k := range []string{stale, stale2, foreign, keys[0]} {
				mustWrite(t, dest, k, []byte("current"))
			}
			cm := &fakeClusterManifest{entries: []ManifestFile{
				entryFor(stale, []byte("current")), entryFor(stale2, []byte("current")),
				entryFor(foreign, []byte("current")), entryFor(keys[0], []byte("current")),
			}}
			m := newClusterManager(t, dest, backupDir, cm, nil)

			if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true, Mode: RestoreModeReplace}); err != nil {
				t.Fatalf("RestoreBackup: %v", err)
			}
			p := m.GetProgress()
			if p.Status != "completed" || p.Mode != RestoreModeReplace || p.ReplacedFiles != 2 || p.FilesRegistered != 3 {
				t.Fatalf("status=%s mode=%s replaced=%d registered=%d, want completed replace 2 3", p.Status, p.Mode, p.ReplacedFiles, p.FilesRegistered)
			}
			ops := cm.manifestOps()
			if len(ops) == 0 || ops[0] != "delete" || len(cm.deletes) != 1 {
				t.Fatalf("manifest ops = %v with %d delete calls, want one delete before every register", ops, len(cm.deletes))
			}
			for _, op := range ops[1:] {
				if op != "register" {
					t.Errorf("manifest ops = %v, want deletes strictly before registers", ops)
				}
			}
			if cm.reads != 1 {
				t.Errorf("ManifestFiles calls = %d, want 1 (one snapshot serves the labels and the replace half)", cm.reads)
			}
			if got := strings.Join(cm.deletes[0], ","); got != stale+","+stale2 {
				t.Errorf("deleted = %v, want [%s %s] (sorted; not the foreign database, not a path the restore writes)", cm.deletes[0], stale, stale2)
			}
			if cm.reasons[0] != restoreReplaceReason {
				t.Errorf("reason = %q, want %q", cm.reasons[0], restoreReplaceReason)
			}
			if got := cm.registeredPaths(); strings.Join(got, ",") != strings.Join(keys, ",") {
				t.Errorf("registered = %v, want %v", got, keys)
			}
			ok := existsIn(t, dest, stale)
			if shared && ok {
				t.Error("shared backend: the manifest-deleted object is still in storage; the issuer must remove it")
			}
			if !shared && !ok {
				t.Error("local backend: the manager unlinked the file itself; that is the delete workers job")
			}
			if !existsIn(t, dest, foreign) {
				t.Error("a database the backup does not hold lost a file")
			}
		})
	}
}

// sharedTypeBackend is a local backend that reports a shared type, the way
// the coordinator's delete callback tells the two apart.
type sharedTypeBackend struct {
	*storage.LocalBackend
}

func (sharedTypeBackend) Type() string { return "s3" }

// Replace refuses to delete anything when the backup is known incomplete,
// when too large a share of the listed data files was unregistered at backup
// time (a stale or partial manifest view), and when the manifest delete
// itself is refused nothing is written.
func TestRestore_ReplaceRefusesIncompleteBackupAndAbortsOnDeleteFailure(t *testing.T) {
	ctx := context.Background()
	refuse := func(t *testing.T, edit func(*Manifest), want string, n int) {
		t.Helper()
		backupDir, backupID, _, _ := seedClusterBackup(t, n)
		rewriteManifest(t, backupDir, backupID, edit)
		cm := &fakeClusterManifest{entries: []ManifestFile{entryFor("db/cpu/2026/09/13/00/stale.parquet", []byte("x"))}}
		dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		m := newClusterManager(t, dest, backupDir, cm, nil)
		_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true, Mode: RestoreModeReplace})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want a refusal containing %q", err, want)
		}
		if len(cm.deletes) != 0 || len(cm.registers) != 0 || m.GetProgress().ProcessedFiles != 0 {
			t.Errorf("deletes=%d registers=%d processed=%d, want nothing done", len(cm.deletes), len(cm.registers), m.GetProgress().ProcessedFiles)
		}
	}
	t.Run("incomplete backup", func(t *testing.T) {
		refuse(t, func(m *Manifest) { m.TotalFiles++; m.SkippedFiles = 1 }, "incomplete", 2)
	})
	t.Run("large unregistered share", func(t *testing.T) {
		// 10 listed and copied + 2 unregistered: 2 of 12 is over maxSkipRatio.
		refuse(t, func(m *Manifest) { m.UnregisteredSkipped = 2 }, "stale or partial manifest view", 10)
	})
	t.Run("small unregistered share is allowed", func(t *testing.T) {
		backupDir, backupID, _, _ := seedClusterBackup(t, 10)
		rewriteManifest(t, backupDir, backupID, func(m *Manifest) { m.UnregisteredSkipped = 1 }) // 1 of 11
		cm := &fakeClusterManifest{entries: []ManifestFile{entryFor("db/cpu/2026/09/13/00/stale.parquet", []byte("x"))}}
		m := newClusterManager(t, mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupDir, cm, nil)
		if _, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true, Mode: RestoreModeReplace}); err != nil {
			t.Fatalf("RestoreBackup: %v", err)
		}
		if m.GetProgress().ReplacedFiles != 1 {
			t.Errorf("replaced = %d, want 1", m.GetProgress().ReplacedFiles)
		}
	})
	t.Run("manifest delete refused", func(t *testing.T) {
		backupDir, backupID, keys, _ := seedClusterBackup(t, 2)
		cm := &fakeClusterManifest{failDelete: true, entries: []ManifestFile{entryFor("db/cpu/2026/09/13/00/stale.parquet", []byte("x"))}}
		dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		m := newClusterManager(t, dest, backupDir, cm, nil)
		_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true, Mode: RestoreModeReplace})
		if err == nil || !strings.Contains(err.Error(), "before any file was written") {
			t.Fatalf("err = %v, want an abort before any write", err)
		}
		if existsIn(t, dest, keys[0]) {
			t.Error("a file was written after the manifest delete was refused")
		}
		if len(cm.registers) != 0 {
			t.Error("registers were attempted after the manifest delete was refused")
		}
	})
}

// The manager-level refusals: metadata or config on a cluster node, replace
// without a cluster, an unknown mode, and a cluster restore of a backup with
// no sidecar. None of them writes anything.
func TestRestore_Refusals(t *testing.T) {
	ctx := context.Background()
	backupDir, backupID, keys, _ := seedClusterBackup(t, 2)
	cases := []struct {
		name    string
		cluster bool
		opts    RestoreOptions
		want    string
	}{
		{"metadata on a cluster node", true, RestoreOptions{BackupID: backupID, RestoreData: true, RestoreMetadata: true}, "refused on a cluster node"},
		{"config on a cluster node", true, RestoreOptions{BackupID: backupID, RestoreData: true, RestoreConfig: true}, "refused on a cluster node"},
		{"replace without a cluster", false, RestoreOptions{BackupID: backupID, RestoreData: true, Mode: RestoreModeReplace}, "only available on a cluster node"},
		{"unknown mode", false, RestoreOptions{BackupID: backupID, RestoreData: true, Mode: "overwrite"}, "invalid restore mode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
			var m *Manager
			if tc.cluster {
				m = newClusterManager(t, dest, backupDir, &fakeClusterManifest{}, nil)
			} else {
				m = newClusterManager(t, dest, backupDir, nil, nil)
			}
			_, err := m.RestoreBackup(ctx, tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
			if p := m.GetProgress(); p.Status != "failed" || p.ProcessedFiles != 0 {
				t.Errorf("status=%s processed=%d, want failed 0", p.Status, p.ProcessedFiles)
			}
			if existsIn(t, dest, keys[0]) {
				t.Error("a refused restore wrote a file")
			}
		})
	}
	t.Run("cluster restore of a backup without a sidecar", func(t *testing.T) {
		if err := os.Remove(filepath.Join(backupDir, backupID, sidecarName)); err != nil {
			t.Fatal(err)
		}
		dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
		m := newClusterManager(t, dest, backupDir, &fakeClusterManifest{}, nil)
		_, err := m.RestoreBackup(ctx, RestoreOptions{BackupID: backupID, RestoreData: true})
		if err == nil || !strings.Contains(err.Error(), "has no "+sidecarName) {
			t.Fatalf("err = %v, want a refusal naming the missing sidecar", err)
		}
		if existsIn(t, dest, keys[0]) {
			t.Error("a file was written before the sidecar check")
		}
	})
}

// Standalone with tiering: every data file written is reported, the anchor
// is not, and nothing else changes (no manifest hook, no sidecar needed).
func TestRestore_StandaloneTierRecorderPerDataFile(t *testing.T) {
	backupDir, backupID, keys, contents := seedClusterBackup(t, 5)
	tr := &fakeTierRecorder{}
	m := newClusterManager(t, mustLocalBackend(t, t.TempDir(), zerolog.Nop()), backupDir, nil, tr)
	if _, err := m.RestoreBackup(context.Background(), RestoreOptions{BackupID: backupID, RestoreData: true}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if len(tr.reports) != len(keys) {
		t.Fatalf("tier reports = %d, want %d", len(tr.reports), len(keys))
	}
	for i, r := range tr.reports {
		if r.path != keys[i] || r.size != int64(len(contents[keys[i]])) {
			t.Errorf("report %d = %+v, want %s/%d", i, r, keys[i], len(contents[keys[i]]))
		}
	}
	if p := m.GetProgress(); p.FilesRegistered != 0 || p.Status != "completed" {
		t.Errorf("standalone restore registered %d files, status %s", p.FilesRegistered, p.Status)
	}
}

// With no hooks wired a backup taken before the sidecar existed restores as
// it always did.
func TestRestore_StandaloneDoesNotNeedTheSidecar(t *testing.T) {
	backupDir, backupID, keys, _ := seedClusterBackup(t, 3)
	if err := os.Remove(filepath.Join(backupDir, backupID, sidecarName)); err != nil {
		t.Fatal(err)
	}
	dest := mustLocalBackend(t, t.TempDir(), zerolog.Nop())
	m, err := restoreInto(t, mustLocalBackend(t, backupDir, zerolog.Nop()), dest, backupID)
	if err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if p := m.GetProgress(); p.Status != "completed" || p.ProcessedFiles != 4 {
		t.Errorf("status=%s processed=%d, want completed 4 (3 data files + anchor)", p.Status, p.ProcessedFiles)
	}
	for _, k := range keys {
		if !existsIn(t, dest, k) {
			t.Errorf("%s not restored", k)
		}
	}
}

// Nil hooks are ignored by the setters, so a caller may pass a nil interface
// unconditionally and get standalone behaviour.
func TestSetters_IgnoreNil(t *testing.T) {
	m := &Manager{logger: zerolog.Nop()}
	m.SetClusterManifest(nil)
	m.SetTierRecorder(nil)
	if m.cluster != nil || m.tierRecorder != nil {
		t.Error("nil hooks were stored")
	}
}

func TestIsRegistrableDataFile(t *testing.T) {
	cases := map[string]bool{
		"db/cpu/2026/01/01/00/f.parquet":             true,
		"db/metadata/2026/01/01/00/f.parquet":        true, // a measurement named metadata holds data
		"_schema/db/cpu/anchor.parquet":              false,
		"_compaction_state/hourly/db/job.json":       false,
		".edge/db/cpu/2026/01/01/00/f.parquet":       false,
		"arc_db.db/cpu/metadata/v1.metadata.json":    false,
		"db/cpu/2026/01/01/00/f.parquet.part":        false,
		"lonely.parquet":                             false,
		"db/":                                        false,
		"spoke/db/cpu/2026/01/01/00/f.parquet":       true,
		"db\\cpu\\2026\\01\\01\\00\\windows.parquet": filepath.Separator == '\\',
	}
	for p, want := range cases {
		if got := isRegistrableDataFile(p); got != want {
			t.Errorf("isRegistrableDataFile(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestPartitionTimeFromPath(t *testing.T) {
	cases := map[string]time.Time{
		"db/cpu/2026/09/13/07/f.parquet":            time.Date(2026, 9, 13, 7, 0, 0, 0, time.UTC),
		"db/cpu/2026/09/13/07/sub/f.parquet":        time.Date(2026, 9, 13, 7, 0, 0, 0, time.UTC),
		"db/cpu/2026/02/30/07/f.parquet":            {},
		"db/cpu/2026/13/01/07/f.parquet":            {},
		"db/cpu/2026/09/13/24/f.parquet":            {},
		"db/cpu/26/09/13/07/f.parquet":              {},
		"db/cpu/year/09/13/07/f.parquet":            {},
		"db/cpu/f.parquet":                          {},
		"_schema/db/cpu/anchor.parquet":             {},
		"db/cpu/2026/09/13/07/2026/09/13/f.parquet": time.Date(2026, 9, 13, 7, 0, 0, 0, time.UTC),
	}
	for p, want := range cases {
		if got := partitionTimeFromPath(p); !got.Equal(want) {
			t.Errorf("partitionTimeFromPath(%q) = %v, want %v", p, got, want)
		}
	}
}
