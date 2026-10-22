package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
)

// #1236: a database DELETE removes a parked file only when everything proves
// the file's rows are the deleted database's. A parked file can be the only
// copy of acknowledged rows, so each guard below preserves it otherwise.

func parkedOnly(t *testing.T) (dir, base string, r *RegistrationOutbox, m *takeoverManifest, l *settleLog) {
	t.Helper()
	dir, base, r, m, l = parkingRig(t)
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	if got := l.wait(t, outPathA); got != ingest.CoverageRefused {
		t.Fatalf("parked outcome = %v", got)
	}
	return dir, base, r, m, l
}

func neverRemove(t *testing.T) func(context.Context, string) error {
	return func(_ context.Context, p string) error {
		t.Errorf("removed parked file %s", p)
		return nil
	}
}

func removeFrom(base string) func(context.Context, string) error {
	return func(_ context.Context, p string) error {
		return os.Remove(filepath.Join(base, filepath.FromSlash(p)))
	}
}

func assertPreserved(t *testing.T, r *RegistrationOutbox, base string) {
	t.Helper()
	if !r.IsParked(outPathA) || !exists(base, "_refused/"+outPathA) {
		t.Fatal("parked data was not preserved")
	}
}

func dbRecord(id uint64, mut func(*raft.DeleteRecord)) []raft.DeleteRecord {
	rec := raft.DeleteRecord{ID: id, Database: "db", Before: []raft.RecordScope{{Instance: 9}}}
	if mut != nil {
		mut(&rec)
	}
	return []raft.DeleteRecord{rec}
}

// No durable record, no proof: the delete is refused and the file stays.
func TestParkedDeleteNeedsADurableRecord(t *testing.T) {
	_, base, r, _, _ := parkedOnly(t)
	r.SetDeleteRecordHooks((&recordHooks{instance: 1, recs: dbRecord(40, nil), removeFn: neverRemove(t)}).hooks())
	if err := r.WithdrawDatabase(context.Background(), "db", 0); err == nil {
		t.Fatal("withdrew a parked file with no durable delete record")
	}
	assertPreserved(t, r, base)
}

// The record must be the deletion of this whole database: another database,
// one measurement, or a WHERE does not cover every row of the file.
func TestParkedDeleteNeedsTheWholeDatabaseRecord(t *testing.T) {
	for name, mut := range map[string]func(*raft.DeleteRecord){
		"other database":  func(r *raft.DeleteRecord) { r.Database = "other" },
		"one measurement": func(r *raft.DeleteRecord) { r.Measurement = "cpu" },
		"row predicate":   func(r *raft.DeleteRecord) { r.Where = "v > 0" },
	} {
		t.Run(name, func(t *testing.T) {
			_, base, r, _, _ := parkedOnly(t)
			r.SetDeleteRecordHooks((&recordHooks{instance: 1, recs: dbRecord(40, mut), removeFn: neverRemove(t)}).hooks())
			if err := r.WithdrawDatabase(context.Background(), "db", 40); err == nil {
				t.Fatal("withdrew a parked file under a record that does not delete the whole database")
			}
			assertPreserved(t, r, base)
		})
	}
}

// A registration still in flight for the parked path settles first.
func TestParkedDeleteWaitsForAnInFlightRegistration(t *testing.T) {
	_, base, r, _, _ := parkedOnly(t)
	r.mu.Lock()
	r.byPath[outPathA] = &outboxEntry{}
	r.mu.Unlock()
	r.SetDeleteRecordHooks((&recordHooks{instance: 1, recs: dbRecord(40, nil), removeFn: neverRemove(t)}).hooks())
	if err := r.WithdrawDatabase(context.Background(), "db", 40); err == nil {
		t.Fatal("withdrew a parked file whose registration is still in flight")
	}
	assertPreserved(t, r, base)
}

// Without batch coverage the delete scope cannot be checked.
func TestParkedDeleteNeedsCoverage(t *testing.T) {
	_, base, r, _, _ := parkedOnly(t)
	r.mu.Lock()
	r.parked[outPathA].rec.Rec.Cov = nil
	r.mu.Unlock()
	r.SetDeleteRecordHooks((&recordHooks{instance: 1, recs: dbRecord(40, nil), removeFn: neverRemove(t)}).hooks())
	if err := r.WithdrawDatabase(context.Background(), "db", 40); err == nil {
		t.Fatal("withdrew a parked file with no batch coverage")
	}
	assertPreserved(t, r, base)
}

// A completed withdrawal settles the file's WAL coverage as deleted and is
// durable: after a restart the file is no longer parked.
func TestParkedDeleteSettlesAndSurvivesARestart(t *testing.T) {
	dir, base, r, _, l := parkedOnly(t)
	r.SetDeleteRecordHooks((&recordHooks{instance: 1, recs: dbRecord(40, nil), removeFn: removeFrom(base)}).hooks())
	if err := r.WithdrawDatabase(context.Background(), "db", 40); err != nil {
		t.Fatal(err)
	}
	if got := l.outcomes(outPathA); len(got) != 2 || got[1] != ingest.CoverageDeleted {
		t.Fatalf("coverage outcomes = %v, want refused then deleted", got)
	}
	crash(r)
	r = newTestOutbox(t, dir)
	r.SetLocalParking(LocalParking{Base: base})
	if r.IsParked(outPathA) {
		t.Fatal("a completed withdrawal came back as parked after a restart")
	}
}

// A withdrawal interrupted after its intent was written finishes on its own,
// from the parked-file resolver, with no second DELETE request.
func TestParkedDeleteFinishesFromTheResolverAfterRestart(t *testing.T) {
	dir, base, r, m, _ := parkedOnly(t)
	attempts := 0
	h := &recordHooks{instance: 1, recs: dbRecord(40, nil), removeFn: func(_ context.Context, p string) error {
		attempts++
		if attempts == 1 {
			return os.ErrPermission // interrupted: the intent is on disk, the file is not removed
		}
		return os.Remove(filepath.Join(base, filepath.FromSlash(p)))
	}}
	r.SetDeleteRecordHooks(h.hooks())
	if err := r.WithdrawDatabase(context.Background(), "db", 40); err == nil {
		t.Fatal("expected the interrupted removal to fail")
	}
	crash(r)
	r = newTestOutbox(t, dir)
	r.SetLocalParking(LocalParking{Base: base})
	r.SetDeleteRecordHooks(h.hooks())
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}
	r.resolveParked(context.Background(), m, m, store)
	if r.IsParked(outPathA) || exists(base, "_refused/"+outPathA) {
		t.Fatalf("resolver did not finish the withdrawal: parked=%v file=%v", r.IsParked(outPathA), exists(base, "_refused/"+outPathA))
	}
}

// With another database's file still parked, the journal is not compacted
// after the withdrawal, so only the completion record keeps the withdrawn file
// from coming back as parked after a restart.
func TestParkedDeleteCompletionIsJournaledBesideOtherParkedFiles(t *testing.T) {
	dir, base := t.TempDir(), t.TempDir()
	r := newTestOutbox(t, dir)
	r.SetLocalParking(LocalParking{Base: base})
	const keepPath = "keep/cpu/2026/10/11/10/cpu_k.parquet"
	writeData(t, base, outPathA, 10)
	writeData(t, base, keepPath, 10)
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	keepCov := ingest.NewGroupCoverage("g2", "keep/cpu", 1, l.record)
	keepCov.Ranges = []ingest.CoverageRange{{Instance: 9, Lo: 8, Hi: 9}}
	r.RegisterCoveredFile("keep", "cpu", keepPath, time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC), 10, "abc", keepCov)
	m := &takeoverManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	m.refuse = func(p string) bool { return p == outPathA || p == keepPath }
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, keepPath: 10}}, "writer-1")
	l.wait(t, outPathA)
	l.wait(t, keepPath)

	r.SetDeleteRecordHooks((&recordHooks{instance: 1, recs: dbRecord(40, nil), removeFn: removeFrom(base)}).hooks())
	if err := r.WithdrawDatabase(context.Background(), "db", 40); err != nil {
		t.Fatal(err)
	}
	crash(r)
	r = newTestOutbox(t, dir)
	r.SetLocalParking(LocalParking{Base: base})
	if r.IsParked(outPathA) {
		t.Fatal("the withdrawn file came back as parked after a restart")
	}
	if !r.IsParked(keepPath) {
		t.Fatal("the other database's parked file was lost")
	}
}
