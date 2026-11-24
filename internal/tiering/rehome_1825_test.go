package tiering

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1825: a re-home job owns the file it replaces and the files replacing it.
// Tiering leaves them on the hot tier, on a standalone node (no manifest).

type owned1825 struct {
	mu    sync.Mutex
	paths map[string]bool
}

func (o *owned1825) set(path string, v bool) {
	o.mu.Lock()
	o.paths[path] = v
	o.mu.Unlock()
}

func (o *owned1825) owns(path string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.paths[path]
}

func standaloneRig1825(t *testing.T, files ...string) (*Manager, *hookHot, *hookCold, *owned1825) {
	t.Helper()
	m, hot, cold, _ := moveRig(t, files...)
	m.clusterGate = nil
	if m.manifest != nil {
		t.Fatal("the rig has a manifest: this is the standalone case")
	}
	o := &owned1825{paths: map[string]bool{}}
	m.SetOwnedFilter(o.owns)
	return m, hot, cold, o
}

func TestRehome1825_AnOwnedFileIsNotACandidate(t *testing.T) {
	m, _, _, o := standaloneRig1825(t, gateDailyA, gateDailyB)
	ctx := context.Background()
	names := func() map[string]bool {
		cands, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, c := range cands {
			out[c.Path] = true
		}
		return out
	}
	if got := names(); !got[gateDailyA] || !got[gateDailyB] {
		t.Fatalf("with no job, candidates = %v; the rig's files are not candidates", got)
	}
	o.set(gateDailyA, true)
	if got := names(); got[gateDailyA] || !got[gateDailyB] {
		t.Fatalf("candidates = %v; want the owned file left out and the other kept", got)
	}
	o.set(gateDailyA, false)
	if got := names(); !got[gateDailyA] {
		t.Fatalf("candidates = %v; a file is a candidate again once the job ends", got)
	}
}

// Owned between its selection and its copy: nothing is copied, and it is not
// a failure of the cycle.
func TestRehome1825_OwnedBeforeTheCopyIsLeftAlone(t *testing.T) {
	m, hot, cold, o := standaloneRig1825(t, gateDailyA, gateDailyB)
	ctx := context.Background()
	o.set(gateDailyA, true)
	migrated, failed := m.migrator.MigrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if migrated != 1 || failed != 0 {
		t.Fatalf("MigrateBatch = (%d migrated, %d failed), want (1, 0): an owned file is not a failure", migrated, failed)
	}
	if coldHas(t, cold, gateDailyA) {
		t.Fatal("an owned file was copied to cold storage")
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok || fileMeta(t, m, gateDailyA).Tier != TierHot {
		t.Fatal("an owned file is not hot any more")
	}
	if !coldHas(t, cold, gateDailyB) {
		t.Fatal("the file no job owns was not migrated")
	}
}

// A skipped file's claim is given back at once, not when the batch ends: its
// owner may be asking about that very file while the batch copies others.
func TestRehome1825_ASkippedFileIsNotHeldForTheBatch(t *testing.T) {
	m, _, cold, o := standaloneRig1825(t, gateDailyA, gateDailyB)
	ctx := context.Background()
	m.migrator.maxConcurrent = 1
	o.set(gateDailyA, true)
	var during error
	asked := false
	cold.onWrite = func(p string) {
		if p == gateDailyB {
			asked = true
			during = m.CheckRehomeSource(ctx, gateDailyA)
		}
	}
	m.migrator.MigrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if !asked {
		t.Fatal("the other file was never copied: the check did not run during the batch")
	}
	if during != nil {
		t.Fatalf("a job asking about its own file while the batch copies another was refused: %v", during)
	}
}

// Owned while it is copied: the copy is removed and nothing flips.
func TestRehome1825_OwnedDuringTheCopyLosesItsColdCopy(t *testing.T) {
	m, hot, cold, o := standaloneRig1825(t, gateDailyA)
	ctx := context.Background()
	cold.onWrite = func(p string) {
		if p == gateDailyA {
			o.set(p, true)
		}
	}
	migrated, failed := m.migrator.MigrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA)})
	if migrated != 0 || failed != 1 {
		t.Fatalf("MigrateBatch = (%d migrated, %d failed), want (0, 1): a copy made and thrown away is reported", migrated, failed)
	}
	if coldHas(t, cold, gateDailyA) {
		t.Fatal("a file a job took over during its copy kept the copy")
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok || fileMeta(t, m, gateDailyA).Tier != TierHot {
		t.Fatal("the file is not hot any more")
	}
}

func TestCheckRehomeSource1825(t *testing.T) {
	const hourly = "db1/cpu/2024/03/15/10/cpu_20240315_100000.parquet"
	ctx := context.Background()

	t.Run("a free hot file", func(t *testing.T) {
		m, _, _, _ := standaloneRig1825(t, gateDailyA)
		if err := m.CheckRehomeSource(ctx, gateDailyA); err != nil {
			t.Fatal(err)
		}
		// It holds no claim afterwards: ownership is what keeps tiering off.
		if !m.claims.claimMove(gateDailyA) {
			t.Fatal("the check kept a claim")
		}
	})
	t.Run("a file being moved", func(t *testing.T) {
		m, _, _, _ := standaloneRig1825(t, gateDailyA)
		m.claims.claimMove(gateDailyA)
		if err := m.CheckRehomeSource(ctx, gateDailyA); !errors.Is(err, ErrFileMoving) {
			t.Fatalf("err = %v, want ErrFileMoving", err)
		}
	})
	t.Run("a copy in cold storage, whatever the file's name", func(t *testing.T) {
		m, hot, cold, _ := standaloneRig1825(t, gateDailyA)
		mustWrite(t, hot.mockBackend, hourly)
		for _, p := range []string{gateDailyA, hourly} {
			mustWrite(t, cold.mockBackend, p)
			if err := m.CheckRehomeSource(ctx, p); !errors.Is(err, ErrRehomeColdCopy) {
				t.Fatalf("%s: err = %v, want ErrRehomeColdCopy", p, err)
			}
		}
	})
	t.Run("a row that says cold", func(t *testing.T) {
		m, _, _, _ := standaloneRig1825(t, gateDailyA)
		if err := m.metadata.UpdateTier(ctx, gateDailyA, TierCold); err != nil {
			t.Fatal(err)
		}
		if err := m.CheckRehomeSource(ctx, gateDailyA); !errors.Is(err, ErrRehomeColdCopy) {
			t.Fatalf("err = %v, want ErrRehomeColdCopy", err)
		}
	})
	t.Run("cold storage cannot be asked", func(t *testing.T) {
		m, _, cold, _ := standaloneRig1825(t, gateDailyA)
		cold.statErr = errors.New("cold unreachable")
		if err := m.CheckRehomeSource(ctx, gateDailyA); !errors.Is(err, ErrColdCheckFailed) {
			t.Fatalf("err = %v, want ErrColdCheckFailed", err)
		}
	})
	t.Run("cold storage enabled and never built", func(t *testing.T) {
		m, _, _, _ := standaloneRig1825(t, gateDailyA)
		m.coldBackend = nil
		if !m.ColdTierMissing() {
			t.Fatal("the rig is not the missing-cold case")
		}
		if err := m.CheckRehomeSource(ctx, gateDailyA); !errors.Is(err, ErrColdCheckFailed) {
			t.Fatalf("err = %v, want ErrColdCheckFailed", err)
		}
	})
	t.Run("no cold tier configured", func(t *testing.T) {
		m, _, _, _ := standaloneRig1825(t, gateDailyA)
		m.coldBackend = nil
		m.config.Cold.Enabled = false
		if err := m.CheckRehomeSource(ctx, gateDailyA); err != nil {
			t.Fatalf("with no cold tier nothing can hold a copy: %v", err)
		}
	})
}

func TestRehomedFileRows1825(t *testing.T) {
	const (
		out1 = "db1/cpu/2024/03/16/cpu_20261123_000001_daily.parquet"
		out2 = "db1/cpu/2024/03/17/04/cpu_20261123_000002.parquet"
	)
	m, _, _, _ := standaloneRig1825(t, gateDailyA)
	ctx := context.Background()
	if err := m.CanRecord(out1); err != nil {
		t.Fatal(err)
	}
	if err := m.CanRecord("db1/cpu/1969/12/31/23/x.parquet"); err == nil {
		t.Fatal("a directory before 1970 was accepted; the scan would never record a file there")
	}
	files := []RehomedFile{{Path: out1, SizeBytes: 11}, {Path: out2, SizeBytes: 22}}
	for i := 0; i < 2; i++ { // safe to repeat
		if err := m.RecordRehomedFiles(ctx, files); err != nil {
			t.Fatal(err)
		}
	}
	a, b := fileMeta(t, m, out1), fileMeta(t, m, out2)
	if a.Tier != TierHot || a.SizeBytes != 11 || a.Database != "db1" || a.Measurement != "cpu" ||
		a.PartitionTime.Format("2006-01-02T15") != "2024-03-16T00" || b.PartitionTime.Format("2006-01-02T15") != "2024-03-17T04" {
		t.Fatalf("rows: %+v %+v", a, b)
	}
	// A row that says cold is not made hot.
	if err := m.metadata.UpdateTier(ctx, out1, TierCold); err != nil {
		t.Fatal(err)
	}
	if err := m.RecordRehomedFiles(ctx, files); err != nil {
		t.Fatal(err)
	}
	if got := fileMeta(t, m, out1).Tier; got != TierCold {
		t.Fatalf("a cold row was made %s", got)
	}
	// Forgetting removes hot rows only.
	if err := m.ForgetRehomedFiles(ctx, []string{out1, out2, gateDailyA, "db1/cpu/2024/03/18/nosuch.parquet"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.metadata.GetFile(ctx, out2); got != nil {
		t.Fatal("a hot row was not forgotten")
	}
	if got, _ := m.metadata.GetFile(ctx, gateDailyA); got != nil {
		t.Fatal("the replaced file's hot row was not forgotten")
	}
	if got, _ := m.metadata.GetFile(ctx, out1); got == nil || got.Tier != TierCold {
		t.Fatal("a cold row was forgotten")
	}
}

func TestRehome1825_NilManager(t *testing.T) {
	var m *Manager
	ctx := context.Background()
	m.SetOwnedFilter(func(string) bool { return true })
	if m.CheckRehomeSource(ctx, "a") != nil || m.CanRecord("a") != nil || m.RecordRehomedFiles(ctx, nil) != nil || m.ForgetRehomedFiles(ctx, nil) != nil {
		t.Fatal("a nil manager refused something")
	}
}

// coldRig1825 has gateDailyA in cold storage only, with a cold row.
func coldRig1825(t *testing.T) (*Manager, *hookHot, *hookCold) {
	t.Helper()
	m, hot, cold, _ := standaloneRig1825(t, gateDailyA)
	ctx := context.Background()
	mustWrite(t, cold.mockBackend, gateDailyA)
	if err := hot.mockBackend.Delete(ctx, gateDailyA); err != nil {
		t.Fatal(err)
	}
	if err := m.metadata.UpdateTier(ctx, gateDailyA, TierCold); err != nil {
		t.Fatal(err)
	}
	return m, hot, cold
}

func TestCheckRehomeColdSource1825(t *testing.T) {
	ctx := context.Background()
	const rewrite = "db1/cpu/2024/03/15/cpu_20240315_daily_rewrite_1795000000000000000.parquet"

	t.Run("a file in cold storage only", func(t *testing.T) {
		m, _, _ := coldRig1825(t)
		size, store, err := m.CheckRehomeColdSource(ctx, gateDailyA)
		if err != nil || size != 7 || store == "" || store != m.ColdBackendIdentity() {
			t.Fatalf("size %d, store %q, err %v", size, store, err)
		}
	})
	for name, tc := range map[string]struct {
		break_ func(t *testing.T, m *Manager, hot *hookHot, cold *hookCold)
		want   error
	}{
		"no tier row": {func(t *testing.T, m *Manager, _ *hookHot, _ *hookCold) {
			if err := m.metadata.DeleteFile(ctx, gateDailyA); err != nil {
				t.Fatal(err)
			}
		}, ErrRehomeNotCold},
		"a row that says hot": {func(t *testing.T, m *Manager, _ *hookHot, _ *hookCold) {
			if err := m.metadata.UpdateTier(ctx, gateDailyA, TierHot); err != nil {
				t.Fatal(err)
			}
		}, ErrRehomeNotCold},
		"on both tiers": {func(t *testing.T, _ *Manager, hot *hookHot, _ *hookCold) {
			mustWrite(t, hot.mockBackend, gateDailyA)
		}, ErrRehomeNotCold},
		"a row with no object": {func(t *testing.T, _ *Manager, _ *hookHot, cold *hookCold) {
			if err := cold.mockBackend.Delete(ctx, gateDailyA); err != nil {
				t.Fatal(err)
			}
		}, ErrRehomeNotCold},
		"a DELETE rewrite beside it in cold storage": {func(t *testing.T, _ *Manager, _ *hookHot, cold *hookCold) {
			mustWrite(t, cold.mockBackend, rewrite)
		}, ErrRehomeNotCold},
		"a DELETE rewrite of it on the hot tier": {func(t *testing.T, _ *Manager, hot *hookHot, _ *hookCold) {
			mustWrite(t, hot.mockBackend, rewrite)
		}, ErrRehomeNotCold},
		"cold storage cannot be asked": {func(_ *testing.T, _ *Manager, _ *hookHot, cold *hookCold) {
			cold.statErr = errors.New("cold unreachable")
		}, ErrColdCheckFailed},
		"no cold tier": {func(_ *testing.T, m *Manager, _ *hookHot, _ *hookCold) {
			m.coldBackend = nil
		}, ErrColdCheckFailed},
	} {
		t.Run(name, func(t *testing.T) {
			m, hot, cold := coldRig1825(t)
			if storage.RewriteFamily(rewrite) != storage.RewriteFamily(gateDailyA) {
				t.Fatal("the test's rewrite name is not of the file's family")
			}
			tc.break_(t, m, hot, cold)
			if _, _, err := m.CheckRehomeColdSource(ctx, gateDailyA); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRemoveRehomedColdFile1825(t *testing.T) {
	ctx := context.Background()
	t.Run("the object, then the row, then the caches", func(t *testing.T) {
		m, _, cold := coldRig1825(t)
		store := m.ColdBackendIdentity()
		var notified, stale int
		m.SetOnMigrationComplete(func() { notified++ })
		m.SetOnReadCachesStale(func() {
			if stale++; stale == 1 && !coldHas(t, cold, gateDailyA) {
				t.Error("the read caches were first dropped after the object went, not before")
			}
		})
		if m.ColdRemovalRecent(time.Minute) {
			t.Fatal("a removal is reported before any ran")
		}
		if err := m.RemoveRehomedColdFile(ctx, gateDailyA, store); err != nil {
			t.Fatal(err)
		}
		if coldHas(t, cold, gateDailyA) {
			t.Fatal("the object is still in cold storage")
		}
		if row, _ := m.metadata.GetFile(ctx, gateDailyA); row != nil {
			t.Fatalf("the cold row was kept: %+v", row)
		}
		if notified != 1 || stale < 1 {
			t.Fatalf("notified %d, read caches dropped %d; want once, and at least once", notified, stale)
		}
		if !m.ColdRemovalRecent(time.Minute) || m.ColdRemovalRecent(0) {
			t.Fatal("a removal that just ended is not reported as recent, or is reported with no window")
		}
		// Safe to repeat: the object and the row are gone.
		if err := m.RemoveRehomedColdFile(ctx, gateDailyA, store); err != nil {
			t.Fatalf("a second removal: %v", err)
		}
	})
	t.Run("the object already gone: the row is still retired", func(t *testing.T) {
		m, _, cold := coldRig1825(t)
		if err := cold.mockBackend.Delete(ctx, gateDailyA); err != nil {
			t.Fatal(err)
		}
		if err := m.RemoveRehomedColdFile(ctx, gateDailyA, m.ColdBackendIdentity()); err != nil {
			t.Fatal(err)
		}
		if row, _ := m.metadata.GetFile(ctx, gateDailyA); row != nil {
			t.Fatal("the row of an object that was already gone was kept")
		}
	})
	t.Run("another cold store than the one recorded: nothing is touched", func(t *testing.T) {
		m, _, cold := coldRig1825(t)
		err := m.RemoveRehomedColdFile(ctx, gateDailyA, "s3://another-bucket/")
		if !errors.Is(err, ErrColdStoreChanged) {
			t.Fatalf("err = %v, want ErrColdStoreChanged", err)
		}
		if !coldHas(t, cold, gateDailyA) || fileMeta(t, m, gateDailyA).Tier != TierCold {
			t.Fatal("the file or its row was touched")
		}
		if _, err := m.ColdSourceStat(ctx, gateDailyA, "s3://another-bucket/"); !errors.Is(err, ErrColdStoreChanged) {
			t.Fatalf("ColdSourceStat err = %v, want ErrColdStoreChanged", err)
		}
	})
	t.Run("cold storage cannot confirm: the row is kept", func(t *testing.T) {
		m, _, cold := coldRig1825(t)
		cold.statErr = errors.New("cold unreachable")
		if err := m.RemoveRehomedColdFile(ctx, gateDailyA, m.ColdBackendIdentity()); err == nil {
			t.Fatal("a removal cold storage could not confirm was reported done")
		}
		if fileMeta(t, m, gateDailyA).Tier != TierCold {
			t.Fatal("the row was retired though cold storage could not say the object is gone")
		}
	})
}

// stickyCold is a cold store that says a delete worked and keeps the object.
type stickyCold struct{ *hookCold }

func (s stickyCold) Delete(context.Context, string) error { return nil }

// A removal is done when cold storage says the object is gone, not when the
// delete returned: a surviving object is recorded again by the next cold
// sync, and its rows are read beside the files that replaced it.
func TestRemoveRehomedColdFile1825_TheObjectSurvivesItsDelete(t *testing.T) {
	m, _, cold := coldRig1825(t)
	store := m.ColdBackendIdentity()
	m.coldBackend = stickyCold{cold}
	if err := m.RemoveRehomedColdFile(context.Background(), gateDailyA, store); err == nil {
		t.Fatal("a removal that left the object in cold storage was reported done")
	}
	if fileMeta(t, m, gateDailyA).Tier != TierCold {
		t.Fatal("the row was retired with the object still in cold storage")
	}
}

// listThen is a cold store whose listing is followed by after, once: what
// happens to cold storage between a sync's listing and its read of the rows.
type listThen struct {
	*hookCold
	after func()
}

func (l *listThen) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objs, err := l.hookCold.ListObjects(ctx, prefix)
	if l.after != nil {
		f := l.after
		l.after = nil
		f()
	}
	return objs, err
}

// A cold sync whose listing still shows a file that is removed, row and all,
// before the sync reads the rows: the file is not recorded again. It used to
// come back as a cold row with no object, which the next sync reports as a
// lost file, for a removal that had gone as intended.
func TestColdSync1825_AFileRemovedUnderTheListingIsNotRecordedAgain(t *testing.T) {
	ctx := context.Background()
	m, _, cold := coldRig1825(t)
	store := m.ColdBackendIdentity()
	lt := &listThen{hookCold: cold}
	m.coldBackend = lt
	lt.after = func() {
		if err := m.RemoveRehomedColdFile(ctx, gateDailyA, store); err != nil {
			t.Errorf("removal: %v", err)
		}
	}
	if _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	if row, _ := m.metadata.GetFile(ctx, gateDailyA); row != nil {
		t.Fatalf("a file removed under the sync's listing was recorded again: %+v", row)
	}
	// With no removal under it, a cold object with no row is still recorded.
	mustWrite(t, cold.mockBackend, gateDailyB)
	if _, err := m.syncColdTierMetadata(ctx); err != nil {
		t.Fatal(err)
	}
	if row, _ := m.metadata.GetFile(ctx, gateDailyB); row == nil || row.Tier != TierCold {
		t.Fatalf("a cold object with no row was not recorded: %+v", row)
	}
}
