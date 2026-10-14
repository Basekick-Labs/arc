package tiering

// #1271: a path that left the manifest while this node no longer had the file
// (its own compaction or retention removed it) retires a hot row this node
// still holds — only a hot row, only once the file is confirmed gone, and
// without consulting the cold tier.

import (
	"context"
	"errors"
	"testing"
)

type statErrBackend struct{ *mockBackend }

func (b *statErrBackend) StatFile(context.Context, string) (int64, error) {
	return -1, errors.New("disk unavailable")
}

// withHotRow returns a manager whose hot tier holds path with a hot row for
// it (the file is on disk when the pull is recorded, as in service).
func withHotRow(t *testing.T, cold *mockBackend, coldEnabled bool, path string) (*Manager, *mockBackend) {
	t.Helper()
	m := newTierEventManager(t, cold, coldEnabled)
	hot := newMockBackend("local")
	hot.seedRaw(path, []byte("123456789"))
	m.hotBackend = hot
	m.RecordReplicatedFile(path, 9)
	waitTierEvents(t, m, 1)
	if tier, ok := rowTier(t, m, path); !ok || tier != string(TierHot) {
		t.Fatalf("setup: hot row %q %v", tier, ok)
	}
	return m, hot
}

func TestRecordMissingFile(t *testing.T) {
	const path = "db1/cpu/2026/10/03/14/a.parquet"

	t.Run("retires the hot row of a file that is gone", func(t *testing.T) {
		m, hot := withHotRow(t, nil, true, path)
		cold := &countingExistsBackend{mockBackend: newMockBackend("s3")}
		m.coldBackend = cold
		_ = hot.Delete(context.Background(), path)
		m.RecordMissingFile(path)
		waitTierEvents(t, m, 2)
		if tier, ok := rowTier(t, m, path); ok {
			t.Fatalf("hot row kept (tier %q)", tier)
		}
		if n := cold.calls.Load(); n != 0 {
			t.Fatalf("the cold tier was consulted %d times", n)
		}
	})

	t.Run("keeps the row of a file that is back on disk", func(t *testing.T) {
		m, _ := withHotRow(t, nil, false, path)
		m.RecordMissingFile(path) // the file is still there
		waitTierEvents(t, m, 2)
		if tier, ok := rowTier(t, m, path); !ok || tier != string(TierHot) {
			t.Fatalf("the row of a live file was retired (%q, %v)", tier, ok)
		}
	})

	t.Run("keeps the row when the stat fails", func(t *testing.T) {
		m, _ := withHotRow(t, nil, false, path)
		// Swapped while the drainer is idle; the queue's lock orders it
		// before the next event is read.
		m.hotBackend = &statErrBackend{newMockBackend("local")}
		m.RecordMissingFile(path)
		waitTierEvents(t, m, 2)
		if _, ok := rowTier(t, m, path); !ok {
			t.Fatal("row retired on a failed stat")
		}
	})

	t.Run("never removes a cold row", func(t *testing.T) {
		cold := newMockBackend("s3")
		cold.seedRaw(path, []byte("x"))
		m, hot := withHotRow(t, cold, true, path)
		_ = hot.Delete(context.Background(), path)
		m.RecordUnlinkedFile(path, "tiering:migrated", 9) // flips to cold
		m.RecordMissingFile(path)
		waitTierEvents(t, m, 3)
		if tier, ok := rowTier(t, m, path); !ok || tier != string(TierCold) {
			t.Fatalf("cold row removed or changed (%q, %v)", tier, ok)
		}
	})

	t.Run("no row: nothing written", func(t *testing.T) {
		m := newTierEventManager(t, nil, false)
		m.hotBackend = newMockBackend("local")
		m.RecordMissingFile(path)
		waitTierEvents(t, m, 1)
		if _, ok := rowTier(t, m, path); ok {
			t.Fatal("a row appeared")
		}
	})

	var nilMgr *Manager
	nilMgr.RecordMissingFile(path) // nil-safe
}
