package cluster

// #676: the standby store's free-space reserve stays above the WAL disk gate's
// off-point on the same volume, so the store gives way before the gate turns
// on. At the WAL defaults its 2 GiB floor already is: nothing changes.

import (
	"testing"

	"github.com/basekick-labs/arc/internal/wal"
)

func walOffPoint(reserve, maxSize int64) func(uint64) uint64 {
	return func(total uint64) uint64 {
		_, off, _ := wal.DiskPressureLevels(reserve, maxSize, total)
		return uint64(off)
	}
}

func TestStandbyReserveUnchangedAtWALDefaults(t *testing.T) {
	c := &Coordinator{}
	c.SetWALDiskOffPoint(walOffPoint(0, 100<<20))
	for _, total := range []uint64{1 << 30, 4 << 30, 20 << 30, 100 << 30, 10 << 40} {
		if got, want := c.standbyReserve(total), standbyStoreReserve(total); got != want {
			t.Fatalf("total %d: reserve %d, want the unchanged %d", total, got, want)
		}
	}
}

func TestStandbyReserveStaysAboveALargeWALOffPoint(t *testing.T) {
	c := &Coordinator{}
	if got := c.standbyReserve(100 << 30); got != standbyStoreReserve(100<<30) {
		t.Fatal("no off-point set changed the reserve")
	}
	c.SetWALDiskOffPoint(walOffPoint(15<<30, 100<<20)) // wal.disk_min_free_mb = 15 GiB
	off := walOffPoint(15<<30, 100<<20)(100 << 30)
	if got := c.standbyReserve(100 << 30); got < off+standbyStoreWALMargin {
		t.Fatalf("store reserve %d is not above the WAL off-point %d", got, off)
	}
}

// The space watch judges the volume against that reserve.
func TestStandbyLowSpaceUsesTheWALOffPoint(t *testing.T) {
	c := &Coordinator{}
	c.SetWALDiskOffPoint(walOffPoint(15<<30, 100<<20))
	low, reserve := c.standbyLowSpaceNext(false, 100<<30, 15<<30)
	if !low || reserve != c.standbyReserve(100<<30) {
		t.Fatalf("low=%v reserve=%d: 15 GiB free is below a store reserve above the WAL off-point", low, reserve)
	}
}
