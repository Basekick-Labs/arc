package api

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/rs/zerolog"
)

// A measurement's publish generation is read the first time the transform
// names it, which is before its files are listed, and not again: a file
// published between two uses of the same table in one statement must still
// make the cached SQL stale (#1757).
func TestStoragePathEmissionStampsAReferenceOnce(t *testing.T) {
	p := pruning.NewPartitionPruner(zerolog.Nop())
	late := time.Now().UTC().Add(-3 * time.Hour)

	ctx, f := withStoragePathFailure(context.Background())
	f.stampOf = p.PublishStamp
	recordStoragePathEmission(ctx, "db", "cpu")
	p.FilePublished("db", "cpu", late) // lands while the transform is still running
	recordStoragePathEmission(ctx, "db", "cpu")
	recordStoragePathEmission(ctx, "db", "mem")

	cpu, ok := f.stamps[TableReference{Database: "db", Measurement: "cpu"}]
	if !ok {
		t.Fatal("no stamp recorded for the reference")
	}
	if p.StampsCurrent([]pruning.PublishStamp{cpu}) {
		t.Error("the stamp was taken again at the second use: a file published during the transform would not make the cached SQL stale")
	}
	mem, ok := f.stamps[TableReference{Database: "db", Measurement: "mem"}]
	if !ok || !p.StampsCurrent([]pruning.PublishStamp{mem}) {
		t.Errorf("a reference first named after the file was published must be current (recorded=%v)", ok)
	}
	if len(f.stamps) != len(f.emitted) {
		t.Errorf("%d stamps for %d references", len(f.stamps), len(f.emitted))
	}

	// Without a stamp reader (the uncached transform paths) nothing is recorded.
	ctx, plain := withStoragePathFailure(context.Background())
	recordStoragePathEmission(ctx, "db", "cpu")
	if len(plain.stamps) != 0 || len(plain.emitted) != 1 {
		t.Errorf("stamps=%d emitted=%d without a stamp reader", len(plain.stamps), len(plain.emitted))
	}
}
