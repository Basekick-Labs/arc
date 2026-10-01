package reconciliation

import (
	"reflect"
	"testing"
	"time"
)

// computeDiff's per-node scoping: the membership set is every entry, and
// only the orphan-manifest direction is scoped to this node's own-origin
// entries (plus entries with no origin at all).
func TestComputeDiff_PerNodeStorage(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	manifest := []*ObjectKey{
		{Path: "db/m/own-missing.parquet", Database: "db", Measurement: "m", OriginNodeID: "node-a"},
		{Path: "db/m/foreign-missing.parquet", Database: "db", Measurement: "m", OriginNodeID: "node-b"},
		{Path: "db/m/no-origin-missing.parquet", Database: "db", Measurement: "m"},
		{Path: "db/m/foreign-replica.parquet", Database: "db", Measurement: "m", OriginNodeID: "node-b"},
	}
	storage := []objectRecord{
		{path: "db/m/foreign-replica.parquet", lastModified: old},
		{path: "db/m/true-orphan.parquet", lastModified: old},
	}

	d := computeDiff(manifest, storage, now, 24*time.Hour, "node-a", true)
	if want := []string{"db/m/no-origin-missing.parquet", "db/m/own-missing.parquet"}; !reflect.DeepEqual(d.orphanManifest, want) {
		t.Errorf("per-node orphan-manifest = %v, want %v (foreign-origin entries skipped)", d.orphanManifest, want)
	}
	if len(d.orphanStorage) != 1 || d.orphanStorage[0].path != "db/m/true-orphan.parquet" {
		t.Errorf("per-node orphan-storage = %+v, want only the untracked path: a replica of a foreign-origin entry is tracked", d.orphanStorage)
	}

	// Shared / standalone: no scoping, every missing entry is a candidate.
	d = computeDiff(manifest, storage, now, 24*time.Hour, "", false)
	if want := []string{"db/m/foreign-missing.parquet", "db/m/no-origin-missing.parquet", "db/m/own-missing.parquet"}; !reflect.DeepEqual(d.orphanManifest, want) {
		t.Errorf("unscoped orphan-manifest = %v, want %v", d.orphanManifest, want)
	}

	// The invalid combination fails safe: with no local id, every
	// originated entry is skipped and only the origin-less one is reported.
	d = computeDiff(manifest, storage, now, 24*time.Hour, "", true)
	if want := []string{"db/m/no-origin-missing.parquet"}; !reflect.DeepEqual(d.orphanManifest, want) {
		t.Errorf("per-node with empty local id: orphan-manifest = %v, want %v (fail safe, report less)", d.orphanManifest, want)
	}
}
