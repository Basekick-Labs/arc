package reconciliation

import (
	"sort"
	"testing"
	"time"
)

func TestComputeDiff_Clean(t *testing.T) {
	now := time.Now().UTC()
	manifest := []*ObjectKey{
		{Path: "db/m/2026/04/27/12/a.parquet", Database: "db", Measurement: "m"},
		{Path: "db/m/2026/04/27/12/b.parquet", Database: "db", Measurement: "m"},
	}
	storage := []objectRecord{
		{path: "db/m/2026/04/27/12/a.parquet", lastModified: now.Add(-2 * time.Hour)},
		{path: "db/m/2026/04/27/12/b.parquet", lastModified: now.Add(-2 * time.Hour)},
	}
	d := computeDiff(manifest, storage, now, 24*time.Hour)
	if len(d.orphanManifest) != 0 || len(d.orphanStorage) != 0 || d.skippedGraceCount != 0 {
		t.Fatalf("expected clean diff, got: %+v", d)
	}
}

func TestComputeDiff_OrphanManifestOnly(t *testing.T) {
	now := time.Now().UTC()
	manifest := []*ObjectKey{
		{Path: "db/m/a.parquet", Database: "db", Measurement: "m"},
		{Path: "db/m/b.parquet", Database: "db", Measurement: "m"},
	}
	// b.parquet missing from storage.
	storage := []objectRecord{
		{path: "db/m/a.parquet", lastModified: now.Add(-2 * time.Hour)},
	}
	d := computeDiff(manifest, storage, now, 24*time.Hour)
	if len(d.orphanManifest) != 1 || d.orphanManifest[0] != "db/m/b.parquet" {
		t.Fatalf("expected one orphan-manifest, got: %+v", d.orphanManifest)
	}
	if len(d.orphanStorage) != 0 {
		t.Fatalf("expected zero orphan-storage, got: %+v", d.orphanStorage)
	}
}

func TestComputeDiff_OrphanStorageOnly(t *testing.T) {
	now := time.Now().UTC()
	manifest := []*ObjectKey{
		{Path: "db/m/a.parquet", Database: "db", Measurement: "m"},
	}
	storage := []objectRecord{
		{path: "db/m/a.parquet", lastModified: now.Add(-2 * time.Hour)},
		{path: "db/m/orphan.parquet", lastModified: now.Add(-48 * time.Hour)}, // old, eligible
	}
	d := computeDiff(manifest, storage, now, 24*time.Hour)
	if len(d.orphanManifest) != 0 {
		t.Fatalf("expected zero orphan-manifest, got: %+v", d.orphanManifest)
	}
	if len(d.orphanStorage) != 1 || d.orphanStorage[0].path != "db/m/orphan.parquet" {
		t.Fatalf("expected one orphan-storage, got: %+v", d.orphanStorage)
	}
}

func TestComputeDiff_GraceWindowProtectsYoungFiles(t *testing.T) {
	now := time.Now().UTC()
	manifest := []*ObjectKey{}
	storage := []objectRecord{
		// Young: 1h old, grace is 24h → skip
		{path: "db/m/young.parquet", lastModified: now.Add(-1 * time.Hour)},
		// Old: 25h old, grace is 24h → eligible
		{path: "db/m/old.parquet", lastModified: now.Add(-25 * time.Hour)},
	}
	d := computeDiff(manifest, storage, now, 24*time.Hour)
	if d.skippedGraceCount != 1 {
		t.Fatalf("expected 1 skipped, got %d", d.skippedGraceCount)
	}
	if len(d.orphanStorage) != 1 || d.orphanStorage[0].path != "db/m/old.parquet" {
		t.Fatalf("expected only old file to be orphan-storage, got: %+v", d.orphanStorage)
	}
}

func TestComputeDiff_ZeroMtimeTreatedAsOld(t *testing.T) {
	now := time.Now().UTC()
	manifest := []*ObjectKey{}
	storage := []objectRecord{
		// No mtime — backend doesn't implement ObjectLister and we fell
		// back to plain List. Treat as eligible (re-check before delete
		// will catch any race).
		{path: "db/m/no-mtime.parquet"},
	}
	d := computeDiff(manifest, storage, now, 24*time.Hour)
	if len(d.orphanStorage) != 1 {
		t.Fatalf("expected zero-mtime to be eligible, got: %+v", d.orphanStorage)
	}
	if d.skippedGraceCount != 0 {
		t.Fatalf("expected 0 skipped, got %d", d.skippedGraceCount)
	}
}

func TestComputeDiff_BothKindsMixed(t *testing.T) {
	now := time.Now().UTC()
	manifest := []*ObjectKey{
		{Path: "db/m/a.parquet"},
		{Path: "db/m/b.parquet"}, // missing from storage
		{Path: "db/m/c.parquet"},
	}
	storage := []objectRecord{
		{path: "db/m/a.parquet", lastModified: now.Add(-2 * time.Hour)},
		{path: "db/m/c.parquet", lastModified: now.Add(-2 * time.Hour)},
		{path: "db/m/orphan.parquet", lastModified: now.Add(-48 * time.Hour)}, // missing from manifest
		{path: "db/m/young.parquet", lastModified: now.Add(-30 * time.Minute)}, // grace skip
	}
	d := computeDiff(manifest, storage, now, 24*time.Hour)
	if len(d.orphanManifest) != 1 || d.orphanManifest[0] != "db/m/b.parquet" {
		t.Fatalf("expected b.parquet as orphan-manifest, got: %+v", d.orphanManifest)
	}
	if len(d.orphanStorage) != 1 || d.orphanStorage[0].path != "db/m/orphan.parquet" {
		t.Fatalf("expected orphan.parquet as orphan-storage, got: %+v", d.orphanStorage)
	}
	if d.skippedGraceCount != 1 {
		t.Fatalf("expected 1 grace skip, got %d", d.skippedGraceCount)
	}
}

func TestComputeDiff_ClockSkewAdded(t *testing.T) {
	// Caller passes graceTotal = grace + skew. Simulate a file that
	// would be eligible at exactly grace but not at grace+skew.
	now := time.Now().UTC()
	manifest := []*ObjectKey{}
	storage := []objectRecord{
		// 24h 30min old: eligible with grace=24h, NOT eligible with grace+skew=24h5m
		{path: "db/m/edge.parquet", lastModified: now.Add(-(24*time.Hour + 30*time.Minute))},
	}
	// Grace alone: 24h → eligible
	d1 := computeDiff(manifest, storage, now, 24*time.Hour)
	if len(d1.orphanStorage) != 1 {
		t.Fatalf("with grace=24h, file should be eligible, got: %+v", d1.orphanStorage)
	}
	// Grace + 5m skew: 24h5m → still eligible (file is 24h30m old)
	d2 := computeDiff(manifest, storage, now, 24*time.Hour+5*time.Minute)
	if len(d2.orphanStorage) != 1 {
		t.Fatalf("file is older than grace+skew, should be eligible, got: %+v", d2.orphanStorage)
	}
	// Grace + 1h: file is 24h30m old, grace+skew is 25h → NOT eligible
	d3 := computeDiff(manifest, storage, now, 25*time.Hour)
	if len(d3.orphanStorage) != 0 || d3.skippedGraceCount != 1 {
		t.Fatalf("grace+skew=25h should protect 24h30m file, got: %+v", d3)
	}
}

func TestIsParquetCandidate(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"db/m/2026/04/27/12/a.parquet", true},
		{"db/m/2026/04/27/12/a.tmp.parquet", true}, // .tmp.* is at base — but here base is a.tmp.parquet starts with "a"
		{"db/m/2026/04/27/12/.tmp.a.parquet", false},
		{"db/m/2026/04/27/12/.hidden.parquet", false},
		{"db/m/2026/04/27/12/file.json", false},
		{"_compaction_state/job-123.json", false},
		{"db/_compaction_state/foo.parquet", false},
	}
	for _, c := range cases {
		got := isParquetCandidate(c.path)
		if got != c.want {
			t.Errorf("isParquetCandidate(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

// Sort-stability test: orphan-manifest output is map iteration order.
// We test the *set* equality not the order.
func TestComputeDiff_OrphanManifestStable(t *testing.T) {
	now := time.Now().UTC()
	manifest := []*ObjectKey{
		{Path: "db/m/a.parquet"},
		{Path: "db/m/b.parquet"},
		{Path: "db/m/c.parquet"},
	}
	storage := []objectRecord{
		// nothing in storage — all manifest entries are orphans
	}
	d := computeDiff(manifest, storage, now, 24*time.Hour)
	got := append([]string{}, d.orphanManifest...)
	sort.Strings(got)
	want := []string{"db/m/a.parquet", "db/m/b.parquet", "db/m/c.parquet"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("orphan-manifest set mismatch: got %v, want %v", got, want)
		}
	}
}
