package tiering

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1552: tiering raises the cluster's cold mark before every cold write and
// seeds it from a cold listing.

// fakeColdMarker records ColdWrite calls and the mark state at the write.
type fakeColdMarker struct {
	*fakeManifest
	marks         map[string]int32
	refuse        error
	wroteMarked   []bool
	fenced        bool
	seeded        bool
	seededMarks   map[string]int32
	seededBackend string
}

func (f *fakeColdMarker) ColdWrite(_ context.Context, marks map[string]int32, write func() error) error {
	if f.refuse != nil {
		return f.refuse
	}
	if f.marks == nil {
		f.marks = map[string]int32{}
	}
	for s, d := range marks {
		addColdMark(f.marks, s, d)
	}
	return write()
}

func (f *fakeColdMarker) ColdMarkSeedState() (bool, bool) { return f.fenced, f.seeded }

func (f *fakeColdMarker) SeedColdMarks(_ context.Context, marks map[string]int32, backend string) error {
	f.seededMarks, f.seededBackend = marks, backend
	return nil
}

func TestColdMarkDay(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	scope, day, ok := m.coldMarkDay("db/cpu/2024/03/15/14/f.parquet")
	if !ok || scope != "db/cpu" || day != 19797 { // 2024-03-15
		t.Fatalf("hour file: %q %d %v", scope, day, ok)
	}
	if _, day, _ := m.coldMarkDay("db/cpu/2024/03/15/f_daily.parquet"); day != 19797 {
		t.Fatalf("day file: %d", day)
	}
	if scope, day, ok := m.coldMarkDay("db/cpu/odd/f.parquet"); !ok || scope != "db/cpu" || day != coldMarkUnknownDay {
		t.Fatalf("unparseable under a scope: %q %d %v, want the unknown day", scope, day, ok)
	}
	if _, _, ok := m.coldMarkDay("f.parquet"); ok {
		t.Fatal("a key under no scope got a mark")
	}
}

// The migrator writes a cold object only inside ColdWrite, after the mark
// covers its day; refused, nothing is written.
func TestMigrationRaisesTheColdMarkBeforeCopying(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	const path = "db1/cpu/2024/03/15/14/f.parquet"
	mustWrite(t, hot, path)
	fm := &fakeColdMarker{fakeManifest: &fakeManifest{hot: hot, entries: map[string]int64{path: 7}}}
	m.manifest = fm

	fm.refuse = errors.New("no quorum")
	cand := candidateFor(path)
	cand.Database, cand.Measurement = "db1", "cpu"
	if err := m.migrator.copyAndFlip(ctx, cand); err == nil {
		t.Fatal("copied with the mark refused")
	}
	if ok, _ := cold.Exists(ctx, path); ok {
		t.Fatal("cold object written although the mark was refused")
	}

	fm.refuse = nil
	_ = m.migrator.copyAndFlip(ctx, cand)
	if ok, _ := cold.Exists(ctx, path); !ok {
		t.Fatal("not copied")
	}
	if fm.marks["db1/cpu"] != 19797 {
		t.Fatalf("marks %v", fm.marks)
	}
}

// A cold sync seeds the mark only when fenced and not seeded before the
// listing, and only on a node whose tiering is not role-gated.
func TestColdSyncSeedsTheMark(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	m.config.Cold.S3Bucket, m.config.Cold.S3Prefix = "arc-archive-test", "/p/"
	fm := &fakeColdMarker{fakeManifest: &fakeManifest{hot: hot, entries: map[string]int64{}}}
	m.manifest = fm
	sync := func() {
		t.Helper()
		syncListing(t, m, cold, "db1/cpu/2024/03/15/cpu_a_daily.parquet", "db1/cpu/2024/03/17/14/b.parquet", "db1/mem/x/y.parquet")
	}

	sync()
	if fm.seededMarks != nil {
		t.Fatal("seeded before the fence")
	}
	fm.fenced = true
	gate := &mockGate{role: "reader"}
	m.clusterGate = gate
	sync()
	if fm.seededMarks != nil {
		t.Fatal("a role-gated reader seeded")
	}
	gate.primary.Store(true)
	sync()
	want := map[string]int32{"db1/cpu": 19799, "db1/mem": coldMarkUnknownDay}
	if len(fm.seededMarks) != 2 || fm.seededMarks["db1/cpu"] != want["db1/cpu"] || fm.seededMarks["db1/mem"] != want["db1/mem"] || fm.seededBackend != "s3://arc-archive-test/p" {
		t.Fatalf("seed %v %q, want %v s3://arc-archive-test/p", fm.seededMarks, fm.seededBackend, want)
	}
	fm.seededMarks, fm.seeded = nil, true
	sync()
	if fm.seededMarks != nil {
		t.Fatal("seeded again")
	}
}

// Cold-writer inventory: every storage write in tiering and backup, with
// whether it can write to the cold tier. A cold write must go through
// GuardColdWrite (#1552); a new write here must be added with that decision.
func TestColdWriterInventory(t *testing.T) {
	allowed := map[string]string{
		"tiering/migrator.go:dst.Write(ctx":                "copyFile: unused, no caller (cold write would need GuardColdWrite)",
		"tiering/migrator.go:dst.WriteReader(ctx":          "copyFileStreaming: cold write, guarded in copyAndFlip",
		"backup/restore.go:dest.WriteReader(ctx":           "streamRestoreFile: cold write, guarded in the restore loop",
		"backup/cluster.go:dest.backend.WriteReader(ctx":   "backup target, not cold",
		"backup/warehouse.go:dest.backend.WriteReader(ctx": "backup target, not cold",
		"backup/backup.go:dest.backend.WriteReader(ctx":    "backup target, not cold",
	}
	re := regexp.MustCompile(`[A-Za-z_.]+\.(WriteReader|Write)\(ctx`)
	pkg := map[string]string{".": "tiering", "../backup": "backup"}
	var found []string
	for _, dir := range []string{".", "../backup"} {
		files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			fh, err := os.Open(f)
			if err != nil {
				t.Fatal(err)
			}
			s := bufio.NewScanner(fh)
			for s.Scan() {
				for _, call := range re.FindAllString(s.Text(), -1) {
					key := pkg[dir] + "/" + filepath.Base(f) + ":" + call
					found = append(found, key)
				}
			}
			fh.Close()
		}
	}
	sort.Strings(found)
	if len(found) < len(allowed) {
		t.Fatalf("found %v: the scan missed known writes", found)
	}
	for _, k := range found {
		if _, ok := allowed[k]; !ok {
			t.Errorf("new storage write %s: decide whether it can write to the cold tier and, if so, guard it with GuardColdWrite", k)
		}
	}
}

// fenceDuringListing flips the marker to fenced while the cold listing runs.
type fenceDuringListing struct {
	*listedColdObjects
	fm *fakeColdMarker
}

func (l *fenceDuringListing) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	l.fm.fenced = true
	return l.listedColdObjects.ListObjects(ctx, prefix)
}

// The seed decision is taken before the listing: a listing that began before
// the fence may have missed a write the fence drained, so it must not seed.
func TestColdSyncSeedDecisionPrecedesTheListing(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	fm := &fakeColdMarker{fakeManifest: &fakeManifest{hot: hot, entries: map[string]int64{}}}
	m.manifest = fm
	m.coldBackend = &fenceDuringListing{
		listedColdObjects: &listedColdObjects{mockBackend: cold, objects: coldObjects("db1/cpu/2024/03/15/cpu_a_daily.parquet")},
		fm:                fm,
	}
	if _, err := m.syncColdTierMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fm.seededMarks != nil {
		t.Fatal("seeded from a listing that started before the fence")
	}
}
