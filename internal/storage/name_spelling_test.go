package storage

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// foldingBackend returns a LocalBackend on a temp dir, skipping the test
// when that volume does not fold names (Linux CI); macOS APFS does.
func foldingBackend(t *testing.T) *LocalBackend {
	t.Helper()
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if !b.CaseFolds() {
		t.Skip("temp volume does not fold letter case")
	}
	return b
}

func mkdirs(t *testing.T, b *LocalBackend, rel string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(b.GetBasePath(), rel), 0o700); err != nil {
		t.Fatal(err)
	}
}

func wantSpellingErr(t *testing.T, err error, what, onDisk string) {
	t.Helper()
	se, ok := err.(*NameSpellingError)
	if !ok {
		t.Fatalf("got %v, want a NameSpellingError", err)
	}
	if se.What != what || se.OnDisk != onDisk {
		t.Fatalf("got %+v, want what=%s onDisk=%s", se, what, onDisk)
	}
}

func TestCheckNameSpelling(t *testing.T) {
	b := foldingBackend(t)
	mkdirs(t, b, "db/cpu")
	if err := b.CheckNameSpelling("db", "cpu"); err != nil {
		t.Fatalf("on-disk spelling refused: %v", err)
	}
	wantSpellingErr(t, b.CheckNameSpelling("db", "CPU"), "measurement", "cpu")
	wantSpellingErr(t, b.CheckNameSpelling("DB", "cpu"), "database", "db")
	if err := b.CheckNameSpelling("db", "mem"); err != nil {
		t.Fatalf("missing measurement refused: %v", err)
	}
	if err := b.CheckNameSpelling("other", "CPU"); err != nil {
		t.Fatalf("missing database refused: %v", err)
	}
}

// Sameness is the filesystem's call, not an ASCII fold in Go: APFS folds
// the Kelvin sign to K.
func TestCheckNameSpellingUsesTheFilesystemsFold(t *testing.T) {
	b := foldingBackend(t)
	mkdirs(t, b, "db/kpu")
	if _, err := os.Stat(filepath.Join(b.GetBasePath(), "db", "Kpu")); err != nil {
		t.Skip("volume does not fold the Kelvin sign")
	}
	wantSpellingErr(t, b.CheckNameSpelling("db", "Kpu"), "measurement", "kpu")
}

func TestGetStoragePathRefusesAnotherSpelling(t *testing.T) {
	b := foldingBackend(t)
	mkdirs(t, b, "db/cpu")
	if _, err := GetStoragePath(b, "db", "CPU"); !IsNameSpellingError(err) {
		t.Fatalf("GetStoragePath(CPU) = %v, want a spelling refusal", err)
	}
	if _, err := GetStoragePath(b, "db", "cpu"); err != nil {
		t.Fatalf("GetStoragePath(cpu) = %v", err)
	}
}

func TestListRefusesAnotherSpelling(t *testing.T) {
	b := foldingBackend(t)
	mkdirs(t, b, "db/cpu/2026")
	ctx := context.Background()
	for _, prefix := range []string{"db/CPU/", "DB/cpu/", "DB", "db/CPU/2026/"} {
		if _, err := b.List(ctx, prefix); !IsNameSpellingError(err) {
			t.Errorf("List(%q) = %v, want a spelling refusal", prefix, err)
		}
	}
	for _, prefix := range []string{"", "db/", "db/cpu/", "db/cpu/2026/", "db/mem/"} {
		if _, err := b.List(ctx, prefix); err != nil {
			t.Errorf("List(%q) = %v", prefix, err)
		}
	}
}

func TestAdmitNameSpelling(t *testing.T) {
	b := foldingBackend(t)
	if err := b.AdmitNameSpelling("db", "cpu"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(b.GetBasePath(), "db", "cpu")); err != nil {
		t.Fatalf("admitted name has no directory: %v", err)
	}
	wantSpellingErr(t, b.AdmitNameSpelling("db", "CPU"), "measurement", "cpu")
	wantSpellingErr(t, b.AdmitNameSpelling("Db", "mem"), "database", "db")
}

// Racing first writes of two spellings: exactly one spelling is admitted.
func TestAdmitNameSpellingRace(t *testing.T) {
	b := foldingBackend(t)
	names := []string{"cpu", "CPU", "Cpu", "cPU"}
	var wg sync.WaitGroup
	admitted := make([]bool, 64)
	for i := range admitted {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			admitted[i] = b.AdmitNameSpelling("db", names[i%len(names)]) == nil
		}(i)
	}
	wg.Wait()
	won := map[string]bool{}
	for i, ok := range admitted {
		if ok {
			won[names[i%len(names)]] = true
		}
	}
	entries, err := os.ReadDir(filepath.Join(b.GetBasePath(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	if len(won) != 1 || len(entries) != 1 || !won[entries[0].Name()] {
		t.Fatalf("admitted %v, on disk %v: want exactly one spelling", won, entries)
	}
}

// A measurement removed and re-created in another spelling: the old
// spelling is refused, not served from a cached listing.
func TestCheckNameSpellingAfterRecreate(t *testing.T) {
	b := foldingBackend(t)
	ctx := context.Background()
	if err := b.AdmitNameSpelling("db", "cpu"); err != nil {
		t.Fatal(err)
	}
	if err := b.CheckNameSpelling("db", "cpu"); err != nil {
		t.Fatal(err)
	}
	if err := b.RemoveDirectory(ctx, "db/cpu"); err != nil {
		t.Fatal(err)
	}
	if err := b.AdmitNameSpelling("db", "CPU"); err != nil {
		t.Fatalf("new spelling after removal refused: %v", err)
	}
	wantSpellingErr(t, b.CheckNameSpelling("db", "cpu"), "measurement", "CPU")
}

// Armed on a volume that does not fold case (Linux, or case-sensitive APFS,
// which still folds normalization), the guard never refuses a case
// variant: a stat that finds an entry there means that exact name exists.
func TestNameSpellingNeverRefusesWithoutFolding(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if b.CaseFolds() {
		t.Skip("temp volume folds letter case")
	}
	b.spelling.armed = true
	mkdirs(t, b, "db/cpu")
	for _, segs := range [][]string{{"db", "CPU"}, {"DB", "cpu"}, {"db", "cpu"}} {
		if err := b.CheckNameSpelling(segs...); err != nil {
			t.Errorf("CheckNameSpelling(%v) = %v", segs, err)
		}
	}
	if err := b.AdmitNameSpelling("db", "CPU"); err != nil {
		t.Errorf("AdmitNameSpelling(db, CPU) = %v", err)
	}
}

// A listing taken within listingTrustAge of its directory's mtime is never
// trusted, however old it grows: a remove and a create in the same mtime
// tick leave the mtime as it was (coarse-timestamp volumes).
func TestListingTakenInTheMtimeTickIsNotTrusted(t *testing.T) {
	b := foldingBackend(t)
	db := filepath.Join(b.GetBasePath(), "db")
	mkdirs(t, b, "db/cpu")
	mod := time.Now().Add(-1500 * time.Millisecond)
	if err := os.Chtimes(db, mod, mod); err != nil {
		t.Fatal(err)
	}
	if err := b.CheckNameSpelling("db", "cpu"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(db, "cpu")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(db, "CPU"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(db, mod, mod); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond) // the mtime is now over 2 s old
	wantSpellingErr(t, b.CheckNameSpelling("db", "cpu"), "measurement", "CPU")
}

// A listing taken long after its mtime is trusted while the mtime holds, and
// dropped once the directory changes.
func TestListingFollowsTheDirectoryMtime(t *testing.T) {
	b := foldingBackend(t)
	db := filepath.Join(b.GetBasePath(), "db")
	mkdirs(t, b, "db/cpu")
	old := time.Now().Add(-10 * time.Second)
	if err := os.Chtimes(db, old, old); err != nil {
		t.Fatal(err)
	}
	if err := b.CheckNameSpelling("db", "cpu"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(db, "cpu")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(db, "CPU"), 0o700); err != nil {
		t.Fatal(err)
	}
	wantSpellingErr(t, b.CheckNameSpelling("db", "cpu"), "measurement", "CPU")
}

// A symlinked storage root is checked through the link: the link's own
// mtime never changes.
func TestSymlinkedRootFollowsTheTarget(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Skip(err)
	}
	b, err := NewLocalBackend(link, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if !b.CaseFolds() {
		t.Skip("temp volume does not fold letter case")
	}
	// Age the link itself, so only following it sees the change below.
	if out, err := exec.Command("touch", "-h", "-t", time.Now().Add(-time.Hour).Format("200601021504.05"), link).CombinedOutput(); err != nil {
		t.Skipf("touch -h: %v %s", err, out)
	}
	if err := b.AdmitNameSpelling("metrics", "cpu"); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-10 * time.Second)
	if err := os.Chtimes(real, old, old); err != nil {
		t.Fatal(err)
	}
	if err := b.CheckNameSpelling("metrics", "cpu"); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(real, "metrics")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(real, "METRICS", "cpu"), 0o700); err != nil {
		t.Fatal(err)
	}
	wantSpellingErr(t, b.CheckNameSpelling("metrics", "cpu"), "database", "METRICS")
}
