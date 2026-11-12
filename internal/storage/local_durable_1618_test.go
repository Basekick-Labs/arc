package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/rs/zerolog"
)

// Tests for #1618: every publishing write on the local backend fsyncs the file
// before it gets its final name, and the directory after, so a power loss
// after the call returns keeps both. The order is observed from inside the
// sync seams, which is the only place it is visible: after the call returns
// the file exists either way.
//
// None of these call t.Parallel(): they swap package-level seams.

type syncRecorder struct {
	mu    sync.Mutex
	files []string // names of files synced
	dirs  []string // directories synced
	// observed at sync time
	finalExistedAtFileSync []bool
	finalExistedAtDirSync  map[string]bool
	final                  string
}

func (r *syncRecorder) exists() bool {
	if r.final == "" {
		return false
	}
	_, err := os.Lstat(r.final)
	return err == nil
}

func recordSyncs(t *testing.T, fileErr, dirErr error) *syncRecorder {
	t.Helper()
	r := &syncRecorder{finalExistedAtDirSync: map[string]bool{}}
	origFile, origDir := syncFileFn, syncDirFn
	syncFileFn = func(f *os.File) error {
		r.mu.Lock()
		r.files = append(r.files, f.Name())
		r.finalExistedAtFileSync = append(r.finalExistedAtFileSync, r.exists())
		r.mu.Unlock()
		if fileErr != nil {
			return fileErr
		}
		return origFile(f)
	}
	syncDirFn = func(dir string) error {
		r.mu.Lock()
		r.dirs = append(r.dirs, dir)
		if r.final != "" && dir == filepath.Dir(r.final) {
			r.finalExistedAtDirSync[dir] = r.exists()
		}
		r.mu.Unlock()
		if dirErr != nil {
			return dirErr
		}
		return origDir(dir)
	}
	t.Cleanup(func() { syncFileFn, syncDirFn = origFile, origDir })
	return r
}

func newDurableBackend(t *testing.T) *LocalBackend {
	t.Helper()
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

const durableKey = "db/m/2026/11/12/10/f.parquet"

// assertOrdered checks the file was synced before its final name existed and
// the directory after.
func assertOrdered(t *testing.T, r *syncRecorder) {
	t.Helper()
	if len(r.files) == 0 {
		t.Fatal("the file was never synced")
	}
	for i, existed := range r.finalExistedAtFileSync {
		if existed {
			t.Fatalf("file sync %d (%s) ran after the final name existed; it must precede the rename", i, r.files[i])
		}
	}
	dir := filepath.Dir(r.final)
	existed, ok := r.finalExistedAtDirSync[dir]
	if !ok {
		t.Fatalf("the final directory %s was never synced; synced: %v", dir, r.dirs)
	}
	if !existed {
		t.Fatal("the directory was synced before the final name existed; it must follow the rename")
	}
}

func TestWriteSyncsFileBeforeRenameAndDirectoryAfter(t *testing.T) {
	b := newDurableBackend(t)
	r := recordSyncs(t, nil, nil)
	r.final = filepath.Join(b.basePath, durableKey)
	if err := b.Write(context.Background(), durableKey, []byte("data")); err != nil {
		t.Fatal(err)
	}
	assertOrdered(t, r)
}

func TestWriteIfAbsentSyncsFileBeforeLinkAndDirectoryAfter(t *testing.T) {
	b := newDurableBackend(t)
	r := recordSyncs(t, nil, nil)
	r.final = filepath.Join(b.basePath, durableKey)
	ok, err := b.WriteIfAbsent(context.Background(), durableKey, []byte("data"))
	if err != nil || !ok {
		t.Fatalf("WriteIfAbsent = %v, %v", ok, err)
	}
	assertOrdered(t, r)
}

func TestWriteReaderSyncsFileBeforeRenameAndDirectoryAfter(t *testing.T) {
	b := newDurableBackend(t)
	r := recordSyncs(t, nil, nil)
	r.final = filepath.Join(b.basePath, durableKey)
	if err := b.WriteReader(context.Background(), durableKey, strings.NewReader("data"), 4); err != nil {
		t.Fatal(err)
	}
	assertOrdered(t, r)
}

// The promoting append syncs the staging inode, which also holds the bytes a
// previous call wrote through another descriptor.
func TestAppendReaderPromotionSyncsFileBeforeRenameAndDirectoryAfter(t *testing.T) {
	b := newDurableBackend(t)
	full := filepath.Join(b.basePath, durableKey)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath(full), []byte("da"), 0600); err != nil {
		t.Fatal(err)
	}
	r := recordSyncs(t, nil, nil)
	r.final = full
	if err := b.AppendReader(context.Background(), durableKey, strings.NewReader("ta"), 2); err != nil {
		t.Fatal(err)
	}
	assertOrdered(t, r)
	got, err := os.ReadFile(full)
	if err != nil || string(got) != "data" {
		t.Fatalf("promoted file = %q, %v", got, err)
	}
}

func TestAppendReaderWithoutPromotionDoesNotSync(t *testing.T) {
	b := newDurableBackend(t)
	full := filepath.Join(b.basePath, durableKey)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath(full), nil, 0600); err != nil {
		t.Fatal(err)
	}
	r := recordSyncs(t, nil, nil)
	// appendSize larger than what the reader yields: a partial chunk.
	if err := b.AppendReader(context.Background(), durableKey, strings.NewReader("da"), 4); err != nil {
		t.Fatal(err)
	}
	if len(r.files) != 0 || len(r.dirs) != 0 {
		t.Fatalf("a non-promoting append synced files %v dirs %v", r.files, r.dirs)
	}
}

func TestAdoptFileSyncsSourceBeforeRenameAndDirectoryAfter(t *testing.T) {
	b := newDurableBackend(t)
	src := filepath.Join(t.TempDir(), "out.parquet")
	if err := os.WriteFile(src, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	r := recordSyncs(t, nil, nil)
	r.final = filepath.Join(b.basePath, durableKey)
	if err := b.AdoptFile(context.Background(), durableKey, src); err != nil {
		t.Fatal(err)
	}
	assertOrdered(t, r)
	if r.files[0] != src {
		t.Fatalf("synced %s, want the adopted source %s", r.files[0], src)
	}
}

// A failed file sync publishes nothing and leaves no temp file behind.
func TestWriteFileSyncFailurePublishesNothing(t *testing.T) {
	b := newDurableBackend(t)
	recordSyncs(t, syscall.EIO, nil)
	err := b.Write(context.Background(), durableKey, []byte("data"))
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Write = %v, want EIO", err)
	}
	assertDirHoldsNothing(t, filepath.Dir(filepath.Join(b.basePath, durableKey)))
}

func TestWriteReaderFileSyncFailureRemovesStaging(t *testing.T) {
	b := newDurableBackend(t)
	recordSyncs(t, syscall.EIO, nil)
	err := b.WriteReader(context.Background(), durableKey, strings.NewReader("data"), 4)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("WriteReader = %v, want EIO", err)
	}
	assertDirHoldsNothing(t, filepath.Dir(filepath.Join(b.basePath, durableKey)))
}

func TestAdoptFileSyncFailureIsAHardErrorAndKeepsTheSource(t *testing.T) {
	b := newDurableBackend(t)
	src := filepath.Join(t.TempDir(), "out.parquet")
	if err := os.WriteFile(src, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	recordSyncs(t, syscall.EIO, nil)
	err := b.AdoptFile(context.Background(), durableKey, src)
	if !errors.Is(err, syscall.EIO) || errors.Is(err, ErrAdoptUnsupported) {
		t.Fatalf("AdoptFile = %v, want EIO and not ErrAdoptUnsupported", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatalf("the source should be untouched: %v", err)
	}
	if _, err := os.Stat(filepath.Join(b.basePath, durableKey)); !os.IsNotExist(err) {
		t.Fatalf("nothing should be published, stat = %v", err)
	}
}

// A directory sync that fails after the rename unpublishes the file: every
// caller reads an error as "nothing was written".
func TestDirectorySyncFailureUnpublishes(t *testing.T) {
	for name, write := range map[string]func(*LocalBackend) error{
		"Write": func(b *LocalBackend) error { return b.Write(context.Background(), durableKey, []byte("data")) },
		"WriteReader": func(b *LocalBackend) error {
			return b.WriteReader(context.Background(), durableKey, strings.NewReader("data"), 4)
		},
		"AdoptFile": func(b *LocalBackend) error {
			src := filepath.Join(t.TempDir(), "out.parquet")
			if err := os.WriteFile(src, []byte("data"), 0600); err != nil {
				t.Fatal(err)
			}
			return b.AdoptFile(context.Background(), durableKey, src)
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := newDurableBackend(t)
			full := filepath.Join(b.basePath, durableKey)
			// Create the directory chain first so only the post-rename sync fails.
			if err := b.ensureDir(filepath.Dir(full)); err != nil {
				t.Fatal(err)
			}
			recordSyncs(t, nil, syscall.EIO)
			if err := write(b); !errors.Is(err, syscall.EIO) {
				t.Fatalf("%s = %v, want EIO", name, err)
			}
			if _, err := os.Stat(full); !os.IsNotExist(err) {
				t.Fatalf("a write whose directory sync failed is still published: stat = %v", err)
			}
		})
	}
}

// WriteIfAbsent is the Iceberg publish fence: a reader may already have
// adopted the linked version, so it reports the error but keeps the file.
func TestWriteIfAbsentDirectorySyncFailureKeepsTheFile(t *testing.T) {
	b := newDurableBackend(t)
	full := filepath.Join(b.basePath, durableKey)
	if err := b.ensureDir(filepath.Dir(full)); err != nil {
		t.Fatal(err)
	}
	recordSyncs(t, nil, syscall.EIO)
	if _, err := b.WriteIfAbsent(context.Background(), durableKey, []byte("data")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("WriteIfAbsent = %v, want EIO", err)
	}
	if got, err := os.ReadFile(full); err != nil || !bytes.Equal(got, []byte("data")) {
		t.Fatalf("the linked file should stay: %q, %v", got, err)
	}
}

// A volume that refuses directory fsync outright is tolerated; a file fsync
// refusal never is.
func TestUnsupportedDirectorySyncIsTolerated(t *testing.T) {
	for _, errno := range []error{syscall.EINVAL, syscall.ENOTSUP, syscall.ENOSYS} {
		b := newDurableBackend(t)
		recordSyncs(t, nil, errno)
		if err := b.Write(context.Background(), durableKey, []byte("data")); err != nil {
			t.Fatalf("Write with directory fsync refused by %v = %v, want nil", errno, err)
		}
		if _, err := os.Stat(filepath.Join(b.basePath, durableKey)); err != nil {
			t.Fatal(err)
		}
	}
	b := newDurableBackend(t)
	recordSyncs(t, syscall.EINVAL, nil)
	if err := b.Write(context.Background(), durableKey, []byte("data")); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("Write with file fsync refused = %v, want EINVAL", err)
	}
}

// Each directory the write creates is synced into its parent; directories
// already known durable are not synced again.
func TestEnsureDirSyncsEachNewLevelIntoItsParent(t *testing.T) {
	b := newDurableBackend(t)
	r := recordSyncs(t, nil, nil)
	root := b.basePath
	j := func(p string) string { return filepath.Join(root, filepath.FromSlash(p)) }

	if err := b.Write(context.Background(), durableKey, []byte("data")); err != nil {
		t.Fatal(err)
	}
	want := []string{root, j("db"), j("db/m"), j("db/m/2026"), j("db/m/2026/11"), j("db/m/2026/11/12"), j("db/m/2026/11/12/10")}
	if strings.Join(r.dirs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("first write synced\n%s\nwant\n%s", strings.Join(r.dirs, "\n"), strings.Join(want, "\n"))
	}

	r.dirs = nil
	if err := b.Write(context.Background(), "db/m/2026/11/12/10/g.parquet", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.dirs, "\n") != j("db/m/2026/11/12/10") {
		t.Fatalf("a write into a known directory synced %v, want only that directory", r.dirs)
	}

	r.dirs = nil
	if err := b.Write(context.Background(), "db/m/2026/11/12/11/f.parquet", []byte("data")); err != nil {
		t.Fatal(err)
	}
	want = []string{j("db/m/2026/11/12"), j("db/m/2026/11/12/11")}
	if strings.Join(r.dirs, "\n") != strings.Join(want, "\n") {
		t.Fatalf("a new hour synced %v, want %v", r.dirs, want)
	}
}

// A directory that already exists on disk but is not cached (a restart) still
// has its parent synced: whoever created it may not have.
func TestEnsureDirSyncsParentOfAnExistingUncachedDirectory(t *testing.T) {
	b := newDurableBackend(t)
	hour := filepath.Join(b.basePath, filepath.FromSlash("db/m/2026/11/12/10"))
	if err := os.MkdirAll(hour, 0700); err != nil {
		t.Fatal(err)
	}
	r := recordSyncs(t, nil, nil)
	if err := b.ensureDir(hour); err != nil {
		t.Fatal(err)
	}
	if len(r.dirs) != 6 { // root, db, m, 2026, 11, 12
		t.Fatalf("synced %v, want the six parents", r.dirs)
	}
}

// Concurrent writers that miss the cache for the same new directory all
// succeed, and none returns before the directory is synced into its parent.
func TestEnsureDirConcurrentMissesShareOneDurableCreation(t *testing.T) {
	b := newDurableBackend(t)
	r := recordSyncs(t, nil, nil)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "db/m/2026/11/12/10/f" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".parquet"
			errs <- b.Write(context.Background(), key, []byte("data"))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	parentOfHour := filepath.Join(b.basePath, filepath.FromSlash("db/m/2026/11/12"))
	n := 0
	for _, d := range r.dirs {
		if d == parentOfHour {
			n++
		}
	}
	if n == 0 {
		t.Fatal("the new hour directory was never synced into its parent")
	}
}

// A cached ancestor removed under the backend (retention, an operator) is
// recreated, not reported as a failure.
func TestEnsureDirRecoversFromARemovedCachedAncestor(t *testing.T) {
	b := newDurableBackend(t)
	if err := b.Write(context.Background(), durableKey, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(b.basePath, "db", "m")); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(context.Background(), "db/m/2026/11/12/11/f.parquet", []byte("data")); err != nil {
		t.Fatalf("write after an ancestor was removed: %v", err)
	}
	if err := b.Write(context.Background(), durableKey, []byte("data")); err != nil {
		t.Fatalf("write into the removed directory: %v", err)
	}
}

func assertDirHoldsNothing(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("%s should hold nothing after a failed write, holds %v", dir, names)
	}
}
