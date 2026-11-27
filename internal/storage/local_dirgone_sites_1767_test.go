package storage

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// These tests override package variables (dirGoneGOOS, createTempFn,
// createStagingFn, mkdirFn, renameFile), so none of them may call
// t.Parallel().

// asGOOS runs the rest of the test with dirGone classifying for goos.
func asGOOS(t *testing.T, goos string) {
	t.Helper()
	orig := dirGoneGOOS
	dirGoneGOOS = goos
	t.Cleanup(func() { dirGoneGOOS = orig })
}

func einvalAt(op, path string) error {
	return &os.PathError{Op: op, Path: path, Err: syscall.EINVAL}
}

func storageErrors(t *testing.T) int64 {
	t.Helper()
	n, ok := metrics.Get().Snapshot()["storage_errors_total"].(int64)
	if !ok {
		t.Fatal("storage_errors_total missing from the metrics snapshot")
	}
	return n
}

// Each retry site, given the EINVAL macOS returns for a directory removed
// under the call: retried under the darwin rule, reported as-is under Linux's.
// Linux CI cannot produce the EINVAL, so without these a site that lost the
// darwin rule would pass there (#1767).

func TestWriteRetriesDarwinEINVAL(t *testing.T) {
	for _, c := range []struct {
		goos      string
		wantCalls int
		wantErr   bool
	}{{"darwin", 2, false}, {"linux", 1, true}} {
		t.Run(c.goos, func(t *testing.T) {
			asGOOS(t, c.goos)
			b, root := adoptRig(t)
			calls := 0
			orig := createTempFn
			createTempFn = func(dir, pattern string) (*os.File, error) {
				calls++
				if calls == 1 {
					return nil, einvalAt("open", filepath.Join(dir, ".arc-1.tmp"))
				}
				return orig(dir, pattern)
			}
			t.Cleanup(func() { createTempFn = orig })

			err := b.Write(context.Background(), "db/cpu/f.parquet", []byte("x"))
			if calls != c.wantCalls {
				t.Errorf("create called %d times, want %d", calls, c.wantCalls)
			}
			if gotErr := err != nil; gotErr != c.wantErr {
				t.Fatalf("Write error = %v, want error %v", err, c.wantErr)
			}
			if !c.wantErr {
				if got, _ := os.ReadFile(filepath.Join(root, "db/cpu/f.parquet")); string(got) != "x" {
					t.Fatalf("written file holds %q", got)
				}
			} else if !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("Write error %v does not carry the EINVAL", err)
			}
		})
	}
}

func TestWriteReaderRetriesDarwinEINVAL(t *testing.T) {
	for _, c := range []struct {
		goos      string
		wantCalls int
		wantErr   bool
	}{{"darwin", 2, false}, {"linux", 1, true}} {
		t.Run(c.goos, func(t *testing.T) {
			asGOOS(t, c.goos)
			b, root := adoptRig(t)
			calls := 0
			orig := createStagingFn
			createStagingFn = func(path string) (*os.File, error) {
				calls++
				if calls == 1 {
					return nil, einvalAt("open", path)
				}
				return orig(path)
			}
			t.Cleanup(func() { createStagingFn = orig })

			err := b.WriteReader(context.Background(), "db/cpu/f.parquet", bytes.NewReader([]byte("x")), 1)
			if calls != c.wantCalls {
				t.Errorf("staging create called %d times, want %d", calls, c.wantCalls)
			}
			if gotErr := err != nil; gotErr != c.wantErr {
				t.Fatalf("WriteReader error = %v, want error %v", err, c.wantErr)
			}
			if !c.wantErr {
				if got, _ := os.ReadFile(filepath.Join(root, "db/cpu/f.parquet")); string(got) != "x" {
					t.Fatalf("written file holds %q", got)
				}
			}
		})
	}
}

func TestEnsureDirRetriesDarwinEINVAL(t *testing.T) {
	for _, c := range []struct {
		goos    string
		wantErr bool
	}{{"darwin", false}, {"linux", true}} {
		t.Run(c.goos, func(t *testing.T) {
			asGOOS(t, c.goos)
			b, root := adoptRig(t)
			leaf := filepath.Join(root, "db", "cpu")
			failed := false
			orig := mkdirFn
			mkdirFn = func(name string, perm os.FileMode) error {
				if name == leaf && !failed {
					failed = true
					return einvalAt("mkdir", name)
				}
				return orig(name, perm)
			}
			t.Cleanup(func() { mkdirFn = orig })

			err := b.Write(context.Background(), "db/cpu/f.parquet", []byte("x"))
			if !failed {
				t.Fatal("the injected mkdir failure never ran")
			}
			if gotErr := err != nil; gotErr != c.wantErr {
				t.Fatalf("Write error = %v, want error %v", err, c.wantErr)
			}
		})
	}
}

// A directory removed between createDurable's Mkdir (EEXIST) and its Stat
// returned "exists and is not a directory", with no error wrapped, so
// ensureDir never retried. Platform-independent.
func TestEnsureDirRetriesWhenRemovedBetweenMkdirAndStat(t *testing.T) {
	b, root := adoptRig(t)
	leaf := filepath.Join(root, "db", "cpu")
	raced := false
	orig := mkdirFn
	mkdirFn = func(name string, perm os.FileMode) error {
		if name == leaf && !raced {
			raced = true // it "existed" and was removed before the Stat
			return &os.PathError{Op: "mkdir", Path: name, Err: syscall.EEXIST}
		}
		return orig(name, perm)
	}
	t.Cleanup(func() { mkdirFn = orig })

	if err := b.Write(context.Background(), "db/cpu/f.parquet", []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !raced {
		t.Fatal("the injected EEXIST never ran")
	}
}

// AdoptFile after an EINVAL whose directory cannot be recreated: a hard
// error, counted, not the copy fallback, whose own write would fail in the
// same directory.
func TestAdoptFileDarwinEINVALDirectoryNotRecreated(t *testing.T) {
	asGOOS(t, "darwin")
	b, root := adoptRig(t)
	key := "db/cpu/2026/11/21/10/out_compacted.parquet"
	leaf := filepath.Dir(filepath.Join(root, key))
	if err := b.Write(context.Background(), "db/cpu/2026/11/21/10/warm", []byte("x")); err != nil {
		t.Fatal(err)
	}
	src, _ := writeTempOutput(t, 0o644)
	origRename, origMkdir := renameFile, mkdirFn
	renameFile = func(from, to string) error {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: syscall.EINVAL}
	}
	mkdirFn = func(name string, perm os.FileMode) error {
		if name == leaf {
			return &os.PathError{Op: "mkdir", Path: name, Err: syscall.EACCES}
		}
		return origMkdir(name, perm)
	}
	t.Cleanup(func() { renameFile, mkdirFn = origRename, origMkdir })

	before := storageErrors(t)
	err := b.AdoptFile(context.Background(), key, src)
	if err == nil || errors.Is(err, ErrAdoptUnsupported) || !errors.Is(err, syscall.EACCES) {
		t.Fatalf("AdoptFile = %v, want the EACCES from recreating the directory, not ErrAdoptUnsupported", err)
	}
	if storageErrors(t) == before {
		t.Error("storage error not counted")
	}
}

// A symlink to nothing where a level should be: Mkdir says EEXIST and Stat
// says ENOENT, but something is there, so the refusal names it.
func TestEnsureDirRefusesADanglingSymlink(t *testing.T) {
	b, root := adoptRig(t)
	if err := os.MkdirAll(filepath.Join(root, "db"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "nowhere"), filepath.Join(root, "db", "cpu")); err != nil {
		t.Fatal(err)
	}
	err := b.Write(context.Background(), "db/cpu/f.parquet", []byte("x"))
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Write through a dangling symlink returned %v, want the not-a-directory refusal", err)
	}
}

func TestEnsureDirStillRefusesAFileInTheWay(t *testing.T) {
	b, root := adoptRig(t)
	if err := os.MkdirAll(filepath.Join(root, "db"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "db", "cpu"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := b.Write(context.Background(), "db/cpu/f.parquet", []byte("x"))
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Write over a file in the way returned %v, want the not-a-directory refusal", err)
	}
}

// AdoptFile: an EINVAL is retried under the darwin rule, and when the retry
// is refused too the job still copies instead (ErrAdoptUnsupported) rather
// than failing, as any non-ENOENT refusal did before the retry existed.
func TestAdoptFileDarwinEINVAL(t *testing.T) {
	key := "db/cpu/2026/11/21/10/out_compacted.parquet"
	for _, c := range []struct {
		name        string
		goos        string
		errs        []error // returned by successive renames; nil = real rename
		wantCalls   int
		wantAdopted bool
		wantUnsupp  bool
		wantCounted bool // storage_errors_total incremented
	}{
		{"darwin EINVAL then ok", "darwin", []error{syscall.EINVAL, nil}, 2, true, false, false},
		{"darwin EINVAL twice", "darwin", []error{syscall.EINVAL, syscall.EINVAL}, 2, false, true, false},
		{"darwin EINVAL then EXDEV", "darwin", []error{syscall.EINVAL, syscall.EXDEV}, 2, false, true, false},
		{"darwin ENOENT then EINVAL", "darwin", []error{syscall.ENOENT, syscall.EINVAL}, 2, false, false, true},
		// Lenient by design: the first refusal was EINVAL, so a failed retry
		// copies whatever it returned; ENOENT twice stays a hard error.
		{"darwin EINVAL then ENOENT", "darwin", []error{syscall.EINVAL, syscall.ENOENT}, 2, false, true, false},
		{"darwin ENOENT twice", "darwin", []error{syscall.ENOENT, syscall.ENOENT}, 2, false, false, true},
		{"linux EINVAL", "linux", []error{syscall.EINVAL}, 1, false, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			asGOOS(t, c.goos)
			b, root := adoptRig(t)
			src, payload := writeTempOutput(t, 0o644)
			calls := 0
			orig := renameFile
			renameFile = func(from, to string) error {
				calls++
				if calls <= len(c.errs) && c.errs[calls-1] != nil {
					return &os.LinkError{Op: "rename", Old: from, New: to, Err: c.errs[calls-1]}
				}
				return orig(from, to)
			}
			t.Cleanup(func() { renameFile = orig })

			before := storageErrors(t)
			err := b.AdoptFile(context.Background(), key, src)
			counted := storageErrors(t) > before
			if calls != c.wantCalls {
				t.Errorf("rename called %d times, want %d", calls, c.wantCalls)
			}
			if got := errors.Is(err, ErrAdoptUnsupported); got != c.wantUnsupp {
				t.Errorf("AdoptFile error %v: ErrAdoptUnsupported %v, want %v", err, got, c.wantUnsupp)
			}
			if (err == nil) != c.wantAdopted {
				t.Fatalf("AdoptFile error = %v, want adopted %v", err, c.wantAdopted)
			}
			if counted != c.wantCounted {
				t.Errorf("storage error counted %v, want %v", counted, c.wantCounted)
			}
			if c.wantAdopted {
				got, rerr := os.ReadFile(filepath.Join(root, key))
				if rerr != nil || !bytes.Equal(got, payload) {
					t.Fatalf("adopted object = %q, %v", got, rerr)
				}
			} else if c.wantUnsupp {
				if _, serr := os.Stat(src); serr != nil {
					t.Fatalf("source gone after ErrAdoptUnsupported; the copy fallback needs it: %v", serr)
				}
			}
		})
	}
}

// Race tests: each write overlaps one removal of its (emptied, cached)
// directory, the shape of #1425. They pass trivially on Linux, which reports
// ENOENT; on macOS they failed at the EINVAL share before #1767.

// Each hits the EINVAL within the first few iterations on macOS when its site
// lacks the darwin rule; every iteration there costs several full fsyncs.
const dirRaceIterations = 60

func TestWriteSurvivesItsDirectoryRemoved(t *testing.T) {
	b, root := adoptRig(t)
	ctx := context.Background()
	dir := filepath.Join(root, "db", "cpu")
	for i := 0; i < dirRaceIterations; i++ {
		if err := b.Write(ctx, "db/cpu/w", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(ctx, "db/cpu/w"); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var werr error
		wg.Add(2)
		go func() { defer wg.Done(); _ = os.Remove(dir) }()
		go func() { defer wg.Done(); werr = b.Write(ctx, "db/cpu/w", []byte("y")) }()
		wg.Wait()
		if werr != nil {
			t.Fatalf("iteration %d: %v", i, werr)
		}
	}
}

func TestWriteReaderSurvivesItsDirectoryRemoved(t *testing.T) {
	b, root := adoptRig(t)
	ctx := context.Background()
	dir := filepath.Join(root, "db", "cpu")
	for i := 0; i < dirRaceIterations; i++ {
		if err := b.Write(ctx, "db/cpu/w", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(ctx, "db/cpu/w"); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var werr error
		wg.Add(2)
		go func() { defer wg.Done(); _ = os.Remove(dir) }()
		go func() {
			defer wg.Done()
			werr = b.WriteReader(ctx, "db/cpu/w", bytes.NewReader([]byte("y")), 1)
		}()
		wg.Wait()
		if werr != nil {
			t.Fatalf("iteration %d: %v", i, werr)
		}
	}
}

// removeAsCallStarts starts removing dir and returns once the remover is about
// to run, so the real syscall the caller makes next overlaps the rmdir. The
// returned channel closes when the removal has returned.
func removeAsCallStarts(dir string) <-chan struct{} {
	ready, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		close(ready)
		_ = os.Remove(dir)
	}()
	<-ready
	return done
}

// AdoptFile fsyncs its source before the rename, long enough for a removal
// started beside the call to finish first; the removal starts at the rename
// instead, through the renameFile seam, against the real rename.
func TestAdoptFileSurvivesItsDirectoryRemoved(t *testing.T) {
	b, root := adoptRig(t)
	ctx := context.Background()
	dir := filepath.Join(root, "db", "cpu")
	srcDir := t.TempDir()
	orig := renameFile
	first := true
	var removed <-chan struct{}
	renameFile = func(from, to string) error {
		if first {
			first = false
			removed = removeAsCallStarts(dir)
		}
		return orig(from, to)
	}
	t.Cleanup(func() { renameFile = orig })
	for i := 0; i < dirRaceIterations; i++ {
		if err := b.Write(ctx, "db/cpu/w", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(ctx, "db/cpu/w"); err != nil {
			t.Fatal(err)
		}
		src := filepath.Join(srcDir, "out.parquet")
		if err := os.WriteFile(src, []byte("y"), 0o600); err != nil {
			t.Fatal(err)
		}
		first = true
		err := b.AdoptFile(ctx, "db/cpu/w", src)
		<-removed // no removal left over for the next iteration
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}

// A cold chain whose existing parent is removed while ensureDir creates the
// level below it: the removal starts at the child's mkdir, against the real
// mkdir.
func TestEnsureDirSurvivesItsParentRemoved(t *testing.T) {
	ctx := context.Background()
	orig := mkdirFn
	t.Cleanup(func() { mkdirFn = orig })
	for i := 0; i < dirRaceIterations; i++ {
		root := t.TempDir()
		parent := filepath.Join(root, "db")
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		b, err := NewLocalBackend(root, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		first := true
		var removed <-chan struct{}
		mkdirFn = func(name string, perm os.FileMode) error {
			if first && name == filepath.Join(parent, "cpu") {
				first = false
				removed = removeAsCallStarts(parent)
			}
			return orig(name, perm)
		}
		werr := b.Write(ctx, "db/cpu/f", []byte("y"))
		if removed != nil {
			<-removed
		}
		b.Close()
		if werr != nil {
			t.Fatalf("iteration %d: %v", i, werr)
		}
	}
}
