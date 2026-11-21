package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/rs/zerolog"
)

// These tests override package variables (mkdirFn, renameFile, mkdirAllFn,
// readDirFn, dirGoneGOOS), so none of them may call t.Parallel().

// ensureDir walks the chain again after each strike, up to the directory's
// depth below the root, not once (#1804): a remover climbing toward the root
// can strike once per level it takes.
func TestEnsureDirRetriesOncePerLevel(t *testing.T) {
	const key = "db/m/2026/11/21/10/f.parquet" // 6 levels below the root
	for _, c := range []struct {
		strikes int
		wantErr bool
	}{{1, false}, {3, false}, {6, false}, {7, true}} {
		b, root := adoptRig(t)
		top := filepath.Join(root, "db")
		left := c.strikes
		orig := mkdirFn
		mkdirFn = func(name string, perm os.FileMode) error {
			if name == top && left > 0 {
				left--
				return &os.PathError{Op: "mkdir", Path: name, Err: syscall.ENOENT}
			}
			return orig(name, perm)
		}
		err := b.Write(context.Background(), key, []byte("x"))
		mkdirFn = orig
		if (err != nil) != c.wantErr {
			t.Errorf("%d strikes: Write error = %v, want error %v", c.strikes, err, c.wantErr)
		}
	}
}

// probeClimbRemove removes dir and its parents up to the root through the
// backend, the way tiering's CleanupEmptyDirectories climbs.
func probeClimbRemove(ctx context.Context, b *LocalBackend, dir string) {
	for d := dir; d != "." && d != ""; d = filepath.Dir(d) {
		if b.RemoveDirectory(ctx, d) != nil {
			return
		}
	}
}

// A write into a partition whose directories a climbing remover is taking,
// all the way up to the database: with one retry, most of these failed on
// macOS (#1804). Real removers, real syscalls; the remover starts at the
// writer's first mkdir.
func TestWriteSurvivesAClimbingRemover(t *testing.T) {
	b, _ := adoptRig(t)
	ctx := context.Background()
	const leaf = "db/m/2026/11/21/10"
	orig := mkdirFn
	t.Cleanup(func() { mkdirFn = orig })
	for i := 0; i < 60; i++ {
		if err := b.Write(ctx, leaf+"/seed", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(ctx, leaf+"/seed"); err != nil {
			t.Fatal(err)
		}
		// Cold cache, as after a real remover's RemoveDirectory.
		b.forgetDir(filepath.Join(b.GetBasePath(), filepath.FromSlash(leaf)))
		var once sync.Once
		var wg sync.WaitGroup
		mkdirFn = func(name string, perm os.FileMode) error {
			once.Do(func() {
				wg.Add(1)
				go func() { defer wg.Done(); probeClimbRemove(ctx, b, leaf) }()
			})
			return orig(name, perm)
		}
		err := b.Write(ctx, leaf+"/w", []byte("y"))
		wg.Wait()
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		_ = b.Delete(ctx, leaf+"/w")
	}
}

func TestMoveObjectRetriesARemovedDirectory(t *testing.T) {
	for _, c := range []struct {
		name      string
		goos      string
		errs      []error
		wantCalls int
		wantErr   bool
	}{
		{"ENOENT then ok", "linux", []error{syscall.ENOENT}, 2, false},
		{"darwin EINVAL then ok", "darwin", []error{syscall.EINVAL}, 2, false},
		{"linux EINVAL", "linux", []error{syscall.EINVAL}, 1, true},
		{"ENOENT twice", "linux", []error{syscall.ENOENT, syscall.ENOENT}, 2, true},
		{"EXDEV", "darwin", []error{syscall.EXDEV}, 1, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			asGOOS(t, c.goos)
			b, root := adoptRig(t)
			ctx := context.Background()
			if err := b.Write(ctx, "_staged/job/f", []byte("y")); err != nil {
				t.Fatal(err)
			}
			calls := 0
			orig := renameFile
			renameFile = func(from, to string) error {
				calls++
				if calls <= len(c.errs) {
					return &os.LinkError{Op: "rename", Old: from, New: to, Err: c.errs[calls-1]}
				}
				return orig(from, to)
			}
			t.Cleanup(func() { renameFile = orig })

			err := b.MoveObject(ctx, "_staged/job/f", "db/m/2026/11/21/10/f")
			if calls != c.wantCalls {
				t.Errorf("rename called %d times, want %d", calls, c.wantCalls)
			}
			if (err != nil) != c.wantErr {
				t.Fatalf("MoveObject error = %v, want error %v", err, c.wantErr)
			}
			if !c.wantErr {
				if got, _ := os.ReadFile(filepath.Join(root, "db/m/2026/11/21/10/f")); string(got) != "y" {
					t.Fatalf("moved object holds %q", got)
				}
			}
		})
	}
}

func armedSpellingRig(t *testing.T) *LocalBackend {
	t.Helper()
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	b.spelling.armed = true
	return b
}

// AdmitNameSpelling creates a new name's directories; a database delete that
// removes <database> mid-call failed it, and the caller admitted the write
// without the spelling check (#1804). It now creates them again, once.
func TestAdmitNameSpellingRetriesARemovedDatabase(t *testing.T) {
	for _, c := range []struct {
		name    string
		goos    string
		errs    []error
		wantErr bool
	}{
		{"ENOENT then ok", "linux", []error{syscall.ENOENT}, false},
		{"darwin EINVAL then ok", "darwin", []error{syscall.EINVAL}, false},
		{"linux EINVAL", "linux", []error{syscall.EINVAL}, true},
		{"ENOENT twice", "linux", []error{syscall.ENOENT, syscall.ENOENT}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			asGOOS(t, c.goos)
			b := armedSpellingRig(t)
			calls := 0
			orig := mkdirAllFn
			mkdirAllFn = func(p string, perm os.FileMode) error {
				calls++
				if calls <= len(c.errs) {
					return &os.PathError{Op: "mkdir", Path: p, Err: c.errs[calls-1]}
				}
				return orig(p, perm)
			}
			t.Cleanup(func() { mkdirAllFn = orig })

			err := b.AdmitNameSpelling("db", "cpu")
			if (err != nil) != c.wantErr {
				t.Fatalf("AdmitNameSpelling error = %v, want error %v", err, c.wantErr)
			}
			if !c.wantErr {
				if _, serr := os.Stat(filepath.Join(b.GetBasePath(), "db", "cpu")); serr != nil {
					t.Fatalf("directories not created: %v", serr)
				}
			}
		})
	}
}

// The retry still runs the spelling check: another spelling created during
// the retry window is refused, not admitted.
func TestAdmitNameSpellingRetryStillChecksTheSpelling(t *testing.T) {
	b := foldingBackend(t)
	root := b.GetBasePath()
	calls := 0
	orig := mkdirAllFn
	mkdirAllFn = func(p string, perm os.FileMode) error {
		calls++
		if calls == 1 {
			// The database is removed and another spelling of the
			// measurement arrives before the retry.
			if err := os.MkdirAll(filepath.Join(root, "db", "CPU"), 0o700); err != nil {
				t.Fatal(err)
			}
			return &os.PathError{Op: "mkdir", Path: p, Err: syscall.ENOENT}
		}
		return orig(p, perm)
	}
	t.Cleanup(func() { mkdirAllFn = orig })
	wantSpellingErr(t, b.AdmitNameSpelling("db", "cpu"), "measurement", "CPU")
}

// A listing of a directory removed after its Stat found it means nothing is
// there, as a Stat that finds nothing does, not an error the caller admits
// the write on without a check (#1804).
func TestSpellingEntryRemovedBetweenStatAndList(t *testing.T) {
	b := armedSpellingRig(t)
	mkdirs(t, b, "db")
	orig := readDirFn
	readDirFn = func(name string) ([]os.DirEntry, error) {
		if strings.HasSuffix(name, string(filepath.Separator)+"db") {
			return nil, &os.PathError{Op: "open", Path: name, Err: syscall.ENOENT}
		}
		return orig(name)
	}
	t.Cleanup(func() { readDirFn = orig })
	missing, err := b.checkSpelling([]string{"db", "cpu"})
	if err != nil || !missing {
		t.Fatalf("checkSpelling = missing %v, err %v; want missing, no error", missing, err)
	}
}
