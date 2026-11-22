package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These tests override package variables (staleBeforeAside, staleAfterAside,
// staleLinkFn, staleKeepRename), so none of them may call t.Parallel().

// staleReadyToUnlink registers key, has the sweep see it, and unregisters it,
// so the next pass unlinks it.
func staleReadyToUnlink(r *staleRigT, key, body string) {
	r.register(key, body)
	r.pass()
	r.unregister(key)
	r.pass()
}

func asideEntries(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	_ = filepath.WalkDir(filepath.Join(root, staleLocalAsideDir), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(filepath.Join(root, staleLocalAsideDir), p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	return out
}

func crashAfterAside(t *testing.T) {
	t.Helper()
	staleAfterAside = func(string) bool { return true }
	t.Cleanup(func() { staleAfterAside = nil })
}

// A crash right after the stale copy was moved aside: the next start removes
// it (#1814).
func TestStaleAsideCrashStaleCopyRemoved(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	staleReadyToUnlink(r, key, "rows a")
	crashAfterAside(t)
	r.pass()
	if r.exists(key) {
		t.Fatal("the stale copy is still at its path after the rename")
	}
	if got := asideEntries(t, r.root); len(got) != 1 || !strings.HasSuffix(got[0], "/"+key) {
		t.Fatalf("aside after the crash = %v, want <tag>/%s", got, key)
	}
	staleAfterAside = nil
	r.c.recoverStaleAsides(r.root)
	if got := asideEntries(t, r.root); len(got) != 0 {
		t.Fatalf("aside after recovery = %v, want empty", got)
	}
	if left, _ := os.ReadDir(filepath.Join(r.root, staleLocalAsideDir)); len(left) != 0 {
		t.Fatalf("aside root not emptied: %v", left)
	}
}

// A crash right after a file written to the path in the window was moved
// aside: the next start keeps it for an operator, never deletes it and never
// puts it back without the manifest (#1814). Before, the start deleted it.
func TestStaleAsideCrashReplacementKept(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	full := filepath.Join(r.root, key)
	staleReadyToUnlink(r, key, "rows a")
	staleBeforeAside = func(p string) {
		if p == full {
			_ = os.Remove(p)
			r.write(key, "rows A") // a pull of the path, in the window
		}
	}
	t.Cleanup(func() { staleBeforeAside = nil })
	crashAfterAside(t)
	r.pass()
	staleBeforeAside, staleAfterAside = nil, nil

	r.c.recoverStaleAsides(r.root)
	kept, _ := filepath.Glob(filepath.Join(r.root, staleLocalAsideDir, staleLocalKeepAside+"*", filepath.FromSlash(key)))
	if len(kept) != 1 {
		t.Fatalf("replacement not kept: aside = %v", asideEntries(t, r.root))
	}
	if got, _ := os.ReadFile(kept[0]); string(got) != "rows A" {
		t.Fatalf("kept file = %q, want the replacement", got)
	}
	if r.exists(key) {
		t.Fatal("the replacement was put back at start, without the manifest")
	}
	// Recovery again: the kept copy stays.
	r.c.recoverStaleAsides(r.root)
	if again, _ := filepath.Glob(filepath.Join(r.root, staleLocalAsideDir, staleLocalKeepAside+"*", filepath.FromSlash(key))); len(again) != 1 {
		t.Fatal("a kept copy did not survive a second recovery")
	}
}

// A crash after the put-back linked the file back but before the aside name
// went: the aside name is removed, the file at the path stays.
func TestStaleAsideCrashAfterPutBack(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	full := filepath.Join(r.root, key)
	r.write(key, "rows A")
	tagDir := filepath.Join(r.root, staleLocalAsideDir, staleTag(seenRecord{ino: 1, size: 1, mtime: 1}))
	aside := filepath.Join(tagDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(aside), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(full, aside); err != nil {
		t.Fatal(err)
	}
	r.c.recoverStaleAsides(r.root)
	if got := asideEntries(t, r.root); len(got) != 0 {
		t.Fatalf("aside after recovery = %v", got)
	}
	if got, _ := os.ReadFile(full); string(got) != "rows A" {
		t.Fatalf("file at the path = %q", got)
	}
}

// A tag directory with no file (the rename did not survive the crash) goes;
// an unrecognised directory is left.
func TestStaleAsideEmptyTagAndStrangers(t *testing.T) {
	r := staleRig(t)
	root := filepath.Join(r.root, staleLocalAsideDir)
	empty := filepath.Join(root, staleTag(seenRecord{ino: 2, size: 3, mtime: 4}), "testdb", "cpu")
	stranger := filepath.Join(root, "not-a-tag")
	for _, d := range []string{empty, stranger} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r.c.recoverStaleAsides(r.root)
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 || entries[0].Name() != "not-a-tag" {
		t.Fatalf("aside root after recovery = %v, want only the unrecognised directory", entries)
	}
}

// A keep that cannot be renamed leaves the tag directory as it is, and the
// next start keeps it: the replacement is never deleted on the way.
func TestStaleAsideKeepRenameFailureLosesNothing(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	full := filepath.Join(r.root, key)
	staleReadyToUnlink(r, key, "rows a")
	// In the run: replaced in the window, the put-back fails, the keep
	// rename fails.
	staleBeforeAside = func(p string) {
		if p == full {
			_ = os.Remove(p)
			r.write(key, "rows A")
		}
	}
	origLink := staleLinkFn
	staleLinkFn = func(string, string) error { return errors.New("injected link failure") }
	staleKeepRename = func(string, string) error { return errors.New("injected rename failure") }
	t.Cleanup(func() { staleBeforeAside, staleLinkFn, staleKeepRename = nil, origLink, os.Rename })
	r.pass()
	if got := asideEntries(t, r.root); len(got) != 1 || strings.HasPrefix(got[0], staleLocalKeepAside) {
		t.Fatalf("aside after a failed keep = %v, want the tag directory left", got)
	}
	// A start whose keep rename fails too: still there.
	r.c.recoverStaleAsides(r.root)
	if got := asideEntries(t, r.root); len(got) != 1 {
		t.Fatalf("aside after a recovery whose keep failed = %v", got)
	}
	// The next start can keep it.
	staleKeepRename = os.Rename
	r.c.recoverStaleAsides(r.root)
	kept, _ := filepath.Glob(filepath.Join(r.root, staleLocalAsideDir, staleLocalKeepAside+"*", filepath.FromSlash(key)))
	if len(kept) != 1 {
		t.Fatalf("not kept once the rename works: %v", asideEntries(t, r.root))
	}
	if got, _ := os.ReadFile(kept[0]); string(got) != "rows A" {
		t.Fatalf("kept file = %q", got)
	}
}

func TestStaleTagRoundTrip(t *testing.T) {
	rec := seenRecord{ino: 1234567, size: 42, mtime: 1795302300055484000}
	got, ok := parseStaleTag(staleTag(rec))
	if !ok || got.ino != rec.ino || got.size != rec.size || got.mtime != rec.mtime {
		t.Fatalf("round trip = %+v %v", got, ok)
	}
	for _, bad := range []string{"abc", "keep-x", "0123456789abcdef.1.2", "0123456789abcdef.x.2.3", "short.1.2.3"} {
		if _, ok := parseStaleTag(bad); ok {
			t.Errorf("parseStaleTag(%q) accepted", bad)
		}
	}
}
