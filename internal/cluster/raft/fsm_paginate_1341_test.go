package raft

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1341: a manifest walk must never hold the FSM write lock while it sorts,
// and must sort at most once per walk instead of on nearly every page.

func pagPath(i int) string { return fmt.Sprintf("db/m/2026/04/11/14/f%05d.parquet", i) }

// registerPath reports with t.Error so it can run off the test goroutine.
func registerPath(t *testing.T, f *ClusterFSM, idx uint64, path string, size int64) {
	t.Helper()
	if res := applyCmd(t, f, idx, CommandRegisterFile, RegisterFilePayload{File: makeFileEntry(path, "db", "m", size)}); res != nil {
		t.Errorf("register %s: %v", path, res)
	}
}

// walkAll walks the manifest and checks a walk never repeats or goes back:
// paths strictly ascend, whichever snapshot each page used. Reports with
// t.Error so it can run off the test goroutine.
func walkAll(t *testing.T, f *ClusterFSM, limit int, between func(page int)) []string {
	t.Helper()
	var got []string
	cursor := ""
	for page := 0; page < 10000; page++ {
		files, next, err := f.GetFilesPaginated(cursor, limit)
		if err != nil {
			t.Error(err)
			return got
		}
		for _, e := range files {
			if len(got) > 0 && e.Path <= got[len(got)-1] {
				t.Errorf("walk went back or repeated: %s after %s", e.Path, got[len(got)-1])
			}
			got = append(got, e.Path)
		}
		if next == "" {
			return got
		}
		cursor = next
		if between != nil {
			between(page)
		}
	}
	t.Error("walk did not end")
	return got
}

// countingSorter counts rebuilds.
func countingSorter(f *ClusterFSM) *atomic.Int32 {
	var n atomic.Int32
	f.sortKeys = func(k []string) { n.Add(1); sort.Strings(k) }
	return &n
}

// The issue's regression: an apply must not wait for the sort. The sort is
// parked; a registration of a new path (which needs the write lock) must
// complete while it is.
func TestManifestWalkSortHoldsNoFSMLock(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	for i := 0; i < 50; i++ {
		registerPath(t, f, uint64(i+1), pagPath(i), 10)
	}
	parked, release := make(chan struct{}), make(chan struct{})
	f.sortKeys = func(k []string) {
		close(parked)
		<-release
		sort.Strings(k)
	}
	walked := make(chan struct{})
	go func() {
		defer close(walked)
		if _, _, err := f.GetFilesPaginated("", 10); err != nil {
			t.Error(err)
		}
	}()
	<-parked

	applied := make(chan struct{})
	go func() {
		defer close(applied)
		registerPath(t, f, 100, pagPath(500), 10)
	}()
	select {
	case <-applied:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("a Raft apply waited for the manifest sort: the walk holds an FSM lock across it")
	}
	close(release)
	<-walked
}

func TestManifestWalkSortsOncePerWalk(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	for i := 0; i < 100; i++ {
		registerPath(t, f, uint64(i+1), pagPath(i*2), 10)
	}
	sorts := countingSorter(f)
	idx := uint64(1000)
	got := walkAll(t, f, 10, func(int) {
		// Ingest during the walk: new keys on every page.
		idx++
		registerPath(t, f, idx, pagPath(int(idx)), 10)
	})
	if n := sorts.Load(); n != 1 {
		t.Fatalf("walk sorted %d times, want once", n)
	}
	if len(got) != 100 {
		t.Fatalf("walk returned %d keys, want the 100 present at its start", len(got))
	}
	// The next walk sees the keys added during this one.
	got = walkAll(t, f, 10, nil)
	if n := sorts.Load(); n != 2 {
		t.Fatalf("second walk: %d sorts total, want 2", n)
	}
	if len(got) != 100+int(idx-1000) {
		t.Fatalf("second walk returned %d keys, want %d", len(got), 100+int(idx-1000))
	}
}

// A content update of a path already present changes no key: no rebuild,
// and the page returns the new content.
func TestManifestWalkContentUpdateNeedsNoRebuild(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	registerPath(t, f, 1, pagPath(1), 10)
	sorts := countingSorter(f)
	walkAll(t, f, 10, nil)
	registerPath(t, f, 2, pagPath(1), 99) // same path, new size
	files, _, err := f.GetFilesPaginated("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if n := sorts.Load(); n != 1 {
		t.Errorf("%d sorts after a content-only update, want 1", n)
	}
	if len(files) != 1 || files[0].SizeBytes != 99 {
		t.Errorf("page = %+v, want the updated entry", files)
	}
}

// Deleted keys are skipped; a walk still returns each live key exactly once.
func TestManifestWalkSkipsKeysDeletedMidWalk(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	for i := 0; i < 40; i++ {
		registerPath(t, f, uint64(i+1), pagPath(i), 10)
	}
	idx := uint64(100)
	got := walkAll(t, f, 5, func(page int) {
		if page == 0 {
			for i := 20; i < 30; i++ {
				idx++
				if res := applyCmd(t, f, idx, CommandDeleteFile, DeleteFilePayload{Path: pagPath(i)}); res != nil {
					t.Fatal(res)
				}
			}
		}
	})
	seen := map[string]int{}
	for _, p := range got {
		seen[p]++
	}
	for i := 0; i < 40; i++ {
		want := 1
		if i >= 20 && i < 30 {
			want = 0
		}
		if seen[pagPath(i)] != want {
			t.Errorf("%s seen %d times, want %d", pagPath(i), seen[pagPath(i)], want)
		}
	}
}

// Keys a snapshot install adds are announced to no callback: a walk in
// progress must still reach them, so a Restore rebuilds on any page.
func TestManifestWalkSeesKeysARestoreAdded(t *testing.T) {
	leader := NewClusterFSM(zerolog.Nop())
	f := NewClusterFSM(zerolog.Nop())
	for i := 0; i < 20; i++ {
		registerPath(t, leader, uint64(i+1), pagPath(i), 10)
		registerPath(t, f, uint64(i+1), pagPath(i), 10)
	}
	for i := 100; i < 110; i++ { // sort after every key the walk starts with
		registerPath(t, leader, uint64(i+1), pagPath(i), 10)
	}
	got := walkAll(t, f, 5, func(page int) {
		if page == 0 {
			if err := f.Restore(io.NopCloser(bytes.NewReader(persistFSM(t, leader)))); err != nil {
				t.Fatal(err)
			}
		}
	})
	seen := map[string]bool{}
	for _, p := range got {
		seen[p] = true
	}
	for i := 100; i < 110; i++ {
		if !seen[pagPath(i)] {
			t.Fatalf("%s, added by a snapshot install mid-walk, never listed", pagPath(i))
		}
	}
}

func TestManifestPaginationEdges(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	for i := 0; i < 5; i++ {
		registerPath(t, f, uint64(i+1), pagPath(i), 10)
	}
	if files, _, _ := f.GetFilesPaginated("", 0); len(files) != 5 {
		t.Errorf("limit 0: %d files, want the default page holding all 5", len(files))
	}
	if files, next, _ := f.GetFilesPaginated(pagPath(9), 10); len(files) != 0 || next != "" {
		t.Errorf("cursor past the end: %d files, next %q", len(files), next)
	}
	// A cursor on a key since deleted resumes at the next key.
	if res := applyCmd(t, f, 10, CommandDeleteFile, DeleteFilePayload{Path: pagPath(2)}); res != nil {
		t.Fatal(res)
	}
	files, _, _ := f.GetFilesPaginated(pagPath(2), 10)
	if len(files) != 2 || files[0].Path != pagPath(3) {
		t.Errorf("cursor on a deleted key: %d files, first %v", len(files), files)
	}
}

// Walkers and appliers concurrently (run with -race).
func TestManifestWalkConcurrentWithApplies(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	for i := 0; i < 200; i++ {
		registerPath(t, f, uint64(i+1), pagPath(i), 10)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		idx := uint64(1000)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			idx++
			if i%3 == 0 {
				applyCmd(t, f, idx, CommandDeleteFile, DeleteFilePayload{Path: pagPath(i % 200)})
			} else {
				registerPath(t, f, idx, pagPath(1000+i), 10)
			}
		}
	}()
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < 20; r++ {
				walkAll(t, f, 17, nil)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// Deleting keys changes the key set too: the next walk rebuilds, and does
// not page through the deleted paths.
func TestManifestWalkRebuildsAfterDeletesOnly(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	for i := 0; i < 30; i++ {
		registerPath(t, f, uint64(i+1), pagPath(i), 10)
	}
	sorts := countingSorter(f)
	walkAll(t, f, 10, nil)
	for i := 0; i < 20; i++ {
		if res := applyCmd(t, f, uint64(100+i), CommandDeleteFile, DeleteFilePayload{Path: pagPath(i)}); res != nil {
			t.Fatal(res)
		}
	}
	walkAll(t, f, 10, nil)
	if n := sorts.Load(); n != 2 {
		t.Fatalf("%d sorts, want a rebuild after deletes alone", n)
	}
	f.keysMu.Lock()
	cached := len(f.keysCache)
	f.keysMu.Unlock()
	if cached != 10 {
		t.Errorf("snapshot holds %d keys after the rebuild, want the 10 left", cached)
	}
}
