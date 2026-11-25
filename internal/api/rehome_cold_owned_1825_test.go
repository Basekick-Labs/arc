package api

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1825: a re-home job can own a file that is in cold storage. Cold DELETE
// and cold retention leave it, in every branch that touches a file.

func TestColdDeleteLeavesAFileAReHomeJobOwns1825(t *testing.T) {
	r := newColdDeleteRig(t)
	r.coldFile(coldWhole, "h0", "h0")
	owned := true
	r.h.SetRehomeOwns(func(path string) bool { return owned && path == coldWhole })

	code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`)
	if !r.coldExists(coldWhole) {
		t.Fatalf("a cold file a job owns was deleted (%d %+v)", code, out)
	}
	if code != 207 || !strings.Contains(out.Error, "being re-homed") {
		t.Fatalf("got %d %q; want 207 naming the job", code, out.Error)
	}
	if len(r.tier.retired) != 0 {
		t.Fatalf("its row was retired: %v", r.tier.retired)
	}
	// Once the job has ended the same DELETE removes it.
	owned = false
	if code, out := r.delete(`{"database":"db","measurement":"cpu","where":"host = 'h0'","confirm":true}`); code != 200 || r.coldExists(coldWhole) {
		t.Fatalf("after the job: %d %+v, file there %v", code, out, r.coldExists(coldWhole))
	}
}

// A row whose object the listing did not show: retired or pointed back at a
// hot copy by a DELETE, unless a job owns the file.
func TestRunColdLeavesTheRowOfAFileAReHomeJobOwns1825(t *testing.T) {
	r := newColdDeleteRig(t)
	r.h.SetRehomeOwns(func(path string) bool { return path == coldWhole })
	c := &coldPass{backend: r.cold, missingRows: []string{coldWhole}} // no object in cold storage
	r.h.runCold(context.Background(), c, "host = 'h0'")
	if len(r.tier.retired) != 0 || len(r.tier.reverted) != 0 {
		t.Fatalf("retired %v reverted %v; the job retires or keeps its own file's row", r.tier.retired, r.tier.reverted)
	}
	r.h.SetRehomeOwns(nil)
	r.h.runCold(context.Background(), c, "host = 'h0'")
	if len(r.tier.retired) != 1 {
		t.Fatalf("with no job: retired %v, want the row of an object that is gone", r.tier.retired)
	}
}

// Cold retention holds the registry's claim on each file from before it
// looks at it until it moves on, and leaves a file a job owns.
func TestColdRetentionLeavesAFileAReHomeJobOwns1825(t *testing.T) {
	const (
		ownedKey = "testdb/cpu/2020/01/01/00/owned.parquet"
		freeKey  = "testdb/cpu/2020/01/01/00/free.parquet"
		goneKey  = "testdb/cpu/2020/01/01/00/gone.parquet" // a row with no object
	)
	r := newColdRetentionRig(t)
	ownedPath := r.cold(t, "cpu", ownedKey, expiredTS)
	freePath := r.cold(t, "cpu", freeKey, expiredTS)
	r.row("cpu", goneKey)

	owned := map[string]bool{ownedKey: true, goneKey: true}
	held := map[string]int{}
	var maxHeld int
	r.h.SetRehomeClaim(func(path string) (func(), bool) {
		if owned[path] {
			return func() {}, false
		}
		held[path]++
		if n := len(held); n > maxHeld {
			maxHeld = n
		}
		return func() {
			if held[path]--; held[path] == 0 {
				delete(held, path)
			}
		}, true
	})
	// The claim is held while the file is deleted.
	r.h.coldDeleteHook = func(ctx context.Context, b storage.Backend, path string) error {
		if held[path] != 1 {
			t.Errorf("%s deleted with its claim held %d time(s), want 1", path, held[path])
		}
		return b.Delete(ctx, path)
	}
	policy := r.policy(t, "")
	if _, err := r.h.ExecutePolicy(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ownedPath); err != nil {
		t.Fatalf("a cold file a job owns was deleted: %v", err)
	}
	if _, err := os.Stat(freePath); !os.IsNotExist(err) {
		t.Fatalf("the file no job owns was not deleted: %v", err)
	}
	for _, p := range append(append([]string{}, r.tier.retired...), r.tier.reverted...) {
		if owned[p] {
			t.Fatalf("the row of %s, which a job owns, was retired or reverted", p)
		}
	}
	if len(held) != 0 || maxHeld > 1 {
		t.Fatalf("claims left held: %v; most held at once: %d", held, maxHeld)
	}
	// With no job, the row whose object is gone is retired as before.
	owned = map[string]bool{}
	if _, err := r.h.ExecutePolicy(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	retired := strings.Join(r.tier.retired, " ")
	if !strings.Contains(retired, goneKey) || !strings.Contains(retired, ownedKey) {
		t.Fatalf("after the job: retired %v", r.tier.retired)
	}
}
