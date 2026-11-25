//go:build duckdb_arrow

package api

import (
	"strings"
	"testing"
)

// #1851 item 3: a job that would not fit on disk is refused before it copies
// anything, and the file is as it was.
func TestRehomeRefusesAJobThatWouldNotFit1851(t *testing.T) {
	old := rehomeDiskSpace
	t.Cleanup(func() { rehomeDiskSpace = old })
	rehomeDiskSpace = func(string) (uint64, uint64, error) { return 1 << 40, 1 << 20, nil } // 1 MiB free
	r := newSwapRig(t)
	id, _, _ := r.start()
	job := r.wait(id)
	if job["state"] != "refused" || !strings.Contains(job["reason"].(string), "not enough free space") {
		t.Fatalf("job: %v", job)
	}
	if !r.sourceThere() || r.reg.Active() {
		t.Fatal("a refused job changed the file or kept its record")
	}
	r.requireOnce("after the refusal")

	// With room, the same file goes through.
	rehomeDiskSpace = func(string) (uint64, uint64, error) { return 1 << 40, 1 << 39, nil }
	id, _, _ = r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job with room: %v", job)
	}
}
