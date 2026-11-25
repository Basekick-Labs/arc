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

// One volume for scratch and hot storage: the needs add up (3x); two
// volumes: 2x on scratch and 1x on hot, each on its own.
func TestRoomForJobAddsUpOnOneVolume1851(t *testing.T) {
	old := rehomeDiskSpace
	t.Cleanup(func() { rehomeDiskSpace = old })
	r := newSwapRig(t)
	const mib = 1 << 20
	size := int64(100 * mib)
	// 300 MiB free: enough for 2x + margin, not for 3x + margin.
	rehomeDiskSpace = func(string) (uint64, uint64, error) { return 1 << 40, 300 * mib, nil }
	if why := r.h.roomForJob(size); !strings.Contains(why, "scratch and hot storage volume") {
		t.Fatalf("one volume: %q", why)
	}
	scratch := r.h.scratchRoot
	rehomeDiskSpace = func(dir string) (uint64, uint64, error) {
		if dir == scratch {
			return 1 << 40, 300 * mib, nil
		}
		return 1 << 41, 200 * mib, nil
	}
	if why := r.h.roomForJob(size); why != "" {
		t.Fatalf("two volumes with room: %q", why)
	}
	rehomeDiskSpace = func(dir string) (uint64, uint64, error) {
		if dir == scratch {
			return 1 << 40, 300 * mib, nil
		}
		return 1 << 41, 150 * mib, nil
	}
	if why := r.h.roomForJob(size); !strings.Contains(why, "the hot storage volume") {
		t.Fatalf("hot volume short: %q", why)
	}
}
