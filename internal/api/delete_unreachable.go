package api

import (
	"sort"
	"time"
)

// deleteGiveUpAfter is how long a DELETE run with allow_unreachable waits for
// a manifest file that is not on this node and that nothing is delivering
// (#1426). Without the flag a DELETE waits deleteLocalWait for every missing
// file, then refuses; with it, it used to wait just as long before skipping
// them, 30 s per scan for a file no node holds any more.
var deleteGiveUpAfter = 5 * time.Second

// fileArrivalChecker is the coordinator's view of what is on its way here:
// a file the replication puller has enqueued or in progress, or a staged
// compaction output (cluster.Coordinator.FileArriving).
type fileArrivalChecker interface {
	FileArriving(path string) bool
}

// missingTracker is one DELETE request's account of the manifest files it
// found missing. A file missing for deleteGiveUpAfter that nothing is
// delivering is given up: no later wait of the request waits for it again,
// and it is reported as unreachable. A file still arriving is waited for up
// to deleteLocalWait, as without the flag. Tracking each file, not the set,
// keeps files that other nodes go on registering during the DELETE from
// holding the wait open for one that will never come.
type missingTracker struct {
	arriving func(path string) bool // nil: nothing delivers files here
	since    map[string]time.Time
	givenUp  map[string]bool
}

func newMissingTracker(coordinator any) *missingTracker {
	t := &missingTracker{since: map[string]time.Time{}, givenUp: map[string]bool{}}
	if c, ok := coordinator.(fileArrivalChecker); ok && c != nil {
		t.arriving = c.FileArriving
	}
	return t
}

// waitFor returns the files of missing still worth waiting for at now,
// giving up the ones that have waited long enough with nothing delivering
// them. A nil tracker waits for all of them.
func (t *missingTracker) waitFor(missing []string, now time.Time) []string {
	if t == nil {
		return missing
	}
	var out []string
	for _, p := range missing {
		if t.givenUp[p] {
			continue
		}
		first, seen := t.since[p]
		if !seen {
			t.since[p] = now
			first = now
		}
		if now.Sub(first) >= deleteGiveUpAfter && (t.arriving == nil || !t.arriving(p)) {
			t.givenUp[p] = true
			continue
		}
		out = append(out, p)
	}
	return out
}

// withGivenUp returns missing plus the given-up files keep accepts, sorted
// and without duplicates: what the request reports as unreachable.
func (t *missingTracker) withGivenUp(missing []string, keep func(string) bool) []string {
	if t == nil || len(t.givenUp) == 0 {
		return missing
	}
	set := make(map[string]bool, len(missing)+len(t.givenUp))
	for _, p := range missing {
		set[p] = true
	}
	for p := range t.givenUp {
		if keep == nil || keep(p) {
			set[p] = true
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
