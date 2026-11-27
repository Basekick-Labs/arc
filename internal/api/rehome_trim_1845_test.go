package api

import (
	"fmt"
	"testing"
)

// #1845: a running job is not dropped from the job list by newer ones.
func TestTrimJobsKeepsARunningJob(t *testing.T) {
	h := &RehomeHandler{jobs: map[string]*rehomeJob{}}
	add := func(id, state string) {
		h.jobs[id] = &rehomeJob{ID: id, State: state}
		h.order = append(h.order, id)
	}
	add("waiting", "running")
	for i := 0; i < rehomeJobsKept+10; i++ {
		add(fmt.Sprintf("dry%d", i), "done")
	}
	h.trimJobsLocked()
	if _, ok := h.jobs["waiting"]; !ok || h.order[0] != "waiting" {
		t.Fatal("the running job was dropped")
	}
	if len(h.order) != rehomeJobsKept || len(h.jobs) != rehomeJobsKept {
		t.Fatalf("kept %d/%d, want %d", len(h.order), len(h.jobs), rehomeJobsKept)
	}
}
