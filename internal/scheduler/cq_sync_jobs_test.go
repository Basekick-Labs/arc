package scheduler

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/rs/zerolog"
)

func runningCQScheduler(t *testing.T) *CQScheduler {
	t.Helper()
	s, err := NewCQScheduler(&CQSchedulerConfig{Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	s.running = true // Start needs a licence and a handler; SyncJobs needs neither.
	t.Cleanup(s.Stop)
	return s
}

func (s *CQScheduler) jobFor(id int64) *cqJob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jobs[id]
}

func waitJob(t *testing.T, s *CQScheduler, id int64, interval time.Duration) *cqJob {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if j := s.jobFor(id); j != nil && j.interval == interval {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %d never ran at %v (now %+v)", id, interval, s.jobFor(id))
	return nil
}

// SyncJobs makes the jobs match the cluster's CQs, and restarts only a job
// whose interval changed: every other job keeps its ticker, so a long
// interval still fires.
func TestSyncJobs(t *testing.T) {
	s := runningCQScheduler(t)
	s.SyncJobs([]api.CQJobDef{{ID: 1, Name: "a", Interval: "1h", IsActive: true}, {ID: 2, Name: "b", Interval: "1h", IsActive: true}, {ID: 3, Name: "off", Interval: "1h"}})
	a := waitJob(t, s, 1, time.Hour)
	waitJob(t, s, 2, time.Hour)
	if s.jobFor(3) != nil {
		t.Fatal("an inactive CQ got a job")
	}
	// A rename keeps the job; an interval change replaces it.
	s.SyncJobs([]api.CQJobDef{{ID: 1, Name: "a2", Interval: "1h", IsActive: true}, {ID: 2, Name: "b", Interval: "2h", IsActive: true}})
	if j := s.jobFor(1); j != a {
		t.Fatal("a rename restarted the job (its ticker phase reset)")
	}
	waitJob(t, s, 2, 2*time.Hour)
	// Two changes in a row: the job ends at the last one.
	s.SyncJobs([]api.CQJobDef{{ID: 1, Name: "a2", Interval: "1h", IsActive: true}, {ID: 2, Name: "b", Interval: "3h", IsActive: true}})
	s.SyncJobs([]api.CQJobDef{{ID: 1, Name: "a2", Interval: "1h", IsActive: true}, {ID: 2, Name: "b", Interval: "4h", IsActive: true}})
	waitJob(t, s, 2, 4*time.Hour)
	time.Sleep(50 * time.Millisecond)
	if j := s.jobFor(2); j == nil || j.interval != 4*time.Hour {
		t.Fatalf("job 2 at %+v, want the last interval", j)
	}
	// Gone and deactivated CQs stop.
	s.SyncJobs([]api.CQJobDef{{ID: 1, Name: "a2", Interval: "1h"}})
	time.Sleep(50 * time.Millisecond)
	if s.JobCount() != 0 {
		t.Fatalf("%d jobs left", s.JobCount())
	}
}

// Not running (no licence, or stopped): SyncJobs starts nothing.
func TestSyncJobsWhenNotRunning(t *testing.T) {
	s, _ := NewCQScheduler(&CQSchedulerConfig{Logger: zerolog.Nop()})
	s.SyncJobs([]api.CQJobDef{{ID: 1, Name: "a", Interval: "1h", IsActive: true}})
	if s.JobCount() != 0 {
		t.Fatal("a stopped scheduler started a job")
	}
}

// A second start of the same CQ (a reload racing SyncJobs) replaces the job
// instead of orphaning it, so Stop still stops every job; a stopped
// scheduler starts nothing.
func TestStartJobReplacesInsteadOfOrphaning(t *testing.T) {
	s := runningCQScheduler(t)
	s.mu.Lock()
	_ = s.startJob(1, "a", "1h")
	first := s.jobs[1]
	_ = s.startJob(1, "a", "1h")
	s.mu.Unlock()
	select {
	case <-first.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the replaced job was left running")
	}
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung on an orphaned job")
	}
	s.mu.Lock()
	_ = s.startJob(2, "b", "1h")
	s.mu.Unlock()
	if s.JobCount() != 0 {
		t.Fatal("a stopped scheduler started a job")
	}
}
