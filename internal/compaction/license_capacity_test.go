package compaction

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

type capacityScheduleGate struct{ stubGate }

func (*capacityScheduleGate) CanScheduleCompaction() bool { return true }

func TestLicenseCapacityCompactorCronSurvivesTemporaryFence(t *testing.T) {
	gate := &capacityScheduleGate{stubGate{role: "compactor"}}
	scheduler, err := NewScheduler(&SchedulerConfig{Enabled: true, Schedule: "0 0 1 1 *", ClusterGate: gate, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	defer scheduler.Stop()
	if !scheduler.IsRunning() {
		t.Fatal("temporary capacity fence permanently disabled cron")
	}
	// The manager is deliberately nil. Execution reaching it would panic;
	// both scheduled and manual work must stop at the dynamic capacity gate.
	scheduler.runCompaction()
	if _, err := scheduler.TriggerNow(context.Background()); !errors.Is(err, ErrCompactionRoleGated) {
		t.Fatalf("manual compaction bypassed capacity fence: %v", err)
	}
}
