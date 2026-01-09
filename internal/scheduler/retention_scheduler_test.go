package scheduler

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
)

func TestRetentionScheduler_NewRetentionScheduler(t *testing.T) {
	logger := zerolog.Nop()

	// Create scheduler without handler (will fail on Start)
	s, err := NewRetentionScheduler(&RetentionSchedulerConfig{
		RetentionHandler: nil,
		LicenseClient:    nil,
		Schedule:         "0 3 * * *",
		Logger:           logger,
	})

	if err != nil {
		t.Fatalf("NewRetentionScheduler failed: %v", err)
	}

	if s == nil {
		t.Fatal("expected scheduler, got nil")
	}

	if s.running {
		t.Error("scheduler should not be running after creation")
	}

	if s.schedule != "0 3 * * *" {
		t.Errorf("schedule = %v, want 0 3 * * *", s.schedule)
	}
}

func TestRetentionScheduler_NewRetentionScheduler_DefaultSchedule(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewRetentionScheduler(&RetentionSchedulerConfig{
		RetentionHandler: nil,
		LicenseClient:    nil,
		Schedule:         "", // Empty schedule should use default
		Logger:           logger,
	})

	if err != nil {
		t.Fatalf("NewRetentionScheduler failed: %v", err)
	}

	if s.schedule != "0 3 * * *" {
		t.Errorf("schedule = %v, want default 0 3 * * *", s.schedule)
	}
}

func TestRetentionScheduler_NewRetentionScheduler_InvalidSchedule(t *testing.T) {
	logger := zerolog.Nop()

	_, err := NewRetentionScheduler(&RetentionSchedulerConfig{
		RetentionHandler: nil,
		LicenseClient:    nil,
		Schedule:         "invalid schedule",
		Logger:           logger,
	})

	if err == nil {
		t.Error("expected error for invalid cron schedule")
	}
}

func TestRetentionScheduler_Status_NotRunning(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewRetentionScheduler(&RetentionSchedulerConfig{
		RetentionHandler: nil,
		LicenseClient:    nil,
		Schedule:         "0 3 * * *",
		Logger:           logger,
	})
	if err != nil {
		t.Fatalf("NewRetentionScheduler failed: %v", err)
	}

	status := s.Status()

	running, ok := status["running"].(bool)
	if !ok || running {
		t.Error("status should show running=false")
	}

	schedule, ok := status["schedule"].(string)
	if !ok || schedule != "0 3 * * *" {
		t.Errorf("schedule = %v, want 0 3 * * *", schedule)
	}

	licenseValid, ok := status["license_valid"].(bool)
	if !ok || licenseValid {
		t.Error("license_valid should be false when no license client")
	}

	// next_run should not be present when not running
	if _, ok := status["next_run"]; ok {
		t.Error("next_run should not be present when not running")
	}
}

func TestRetentionScheduler_IsRunning(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewRetentionScheduler(&RetentionSchedulerConfig{
		RetentionHandler: nil,
		LicenseClient:    nil,
		Schedule:         "0 3 * * *",
		Logger:           logger,
	})
	if err != nil {
		t.Fatalf("NewRetentionScheduler failed: %v", err)
	}

	if s.IsRunning() {
		t.Error("scheduler should not be running initially")
	}
}

func TestRetentionScheduler_GetSchedule(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewRetentionScheduler(&RetentionSchedulerConfig{
		RetentionHandler: nil,
		LicenseClient:    nil,
		Schedule:         "30 4 * * *",
		Logger:           logger,
	})
	if err != nil {
		t.Fatalf("NewRetentionScheduler failed: %v", err)
	}

	if s.GetSchedule() != "30 4 * * *" {
		t.Errorf("GetSchedule() = %v, want 30 4 * * *", s.GetSchedule())
	}
}

func TestRetentionScheduler_Stop_WhenNotRunning(t *testing.T) {
	logger := zerolog.Nop()

	s, err := NewRetentionScheduler(&RetentionSchedulerConfig{
		RetentionHandler: nil,
		LicenseClient:    nil,
		Schedule:         "0 3 * * *",
		Logger:           logger,
	})
	if err != nil {
		t.Fatalf("NewRetentionScheduler failed: %v", err)
	}

	// Stop should be safe to call even when not running
	s.Stop()

	if s.IsRunning() {
		t.Error("scheduler should still be stopped")
	}
}

func TestCronScheduleValidation(t *testing.T) {
	tests := []struct {
		name     string
		schedule string
		wantErr  bool
	}{
		{"daily at 3am", "0 3 * * *", false},
		{"every hour at :05", "5 * * * *", false},
		{"every 6 hours", "0 */6 * * *", false},
		{"weekly on Sunday at midnight", "0 0 * * 0", false},
		{"first of month at noon", "0 12 1 * *", false},
		{"invalid - missing fields", "3 * *", true},
		{"invalid - bad expression", "invalid", true},
		{"invalid - out of range", "60 25 * * *", true},
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.Parse(tt.schedule)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for schedule %q", tt.schedule)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error for schedule %q: %v", tt.schedule, err)
			}
		})
	}
}

func TestCronNextRun(t *testing.T) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	// Test "0 3 * * *" (daily at 3am)
	schedule, err := parser.Parse("0 3 * * *")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	now := time.Now()
	nextRun := schedule.Next(now)

	// Next run should be in the future
	if !nextRun.After(now) {
		t.Error("next run should be in the future")
	}

	// Next run should be at 3:00
	if nextRun.Hour() != 3 || nextRun.Minute() != 0 {
		t.Errorf("next run should be at 03:00, got %02d:%02d", nextRun.Hour(), nextRun.Minute())
	}

	// Next run should be within 24 hours
	if nextRun.Sub(now) > 24*time.Hour {
		t.Error("next run should be within 24 hours")
	}
}
