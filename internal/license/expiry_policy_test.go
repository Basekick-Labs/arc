package license

import (
	"fmt"
	"testing"
	"time"
)

func TestPaidGraceDailyBehavior(t *testing.T) {
	term := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	p := &ExpiryPolicy{Version: 1, Kind: "paid", TermEndsAt: term, GraceDays: 14}
	for day := 1; day <= 14; day++ {
		t.Run(fmt.Sprintf("day_%02d", day), func(t *testing.T) {
			now := time.Date(2026, 11, 1+day, 0, 0, 0, 0, time.UTC)
			state := p.state(now)
			phase := PhaseGrace
			if day >= 7 {
				phase = Phase("writers_stopped")
			} else if day >= 4 {
				phase = Phase("grace_restarts")
			}
			if state.Phase != phase || state.GraceDay != day || !state.Valid {
				t.Errorf("state = %+v, want phase %s, grace day %d, valid", state, phase, day)
			}
			wantInterval := 0
			if day >= 4 {
				wantInterval = 4 * 60 * 60
			}
			if state.RestartIntervalSeconds != wantInterval || state.WritersStopped != (day >= 7) {
				t.Errorf("non-cumulative restrictions on day %d: %+v", day, state)
			}
			l := &License{Policy: p, ExpiresAt: p.cutoff(), Features: []string{FeatureRBAC, FeatureTieredStorage},
				clock: &authorityClock{tick: func() time.Time { return now }, origin: now, at: now}}
			if !l.CanManageRBAC() || !l.CanMigrateTieredStorage() {
				t.Error("the revised table does not restrict RBAC management or tier migrations during grace")
			}
		})
	}
}

func TestPaidGraceBoundariesAndTrialHasNoDegradation(t *testing.T) {
	term := time.Date(2026, 11, 1, 12, 34, 56, 0, time.UTC)
	paid := &ExpiryPolicy{Version: 1, Kind: "paid", TermEndsAt: term, GraceDays: 14}
	for _, tc := range []struct {
		at    time.Time
		phase Phase
		day   int
	}{
		{term.Add(-time.Nanosecond), PhaseActive, 0},
		{term, PhaseExpirationDay, 0},
		{time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond), PhaseGrace, 3},
		{time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), PhaseGraceRestarts, 4},
		{time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC), PhaseWritersStopped, 7},
		{paid.cutoff().Add(-time.Nanosecond), PhaseWritersStopped, 14},
		{paid.cutoff(), PhaseOSS, 0},
	} {
		s := paid.state(tc.at.In(time.FixedZone("local", -3*60*60)))
		if s.Phase != tc.phase || s.GraceDay != tc.day {
			t.Errorf("at %s: %+v, want %s day %d", tc.at, s, tc.phase, tc.day)
		}
	}
	start := term.AddDate(0, 0, -14)
	trial := &ExpiryPolicy{Version: 1, Kind: "trial", TermEndsAt: term}
	for day := 0; day < 14; day++ {
		s := trial.state(start.AddDate(0, 0, day))
		if !s.Valid || s.Phase != PhaseActive || s.RestartIntervalSeconds != 0 || s.WritersStopped {
			t.Errorf("trial day %d degraded: %+v", day+1, s)
		}
	}
	if s := trial.state(term.Add(-time.Nanosecond)); !s.Valid {
		t.Fatalf("trial ended early: %+v", s)
	}
	if s := trial.state(term); s.Valid || s.Phase != PhaseOSS || s.RestartIntervalSeconds != 0 || s.WritersStopped {
		t.Fatalf("trial must transition directly to OSS: %+v", s)
	}
}
