package license

import (
	"fmt"
	"time"
)

// ExpiryPolicy explicitly separates the paid term from its final entitlement
// cutoff. Legacy license expires_at values are never guessed to be term ends.
type ExpiryPolicy struct {
	Version    int       `json:"version"`
	Kind       string    `json:"kind"`
	TermEndsAt time.Time `json:"term_ends_at"`
	GraceDays  int       `json:"grace_days"`
}

type Phase string

const (
	PhaseAuthorityUnavailable Phase = "authority_unavailable"
	PhaseActive               Phase = "active"
	PhaseExpirationDay        Phase = "expiration_day"
	PhaseGrace                Phase = "grace_full"
	PhaseReducedRoles         Phase = "reduced_roles"
	PhaseRBACReadOnly         Phase = "rbac_read_only"
	PhaseTieringReadOnly      Phase = "tiering_read_only"
	PhaseOSS                  Phase = "oss_required"
)

type ExpiryState struct {
	Phase    Phase  `json:"phase"`
	GraceDay int    `json:"grace_day"`
	Status   string `json:"status"`
	Valid    bool   `json:"valid"`
}

func (p *ExpiryPolicy) cutoff() time.Time {
	if p.Kind == "trial" {
		return p.TermEndsAt.UTC()
	}
	term := p.TermEndsAt.UTC()
	return time.Date(term.Year(), term.Month(), term.Day()+15, 0, 0, 0, 0, time.UTC)
}

func (p *ExpiryPolicy) validate(expires time.Time) error {
	if p == nil {
		return nil
	}
	if p.Version != 1 || p.TermEndsAt.IsZero() ||
		(p.Kind != "paid" && p.Kind != "trial") ||
		(p.Kind == "paid" && p.GraceDays != 14) || (p.Kind == "trial" && p.GraceDays != 0) ||
		!expires.Equal(p.cutoff()) {
		return fmt.Errorf("unsupported or contradictory signed expiry policy")
	}
	return nil
}

// state uses UTC calendar dates, not elapsed 24-hour blocks after the timestamp.
// The first grace day starts at midnight following the paid expiration date.
func (p *ExpiryPolicy) state(now time.Time) ExpiryState {
	now = now.UTC()
	if now.Before(p.TermEndsAt) {
		return ExpiryState{Phase: PhaseActive, Status: "active", Valid: true}
	}
	if p.Kind == "trial" || !now.Before(p.cutoff()) {
		return ExpiryState{Phase: PhaseOSS, Status: "expired"}
	}
	term := p.TermEndsAt.UTC()
	date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	termDate := time.Date(term.Year(), term.Month(), term.Day(), 0, 0, 0, 0, time.UTC)
	day := int(date.Sub(termDate) / (24 * time.Hour))
	phase := PhaseGrace
	switch {
	case day == 0:
		phase = PhaseExpirationDay
	case day >= 10:
		phase = PhaseTieringReadOnly
	case day >= 9:
		phase = PhaseRBACReadOnly
	case day >= 8:
		phase = PhaseReducedRoles
	}
	return ExpiryState{Phase: phase, GraceDay: day, Status: "grace_period", Valid: true}
}

// ExpiryState reports desired policy phase. It does not assert that topology
// contraction or an OSS ownership handoff has completed.
func (l *License) ExpiryState() ExpiryState {
	if l == nil {
		return ExpiryState{Phase: PhaseOSS, Status: "unlicensed"}
	}
	return l.expiryStateAt(l.currentTime())
}

func (l *License) expiryStateAt(now time.Time) ExpiryState {
	if l.clock != nil && now.IsZero() {
		return ExpiryState{Phase: PhaseAuthorityUnavailable, Status: "time_unavailable"}
	}
	if l.Policy != nil {
		return l.Policy.state(now)
	}
	if validAt(l.ExpiresAt, now) {
		return ExpiryState{Phase: PhaseActive, Status: "active", Valid: true}
	}
	return ExpiryState{Phase: PhaseOSS, Status: "expired"}
}

// CanManageRBAC restricts grant/configuration changes, never existing data auth.
func (l *License) CanManageRBAC() bool {
	if !l.HasFeature(FeatureRBAC) {
		return false
	}
	state := l.ExpiryState()
	return state.GraceDay < 9
}

// CanMigrateTieredStorage is independent of read access to existing cold data.
func (l *License) CanMigrateTieredStorage() bool {
	if !l.CanUseTieredStorage() {
		return false
	}
	return l.ExpiryState().GraceDay < 10
}
