package license

import (
	"strings"
	"time"
)

// The grace schedule after a license expires, computed in Arc from what the
// v1 server already signs: the license's ExpiresAt and its CustomerID (a
// self-serve trial's carries the "trial-" prefix, which the server reserves
// for trials).
//
// A trial ends at its signed expiry, with no grace. A paid license keeps its
// features for fourteen grace days counted from the midnight UTC after it
// expires: grace days 1-6 run as usual (4-6 log a warning every hour), grace
// days 7-14 refuse ingestion, and at the start of day 15 the license expires
// and every Enterprise gate closes. The expiry day itself, before that first
// midnight, also runs as usual.
//
// Nothing here talks to the server or restarts anything: the phase is a pure
// function of the signed expiry, the license kind and the clock (licenseNow),
// and the gates read it.

// TrialCustomerPrefix marks a self-serve trial's CustomerID, as the license
// server's license.TrialCustomerPrefix does.
const TrialCustomerPrefix = "trial-"

// GraceDays is how many whole grace days a paid license gets after expiry.
const GraceDays = 14

// graceWarnFromDay and writesStopFromDay are the grace days on which the
// warnings and the ingestion stop begin.
const (
	graceWarnFromDay  = 4
	writesStopFromDay = 7
)

// Phase is where a license stands on the grace schedule.
type Phase int

const (
	// PhaseActive: before the signed expiry.
	PhaseActive Phase = iota
	// PhaseGrace: a paid license past its expiry, grace days 0-3; full service.
	PhaseGrace
	// PhaseGraceWarn: grace days 4-6; full service, with an hourly warning.
	PhaseGraceWarn
	// PhaseWritesStopped: grace days 7-14; Enterprise features stay on and
	// live ingestion is refused.
	PhaseWritesStopped
	// PhaseExpired: past the cutoff (paid) or the signed expiry (trial);
	// every Enterprise gate is closed.
	PhaseExpired
)

// String is the status /health reports for the phase.
func (p Phase) String() string {
	switch p {
	case PhaseActive:
		return "active"
	case PhaseGrace, PhaseGraceWarn:
		return "grace_period"
	case PhaseWritesStopped:
		return "writes_stopped"
	default:
		return "expired"
	}
}

// GraceSchedule is a license's grace boundaries. For a trial all three equal
// its signed expiry.
type GraceSchedule struct {
	// GraceStart is the midnight UTC after the signed expiry: the start of
	// grace day 1.
	GraceStart time.Time
	// WritesStopAt is the start of grace day 7.
	WritesStopAt time.Time
	// Cutoff is the start of grace day 15, when the license expires.
	Cutoff time.Time
}

// IsTrial reports whether the license is a self-serve trial.
func (l *License) IsTrial() bool {
	return l != nil && strings.HasPrefix(l.CustomerID, TrialCustomerPrefix)
}

// Schedule returns the license's grace boundaries.
func (l *License) Schedule() GraceSchedule {
	return scheduleFor(l.ExpiresAt, l.IsTrial())
}

func scheduleFor(expiresAt time.Time, trial bool) GraceSchedule {
	if trial {
		return GraceSchedule{GraceStart: expiresAt, WritesStopAt: expiresAt, Cutoff: expiresAt}
	}
	e := expiresAt.UTC()
	start := time.Date(e.Year(), e.Month(), e.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	return GraceSchedule{
		GraceStart:   start,
		WritesStopAt: start.AddDate(0, 0, writesStopFromDay-1),
		Cutoff:       start.AddDate(0, 0, GraceDays),
	}
}

// PhaseAt returns the license's phase at now, and its grace day (0 on the
// expiry day itself, 1-14 after; 0 when active or expired).
func (l *License) PhaseAt(now time.Time) (Phase, int) {
	if l == nil {
		return PhaseExpired, 0
	}
	return phaseAt(l.ExpiresAt, l.IsTrial(), now)
}

// Phase is PhaseAt at the license clock (licenseNow).
func (l *License) Phase() (Phase, int) {
	return l.PhaseAt(licenseNow())
}

func phaseAt(expiresAt time.Time, trial bool, now time.Time) (Phase, int) {
	if now.Before(expiresAt) {
		return PhaseActive, 0
	}
	s := scheduleFor(expiresAt, trial)
	if !now.Before(s.Cutoff) {
		return PhaseExpired, 0
	}
	day := 0
	if !now.Before(s.GraceStart) {
		day = int(now.Sub(s.GraceStart)/(24*time.Hour)) + 1
	}
	switch {
	case day >= writesStopFromDay:
		return PhaseWritesStopped, day
	case day >= graceWarnFromDay:
		return PhaseGraceWarn, day
	default:
		return PhaseGrace, day
	}
}

// validAt reports whether a license with this signed expiry and kind is still
// valid at now: before its cutoff.
func validAt(expiresAt time.Time, trial bool, now time.Time) bool {
	p, _ := phaseAt(expiresAt, trial, now)
	return p != PhaseExpired
}

// expiryStatus derives the display status and whole days remaining from the
// same signed expiry the gates use: days to the signed expiry while active,
// days to the cutoff during grace.
func expiryStatus(expiresAt time.Time, trial bool, now time.Time) (string, int) {
	p, _ := phaseAt(expiresAt, trial, now)
	switch p {
	case PhaseActive:
		return p.String(), int(expiresAt.Sub(now) / (24 * time.Hour))
	case PhaseExpired:
		return p.String(), 0
	default:
		return p.String(), int(scheduleFor(expiresAt, trial).Cutoff.Sub(now) / (24 * time.Hour))
	}
}
