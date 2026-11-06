package main

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/license"
)

func TestGraceRestartPersistsThroughDaySevenAndDefersHandoff(t *testing.T) {
	now := time.Now()
	s := license.ExpiryState{Valid: true, Phase: license.PhaseGraceRestarts, GraceDay: 6, RestartIntervalSeconds: 4 * 60 * 60}
	var r licenseGraceRestart
	if r.due(now, s, false) || r.due(now.Add(4*time.Hour-time.Nanosecond), s, false) {
		t.Fatal("restart before four hours")
	}
	s.Phase, s.GraceDay, s.WritersStopped = license.PhaseWritersStopped, 7, true
	if !r.due(now.Add(4*time.Hour), s, false) {
		t.Fatal("writer stop reset or removed the restart restriction")
	}
	if r.due(now.Add(4*time.Hour), s, true) {
		t.Fatal("periodic restart interrupted the sealed handoff")
	}
	if !r.due(now.Add(5*time.Hour), s, false) {
		t.Fatal("deferring the handoff lost the pending restart")
	}
}

func TestGraceRestartRenewalTrialAndFinalCutoff(t *testing.T) {
	now := time.Now()
	grace := license.ExpiryState{Valid: true, Phase: license.PhaseGraceRestarts, RestartIntervalSeconds: 4 * 60 * 60}
	for _, state := range []license.ExpiryState{
		{Valid: true, Phase: license.PhaseActive}, // active paid or trial, including a replacement key
		{Valid: true, Phase: license.PhaseGrace, GraceDay: 3},
		{Phase: license.PhaseOSS},
	} {
		var r licenseGraceRestart
		r.due(now, grace, false)
		if r.due(now.Add(5*time.Hour), state, false) || !r.next.IsZero() {
			t.Fatalf("restart survived renewal/trial/cutoff: %+v", state)
		}
		if r.due(now.Add(6*time.Hour), grace, false) || !r.due(now.Add(10*time.Hour), grace, false) {
			t.Fatal("new grace interval did not restart its four-hour clock")
		}
	}
	var r licenseGraceRestart
	r.due(now, grace, false)
	if r.due(now.Add(5*time.Hour), license.ExpiryState{Phase: license.PhaseAuthorityUnavailable}, false) {
		t.Fatal("restart without trusted license time")
	}
	if !r.due(now.Add(6*time.Hour), grace, false) {
		t.Fatal("temporary unavailable authority reset the restart deadline")
	}
}
