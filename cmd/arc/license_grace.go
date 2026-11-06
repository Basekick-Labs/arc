package main

import (
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/license"
)

// The interval measures eligible process uptime, not UTC slots shared by all
// nodes. A phase advance or ordinary authority refresh must not reset it.
type licenseGraceRestart struct {
	next time.Time
}

func (r *licenseGraceRestart) due(now time.Time, state license.ExpiryState, preparing bool) bool {
	if state.Phase == license.PhaseAuthorityUnavailable {
		return false
	}
	if !state.Valid || state.RestartIntervalSeconds == 0 {
		r.next = time.Time{}
		return false
	}
	interval := time.Duration(state.RestartIntervalSeconds) * time.Second
	if r.next.IsZero() {
		r.next = now.Add(interval)
		return false
	}
	return !preparing && !now.Before(r.next)
}

func licenseIngestGate(client *license.Client) func() error {
	return func() error {
		if client != nil && client.GetLicense().ExpiryState().WritersStopped {
			return fmt.Errorf("%w: paid license grace period has stopped writers; renew the license", ingest.ErrIngestFenced)
		}
		return nil
	}
}
