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
	return licenseRuntimeIngestGate(client, client != nil, false)
}

// A configured online installation cannot become an independent OSS writer
// merely because admission is unavailable. Only an authorized sealed owner or
// a positive terminal key/cutoff verdict may use the OSS runtime.
func licenseDataAllowed(client *license.Client, onlineConfigured, authorizedOSS bool) bool {
	if authorizedOSS {
		return true
	}
	if client == nil {
		return !onlineConfigured
	}
	p := client.LifecycleState()
	if p.Phase == license.PhaseOSS && (p.Reason == "signed_cutoff" || p.Reason == "key_rejected") {
		return true
	}
	return client.ServingAllowed()
}

func licenseRuntimeIngestGate(client *license.Client, onlineConfigured, authorizedOSS bool) func() error {
	return func() error {
		if !licenseDataAllowed(client, onlineConfigured, authorizedOSS) {
			return fmt.Errorf("%w: installation is not admitted by the license authority", ingest.ErrIngestFenced)
		}
		if !authorizedOSS && client != nil && client.LifecycleState().WritersStopped {
			return fmt.Errorf("%w: paid license grace period has stopped writers; renew the license", ingest.ErrIngestFenced)
		}
		return nil
	}
}

// Standalone online deployments have no cluster role gate. Keep cron armed,
// but apply the same runtime admission decision to every maintenance pass.
type standaloneLicenseCompactionGate struct{ allowed func() bool }

func (g *standaloneLicenseCompactionGate) CanCompact() bool            { return g.allowed() }
func (g *standaloneLicenseCompactionGate) CanScheduleCompaction() bool { return true }
func (g *standaloneLicenseCompactionGate) Role() string                { return "standalone" }
func (g *standaloneLicenseCompactionGate) LeaseHolder() string         { return "" }
