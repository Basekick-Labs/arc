package license

import (
	"encoding/json"
	"sort"
)

func runtimeShape(l *License) string {
	if l == nil {
		return ""
	}
	features := append([]string(nil), l.Features...)
	sort.Strings(features)
	raw, _ := json.Marshal(struct {
		Tier            Tier
		Cores, Machines int
		Features        []string
	}{l.Tier, l.MaxCores, l.MaxMachines, features})
	return string(raw)
}

// PinRuntimeLicense records the entitlement used to construct this process.
// Subsequent grants with different capabilities/limits remain pending until a
// fresh process applies them. Time-only refreshes remain uninterrupted.
func (c *Client) PinRuntimeLicense() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runtimeShapePinned = true
	c.pinnedRuntimeShape = runtimeShape(c.license)
}

func (c *Client) storeRuntimeLicenseLocked(l *License) {
	if c.runtimeShapePinned && runtimeShape(l) != c.pinnedRuntimeShape {
		c.runtimeRestartRequired = true
	}
	c.license = l
	c.admissionBlocked = false
	c.keyRejected = false
}

// AdmissionBlocked reports an installation-local refusal. It does not say that
// the key or its signed policy expired, and it never exposes the retained grant.
func (c *Client) AdmissionBlocked() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.admissionBlocked
}

// ServingAllowed requires a currently admitted, constructed licensed runtime.
// Callers distinguish genuine OSS deployments and a completed OSS handoff.
func (c *Client) ServingAllowed() bool {
	return c != nil && c.IsValid()
}

// LifecycleState separates absence of authority from positive terminal
// evidence. The retained signed policy and advancing trusted clock can still
// reach their real cutoff while admission is blocked; cache bytes alone cannot.
func (c *Client) LifecycleState() ExpiryState {
	unknown := ExpiryState{Phase: PhaseAuthorityUnavailable, Status: "authority_unavailable", Reason: "grant_unavailable"}
	if c == nil {
		return unknown
	}
	c.mu.RLock()
	lic, blocked, rejected := c.license, c.admissionBlocked, c.keyRejected
	needsAuthority, offline := c.requireAuthorityTime, c.offline
	c.mu.RUnlock()
	if rejected {
		return ExpiryState{Phase: PhaseOSS, Status: "rejected", Reason: "key_rejected"}
	}
	state := unknown
	if lic != nil && (offline || !needsAuthority || (lic.clock != nil && lic.clock.source() == "license_server")) {
		state = lic.ExpiryState()
		if state.Phase == PhaseOSS {
			state.Reason = "signed_cutoff"
			return state
		}
	}
	if blocked {
		return ExpiryState{Phase: PhaseAdmissionBlocked, Status: "admission_blocked", Reason: "installation_not_admitted"}
	}
	return state
}
