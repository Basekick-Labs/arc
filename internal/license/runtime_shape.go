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
}
