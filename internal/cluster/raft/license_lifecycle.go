package raft

import (
	"encoding/json"
	"fmt"
	"slices"
)

// LicenseLifecycleState separates serving ownership from Raft voting. Nodes
// retain their configured roles and data while only these owners may serve.
// Generation is a compare-and-swap and is persisted in snapshots.
type LicenseLifecycleState struct {
	Controller   string            `json:"controller"`
	Certificate  json.RawMessage   `json:"certificate,omitempty"`
	Generation   uint64            `json:"generation"`
	Mode         string            `json:"mode"`
	Owner        string            `json:"owner"`
	Roles        map[string]string `json:"roles"`
	Participants []string          `json:"participants"`
}

func cloneLicenseLifecycle(s LicenseLifecycleState) LicenseLifecycleState {
	s.Certificate = slices.Clone(s.Certificate)
	s.Participants = slices.Clone(s.Participants)
	roles := make(map[string]string, len(s.Roles))
	for k, v := range s.Roles {
		roles[k] = v
	}
	s.Roles = roles
	return s
}

func (f *ClusterFSM) LicenseLifecycle() LicenseLifecycleState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return cloneLicenseLifecycle(f.licenseLifecycle)
}

func (f *ClusterFSM) applyLicenseLifecycle(payload []byte) interface{} {
	var s LicenseLifecycleState
	if err := json.Unmarshal(payload, &s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.Generation != f.licenseLifecycle.Generation+1 {
		return fmt.Errorf("stale license lifecycle generation")
	}
	switch s.Mode {
	case "active", "reduced", "preparing", "sealed":
	default:
		return fmt.Errorf("invalid license lifecycle mode")
	}
	if f.licenseLifecycle.Mode == "sealed" {
		return fmt.Errorf("sealed cluster cannot reopen its predecessor authority")
	}
	if s.Controller == "" || f.nodes[s.Controller] == nil {
		return fmt.Errorf("lifecycle controller is not a participant")
	}
	if len(s.Certificate) > 512*1024 {
		return fmt.Errorf("lifecycle certificate exceeds the command budget")
	}
	if len(s.Participants) != len(f.nodes) {
		return fmt.Errorf("license lifecycle must account for every participant")
	}
	seen := map[string]bool{}
	for _, id := range s.Participants {
		if id == "" || seen[id] || f.nodes[id] == nil {
			return fmt.Errorf("invalid lifecycle participant")
		}
		seen[id] = true
	}
	if s.Mode != "active" {
		owner := f.nodes[s.Owner]
		if owner == nil || (owner.Role != "writer" && owner.Role != "standalone") {
			return fmt.Errorf("OSS owner must be a configured writer")
		}
		if s.Roles[owner.Role] != s.Owner {
			return fmt.Errorf("OSS owner differs from the selected writer")
		}
		for role, id := range s.Roles {
			if n := f.nodes[id]; n == nil || n.Role != role {
				return fmt.Errorf("invalid owner for role %s", role)
			}
		}
		for _, n := range f.nodes {
			if s.Roles[n.Role] == "" {
				return fmt.Errorf("configured role %s has no owner", n.Role)
			}
		}
	}
	if f.licenseLifecycle.Mode == "preparing" && s.Mode != "sealed" && s.Mode != "active" && s.Mode != "preparing" {
		return fmt.Errorf("invalid preparation transition")
	}
	f.licenseLifecycle = cloneLicenseLifecycle(s)
	return nil
}
