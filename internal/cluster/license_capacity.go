package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/license"
	hraft "github.com/hashicorp/raft"
)

type lifecycleCapacity struct {
	Grant       string `json:"grant"`
	MaxMachines int    `json:"max_machines"`
	MaxCores    int    `json:"max_cores"`
	Ready       bool   `json:"ready"`
}

const lifecycleCapacityCapability = "license_capacity_v1"

func retainedSharedWriter(nodes []*raft.NodeInfo, preferred string, maxCores int) string {
	for _, n := range nodes {
		if n.ID == preferred && n.Role == "writer" && n.CoreCount > 0 && (maxCores <= 0 || n.CoreCount <= maxCores) {
			return preferred
		}
	}
	return ""
}

// Bind agreement to the installed key and its capabilities, without publishing
// a credential. Expiry refreshes do not change constructed runtime capabilities.
func licenseCapacity(l *license.License) lifecycleCapacity {
	if l == nil {
		return lifecycleCapacity{}
	}
	features := slices.Clone(l.Features)
	sort.Strings(features)
	raw, _ := json.Marshal(struct {
		Key             string
		Tier            license.Tier
		Cores, Machines int
		Features        []string
	}{l.LicenseKey, l.Tier, l.MaxCores, l.MaxMachines, features})
	digest := sha256.Sum256(raw)
	return lifecycleCapacity{Grant: hex.EncodeToString(digest[:]), MaxMachines: l.MaxMachines, MaxCores: l.MaxCores, Ready: true}
}

func (c *Coordinator) localLicenseCapacity() lifecycleCapacity {
	if c.licenseClient == nil {
		return lifecycleCapacity{}
	}
	if pending := c.licenseClient.PendingRuntimeLicense(); pending != nil {
		p := licenseCapacity(pending)
		p.Ready = false
		return p
	}
	return licenseCapacity(c.licenseClient.GetLicense())
}

func capacityTopology(nodes []*raft.NodeInfo) string {
	type member struct {
		ID, Role string
		Cores    int
	}
	members := make([]member, 0, len(nodes))
	for _, n := range nodes {
		members = append(members, member{n.ID, n.Role, n.CoreCount})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	raw, _ := json.Marshal(members)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// The designated local-storage primary is mandatory: selecting a cheaper
// standby cannot replace the data-ownership and recovery promotion protocol.
func chooseServingAllocation(nodes []*raft.NodeInfo, primary string, capacity lifecycleCapacity) (raft.LicenseLifecycleState, error) {
	nodes = slices.Clone(nodes)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	s := raft.LicenseLifecycleState{Mode: "active", Allocation: &raft.LicenseServingAllocation{
		Grant: capacity.Grant, Topology: capacityTopology(nodes), MaxMachines: capacity.MaxMachines, MaxCores: capacity.MaxCores,
	}}
	var owner *raft.NodeInfo
	for _, n := range nodes {
		s.Participants = append(s.Participants, n.ID)
		if primary != "" && n.ID == primary && (n.Role == "writer" || n.Role == "standalone") {
			owner = n
		}
	}
	if primary == "" {
		for _, n := range nodes {
			if (n.Role == "writer" || n.Role == "standalone") && n.CoreCount > 0 && (capacity.MaxCores <= 0 || n.CoreCount <= capacity.MaxCores) {
				owner = n
				break
			}
		}
	}
	if owner == nil || owner.CoreCount <= 0 || (capacity.MaxCores > 0 && owner.CoreCount > capacity.MaxCores) {
		return s, fmt.Errorf("licensed capacity cannot serve the designated writer; waiting for its runtime core limit")
	}
	s.Owner = owner.ID
	s.Allocation.ServingIDs = append(s.Allocation.ServingIDs, owner.ID)
	cores := owner.CoreCount
	priority := func(role string) int {
		switch role {
		case "reader":
			return 0
		case "compactor":
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(nodes, func(i, j int) bool { return priority(nodes[i].Role) < priority(nodes[j].Role) })
	for _, n := range nodes {
		if n.ID == owner.ID || n.CoreCount <= 0 {
			continue
		}
		if capacity.MaxMachines > 0 && len(s.Allocation.ServingIDs) >= capacity.MaxMachines {
			break
		}
		if capacity.MaxCores > 0 && n.CoreCount > capacity.MaxCores-cores {
			continue
		}
		s.Allocation.ServingIDs = append(s.Allocation.ServingIDs, n.ID)
		cores += n.CoreCount
	}
	return s, nil
}

// Validate against both the current runtime license and current FSM counts.
// A refreshed member's core count can invalidate a previously committed budget.
func allocationAllows(s raft.LicenseLifecycleState, nodes []*raft.NodeInfo, capacity lifecycleCapacity, id string) bool {
	a := s.Allocation
	if a == nil || !capacity.Ready || capacity.Grant == "" || a.Grant != capacity.Grant || !slices.Contains(a.ServingIDs, id) {
		return false
	}
	if capacity.MaxMachines > 0 && len(a.ServingIDs) > capacity.MaxMachines {
		return false
	}
	byID := make(map[string]*raft.NodeInfo, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}
	cores := 0
	for _, selected := range a.ServingIDs {
		n := byID[selected]
		if n == nil || n.CoreCount <= 0 || (capacity.MaxCores > 0 && n.CoreCount > capacity.MaxCores-cores) {
			return false
		}
		cores += n.CoreCount
	}
	return true
}

func (c *Coordinator) allocationAllows(id string) bool {
	s := c.LifecycleState()
	return s.Allocation == nil || (c.raftFSM != nil && allocationAllows(s, c.raftFSM.GetAllNodes(), c.localLicenseCapacity(), id))
}

func (c *Coordinator) capacityLeadershipTarget(cfg hraft.Configuration, capacity lifecycleCapacity) string {
	eligible := hraft.Configuration{}
	s := c.LifecycleState()
	nodes := c.raftFSM.GetAllNodes()
	for _, srv := range cfg.Servers {
		id := string(srv.ID)
		if !allocationAllows(s, nodes, capacity, id) {
			continue
		}
		if n, ok := c.raftFSM.GetNode(id); ok && n.Role == "writer" {
			eligible.Servers = append(eligible.Servers, srv)
		}
	}
	return c.leadershipTransferTarget(eligible, c.localNode.ID)
}

func (c *Coordinator) reconcileServingAllocation(s raft.LicenseLifecycleState, reports map[string]lifecycleReport) error {
	nodes := c.raftFSM.GetAllNodes()
	var agreed lifecycleCapacity
	allReady := true
	for _, n := range nodes {
		r, ok := reports[n.ID]
		if !ok || !r.Phase.Valid || r.Capacity.Grant == "" {
			return nil
		}
		allReady = allReady && r.Capacity.Ready
		if agreed.Grant == "" {
			agreed = r.Capacity
		}
		if r.Capacity.Grant != agreed.Grant || r.Capacity.MaxCores != agreed.MaxCores || r.Capacity.MaxMachines != agreed.MaxMachines {
			return nil
		}
	}
	if agreed.Grant == "" {
		return nil
	}
	if s.Mode == "preparing" || s.Mode == "reduced" {
		// Cancellation precedes runtime restart: preparation permanently closed
		// admission, and main cannot restart during the final handoff itself.
		// Pending runtimes stay fenced until that restart completes.
		s.Mode, s.Allocation = "active", nil
		s.Generation++
		s.Controller = c.localNode.ID
		_, err := c.proposeLeader(raft.CommandLicenseLifecycle, s)
		return err
	}
	if !allReady {
		return nil
	}
	if s.Mode == "active" && s.Allocation != nil && s.Allocation.Grant == agreed.Grant && s.Allocation.Topology == capacityTopology(nodes) {
		return nil
	}
	primary := c.raftFSM.GetPrimaryWriterID()
	if c.cfg.SharedStorageMode && primary == "" {
		// Keep the required writer stable independently of control leadership.
		primary = retainedSharedWriter(nodes, s.Owner, agreed.MaxCores)
	}
	next, err := chooseServingAllocation(nodes, primary, agreed)
	if err != nil {
		return err
	}
	next.Generation = s.Generation + 1
	next.Controller = c.localNode.ID
	_, err = c.proposeLeader(raft.CommandLicenseLifecycle, next)
	return err
}
