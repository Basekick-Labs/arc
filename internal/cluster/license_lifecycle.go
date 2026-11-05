package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/license"
)

const lifecyclePolicyKind = "license_lifecycle_v1"
const lifecycleReleaseKind = "license_release_v1"
const lifecycleSealKind = "license_seal_v1"
const lifecycleDurableKind = "license_sealed_v1"

type lifecycleRuntime struct {
	ready           atomic.Bool
	mu              sync.Mutex
	closing         bool
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	phase           func() license.ExpiryState
	started         atomic.Bool
	preparingSeen   atomic.Bool
	sealing         atomic.Bool
	releaseApproved atomic.Uint64
	drainMu         sync.Mutex
	drained         atomic.Uint64
	drain           func(context.Context) error
	seal            func(context.Context, raft.LicenseLifecycleState) error
	persist         func(raft.LicenseLifecycleState) error
	release         func(raft.LicenseLifecycleState)
}

type lifecycleReport struct {
	Phase       license.ExpiryState `json:"phase"`
	Generation  uint64              `json:"generation"`
	Mode        string              `json:"mode"`
	Certificate *LicenseBootRecord  `json:"certificate,omitempty"`
	Drained     bool                `json:"drained"`
}

func (c *Coordinator) licensePhase() license.ExpiryState {
	if c.lifecycle.phase != nil {
		return c.lifecycle.phase()
	}
	if c.licenseClient == nil {
		return license.ExpiryState{Phase: license.PhaseOSS}
	}
	l := c.licenseClient.GetLicense()
	if l == nil {
		l = c.licenseClient.PendingRuntimeLicense()
	}
	if l == nil {
		return license.ExpiryState{Phase: license.PhaseOSS}
	}
	return licensePhaseForMode(l, c.cfg.SharedStorageMode)
}

func licensePhaseForMode(l *license.License, shared bool) license.ExpiryState {
	p := l.ExpiryState()
	if p.Valid && (!l.HasFeature(license.FeatureClustering) || (shared && !l.HasFeature(license.FeatureSharedStorageMultiWriter))) {
		p.Valid = false
		p.Phase = license.PhaseOSS
	}
	return p
}

func (c *Coordinator) LifecycleState() raft.LicenseLifecycleState {
	if c == nil || c.raftFSM == nil {
		return raft.LicenseLifecycleState{}
	}
	return c.raftFSM.LicenseLifecycle()
}

// ServingAllowed gates every external data/metadata request. Control-plane
// membership and replication remain alive until the final sealed handoff.
func (c *Coordinator) ServingAllowed() bool {
	if c == nil {
		return true
	}
	if !c.lifecycle.started.Load() && !c.lifecycleRecovery {
		return true
	}
	if c.lifecycleRecovery && !c.lifecycle.ready.Load() {
		return false
	}
	if c.licenseClient != nil && c.licenseClient.PendingRuntimeLicense() != nil {
		return false
	}
	if c.licenseClient != nil && c.raftFSM != nil {
		if l := c.licenseClient.GetLicense(); l != nil && !licenseFitsTopology(l, len(c.raftFSM.GetAllNodes()), c.raftFSM.TotalCores()) {
			return false
		}
	}
	phase := c.licensePhase()
	if phase.Phase == license.PhaseAuthorityUnavailable || phase.Phase == license.PhaseOSS {
		return false
	}
	s := c.LifecycleState()
	if s.Mode == "preparing" || s.Mode == "sealed" {
		return false
	}
	if phase.GraceDay >= 8 || s.Mode == "reduced" {
		return s.Roles[string(c.localNode.Role)] == c.localNode.ID
	}
	return true
}

func licenseFitsTopology(l *license.License, machines, cores int) bool {
	return (l.MaxMachines <= 0 || machines <= l.MaxMachines) && (l.MaxCores <= 0 || cores <= l.MaxCores)
}

func (c *Coordinator) LifecyclePreparing() bool {
	s := c.LifecycleState()
	return s.Mode == "preparing" || s.Mode == "sealed"
}

func (c *Coordinator) serviceOwner(id string) bool {
	s := c.LifecycleState()
	if s.Mode == "sealed" {
		return false
	}
	if s.Mode == "preparing" {
		n, ok := c.raftFSM.GetNode(id)
		return ok && s.Roles[n.Role] == id
	}
	if s.Mode != "reduced" {
		return c.ServingAllowed()
	}
	n, ok := c.raftFSM.GetNode(id)
	return ok && s.Roles[n.Role] == id
}

func (c *Coordinator) automaticFailoverAllowed() bool {
	p := c.licensePhase()
	return p.Valid && p.GraceDay < 8 && c.LifecycleState().Mode != "reduced" && !c.LifecyclePreparing() && c.licenseClient != nil && c.licenseClient.CanUseWriterFailover()
}

// StartLicenseLifecycle is wired only once all data services and drain hooks
// exist. Queries use the authenticated, nonce-bound existing peer RPC, including
// a signed reply bound to the request's kind and epoch. Missing/old peers block
// a transition; a local licensing failure cannot demote licensed peers.
func (c *Coordinator) StartLicenseLifecycle(drain func(context.Context) error, seal func(context.Context, raft.LicenseLifecycleState) error, persist func(raft.LicenseLifecycleState) error, release func(raft.LicenseLifecycleState)) {
	if !c.lifecycle.started.CompareAndSwap(false, true) {
		return
	}
	c.lifecycle.ctx, c.lifecycle.cancel = context.WithCancel(c.ctx)
	c.lifecycle.drain = drain
	c.lifecycle.seal = seal
	c.lifecycle.release = release
	c.lifecycle.persist = persist
	c.SetLocalPolicyLister(lifecyclePolicyKind, func(ctx context.Context) (json.RawMessage, error) {
		s := c.LifecycleState()
		var certificate *LicenseBootRecord
		certificate, _ = ReadLicenseBootRecord(c.LicenseBootPath()+".prepared", c.cfg.ClusterName)
		return json.Marshal(lifecycleReport{Certificate: certificate, Phase: c.licensePhase(), Generation: s.Generation, Mode: s.Mode, Drained: c.lifecycle.drained.Load() == s.Generation && s.Generation > 0})
	})
	c.SetLocalPolicyLister(lifecycleSealKind, func(ctx context.Context) (json.RawMessage, error) {
		s := c.LifecycleState()
		if s.Mode != "preparing" || c.lifecycle.drained.Load() != s.Generation {
			return nil, fmt.Errorf("owner is not drained for sealing")
		}
		prepared, _ := ReadLicenseBootRecord(c.LicenseBootPath()+".prepared", c.cfg.ClusterName)
		if prepared != nil && prepared.Generation == s.Generation+1 {
			return json.Marshal(map[string]bool{"sealed": true})
		}
		c.lifecycle.mu.Lock()
		if c.lifecycle.closing {
			c.lifecycle.mu.Unlock()
			return nil, fmt.Errorf("license lifecycle is stopping")
		}
		if c.lifecycle.sealing.CompareAndSwap(false, true) {
			c.lifecycle.wg.Add(1)
			go func() {
				defer c.lifecycle.wg.Done()
				defer c.lifecycle.sealing.Store(false)
				sealCtx, cancel := context.WithTimeout(c.lifecycle.ctx, 60*time.Second)
				defer cancel()
				if err := seal(sealCtx, s); err != nil {
					c.logger.Warn().Err(err).Msg("License OSS inventory seal did not complete")
				}
			}()
		}
		c.lifecycle.mu.Unlock()
		return json.Marshal(map[string]bool{"sealing": true})
	})
	c.SetLocalPolicyLister(lifecycleReleaseKind, func(ctx context.Context) (json.RawMessage, error) {
		s := c.LifecycleState()
		if s.Mode != "sealed" || c.lifecycle.drained.Load() != s.Generation {
			return nil, fmt.Errorf("release before durable sealed certificate")
		}
		if err := c.authorizeLicenseOSS(s); err != nil {
			return nil, err
		}
		time.AfterFunc(250*time.Millisecond, func() { release(s) })
		return json.Marshal(map[string]bool{"released": true})
	})
	c.SetLocalPolicyLister(lifecycleDurableKind, func(ctx context.Context) (json.RawMessage, error) {
		s := c.LifecycleState()
		if s.Mode != "sealed" || c.lifecycle.drained.Load() != s.Generation {
			return nil, fmt.Errorf("certificate is not durable")
		}
		record, err := ReadLicenseBootRecord(c.LicenseBootPath()+".sealed", c.cfg.ClusterName)
		if err != nil || record == nil || record.Generation != s.Generation {
			return nil, fmt.Errorf("sealed boot certificate unavailable: %v", err)
		}
		raw, _ := json.Marshal(record)
		digest := sha256.Sum256(raw)
		return json.Marshal(licenseBootStatus{Owner: record.Owner, Generation: record.Generation, CertificateSHA256: hex.EncodeToString(digest[:])})
	})
	c.lifecycle.wg.Add(1)
	go func() {
		defer c.lifecycle.wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-c.lifecycle.ctx.Done():
				return
			case <-tick.C:
				c.ingestPauseChanged()
				if err := c.lifecycleTick(c.lifecycle.ctx); err != nil {
					c.logger.Warn().Err(err).Msg("License lifecycle transition waiting; existing state is preserved")
				}
			}
		}
	}()
}

func chooseLifecycleOwners(nodes []*raft.NodeInfo, primary, compactor string) (raft.LicenseLifecycleState, error) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	s := raft.LicenseLifecycleState{Roles: map[string]string{}}
	for _, n := range nodes {
		s.Participants = append(s.Participants, n.ID)
		if n.State != "healthy" {
			continue
		}
		if s.Roles[n.Role] == "" {
			s.Roles[n.Role] = n.ID
		}
		if n.ID == primary && (n.Role == "writer" || n.Role == "standalone") {
			s.Roles[n.Role] = n.ID
		}
		if n.ID == compactor && n.Role == "compactor" {
			s.Roles[n.Role] = n.ID
		}
	}
	for _, n := range nodes {
		if s.Roles[n.Role] == "" {
			return s, fmt.Errorf("no healthy owner for configured role %s", n.Role)
		}
	}
	s.Owner = s.Roles["writer"]
	if s.Owner == "" {
		s.Owner = s.Roles["standalone"]
	}
	if s.Owner == "" {
		return s, fmt.Errorf("no writer for OSS ownership")
	}
	return s, nil
}

func (c *Coordinator) lifecycleReports(ctx context.Context) (map[string]lifecycleReport, error) {
	answers := c.queryAllNodes(ctx, lifecyclePolicyKind, 0)
	out := map[string]lifecycleReport{}
	for id, a := range answers {
		if a.err != nil {
			return nil, fmt.Errorf("participant %s: %w", id, a.err)
		}
		var r lifecycleReport
		if err := json.Unmarshal(a.rows, &r); err != nil {
			return nil, err
		}
		out[id] = r
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no lifecycle participants")
	}
	return out, nil
}

func (c *Coordinator) lifecycleTick(ctx context.Context) error {
	s := c.LifecycleState()
	if s.Mode == "preparing" {
		c.lifecycle.preparingSeen.Store(true)
	}
	if s.Mode == "active" && c.lifecycle.preparingSeen.Load() && c.lifecycle.release != nil {
		c.lifecycle.release(s)
		return nil
	}
	if s.Mode == "preparing" && c.lifecycle.drained.Load() != s.Generation {
		c.lifecycle.drainMu.Lock()
		if c.lifecycle.drained.Load() != s.Generation && c.lifecycle.drain != nil {
			drainCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			err := c.lifecycle.drain(drainCtx)
			cancel()
			if err != nil {
				c.lifecycle.drainMu.Unlock()
				return err
			}
			c.lifecycle.drained.Store(s.Generation)
		}
		c.lifecycle.drainMu.Unlock()
	}
	if s.Mode == "sealed" && c.lifecycle.drained.Load() != s.Generation {
		if c.lifecycle.persist == nil {
			return fmt.Errorf("missing durable lifecycle persistence")
		}
		if err := c.lifecycle.persist(s); err != nil {
			return err
		}
		c.lifecycle.drained.Store(s.Generation)
	}
	if s.Mode == "sealed" && s.Controller == c.localNode.ID {
		if c.lifecycle.releaseApproved.Load() != s.Generation {
			reports, err := c.lifecycleReports(ctx)
			if err != nil {
				return err
			}
			for _, r := range reports {
				if r.Generation != s.Generation || r.Mode != "sealed" || !r.Drained {
					return fmt.Errorf("waiting for all durable sealed certificates")
				}
			}
			// Persist global approval before releasing even the first peer.
			// Canonical boot authority therefore proves all peers had sealed;
			// a merely local .sealed record must restart Raft instead.
			if err := c.authorizeLicenseOSS(s); err != nil {
				return err
			}
			c.lifecycle.releaseApproved.Store(s.Generation)
		}
		return c.releaseLicensePeers(ctx, s)
	}
	if c.raftNode == nil || !c.raftNode.IsLeader() {
		return nil
	}
	reportCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	reports, err := c.lifecycleReports(reportCtx)
	if err != nil {
		return err
	}
	allActive, allReduced, allOSS, allDrained := true, true, true, true
	for _, r := range reports {
		allActive = allActive && r.Phase.Valid && r.Phase.GraceDay < 8
		allReduced = allReduced && r.Phase.Valid && r.Phase.GraceDay >= 8
		allOSS = allOSS && r.Phase.Phase == license.PhaseOSS
		allDrained = allDrained && r.Generation == s.Generation && r.Mode == "preparing" && r.Drained
	}
	if allActive && (s.Mode == "reduced" || s.Mode == "preparing") {
		s.Generation++
		s.Mode = "active"
		_, err = c.proposeLeader(raft.CommandLicenseLifecycle, s)
		return err
	}
	if (allReduced && s.Mode != "reduced") || (allOSS && s.Mode != "preparing" && s.Mode != "sealed") {
		next, err := chooseLifecycleOwners(c.raftFSM.GetAllNodes(), c.raftFSM.GetPrimaryWriterID(), c.raftFSM.GetActiveCompactorID())
		if err != nil {
			return err
		}
		next.Controller = c.localNode.ID
		next.Generation = s.Generation + 1
		next.Mode = "reduced"
		if allOSS {
			next.Mode = "preparing"
		}
		_, err = c.proposeLeader(raft.CommandLicenseLifecycle, next)
		return err
	}
	if allOSS && s.Mode == "preparing" && allDrained && c.lifecycle.seal != nil {
		ownerReport := reports[s.Owner]
		allSealed := true
		for _, r := range reports {
			allSealed = allSealed && r.Certificate != nil && r.Certificate.Generation == s.Generation+1
		}
		if !allSealed {
			answers := c.queryAllNodes(ctx, lifecycleSealKind, s.Generation)
			for id, a := range answers {
				if a.err != nil {
					return fmt.Errorf("seal replica %s: %w", id, a.err)
				}
			}
			return nil
		}
		for id, r := range reports {
			if r.Certificate.InventorySHA256 != ownerReport.Certificate.InventorySHA256 {
				return fmt.Errorf("sealed replica %s disagrees on manifest", id)
			}
		}
		certificate, err := json.Marshal(ownerReport.Certificate)
		if err != nil {
			return err
		}
		s.Certificate = certificate
		s.Generation++
		s.Mode = "sealed"
		_, err = c.proposeLeader(raft.CommandLicenseLifecycle, s)
		return err
	}

	return nil
}

func (c *Coordinator) LicenseBootPath() string {
	return filepath.Join(c.cfg.RaftDataDir, "license-lifecycle.json")
}

func (c *Coordinator) releaseLicensePeers(ctx context.Context, s raft.LicenseLifecycleState) error {
	var wg sync.WaitGroup
	failures := make(chan error, len(s.Participants))
	for _, id := range s.Participants {
		if id == c.localNode.ID {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, err := c.requestLocalPolicies(ctx, id, c.leaderCoordinatorAddress(id), lifecycleReleaseKind, s.Generation)
			if err != nil {
				failures <- fmt.Errorf("release %s: %w", id, err)
			}
		}(id)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		return err
	}
	if c.lifecycle.release != nil {
		c.lifecycle.release(s)
	}
	return nil
}

// StopLicenseLifecycle joins controller/sealing work before metadata, storage or
// Raft can close. RPC handlers cannot start another seal after closing is set.
func (c *Coordinator) StopLicenseLifecycle(ctx context.Context) error {
	c.lifecycle.mu.Lock()
	c.lifecycle.closing = true
	if c.lifecycle.cancel != nil {
		c.lifecycle.cancel()
	}
	c.lifecycle.mu.Unlock()
	done := make(chan struct{})
	go func() { c.lifecycle.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReadServingAllowed retains data access while the immutable inventory is
// materialized. Writes and administrative mutations remain fenced. The existing
// query handlers independently validate read-only SQL and enforce data auth.
func (c *Coordinator) ReadServingAllowed() bool {
	if c == nil {
		return true
	}
	s := c.LifecycleState()
	if s.Mode == "preparing" {
		return c.lifecycle.ready.Load() && s.Roles[string(c.localNode.Role)] == c.localNode.ID
	}
	return c.ServingAllowed()
}
