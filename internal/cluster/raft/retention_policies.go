package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Retention policies, cluster-wide (#1324 stage 2).
//
// A retention policy deletes data, so its adoption is stricter than tiering's:
// the leader seeds the nodes' rows on its own only when they agree and every
// active row has already run somewhere (a row that never ran is never
// activated without an admin choosing it). Until then retention stays in each
// node's SQLite, as before.
//
// Retention has its own flag and capability on top of the cluster policies
// flag: a build that knows only tiering would refuse these commands and drop
// this state from its snapshots, so retention is enabled only once no such
// build can be in the cluster.

// CapabilityClusterRetention is advertised at join by every build that
// applies the retention policy commands.
const CapabilityClusterRetention = "cluster_policies_retention_v1"

// MaxSeedRetentionPolicies bounds the retention policies one seed installs
// (and one node's answer to a collect).
const MaxSeedRetentionPolicies = 2000

// maxRetentionNameLen bounds a retention policy's name and measurement.
const maxRetentionNameLen = 256

// RetentionPolicyEntry is one retention policy. Measurement nil is the whole
// database. Sources, set on a seeded policy, names the node rows it was built
// from: that node's mirror moves those rows' execution history to it.
type RetentionPolicyEntry struct {
	ID            int64             `json:"id"`
	Name          string            `json:"name"`
	Database      string            `json:"database"`
	Measurement   *string           `json:"measurement,omitempty"`
	RetentionDays int               `json:"retention_days"`
	BufferDays    int               `json:"buffer_days"`
	IsActive      bool              `json:"is_active"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Sources       []RetentionSource `json:"sources,omitempty"`
}

// RetentionSource is one node's own row a seeded policy was built from.
type RetentionSource struct {
	Node string `json:"node"`
	ID   int64  `json:"id"`
}

// CreateRetentionPolicyPayload creates a retention policy; the FSM numbers it.
type CreateRetentionPolicyPayload struct {
	Name          string    `json:"name"`
	Database      string    `json:"database"`
	Measurement   *string   `json:"measurement,omitempty"`
	RetentionDays int       `json:"retention_days"`
	BufferDays    int       `json:"buffer_days"`
	IsActive      bool      `json:"is_active"`
	Now           time.Time `json:"now"`
}

// UpdateRetentionPolicyPayload changes the fields it carries. Measurement is
// written only with SetMeasurement (nil then means the whole database).
type UpdateRetentionPolicyPayload struct {
	ID             int64     `json:"id"`
	Name           *string   `json:"name,omitempty"`
	Database       *string   `json:"database,omitempty"`
	SetMeasurement bool      `json:"set_measurement,omitempty"`
	Measurement    *string   `json:"measurement,omitempty"`
	RetentionDays  *int      `json:"retention_days,omitempty"`
	BufferDays     *int      `json:"buffer_days,omitempty"`
	IsActive       *bool     `json:"is_active,omitempty"`
	Now            time.Time `json:"now"`
}

// DeleteRetentionPolicyPayload removes a retention policy.
type DeleteRetentionPolicyPayload struct {
	ID int64 `json:"id"`
}

// EnableRetentionPoliciesPayload turns retention's flag on; Nodes as in
// EnableClusterPoliciesPayload.
type EnableRetentionPoliciesPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// ChoosePolicyAdoptionPayload is an admin choosing Node's policies of Kind
// for the cluster. Hash is the hash of the rows the admin saw: the leader
// seeds only if the node still holds exactly those. Leader only.
type ChoosePolicyAdoptionPayload struct {
	Kind string    `json:"kind"`
	Node string    `json:"node"`
	Hash string    `json:"hash"`
	By   string    `json:"by,omitempty"`
	Now  time.Time `json:"now"`
}

// PolicyAdoptChoice is a pending admin choice.
type PolicyAdoptChoice struct {
	Kind string    `json:"kind"`
	Node string    `json:"node"`
	Hash string    `json:"hash"`
	By   string    `json:"by,omitempty"`
	At   time.Time `json:"at"`
}

// ErrPolicyNameTaken: a create or rename to a name another policy holds.
var ErrPolicyNameTaken = errors.New("a policy with that name already exists")

// ErrRetentionNotAboveBuffer: retention_days must stay above buffer_days.
var ErrRetentionNotAboveBuffer = errors.New("retention_days must be greater than buffer_days")

// ErrInvalidRetentionPolicy: a retention policy the cluster state refuses.
var ErrInvalidRetentionPolicy = errors.New("invalid retention policy")

// IsPolicyNameTaken / IsRetentionNotAboveBuffer report those errors, typed or
// as text (a refusal forwarded from the leader arrives as text).
func IsPolicyNameTaken(err error) bool {
	return err != nil && (errors.Is(err, ErrPolicyNameTaken) || strings.Contains(err.Error(), ErrPolicyNameTaken.Error()))
}

func IsRetentionNotAboveBuffer(err error) bool {
	return err != nil && (errors.Is(err, ErrRetentionNotAboveBuffer) || strings.Contains(err.Error(), ErrRetentionNotAboveBuffer.Error()))
}

// RetentionPoliciesEnabled reports whether retention's own flag is applied.
func (f *ClusterFSM) RetentionPoliciesEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterPolicies && f.retentionEnabled
}

func (f *ClusterFSM) applyEnableRetentionPolicies(payload []byte) interface{} {
	var p EnableRetentionPoliciesPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable retention policies payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.clusterPolicies {
		return errors.New("enable retention policies: cluster policies are not enabled")
	}
	if f.retentionEnabled {
		return nil
	}
	listed := map[string]bool{}
	for _, n := range p.Nodes {
		listed[n] = true
	}
	var missing []string
	for id := range f.nodes {
		if !listed[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("enable retention policies: nodes %v are not known to support it", missing)
	}
	f.retentionEnabled = true
	f.logger.Info().Int("nodes", len(f.nodes)).Msg("Cluster-wide retention policies enabled")
	return nil
}

// RetentionPolicy returns a copy of retention policy id.
func (f *ClusterFSM) RetentionPolicy(id int64) (RetentionPolicyEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.retentionPolicies[id]
	if !ok {
		return RetentionPolicyEntry{}, false
	}
	return copyRetentionPolicy(e), true
}

// RetentionPolicyByName returns a copy of the retention policy named name.
func (f *ClusterFSM) RetentionPolicyByName(name string) (RetentionPolicyEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, e := range f.retentionPolicies {
		if e.Name == name {
			return copyRetentionPolicy(e), true
		}
	}
	return RetentionPolicyEntry{}, false
}

// RetentionPolicies returns a copy of every retention policy, by ID.
func (f *ClusterFSM) RetentionPolicies() []RetentionPolicyEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.retentionPoliciesLocked()
}

// RetentionMirror is what a node's mirror needs: the policies, the seed's
// floor and the IDs the seed installed (see the mirror in the api package).
type RetentionMirror struct {
	Policies  []RetentionPolicyEntry
	Floor     int64
	SeededIDs map[int64]bool
}

// RetentionMirrorState returns the retention state a mirror reconciles to.
func (f *ClusterFSM) RetentionMirrorState() RetentionMirror {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := make(map[int64]bool, len(f.retentionSeededIDs))
	for id := range f.retentionSeededIDs {
		ids[id] = true
	}
	return RetentionMirror{Policies: f.retentionPoliciesLocked(), Floor: f.retentionFloor, SeededIDs: ids}
}

// PolicyAdoptChoiceFor returns kind's pending admin choice.
func (f *ClusterFSM) PolicyAdoptChoiceFor(kind string) (PolicyAdoptChoice, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, ok := f.policyAdoptChoice[kind]
	if !ok {
		return PolicyAdoptChoice{}, false
	}
	return *c, true
}

func copyRetentionPolicy(e *RetentionPolicyEntry) RetentionPolicyEntry {
	c := *e
	if e.Measurement != nil {
		v := *e.Measurement
		c.Measurement = &v
	}
	c.Sources = append([]RetentionSource(nil), e.Sources...)
	return c
}

func (f *ClusterFSM) retentionPoliciesLocked() []RetentionPolicyEntry {
	out := make([]RetentionPolicyEntry, 0, len(f.retentionPolicies))
	for _, e := range f.retentionPolicies {
		out = append(out, copyRetentionPolicy(e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// validRetentionFields applies the rules that never change between releases:
// path safety and reserved names stay with the API, so replaying the log on
// any build gives the same state.
func validRetentionFields(name, database string, measurement *string, retentionDays, bufferDays int) error {
	if name == "" || len(name) > maxRetentionNameLen {
		return fmt.Errorf("%w: a name of 1 to %d bytes is required", ErrInvalidRetentionPolicy, maxRetentionNameLen)
	}
	if err := validPolicyDatabase(database); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRetentionPolicy, err)
	}
	if measurement != nil && len(*measurement) > maxRetentionNameLen {
		return fmt.Errorf("%w: a measurement of at most %d bytes is required", ErrInvalidRetentionPolicy, maxRetentionNameLen)
	}
	if retentionDays <= 0 {
		return fmt.Errorf("%w: retention_days must be greater than 0", ErrInvalidRetentionPolicy)
	}
	if bufferDays < 0 {
		return fmt.Errorf("%w: buffer_days may not be negative", ErrInvalidRetentionPolicy)
	}
	if retentionDays <= bufferDays {
		return ErrRetentionNotAboveBuffer
	}
	return nil
}

// ValidRetentionPolicy applies the cluster state's own rules to a policy: the
// leader seeds only rows that pass them.
func ValidRetentionPolicy(name, database string, measurement *string, retentionDays, bufferDays int) error {
	return validRetentionFields(name, database, normMeasurement(measurement), retentionDays, bufferDays)
}

// normMeasurement stores "" as nil: both mean the whole database.
func normMeasurement(m *string) *string {
	if m == nil || *m == "" {
		return nil
	}
	v := *m
	return &v
}

func (f *ClusterFSM) retentionNameTakenLocked(name string, except int64) bool {
	for id, e := range f.retentionPolicies {
		if id != except && e.Name == name {
			return true
		}
	}
	return false
}

func (f *ClusterFSM) applyCreateRetentionPolicy(payload []byte) interface{} {
	var p CreateRetentionPolicyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal create retention policy payload: %w", err)
	}
	p.Measurement = normMeasurement(p.Measurement)
	if err := validRetentionFields(p.Name, p.Database, p.Measurement, p.RetentionDays, p.BufferDays); err != nil {
		return fmt.Errorf("create retention policy: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindRetention); err != nil {
		return fmt.Errorf("create retention policy: %w", err)
	}
	if !f.retentionEnabled {
		return errors.New("create retention policy: retention policies are not enabled")
	}
	if f.retentionNameTakenLocked(p.Name, 0) {
		return fmt.Errorf("create retention policy %q: %w", p.Name, ErrPolicyNameTaken)
	}
	f.retentionSeq++
	f.retentionPolicies[f.retentionSeq] = &RetentionPolicyEntry{
		ID: f.retentionSeq, Name: p.Name, Database: p.Database, Measurement: p.Measurement,
		RetentionDays: p.RetentionDays, BufferDays: p.BufferDays, IsActive: p.IsActive,
		CreatedAt: p.Now, UpdatedAt: p.Now,
	}
	return nil
}

func (f *ClusterFSM) applyUpdateRetentionPolicy(payload []byte) interface{} {
	var p UpdateRetentionPolicyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update retention policy payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindRetention); err != nil {
		return fmt.Errorf("update retention policy: %w", err)
	}
	prev, ok := f.retentionPolicies[p.ID]
	if !ok {
		return fmt.Errorf("update retention policy %d: %w", p.ID, ErrPolicyNotFound)
	}
	// The partial update is merged here, against the policy as it is when
	// the command applies: two concurrent PUTs cannot undo each other's
	// fields, and the cross-field rule sees the values the policy will hold.
	next := copyRetentionPolicy(prev)
	if p.Name != nil {
		next.Name = *p.Name
	}
	if p.Database != nil {
		next.Database = *p.Database
	}
	if p.SetMeasurement {
		next.Measurement = normMeasurement(p.Measurement)
	}
	if p.RetentionDays != nil {
		next.RetentionDays = *p.RetentionDays
	}
	if p.BufferDays != nil {
		next.BufferDays = *p.BufferDays
	}
	if p.IsActive != nil {
		next.IsActive = *p.IsActive
	}
	if err := validRetentionFields(next.Name, next.Database, next.Measurement, next.RetentionDays, next.BufferDays); err != nil {
		return fmt.Errorf("update retention policy %d: %w", p.ID, err)
	}
	if f.retentionNameTakenLocked(next.Name, p.ID) {
		return fmt.Errorf("update retention policy %d to %q: %w", p.ID, next.Name, ErrPolicyNameTaken)
	}
	next.UpdatedAt = p.Now
	f.retentionPolicies[p.ID] = &next
	return nil
}

func (f *ClusterFSM) applyDeleteRetentionPolicy(payload []byte) interface{} {
	var p DeleteRetentionPolicyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete retention policy payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindRetention); err != nil {
		return fmt.Errorf("delete retention policy: %w", err)
	}
	if _, ok := f.retentionPolicies[p.ID]; !ok {
		return fmt.Errorf("delete retention policy %d: %w", p.ID, ErrPolicyNotFound)
	}
	delete(f.retentionPolicies, p.ID)
	return nil
}

func (f *ClusterFSM) applyChoosePolicyAdoption(payload []byte) interface{} {
	var p ChoosePolicyAdoptionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal choose policy adoption payload: %w", err)
	}
	// Tiering always seeds on its own (its merge never refuses). Retention and
	// continuous queries may need an explicit choice when nodes disagree.
	if (p.Kind != PolicyKindRetention && p.Kind != PolicyKindCQ) || p.Node == "" || p.Hash == "" {
		return fmt.Errorf("choose policy adoption: a choice-requiring kind, node and hash are required (kind %q)", p.Kind)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.policyKindEnabledLocked(p.Kind) {
		return fmt.Errorf("choose policy adoption: %s policies are not enabled", p.Kind)
	}
	if f.policiesSeeded[p.Kind] {
		return fmt.Errorf("choose policy adoption: %s policies are already the cluster's", p.Kind)
	}
	if _, ok := f.nodes[p.Node]; !ok {
		return fmt.Errorf("choose policy adoption: node %q is not in the cluster", p.Node)
	}
	f.policyAdoptChoice[p.Kind] = &PolicyAdoptChoice{Kind: p.Kind, Node: p.Node, Hash: p.Hash, By: p.By, At: p.Now}
	return nil
}

// validRetentionSeed checks a retention seed before it is applied: the bound,
// unique names, and with kept IDs, unique IDs below the floor. The day rules
// are not re-checked: the leader seeds only rows that pass them.
func validRetentionSeed(p *SeedPoliciesPayload) error {
	if len(p.Tiering) > 0 {
		return errors.New("tiering rows in a retention seed")
	}
	if len(p.Retention) > MaxSeedRetentionPolicies {
		return fmt.Errorf("%d retention policies exceed %d", len(p.Retention), MaxSeedRetentionPolicies)
	}
	if p.RetentionFloor < 1 {
		return errors.New("a retention seed needs a floor of at least 1")
	}
	names := make(map[string]bool, len(p.Retention))
	ids := make(map[int64]bool, len(p.Retention))
	for _, e := range p.Retention {
		if e.Name == "" || len(e.Name) > maxRetentionNameLen {
			return fmt.Errorf("retention policy name %q is not 1 to %d bytes", e.Name, maxRetentionNameLen)
		}
		if names[e.Name] {
			return fmt.Errorf("retention policy %q listed twice", e.Name)
		}
		names[e.Name] = true
		if p.RetentionKeepIDs {
			if e.ID <= 0 || e.ID >= p.RetentionFloor || ids[e.ID] {
				return fmt.Errorf("retention policy %q: kept ID %d must be unique and in [1, %d)", e.Name, e.ID, p.RetentionFloor)
			}
			ids[e.ID] = true
		}
	}
	return nil
}

// seedRetentionLocked installs a validated retention seed.
func (f *ClusterFSM) seedRetentionLocked(p *SeedPoliciesPayload) {
	f.retentionPolicies = make(map[int64]*RetentionPolicyEntry, len(p.Retention))
	f.retentionSeededIDs = make(map[int64]bool, len(p.Retention))
	f.retentionFloor = p.RetentionFloor
	if f.retentionSeq < p.RetentionFloor-1 {
		f.retentionSeq = p.RetentionFloor - 1
	}
	for i := range p.Retention {
		c := copyRetentionPolicy(&p.Retention[i])
		c.Measurement = normMeasurement(c.Measurement)
		if !p.RetentionKeepIDs {
			f.retentionSeq++
			c.ID = f.retentionSeq
		}
		if c.CreatedAt.IsZero() {
			c.CreatedAt = p.Now
		}
		if c.UpdatedAt.IsZero() {
			c.UpdatedAt = p.Now
		}
		f.retentionPolicies[c.ID] = &c
		f.retentionSeededIDs[c.ID] = true
	}
}

func (f *ClusterFSM) snapshotRetentionLocked(st *clusterPoliciesState) {
	for _, c := range f.policyAdoptChoice {
		cc := *c
		st.Choices = append(st.Choices, &cc)
	}
	sort.Slice(st.Choices, func(i, j int) bool { return st.Choices[i].Kind < st.Choices[j].Kind })
	if !f.retentionEnabled {
		return
	}
	st.RetentionEnabled = true
	st.RetentionSeq = f.retentionSeq
	st.RetentionFloor = f.retentionFloor
	for id := range f.retentionSeededIDs {
		st.RetentionSeededIDs = append(st.RetentionSeededIDs, id)
	}
	sort.Slice(st.RetentionSeededIDs, func(i, j int) bool { return st.RetentionSeededIDs[i] < st.RetentionSeededIDs[j] })
	for _, e := range f.retentionPoliciesLocked() {
		c := e
		st.Retention = append(st.Retention, &c)
	}
}

func (f *ClusterFSM) restoreRetentionLocked(st *clusterPoliciesState) {
	f.retentionEnabled = false
	f.retentionPolicies = map[int64]*RetentionPolicyEntry{}
	f.retentionSeq = 0
	f.retentionFloor = 0
	f.retentionSeededIDs = map[int64]bool{}
	f.policyAdoptChoice = map[string]*PolicyAdoptChoice{}
	if st == nil {
		return
	}
	for _, c := range st.Choices {
		if c != nil && c.Kind != "" {
			cc := *c
			f.policyAdoptChoice[c.Kind] = &cc
		}
	}
	f.retentionEnabled = st.RetentionEnabled
	f.retentionSeq = st.RetentionSeq
	f.retentionFloor = st.RetentionFloor
	for _, id := range st.RetentionSeededIDs {
		f.retentionSeededIDs[id] = true
	}
	for _, e := range st.Retention {
		if e != nil && e.ID > 0 {
			c := copyRetentionPolicy(e)
			f.retentionPolicies[c.ID] = &c
		}
	}
}
