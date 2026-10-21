package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Continuous-query policies are cluster state (#1324 stage 3). Execution
// output is not part of Raft: only its definition and durable watermark are.
const (
	CapabilityClusterCQ = "cluster_policies_cq_v1"
	MaxSeedCQPolicies   = 2000
	// A forwarded Raft command JSON-encodes its byte payload (base64), so the
	// seed must stay below the coordinator protocol's 1 MiB frame limit.
	MaxSeedCQPayloadBytes = 700 << 10
)

// CQPolicyEntry is the canonical definition and durable progress of a CQ.
type CQPolicyEntry struct {
	ID                     int64      `json:"id"`
	Name                   string     `json:"name"`
	Description            *string    `json:"description,omitempty"`
	Database               string     `json:"database"`
	SourceMeasurement      string     `json:"source_measurement"`
	DestinationMeasurement string     `json:"destination_measurement"`
	Query                  string     `json:"query"`
	Interval               string     `json:"interval"`
	TagColumns             []string   `json:"tag_columns,omitempty"`
	RetentionDays          *int       `json:"retention_days,omitempty"`
	DeleteSourceAfterDays  *int       `json:"delete_source_after_days,omitempty"`
	IsActive               bool       `json:"is_active"`
	LastProcessedTime      *time.Time `json:"last_processed_time,omitempty"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
	Version                uint64     `json:"version"`
	Sources                []CQSource `json:"sources,omitempty"`
}

type CQSource struct {
	Node string `json:"node"`
	ID   int64  `json:"id"`
}

type CreateCQPayload struct {
	Entry CQPolicyEntry `json:"entry"`
	Now   time.Time     `json:"now"`
}
type UpdateCQPayload struct {
	ID              int64         `json:"id"`
	ExpectedVersion uint64        `json:"expected_version"`
	Entry           CQPolicyEntry `json:"entry"`
	Now             time.Time     `json:"now"`
}
type DeleteCQPayload struct {
	ID              int64  `json:"id"`
	ExpectedVersion uint64 `json:"expected_version"`
}
type CompleteCQPayload struct {
	ID             int64     `json:"id"`
	Version        uint64    `json:"version"`
	AuthorityEpoch uint64    `json:"authority_epoch"`
	Executor       string    `json:"executor"`
	ExpectedStart  time.Time `json:"expected_start"`
	End            time.Time `json:"end"`
}
type SeedCQPayload struct {
	Epoch   uint64          `json:"epoch"`
	Entries []CQPolicyEntry `json:"entries"`
	Floor   int64           `json:"floor"`
	Now     time.Time       `json:"now"`
}
type EnableCQPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

var (
	ErrCQNotFound  = errors.New("continuous query not found")
	ErrCQConflict  = errors.New("continuous query changed; retry")
	ErrCQNameTaken = errors.New("a continuous query with that name already exists")
	ErrCQNotSeeded = errors.New("cluster continuous queries are not active")
	ErrCQTooLarge  = errors.New("continuous query definition exceeds the Raft forwarding limit")
)

type CQMirror struct {
	Entries   []CQPolicyEntry
	Floor     int64
	SeededIDs map[int64]bool
	Epoch     uint64
}

func (f *ClusterFSM) CQEnabled() bool { f.mu.RLock(); defer f.mu.RUnlock(); return f.cqEnabled }
func (f *ClusterFSM) CQsSeeded() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cqEnabled && f.cqSeeded
}
func (f *ClusterFSM) PrimaryEpoch() uint64 { f.mu.RLock(); defer f.mu.RUnlock(); return f.primaryEpoch }
func (f *ClusterFSM) CQ(id int64) (CQPolicyEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.cqPolicies[id]
	if !ok {
		return CQPolicyEntry{}, false
	}
	return copyCQ(*e), true
}
func (f *ClusterFSM) CQByName(name string) (CQPolicyEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, e := range f.cqPolicies {
		if e.Name == name {
			return copyCQ(*e), true
		}
	}
	return CQPolicyEntry{}, false
}
func (f *ClusterFSM) CQs() []CQPolicyEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.cqEntriesLocked()
}
func (f *ClusterFSM) CQMirrorState() CQMirror {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := map[int64]bool{}
	for id := range f.cqSeededIDs {
		ids[id] = true
	}
	return CQMirror{Entries: f.cqEntriesLocked(), Floor: f.cqFloor, SeededIDs: ids, Epoch: f.primaryEpoch}
}

func copyCQ(e CQPolicyEntry) CQPolicyEntry {
	if e.Description != nil {
		v := *e.Description
		e.Description = &v
	}
	if e.RetentionDays != nil {
		v := *e.RetentionDays
		e.RetentionDays = &v
	}
	if e.DeleteSourceAfterDays != nil {
		v := *e.DeleteSourceAfterDays
		e.DeleteSourceAfterDays = &v
	}
	if e.LastProcessedTime != nil {
		v := *e.LastProcessedTime
		e.LastProcessedTime = &v
	}
	e.TagColumns = append([]string(nil), e.TagColumns...)
	e.Sources = append([]CQSource(nil), e.Sources...)
	return e
}
func (f *ClusterFSM) cqEntriesLocked() []CQPolicyEntry {
	out := make([]CQPolicyEntry, 0, len(f.cqPolicies))
	for _, e := range f.cqPolicies {
		out = append(out, copyCQ(*e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (f *ClusterFSM) applyEnableCQ(raw []byte) interface{} {
	var p EnableCQPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.clusterPolicies {
		return errors.New("enable continuous queries: cluster policies are not enabled")
	}
	if f.cqEnabled {
		return nil
	}
	listed := map[string]bool{}
	for _, id := range p.Nodes {
		listed[id] = true
	}
	var missing []string
	for id := range f.nodes {
		if !listed[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("enable continuous queries: nodes %v lack support", missing)
	}
	f.cqEnabled = true
	return nil
}
func (f *ClusterFSM) applyCreateCQ(raw []byte) interface{} {
	var p CreateCQPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cqSeeded {
		return ErrCQNotSeeded
	}
	if !validCQ(p.Entry) {
		return errors.New("invalid continuous query")
	}
	if err := ValidateCQEntrySize(p.Entry); err != nil {
		return err
	}
	for _, e := range f.cqPolicies {
		if e.Name == p.Entry.Name {
			return ErrCQNameTaken
		}
	}
	f.cqSeq++
	e := copyCQ(p.Entry)
	e.ID = f.cqSeq
	e.Version = 1
	e.CreatedAt = p.Now.UTC()
	e.UpdatedAt = e.CreatedAt
	f.cqPolicies[e.ID] = &e
	return nil
}
func (f *ClusterFSM) applyUpdateCQ(raw []byte) interface{} {
	var p UpdateCQPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cqSeeded {
		return ErrCQNotSeeded
	}
	prev := f.cqPolicies[p.ID]
	if prev == nil {
		return ErrCQNotFound
	}
	if prev.Version != p.ExpectedVersion {
		return ErrCQConflict
	}
	if !validCQ(p.Entry) {
		return errors.New("invalid continuous query")
	}
	if err := ValidateCQEntrySize(p.Entry); err != nil {
		return err
	}
	for id, e := range f.cqPolicies {
		if id != p.ID && e.Name == p.Entry.Name {
			return ErrCQNameTaken
		}
	}
	e := copyCQ(p.Entry)
	e.ID = p.ID
	e.Version = prev.Version + 1
	e.LastProcessedTime = prev.LastProcessedTime
	e.CreatedAt = prev.CreatedAt
	e.UpdatedAt = p.Now.UTC()
	e.Sources = append([]CQSource(nil), prev.Sources...)
	f.cqPolicies[p.ID] = &e
	return nil
}
func (f *ClusterFSM) applyDeleteCQ(raw []byte) interface{} {
	var p DeleteCQPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cqSeeded {
		return ErrCQNotSeeded
	}
	e := f.cqPolicies[p.ID]
	if e == nil {
		return ErrCQNotFound
	}
	if e.Version != p.ExpectedVersion {
		return ErrCQConflict
	}
	delete(f.cqPolicies, p.ID)
	return nil
}
func (f *ClusterFSM) applyCompleteCQ(raw []byte) interface{} {
	var p CompleteCQPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cqSeeded {
		return ErrCQNotSeeded
	}
	e := f.cqPolicies[p.ID]
	if e == nil {
		return ErrCQNotFound
	}
	if e.Version != p.Version || f.primaryWriterID != p.Executor || f.primaryEpoch != p.AuthorityEpoch {
		return ErrCQConflict
	}
	var current time.Time
	if e.LastProcessedTime != nil {
		current = e.LastProcessedTime.UTC()
	}
	if !current.Equal(p.ExpectedStart.UTC()) || !p.End.After(p.ExpectedStart) {
		return ErrCQConflict
	}
	end := p.End.UTC()
	e.LastProcessedTime = &end
	return nil
}
func (f *ClusterFSM) applySeedCQ(raw []byte) interface{} {
	if len(raw) > MaxSeedCQPayloadBytes {
		return fmt.Errorf("seed continuous queries: payload of %d bytes exceeds %d", len(raw), MaxSeedCQPayloadBytes)
	}
	var p SeedCQPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cqEnabled {
		return errors.New("seed continuous queries: feature is not enabled")
	}
	if f.cqSeeded {
		return nil
	}
	if open, ok := f.policyCollecting[PolicyKindCQ]; !ok || open.Epoch != p.Epoch {
		return errors.New("seed continuous queries: collection is not open")
	}
	if len(p.Entries) > MaxSeedCQPolicies {
		return errors.New("seed continuous queries: too many entries")
	}
	seenID := map[int64]bool{}
	seenName := map[string]bool{}
	policies := map[int64]*CQPolicyEntry{}
	var maxID int64
	for i := range p.Entries {
		e := copyCQ(p.Entries[i])
		if e.ID <= 0 || seenID[e.ID] || seenName[e.Name] || !validCQ(e) {
			return errors.New("seed continuous queries: invalid or duplicate entry")
		}
		if e.Version == 0 {
			e.Version = 1
		}
		seenID[e.ID] = true
		seenName[e.Name] = true
		maxID = max(maxID, e.ID)
		policies[e.ID] = &e
	}
	if p.Floor < maxID {
		return errors.New("seed continuous queries: sequence floor is below an entry ID")
	}
	f.cqPolicies = policies
	f.cqSeq = p.Floor
	f.cqFloor = p.Floor
	f.cqSeeded = true
	f.policiesSeeded[PolicyKindCQ] = true
	f.cqSeededIDs = seenID
	delete(f.policyCollecting, PolicyKindCQ)
	delete(f.policyAwaiting, PolicyKindCQ)
	delete(f.policyAdoptChoice, PolicyKindCQ)
	return nil
}
func validCQ(e CQPolicyEntry) bool {
	return e.Name != "" && e.Database != "" && e.SourceMeasurement != "" && e.DestinationMeasurement != "" && e.Query != "" && e.Interval != ""
}

func ValidateCQEntrySize(e CQPolicyEntry) error {
	encoded, err := json.Marshal(CreateCQPayload{Entry: e})
	if err != nil {
		return err
	}
	if len(encoded) > MaxSeedCQPayloadBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrCQTooLarge, len(encoded), MaxSeedCQPayloadBytes)
	}
	return nil
}
